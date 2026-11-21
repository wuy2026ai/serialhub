"""/ws 上的 ZMODEM 会话转发协议 + 会话期间 REST send 的错误形态（``loop://``，无需外部工具）。

``loop://`` 回读写入的字节：``hub.write_bytes`` 等于“设备发来”，owner 的
``zmodem_write`` 也会被回读进会话队列。
"""

from __future__ import annotations

import base64
import time

import pytest
from fastapi.testclient import TestClient

from serialhub.server import create_app
from serialhub.zmodem import ABORT_SEQ

ZRQINIT = b"**\x18B00000000000000\r\x8a\x11"


@pytest.fixture
def app_hub(hub_factory, make_config):
    cfg = make_config(ports=[{"name": "z", "device": "loop://", "autostart": True,
                              "flush_ms": 20, "reconnect_ms": 200, "reconnect_max_ms": 500}])
    hub = hub_factory(cfg)
    with TestClient(create_app(hub, cfg)) as tc:
        yield tc, hub


def _until(pred, timeout: float = 3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return pred()


def _start(hub) -> None:
    assert _until(lambda: hub.status("z")["open"])
    hub.write_bytes("z", b"rz\r" + ZRQINIT)
    assert _until(lambda: hub.zmodem_status("z")["active"])


def _recv_until(ws, mtype: str, limit: int = 200) -> dict:
    for _ in range(limit):
        msg = ws.receive_json()
        if msg.get("type") == mtype:
            return msg
    raise AssertionError(f"no {mtype}")


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def test_claim_without_session_errors(app_hub) -> None:
    tc, _hub = app_hub
    with tc.websocket_connect("/ws") as ws:
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        err = _recv_until(ws, "zmodem_error")
        assert err == {"type": "zmodem_error", "port": "z", "code": "no_session",
                       "error": "zmodem: no active session"}
        ws.send_json({"type": "zmodem_claim", "port": "nope"})
        err = _recv_until(ws, "zmodem_error")
        assert err["port"] == "nope" and err["code"] == "bad_request"


def test_claim_relays_data_write_and_end(app_hub) -> None:
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        sid = hub.zmodem_status("z")["session"]
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        claimed = _recv_until(ws, "zmodem_claimed")
        assert claimed == {"type": "zmodem_claimed", "port": "z", "session": sid,
                           "direction": "download"}
        data = _recv_until(ws, "zmodem_data")
        assert data["session"] == sid and base64.b64decode(data["b64"]) == ZRQINIT

        ws.send_json({"type": "zmodem_write", "port": "z", "session": sid, "b64": _b64(b"pong")})
        echoed = _recv_until(ws, "zmodem_data")  # loop:// 回读 owner 的写入
        assert base64.b64decode(echoed["b64"]) == b"pong"
        assert hub.zmodem_status("z")["tx_bytes"] == 4

        ws.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": False})
        closed = _recv_until(ws, "zmodem_closed")
        assert closed == {"type": "zmodem_closed", "port": "z", "session": sid, "reason": "done"}
        assert hub.zmodem_status("z")["active"] is False


def test_payload_goes_to_owner_socket_only(app_hub) -> None:
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as owner, tc.websocket_connect("/ws") as other:
        _start(hub)
        owner.send_json({"type": "zmodem_claim", "port": "z"})
        _recv_until(owner, "zmodem_claimed")
        other.send_json({"type": "zmodem_claim", "port": "z"})
        assert _recv_until(other, "zmodem_error")["error"] == "zmodem: claimed by another client"
        _recv_until(owner, "zmodem_data")
        sid = hub.zmodem_status("z")["session"]
        owner.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": True})
        assert _recv_until(owner, "zmodem_closed")["reason"] == "abort"
        # 旁观者只看到 start/end 广播，没有任何 zmodem_data
        other.send_json({"type": "ping"})
        seen = []
        while (msg := other.receive_json())["type"] != "pong":
            seen.append(msg["type"])
        assert "zmodem_data" not in seen


def test_write_by_non_owner_and_bad_base64_error(app_hub) -> None:
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        sid = hub.zmodem_status("z")["session"]
        ws.send_json({"type": "zmodem_write", "port": "z", "session": sid, "b64": _b64(b"x")})
        err = _recv_until(ws, "zmodem_error")
        assert "claimed by another client" in err["error"]
        assert err["code"] == "not_owner" and err["session"] == sid  # 回显 session
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        _recv_until(ws, "zmodem_claimed")
        ws.send_json({"type": "zmodem_write", "port": "z", "session": sid, "b64": "%%%"})
        err = _recv_until(ws, "zmodem_error")
        assert "base64" in err["error"] and err["code"] == "bad_request" and err["session"] == sid
        assert hub.zmodem_status("z")["active"] is True  # 出错不影响会话


def test_disconnect_aborts_owned_session(app_hub) -> None:
    tc, hub = app_hub
    worker = hub._require_worker("z")
    written: list[bytes] = []
    real = worker._write_raw
    worker._write_raw = lambda data: (written.append(bytes(data)), real(data))[1]
    sub = hub.subscribe()
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        _recv_until(ws, "zmodem_claimed")
    assert _until(lambda: not hub.zmodem_status("z")["active"])
    ends = []
    while not sub.empty():
        ev = sub.get_nowait()
        if ev.get("type") == "zmodem" and ev.get("event") == "end":
            ends.append(ev)
    assert [e["reason"] for e in ends] == ["disconnect"]
    assert ABORT_SEQ in written


def test_rest_send_during_session_is_json_409(app_hub) -> None:
    tc, hub = app_hub
    _start(hub)
    r = tc.post("/api/ports/z/send", json={"data": "ls"})
    assert r.status_code == 409
    body = r.json()
    assert body["ok"] is False and "zmodem: session active" in body["error"]


def test_end_with_tail_b64_reinjects_text(app_hub) -> None:
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        sid = _recv_until(ws, "zmodem_claimed")["session"]
        _recv_until(ws, "zmodem_data")
        ws.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": False,
                      "tail_b64": _b64(b"sz done rc=0\r\n")})
        assert _recv_until(ws, "zmodem_closed")["reason"] == "done"
        ws.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": False,
                      "tail_b64": "%%%"})
        assert "base64" in _recv_until(ws, "zmodem_error")["error"]
    assert _until(lambda: "sz done rc=0" in [
        ln["text"] for ln in hub.read("z", kinds={"rx"})["lines"]])


def _fail_send_of(monkeypatch, mtype: str) -> None:
    """让服务端发送某类消息时抛错（模拟客户端在这一刻断开）。"""
    from starlette.websockets import WebSocket

    real = WebSocket.send_json

    async def send_json(self, data, mode="text"):
        if isinstance(data, dict) and data.get("type") == mtype:
            raise RuntimeError("simulated send failure")
        return await real(self, data, mode)

    monkeypatch.setattr(WebSocket, "send_json", send_json)


def _end_reasons(sub) -> list[str]:
    out = []
    while not sub.empty():
        ev = sub.get_nowait()
        if ev.get("type") == "zmodem" and ev.get("event") == "end":
            out.append(ev["reason"])
    return out


def test_pump_send_failure_still_aborts_on_disconnect(app_hub, monkeypatch) -> None:
    tc, hub = app_hub
    _fail_send_of(monkeypatch, "zmodem_data")
    sub = hub.subscribe()
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        _recv_until(ws, "zmodem_claimed")
        time.sleep(0.3)  # 转发任务发送 zmodem_data 失败并退出
        assert hub.zmodem_status("z")["active"] is True
    assert _until(lambda: not hub.zmodem_status("z")["active"])
    assert _end_reasons(sub) == ["disconnect"]


def test_claim_reply_failure_still_aborts(app_hub, monkeypatch) -> None:
    tc, hub = app_hub
    _fail_send_of(monkeypatch, "zmodem_claimed")
    sub = hub.subscribe()
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        assert _until(lambda: not hub.zmodem_status("z")["active"])
    assert _end_reasons(sub) == ["disconnect"]


def test_abort_must_be_literal_true(app_hub) -> None:
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        sid = _recv_until(ws, "zmodem_claimed")["session"]
        ws.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": "false"})
        assert _recv_until(ws, "zmodem_closed")["reason"] == "done"


def test_error_codes_and_session_echo(app_hub) -> None:
    """claim 失败的 code 区分无会话 / 被他人认领；write/end 的错误回显客户端带来的 session。"""
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as owner, tc.websocket_connect("/ws") as other:
        _start(hub)
        sid = hub.zmodem_status("z")["session"]
        owner.send_json({"type": "zmodem_claim", "port": "z"})
        _recv_until(owner, "zmodem_claimed")
        other.send_json({"type": "zmodem_claim", "port": "z"})
        err = _recv_until(other, "zmodem_error")
        assert err["code"] == "claimed_by_other" and "session" not in err
        other.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": True})
        err = _recv_until(other, "zmodem_error")
        assert err["code"] == "not_owner" and err["session"] == sid
        owner.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": True})
        _recv_until(owner, "zmodem_closed")
        owner.send_json({"type": "zmodem_write", "port": "z", "session": sid, "b64": _b64(b"x")})
        err = _recv_until(owner, "zmodem_error")  # 迟到的写：会话已结束
        assert err == {"type": "zmodem_error", "port": "z", "code": "no_session",
                       "error": "zmodem: no active session", "session": sid}


def test_claim_broadcasts_claimed_to_every_socket(app_hub) -> None:
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as owner, tc.websocket_connect("/ws") as other:
        _start(hub)
        sid = hub.zmodem_status("z")["session"]
        owner.send_json({"type": "zmodem_claim", "port": "z"})
        ev = {"type": "zmodem", "event": "claimed", "port": "z", "session": sid}
        for _ in range(200):
            if other.receive_json() == ev:
                break
        else:
            raise AssertionError("no claimed broadcast")


def test_oversized_tail_rejected_session_kept(app_hub) -> None:
    """tail 解码后超过 16 KiB → bad_request，消息被拒、会话保留，客户端可重发合规的 end。"""
    tc, hub = app_hub
    with tc.websocket_connect("/ws") as ws:
        _start(hub)
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        sid = _recv_until(ws, "zmodem_claimed")["session"]
        ws.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": False,
                      "tail_b64": _b64(b"x" * (16 * 1024 + 1))})
        err = ws.receive_json()
        while err.get("type") not in ("zmodem_error", "zmodem_closed"):
            err = ws.receive_json()
        assert err["code"] == "bad_request" and err["session"] == sid
        assert hub.zmodem_status("z")["active"] is True
        ws.send_json({"type": "zmodem_end", "port": "z", "session": sid, "abort": False,
                      "tail_b64": _b64(b"y" * (16 * 1024 - 1) + b"\n")})
        assert _recv_until(ws, "zmodem_closed")["reason"] == "done"
    assert _until(lambda: "y" * (16 * 1024 - 1) in [
        ln["text"] for ln in hub.read("z", kinds={"rx"})["lines"]])
