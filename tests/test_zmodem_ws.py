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
        assert err == {"type": "zmodem_error", "port": "z", "error": "zmodem: no active session"}
        ws.send_json({"type": "zmodem_claim", "port": "nope"})
        assert _recv_until(ws, "zmodem_error")["port"] == "nope"


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
        assert "claimed by another client" in _recv_until(ws, "zmodem_error")["error"]
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        _recv_until(ws, "zmodem_claimed")
        ws.send_json({"type": "zmodem_write", "port": "z", "session": sid, "b64": "%%%"})
        assert "base64" in _recv_until(ws, "zmodem_error")["error"]
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
