"""ZMODEM 会话（raw 模式）测试——只用 ``loop://`` 虚拟口。

``loop://`` 会把写入的字节原样回读，于是 ``hub.write_bytes`` 就等于“设备发来”这些
字节，worker 的读循环照常检测/断行/路由。
"""

from __future__ import annotations

import queue
import time

import pytest

from serialhub.zmodem import ABORT_SEQ

ZRQINIT = b"**\x18B00000000000000\r\x8a\x11"
POLL_TIMEOUT = 3.0


def _port(**over):
    base = {
        "name": "z",
        "device": "loop://",
        "autostart": True,
        "buffer_lines": 256,
        "buffer_bytes": 1 << 20,
        "reconnect_ms": 200,
        "reconnect_max_ms": 500,
        "flush_ms": 20,
    }
    base.update(over)
    return base


def _until(pred, timeout: float = POLL_TIMEOUT):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = pred()
        if value:
            return value
        time.sleep(0.02)
    return pred()


def _lines(hub, kinds):
    return [ln["text"] for ln in hub.read("z", kinds=kinds)["lines"]]


def _zm_events(q: "queue.Queue[dict]") -> list[dict]:
    out = []
    while True:
        try:
            ev = q.get_nowait()
        except queue.Empty:
            return out
        if ev.get("type") == "zmodem":
            out.append(ev)


def _drain(q: "queue.Queue", timeout: float = 0.5) -> bytes:
    data = bytearray()
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            chunk = q.get(timeout=0.05)
        except queue.Empty:
            continue
        if chunk is None:
            break
        data.extend(chunk)
    return bytes(data)


def _device_sends(hub, data: bytes) -> None:
    """模拟设备在会话中发字节：绕过会话期间对普通写入的拦截，直接写 loop://。"""
    hub._require_worker("z")._write_raw(data)


def _start(hub) -> None:
    hub.write_bytes("z", b"rz\r" + ZRQINIT)
    assert _until(lambda: hub.zmodem_status("z")["active"])


@pytest.fixture
def zhub(hub_factory, make_config):
    return hub_factory(make_config(ports=[_port()]))


def test_detection_broadcasts_start_and_marks(zhub) -> None:
    sub = zhub.subscribe()
    _start(zhub)
    st = zhub.zmodem_status("z")
    assert st["direction"] == "download" and st["owner"] is None
    assert st["rx_bytes"] == len(ZRQINIT)
    events = _until(lambda: _zm_events(sub) or None)
    assert events[0] == {
        "type": "zmodem", "event": "start", "port": "z",
        "session": st["session"], "direction": "download",
    }
    assert "[zmodem] start download" in _lines(zhub, {"mark"})
    assert _lines(zhub, {"rx"}) == ["rz"]  # 头前文本成行，头本身不入环


def test_payload_only_in_claimed_queue_with_replay(zhub) -> None:
    _start(zhub)
    _device_sends(zhub, b"pre-claim\n")  # 认领前到达：进 pending
    assert _until(lambda: zhub.zmodem_status("z")["rx_bytes"] > len(ZRQINIT))
    sid, q = zhub.zmodem_claim("z", "tab-1")
    assert sid == zhub.zmodem_status("z")["session"]
    _device_sends(zhub, b"post-claim\n")
    got = _drain(q)
    assert got == ZRQINIT + b"pre-claim\n" + b"post-claim\n"
    assert _lines(zhub, {"rx"}) == ["rz"]  # ring buffer 没有新 rx 行
    assert zhub.zmodem_status("z")["owner"] == "tab-1"


def test_second_claim_by_other_client_raises(zhub) -> None:
    _start(zhub)
    zhub.zmodem_claim("z", "tab-1")
    zhub.zmodem_claim("z", "tab-1")  # 同一客户端重复认领可以
    with pytest.raises(RuntimeError, match="zmodem: claimed by another client"):
        zhub.zmodem_claim("z", "tab-2")


def test_claim_without_session_raises(zhub) -> None:
    with pytest.raises(RuntimeError, match="zmodem: no active session"):
        zhub.zmodem_claim("z", "tab-1")


def test_unknown_port_raises_keyerror(zhub) -> None:
    with pytest.raises(KeyError):
        zhub.zmodem_status("nope")


def test_write_owner_only_counts_tx(zhub) -> None:
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    with pytest.raises(RuntimeError):
        zhub.zmodem_write("z", sid, "tab-2", b"x")
    assert zhub.zmodem_write("z", sid, "tab-1", b"\x18hello") == 6
    assert zhub.zmodem_status("z")["tx_bytes"] == 6
    assert _drain(q).endswith(b"\x18hello")  # loop:// 回读也进会话队列


def test_ordinary_write_refused_while_active(zhub) -> None:
    _start(zhub)
    with pytest.raises(RuntimeError, match="zmodem"):
        zhub.send("z", "ls")
    with pytest.raises(RuntimeError, match="zmodem"):
        zhub.write_bytes("z", b"x")


def test_end_is_idempotent_and_resumes_line_mode(zhub) -> None:
    sub = zhub.subscribe()
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    first = zhub.zmodem_end("z", sid, "tab-1")
    assert first["reason"] == "done"
    assert zhub.zmodem_end("z", sid, "tab-1") == first
    assert _drain(q, timeout=1.0) == ZRQINIT  # 队列以 None 结尾
    assert zhub.zmodem_status("z")["active"] is False
    ends = [e for e in _until(lambda: _zm_events(sub) or None) if e["event"] == "end"]
    assert ends == [{
        "type": "zmodem", "event": "end", "port": "z", "session": sid,
        "reason": "done", "rx_bytes": len(ZRQINIT), "tx_bytes": 0,
    }]
    assert f"[zmodem] end reason=done rx={len(ZRQINIT)} tx=0" in _lines(zhub, {"mark"})
    zhub.write_bytes("z", b"back\n")
    assert _until(lambda: "back" in _lines(zhub, {"rx"}))


def test_abort_writes_abort_seq(zhub) -> None:
    _start(zhub)
    sid, _ = zhub.zmodem_claim("z", "tab-1")
    assert zhub.zmodem_end("z", sid, "tab-1", abort=True, reason="abort")["reason"] == "abort"
    zhub.write_bytes("z", b"\n")  # loop:// 把 ABORT_SEQ 回读成文本，补一个换行收行
    assert _until(lambda: ABORT_SEQ.decode() in _lines(zhub, {"rx"}))


def test_unclaimed_timeout_aborts_and_resumes(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(zmodem_claim_ms=200)]))
    sub = hub.subscribe()
    _start(hub)
    assert _until(lambda: not hub.zmodem_status("z")["active"], timeout=2.0)
    ends = [e for e in _zm_events(sub) if e["event"] == "end"]
    assert [e["reason"] for e in ends] == ["unclaimed"]
    hub.write_bytes("z", b"hello\n")
    # ABORT_SEQ 写给设备，loop:// 把它回读成下一行文本的开头
    assert _until(lambda: ABORT_SEQ.decode() + "hello" in _lines(hub, {"rx"}))


def test_idle_timeout(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(zmodem_idle_ms=300)]))
    sub = hub.subscribe()
    _start(hub)
    hub.zmodem_claim("z", "tab-1")
    assert _until(lambda: not hub.zmodem_status("z")["active"], timeout=2.0)
    ends = [e for e in _zm_events(sub) if e["event"] == "end"]
    assert [e["reason"] for e in ends] == ["timeout"]


def test_detector_disabled_while_active(zhub) -> None:
    sub = zhub.subscribe()
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    _device_sends(zhub, ZRQINIT)
    assert _drain(q) == ZRQINIT + ZRQINIT
    assert zhub.zmodem_status("z")["session"] == sid
    starts = [e for e in _zm_events(sub) if e["event"] == "start"]
    assert len(starts) == 1


def test_close_ends_session_closed(zhub) -> None:
    sub = zhub.subscribe()
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    zhub.close("z")
    assert zhub.zmodem_status("z")["active"] is False
    ends = [e for e in _zm_events(sub) if e["event"] == "end"]
    assert [(e["session"], e["reason"]) for e in ends] == [(sid, "closed")]
    assert _drain(q, timeout=1.0) == ZRQINIT


def test_zmodem_disabled_keeps_header_as_text(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(zmodem=False)]))
    hub.write_bytes("z", b"rz\r" + ZRQINIT + b"\n")
    assert _until(lambda: len(_lines(hub, {"rx"})) >= 3)
    assert hub.zmodem_status("z")["active"] is False
    assert "".join(_lines(hub, {"rx"})).startswith("rz**\x18B0")


def test_idle_framing_port_detects_and_flushes_star(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="idle", flush_ms=20)]))
    hub.write_bytes("z", b"total **")
    assert _until(lambda: "total **" in _lines(hub, {"rx"}))  # 留住的 ** 不会卡住
    _start(hub)


def test_config_roundtrip_and_validation(make_config) -> None:
    cfg = make_config(ports=[_port(zmodem=False, zmodem_claim_ms=1234, zmodem_idle_ms=5678)])
    d = cfg.to_dict()["ports"][0]
    assert (d["zmodem"], d["zmodem_claim_ms"], d["zmodem_idle_ms"]) == (False, 1234, 5678)
    default = make_config(ports=[_port()]).port("z")
    assert (default.zmodem, default.zmodem_claim_ms, default.zmodem_idle_ms) == (True, 5000, 15000)
    with pytest.raises(ValueError, match="zmodem_claim_ms"):
        make_config(ports=[_port(zmodem_claim_ms=0)])
    with pytest.raises(ValueError, match=r"ports\[z\]\.zmodem"):
        make_config(ports=[_port(zmodem="yes")])
