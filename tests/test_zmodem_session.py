"""ZMODEM 会话（raw 模式）测试——只用 ``loop://`` 虚拟口。

``loop://`` 会把写入的字节原样回读，于是 ``hub.write_bytes`` 就等于“设备发来”这些
字节，worker 的读循环照常检测/断行/路由。
"""

from __future__ import annotations

import queue
import threading
import time

import pytest

from serialhub import zmodem
from serialhub.tcp_bridge import TcpBridge
from serialhub.zmodem import ABORT_SEQ, Detection

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


def _spy_writes(hub) -> list[bytes]:
    """记录 worker 的全部原始写入（含 ABORT_SEQ）。"""
    worker = hub._require_worker("z")
    real = worker._write_raw
    written: list[bytes] = []

    def spy(data: bytes, **kw) -> int:
        written.append(bytes(data))
        return real(data, **kw)

    worker._write_raw = spy  # type: ignore[method-assign]
    return written


def _drain_over(hub, timeout: float = 5.0) -> bool:
    worker = hub._require_worker("z")
    return bool(_until(lambda: worker._zm_drain_started is None, timeout=timeout))


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


def test_first_claim_broadcasts_claimed_once(zhub) -> None:
    _start(zhub)
    sub = zhub.subscribe()
    sid, _q = zhub.zmodem_claim("z", "tab-1")
    zhub.zmodem_claim("z", "tab-1")  # 重复认领不再广播
    with pytest.raises(RuntimeError):
        zhub.zmodem_claim("z", "tab-2")
    time.sleep(0.1)
    claimed = [e for e in _zm_events(sub) if e["event"] == "claimed"]
    assert claimed == [{"type": "zmodem", "event": "claimed", "port": "z", "session": sid}]


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
    assert q.get(timeout=POLL_TIMEOUT) == ZRQINIT  # 回放的 pending 已送出
    first = zhub.zmodem_end("z", sid, "tab-1")
    assert first["reason"] == "done"
    assert zhub.zmodem_end("z", sid, "tab-1") == first
    assert _drain(q, timeout=1.0) == b""  # 队列以 None 结尾
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
    written = _spy_writes(zhub)
    sid, _ = zhub.zmodem_claim("z", "tab-1")
    assert zhub.zmodem_end("z", sid, "tab-1", abort=True, reason="abort")["reason"] == "abort"
    assert written == [ABORT_SEQ]
    # loop:// 回读的 ABORT_SEQ 落在排空窗口里：被丢弃，不入环
    assert _drain_over(zhub)
    assert _lines(zhub, {"rx"}) == ["rz"]


def test_end_rejects_unknown_reason(zhub) -> None:
    _start(zhub)
    sid = zhub.zmodem_status("z")["session"]
    with pytest.raises(ValueError, match="reason"):
        zhub.zmodem_end("z", sid, None, reason="bogus")
    assert zhub.zmodem_status("z")["active"] is True


def test_done_end_resumes_immediately_without_drain(zhub) -> None:
    _start(zhub)
    sid, _ = zhub.zmodem_claim("z", "tab-1")
    zhub.zmodem_end("z", sid, "tab-1")
    assert zhub._require_worker("z")._zm_drain_started is None


def test_drain_discards_trailing_payload_then_resumes(zhub) -> None:
    _start(zhub)
    sid, _ = zhub.zmodem_claim("z", "tab-1")
    zhub.zmodem_end("z", sid, "tab-1", abort=True, reason="abort")
    for i in range(4):  # 每段间隔 < DRAIN_SILENCE_MS：都落在排空窗口内
        _device_sends(zhub, bytes([0x18, 0x41 + i, 0x0A, 0x8A, 0x0D, 0xFF]) * 20)
        time.sleep(0.1)
    assert _drain_over(zhub)
    assert _lines(zhub, {"rx"}) == ["rz"]  # 残余负载一行都没进环
    zhub.write_bytes("z", b"hello\n")
    assert _until(lambda: "hello" in _lines(zhub, {"rx"}))
    assert _lines(zhub, {"rx"}) == ["rz", "hello"]  # 行缓冲/解码器是干净的


def test_drain_cap_honoured_when_garbage_never_stops(zhub, monkeypatch) -> None:
    monkeypatch.setattr(zmodem, "DRAIN_MAX_MS", 600)
    _start(zhub)
    sid, _ = zhub.zmodem_claim("z", "tab-1")
    worker = zhub._require_worker("z")
    t0 = time.time()
    zhub.zmodem_end("z", sid, "tab-1", abort=True, reason="abort")
    while worker._zm_drain_started is not None and time.time() - t0 < 3.0:
        _device_sends(zhub, b"\x18\xff\x00garbage")  # 从不静默
        time.sleep(0.03)
    elapsed = time.time() - t0
    assert worker._zm_drain_started is None
    assert 0.5 <= elapsed < 1.5


def test_claim_landing_at_deadline_is_not_killed(hub_factory, make_config) -> None:
    """确定性复现“判定后、摘除前”的空隙：在 _zm_lock 第一次释放时插入认领。"""
    hub = hub_factory(make_config(ports=[_port(autostart=False)]))
    worker = hub._require_worker("z")  # 不打开：没有读循环线程干扰
    worker._zm_begin(Detection(direction="download", initial=ZRQINIT))
    worker._zm_session.started_ms -= 10_000  # 认领期限已过
    sub = hub.subscribe()

    outcome: dict = {}

    class HookLock:
        def __init__(self, inner):
            self.inner = inner
            self.armed = False

        def __enter__(self):
            self.inner.acquire()
            return self

        def __exit__(self, *exc):
            self.inner.release()
            if self.armed:
                self.armed = False
                try:
                    outcome["claim"] = worker.zmodem_claim("tab-1")
                except RuntimeError as err:
                    outcome["error"] = str(err)
            return False

    lock = HookLock(worker._zm_lock)
    worker._zm_lock = lock
    lock.armed = True
    worker._zm_check_deadlines()

    ends = [e for e in _zm_events(sub) if e["event"] == "end"]
    assert [e["reason"] for e in ends] == ["unclaimed"]
    # 认领要么干净地失败（会话已被摘除），绝不能“认领成功后又被当作 unclaimed 杀掉”
    assert "claim" not in outcome
    assert outcome["error"] == "zmodem: no active session"


def test_session_writes_not_starved_by_reader(zhub) -> None:
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    n = 20
    t0 = time.time()
    for _ in range(n):
        zhub.zmodem_write("z", sid, "tab-1", b"x" * 64)
    elapsed = time.time() - t0
    assert elapsed < n * 0.1 / 4, elapsed  # 旧实现每次要等读循环的 0.1 s 读超时


def test_bridge_does_not_ingest_tx_when_write_refused(zhub, free_tcp_port) -> None:
    bridge = TcpBridge(zhub, "z", mode="server", tcp_port=free_tcp_port())
    _start(zhub)
    bridge._forward_to_serial(b"ls\n")
    assert _lines(zhub, {"tx"}) == []
    assert "zmodem" in (bridge._last_error or "")


def test_read_error_ends_session_closed(zhub) -> None:
    sub = zhub.subscribe()
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    worker = zhub._require_worker("z")

    class Broken:
        @property
        def in_waiting(self):
            raise OSError("unplugged")

        def read(self, *_a):
            raise OSError("unplugged")

        def close(self):
            pass

    with worker._io_lock:
        real, worker._serial = worker._serial, Broken()
    real.close()
    assert _until(lambda: [e for e in _zm_events(sub) if e["event"] == "end"] or None) == [{
        "type": "zmodem", "event": "end", "port": "z", "session": sid,
        "reason": "closed", "rx_bytes": len(ZRQINIT), "tx_bytes": 0,
    }]
    assert _drain(q, timeout=1.0) == ZRQINIT


def test_unclaimed_timeout_aborts_and_resumes(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(zmodem_claim_ms=200)]))
    sub = hub.subscribe()
    written = _spy_writes(hub)
    _start(hub)
    assert _until(lambda: not hub.zmodem_status("z")["active"], timeout=2.0)
    ends = [e for e in _zm_events(sub) if e["event"] == "end"]
    assert [e["reason"] for e in ends] == ["unclaimed"]
    assert ABORT_SEQ in written
    assert _drain_over(hub)  # 回读的 ABORT_SEQ 被排空丢弃
    hub.write_bytes("z", b"hello\n")
    assert _until(lambda: "hello" in _lines(hub, {"rx"}))
    assert _lines(hub, {"rx"}) == ["rz", "hello"]


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


def test_flush_runs_outside_io_lock(zhub) -> None:
    """flush()=tcdrain 可能要等对端读走数据；若持 _io_lock 等，读循环读不了，
    设备（如正在发送的 lsz）写满缓冲后不再读输入 → tcdrain 永不返回（死锁）。"""
    worker = zhub._require_worker("z")
    _until(lambda: worker.is_open)
    ser = worker._serial
    held: list[bool] = []
    real_flush = ser.flush

    def spy_flush() -> None:
        held.append(worker._io_lock.locked())
        real_flush()

    ser.flush = spy_flush  # type: ignore[method-assign]
    _start(zhub)
    sid, _ = zhub.zmodem_claim("z", "tab-1")
    zhub.zmodem_write("z", sid, "tab-1", b"x")
    zhub.zmodem_end("z", sid, "tab-1", abort=True, reason="abort")
    assert held and not any(held)


def _rx_after_end_mark(hub) -> list[str]:
    lines = hub.read("z")["lines"]
    end = next(i for i, ln in enumerate(lines)
               if ln["kind"] == "mark" and ln["text"].startswith("[zmodem] end"))
    return [ln["text"] for ln in lines[end + 1:] if ln["kind"] == "rx"]


def test_done_end_reinjects_tail_then_queue_then_live(zhub) -> None:
    """done：客户端交回的 tail → 队列里未送出的字节 → 实时字节，依次作为 rx 行排在 end 标记之后。"""
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    assert q.get(timeout=POLL_TIMEOUT) == ZRQINIT
    _device_sends(zhub, b"queued\n")  # 进队列但没人取走
    assert _until(lambda: zhub.zmodem_status("z")["rx_bytes"] > len(ZRQINIT))
    _until(lambda: q.qsize() > 0)
    zhub.zmodem_end("z", sid, "tab-1", tail=b"sz done rc=0\r\nprom")  # 半行：与队列字节拼接
    assert _drain(q, timeout=0.5) == b""  # 已回注的字节不再经队列转发
    zhub.write_bytes("z", b"live\n")
    assert _until(lambda: _rx_after_end_mark(zhub) == ["sz done rc=0", "promqueued", "live"])
    assert _lines(zhub, {"rx"})[:1] == ["rz"]  # 会话前的行不受影响


def test_tail_ignored_on_abort(zhub) -> None:
    _start(zhub)
    sid, _ = zhub.zmodem_claim("z", "tab-1")
    zhub.zmodem_end("z", sid, "tab-1", abort=True, reason="abort", tail=b"tail-line\n")
    assert _drain_over(zhub)
    zhub.write_bytes("z", b"after\n")
    assert _until(lambda: "after" in _lines(zhub, {"rx"}))
    assert "tail-line" not in _lines(zhub, {"rx"})


def test_bytes_read_before_end_mark_wait_behind_reinjected(zhub, monkeypatch) -> None:
    """摘除会话到写入 end 标记之间读到的实时字节：不抢在 end 标记/回注字节之前成行。"""
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    assert q.get(timeout=POLL_TIMEOUT) == ZRQINIT
    real_mark = zhub.mark

    def slow_mark(port, label):
        if label.startswith("[zmodem] end"):
            _device_sends(zhub, b"during\n")
            time.sleep(0.3)  # 读循环此间已读到 during
        return real_mark(port, label)

    monkeypatch.setattr(zhub, "mark", slow_mark)
    zhub.zmodem_end("z", sid, "tab-1", tail=b"tail\n")
    assert _until(lambda: _rx_after_end_mark(zhub) == ["tail", "during"])


def test_done_end_of_unclaimed_session_drops_pending(zhub) -> None:
    """从未认领的会话：pending 里是没人看过的协议帧，done 结束也不当文本回注。"""
    _start(zhub)
    _device_sends(zhub, b"frame-bytes\n")
    assert _until(lambda: zhub.zmodem_status("z")["rx_bytes"] > len(ZRQINIT))
    sid = zhub.zmodem_status("z")["session"]
    zhub.zmodem_end("z", sid, None)
    zhub.write_bytes("z", b"live\n")
    assert _until(lambda: _rx_after_end_mark(zhub) == ["live"])


def test_reopen_after_closed_session_ingests_first_output(zhub, monkeypatch) -> None:
    """会话以 closed 结束（关口/拔线）后重连：新连接的第一段输出（开机横幅）照常成行，
    不被上一个连接残留的排空状态吞掉。"""
    monkeypatch.setattr(zmodem, "DRAIN_SILENCE_MS", 10_000)  # 若排空残留，横幅必被丢弃
    monkeypatch.setattr(zmodem, "DRAIN_MAX_MS", 20_000)
    _start(zhub)
    zhub.zmodem_claim("z", "tab-1")
    zhub.close("z")
    zhub.open("z")
    worker = zhub._require_worker("z")
    assert _until(lambda: worker.is_open)
    zhub.write_bytes("z", b"boot banner\n")
    assert _until(lambda: "boot banner" in _lines(zhub, {"rx"}))


@pytest.mark.parametrize("baud,limit", [(9600, 480), (115200, 5760)])
def test_session_write_split_into_wire_time_bounded_pieces(
    hub_factory, make_config, baud, limit
) -> None:
    """单次 ser.write 不超过约 0.5 s 线上时间（max(64, baud//20) 字节），按序拼回原数据。"""
    hub = hub_factory(make_config(ports=[_port(baud=baud)]))
    _start(hub)
    sid, _ = hub.zmodem_claim("z", "tab-1")
    worker = hub._require_worker("z")
    ser = worker._serial
    sizes: list[bytes] = []

    def spy_write(data):  # 只记录、不回读：loop:// 的 4 KiB 回读缓冲与本测试无关
        sizes.append(bytes(data))
        return len(data)

    ser.write = spy_write  # type: ignore[method-assign]
    data = bytes(range(256)) * 19 + bytes(136)  # 5000 字节
    assert len(data) == 5000
    assert hub.zmodem_write("z", sid, "tab-1", data) == 5000
    assert all(len(p) <= limit for p in sizes), [len(p) for p in sizes]
    assert b"".join(sizes) == data
    assert hub.zmodem_status("z")["tx_bytes"] == 5000


def test_queue_high_water_aborts_stalled_claimer(zhub, monkeypatch) -> None:
    """已认领但不取数据的客户端：队列里未送出的字节超过上限 → 以 abort 结束会话并写中止串。"""
    monkeypatch.setattr(zmodem, "QUEUE_MAX_BYTES", 1000)
    sub = zhub.subscribe()
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    written = _spy_writes(zhub)
    _device_sends(zhub, b"\x18" * 600)
    time.sleep(0.2)
    assert zhub.zmodem_status("z")["active"] is True  # 未超上限：继续
    _device_sends(zhub, b"\x18" * 600)
    ends = _until(lambda: [e for e in _zm_events(sub) if e["event"] == "end"] or None)
    assert [(e["session"], e["reason"]) for e in ends] == [(sid, "abort")]
    assert ABORT_SEQ in written


def test_queue_bytes_consumed_by_reader_do_not_count(zhub, monkeypatch) -> None:
    """取走的字节不再计入上限：持续消费的客户端可以收远超上限的数据。"""
    monkeypatch.setattr(zmodem, "QUEUE_MAX_BYTES", 1000)
    _start(zhub)
    sid, q = zhub.zmodem_claim("z", "tab-1")
    got = bytearray(q.get(timeout=POLL_TIMEOUT))
    for _ in range(5):
        _device_sends(zhub, b"\x18" * 600)
        got.extend(_drain(q, timeout=0.3))
    assert zhub.zmodem_status("z")["active"] is True
    assert q.queued_bytes == 0
    assert len(got) == len(ZRQINIT) + 3000


@pytest.mark.parametrize("value", ["cr", "lf", "crlf", "ctrl-u-cr", "none", "CRLF"])
def test_cleanup_eol_config(make_config, value) -> None:
    cfg = make_config(ports=[_port(zmodem_cleanup_eol=value)])
    assert cfg.port("z").zmodem_cleanup_eol == value.lower()
    assert cfg.to_dict()["ports"][0]["zmodem_cleanup_eol"] == value.lower()


def test_cleanup_eol_default_and_validation(make_config) -> None:
    assert make_config(ports=[_port()]).port("z").zmodem_cleanup_eol == "cr"
    with pytest.raises(ValueError, match=r"zmodem_cleanup_eol"):
        make_config(ports=[_port(zmodem_cleanup_eol="tab")])


def test_abort_seq_is_written_without_tcdrain(hub_factory, make_config) -> None:
    """The read loop writes ABORT_SEQ itself on a deadline (here: unclaimed); it must not wait
    in tcdrain for a peer that may itself be blocked writing to us (seen with socat + a chatty
    shell): a deadlock of the read loop."""
    hub = hub_factory(make_config(ports=[_port(zmodem_claim_ms=200)]))
    worker = hub._require_worker("z")
    ser = worker._serial
    flushes: list = []
    real_flush = ser.flush
    ser.flush = lambda: (flushes.append(threading.current_thread() is worker._loop_thread),
                         real_flush())[1]
    hub.write_bytes("z", b"rz\r" + ZRQINIT)
    assert _until(lambda: hub.zmodem_status("z")["active"])
    assert _until(lambda: not hub.zmodem_status("z")["active"], timeout=5)
    assert True not in flushes, flushes
