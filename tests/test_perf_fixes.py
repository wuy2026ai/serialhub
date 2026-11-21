"""Fix-wave tests (perf-latency review): boot id, echo-aware exchange, thread-pool
starvation, early close, vendor cache, residual rescan bound, idle-flush deadline.

Timing assertions keep the generous margins used by ``test_latency.py``.
"""

from __future__ import annotations

import os
import statistics
import threading
import time
import tty

import pytest

from serialhub.config import Config
from serialhub.hub import Hub


def _until(pred, timeout: float = 2.0, step: float = 0.001):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = pred()
        if value:
            return value
        time.sleep(step)
    return pred()


def _rx(hub: Hub, port: str = "t") -> list[str]:
    return [ln["text"] for ln in hub.read(port, kinds={"rx"})["lines"]]


# --------------------------------------------------------------------------- #
# D4: daemon identity (boot id) for the UI
# --------------------------------------------------------------------------- #
def test_health_and_ws_hello_expose_boot_id(client) -> None:
    first = client.get("/health").json()
    second = client.get("/health").json()
    boot = first.get("boot")
    assert isinstance(boot, str) and boot
    assert second["boot"] == boot  # constant for the process lifetime
    with client.websocket_connect("/ws") as ws:
        hello = ws.receive_json()
    assert hello["type"] == "hello"
    assert hello["boot"] == boot


def test_boot_id_differs_per_process() -> None:
    """A daemon restart is a new process: the boot id must change with it."""
    import subprocess
    import sys

    code = "from serialhub.server import BOOT_ID; print(BOOT_ID)"
    ids = {
        subprocess.run([sys.executable, "-c", code], capture_output=True,
                       text=True, check=True).stdout.strip()
        for _ in range(2)
    }
    assert len(ids) == 2 and all(ids)


# --------------------------------------------------------------------------- #
# D1: /exchange must not match a half-arrived echo of the sent command
# --------------------------------------------------------------------------- #
class _PtyDev:
    """Raw pty pair driven like a device: master is ours, slave is the port.

    Raw mode on the master => no echo: whatever we write is exactly what the
    daemon receives, and the daemon's writes appear at ``read_*`` only.
    """

    def __init__(self) -> None:
        self.master, slave = os.openpty()
        tty.setraw(self.master)
        self.path = os.ttyname(slave)
        self._slave = slave
        self._closed = False

    def write(self, data: bytes) -> None:
        if not self._closed:
            try:
                os.write(self.master, data)
            except OSError:  # 测试已收尾、fd 已关：迟到的设备脚本安静退出
                pass

    def read_until(self, needle: bytes, timeout: float = 2.0) -> bytes:
        end = time.monotonic() + timeout
        seen = bytearray()
        import select

        while time.monotonic() < end:
            left = end - time.monotonic()
            ready, _, _ = select.select([self.master], [], [], min(left, 0.05))
            if not ready:
                continue
            try:
                seen += os.read(self.master, 4096)
            except OSError:
                break
            if needle in seen:
                break
        return bytes(seen)

    def close(self) -> None:
        self._closed = True
        for fd in (self.master, self._slave):
            try:
                os.close(fd)
            except OSError:
                pass


@pytest.fixture
def pty_hub(hub_factory, make_config):
    """Factory: ``pty_hub(**port_extra) -> (hub, _PtyDev)`` on one raw pty port ``d``."""
    devs: list[_PtyDev] = []

    def _make(**extra) -> tuple[Hub, _PtyDev]:
        dev = _PtyDev()
        devs.append(dev)
        hub = hub_factory(make_config(ports=[
            {"name": "d", "device": dev.path, "autostart": True,
             "buffer_lines": 2000, "buffer_bytes": 1 << 22, **extra},
        ]))
        assert _until(lambda: hub.is_open("d"))
        return hub, dev

    yield _make
    for dev in devs:
        dev.close()


def _device_says(dev: _PtyDev, script) -> None:
    """Run ``script(dev)`` in a thread once the daemon's command line arrived."""

    def run() -> None:
        dev.read_until(b"\r")
        script(dev)

    threading.Thread(target=run, daemon=True).start()


def test_exchange_regex_waits_for_split_echo_reply(pty_hub) -> None:
    """Regex that occurs in the command text must not fire on the half echo."""
    hub, dev = pty_hub()
    reply_at = {}

    def script(d: _PtyDev) -> None:
        d.write(b"=> cat /proc/ver")
        time.sleep(0.003)
        d.write(b"sion\r\n")
        time.sleep(0.15)
        reply_at["t"] = time.monotonic()
        d.write(b"Linux version 6.1.0\r\n=> ")

    _device_says(dev, script)
    t0 = time.monotonic()
    res = hub.exchange("d", "cat /proc/version", wait_regex="proc|Linux", timeout_ms=2000)
    took = time.monotonic() - t0
    assert res["reason"] == "regex" and res["matched"] is True, res
    assert [ln["text"] for ln in res["lines"]] == [
        "=> cat /proc/version", "Linux version 6.1.0",
    ]
    assert took >= 0.1, took  # the half echo (≈4 ms in) must not end the exchange
    assert time.monotonic() - reply_at["t"] < 0.3


@pytest.mark.parametrize("cut", [1, 3, 7, 11, 17])
def test_exchange_echo_split_at_several_offsets(pty_hub, cut) -> None:
    """However the echo is sliced, only the real reply may match."""
    hub, dev = pty_hub()
    echo = b"cat /proc/version"
    reply_at = {}

    def script(d: _PtyDev) -> None:
        d.write(echo[:cut])
        time.sleep(0.003)
        d.write(echo[cut:] + b"\r\n")
        time.sleep(0.12)
        reply_at["t"] = time.monotonic()
        d.write(b"OK 42\r\n")

    _device_says(dev, script)
    t0 = time.monotonic()
    res = hub.exchange("d", "cat /proc/version", wait_regex="version|OK", timeout_ms=2000)
    took = time.monotonic() - t0
    assert res["reason"] == "regex", res
    assert [ln["text"] for ln in res["lines"]] == ["cat /proc/version", "OK 42"]
    assert took >= 0.08, took


def test_exchange_prompt_prefixed_echo_not_matched(pty_hub) -> None:
    hub, dev = pty_hub()

    def script(d: _PtyDev) -> None:
        d.write(b"root@box:~# ")
        time.sleep(0.003)
        d.write(b"cat /proc/version\r\n")
        time.sleep(0.12)
        d.write(b"Linux 6.1\r\nroot@box:~# ")

    _device_says(dev, script)
    res = hub.exchange("d", "cat /proc/version", wait_regex="cat|Linux", timeout_ms=2000)
    assert res["reason"] == "regex", res
    assert [ln["text"] for ln in res["lines"]] == [
        "root@box:~# cat /proc/version", "Linux 6.1",
    ]


def test_exchange_no_echo_device_matches_pending_immediately(pty_hub) -> None:
    """A device that does not echo still matches an unterminated reply at once."""
    hub, dev = pty_hub()

    def script(d: _PtyDev) -> None:
        d.write(b"U-Boot 2024")
        time.sleep(0.5)
        d.write(b"\r\n=> ")

    _device_says(dev, script)
    t0 = time.monotonic()
    res = hub.exchange("d", "version", wait_regex="U-Boot", timeout_ms=2000)
    took = time.monotonic() - t0
    assert res["reason"] == "regex" and res["pending"] == "U-Boot 2024", res
    assert took < 0.3, took
    assert res["lines"] == []


def test_exchange_prompt_deferred_while_echo_pending(pty_hub) -> None:
    """An unanchored prompt regex must not fire on the echo being typed back."""
    hub, dev = pty_hub(prompt="box#")

    def script(d: _PtyDev) -> None:
        d.write(b"box# cat /proc/version\r\n")
        time.sleep(0.12)
        d.write(b"done\r\nbox# ")

    _device_says(dev, script)
    t0 = time.monotonic()
    res = hub.exchange("d", "cat /proc/version", quiet_ms=5000, timeout_ms=2000)
    took = time.monotonic() - t0
    assert res["reason"] == "prompt" and res["pending"] == "box# ", res
    assert [ln["text"] for ln in res["lines"]] == ["box# cat /proc/version", "done"]
    assert took >= 0.1, took  # not the echo's own prompt prefix (~2 ms in)


# --------------------------------------------------------------------------- #
# D2: pending wait/send/exchange must not eat the thread pool
# --------------------------------------------------------------------------- #
@pytest.fixture
def live(free_tcp_port, make_config):
    """Started Hub + uvicorn on a free port; yields ``(hub, Client)`` (as test_latency)."""
    import uvicorn

    from serialhub.client import Client
    from serialhub.server import create_app

    cfg = make_config(ports=[
        {"name": "t", "device": "loop://", "autostart": True,
         "buffer_lines": 2000, "buffer_bytes": 1 << 22},
    ])
    hub = Hub(cfg)
    hub.start()
    port = free_tcp_port()
    server = uvicorn.Server(uvicorn.Config(create_app(hub, cfg), host="127.0.0.1",
                                           port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started and time.time() < deadline:
        time.sleep(0.02)
    try:
        assert server.started, "uvicorn failed to start"
        yield hub, Client(f"http://127.0.0.1:{port}", timeout=30)
    finally:
        server.should_exit = True
        thread.join(10)
        hub.stop()


def test_many_pending_waits_do_not_starve_other_clients(live) -> None:
    """60 pending 5 s waits (> the 40-thread pool): send+reply and /health stay fast."""
    from serialhub.client import Client

    hub, client = live
    n = 60

    def waiter() -> None:
        Client(client.base_url, timeout=30).wait("t", "NEVERX", timeout_ms=5000)

    ths = [threading.Thread(target=waiter, daemon=True) for _ in range(n)]
    for th in ths:
        th.start()
    try:
        time.sleep(0.8)  # let all waits check in
        client.read("t", limit=0)  # warm connection
        health, sends = [], []
        for _ in range(20):
            t0 = time.monotonic()
            assert client.call("GET", "/health")["ok"]
            health.append(time.monotonic() - t0)
            t0 = time.monotonic()
            env = client.send("t", "ping", wait_regex="ping")  # write + loopback echo
            sends.append(time.monotonic() - t0)
            assert env["ok"] and env["matched"], env
        # old sync-def endpoints: 60 pending waits ate the pool -> one send took 4.4 s
        assert statistics.median(sends) < 0.1, sends
        assert max(sends) < 0.5, sends
        assert max(health) < 0.1, health
    finally:
        for th in ths:
            th.join(6)


def test_rest_timeout_ms_clamped(live) -> None:
    """wait caps at 120 s, send/exchange at 60 s: clamped, not an error."""
    hub, client = live
    assert client.close("t")["ok"]
    t0 = time.monotonic()
    # a closed port ends the wait at once (D3); an unclamped request would hang
    env = client.wait("t", "NEVERX", timeout_ms=10_000_000)
    assert env["ok"] and not env["matched"], env
    assert time.monotonic() - t0 < 5
    assert client.exchange("t", "z", quiet_ms=999_999, timeout_ms=10_000_000)["ok"] is False
    assert client.open("t")["ok"]


class _StubRequest:
    """``server._await_or_disconnect`` 的断线探针：``disconnected`` 置位后返回 True。"""

    def __init__(self) -> None:
        self.disconnected = False
        self.probes = 0

    async def is_disconnected(self) -> bool:
        self.probes += 1
        return self.disconnected


def test_await_or_disconnect_cancels_wait() -> None:
    import asyncio

    from serialhub.server import _await_or_disconnect

    async def scenario() -> tuple[Any, bool, list]:
        request = _StubRequest()
        cancelled: list = []

        async def slow() -> str:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
            return "done"

        async def hang_up() -> None:
            await asyncio.sleep(0.3)
            request.disconnected = True

        result, _ = await asyncio.gather(
            _await_or_disconnect(request, slow()), hang_up())
        return result, request.probes > 0, list(cancelled)

    result, probed, cancelled = asyncio.run(scenario())
    assert result is None and probed  # client gone: no result to send
    assert cancelled == [True]  # the pending wait was actually cancelled


# --------------------------------------------------------------------------- #
# D3: wait/exchange end early when the port closes mid-request
# --------------------------------------------------------------------------- #
def test_exchange_returns_closed_when_port_closes_mid_request(live) -> None:
    from serialhub.client import Client

    hub, client = live
    out: dict = {}

    def run() -> None:
        try:
            c = Client(client.base_url, timeout=30)
            out["res"] = c.exchange("t", "z", wait_regex="NEVERX", timeout_ms=3000)
        except Exception as exc:  # noqa: BLE001 - 让断言看到线程里的失败
            out["exc"] = repr(exc)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    time.sleep(0.3)
    t0 = time.monotonic()
    assert client.close("t")["ok"]
    th.join(5)
    took = time.monotonic() - t0
    assert "exc" not in out, out.get("exc")
    res = out["res"]
    assert res["ok"] and res["reason"] == "closed" and res["matched"] is False, res
    assert took < 1.5, took  # not the full 3 s timeout
    # an exchange on an already-closed port keeps today's 409 error shape
    bad = Client(client.base_url, timeout=10).exchange("t", "z")
    assert not bad["ok"] and "not open" in bad["error"], bad
    assert client.open("t")["ok"]


def test_wait_ends_early_when_port_closes_mid_request(live) -> None:
    from serialhub.client import Client

    hub, client = live
    out: dict = {}

    def run() -> None:
        try:
            c = Client(client.base_url, timeout=30)
            out["res"] = c.wait("t", "NEVERX", timeout_ms=3000)
        except Exception as exc:  # noqa: BLE001 - 让断言看到线程里的失败
            out["exc"] = repr(exc)

    th = threading.Thread(target=run, daemon=True)
    th.start()
    time.sleep(0.3)
    t0 = time.monotonic()
    assert client.close("t")["ok"]
    th.join(5)
    assert time.monotonic() - t0 < 1.5
    assert "exc" not in out, out.get("exc")
    res = out["res"]
    assert res["ok"] and res["matched"] is False and res["line"] is None, res
    assert client.open("t")["ok"]


# --------------------------------------------------------------------------- #
# D5: our own file inside vendor/ must not get the third-party long cache
# --------------------------------------------------------------------------- #
def test_vendor_cache_rules(client) -> None:
    ours = client.get("/static/vendor/zmodem-shim.js")
    assert ours.status_code == 200
    assert ours.headers["cache-control"] == "no-cache"  # ours: revalidate every load
    theirs = client.get("/static/vendor/xterm.min.js")
    assert theirs.status_code == 200
    assert theirs.headers["cache-control"].startswith("public, max-age=")
    assert int(theirs.headers["cache-control"].split("max-age=")[1]) >= 86400
    # a same-named file outside vendor/ keeps no-cache
    assert client.get("/static/app.js").headers["cache-control"] == "no-cache"


# --------------------------------------------------------------------------- #
# D6: EOL scan resumes; unterminated residual is bounded
# --------------------------------------------------------------------------- #
class _SinkHub:
    """SerialPortWorker 的最小 hub 替身：只记 rx 文本与 pending。"""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.pending: list[str] = []

    def ingest(self, port, text, kind="rx", ts_ms=None):
        self.lines.append(text)

    def set_pending(self, port, text):
        self.pending.append(text)

    def mark(self, port, label):
        pass

    def _broadcast(self, event):
        pass


def _worker(**extra):
    from serialhub.config import PortConfig
    from serialhub.serialport import RESIDUAL_MAX_BYTES, SerialPortWorker

    sink = _SinkHub()
    opts = {"buffer_bytes": 1 << 22}
    opts.update(extra)
    pc = PortConfig(name="x", device="loop://", autostart=False, **opts)
    return SerialPortWorker(pc, sink), sink, RESIDUAL_MAX_BYTES


@pytest.mark.parametrize("chunks", [
    [b"abc\r\ndef\n"],
    [b"a", b"b", b"\r", b"\n", b"c\r", b"\n"],
    [b"line1\r", b"\nline2\r", b"\rline3\n", b"tail"],
    [b"\r", b"\n", b"x", b"\r", b"y", b"\n", b"\r"],
    [b"one\rtwo\nthree\r\n\r\nfour\n\n"],
])
def test_emit_complete_lines_chunk_boundaries(chunks) -> None:
    """EOL 续扫不改变断行规则：任意分片下与一次性喂入的结果一致。"""
    worker, sink, _ = _worker()
    buf = bytearray()
    for chunk in chunks:
        buf.extend(chunk)
        worker._emit_complete_lines(buf)
    expected = bytearray()
    worker2, sink2, _ = _worker()
    buf2 = bytearray()
    buf2.extend(b"".join(chunks))
    worker2._emit_complete_lines(buf2)
    assert sink.lines == sink2.lines
    # 结尾孤立 \r 仍暂缓（等待可能的 \n）
    if b"".join(chunks).endswith(b"\r"):
        assert not buf or buf.endswith(b"\r")


def test_unterminated_residual_scan_and_bound() -> None:
    """1 MiB 无换行：不重扫（时间有界），且按 64 KiB 切成自己的行。"""
    worker, sink, limit = _worker()
    buf = bytearray()
    t0 = time.monotonic()
    total = 1 << 20
    sent = 0
    k = 0
    while sent < total:
        k += 1
        chunk = (b"%08d" % k) * 32  # 256 B
        buf.extend(chunk)
        worker._emit_complete_lines(buf)
        sent += len(chunk)
    took = time.monotonic() - t0
    assert [len(ln) for ln in sink.lines] == [limit] * (total // limit)
    assert not buf  # 1 MiB 恰好切完
    assert worker._eol_scanned == 0
    assert took < 0.5, took  # old full rescan per read: ~0.5 s CPU for 1 MiB


def test_residual_bound_honours_smaller_buffer_bytes() -> None:
    worker, sink, _ = _worker(buffer_bytes=4096)
    buf = bytearray()
    buf.extend(b"z" * 10000)
    worker._emit_complete_lines(buf)
    assert [len(ln) for ln in sink.lines] == [4096, 4096]
    assert len(buf) == 10000 - 2 * 4096
    assert worker._eol_scanned == len(buf)


def test_residual_cut_feeds_real_port_lines(hub_factory, make_config) -> None:
    """端到端：无换行洪水按 64 KiB 成行入环，pending 仍有界。"""
    hub = hub_factory(make_config(ports=[
        {"name": "f", "device": "loop://", "autostart": True,
         "buffer_lines": 200, "buffer_bytes": 1 << 22},
    ]))
    for off in range(0, 70 * 1024, 4096):  # loop:// 缓冲有限：分片写让读循环跟得上
        hub.write_bytes("f", b"A" * 4096)
        time.sleep(0.004)
    assert _until(lambda: len(hub.read("f", kinds={"rx"})["lines"]) >= 1)
    texts = [ln["text"] for ln in hub.read("f", kinds={"rx"})["lines"]]
    assert len(texts) == 1 and len(texts[0]) == 64 * 1024
    assert len(hub.pending("f")) <= 240


# --------------------------------------------------------------------------- #
# D7: idle framing flushes at flush_ms, not at the read timeout
# --------------------------------------------------------------------------- #
def test_idle_flush_happens_at_flush_ms(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[
        {"name": "i", "device": "loop://", "autostart": True,
         "framing": "idle", "flush_ms": 50, "buffer_lines": 64,
         "buffer_bytes": 1 << 20},
    ]))
    time.sleep(0.15)  # let the reader settle into its idle wait
    lat = []
    for i in range(8):
        time.sleep(0.011 * (i % 4))  # land at different points of the read cycle
        t0 = time.monotonic()
        hub.write_bytes("i", f"p{i}".encode())  # loop:// reads it right back
        assert _until(lambda: f"p{i}" in _rx(hub, "i"))
        lat.append(time.monotonic() - t0)
    # old loop: the residual was only checked after the 0.1 s read timeout (p50 109 ms)
    assert statistics.median(lat) < 0.09, lat


def test_idle_flush_deadline_on_fd_port(pty_hub) -> None:
    """fd 路径（select）同样只睡到下一次 idle 吐出。"""
    hub, dev = pty_hub(framing="idle", flush_ms=40)
    time.sleep(0.15)
    lat = []
    for i in range(8):
        t0 = time.monotonic()
        dev.write(f"q{i}".encode())
        assert _until(lambda: f"q{i}" in _rx(hub, "d"))
        lat.append(time.monotonic() - t0)
        time.sleep(0.011 * (i % 3))
    assert statistics.median(lat) < 0.09, lat
