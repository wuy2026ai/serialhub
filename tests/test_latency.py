"""Low-latency paths (ARCHITECTURE §16): read loop, waits, pending tail, /exchange.

Timing assertions use generous margins (targets are ~1-5 ms; asserts are tens of ms)
so they do not flake on a loaded machine. Ports are ``loop://``, a real POSIX pty
(pyserial ``Serial`` with an ``fd``) or fakes; no hardware.
"""

from __future__ import annotations

import os
import queue
import statistics
import sys
import threading
import time

import pytest

from serialhub.config import Config, PortConfig
from serialhub.hub import Hub
from serialhub.serialport import SerialPortWorker


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


def _one_port_hub(hub_factory, make_config, **extra) -> Hub:
    port = {"name": "t", "device": "loop://", "autostart": True,
            "buffer_lines": 2000, "buffer_bytes": 1 << 22, **extra}
    return hub_factory(make_config(ports=[port]))


# --------------------------------------------------------------------------- #
# 1. serial read loop
# --------------------------------------------------------------------------- #
def test_loop_rx_line_delivered_promptly(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    time.sleep(0.15)  # let the reader settle into its idle wait
    lat = []
    for i in range(10):
        time.sleep(0.013 * (i % 4))  # land at different points of the read cycle
        t0 = time.monotonic()
        hub.write_bytes("t", f"probe{i}\n".encode())
        assert _until(lambda: f"probe{i}" in _rx(hub))
        lat.append(time.monotonic() - t0)
    # old loop: read(4096) waited for the full 0.1 s timeout -> ~100 ms each
    assert statistics.median(lat) < 0.03, lat


def test_write_not_delayed_by_idle_reader(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    time.sleep(0.15)
    durations = []
    for i in range(10):
        time.sleep(0.011 * (i % 5))
        t0 = time.monotonic()
        hub.write_bytes("t", b"x")
        durations.append(time.monotonic() - t0)
    # old loop held _io_lock across the 0.1 s read -> writes waited ~50 ms on average
    assert statistics.median(durations) < 0.02, durations
    assert max(durations) < 0.06, durations


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX pty")
def test_posix_pty_rx_and_write_fast(hub_factory, make_config) -> None:
    import tty

    master, slave = os.openpty()
    tty.setraw(master)
    name = os.ttyname(slave)
    stop = threading.Event()

    def drain() -> None:  # tcdrain() on the slave waits until the master side reads
        import select as _select
        while not stop.is_set():
            if _select.select([master], [], [], 0.05)[0]:
                try:
                    os.read(master, 4096)
                except OSError:
                    return

    threading.Thread(target=drain, daemon=True).start()
    try:
        hub = _one_port_hub(hub_factory, make_config, device=name)
        assert _until(lambda: hub.is_open("t"))
        worker = hub._require_worker("t")
        assert isinstance(getattr(worker._serial, "fd", None), int)
        time.sleep(0.15)
        lat, wr = [], []
        for i in range(10):
            time.sleep(0.013 * (i % 4))
            t0 = time.monotonic()
            os.write(master, f"pty{i}\r\n".encode())
            assert _until(lambda: f"pty{i}" in _rx(hub))
            lat.append(time.monotonic() - t0)
            t0 = time.monotonic()
            hub.write_bytes("t", b"w")
            wr.append(time.monotonic() - t0)
        assert statistics.median(lat) < 0.03, lat
        assert statistics.median(wr) < 0.02, wr
        # final chunk of a burst (< 4096 bytes) is not held for the read timeout
        burst = b"".join(f"burst line {i:05d}\r\n".encode() for i in range(300))
        t0 = time.monotonic()
        os.write(master, burst)
        assert _until(lambda: "burst line 00299" in _rx(hub))
        assert time.monotonic() - t0 < 0.05
    finally:
        hub.stop()
        stop.set()
        time.sleep(0.1)
        os.close(master)
        os.close(slave)


class _NoFdSerial:
    """Windows-style pyserial object: no ``fd``, no ``_socket``; read blocks up to timeout."""

    def __init__(self, io_lock_probe=None) -> None:
        self.timeout = 0.1
        self.is_open = True
        self._q: "queue.Queue[int]" = queue.Queue()
        self.read_sizes: list[int] = []
        self.lock_free_during_block: list[bool] = []
        self.probe = io_lock_probe
        self.written = bytearray()

    def feed(self, data: bytes) -> None:
        for b in data:
            self._q.put(b)

    @property
    def in_waiting(self) -> int:
        return self._q.qsize()

    def read(self, size: int = 1) -> bytes:
        self.read_sizes.append(size)
        out = bytearray()
        end = time.monotonic() + self.timeout
        while len(out) < size:
            left = end - time.monotonic()
            if left <= 0:
                break
            if self.probe is not None and size == 1 and not out:
                self.lock_free_during_block.append(self.probe())
            try:
                out.append(self._q.get(timeout=left))
            except queue.Empty:
                break
        return bytes(out)

    def write(self, data: bytes) -> int:
        self.written.extend(data)
        return len(data)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.is_open = False


class _StubHub:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.pending: list[str] = []

    def ingest(self, port, text, kind="rx", ts_ms=None):
        self.lines.append(text)

    def _broadcast(self, event):
        pass

    def mark(self, port, label):
        pass

    def set_pending(self, port, text):
        self.pending.append(text)


def test_windows_style_no_fd_reads_outside_lock(monkeypatch) -> None:
    holder: dict = {}

    def probe() -> bool:
        lock = holder["worker"]._io_lock
        free = lock.acquire(blocking=False)
        if free:
            lock.release()
        return free

    fake = _NoFdSerial(io_lock_probe=probe)
    monkeypatch.setattr("serial.serial_for_url", lambda *a, **kw: fake)
    pc = PortConfig(name="w", device="COM9", autostart=False, reconnect_ms=50)
    worker = SerialPortWorker(pc, _StubHub())
    holder["worker"] = worker
    worker.open()
    try:
        time.sleep(0.15)
        t0 = time.monotonic()
        fake.feed(b"hello\r\nwor")
        assert _until(lambda: worker._hub.lines == ["hello"])
        assert time.monotonic() - t0 < 0.05
        fake.feed(b"ld\n")
        assert _until(lambda: worker._hub.lines == ["hello", "world"])
        # idle wait is a 1-byte read outside _io_lock; the rest is drained via in_waiting
        assert 1 in fake.read_sizes and 4096 not in fake.read_sizes
        assert fake.lock_free_during_block and all(fake.lock_free_during_block)
        t0 = time.monotonic()
        worker.write(b"cmd\r")
        assert time.monotonic() - t0 < 0.02
        assert bytes(fake.written) == b"cmd\r"
    finally:
        t0 = time.monotonic()
        worker.close()
        assert time.monotonic() - t0 < 1.0


# --------------------------------------------------------------------------- #
# 2. waits never block the event loop
# --------------------------------------------------------------------------- #
@pytest.fixture
def live(free_tcp_port, make_config):
    """Started Hub + uvicorn on a free port; yields ``(hub, Client)``."""
    import uvicorn

    from serialhub.client import Client
    from serialhub.server import create_app

    cfg = make_config(ports=[
        {"name": "t", "device": "loop://", "autostart": True,
         "buffer_lines": 2000, "buffer_bytes": 1 << 22},
        {"name": "p", "device": "loop://", "autostart": True, "prompt": r"=> $",
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
        yield hub, Client(f"http://127.0.0.1:{port}", timeout=10)
    finally:
        server.should_exit = True
        thread.join(10)
        hub.stop()


@pytest.mark.parametrize("blocker", ["wait", "send"])
def test_pending_wait_does_not_delay_other_clients(live, blocker) -> None:
    hub, client = live
    from serialhub.client import Client

    other = Client(client.base_url, timeout=10)
    client.read("t", limit=0)  # warm up
    if blocker == "wait":
        target = lambda: client.wait("t", "never-matches", timeout_ms=1500)  # noqa: E731
    else:
        target = lambda: client.send("t", "x", wait_regex="never-matches",  # noqa: E731
                                     timeout_ms=1500)
    th = threading.Thread(target=target, daemon=True)
    th.start()
    time.sleep(0.2)
    lat = []
    for _ in range(5):
        t0 = time.monotonic()
        assert other.read("t", limit=1)["ok"]
        lat.append(time.monotonic() - t0)
    th.join(5)
    # old async-def handler ran hub.wait_for on the event loop: every request stalled ~1.3 s
    assert max(lat) < 0.2, lat


def test_wait_returns_promptly_after_match(live) -> None:
    hub, client = live
    since = int(client.read("t", limit=0)["next_seq"])
    stamp: dict = {}

    def later() -> None:
        time.sleep(0.2)
        stamp["t"] = time.monotonic()
        hub.ingest("t", "READY 1")

    threading.Thread(target=later, daemon=True).start()
    env = client.wait("t", "READY", timeout_ms=3000, since_seq=since)
    done = time.monotonic()
    assert env["matched"] and env["line"]["text"] == "READY 1"
    assert done - stamp["t"] < 0.05


# --------------------------------------------------------------------------- #
# 5. config keys
# --------------------------------------------------------------------------- #
def test_config_reply_keys_defaults_and_validation() -> None:
    cfg = Config.from_dict({"ports": [{"name": "a", "device": "loop://"}]})
    pc = cfg.port("a")
    assert (pc.prompt, pc.reply_quiet_ms, pc.reply_first_ms) == (None, 100, 300)
    out = cfg.to_dict()["ports"][0]
    assert (out["prompt"], out["reply_quiet_ms"], out["reply_first_ms"]) == (None, 100, 300)
    cfg = Config.from_dict({"ports": [{"name": "a", "device": "loop://", "prompt": "=> $",
                                       "reply_quiet_ms": "40", "reply_first_ms": 0}]})
    assert (cfg.port("a").prompt, cfg.port("a").reply_quiet_ms,
            cfg.port("a").reply_first_ms) == ("=> $", 40, 0)
    for bad, field in (({"prompt": "("}, "prompt"), ({"prompt": 5}, "prompt"),
                       ({"reply_quiet_ms": -1}, "reply_quiet_ms"),
                       ({"reply_first_ms": "x"}, "reply_first_ms")):
        with pytest.raises(ValueError, match=field):
            Config.from_dict({"ports": [{"name": "a", "device": "loop://", **bad}]})


# --------------------------------------------------------------------------- #
# 3. pending (unterminated tail)
# --------------------------------------------------------------------------- #
def test_pending_tail_appears_and_clears(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    sub = hub.subscribe()
    hub.write_bytes("t", b"boot ok\r\n\x1b[1m=> \x1b[0m")
    assert _until(lambda: hub.pending("t") == "=> ")
    assert _rx(hub) == ["boot ok"]  # no seq for the tail
    assert hub.read("t")["pending"] == "=> "
    assert hub.status("t")["pending"] == "=> "
    assert hub.list_ports()["ports"][0]["pending"] == "=> "
    partials = []
    while not sub.empty():
        ev = sub.get_nowait()
        if ev.get("type") == "partial":
            partials.append(ev)
    assert partials and partials[-1] == {"type": "partial", "port": "t", "text": "=> "}
    assert all("seq" not in ev for ev in partials)
    hub.write_bytes("t", b"help\r\n")
    assert _until(lambda: "\x1b[1m=> \x1b[0mhelp" in _rx(hub))
    assert _until(lambda: hub.pending("t") == "")
    assert "pending" not in hub.read("t")
    assert "pending" not in hub.status("t")
    ev = None
    while not sub.empty():
        e = sub.get_nowait()
        if e.get("type") == "partial":
            ev = e
    assert ev == {"type": "partial", "port": "t", "text": ""}


def test_pending_tail_capped_and_cleared_on_close(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    hub.write_bytes("t", b"x" * 300 + b"END")
    assert _until(lambda: hub.pending("t").endswith("END"))
    assert len(hub.pending("t")) == 240
    hub.close("t")
    assert hub.pending("t") == ""
    assert _rx(hub)[-1].endswith("END")  # close still flushes the residual as a line


def test_pending_tail_multibyte_split_not_garbled(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    data = "密码: ".encode()
    hub.write_bytes("t", data[:2])
    time.sleep(0.05)
    assert "�" not in hub.pending("t")
    hub.write_bytes("t", data[2:])
    assert _until(lambda: hub.pending("t") == "密码: ")
    hub.write_bytes("t", b"\n")
    assert _until(lambda: _rx(hub) == ["密码: "])


def test_wait_for_matches_pending_tail(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    since = hub.read("t", limit=0)["next_seq"]
    threading.Timer(0.1, hub.write_bytes, ("t", b"login: ")).start()
    res = hub.wait_for("t", r"login: $", timeout_ms=2000, since_seq=since)
    assert res["matched"] is True and res["line"] is None and res["seq"] is None
    assert res["pending"] == "login: "
    # already pending at call time: immediate
    t0 = time.monotonic()
    assert hub.wait_for("t", "login", timeout_ms=2000, since_seq=since)["matched"]
    assert time.monotonic() - t0 < 0.05


# --------------------------------------------------------------------------- #
# 4. exchange
# --------------------------------------------------------------------------- #
def _reply(hub: Hub, port: str, data: bytes, delay: float) -> None:
    threading.Timer(delay, hub.write_bytes, (port, data)).start()


def test_exchange_regex_skips_echo(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    _reply(hub, "t", b"pong\r\n", 0.05)
    res = hub.exchange("t", "pong", wait_regex="pong", timeout_ms=2000)
    assert res["reason"] == "regex" and res["matched"] is True
    assert [ln["text"] for ln in res["lines"]] == ["pong", "pong"]  # echo + reply
    assert res["sent_bytes"] == 6
    assert res["next_seq"] == res["lines"][-1]["seq"]
    assert all(ln["kind"] == "rx" for ln in res["lines"])


def test_exchange_quiet_from_last_byte(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config, reply_quiet_ms=150)
    _reply(hub, "t", b"partial ", 0.1)   # bytes but no line: still resets the quiet clock
    _reply(hub, "t", b"done\r\n", 0.2)
    t0 = time.monotonic()
    res = hub.exchange("t", "cmd")
    took = time.monotonic() - t0
    assert res["reason"] == "quiet" and res["matched"] is False
    assert [ln["text"] for ln in res["lines"]] == ["cmd", "partial done"]
    assert 0.33 < took < 0.6, took


def test_exchange_quiet_default_and_first_byte(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config, eol="none")
    hub.write_bytes("t", b"\n")
    _until(lambda: _rx(hub))
    # loop:// echoes immediately -> quiet ends ~100 ms after the echo
    t0 = time.monotonic()
    res = hub.exchange("t", "x\n")
    took = time.monotonic() - t0
    assert res["reason"] == "quiet" and 0.08 < took < 0.3, took
    # nothing at all arrives (eol none, empty data) -> wait reply_first_ms (300) then quiet
    t0 = time.monotonic()
    res = hub.exchange("t", "", timeout_ms=5000)
    took = time.monotonic() - t0
    assert res["reason"] == "quiet" and res["lines"] == [] and 0.28 < took < 0.6, took
    t0 = time.monotonic()
    res = hub.exchange("t", "", timeout_ms=100)  # min(timeout_ms, reply_first_ms)
    assert res["reason"] == "quiet" and time.monotonic() - t0 < 0.3


def test_exchange_prompt_and_timeout(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config, prompt=r"=> $", reply_quiet_ms=1000)
    _reply(hub, "t", b"ver 1.2\r\n=> ", 0.05)
    t0 = time.monotonic()
    res = hub.exchange("t", "version")
    took = time.monotonic() - t0
    assert res["reason"] == "prompt" and res["pending"] == "=> ", res
    assert [ln["text"] for ln in res["lines"]] == ["version", "ver 1.2"]
    assert took < 0.3, took
    res = hub.exchange("t", "foo", wait_regex="never", timeout_ms=200)
    assert res["reason"] == "timeout" and res["matched"] is False


def test_exchange_regex_matches_pending(hub_factory, make_config) -> None:
    hub = _one_port_hub(hub_factory, make_config)
    _reply(hub, "t", b"Password: ", 0.05)
    res = hub.exchange("t", "su", wait_regex=r"Password:", timeout_ms=2000)
    assert res["reason"] == "regex" and res["pending"] == "Password: "


def test_exchange_rest_endpoint(live) -> None:
    hub, client = live
    _reply(hub, "p", b"ok\r\n=> ", 0.05)
    env = client.exchange("p", "status")
    assert env["ok"] and env["reason"] == "prompt" and env["pending"] == "=> "
    assert [ln["text"] for ln in env["lines"]] == ["status", "ok"]
    assert env["sent_bytes"] == 8
    bad = client.exchange("p", "x", wait_regex="(")
    assert not bad["ok"]
    assert not client.exchange("nope", "x")["ok"]


# --------------------------------------------------------------------------- #
# lite: one request per send, `~` pending line, 404 fallback
# --------------------------------------------------------------------------- #
def _count_calls(monkeypatch):
    from serialhub.client import Client

    calls: list[str] = []
    orig = Client.call

    def call(self, method, path, **kw):
        calls.append(f"{method} {path.rsplit('/', 1)[-1]}")
        return orig(self, method, path, **kw)

    monkeypatch.setattr(Client, "call", call)
    return calls


def test_lite_send_is_one_request(live, monkeypatch) -> None:
    from serialhub import lite

    hub, client = live
    calls = _count_calls(monkeypatch)
    _reply(hub, "t", b"pong\r\n", 0.03)
    t0 = time.monotonic()
    out = lite.call(client, "send", {"port": "t", "data": "ping", "wait": "pong"})
    assert time.monotonic() - t0 < 0.5
    assert out.split("\n")[0] == "sent 6B matched"
    assert calls == ["POST exchange"]
    calls.clear()
    t0 = time.monotonic()
    out = lite.call(client, "send", {"port": "t", "data": "ping"})
    took = time.monotonic() - t0
    assert calls == ["POST exchange"] and took < 0.35, took
    assert out.split("\n")[2].endswith(" ping")


def test_lite_pending_tilde_line(live) -> None:
    from serialhub import lite

    hub, client = live
    _reply(hub, "p", b"U-Boot 2024\r\n=> ", 0.03)
    out = lite.call(client, "send", {"port": "p", "data": "version"})
    assert out.split("\n")[-1] == "~=> "
    out = lite.call(client, "read", {"port": "p"})
    assert out.split("\n")[-1] == "~=> "
    assert lite.call(client, "wait", {"port": "p", "regex": "=> $",
                                      "timeout_ms": 1000}).split("\n")[-1] == "~=> "
    hub.write_bytes("p", b"\r\n")
    assert _until(lambda: hub.pending("p") == "")
    assert "~" not in lite.call(client, "read", {"port": "p"})


def test_lite_wait_matches_pending_prompt(live) -> None:
    from serialhub import lite

    hub, client = live
    _reply(hub, "t", b"login: ", 0.1)
    out = lite.call(client, "wait", {"port": "t", "regex": "login:", "timeout_ms": 2000})
    lines = out.split("\n")
    assert lines[0] == "matched" and lines[-1] == "~login: "


def test_lite_send_falls_back_on_old_daemon(live, monkeypatch) -> None:
    from serialhub import lite
    from serialhub.client import Client

    hub, client = live
    orig = Client.call

    def old_daemon(self, method, path, **kw):
        if path.endswith("/exchange"):
            return {"ok": False, "detail": "Not Found", "error": "HTTP 404: Not Found"}
        return orig(self, method, path, **kw)

    monkeypatch.setattr(Client, "call", old_daemon)
    _reply(hub, "t", b"pong\r\n", 0.03)
    out = lite.call(client, "send", {"port": "t", "data": "ping", "wait": "pong"})
    assert out.split("\n")[0] == "sent 6B matched"
    assert lite.call(client, "send", {"port": "nope", "data": "x"}).startswith("error:")


# --------------------------------------------------------------------------- #
# B. WebSocket pump: one wake-up per batch, partial events coalesced
# --------------------------------------------------------------------------- #
def test_ws_burst_keeps_up_with_three_clients(live) -> None:
    import json as _json

    from websockets.sync.client import connect

    hub, client = live
    url = client.base_url.replace("http", "ws") + "/ws"
    import contextlib

    with contextlib.ExitStack() as stack:
        socks = [stack.enter_context(connect(url)) for _ in range(3)]
        for ws in socks:
            ws.recv(timeout=5)  # hello
        n = 2000
        t0 = time.monotonic()
        for i in range(n):
            hub.ingest("t", f"burst {i:05d} the quick brown fox jumps over the lazy dog")
        ingest_done = time.monotonic()
        finish = []
        for ws in socks:
            count = 0
            while True:
                ev = _json.loads(ws.recv(timeout=10))
                if ev.get("type") == "line":
                    count += 1
                    if ev["line"]["text"].startswith(f"burst {n - 1:05d}"):
                        break
            finish.append(time.monotonic())
            assert count == n
        # old pump: one asyncio.to_thread(q.get) hop per event -> ~0.1-0.3 s behind
        assert max(finish) - ingest_done < 0.25, (ingest_done - t0, [f - ingest_done for f in finish])


def test_ws_partial_events_coalesced(live) -> None:
    import json as _json

    from websockets.sync.client import connect

    hub, client = live
    url = client.base_url.replace("http", "ws") + "/ws"
    with connect(url) as ws:
        ws.recv(timeout=5)
        time.sleep(0.1)
        for i in range(200):
            hub.set_pending("t", f"typing {i}")
        hub.ingest("t", "sentinel")
        partials = []
        while True:
            ev = _json.loads(ws.recv(timeout=5))
            if ev.get("type") == "partial":
                partials.append(ev["text"])
            if ev.get("type") == "line" and ev["line"]["text"] == "sentinel":
                break
        assert partials and partials[-1] == "typing 199"
        assert len(partials) < 200
        hub.set_pending("t", "")


# --------------------------------------------------------------------------- #
# 6. static/HTTP: gzip, Cache-Control, access log flag
# --------------------------------------------------------------------------- #
def test_static_gzip_and_cache_control(client) -> None:
    gz = {"Accept-Encoding": "gzip"}
    r = client.get("/static/app.js", headers=gz)
    assert r.status_code == 200 and r.headers.get("content-encoding") == "gzip"
    assert r.headers["cache-control"] == "no-cache"
    r = client.get("/", headers=gz)
    assert r.headers.get("content-encoding") == "gzip"
    assert r.headers["cache-control"] == "no-cache"
    r = client.get("/static/vendor/xterm.min.js", headers=gz)
    assert r.status_code == 200 and r.headers.get("content-encoding") == "gzip"
    assert r.headers["cache-control"].startswith("public, max-age=")
    assert int(r.headers["cache-control"].split("max-age=")[1]) >= 86400
    # revalidation still works (ETag -> 304)
    etag = client.get("/static/app.js").headers["etag"]
    assert client.get("/static/app.js", headers={"If-None-Match": etag}).status_code == 304
    # small JSON is not compressed, API responses carry no static cache header
    r = client.get("/health", headers=gz)
    assert "content-encoding" not in r.headers and "cache-control" not in r.headers


def test_large_json_gzipped(client, hub) -> None:
    for i in range(60):
        hub.ingest("t", f"line {i} " + "x" * 40)
    r = client.get("/api/ports/t/read", headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("content-encoding") == "gzip"
    assert r.json()["ok"] is True


def test_serve_access_log_flag(monkeypatch, tmp_path) -> None:
    import json as _json

    from serialhub import cli

    cfg = tmp_path / "c.json"
    cfg.write_text(_json.dumps({"ports": [{"name": "a", "device": "loop://",
                                           "autostart": False}]}))
    seen: list = []

    class FakeServer:
        def __init__(self, config) -> None:
            seen.append(config)
            self.should_exit = False

        def run(self) -> None:
            pass

    import uvicorn

    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    assert cli.main(["serve", "--config", str(cfg)]) in (0, None)
    assert cli.main(["serve", "--config", str(cfg), "--access-log"]) in (0, None)
    assert [c.access_log for c in seen] == [False, True]


def test_i18n_access_log_help_in_both_catalogs() -> None:
    import json as _json
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent / "serialhub" / "i18n"
    for lang in ("en", "zh"):
        cat = _json.loads((root / f"{lang}.json").read_text(encoding="utf-8"))
        assert cat.get("cli.arg.access_log")


# --------------------------------------------------------------------------- #
# D. RingBuffer.read does not copy the whole ring for a cursor near the tail
# --------------------------------------------------------------------------- #
def test_ringbuffer_cursor_read_scans_only_tail() -> None:
    from serialhub.models import Line
    from serialhub.ringbuffer import RingBuffer

    rb = RingBuffer(max_lines=20000, max_bytes=1 << 30)
    for seq in range(1, 20001):
        rb.append(Line(seq=seq * 2, port="p", ts_ms=seq, text=f"l{seq}"))
    out = rb.read(since_seq=39990)
    assert [ln["seq"] for ln in out["lines"]] == [39992, 39994, 39996, 39998, 40000]
    assert out["first_seq"] == 2 and out["last_seq"] == 40000 and out["next_seq"] == 40000
    assert rb.read(since_seq=40000)["lines"] == []
    assert rb.read(since_seq=0, limit=2)["has_more"] is True
    assert rb.read(limit=3)["lines"][0]["seq"] == 39996
    # a slightly out-of-order append (tx ingested by another thread) is not lost
    rb.append(Line(seq=40003, port="p", ts_ms=1, text="a"))
    rb.append(Line(seq=40002, port="p", ts_ms=1, text="b"))
    assert [ln["seq"] for ln in rb.read(since_seq=40000)["lines"]] == [40003, 40002]
    t0 = time.perf_counter()
    for _ in range(200):
        rb.read(since_seq=39990)
    per_call = (time.perf_counter() - t0) / 200
    assert per_call < 0.0002, per_call  # whole-ring copy: ~0.5 ms


def test_emit_complete_lines_matches_find_eol_reference() -> None:
    import random

    from serialhub.serialport import _find_eol

    def reference(raw: bytes):
        b, out = bytearray(raw), []
        while (idx := _find_eol(b)) is not None:
            out.append(bytes(b[:idx[0]]))
            del b[:idx[1]]
        return out, bytes(b)

    rng = random.Random(7)
    worker = SerialPortWorker(PortConfig(name="x", device="loop://"), _StubHub())
    for _ in range(3000):
        raw = bytes(rng.choice(b"ab\r\n") for _ in range(rng.randint(0, 14)))
        got: list = []
        worker._emit = got.append  # type: ignore[method-assign]
        buf = bytearray(raw)
        worker._emit_complete_lines(buf)
        assert (got, bytes(buf)) == reference(raw), raw


# --------------------------------------------------------------------------- #
# /ws: a write racing the end of its session gets zmodem_closed before the error
# --------------------------------------------------------------------------- #
def test_ws_closed_precedes_late_write_error(hub_factory, make_config, monkeypatch) -> None:
    import asyncio
    import base64

    from fastapi.testclient import TestClient

    from serialhub.server import create_app

    cfg = make_config(ports=[{"name": "z", "device": "loop://", "autostart": True,
                              "reconnect_ms": 200, "reconnect_max_ms": 500}])
    hub = hub_factory(cfg)
    orig = asyncio.to_thread

    async def slow_sentinel(func, *a, **kw):  # the session pump notices the end late
        result = await orig(func, *a, **kw)
        if result is None and getattr(func, "__name__", "") == "get":
            await asyncio.sleep(0.3)
        return result

    monkeypatch.setattr(asyncio, "to_thread", slow_sentinel)
    with TestClient(create_app(hub, cfg)) as tc, tc.websocket_connect("/ws") as ws:
        assert _until(lambda: hub.status("z")["open"])
        hub.write_bytes("z", b"rz\r**\x18B00000000000000\r\x8a\x11")
        assert _until(lambda: hub.zmodem_status("z")["active"])
        ws.send_json({"type": "zmodem_claim", "port": "z"})
        sid = None
        while sid is None:
            msg = ws.receive_json()
            if msg.get("type") == "zmodem_claimed":
                sid = msg["session"]
        hub.close("z")  # server ends the session ("closed")
        ws.send_json({"type": "zmodem_write", "port": "z", "session": sid,
                      "b64": base64.b64encode(b"late").decode()})
        order = []
        while len(order) < 2:
            msg = ws.receive_json()
            if msg.get("type") in ("zmodem_closed", "zmodem_error"):
                order.append(msg["type"])
        assert order == ["zmodem_closed", "zmodem_error"]
