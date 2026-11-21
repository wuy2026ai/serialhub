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
