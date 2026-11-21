"""``socket://`` 设备的读路径测试：对端关闭前的最后一段数据不能丢。

pyserial 的 socket ``read()`` 在同一次调用里累积字节，遇到 EOF 抛异常并丢掉已累积的部分；
worker 因此对 socket 端口自己 select + recv。用本机临时 TCP 服务端验证。
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from tests.test_zmodem_session import ZRQINIT, _drain, _until

TIMEOUT = 8.0


class Server:
    """单连接脚本化 TCP 服务端：每次 accept 调用一个 handler(conn)。"""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(4)
        self.port = self.sock.getsockname()[1]
        self.conns: list[socket.socket] = []

    def serve(self, handler) -> threading.Thread:
        def run() -> None:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self.conns.append(conn)
            try:
                handler(conn)
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

        th = threading.Thread(target=run, daemon=True)
        th.start()
        return th

    def close(self) -> None:
        for c in self.conns:
            try:
                c.close()
            except OSError:
                pass
        self.sock.close()


@pytest.fixture
def server():
    s = Server()
    yield s
    s.close()


def _hub(hub_factory, make_config, port: int):
    return hub_factory(make_config(ports=[{
        "name": "s", "device": f"socket://127.0.0.1:{port}", "autostart": True,
        "buffer_lines": 5000, "buffer_bytes": 1 << 22,
        "reconnect_ms": 100, "reconnect_max_ms": 200, "flush_ms": 20,
    }]))


def _texts(hub) -> list[str]:
    return [ln["text"] for ln in hub.read("s", kinds={"rx"})["lines"]]


def _numbered(n: int, start: int = 0) -> list[str]:
    return [f"line {i:05d} " + "x" * 40 for i in range(start, start + n)]


def _wait_lines(hub, count: int) -> list[str]:
    _until(lambda: len(_texts(hub)) >= count, timeout=TIMEOUT)
    return _texts(hub)


def test_burst_then_close_keeps_every_line(server, hub_factory, make_config) -> None:
    lines = _numbered(120)  # ~6 KB
    payload = ("\r\n".join(lines) + "\r\n").encode()
    assert len(payload) > 5000
    server.serve(lambda c: c.sendall(payload))
    hub = _hub(hub_factory, make_config, server.port)
    assert _wait_lines(hub, len(lines)) == lines


def test_large_multi_write_then_close(server, hub_factory, make_config) -> None:
    lines = _numbered(2000)  # ~100 KB
    payload = ("\r\n".join(lines) + "\r\n").encode()

    def handler(c) -> None:
        for i in range(0, len(payload), 17000):
            c.sendall(payload[i:i + 17000])

    server.serve(handler)
    hub = _hub(hub_factory, make_config, server.port)
    assert _wait_lines(hub, len(lines)) == lines


def test_partial_last_line_flushed_on_close(server, hub_factory, make_config) -> None:
    lines = _numbered(100)
    payload = ("\r\n".join(lines) + "\r\n").encode() + b"partial tail"
    server.serve(lambda c: c.sendall(payload))
    hub = _hub(hub_factory, make_config, server.port)
    assert _wait_lines(hub, len(lines) + 1) == lines + ["partial tail"]


def test_reconnect_after_close_keeps_old_buffer(server, hub_factory, make_config) -> None:
    first = _numbered(120)
    second = _numbered(30, start=1000)
    server.serve(lambda c: c.sendall(("\r\n".join(first) + "\r\n").encode()))
    hub = _hub(hub_factory, make_config, server.port)
    assert _wait_lines(hub, len(first)) == first
    server.serve(lambda c: (c.sendall(("\r\n".join(second) + "\r\n").encode()), time.sleep(0.3)))
    assert _wait_lines(hub, len(first) + len(second)) == first + second


def test_zmodem_session_read_on_socket(server, hub_factory, make_config) -> None:
    release = threading.Event()
    body = bytes(range(256)) * 40  # 10 KB 二进制负载

    def handler(c) -> None:
        c.sendall(b"rz\r" + ZRQINIT)
        release.wait(TIMEOUT)
        c.sendall(body)
        time.sleep(0.3)

    server.serve(handler)
    hub = _hub(hub_factory, make_config, server.port)
    assert _until(lambda: hub.zmodem_status("s")["active"], timeout=TIMEOUT)
    sid, q = hub.zmodem_claim("s", "tab-1")
    release.set()
    got = bytearray()
    deadline = time.time() + TIMEOUT
    expected = ZRQINIT + body
    while time.time() < deadline and len(got) < len(expected):
        got.extend(_drain(q, timeout=0.3))
    assert bytes(got) == expected
