"""``POST /api/shutdown`` against a real uvicorn: the server thread / ``serialhub serve``
process actually exits, the TCP port and serial ports are released, WS clients are told,
and an active ZMODEM session is aborted (``ABORT_SEQ``) before the port closes.

``loop://`` ports only — no socat / lrzsz needed.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from websockets.sync.client import connect as ws_connect

from serialhub import cli
from serialhub.zmodem import ABORT_SEQ

from .simenv import ROOT, LiveServer, _free_port

ZRQINIT = b"**\x18B00000000000000\r\x8a\x11"
LOOP_PORTS = [{"name": "z", "device": "loop://", "autostart": True}]


def _until(pred, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return pred()


def _port_free(port: int) -> bool:
    """Nobody listens and a restarted serialhub could bind it (uvicorn sets SO_REUSEADDR,
    so TIME_WAIT leftovers of closed client connections do not count)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
        s.listen(1)
        return True
    except OSError:
        return False
    finally:
        s.close()


def _recv_type(ws, mtype: str, timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = json.loads(ws.recv(timeout=max(0.05, deadline - time.time())))
        if msg.get("type") == mtype:
            return msg
    raise AssertionError(f"no {mtype}")


def _assert_stopped(srv: LiveServer) -> None:
    thread = srv._thread
    assert thread is not None
    thread.join(5)
    assert not thread.is_alive(), "uvicorn thread did not exit within 5 s"
    assert _until(lambda: _port_free(srv.port), 2.0), "TCP port still bound"
    for name in srv.hub.list_ports()["ports"]:
        assert srv.hub.status(name["name"])["open"] is False


def test_live_shutdown_stops_server_and_notifies_ws() -> None:
    with LiveServer(LOOP_PORTS) as srv:
        assert _until(lambda: srv.hub.status("z")["open"])
        with ws_connect(srv.ws_url) as ws:
            _recv_type(ws, "hello")
            r = httpx.post(f"{srv.base_url}/api/shutdown", timeout=5)
            assert r.status_code == 200
            assert r.json() == {"ok": True, "data": {"stopping": True}}
            assert _recv_type(ws, "shutdown") == {"type": "shutdown"}
        _assert_stopped(srv)


def test_live_shutdown_aborts_claimed_zmodem_before_close() -> None:
    with LiveServer(LOOP_PORTS) as srv:
        hub = srv.hub
        assert _until(lambda: hub.status("z")["open"])
        worker = hub._require_worker("z")
        events: list[object] = []
        real_write, real_close = worker._write_raw, worker._close_serial
        worker._write_raw = lambda data: (events.append(bytes(data)), real_write(data))[1]
        worker._close_serial = lambda: (events.append("close"), real_close())[1]
        with ws_connect(srv.ws_url) as ws:
            _recv_type(ws, "hello")
            hub.write_bytes("z", b"rz\r" + ZRQINIT)
            assert _until(lambda: hub.zmodem_status("z")["active"])
            ws.send(json.dumps({"type": "zmodem_claim", "port": "z"}))
            _recv_type(ws, "zmodem_claimed")
            events.clear()
            r = httpx.post(f"{srv.base_url}/api/shutdown", timeout=5)
            assert r.status_code == 200
            _recv_type(ws, "shutdown")
            _assert_stopped(srv)
        assert ABORT_SEQ in events, events
        assert "close" in events
        assert events.index(ABORT_SEQ) < events.index("close")
        assert hub.zmodem_status("z")["active"] is False


def test_cli_shutdown_against_live_server(capsys) -> None:
    with LiveServer(LOOP_PORTS) as srv:
        code = cli.main(["shutdown", "--url", srv.base_url])
        out = capsys.readouterr().out.strip()
        assert code == 0
        assert json.loads(out) == {"ok": True, "data": {"stopping": True}}
        _assert_stopped(srv)


def test_cli_shutdown_failure_exits_1(capsys) -> None:
    port = _free_port()  # nobody listens there
    code = cli.main(["shutdown", "--url", f"http://127.0.0.1:{port}"])
    env = json.loads(capsys.readouterr().out.strip())
    assert code == 1 and env["ok"] is False and env["error"]


def test_serve_subprocess_exits_zero_after_shutdown(tmp_path: Path) -> None:
    port = _free_port()
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps({
        "server": {"host": "127.0.0.1", "port": port, "token": None},
        "ports": [{"name": "z", "device": "loop://", "autostart": True}],
    }))
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    proc = subprocess.Popen(
        [sys.executable, "-m", "serialhub", "serve", "--config", str(cfg),
         "--log-level", "warning"],
        cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        base = f"http://127.0.0.1:{port}"

        def _up() -> bool:
            try:
                return httpx.get(f"{base}/health", timeout=1).status_code == 200
            except httpx.HTTPError:
                return False

        assert _until(_up, 15.0), "serve did not come up"
        r = httpx.post(f"{base}/api/shutdown", timeout=5)
        assert r.status_code == 200 and r.json()["data"]["stopping"] is True
        try:
            code = proc.wait(10)
        except subprocess.TimeoutExpired:
            raise AssertionError("serve did not exit after /api/shutdown")
        out = proc.stdout.read().decode("utf-8", "replace") if proc.stdout else ""
        assert code == 0, out
        assert _port_free(port)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(5)
