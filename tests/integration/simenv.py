"""仿真环境：socat PTY 对 + mock 设备子进程 + 进程内 uvicorn 服务。"""

from __future__ import annotations

import importlib.util
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Optional

import pytest
import uvicorn

ROOT = Path(__file__).resolve().parents[2]
MOCK_DEVICE = ROOT / "tools" / "mock_device.py"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from serialhub.config import Config  # noqa: E402
from serialhub.hub import Hub  # noqa: E402
from serialhub.server import create_app  # noqa: E402

_ALIASES = {"lrz": ("lrz", "rz"), "rz": ("lrz", "rz"), "lsz": ("lsz", "sz"), "sz": ("lsz", "sz")}


def need_tools(*names: str) -> None:
    """缺少任一工具则 skip（lrz/lsz 接受 Homebrew 与 Debian 两种命名）。"""
    for n in names:
        if not any(shutil.which(c) for c in _ALIASES.get(n, (n,))):
            pytest.skip(f"missing tool: {n}")


def crc16_ccitt(data: bytes) -> int:
    """复用 mock_device 的 CRC 实现，保证测试与设备一致。"""
    spec = importlib.util.spec_from_file_location("mock_device", MOCK_DEVICE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.crc16_ccitt(data)


def _terminate(p: subprocess.Popen) -> None:
    if p.poll() is None:
        p.terminate()
        try:
            p.wait(3)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(3)


class PtyPair:
    """socat 虚拟串口对；可指定目录/链接名以便断开后在同一路径重建。"""

    _n = 0

    def __init__(self, directory: Optional[str] = None,
                 host_name: Optional[str] = None, dev_name: Optional[str] = None) -> None:
        PtyPair._n += 1
        self._own_dir = directory is None
        self.directory = directory or tempfile.mkdtemp(prefix="sim-")
        self.host_path = os.path.join(self.directory, host_name or f"host{PtyPair._n}")
        self.dev_path = os.path.join(self.directory, dev_name or f"dev{PtyPair._n}")
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> "PtyPair":
        need_tools("socat")
        for p in (self.host_path, self.dev_path):
            if os.path.lexists(p):
                os.unlink(p)
        self.proc = subprocess.Popen(
            ["socat", "-d", "-d",
             f"pty,raw,echo=0,link={self.host_path}",
             f"pty,raw,echo=0,link={self.dev_path}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 5
        while time.time() < deadline:
            if os.path.exists(self.host_path) and os.path.exists(self.dev_path):
                return self
            if self.proc.poll() is not None:
                break
            time.sleep(0.02)
        self.stop()
        raise RuntimeError("socat did not create PTY links")

    def kill(self) -> None:
        """杀掉 socat（模拟拔线）；链接文件随之消失/失效。"""
        if self.proc:
            _terminate(self.proc)

    def stop(self) -> None:
        self.kill()
        if self._own_dir:
            shutil.rmtree(self.directory, ignore_errors=True)

    def __enter__(self) -> "PtyPair":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()


class MockDevice:
    """以子进程运行 tools/mock_device.py。"""

    def __init__(self, dev_path: str, name: str, files_dir: str, recv_dir: str,
                 interval: float = 0.2) -> None:
        self.cmd = [sys.executable, str(MOCK_DEVICE), "--device", dev_path,
                    "--name", name, "--interval", str(interval),
                    "--files-dir", str(files_dir), "--recv-dir", str(recv_dir)]
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> "MockDevice":
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        return self

    def stop(self) -> None:
        if self.proc:
            _terminate(self.proc)

    def __enter__(self) -> "MockDevice":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()


def _free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class LiveServer:
    """在线程中运行 uvicorn(create_app)。ports 项如 {"name","device","autostart"}。"""

    def __init__(self, ports: list[dict[str, Any]]) -> None:
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.ws_url = f"ws://127.0.0.1:{self.port}/ws"
        raw = {
            "server": {"host": "127.0.0.1", "port": self.port, "token": None, "cors": True},
            "defaults": {"baud": 115200, "reconnect_ms": 200, "reconnect_max_ms": 500,
                         "flush_ms": 20},
            "ports": ports,
        }
        self.cfg = Config.from_dict(raw)
        self.hub = Hub(self.cfg)
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "LiveServer":
        self.hub.start()
        app = create_app(self.hub, self.cfg)
        conf = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self._server = uvicorn.Server(conf)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        deadline = time.time() + 10
        while not self._server.started and time.time() < deadline:
            time.sleep(0.02)
        if not self._server.started:
            self.stop()
            raise RuntimeError("uvicorn failed to start")
        return self

    def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(10)
        try:
            self.hub.stop()
        except Exception:  # noqa: BLE001
            pass
        self._server = self._thread = None

    def __enter__(self) -> "LiveServer":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()
