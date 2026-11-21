"""仿真基线：socat PTY + mock 设备 + 真实 serialhub 服务。"""

from __future__ import annotations

import os
import re
import time

import httpx
import pytest

from .simenv import LiveServer, MockDevice, PtyPair, crc16_ccitt, need_tools

pytestmark = pytest.mark.sim

PKT_RE = re.compile(r"^PKT name=(\w+) seq=(\d+) t=\d+ payload=([0-9a-f]{16}) crc=([0-9A-F]{4})$")


def _lines(base: str, port: str) -> list[str]:
    r = httpx.get(f"{base}/api/ports/{port}/read", timeout=5).json()
    assert r["ok"]
    return [ln["text"] for ln in r["lines"]]


def _port(name: str, pair: PtyPair) -> dict:
    return {"name": name, "device": pair.host_path, "autostart": True}


def test_packets_arrive(tmp_path):
    need_tools("socat")
    with PtyPair() as pair, MockDevice(pair.dev_path, "a", tmp_path, tmp_path), \
            LiveServer([_port("a", pair)]) as srv:
        time.sleep(3)
        pk = [m for ln in _lines(srv.base_url, "a") if (m := PKT_RE.match(ln))]
        assert len(pk) >= 3
        seqs = [int(m.group(2)) for m in pk]
        assert seqs == sorted(set(seqs))
        for m in pk:
            assert m.group(1) == "a"
            assert crc16_ccitt(bytes.fromhex(m.group(3))) == int(m.group(4), 16)


def test_send_roundtrip(tmp_path):
    need_tools("socat")
    with PtyPair() as pair, MockDevice(pair.dev_path, "a", tmp_path, tmp_path), \
            LiveServer([_port("a", pair)]) as srv:
        time.sleep(0.5)
        httpx.post(f"{srv.base_url}/api/ports/a/send", json={"data": "echo hello-42"}, timeout=5)
        r = httpx.get(f"{srv.base_url}/api/ports/a/wait",
                      params={"regex": r"^hello-42$", "timeout_ms": 2000}, timeout=5).json()
        assert r["ok"] and r.get("matched", True)
        assert "hello-42" in _lines(srv.base_url, "a")


def test_two_ports_independent(tmp_path):
    need_tools("socat")
    with PtyPair() as pa, PtyPair() as pb, \
            MockDevice(pa.dev_path, "a", tmp_path, tmp_path), \
            MockDevice(pb.dev_path, "b", tmp_path, tmp_path), \
            LiveServer([_port("a", pa), _port("b", pb)]) as srv:
        time.sleep(0.5)
        httpx.post(f"{srv.base_url}/api/ports/b/send", json={"data": "echo only-b"}, timeout=5)
        time.sleep(1.5)
        a, b = _lines(srv.base_url, "a"), _lines(srv.base_url, "b")
        assert any("name=a" in ln for ln in a) and any("name=b" in ln for ln in b)
        assert not any("name=b" in ln for ln in a)
        assert not any("name=a" in ln for ln in b)
        assert "only-b" in b and "only-b" not in a


def test_device_unplug_reconnect(tmp_path):
    need_tools("socat")
    pair = PtyPair().start()
    dev = MockDevice(pair.dev_path, "a", tmp_path, tmp_path).start()
    pair2 = dev2 = None
    try:
        with LiveServer([_port("a", pair)]) as srv:
            time.sleep(1)
            before = len([ln for ln in _lines(srv.base_url, "a") if ln.startswith("PKT")])
            assert before >= 1
            dev.stop()
            pair.kill()
            time.sleep(0.5)
            pair2 = PtyPair(pair.directory, os.path.basename(pair.host_path),
                            os.path.basename(pair.dev_path)).start()
            dev2 = MockDevice(pair2.dev_path, "a", tmp_path, tmp_path).start()
            deadline = time.time() + 10
            n = before
            while time.time() < deadline:
                n = len([ln for ln in _lines(srv.base_url, "a") if ln.startswith("PKT")])
                if n > before:
                    break
                time.sleep(0.3)
            assert n > before, "packets did not resume after reconnect"
    finally:
        for x in (dev2, dev, pair2, pair):
            if x:
                x.stop()


def _raw_open(path: str) -> int:
    import tty
    fd = os.open(path, os.O_RDWR | os.O_NOCTTY)
    tty.setraw(fd)
    return fd


def _read_until(fd: int, needle: bytes, timeout: float) -> bytes:
    import select
    buf, deadline = b"", time.time() + timeout
    while needle not in buf and time.time() < deadline:
        if select.select([fd], [], [], 0.2)[0]:
            buf += os.read(fd, 4096)
    return buf


def test_device_sz_to_host_rz_1mib(tmp_path):
    """设备 sz -> 宿主 lrz：1 MiB 足以填满 PTY 缓冲，验证子进程 fd 为阻塞模式。"""
    import subprocess
    need_tools("socat", "lrz", "lsz")
    import shutil
    rz = shutil.which("lrz") or shutil.which("rz")
    files, recv = tmp_path / "files", tmp_path / "recv"
    files.mkdir(); recv.mkdir()
    data = os.urandom(1 << 20)
    (files / "blob.bin").write_bytes(data)
    with PtyPair() as pair, MockDevice(pair.dev_path, "a", files, tmp_path, interval=0):
        time.sleep(0.5)
        fd = _raw_open(pair.host_path)
        try:
            os.write(fd, b"sz blob.bin\r")
            rc = subprocess.call([rz, "-y"], stdin=fd, stdout=fd,
                                 stderr=subprocess.DEVNULL, cwd=recv, timeout=60)
            assert rc == 0
            assert b"sz done rc=0" in _read_until(fd, b"sz done rc=0", 5)
        finally:
            os.close(fd)
    assert (recv / "blob.bin").read_bytes() == data


def test_host_sz_to_device_rz_1mib(tmp_path):
    """宿主 lsz -> 设备 rz：文件落入 recv-dir 且设备打印 rz done rc=0。"""
    import shutil
    import subprocess
    need_tools("socat", "lrz", "lsz")
    sz = shutil.which("lsz") or shutil.which("sz")
    recv = tmp_path / "recv"
    recv.mkdir()
    src = tmp_path / "up.bin"
    data = os.urandom(1 << 20)
    src.write_bytes(data)
    with PtyPair() as pair, MockDevice(pair.dev_path, "a", tmp_path, recv, interval=0):
        time.sleep(0.5)
        fd = _raw_open(pair.host_path)
        try:
            os.write(fd, b"rz\r")
            time.sleep(0.5)
            rc = subprocess.call([sz, str(src)], stdin=fd, stdout=fd,
                                 stderr=subprocess.DEVNULL, timeout=60)
            assert rc == 0
            assert b"rz done rc=0" in _read_until(fd, b"rz done rc=0", 5)
        finally:
            os.close(fd)
    assert (recv / "up.bin").read_bytes() == data
