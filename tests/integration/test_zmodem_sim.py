"""ZMODEM 端到端：socat PTY + mock 设备（真实 lrzsz）+ serialhub /ws + Node zmodem.js 客户端。"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from contextlib import ExitStack, contextmanager
from typing import Any, Iterator

import httpx
import pytest
from websockets.sync.client import connect as ws_connect

from .simenv import LiveServer, MockDevice, PtyPair, need_tools, node_client

pytestmark = pytest.mark.sim

PKT_RE = re.compile(r"^PKT name=(\w+) seq=(\d+) ")
BLOB = 1 << 20


@pytest.fixture(autouse=True)
def _tools():
    need_tools("socat", "lrz", "lsz", "node")


def _blob(files_dir) -> bytes:
    data = bytearray(os.urandom(BLOB))
    data[500_000:500_006] = b"**\x18B00"  # 负载里藏一个假起始头：会话中不得再触发检测
    (files_dir / "blob.bin").write_bytes(bytes(data))
    return bytes(data)


@contextmanager
def _sim(tmp_path, names=("a",), interval: float = 0.2) -> Iterator[dict[str, Any]]:
    """起 PTY 对 + mock 设备 + 服务；返回 {srv, files, recv, dev: {name: MockDevice}}。"""
    files, recv = tmp_path / "files", tmp_path / "recv"
    files.mkdir()
    recv.mkdir()
    with ExitStack() as st:
        ports, devices = [], {}
        for n in names:
            pair = st.enter_context(PtyPair())
            devices[n] = st.enter_context(
                MockDevice(pair.dev_path, n, files, recv, interval=interval))
            ports.append({"name": n, "device": pair.host_path, "autostart": True})
        srv = st.enter_context(LiveServer(ports))
        time.sleep(0.5)
        yield {"srv": srv, "files": files, "recv": recv, "dev": devices}


class Observer:
    """旁观 /ws 连接：后台收集全部 JSON 事件。"""

    def __init__(self, url: str) -> None:
        self._conn = ws_connect(url, max_size=None)
        self.ws = self._conn.__enter__()
        self.events: list[dict] = []
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        self.wait(lambda e: e.get("type") == "hello")
        self.send({"type": "ping"})  # pong ⇒ 已订阅
        self.wait(lambda e: e.get("type") == "pong")

    def _run(self) -> None:
        try:
            for raw in self.ws:
                self.events.append(json.loads(raw))
        except Exception:  # noqa: BLE001 - 连接关闭
            pass

    def send(self, msg: dict) -> None:
        self.ws.send(json.dumps(msg))

    def wait(self, pred, timeout: float = 10.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for e in list(self.events):
                if pred(e):
                    return e
            time.sleep(0.02)
        raise AssertionError(f"event not seen; got {self.events[-10:]}")

    def close(self) -> None:
        self._conn.__exit__(None, None, None)
        self._t.join(3)


@contextmanager
def _observer(url: str) -> Iterator[Observer]:
    ob = Observer(url)
    try:
        yield ob
    finally:
        ob.close()


def _is_end(port: str):
    return lambda e: e.get("type") == "zmodem" and e.get("event") == "end" and e["port"] == port


def _send(srv, port: str, text: str) -> dict:
    return httpx.post(f"{srv.base_url}/api/ports/{port}/send", json={"data": text}, timeout=5).json()


def _wait_line(srv, port: str, regex: str, timeout: float) -> str:
    r = httpx.get(f"{srv.base_url}/api/ports/{port}/wait",
                  params={"regex": regex, "timeout_ms": int(timeout * 1000)},
                  timeout=timeout + 5).json()
    assert r["ok"] and r["matched"], f"no line matching {regex!r}"
    return r["line"]["text"]


def _command_until(srv, port: str, cmd: str, regex: str, timeout: float) -> None:
    """排空窗口内发出的命令可能被丢弃：反复发送直到看到响应。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = _send(srv, port, cmd)
        if r.get("ok"):
            got = httpx.get(f"{srv.base_url}/api/ports/{port}/wait",
                            params={"regex": regex, "timeout_ms": 700}, timeout=5).json()
            if got.get("matched"):
                return
        else:
            time.sleep(0.2)
    raise AssertionError(f"{cmd!r} never answered with {regex!r}")


def _lines(srv, port: str) -> list[dict]:
    r = httpx.get(f"{srv.base_url}/api/ports/{port}/read", timeout=5).json()
    assert r["ok"]
    return r["lines"]


def test_download_1mib_random(tmp_path):
    with _sim(tmp_path) as env:
        srv, data = env["srv"], _blob(env["files"])
        out = tmp_path / "out"
        out.mkdir()
        with node_client(srv.ws_url, "a", recv_dir=out) as zc:
            _send(srv, "a", "sz blob.bin")
            res = zc.result()
        assert res["ok"] and res["rc"] == 0, res
        assert res["files"] == [{"name": "blob.bin", "size": BLOB}]
        assert (out / "blob.bin").read_bytes() == data

        lines = _lines(srv, "a")
        marks = [ln for ln in lines if ln["kind"] == "mark"]
        start = next(m for m in marks if m["text"] == "[zmodem] start download")
        end = next(m for m in marks if m["text"].startswith("[zmodem] end reason=done"))
        inside = [ln for ln in lines
                  if ln["kind"] == "rx" and start["seq"] < ln["seq"] < end["seq"]]
        assert inside == []
        for ln in lines:  # 没有任何负载字节漏进环形缓冲
            assert "\x18" not in ln["text"] and "�" not in ln["text"], ln

        _send(srv, "a", "echo after")
        _wait_line(srv, "a", r"^after$", 2)


def test_upload_files(tmp_path):
    with _sim(tmp_path) as env:
        srv = env["srv"]
        src = tmp_path / "src"
        src.mkdir()
        blobs = {"big.bin": os.urandom(300 * 1024), "empty.bin": b"",
                 "测试 file.bin": os.urandom(4096)}
        for name, data in blobs.items():
            (src / name).write_bytes(data)
        paths = [src / n for n in blobs]
        with node_client(srv.ws_url, "a", send=paths) as zc:
            _send(srv, "a", "rz")
            res = zc.result()
        assert res["ok"] and res["rc"] == 0, res
        assert sorted(f["name"] for f in res["files"]) == sorted(blobs)
        _wait_line(srv, "a", r"^rz done rc=0$", 5)
        for name, data in blobs.items():
            assert (env["recv"] / name).read_bytes() == data, name


def test_owner_disconnect_aborts(tmp_path):
    with _sim(tmp_path) as env, _observer(env["srv"].ws_url) as ob:
        srv = env["srv"]
        _blob(env["files"])
        with node_client(srv.ws_url, "a", recv_dir=tmp_path,
                         close_after_bytes=65536) as zc:
            _send(srv, "a", "sz blob.bin")
            res = zc.result()
        assert not res["ok"] and "closed after" in res["error"], res
        assert ob.wait(_is_end("a"), timeout=5)["reason"] == "disconnect"
        # 设备这行回显多半落在排空窗口里被丢弃，故从设备 stdout 确认 lsz 的退出码
        dev = env["dev"]["a"]
        deadline = time.time() + 20
        while time.time() < deadline and not (m := re.search(r"sz done rc=(\d+)", dev.output())):
            time.sleep(0.1)
        assert m, "device sz never exited"
        assert int(m.group(1)) != 0
        _command_until(srv, "a", "echo ok", r"^ok$", 5)


def test_unclaimed_aborts(tmp_path):
    with _sim(tmp_path) as env, _observer(env["srv"].ws_url) as ob:
        srv = env["srv"]
        _blob(env["files"])
        t0 = time.time()
        _send(srv, "a", "sz blob.bin")
        assert ob.wait(_is_end("a"), timeout=7)["reason"] == "unclaimed"
        assert time.time() - t0 < 7
        _command_until(srv, "a", "ping", r"^pong$", 8)


def test_second_claim_rejected(tmp_path):
    with _sim(tmp_path) as env, _observer(env["srv"].ws_url) as c1, \
            _observer(env["srv"].ws_url) as c2:
        srv = env["srv"]
        _blob(env["files"])
        _send(srv, "a", "sz blob.bin")
        start = c1.wait(lambda e: e.get("type") == "zmodem" and e.get("event") == "start")
        c2.wait(lambda e: e.get("type") == "zmodem" and e.get("event") == "start")
        for c in (c1, c2):
            c.send({"type": "zmodem_claim", "port": "a"})
        replies = [c.wait(lambda e: e.get("type") in ("zmodem_claimed", "zmodem_error"))
                   for c in (c1, c2)]
        kinds = sorted(r["type"] for r in replies)
        assert kinds == ["zmodem_claimed", "zmodem_error"], replies
        owner = (c1, c2)[[r["type"] for r in replies].index("zmodem_claimed")]
        claimed = next(r for r in replies if r["type"] == "zmodem_claimed")
        assert claimed["session"] == start["session"] and claimed["direction"] == "download"
        owner.send({"type": "zmodem_end", "port": "a", "session": claimed["session"],
                    "abort": True})
        closed = owner.wait(lambda e: e.get("type") == "zmodem_closed")
        assert closed["reason"] == "abort"


def test_other_port_unaffected(tmp_path):
    with _sim(tmp_path, names=("a", "b"), interval=0.05) as env:
        srv = env["srv"]
        data = _blob(env["files"])

        def b_seqs() -> list[int]:
            return [int(m.group(2)) for ln in _lines(srv, "b")
                    if (m := PKT_RE.match(ln["text"])) and m.group(1) == "b"]

        before = len(b_seqs())
        out = tmp_path / "out"
        out.mkdir()
        with node_client(srv.ws_url, "a", recv_dir=out) as zc:
            _send(srv, "a", "sz blob.bin")
            res = zc.result()
        assert res["ok"], res
        assert (out / "blob.bin").read_bytes() == data
        seqs = b_seqs()
        assert len(seqs) > before
        assert seqs == list(range(seqs[0], seqs[0] + len(seqs))), "seq gap on port b"
