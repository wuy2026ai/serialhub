"""Agent ZMODEM end to end: socat PTY + mock device (real lrzsz) + serialhub /ws + the
pure-Python engine (``serialhub.zmodem_client``) — no Node, no browser."""

from __future__ import annotations

import base64
import json
import os
import re
import time
import zlib
from contextlib import ExitStack, contextmanager
from typing import Any, Iterator

import pytest

from serialhub import cli, lite
from serialhub import zmodem_client as zc
from serialhub.client import Client

from .simenv import LiveServer, MockDevice, PtyPair, need_tools
from .test_zmodem_sim import (PKT_RE, _assert_clean_session, _command_until, _lines,
                              _observer, _send, _wait_line)

pytestmark = pytest.mark.sim

BLOB = 1 << 20
SENT_RE = re.compile(r"^sent (.+) (\d+)B crc=([0-9A-F]{8}) in [\d.]+s$")
RECV_RE = re.compile(r"^received (.+) (\d+)B crc=([0-9A-F]{8}) -> (.+)$")


@pytest.fixture(autouse=True)
def _tools():
    need_tools("socat", "lrz", "lsz")


@contextmanager
def _sim(tmp_path, names=("a",), interval: float = 0.2,
         same_dir: bool = False) -> Iterator[dict[str, Any]]:
    """PTY pair + mock device + server; ``same_dir``: rz saves where sz reads (round trips)."""
    files, recv = tmp_path / "files", tmp_path / ("files" if same_dir else "recv")
    files.mkdir(exist_ok=True)
    recv.mkdir(exist_ok=True)
    with ExitStack() as st:
        ports = []
        for n in names:
            pair = st.enter_context(PtyPair())
            st.enter_context(MockDevice(pair.dev_path, n, files, recv, interval=interval))
            ports.append({"name": n, "device": pair.host_path, "autostart": True})
        srv = st.enter_context(LiveServer(ports))
        for n in names:  # ready = the server has the port open and the device answers
            if interval:
                _wait_line(srv, n, r"^PKT name=", 5)
            else:
                _command_until(srv, n, "ping", r"^pong$", 5)
        yield {"srv": srv, "files": files, "recv": recv, "client": Client(srv.base_url)}


def _blob(path) -> bytes:
    data = bytearray(os.urandom(BLOB))
    data[500_000:500_006] = b"**\x18B00"  # a fake start header inside the payload
    path.write_bytes(bytes(data))
    return bytes(data)


def _parse(out: str, rx: re.Pattern) -> list[tuple]:
    got = [rx.match(line) for line in out.split("\n") if rx.match(line)]
    return [m.groups() for m in got]


def _no_payload_lines(srv, port: str) -> None:
    for ln in _lines(srv, port):
        assert "\x18" not in ln["text"] and "�" not in ln["text"], ln


# --------------------------------------------------------------------------- #
# upload (device rz)
# --------------------------------------------------------------------------- #
def test_zsend_files_via_lite_call(tmp_path):
    with _sim(tmp_path) as env:
        src = tmp_path / "src"
        src.mkdir()
        blobs = {"big.bin": os.urandom(BLOB), "empty.bin": b"", "测试 file.bin": os.urandom(4096)}
        for name, data in blobs.items():
            (src / name).write_bytes(data)
        out = lite.call(env["client"], "zsend",
                        {"port": "a", "path": [str(src / n) for n in blobs]})
        got = _parse(out, SENT_RE)
        assert [g[0] for g in got] == list(blobs), out
        for (_name, size, crc), data in zip(got, blobs.values()):
            assert int(size) == len(data) and crc == f"{zlib.crc32(data):08X}"
        for name, data in blobs.items():
            assert (env["recv"] / name).read_bytes() == data, name
        _assert_clean_session(env["srv"], "a", "upload", "rz done rc=0")
        _no_payload_lines(env["srv"], "a")


def test_zsend_inline_content_variants(tmp_path):
    with _sim(tmp_path) as env:
        cases = [
            ({"content": "hello agent\nline 2 — ünï\n", "name": "notes.txt"},
             "notes.txt", "hello agent\nline 2 — ünï\n".encode()),
            ({"content": base64.b64encode(bytes(range(256))).decode(), "name": "b.bin",
              "encoding": "base64"}, "b.bin", bytes(range(256))),
            ({"content": "", "name": "zero.txt"}, "zero.txt", b""),
            ({"content": "x=1\n", "name": "../etc/sub/conf.ini"}, "conf.ini", b"x=1\n"),
        ]
        for args, name, data in cases:
            out = lite.call(env["client"], "zsend", {"port": "a", **args})
            assert _parse(out, SENT_RE) == [(name, str(len(data)),
                                             f"{zlib.crc32(data):08X}")], out
            assert (env["recv"] / name).read_bytes() == data
        assert not (tmp_path / "etc").exists()


# --------------------------------------------------------------------------- #
# download (device sz)
# --------------------------------------------------------------------------- #
def test_zrecv_1mib_with_fake_header(tmp_path):
    with _sim(tmp_path) as env:
        srv = env["srv"]
        data = _blob(env["files"] / "blob.bin")
        out_dir = tmp_path / "out"
        t0 = time.monotonic()
        out = zc.zrecv(srv.base_url, None, "a", "blob.bin", str(out_dir))
        took = time.monotonic() - t0
        got = _parse(out, RECV_RE)
        assert got == [("blob.bin", str(BLOB), f"{zlib.crc32(data):08X}",
                        str(out_dir / "blob.bin"))], out
        assert (out_dir / "blob.bin").read_bytes() == data
        assert took < 30
        _assert_clean_session(srv, "a", "download", "sz done rc=0")
        _no_payload_lines(srv, "a")
        _send(srv, "a", "echo after")
        _wait_line(srv, "a", r"^after$", 2)


def test_zrecv_missing_remote_file(tmp_path):
    """lsz starts a session even for a missing file and sends no file: clear error + hint."""
    with _sim(tmp_path) as env:
        out = lite.call(env["client"], "zrecv",
                        {"port": "a", "remote": "nope.bin", "dir": str(tmp_path / "o")})
        assert out.startswith("error: device sent no file"), out
        assert "sz done rc=" in out
        assert os.listdir(tmp_path / "o") == []
        _command_until(env["srv"], "a", "ping", r"^pong$", 5)


def test_device_did_not_start_zmodem_hint(tmp_path, monkeypatch):
    monkeypatch.setattr(zc, "START_TIMEOUT", 1.5)
    with _sim(tmp_path, interval=0) as env:
        out = lite.call(env["client"], "zrecv", {
            "port": "a", "remote": "nope.bin", "dir": str(tmp_path / "o"),
            "cmd": "echo sz: cannot open nope.bin"})
        assert out == "error: device did not start ZMODEM; device said: sz: cannot open nope.bin"
        _command_until(env["srv"], "a", "ping", r"^pong$", 5)


def test_zrecv_rename_and_overwrite_via_cli(tmp_path, capsys):
    with _sim(tmp_path) as env:
        srv = env["srv"]
        (env["files"] / "f.txt").write_bytes(b"v1\n")
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        (out_dir / "f.txt").write_bytes(b"local\n")
        base = ["--url", srv.base_url]
        assert cli.main(["zrecv", "a", "f.txt", "--dir", str(out_dir), *base]) == 0
        assert capsys.readouterr().out.strip().endswith(f"-> {out_dir / 'f.txt.1'}")
        assert cli.main(["zrecv", "a", "f.txt", "--dir", str(out_dir), "--overwrite",
                         *base]) == 0
        assert capsys.readouterr().out.strip().endswith(f"-> {out_dir / 'f.txt'}")
        assert (out_dir / "f.txt").read_bytes() == b"v1\n"
        assert (out_dir / "f.txt.1").read_bytes() == b"v1\n"
        assert cli.main(["zrecv", "a", "missing.bin", "--dir", str(out_dir), *base]) == 1
        assert capsys.readouterr().out.startswith("error: ")


def test_cli_zsend_content_and_files(tmp_path, capsys, monkeypatch):
    import io
    import sys

    with _sim(tmp_path) as env:
        srv = env["srv"]
        f = tmp_path / "local.bin"
        f.write_bytes(os.urandom(3000))
        assert cli.main(["zsend", "a", str(f), "--url", srv.base_url]) == 0
        assert capsys.readouterr().out.startswith("sent local.bin 3000B")
        monkeypatch.setattr(sys, "stdin", io.StringIO("from stdin\n"))
        assert cli.main(["zsend", "a", "--content", "-", "--name", "in.txt",
                         "--url", srv.base_url]) == 0
        assert capsys.readouterr().out.startswith("sent in.txt 11B")
        assert (env["recv"] / "local.bin").read_bytes() == f.read_bytes()
        assert (env["recv"] / "in.txt").read_bytes() == b"from stdin\n"
        assert cli.main(["zsend", "a", "--url", srv.base_url]) == 1
        assert capsys.readouterr().out.startswith("error: ")


def test_agent_round_trip_inline(tmp_path):
    """The agent writes a file inline, pushes it, pulls it back and reads it."""
    with _sim(tmp_path, same_dir=True) as env:
        text = "# config written by the agent\nmode=fast\n" + "é" * 10 + "\n"
        out = lite.call(env["client"], "zsend",
                        {"port": "a", "content": text, "name": "agent.cfg"})
        assert out.startswith("sent agent.cfg "), out
        (env["files"] / "bin.dat").write_bytes(b"\x00\x01\x02" * 10)
        (env["files"] / "big.txt").write_text("y" * 6000)
        out = lite.call(env["client"], "zrecv", {
            "port": "a", "remote": ["agent.cfg", "bin.dat", "big.txt"],
            "dir": str(tmp_path / "pulled"), "inline": True})
        lines = out.split("\n")
        crc = f"{zlib.crc32(text.encode()):08X}"
        assert lines[0].startswith(f"received agent.cfg {len(text.encode())}B crc={crc} -> ")
        assert lines[1].endswith("(binary, not shown)")
        assert f"--- agent.cfg\n{text}\n--- big.txt\n" in out
        shown = 4000 - len(text)
        assert out.endswith("y" * shown + f"…+{6000 - shown} chars, full file at "
                            f"{tmp_path / 'pulled' / 'big.txt'}")


# --------------------------------------------------------------------------- #
# coexistence
# --------------------------------------------------------------------------- #
def test_second_claimer_gets_clear_error(tmp_path, monkeypatch):
    """A browser-style client claims first: zrecv reports it instead of hanging."""
    real_connect = zc._connect

    class _SlowClaim:
        def __init__(self, ws):
            self._ws = ws

        def send(self, raw):
            if '"zmodem_claim"' in raw:
                time.sleep(0.5)
            return self._ws.send(raw)

        def __getattr__(self, name):
            return getattr(self._ws, name)

    @contextmanager
    def slow_connect(*a):
        with real_connect(*a) as ws:
            yield _SlowClaim(ws)

    monkeypatch.setattr(zc, "_connect", slow_connect)
    with _sim(tmp_path) as env, _observer(env["srv"].ws_url) as browser:
        _blob(env["files"] / "blob.bin")
        result: dict = {}

        def browser_claims():
            start = browser.wait(lambda e: e.get("type") == "zmodem" and e.get("event") == "start")
            browser.send({"type": "zmodem_claim", "port": "a"})
            result["start"] = start

        import threading

        th = threading.Thread(target=browser_claims, daemon=True)
        th.start()
        out = zc.zrecv(env["srv"].base_url, None, "a", "blob.bin", str(tmp_path / "o"))
        th.join(5)
        assert out.startswith("error: ZMODEM session on 'a' was claimed by another client"), out
        claimed = browser.wait(lambda e: e.get("type") == "zmodem_claimed")
        browser.send({"type": "zmodem_end", "port": "a", "session": claimed["session"],
                      "abort": True})
        browser.wait(lambda e: e.get("type") == "zmodem_closed")
        _command_until(env["srv"], "a", "ping", r"^pong$", 8)


def test_session_active_send_is_409_text(tmp_path):
    with _sim(tmp_path) as env, _observer(env["srv"].ws_url) as browser:
        _blob(env["files"] / "blob.bin")
        _send(env["srv"], "a", "sz blob.bin")
        start = browser.wait(lambda e: e.get("type") == "zmodem" and e.get("event") == "start")
        out = lite.call(env["client"], "zsend", {"port": "a", "content": "x", "name": "x"})
        assert out.startswith("error: zmodem: session active on port 'a'"), out
        browser.send({"type": "zmodem_claim", "port": "a"})
        browser.wait(lambda e: e.get("type") == "zmodem_claimed")
        browser.send({"type": "zmodem_end", "port": "a", "session": start["session"],
                      "abort": True})


def test_wrong_direction_is_clear_error(tmp_path):
    with _sim(tmp_path) as env:
        _blob(env["files"] / "blob.bin")
        out = lite.call(env["client"], "zsend",
                        {"port": "a", "content": "x", "name": "x", "cmd": "sz blob.bin"})
        assert out.startswith("error: device started a download (device runs sz), "
                              "expected a upload"), out
        _command_until(env["srv"], "a", "ping", r"^pong$", 10)


def test_other_port_unaffected(tmp_path):
    with _sim(tmp_path, names=("a", "b"), interval=0.05) as env:
        srv = env["srv"]
        data = _blob(env["files"] / "blob.bin")

        def b_seqs() -> list[int]:
            return [int(m.group(2)) for ln in _lines(srv, "b")
                    if (m := PKT_RE.match(ln["text"])) and m.group(1) == "b"]

        before = len(b_seqs())
        out = zc.zrecv(srv.base_url, None, "a", "blob.bin", str(tmp_path / "o"))
        assert out.startswith("received blob.bin"), out
        assert (tmp_path / "o" / "blob.bin").read_bytes() == data
        src = tmp_path / "up.bin"
        src.write_bytes(data)
        assert zc.zsend(srv.base_url, None, "a", [str(src)]).startswith("sent up.bin")
        assert (env["recv"] / "up.bin").read_bytes() == data
        seqs = b_seqs()
        assert len(seqs) > before
        assert seqs == list(range(seqs[0], seqs[0] + len(seqs))), "seq gap on port b"


def test_throughput_report(tmp_path, capsys):
    """Not an assertion on speed: prints sim throughput both ways for the record."""
    with _sim(tmp_path, interval=0) as env:
        srv = env["srv"]
        _blob(env["files"] / "blob.bin")
        src = tmp_path / "up.bin"
        src.write_bytes(os.urandom(BLOB))
        t0 = time.monotonic()
        assert zc.zsend(srv.base_url, None, "a", [str(src)]).startswith("sent")
        up = time.monotonic() - t0
        t0 = time.monotonic()
        assert zc.zrecv(srv.base_url, None, "a", "blob.bin", str(tmp_path / "o")).startswith(
            "received")
        down = time.monotonic() - t0
        with capsys.disabled():
            print(json.dumps({"upload_kib_s": round(1024 / up), "download_kib_s":
                              round(1024 / down), "upload_s": round(up, 2),
                              "download_s": round(down, 2)}))


def test_full_mcp_profile_tools(tmp_path):
    pytest.importorskip("mcp")
    import asyncio

    from serialhub import mcp_server

    saved = (mcp_server._BASE_URL, mcp_server._TOKEN)
    crc = f"{zlib.crc32(b'hi' + bytes([10])):08X}"
    with _sim(tmp_path, same_dir=True) as env:
        mcp_server.configure(env["srv"].base_url, None)
        try:
            sent = asyncio.run(mcp_server.zmodem_send("a", content="hi\n", name="m.txt"))
            assert sent["ok"] and sent["files"][0]["crc32"] == crc
            got = asyncio.run(mcp_server.zmodem_receive("a", ["m.txt"], dir=str(tmp_path / "o"),
                                                        inline=True))
            assert got["ok"] and got["files"][0]["path"] == str(tmp_path / "o" / "m.txt")
            assert got["text"].endswith("--- m.txt\nhi\n")
            bad = asyncio.run(mcp_server.zmodem_send("a"))
            assert bad == {"ok": False, "error": "give exactly one of paths or content"}
        finally:
            mcp_server.configure(*saved)
