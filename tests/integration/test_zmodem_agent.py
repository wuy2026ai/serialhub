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
from serialhub import zmodem_proto as zp
from serialhub.client import Client

import httpx

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
def _sim(tmp_path, names=("a",), interval: float = 0.2, same_dir: bool = False,
         extra: list[str] | None = None, port_cfg: dict | None = None
         ) -> Iterator[dict[str, Any]]:
    """PTY pair + mock device + server; ``same_dir``: rz saves where sz reads (round trips)."""
    files, recv = tmp_path / "files", tmp_path / ("files" if same_dir else "recv")
    files.mkdir(exist_ok=True)
    recv.mkdir(exist_ok=True)
    with ExitStack() as st:
        ports, pairs, devs = [], [], []
        for n in names:
            pair = st.enter_context(PtyPair())
            pairs.append(pair)
            devs.append(st.enter_context(MockDevice(pair.dev_path, n, files, recv,
                                                    interval=interval, extra=extra)))
            ports.append({"name": n, "device": pair.host_path, "autostart": True,
                          **(port_cfg or {})})
        srv = st.enter_context(LiveServer(ports))
        # teardown: cut the line (kill socat) before the server closes its ports, so a device
        # still chewing on bytes can never leave the daemon waiting in tcdrain on socat
        st.callback(lambda: [p.kill() for p in pairs])
        for n in names:  # ready = the server has the port open and the device answers
            if interval:
                _wait_line(srv, n, r"^PKT name=", 5)
            else:
                _command_until(srv, n, "ping", r"^pong$", 5)
        yield {"srv": srv, "files": files, "recv": recv, "client": Client(srv.base_url),
               "devs": devs}


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


def _shell_clean(srv, port: str = "a") -> None:
    """After a session: ``ping`` sent EXACTLY ONCE gets ``pong`` as the first reply (PKT lines
    aside), and no line in the ring shows the device chewing on protocol bytes."""
    deadline = time.time() + 20
    while True:  # the session (and any drain) must be over first; a 409 writes nothing
        seq = httpx.get(f"{srv.base_url}/api/ports/{port}/read",
                        params={"limit": 0}, timeout=20).json()["next_seq"]
        r = httpx.post(f"{srv.base_url}/api/ports/{port}/send", json={"data": "ping"},
                       timeout=20).json()
        if r.get("ok") or "session active" not in str(r.get("error")) or time.time() > deadline:
            break
        time.sleep(0.2)
    assert r.get("ok"), r
    end = time.time() + 3
    first = None
    while time.time() < end and first is None:
        for ln in httpx.get(f"{srv.base_url}/api/ports/{port}/read",
                            params={"since_seq": seq, "kinds": "rx"}).json()["lines"]:
            if not PKT_RE.match(ln["text"]):
                first = ln["text"]
                break
        time.sleep(0.05)
    assert first == "pong", first
    bad = [ln["text"] for ln in _lines(srv, port) if ln["text"].startswith("ERR unknown")]
    assert bad == [], bad


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
        _shell_clean(env["srv"])


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
        _shell_clean(env["srv"])


def test_zsend_existing_file_overwrite_and_refusal(tmp_path, capsys):
    """``rz -v`` = lrz without -y: it refuses existing files unless the sender asks ZMCLOB."""
    with _sim(tmp_path) as env:
        (env["recv"] / "cfg.ini").write_bytes(b"old\n")
        out = lite.call(env["client"], "zsend", {"port": "a", "content": "new\n",
                                                 "name": "cfg.ini", "cmd": "rz -v"})
        assert out.startswith("sent cfg.ini 4B"), out
        assert (env["recv"] / "cfg.ini").read_bytes() == b"new\n"
        _shell_clean(env["srv"])
        out = lite.call(env["client"], "zsend", {"port": "a", "content": "newer\n",
                                                 "name": "cfg.ini", "cmd": "rz -v",
                                                 "overwrite": False})
        assert out.split("\n") == [
            "error: nothing was sent",
            'skipped cfg.ini: device refused it (already exists? try cmd "rz -y")'], out
        assert (env["recv"] / "cfg.ini").read_bytes() == b"new\n"
        _shell_clean(env["srv"])
        f = tmp_path / "cfg.ini"
        f.write_bytes(b"cli\n")
        assert cli.main(["zsend", "a", str(f), "--cmd", "rz -v", "--no-overwrite",
                         "--url", env["srv"].base_url]) == 1
        assert capsys.readouterr().out.startswith("error: nothing was sent")
        _shell_clean(env["srv"])


def test_device_cancels_mid_upload_partial_results_and_clean_shell(tmp_path):
    """lrz is killed (SIGTERM → CAN) mid-transfer: the small first file is reported, then the
    error with the device's own exit line; in-flight bytes never reach the next command."""
    # baud 9600 → pacing window 4 KiB: what is in flight when rz dies is what the shell eats
    with _sim(tmp_path, interval=0, extra=["--rz-kill-after", "1.0"],
              port_cfg={"baud": 9600}) as env:
        small, big = tmp_path / "small.txt", tmp_path / "big.bin"
        small.write_bytes(b"tiny\n")
        big.write_bytes(os.urandom(64 << 20))
        out = zc.zsend(env["srv"].base_url, None, "a", [str(small), str(big)])
        lines = out.split("\n")
        assert lines[0].startswith("sent small.txt 5B crc="), out
        # the sim (socat + a mock that echoes every junk line) digests in-flight bytes slowly,
        # so how much device text is captured varies; the classification and a clean shell don't
        assert re.match(r"^error: device (cancelled the transfer|ended the transfer|stopped "
                        r"accepting data at \d+/\d+ bytes)", lines[-1]), out
        assert "ERR" not in lines[-1], out
        assert len(lines) == 2
        # the shell still chews on what was in flight when rz died (bounded by the 4 KiB window
        # at 9600): let it finish — nothing is sent meanwhile — then ping exactly once
        _wait_quiet(env["srv"], "a")
        _shell_clean(env["srv"])
        # the mock discards what reaches the killed rz (as a UART would swallow it) but counts
        # it: at most the sender's window for 9600 baud plus one subpacket (×1.1 for escaping
        # and framing, as the engine reckons) — and after that window exactly one cleanup
        # terminator arrived (an empty line), then our single ping
        log = env["devs"][0].output()
        m = re.search(r"^rz killed discarded=(\d+)$", log, re.M)
        assert m, log
        _every, window = zp.pacing(9600)
        assert int(m.group(1)) <= 1.1 * (window + zp.SUBPACKET), log
        after = re.findall(r"^rz killed then line=(.*)$", log, re.M)
        assert after == ["b''", "b'ping'"], log


def _wait_quiet(srv, port: str, quiet: float = 2.0, cap: float = 40.0) -> None:
    """Wait until the port's buffer has not grown for ``quiet`` s (reads only, no writes)."""
    end = time.time() + cap
    last, since = None, time.time()
    while time.time() < end:
        n = httpx.get(f"{srv.base_url}/api/ports/{port}/read", params={"limit": 0},
                      timeout=20).json().get("next_seq")
        if n != last:
            last, since = n, time.time()
        elif time.time() - since >= quiet:
            return
        time.sleep(0.2)


def test_own_timeout_message_and_clean_shell(tmp_path):
    """Our deadline expires mid-upload: one ABORT_SEQ, cleanup, actionable message."""
    with _sim(tmp_path, interval=0) as env:
        big = tmp_path / "big.bin"
        big.write_bytes(os.urandom(64 << 20))
        out = lite.call(env["client"], "zsend",
                        {"port": "a", "path": str(big), "timeout_ms": 1500})
        assert re.match(r"^error: timed out at \d+/67108864 bytes after \ds; at 115200 baud "
                        r"this needs ≈5825s; retry with timeout_ms>=\d+", out), out
        _shell_clean(env["srv"])


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
        _shell_clean(srv)


def test_zrecv_missing_remote_file(tmp_path):
    """lsz starts a session even for a missing file, then cancels: nothing of ours reaches
    the shell, the hint is the device's own line."""
    with _sim(tmp_path) as env:
        out = lite.call(env["client"], "zrecv",
                        {"port": "a", "remote": "nope.bin", "dir": str(tmp_path / "o")})
        assert out.startswith("error: device sent no file (missing/unreadable remote file?); "
                              "device said: sz done rc=128"), out  # (+ PKT lines it printed)
        assert os.listdir(tmp_path / "o") == []
        _shell_clean(env["srv"])


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
def test_zrecv_unwritable_dir_for_second_file(tmp_path, monkeypatch):
    """Our save side fails (read-only dir when the second file arrives): the same cleanup path
    as any failure — first file kept and listed, then ``error: cannot write …``, clean shell."""
    real = zc._SaveDir.__call__
    calls = []

    def offer(self, name, size):
        calls.append(name)
        if len(calls) == 2:
            os.chmod(self.dir, 0o500)
        return real(self, name, size)

    monkeypatch.setattr(zc._SaveDir, "__call__", offer)
    with _sim(tmp_path) as env:
        (env["files"] / "one.bin").write_bytes(b"1" * 3000)
        (env["files"] / "two.bin").write_bytes(b"2" * 3000)
        out_dir = tmp_path / "o"
        try:
            out = zc.zrecv(env["srv"].base_url, None, "a", ["one.bin", "two.bin"], str(out_dir))
        finally:
            os.chmod(out_dir, 0o700)
        lines = out.split("\n")
        assert lines[0].startswith("received one.bin 3000B crc="), out
        assert lines[1].startswith(f"error: cannot write {out_dir / 'two.bin'}: "
                                   "Permission denied"), out
        assert len(lines) == 2, out
        assert sorted(os.listdir(out_dir)) == ["one.bin"]
        _shell_clean(env["srv"])


def test_device_did_not_start_zmodem_hint(tmp_path, monkeypatch):
    monkeypatch.setattr(zc, "START_TIMEOUT", 1.5)
    with _sim(tmp_path, interval=0) as env:
        out = lite.call(env["client"], "zrecv", {
            "port": "a", "remote": "nope.bin", "dir": str(tmp_path / "o"),
            "cmd": "echo sz: cannot open nope.bin"})
        assert out == "error: device did not start ZMODEM; device said: sz: cannot open nope.bin"
        _shell_clean(env["srv"])


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
        _shell_clean(srv)


def test_zrecv_lite_overwrite_and_default_dir(tmp_path, monkeypatch):
    with _sim(tmp_path) as env:
        (env["files"] / "g.txt").write_bytes(b"dev\n")
        here = tmp_path / "cwd"
        here.mkdir()
        (here / "g.txt").write_bytes(b"old\n")
        monkeypatch.chdir(here)
        out = lite.call(env["client"], "zrecv", {"port": "a", "remote": "g.txt"})
        assert out.endswith(f"-> {here / 'g.txt.1'}"), out  # absolute even without dir
        out = lite.call(env["client"], "zrecv", {"port": "a", "remote": ["g.txt"],
                                                 "overwrite": True})
        assert out.endswith(f"-> {here / 'g.txt'}") and (here / "g.txt").read_bytes() == b"dev\n"
        _shell_clean(env["srv"])


def test_cli_zsend_content_and_files(tmp_path, capsys, monkeypatch):
    import io
    import sys

    with _sim(tmp_path) as env:
        srv = env["srv"]
        f = tmp_path / "local.bin"
        f.write_bytes(os.urandom(3000))
        assert cli.main(["zsend", "a", str(f), "--url", srv.base_url]) == 0
        assert capsys.readouterr().out.startswith("sent local.bin 3000B")
        raw = b"from stdin\r\nkeeps CRLF \xff\n"
        monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(raw)))
        assert cli.main(["zsend", "a", "--content", "-", "--name", "in.txt",
                         "--url", srv.base_url]) == 0
        assert capsys.readouterr().out.startswith(f"sent in.txt {len(raw)}B")
        assert (env["recv"] / "local.bin").read_bytes() == f.read_bytes()
        assert (env["recv"] / "in.txt").read_bytes() == raw
        assert cli.main(["zsend", "a", "--url", srv.base_url]) == 1
        assert capsys.readouterr().out.startswith("error: ")
        _shell_clean(srv)


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
        _shell_clean(env["srv"])


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
        assert out.endswith("retry in ~5 s"), out
        claimed = browser.wait(lambda e: e.get("type") == "zmodem_claimed")
        browser.send({"type": "zmodem_end", "port": "a", "session": claimed["session"],
                      "abort": True})
        browser.wait(lambda e: e.get("type") == "zmodem_closed")
        # the browser's abort is the daemon's ABORT_SEQ (known limitation, ARCHITECTURE §14):
        # flush the device line once before the check
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
        assert out.endswith("retry in ~5 s)"), out
        # unclaimed → the daemon's own abort (known limitation, ARCHITECTURE §14)
        _command_until(env["srv"], "a", "ping", r"^pong$", 12)


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
        _shell_clean(srv)


def test_cleanup_eol_setting_is_used(tmp_path, monkeypatch):
    """``zmodem_cleanup_eol: none`` → after a device cancel nothing at all is written."""
    writes: list[bytes] = []
    real = zc._WsTransport.write

    def spy(self, data, **kw):
        writes.append(data)
        return real(self, data, **kw)

    monkeypatch.setattr(zc._WsTransport, "write", spy)
    with _sim(tmp_path, port_cfg={"zmodem_cleanup_eol": "none"}) as env:
        out = lite.call(env["client"], "zrecv", {"port": "a", "remote": "nope.bin",
                                                 "dir": str(tmp_path / "o")})
        assert out.startswith("error: device sent no file"), out
        assert writes == []
        _shell_clean(env["srv"])


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
            assert bad == {"ok": False, "error": "give exactly one of path or content"}
        finally:
            mcp_server.configure(*saved)
        _shell_clean(env["srv"])



def test_lrz_killed_mid_upload_over_pipes():
    """Real lrz over plain pipes (no PTY chain), SIGTERM'd mid-file: the engine sees the CAN,
    the first file is reported, and the cleanup writes no abort sequence (the peer is gone) —
    only the one line terminator."""
    import io
    import select
    import signal
    import subprocess
    import tempfile
    import threading

    from serialhub import zmodem_proto as zp

    lrz = next((p for p in ("lrz", "rz") if __import__("shutil").which(p)), None)
    work = tempfile.mkdtemp()
    proc = subprocess.Popen([lrz, "-y"], cwd=work, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    class Pipe:
        def __init__(self):
            self.writes: list[bytes] = []
            self.ends: list = []
            self.closed = None
            self.cleaning = False
            self._error = None
            self.after_cancel = 0

        def read(self, timeout):
            r, _, _ = select.select([proc.stdout], [], [], timeout)
            return os.read(proc.stdout.fileno(), 65536) if r else b""

        def write(self, data, *, force=False):
            self.writes.append(bytes(data))
            try:
                proc.stdin.write(data)
                proc.stdin.flush()
            except (BrokenPipeError, OSError):
                pass

        def sync(self, timeout=0):
            return True

        def settle(self, silence=0.3, cap=3.0):
            out = bytearray()
            while True:
                d = self.read(silence)
                if not d:
                    return bytes(out), False
                out += d

        def end(self, *, abort, tail=b"", wait=0):
            self.ends.append((abort, tail))

    t = Pipe()
    small, big = b"tiny\n", os.urandom(32 << 20)
    threading.Timer(0.5, lambda: proc.send_signal(signal.SIGTERM)).start()
    with pytest.raises(zp.ZmodemError) as ei:
        zp.send_files(t, [zp.FileSpec("small.txt", io.BytesIO(small), len(small)),
                          zp.FileSpec("big.bin", io.BytesIO(big), len(big))],
                      deadline=time.monotonic() + 30, baud=115200, abort_on_error=False)
    exc = ei.value
    assert exc.cancelled or exc.peer_gone, exc
    assert [f.name for f in exc.files if not f.skipped] == ["small.txt"]
    n = len(t.writes)
    out = zc._cleanup(t, exc, b"\r")
    assert out.gone
    assert t.writes[n:] == [b"\r"]          # no ABORT_SEQ to a peer that is gone
    assert t.ends and t.ends[0][0] is False  # ended "done", not a daemon abort
    assert proc.wait(5) != 0
    assert open(os.path.join(work, "small.txt"), "rb").read() == small
