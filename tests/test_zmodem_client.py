"""Unit tests for ``serialhub/zmodem_client.py`` and the lite zsend/zrecv argument handling.

No daemon: argument errors are reported before any connection is made; the
receive-side file handling runs against the Python sender over an in-memory pipe.
"""

from __future__ import annotations

import base64
import io
import os
import threading
import time
import zlib

import pytest

from serialhub import lite
from serialhub import zmodem_client as zc
from serialhub import zmodem_proto as zp
from serialhub.client import Client
from serialhub.zmodem import ABORT_SEQ

from test_zmodem_proto import _Chan, _End, _spec  # tests/ is on sys.path (rootdir conftest)

NOWHERE = Client("http://127.0.0.1:1", timeout=1.0)


@pytest.mark.parametrize("args,needle", [
    ({"port": "a"}, "exactly one of path or content"),
    ({"port": "a", "path": "x", "content": "y", "name": "n"}, "exactly one"),
    ({"port": "a", "content": "y"}, "needs name"),
    ({"port": "a", "content": "y", "name": "n", "encoding": "base64"}, "bad base64"),
    ({"port": "a", "content": "%%%", "name": "n", "encoding": "base64"}, "bad base64"),
    ({"port": "a", "content": "x" * (zc.INLINE_MAX + 1), "name": "n"}, "too large"),
    ({"port": "a", "content": "y", "name": ".."}, "bad name"),
    ({"port": "a", "path": "x", "name": "n"}, "name goes with content"),
    ({"port": "a", "path": []}, "string or an array"),
    ({"port": "a", "path": "/definitely/not/here.bin"}, "no such file"),
    ({"port": "a", "content": "y", "name": "n", "encoding": "hex"}, "must be one of"),
])
def test_zsend_argument_errors(args, needle) -> None:
    out = lite.call(NOWHERE, "zsend", args)
    assert out.startswith("error: ") and needle in out, out


@pytest.mark.parametrize("args", [{"port": "a"}, {"port": "a", "remote": ""},
                                  {"port": "a", "remote": "x", "inline": "maybe"}])
def test_zrecv_argument_errors(args) -> None:
    assert lite.call(NOWHERE, "zrecv", args).startswith("error: ")


def test_unreachable_daemon_is_error_text(tmp_path) -> None:
    out = lite.call(NOWHERE, "zrecv", {"port": "a", "remote": "f", "dir": str(tmp_path)})
    assert out.startswith("error: cannot connect to daemon websocket"), out
    f = tmp_path / "f.bin"
    f.write_bytes(b"x")
    out = lite.call(NOWHERE, "zsend", {"port": "a", "path": str(f)})
    assert out.startswith("error: cannot connect"), out


def test_inline_content_decoding() -> None:
    assert zc.inline_content("héllo\n", "dir/sub/notes.txt") == ("notes.txt", "héllo\n".encode())
    assert zc.inline_content(base64.b64encode(b"\x00\xff").decode(), "C:\\x\\b.bin",
                             "base64") == ("b.bin", b"\x00\xff")
    assert zc.inline_content("", "empty") == ("empty", b"")


def test_recv_command_quoting() -> None:
    assert zc.recv_command(["a.bin", "log/x.txt"]) == "sz a.bin log/x.txt"
    assert zc.recv_command(["测试 file.bin"]) == "sz '测试 file.bin'"
    assert zc.recv_command(["a"], "sz -b {remote}") == "sz -b a"
    assert zc.recv_command(["a"], "") == ""


def _receive_into(tmp_path, files: dict[str, bytes], overwrite=False) -> zc._SaveDir:
    a2b, b2a = _Chan(), _Chan()
    saver = zc._SaveDir(str(tmp_path), overwrite)
    deadline = time.monotonic() + 10
    out: dict = {}
    th = threading.Thread(target=lambda: out.setdefault(
        "rx", zp.receive_files(_End(a2b, b2a), saver, deadline=deadline)), daemon=True)
    th.start()
    zp.send_files(_End(b2a, a2b), [_spec(n, d) for n, d in files.items()], deadline=deadline)
    th.join(10)
    return saver


def test_path_traversal_names_stay_inside_dest(tmp_path) -> None:
    dest = tmp_path / "dest"
    dest.mkdir()
    saver = _receive_into(dest, {"../../evil.txt": b"a", "/etc/passwd": b"b",
                                 "..\\..\\win.ini": b"c", "..": b"d", "ok.bin": b"e"})
    assert sorted(os.listdir(dest)) == ["evil.txt", "ok.bin", "passwd", "win.ini"]
    assert saver.refused == [".."]
    assert not (tmp_path / "evil.txt").exists()
    assert (dest / "passwd").read_bytes() == b"b"


def test_existing_file_renamed_or_overwritten(tmp_path) -> None:
    (tmp_path / "f.txt").write_bytes(b"old")
    saver = _receive_into(tmp_path, {"f.txt": b"new1"})
    saver2 = _receive_into(tmp_path, {"f.txt": b"new2"})
    assert [s.path for s in saver.saved + saver2.saved] == [
        str(tmp_path / "f.txt.1"), str(tmp_path / "f.txt.2")]
    assert (tmp_path / "f.txt").read_bytes() == b"old"
    _receive_into(tmp_path, {"f.txt": b"new3"}, overwrite=True)
    assert (tmp_path / "f.txt").read_bytes() == b"new3"
    assert not [n for n in os.listdir(tmp_path) if n.endswith(".part")]


def test_partial_file_removed_on_failure(tmp_path) -> None:
    saver = zc._SaveDir(str(tmp_path), False)
    sink = saver("big.bin", 10)
    sink.write(b"12345")
    saver.discard_partial()
    assert os.listdir(tmp_path) == []


def _env(tmp_path, files: dict[str, bytes]) -> dict:
    out = []
    for name, data in files.items():
        (tmp_path / name).write_bytes(data)
        out.append({"name": name, "bytes": len(data), "crc32": f"{zlib.crc32(data):08X}",
                    "path": str(tmp_path / name)})
    return {"ok": True, "files": out}


def test_format_recv_inline_text_binary_and_cap(tmp_path) -> None:
    text = "héllo\nworld\n"
    env = _env(tmp_path, {"a.txt": text.encode(), "b.bin": b"\x00\x01\xff",
                          "c.log": b"x" * 5000})
    out = zc.format_recv(env, inline=True, cap=lite.MAX_BLOCK_CHARS)
    lines = out.split("\n")
    assert lines[0] == (f"received a.txt 13B crc={zlib.crc32(text.encode()):08X}"
                        f" -> {tmp_path / 'a.txt'}")
    assert lines[1].endswith("(binary, not shown)") and lines[1].startswith("received b.bin 3B")
    assert "--- a.txt\nhéllo\nworld\n\n--- c.log\n" in out
    shown = 4000 - len(text)
    assert out.endswith("x" * shown + f"…+{5000 - shown} chars, full file at {tmp_path / 'c.log'}")
    assert "binary" not in zc.format_recv(env)  # inline off: only the lines
    assert len(zc.format_recv(env).split("\n")) == 3


# --------------------------------------------------------------------------- #
# fix round 1
# --------------------------------------------------------------------------- #
class _FakeTransport:
    """Records writes/ends; ``replies`` = what the device 'says' after each write."""

    def __init__(self, replies: list[bytes], chatty: bool = False) -> None:
        self.replies = list(replies)
        self.pending = b""
        self.writes: list[bytes] = []
        self.ends: list[tuple[bool, bytes]] = []
        self.closed = None
        self.chatty = chatty
        self.cleaning = False
        self._error = None

    def write(self, data: bytes, *, force: bool = False) -> None:
        self.writes.append(data)
        if self.replies:
            self.pending += self.replies.pop(0)

    def sync(self, timeout=0):
        return True

    def settle(self, silence=0.3, cap=3.0):
        if self.chatty:
            return b"x" * 10, True
        out, self.pending = self.pending, b""
        return out, False

    def end(self, *, abort: bool, tail: bytes = b"", wait: float = 0) -> str:
        self.ends.append((abort, tail))
        return "abort" if abort else "done"


@pytest.mark.parametrize("setting,eol", [("cr", b"\r"), ("lf", b"\n"), ("crlf", b"\r\n"),
                                         ("ctrl-u-cr", b"\x15\r"), ("none", b""),
                                         (None, b"\r"), ("bogus", b"\r")])
def test_cleanup_eol_values(setting, eol) -> None:
    assert zc.cleanup_eol_bytes(setting) == eol
    t = _FakeTransport([b"rz done rc=1\r\n$ ", b"ERR unknown: \x02\x03junk\r\n$ "])
    lines = zc._cleanup(t, zp.ZmodemError("boom", timed_out=True),
                        zc.cleanup_eol_bytes(setting)).lines
    # ABORT_SEQ exactly once, then the terminator (if any) — nothing else
    assert t.writes == [ABORT_SEQ] + ([eol] if eol else [])
    assert lines == ["rz done rc=1", "$"]
    assert t.ends == [(False, b"rz done rc=1\r\n$\r\n")]  # the reply to our eol is dropped


def test_cleanup_after_peer_cancel_writes_only_the_eol() -> None:
    t = _FakeTransport([b"$ "])
    exc = zp.ZmodemError("cancelled", cancelled=True,
                         before=zp.encode_hex_header(zp.ZRQINIT) + b"sz: cannot open x\r\n",
                         after=b"\x08\x08sz done rc=128\r\n")
    lines = zc._cleanup(t, exc, b"\r").lines
    assert t.writes == [b"\r"]
    assert lines == ["sz: cannot open x", "sz done rc=128"]


def test_cleanup_drops_provoked_junk_and_never_doubles_the_abort() -> None:
    exc = zp.ZmodemError("c", cancelled=True,
                         after=b"rz done rc=143\r\nERR unknown: \x8a\x11\xff\r\nmore\r\n")
    assert zc._device_lines(exc.before, exc.after) == ["rz done rc=143", "more"]
    # the device keeps talking (settle capped): we already wrote ABORT_SEQ, so the daemon must
    # not write another (abort:false, empty tail), and the terminator still goes out
    t = _FakeTransport([], chatty=True)
    zc._cleanup(t, zp.ZmodemError("boom", timed_out=True), b"\r")
    assert t.writes == [ABORT_SEQ, b"\r"] and t.ends == [(False, b"")]


def test_cleanup_classifies_a_write_error_followed_by_can() -> None:
    """Daemon 'Write timeout' first, device CAN right after: no ABORT_SEQ, reported as gone."""
    t = _FakeTransport([])
    t.pending = b"\x18" * 10 + b"\x08" * 10 + b"rz done rc=143\r\nERR unknown: \x01\x02\r\n"
    out = zc._cleanup(t, zp.ZmodemError("daemon: Write timeout"), b"\r")
    assert out.gone and out.lines == ["rz done rc=143"]
    assert t.writes == [b"\r"]


def test_cleanup_skipped_when_daemon_already_closed() -> None:
    t = _FakeTransport([])
    t.closed = "timeout"
    zc._cleanup(t, zp.ZmodemError("boom"), b"\r")
    assert t.writes == [] and t.ends == []


@pytest.mark.parametrize("name", ["CON", "nul.txt", "COM1", "lpt9.log", "a:b", "c:\\x\\con",
                                  "trail.", "trail ", "..", "", "x\x01"])
def test_safe_name_rejects_windows_hazards(name) -> None:
    assert zc.safe_name(name) is None


def test_safe_name_keeps_normal_names() -> None:
    assert zc.safe_name("dir/console.log") == "console.log"
    assert zc.safe_name("测试 file.bin") == "测试 file.bin"
    assert zc.safe_name("C:\\tmp\\fw.bin") == "fw.bin"


def test_inline_tolerates_a_bad_byte_and_reads_at_most_64k(tmp_path) -> None:
    log = b"boot ok\r\n" * 10 + b"\xff garbled\r\n" + b"ready\r\n"
    big = b"y" * (zc.INLINE_READ + 1000)
    env = _env(tmp_path, {"serial.log": log, "big.txt": big})
    out = zc.format_recv(env, inline=True, cap=lite.MAX_BLOCK_CHARS)
    assert "(binary" not in out and "--- serial.log\nboot ok" in out and "\ufffd garbled" in out
    shown = 4000 - len(log.decode("utf-8", "replace"))
    assert out.endswith(f"…+{len(big) - shown} chars, full file at {tmp_path / 'big.txt'}")


def test_mostly_undecodable_is_binary(tmp_path) -> None:
    env = _env(tmp_path, {"b.dat": bytes(range(0x80, 0x100)) * 4})
    assert zc.format_recv(env, inline=True).endswith("(binary, not shown)")


def test_partial_results_come_before_the_error() -> None:
    env = {"ok": False, "error": "device cancelled the transfer",
           "files": [{"name": "a", "bytes": 3, "crc32": "00000001", "seconds": 0.1,
                      "skipped": False}]}
    assert zc.format_send(env).split("\n") == [
        "sent a 3B crc=00000001 in 0.1s", "error: device cancelled the transfer"]
    renv = {"ok": False, "error": "timed out",
            "files": [{"name": "a", "bytes": 3, "crc32": "00000001", "path": "/x/a"}]}
    assert zc.format_recv(renv).split("\n") == ["received a 3B crc=00000001 -> /x/a",
                                                "error: timed out"]


def test_all_skipped_is_an_error() -> None:
    skipped = {"name": "cfg", "bytes": 3, "crc32": "00000001", "seconds": 0.0, "skipped": True}
    out = zc.format_send({"ok": True, "files": [skipped]})
    assert out.split("\n") == ["error: nothing was sent",
                               'skipped cfg: device refused it (already exists? try cmd "rz -y")']
    sent = dict(skipped, name="ok", skipped=False)
    assert not zc.format_send({"ok": True, "files": [sent, skipped]}).startswith("error")


def test_auto_timeout_and_message() -> None:
    assert zc.clamp_timeout(None) is None
    assert zc.auto_seconds(1 << 20, 115200) == pytest.approx((1 << 20) * 10 / 115200 * 1.5 + 30)
    assert zc.auto_seconds(1 << 30, 9600) == zc.TIMEOUT_MAX_MS / 1000
    clock = zc._Clock(None, 9600, 96_000)
    assert clock() - clock.t0 == pytest.approx(96_000 * 10 / 9600 * 1.5 + 30)
    clock.done["f"] = 1000
    msg = clock.message()
    assert msg.startswith("timed out at 1000/96000 bytes after 0s; at 9600 baud this needs ≈100s;")
    assert msg.endswith("retry with timeout_ms>=180000")
    fixed = zc._Clock(5000, 9600, 0)
    fixed.announce(10_000)
    assert fixed() - fixed.t0 == pytest.approx(5.0) and fixed.total == 10_000
    auto = zc._Clock(None, 9600, 0)
    auto.announce(96_000)
    assert auto() - time.monotonic() > 170


def test_default_dir_falls_back_to_temp(tmp_path, monkeypatch) -> None:
    import tempfile

    monkeypatch.chdir(tmp_path)
    assert zc.default_dir() == os.getcwd()
    monkeypatch.setattr(os, "access", lambda p, m: False)
    assert zc.default_dir() == tempfile.gettempdir()


def test_lite_schema_arrays_and_overwrite() -> None:
    by = {t["name"]: t["input_schema"]["properties"] for t in lite.TOOLS}
    assert by["zsend"]["path"]["type"] == "array" and by["zrecv"]["remote"]["type"] == "array"
    assert by["zsend"]["overwrite"]["type"] == by["zrecv"]["overwrite"]["type"] == "boolean"
    assert by["zsend"]["timeout_ms"]["description"] == "Default: auto from size and baud"
    out = lite.call(NOWHERE, "zsend", {"port": "a", "content": "x", "name": "n",
                                       "overwrite": "maybe"})
    assert out.startswith("error: overwrite must be true or false")


def test_mcp_full_profile_errors_stay_in_envelope(monkeypatch) -> None:
    pytest.importorskip("mcp")
    import asyncio

    from serialhub import mcp_server

    def boom(*a, **kw):
        raise OSError("disk full")

    monkeypatch.setattr(zc, "receive_session", boom)
    monkeypatch.setattr(zc, "send_session", boom)
    assert asyncio.run(mcp_server.zmodem_receive("a", "f")) == {
        "ok": False, "error": "OSError: disk full"}
    assert asyncio.run(mcp_server.zmodem_send("a", path="x")) == {
        "ok": False, "error": "OSError: disk full"}


# --------------------------------------------------------------------------- #
# fix round 2
# --------------------------------------------------------------------------- #
_CAN = b"\x18" * 8 + b"\x08" * 8


@pytest.mark.parametrize("before,after,want", [
    # UTF-8 text after the cancel
    (b"", _CAN + "sz: 无法打开 nope.bin: 没有那个文件\r\nsz done rc=128\r\n".encode(),
     ["sz: 无法打开 nope.bin: 没有那个文件", "sz done rc=128"]),
    # a real message after one garbage line
    (b"", _CAN + b"\x8a\x91x\r\nsz: cannot open nope.bin\r\n", ["sz: cannot open nope.bin"]),
    # ANSI colours, before and after the cancel
    (b"\x1b[31mE: flash write failed\x1b[0m\r\n", _CAN, ["E: flash write failed"]),
    (b"", _CAN + b"\x1b[31mE: flash full\x1b[0m\r\n# ", ["E: flash full", "#"]),
    # Latin-1 (not UTF-8) text
    (b"Fehler: Datei \xfcberschritten\r\n", _CAN, ["Fehler: Datei \ufffdberschritten"]),
], ids=["utf8-after", "garbage-line-first", "ansi-before", "ansi-after", "latin1-before"])
def test_hint_keeps_real_device_messages(before, after, want) -> None:
    assert zc._device_lines(before, after) == want


def test_hint_drops_protocol_echo_and_binary_and_caps_length() -> None:
    echo = (b"ERR unknown: B0100000023be50\r\n"          # our ZRINIT, run as a command
            b"ERR unknown: \x8a\x11\xff\r\n"             # our hex-header trailer
            b"\x18\x18B0900\r\n"                          # ZDLE
            + bytes(range(0x80, 0xC0)) + b"\r\n"          # binary
            + b"ok " + b"y" * 400 + b"\r\n")
    lines = zc._device_lines(b"", _CAN + echo)
    assert lines == ["ok " + "y" * 237]
    assert len(lines[0]) == 240


def _device_abort_copies(t: "_FakeTransport") -> int:
    """ABORT_SEQ copies reaching the device: ours plus one per ``abort:true`` end (daemon)."""
    return sum(w.count(ABORT_SEQ) for w in t.writes) + sum(1 for abort, _ in t.ends if abort)


_CANCEL = zp.ZmodemError("cancelled", cancelled=True, after=b"rz done rc=1\r\n")
_GONE = zp.ZmodemError("gone", peer_gone=True, before=b"Killed\r\n")


@pytest.mark.parametrize("exc,replies,chatty,pending,closed,gone", [
    (zp.ZmodemError("t", timed_out=True), [b"$ "], False, b"", None, False),
    (zp.ZmodemError("t", timed_out=True), [], True, b"", None, False),
    (zp.ZmodemError("receiver never sent ZRINIT"), [b"$ "], False, b"", None, False),
    (zp.ZmodemError("receiver never sent ZRINIT"), [], True, b"", None, False),
    (_CANCEL, [b"$ "], False, b"", None, True),
    (_CANCEL, [], True, b"", None, True),
    (_GONE, [], False, b"", None, True),
    (_GONE, [], True, b"", None, True),
    (zp.ZmodemError("daemon: Write timeout"), [], False, _CAN + b"rz done\r\n", None, True),
    (zp.ZmodemError("daemon: Write timeout"), [], False, b"Killed\r\n# ", None, True),
    (zp.ZmodemError("daemon ended the session (timeout)"), [], False, b"", "timeout", False),
], ids=["timeout", "timeout-capped", "own-error", "own-error-capped", "cancel",
        "cancel-capped", "peer-gone", "peer-gone-capped", "write-error-then-can",
        "write-error-then-text", "daemon-closed"])
def test_abort_seq_at_most_once_and_never_to_a_gone_peer(exc, replies, chatty, pending,
                                                         closed, gone) -> None:
    t = _FakeTransport(list(replies), chatty=chatty)
    t.pending, t.closed = pending, closed
    out = zc._cleanup(t, exc, b"\r")
    assert out.gone is gone
    copies = _device_abort_copies(t)
    assert copies == 0 if gone else copies <= 1, (t.writes, t.ends)
    if closed is None:
        assert t.writes[-1:] == [b"\r"]   # the cleanup terminator always goes out


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0, reason="needs POSIX permissions")
def test_unwritable_dest_is_an_engine_error_with_partial_results(tmp_path) -> None:
    """A save-side OSError (read-only dir for the second file) becomes the engine's error type,
    so the client's one cleanup path runs; the first file is kept and reported."""
    dest = tmp_path / "dest"
    dest.mkdir()
    a2b, b2a = _Chan(), _Chan()
    saver = zc._SaveDir(str(dest), False)
    real_call = zc._SaveDir.__call__
    offers = []

    def offer(name, size):
        offers.append(name)
        if len(offers) == 2:
            os.chmod(dest, 0o500)
        return real_call(saver, name, size)

    deadline = time.monotonic() + 10
    out: dict = {}

    def rx():
        try:
            zp.receive_files(_End(a2b, b2a), offer, deadline=deadline, abort_on_error=False)
        except BaseException as exc:  # noqa: BLE001
            out["exc"] = exc

    th = threading.Thread(target=rx, daemon=True)
    th.start()
    try:
        with pytest.raises(zp.ZmodemError):
            zp.send_files(_End(b2a, a2b), [_spec("a.bin", b"first"), _spec("b.bin", b"second")],
                          deadline=time.monotonic() + 3, frame_timeout=0.5)
        th.join(10)
    finally:
        os.chmod(dest, 0o700)
    exc = out["exc"]
    assert isinstance(exc, zp.ZmodemError), exc
    assert str(exc).startswith(f"cannot write {dest / 'b.bin'}: Permission denied"), exc
    assert [f.name for f in exc.files] == ["a.bin"]
    assert [s.name for s in saver.saved] == ["a.bin"]
    assert (dest / "a.bin").read_bytes() == b"first"


def test_cancel_mid_file_names_the_file_and_progress(tmp_path, monkeypatch) -> None:
    """A cancel after a ZFILE is not "device sent no file": it says where it stopped."""
    info = zp.file_info(zp.FileSpec("big.bin", io.BytesIO(b""), 4096), 1, 4096)
    replies = [  # to our ZRINIT, then to our ZRPOS: one data subpacket, then the cancel
        [zp.encode_bin_header(zp.ZFILE, bytes([0, 0, 0, 1]), use32=True)
         + zp.encode_subpacket(info, zp.ZCRCW, use32=True)],
        [zp.encode_bin_header(zp.ZDATA, zp.pos_bytes(0), use32=True)
         + zp.encode_subpacket(b"A" * 1024, zp.ZCRCG, use32=True), _CAN + b"sz: read error\r\n"]]

    class Peer:
        chunks = [zp.encode_hex_header(zp.ZRQINIT)]

        def read(self, timeout):
            return self.chunks.pop(0) if self.chunks else b""

        def write(self, data):
            if replies:
                self.chunks += replies.pop(0)

    def fake_run(url, token, port, cmd, direction, timeout_ms, size, engine):
        try:
            return engine(Peer(), zc._Clock(5000, None, size), None)
        except zp.ZmodemError as exc:
            hint = " | ".join(zc._device_lines(exc.before, exc.after))
            raise zc.ZmodemClientError("device cancelled the transfer", cancelled=True,
                                       hint=hint, files=exc.files) from None

    monkeypatch.setattr(zc, "_run_session", fake_run)
    env = zc.receive_session("http://x", None, "a", ["big.bin"], str(tmp_path))
    assert env == {"ok": False, "files": [], "error": "device cancelled the transfer at "
                   "1024/4096 bytes of big.bin; device said: sz: read error"}
    assert os.listdir(tmp_path) == []


def test_cli_exit_code_comes_from_the_result_not_the_text(tmp_path, monkeypatch, capsys) -> None:
    """Inlined file content with lines like ``error: …`` / ``skipped …`` must not fail the CLI;
    a partial or failed result must."""
    from serialhub import cli

    body = b"boot\nerror: sensor 3 missing\nskipped calibration\nnot received ack\n"
    log = tmp_path / "dev.log"
    log.write_bytes(body)
    ok = {"ok": True, "files": [{"name": "dev.log", "bytes": len(body),
                                 "crc32": f"{zlib.crc32(body):08X}", "path": str(log)}],
          "missing": [], "refused": []}
    monkeypatch.setattr(zc, "receive_session", lambda *a, **kw: dict(ok))
    assert cli.main(["zrecv", "a", "dev.log", "--inline"]) == 0
    assert "error: sensor 3 missing" in capsys.readouterr().out
    monkeypatch.setattr(zc, "receive_session", lambda *a, **kw: dict(ok, missing=["x"]))
    assert cli.main(["zrecv", "a", "dev.log", "x", "--inline"]) == 1
    monkeypatch.setattr(zc, "receive_session",
                        lambda *a, **kw: {"ok": False, "error": "boom", "files": []})
    assert cli.main(["zrecv", "a", "dev.log"]) == 1
    sent = {"name": "a", "bytes": 1, "crc32": "00000001", "seconds": 0.1, "skipped": False}
    monkeypatch.setattr(zc, "send_session", lambda *a, **kw: {"ok": True, "files": [sent]})
    assert cli.main(["zsend", "a", str(log)]) == 0
    monkeypatch.setattr(zc, "send_session", lambda *a, **kw: {
        "ok": True, "files": [sent, dict(sent, name="b", skipped=True)]})
    assert cli.main(["zsend", "a", str(log)]) == 1
    capsys.readouterr()


# --------------------------------------------------------------------------- #
# fix round: a non-protocol error keeps the files already transferred
# --------------------------------------------------------------------------- #
def test_run_engine_wraps_local_error_with_completed_files() -> None:
    done = zp.FileResult("a.bin", 5, zlib.crc32(b"first"), 0.1)

    def engine(t, clock, baud):
        exc = OSError("read error")
        exc.files = [done]
        raise exc

    with pytest.raises(zc._LocalError) as ei:
        zc._run_engine(engine, None, None, None)
    assert str(ei.value) == "OSError: read error" and ei.value.files == [done]


def _quiet(fn):
    """A peer thread whose own (expected) failure must not leak into later tests."""
    def run():
        try:
            fn()
        except Exception:  # noqa: BLE001
            pass
    return run


def test_send_session_oserror_on_second_file_lists_the_first(tmp_path, monkeypatch) -> None:
    """OSError while reading the SECOND local file: ``sent a.bin`` line, then ``error:``."""
    a = tmp_path / "a.bin"
    a.write_bytes(b"first")
    b = tmp_path / "b.bin"
    b.write_bytes(b"second")
    real_open = open

    class _Bad:
        def __init__(self, fh):
            self.fh = fh

        def read(self, *a):
            raise OSError("read error")

        def __getattr__(self, name):
            return getattr(self.fh, name)

    def fake_open(path, *a, **kw):
        fh = real_open(path, *a, **kw)
        return _Bad(fh) if str(path).endswith("b.bin") else fh

    def fake_run(url, token, port, cmd, direction, timeout_ms, size, engine):
        a2b, b2a = _Chan(), _Chan()
        deadline = time.monotonic() + 10
        threading.Thread(target=_quiet(lambda: zp.receive_files(
            _End(a2b, b2a), lambda n, s: io.BytesIO(), deadline=deadline)), daemon=True).start()
        try:
            return zc._run_engine(engine, _End(b2a, a2b), zc._Clock(10000, None, size), None)
        except zp.ZmodemError as exc:
            raise zc.ZmodemClientError(str(exc), files=exc.files) from None

    monkeypatch.setattr("builtins.open", fake_open)
    monkeypatch.setattr(zc, "_run_session", fake_run)
    env = zc.send_session("http://x", None, "a", [str(a), str(b)])
    monkeypatch.setattr("builtins.open", real_open)
    assert env["ok"] is False and [f["name"] for f in env["files"]] == ["a.bin"]
    lines = zc.format_send(env).splitlines()
    assert lines[0].startswith("sent a.bin 5B") and lines[-1] == "error: OSError: read error"
    assert not zc.send_ok(env)
