"""Unit tests for ``serialhub/zmodem_client.py`` and the lite zsend/zrecv argument handling.

No daemon: argument errors are reported before any connection is made; the
receive-side file handling runs against the Python sender over an in-memory pipe.
"""

from __future__ import annotations

import base64
import os
import threading
import time
import zlib

import pytest

from serialhub import lite
from serialhub import zmodem_client as zc
from serialhub import zmodem_proto as zp
from serialhub.client import Client

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
