"""Unit tests for the pure-Python ZMODEM engine (``serialhub/zmodem_proto.py``).

No daemon, no serial port: the sender and receiver talk over an in-memory pipe
pair; scripted peers replay bytes captured from lrzsz.
"""

from __future__ import annotations

import io
import os
import threading
import time
import zlib
from collections import deque
from typing import Callable, Optional

import pytest

from serialhub import zmodem_proto as zp
from serialhub.zmodem import ABORT_SEQ


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
class _Chan:
    """One direction of a byte pipe; ``mangle`` may rewrite bytes in flight."""

    def __init__(self, mangle: Optional[Callable[[bytes, int], bytes]] = None) -> None:
        self._q: deque[bytes] = deque()
        self._cv = threading.Condition()
        self.sent = bytearray()
        self.mangle = mangle

    def put(self, data: bytes) -> None:
        if self.mangle:
            data = self.mangle(data, len(self.sent))
        with self._cv:
            self.sent += data
            self._q.append(bytes(data))
            self._cv.notify_all()

    def get(self, timeout: float) -> bytes:
        with self._cv:
            if not self._q:
                self._cv.wait(timeout)
            out = b"".join(self._q)
            self._q.clear()
            return out


class _End:
    def __init__(self, rx: _Chan, tx: _Chan) -> None:
        self.rx, self.tx = rx, tx

    def read(self, timeout: float) -> bytes:
        return self.rx.get(timeout)

    def write(self, data: bytes) -> None:
        self.tx.put(data)


class _Sink(io.BytesIO):
    def __init__(self, store: dict, name: str) -> None:
        super().__init__()
        self._store, self._name = store, name

    def close(self) -> None:
        self._store[self._name] = self.getvalue()
        super().close()


def _spec(name: str, data: bytes) -> zp.FileSpec:
    return zp.FileSpec(name=name, data=io.BytesIO(data), size=len(data), mtime=1_700_000_000)


def _run_pair(files: dict[str, bytes], *, s2r=None, r2s=None, rx_kw=None, timeout=30.0):
    """Python sender -> Python receiver; returns (send_result, recv_result, saved, chans)."""
    a2b, b2a = _Chan(s2r), _Chan(r2s)
    sender, receiver = _End(b2a, a2b), _End(a2b, b2a)
    saved: dict[str, bytes] = {}
    out: dict[str, object] = {}
    deadline = time.monotonic() + timeout

    def rx() -> None:
        try:
            out["rx"] = zp.receive_files(receiver, lambda n, s: _Sink(saved, n),
                                         deadline=deadline, **(rx_kw or {}))
        except Exception as exc:  # noqa: BLE001
            out["rx"] = exc

    th = threading.Thread(target=rx, daemon=True)
    th.start()
    try:
        out["tx"] = zp.send_files(sender, [_spec(n, d) for n, d in files.items()],
                                  deadline=deadline)
    except Exception as exc:  # noqa: BLE001
        out["tx"] = exc
    th.join(timeout)
    return out["tx"], out.get("rx"), saved, (a2b, b2a)


class _Scripted:
    """Transport that serves pre-built chunks and records what was written."""

    def __init__(self, chunks: list[bytes], *, on_write=None) -> None:
        self.chunks = deque(chunks)
        self.written = bytearray()
        self.on_write = on_write

    def read(self, timeout: float) -> bytes:
        if self.chunks:
            return self.chunks.popleft()
        time.sleep(min(timeout, 0.05))
        return b""

    def write(self, data: bytes) -> None:
        self.written += data
        if self.on_write:
            self.on_write(self, bytes(data))


# Captured from lsz 0.13 (tests/zmodem_client/shim_check.mjs): 0-byte "empty.bin",
# the empty first subpacket ends with ZCRCW, and lsz exits after ZFIN without "OO".
LSZ_EMPTY = [
    bytes([42, 42, 24, 66, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 13, 138, 17]),
    bytes([42, 24, 67, 4, 0, 0, 0, 0, 221, 81, 162, 51, 101, 109, 112, 116, 121, 46, 98, 105, 110,
           0, 48, 32, 49, 53, 51, 48, 48, 50, 50, 55, 48, 50, 49, 32, 49, 48, 48, 54, 52, 52, 32,
           48, 32, 49, 32, 48, 0, 24, 107, 175, 126, 124, 11, 17]),
    bytes([42, 24, 67, 10, 0, 0, 0, 0, 188, 239, 146, 140, 24, 104, 231, 6, 107, 24, 209, 42, 24,
           67, 11, 0, 0, 0, 0, 12, 198, 242, 177]),
    bytes([42, 42, 24, 66, 48, 56, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 50, 100, 13, 138]),
]


# --------------------------------------------------------------------------- #
# primitives
# --------------------------------------------------------------------------- #
def test_crc_vectors() -> None:
    assert zp.crc16(b"123456789") == 0x31C3      # CRC-16/XMODEM
    assert zp.crc32(b"123456789") == 0xCBF43926  # CRC-32 (zlib)
    assert zp.crc16(b"") == 0


@pytest.mark.parametrize("ctl", [False, True])
def test_zdle_escape_roundtrip_all_bytes(ctl) -> None:
    data = bytes(range(256)) * 3
    enc = zp.zdle_escape(data, escape_ctl=ctl)
    raw = enc.replace(b"\x18", b"")  # every ZDLE starts an escape
    for b in (0x10, 0x11, 0x13, 0x90, 0x91, 0x93, 0x0D, 0x8D):
        assert bytes([b]) not in raw.replace(bytes([b ^ 0x40]), b"")
    if ctl:
        assert all(not (c & 0x60) == 0 for c in raw)
    assert zp.zdle_unescape(enc) == data


def test_hex_headers_match_lrzsz_bytes() -> None:
    assert zp.encode_hex_header(zp.ZRQINIT) == LSZ_EMPTY[0]
    assert zp.encode_hex_header(zp.ZFIN) == LSZ_EMPTY[3]
    h, used = zp.parse_header(bytearray(LSZ_EMPTY[0]))
    assert (h.type, h.pos, used) == (zp.ZRQINIT, 0, len(LSZ_EMPTY[0]))
    rinit = zp.encode_hex_header(zp.ZRINIT, zp.flags_bytes(zp.CANFDX | zp.CANFC32, bufsize=0x400))
    h, _ = zp.parse_header(bytearray(b"garbage" + rinit))
    assert h.type == zp.ZRINIT and h.flags == zp.CANFDX | zp.CANFC32 and h.bufsize == 0x400


@pytest.mark.parametrize("use32", [False, True])
def test_binary_header_roundtrip(use32) -> None:
    raw = zp.encode_bin_header(zp.ZDATA, zp.pos_bytes(0x12181311), use32=use32)
    assert raw[2] == (zp.ZBIN32 if use32 else zp.ZBIN)
    for cut in range(len(raw)):  # incomplete input: need more, nothing consumed past '*'
        h, used = zp.parse_header(bytearray(raw[:cut]))
        assert h is None and used == 0
    h, used = zp.parse_header(bytearray(raw + b"tail"))
    assert (h.type, h.pos, used) == (zp.ZDATA, 0x12181311, len(raw))
    bad = bytearray(raw)
    bad[-1] ^= 0x01
    h, _ = zp.parse_header(bad)
    assert h is zp.GARBLED


def test_parse_captured_lsz_binary32_header_and_subpacket() -> None:
    buf = bytearray(LSZ_EMPTY[1])
    h, used = zp.parse_header(buf)
    assert h.type == zp.ZFILE
    status, data, end, used2 = zp.parse_subpacket(buf, used, use32=True)
    assert status == "ok" and end == zp.ZCRCW
    assert data.split(b"\0")[0] == b"empty.bin"


@pytest.mark.parametrize("use32", [False, True])
def test_subpacket_roundtrip_and_corruption(use32) -> None:
    payload = os.urandom(1024)
    enc = zp.encode_subpacket(payload, zp.ZCRCG, use32=use32)
    status, data, end, used = zp.parse_subpacket(bytearray(enc), 0, use32=use32)
    assert (status, data, end, used) == ("ok", payload, zp.ZCRCG, len(enc))
    assert zp.parse_subpacket(bytearray(enc[:-1]), 0, use32=use32)[0] == "need"
    bad = bytearray(enc)
    bad[100] = 0x41 if bad[100] != 0x41 else 0x42
    assert zp.parse_subpacket(bad, 0, use32=use32)[0] == "bad"


# --------------------------------------------------------------------------- #
# self-interop: Python sender <-> Python receiver
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("size", [0, 1, 1023, 1024, 1025, 1 << 20])
def test_self_interop_sizes(size) -> None:
    data = os.urandom(size)
    tx, rx, saved, _ = _run_pair({"f.bin": data})
    assert not isinstance(tx, Exception), tx
    assert not isinstance(rx, Exception), rx
    assert saved == {"f.bin": data}
    for res in (tx.files[0], rx.files[0]):
        assert res.size == size and res.crc32 == zlib.crc32(data)


def test_self_interop_multiple_files_and_unicode_name() -> None:
    files = {"a.bin": os.urandom(5000), "测试 file.bin": os.urandom(3000), "z": b""}
    tx, rx, saved, _ = _run_pair(files)
    assert saved == files
    assert [f.name for f in tx.files] == list(files)
    assert [f.name for f in rx.files] == list(files)


@pytest.mark.parametrize("rx_kw", [{"use_crc32": False}, {"escape_ctl": True},
                                   {"use_crc32": False, "bufsize": 2048}])
def test_self_interop_receiver_options(rx_kw) -> None:
    data = bytes(range(256)) * 40 + os.urandom(7000)
    tx, rx, saved, (a2b, _b2a) = _run_pair({"x": data}, rx_kw=rx_kw)
    assert saved == {"x": data}, (tx, rx)
    if rx_kw.get("escape_ctl"):
        # no bare control characters (headers' CR LF and the XON flow bytes aside)
        body = bytes(a2b.sent).replace(b"\r\x8a", b"").replace(b"\x11", b"")
        assert all((c & 0x60) != 0 or c == 0x18 for c in body)


def test_corrupted_subpacket_recovers_via_zrpos() -> None:
    data = os.urandom(300_000)
    hit = {"done": False}

    def corrupt(chunk: bytes, offset: int) -> bytes:
        target = 150_000
        if not hit["done"] and offset <= target < offset + len(chunk):
            hit["done"] = True
            buf = bytearray(chunk)
            buf[target - offset] ^= 0x01
            return bytes(buf)
        return chunk

    tx, rx, saved, (_a2b, b2a) = _run_pair({"c.bin": data}, s2r=corrupt)
    assert hit["done"]
    assert saved == {"c.bin": data}, (tx, rx)
    zrpos = zp.encode_hex_header(zp.ZRPOS, zp.pos_bytes(0))[:6]  # "**\x18B09"
    assert bytes(b2a.sent).count(zrpos) >= 2  # accept + at least one resend request


def test_receiver_skip() -> None:
    a2b, b2a = _Chan(), _Chan()
    deadline = time.monotonic() + 10
    out: dict = {}
    th = threading.Thread(target=lambda: out.setdefault("rx", zp.receive_files(
        _End(a2b, b2a), lambda n, s: None, deadline=deadline)), daemon=True)
    th.start()
    tx = zp.send_files(_End(b2a, a2b), [_spec("skip.me", b"x" * 5000)], deadline=deadline)
    th.join(10)
    assert tx.files[0].skipped and out["rx"].files == []


# --------------------------------------------------------------------------- #
# scripted peers
# --------------------------------------------------------------------------- #
def test_missing_oo_replay_ends_and_returns_device_text() -> None:
    t = _Scripted([*LSZ_EMPTY, b"sz done rc=0\r\n"])
    saved: dict = {}
    res = zp.receive_files(t, lambda n, s: _Sink(saved, n), deadline=time.monotonic() + 5)
    assert saved == {"empty.bin": b""}
    assert res.tail == b"sz done rc=0\r\n"
    assert zp.encode_hex_header(zp.ZFIN) in bytes(t.written)


def test_missing_oo_and_silence_ends_by_timer() -> None:
    t = _Scripted(list(LSZ_EMPTY))
    t0 = time.monotonic()
    res = zp.receive_files(t, lambda n, s: _Sink({}, n), deadline=t0 + 5)
    assert res.tail == b"" and [f.name for f in res.files] == ["empty.bin"]
    assert time.monotonic() - t0 < 3


def test_oo_then_text_is_tail() -> None:
    t = _Scripted([*LSZ_EMPTY, b"OOsz done rc=0\r\n$ "])  # text already read with OO
    res = zp.receive_files(t, lambda n, s: _Sink({}, n), deadline=time.monotonic() + 5)
    assert res.tail.startswith(b"sz done rc=0\r\n")


def test_peer_cancel_5can_is_clean_error() -> None:
    def respond(t: _Scripted, data: bytes) -> None:
        if data.startswith(b"*\x18") and data[3] == zp.ZFILE:
            t.chunks.append(b"\x18" * 5 + b"\x08" * 5)

    rinit = zp.encode_hex_header(zp.ZRINIT, zp.flags_bytes(zp.CANFDX | zp.CANFC32))
    t = _Scripted([rinit], on_write=respond)
    with pytest.raises(zp.ZmodemError, match="cancel"):
        zp.send_files(t, [_spec("a", b"abc")], deadline=time.monotonic() + 5)

    t2 = _Scripted([LSZ_EMPTY[0], b"\x18" * 8])
    with pytest.raises(zp.ZmodemError, match="cancel"):
        zp.receive_files(t2, lambda n, s: _Sink({}, n), deadline=time.monotonic() + 5)


def test_deadline_expiry_sends_abort() -> None:
    t = _Scripted([])
    t0 = time.monotonic()
    with pytest.raises(zp.ZmodemError, match="timed out"):
        zp.send_files(t, [_spec("a", b"abc")], deadline=t0 + 0.6)
    assert time.monotonic() - t0 < 2
    assert bytes(t.written).endswith(ABORT_SEQ)

    t2 = _Scripted([])
    with pytest.raises(zp.ZmodemError, match="timed out"):
        zp.receive_files(t2, lambda n, s: _Sink({}, n), deadline=time.monotonic() + 0.6)
    assert bytes(t2.written).endswith(ABORT_SEQ)


def test_zfile_info_is_lsz_style() -> None:
    info = zp.file_info(zp.FileSpec(name="测试 file.bin", data=io.BytesIO(b"abc"), size=3,
                                    mtime=0o17), files_left=1, bytes_left=3)
    name, rest, _ = info.split(b"\0")
    assert name == "测试 file.bin".encode() and rest.split()[:2] == [b"3", b"17"]
