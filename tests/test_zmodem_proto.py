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


def _run_pair(files: dict[str, bytes], *, s2r=None, r2s=None, rx_kw=None, tx_kw=None,
              timeout=30.0, chans=None):
    """Python sender -> Python receiver; returns (send_result, recv_result, saved, chans)."""
    a2b, b2a = chans or (_Chan(s2r), _Chan(r2s))
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
                                  deadline=deadline, **(tx_kw or {}))
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

    tx, rx, saved, (a2b, b2a) = _run_pair({"c.bin": data}, s2r=corrupt)
    assert hit["done"]
    assert saved == {"c.bin": data}, (tx, rx)
    rpos = [h.pos for h in _headers(bytes(b2a.sent)) if h.type == zp.ZRPOS]
    assert rpos[0] == 0 and len(rpos) >= 2  # accept, then the resend request
    # the resend offset is a subpacket boundary just before the damaged wire byte ...
    assert rpos[1] % zp.SUBPACKET == 0 and 140_000 <= rpos[1] <= 150_000, rpos
    # ... and the sender restarted exactly there
    assert rpos[1] in [h.pos for h in _headers(bytes(a2b.sent)) if h.type == zp.ZDATA]


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



# --------------------------------------------------------------------------- #
# fix round 1: dead peers, pacing, lossy links
# --------------------------------------------------------------------------- #
def _headers(stream: bytes) -> list:
    """Every valid header in a byte stream (CRC-checked, so data rarely false-matches)."""
    out, buf = [], bytearray(stream)
    while True:
        h, used = zp.parse_header(buf)
        if h is None:
            return out
        del buf[:max(used, 1)]
        if isinstance(h, zp.Header):
            out.append(h)


def test_receiver_cancel_writes_nothing() -> None:
    """lsz cancelling a missing file: nothing of ours may reach the device's shell."""
    t = _Scripted([zp.encode_hex_header(zp.ZRQINIT) + b"sz: cannot open nope.bin\r\n"
                   + b"\x18" * 10 + b"\x08" * 10 + b"sz done rc=128\r\n"])
    with pytest.raises(zp.ZmodemError) as ei:
        zp.receive_files(t, lambda n, s: None, deadline=time.monotonic() + 3)
    assert ei.value.cancelled
    assert bytes(t.written) == b""
    assert b"cannot open nope.bin" in ei.value.before
    assert ei.value.after.startswith(b"sz done rc=128")


def test_receiver_cancel_in_next_read_writes_nothing() -> None:
    t = _Scripted([zp.encode_hex_header(zp.ZRQINIT), b"\x18" * 10 + b"\x08" * 10])
    with pytest.raises(zp.ZmodemError, match="cancel"):
        zp.receive_files(t, lambda n, s: None, deadline=time.monotonic() + 3)
    assert bytes(t.written) == b""


def test_receiver_text_instead_of_protocol_is_peer_gone() -> None:
    t = _Scripted([zp.encode_hex_header(zp.ZRQINIT), b"Segmentation fault\r\n# "])
    with pytest.raises(zp.ZmodemError) as ei:
        zp.receive_files(t, lambda n, s: None, deadline=time.monotonic() + 3)
    assert ei.value.peer_gone and bytes(t.written) == b""
    assert b"Segmentation fault" in ei.value.before


def test_sender_text_instead_of_zrinit_is_peer_gone() -> None:
    t = _Scripted([b"rz: not found\r\n# "])
    with pytest.raises(zp.ZmodemError) as ei:
        zp.send_files(t, [_spec("a", b"abc")], deadline=time.monotonic() + 5)
    assert ei.value.peer_gone and bytes(t.written) == b""


def test_abort_on_error_false_writes_no_abort_seq() -> None:
    t = _Scripted([])
    with pytest.raises(zp.ZmodemError) as ei:
        zp.send_files(t, [_spec("a", b"abc")], deadline=time.monotonic() + 0.5,
                      abort_on_error=False)
    assert ei.value.timed_out
    assert ABORT_SEQ not in bytes(t.written)


def test_deadline_callable_is_reread() -> None:
    box = {"dl": time.monotonic() + 0.3}
    t = _Scripted([])
    t0 = time.monotonic()

    def later() -> float:
        if time.monotonic() - t0 > 0.2:
            box["dl"] = t0 + 1.2  # extended once
        return box["dl"]

    with pytest.raises(zp.ZmodemError, match="timed out"):
        zp.send_files(t, [_spec("a", b"abc")], deadline=later)
    assert 1.1 < time.monotonic() - t0 < 2.0


def test_pacing_from_baud() -> None:
    assert zp.pacing(None) == (8192, 32768)
    assert zp.pacing(115200) == (8192, 32768)
    assert zp.pacing(9600) == (1024, 4096)
    assert zp.pacing(2400) == (1024, 4096)
    assert zp.pacing(38400) == (3840, 15360)


def test_overwrite_sets_zmclob() -> None:
    rinit = zp.encode_hex_header(zp.ZRINIT, zp.flags_bytes(zp.CANFDX | zp.CANFC32))
    for overwrite, zf1 in ((True, zp.ZMCLOB), (False, 0)):
        def respond(t, data):
            if data.startswith(b"*\x18C") and data[3] == zp.ZFILE:
                t.chunks.append(zp.encode_hex_header(zp.ZSKIP))
            elif data.startswith(b"**\x18B08"):
                t.chunks.append(zp.encode_hex_header(zp.ZFIN))
        t = _Scripted([rinit], on_write=respond)
        res = zp.send_files(t, [_spec("a", b"abc")], deadline=time.monotonic() + 5,
                            overwrite=overwrite)
        assert res.files[0].skipped
        zfile = [h for h in _headers(bytes(t.written)) if h.type == zp.ZFILE][0]
        assert zfile.data[2] == zf1 and zfile.data[3] == zp.ZCBIN


def test_parse_header_short_non_hex_keeps_later_header() -> None:
    real = zp.encode_hex_header(zp.ZRINIT, zp.flags_bytes(zp.CANFDX))
    buf = bytearray(b"**\x18Bzz" + real)
    h, used = zp.parse_header(buf)
    assert h is zp.GARBLED and used <= 4
    del buf[:used]
    h, _ = zp.parse_header(buf)
    assert isinstance(h, zp.Header) and h.type == zp.ZRINIT


class _Line:
    """A serial line of ``rate`` bytes/s behind an unbounded host-side pipe (daemon/ws/TCP)."""

    def __init__(self, rate: int) -> None:
        self.q = bytearray()
        self.out: deque[bytes] = deque()
        self.cv = threading.Condition()
        self.rate = rate
        self.sent = 0
        threading.Thread(target=self._run, daemon=True).start()

    def put(self, data: bytes) -> None:
        with self.cv:
            self.q += data
            self.sent += len(data)
            self.cv.notify_all()

    def _run(self) -> None:
        step = max(1, self.rate // 50)
        due = time.monotonic()
        while True:
            with self.cv:
                if not self.q:
                    while not self.q:
                        self.cv.wait()
                    due = time.monotonic()  # line was idle: start timing afresh
                chunk = bytes(self.q[:step])
                del self.q[:step]
            due += len(chunk) / self.rate
            time.sleep(max(0.0, due - time.monotonic()))
            with self.cv:
                self.out.append(chunk)
                self.cv.notify_all()

    def get(self, timeout: float) -> bytes:
        end = time.monotonic() + timeout
        with self.cv:
            while not self.out and time.monotonic() < end:
                self.cv.wait(max(0.0, end - time.monotonic()))
            data = b"".join(self.out)
            self.out.clear()
            return data


# The pacing comes from ``baud`` (9600 → 1 KiB / 4 KiB window, also at 4800); the line runs
# somewhat faster than that so each case takes ~2 s, and the file is larger than the window.
@pytest.mark.parametrize("baud,rate,size", [(9600, 3200, 6000), (4800, 2400, 4300)])
def test_throttled_line_completes_without_flooding(baud, rate, size) -> None:
    """Low baud + deep pipe: no blind resend into a full pipe; wire bytes stay ~ the file."""
    data = os.urandom(size)
    tx, rx, saved, sent, took = _throttled_pair(rate, data, {"baud": baud, "frame_timeout": 0.5},
                                                {"frame_timeout": 0.5}, 20)
    assert saved == {"x": data}, (tx, rx)
    escaped = len(zp.zdle_escape(data))
    assert sent <= 1.2 * escaped, (sent, escaped)
    assert took < 3.5 * escaped / rate + 1, took   # ≥ 3× headroom over the line time


def _drop_once(match: Callable[[bytes, int], Optional[tuple[int, int]]]):
    """Mangle that removes ``[a, b)`` of the first chunk ``match`` selects."""
    state = {"done": False}

    def mangle(chunk: bytes, offset: int) -> bytes:
        if state["done"]:
            return chunk
        cut = match(chunk, offset)
        if cut is None:
            return chunk
        state["done"] = True
        return chunk[:cut[0]] + chunk[cut[1]:]

    mangle.state = state  # type: ignore[attr-defined]
    return mangle


def test_dropped_bytes_recover() -> None:
    data = os.urandom(200_000)
    m = _drop_once(lambda c, off: (len(c) // 4, len(c) // 2)
                   if off + len(c) > 100_000 and len(c) > 500 else None)
    tx, rx, saved, _ = _run_pair({"d": data}, s2r=m)
    assert m.state["done"] and saved == {"d": data}, (tx, rx)


def test_corrupted_zdata_header_recovers() -> None:
    data = os.urandom(50_000)

    def is_zdata(c: bytes, off: int):
        if c.startswith(b"*\x18C") and c[3] == zp.ZDATA:
            return (5, 6)  # drop one byte of the position field
        return None

    m = _drop_once(is_zdata)
    tx, rx, saved, _ = _run_pair({"h": data}, s2r=m)
    assert m.state["done"] and saved == {"h": data}, (tx, rx)


def test_lost_zacks_recover() -> None:
    """Every ZACK of the first window is lost: the sender syncs instead of flooding."""
    data = os.urandom(120_000)
    lost = {"n": 0}

    def drop_acks(chunk: bytes, offset: int) -> bytes:
        if chunk.startswith(b"**\x18B03") and lost["n"] < 4:
            lost["n"] += 1
            return b""
        return chunk

    tx, rx, saved, (a2b, _b2a) = _run_pair({"k": data}, r2s=drop_acks,
                                           tx_kw={"frame_timeout": 1.0},
                                           rx_kw={"frame_timeout": 1.0})
    assert lost["n"] == 4 and saved == {"k": data}, (tx, rx)
    assert len(a2b.sent) < 1.2 * len(zp.zdle_escape(data))


def test_lost_zrpos_recovers() -> None:
    data = os.urandom(120_000)
    hit = {"flip": False, "lost": False}

    def corrupt(chunk: bytes, offset: int) -> bytes:
        if not hit["flip"] and offset <= 60_000 < offset + len(chunk):
            hit["flip"] = True
            b = bytearray(chunk)
            b[60_000 - offset] ^= 0x01
            return bytes(b)
        return chunk

    def drop_first_resend(chunk: bytes, offset: int) -> bytes:
        if chunk.startswith(b"**\x18B09") and offset > 0 and not hit["lost"]:
            hit["lost"] = True
            return b""
        return chunk

    tx, rx, saved, _ = _run_pair({"r": data}, s2r=corrupt, r2s=drop_first_resend,
                                 tx_kw={"frame_timeout": 1.0}, rx_kw={"frame_timeout": 1.0})
    assert hit["flip"] and hit["lost"] and saved == {"r": data}, (tx, rx)


def test_repeated_zrpos_falls_back_to_zcrcw_segments() -> None:
    data = os.urandom(200_000)
    flips = [30_000, 70_000, 110_000, 150_000]

    def corrupt(chunk: bytes, offset: int) -> bytes:
        b = bytearray(chunk)
        for f in list(flips):
            if offset <= f < offset + len(chunk):
                b[f - offset] ^= 0x01
                flips.remove(f)
        return bytes(b)

    tx, rx, saved, (a2b, _b2a) = _run_pair({"z": data}, s2r=corrupt)
    assert not flips and saved == {"z": data}, (tx, rx)
    tail = bytes(a2b.sent)[-60_000:]
    assert tail.count(b"\x18k") >= 5  # ZCRCW-acknowledged segments after the 3rd ZRPOS


def test_hex_header_with_xon_xoff_inside() -> None:
    raw = zp.encode_hex_header(zp.ZACK, zp.pos_bytes(0x8000))
    noisy = bytearray(raw[:8] + b"\x13\x11" + raw[8:12] + b"\x11" + raw[12:])
    h, used = zp.parse_header(noisy)
    assert isinstance(h, zp.Header) and (h.type, h.pos) == (zp.ZACK, 0x8000)
    assert not zp._looks_like_text(b"\r\x8a\x11")


# --------------------------------------------------------------------------- #
# fix round 2
# --------------------------------------------------------------------------- #
class _Timed(_Scripted):
    """A peer that answers each of our writes with the next scripted reply (``first`` is there
    from the start) and notes how many bytes we had written when ``marker`` was read."""

    def __init__(self, first: bytes, replies: list[bytes], marker: bytes) -> None:
        super().__init__([first], on_write=lambda t, d: t.replies and t.chunks.append(
            t.replies.pop(0)))
        self.replies = list(replies)
        self.marker = marker
        self.written_at_marker: Optional[int] = None

    def read(self, timeout: float) -> bytes:
        data = super().read(timeout)
        if self.marker in data and self.written_at_marker is None:
            self.written_at_marker = len(self.written)
        return data


def test_receiver_sender_dies_mid_file_writes_nothing_after_text() -> None:
    """lsz killed mid-file (no CAN): the shell's text ends the session, no ZRPOS to it."""
    info = zp.file_info(zp.FileSpec("f.bin", io.BytesIO(b""), 4096), 1, 4096)
    t = _Timed(zp.encode_hex_header(zp.ZRQINIT), [
        zp.encode_bin_header(zp.ZFILE, bytes([0, 0, 0, 1]), use32=True)
        + zp.encode_subpacket(info, zp.ZCRCW, use32=True),                 # → our ZRINIT
        zp.encode_bin_header(zp.ZDATA, zp.pos_bytes(0), use32=True)         # → our ZRPOS
        + zp.encode_subpacket(b"A" * 1024, zp.ZCRCG, use32=True) + b"Killed\r\n# "],
        b"Killed")
    saved: dict = {}
    with pytest.raises(zp.ZmodemError) as ei:
        zp.receive_files(t, lambda n, s: _Sink(saved, n), deadline=time.monotonic() + 6,
                         frame_timeout=0.5, abort_on_error=False)
    assert ei.value.peer_gone and not ei.value.timed_out, ei.value
    assert t.written_at_marker is not None
    assert len(t.written) == t.written_at_marker, bytes(t.written[t.written_at_marker:])
    assert b"Killed" in ei.value.before


def test_receiver_sender_dies_between_files_writes_nothing_after_text() -> None:
    t = _Timed(LSZ_EMPTY[0], [LSZ_EMPTY[1], LSZ_EMPTY[2], b"Segmentation fault\r\n# "],
               b"Segmentation")
    with pytest.raises(zp.ZmodemError) as ei:
        zp.receive_files(t, lambda n, s: _Sink({}, n), deadline=time.monotonic() + 6,
                         frame_timeout=0.5, abort_on_error=False)
    assert ei.value.peer_gone, ei.value
    assert t.written_at_marker is not None
    assert len(t.written) == t.written_at_marker


def test_receiving_a_text_file_is_not_peer_gone() -> None:
    """File data that looks like shell text must not be mistaken for a dead sender."""
    data = b"Killed\r\n# ls\r\nroot@dev:~# echo hi\r\n" * 2000
    tx, rx, saved, _ = _run_pair({"log.txt": data})
    assert saved == {"log.txt": data}, (tx, rx)


def _throttled_pair(rate: int, data: bytes, tx_kw: dict, rx_kw: dict, timeout: float):
    """Python sender → receiver over a ``rate`` bytes/s line (both ways) behind deep pipes."""
    line_tx, line_rx = _Line(rate), _Line(rate)
    a2b, b2a = _Chan(), _Chan()
    a2b.put, a2b.get = line_tx.put, line_tx.get  # type: ignore[method-assign]
    b2a.put, b2a.get = line_rx.put, line_rx.get  # type: ignore[method-assign]
    t0 = time.monotonic()
    tx, rx, saved, _ = _run_pair({"x": data}, chans=(a2b, b2a), timeout=timeout,
                                 tx_kw=tx_kw, rx_kw=rx_kw)
    return tx, rx, saved, line_tx.sent, time.monotonic() - t0


def test_sender_adapts_when_the_configured_baud_overstates_the_line() -> None:
    """socket://, USB-CDC, ser2net, flow-controlled flash: ACKs come slower than the configured
    baud predicts. Real line 960 B/s ("9600 baud", as rr2/t2c.py), default frame timeout,
    configured baud 12× and 120× the real rate, and unknown: the sender measures the ACK rate
    and paces / waits by it — byte-exact, and hardly anything is sent twice.

    The three cases run at once (each is ~38 s of line time)."""
    size = 36_000                      # > the 32 KiB window a 115200-or-faster baud gives
    cases = {"12x": 12 * 9600, "120x": 120 * 9600, "unknown": None}
    res: dict = {}

    def run(label, nominal):
        data = os.urandom(size)
        tx, rx, saved, sent, took = _throttled_pair(960, data, {"baud": nominal}, {}, 120)
        res[label] = (saved == {"x": data}, sent / len(zp.zdle_escape(data)), took, tx, rx)

    threads = [threading.Thread(target=run, args=kv, daemon=True) for kv in cases.items()]
    for th in threads:
        th.start()
    for th in threads:
        th.join(130)
    for label in cases:
        ok, ratio, took, tx, rx = res[label]
        assert ok, (label, tx, rx)
        assert ratio <= 1.2, (label, round(ratio, 2))
        assert took < 1.3 * size / 960 + 5, (label, round(took, 1))


def test_text_after_xon_xoff_noise_is_still_text() -> None:
    """lrz's hex headers end with a tty's XOFF/XON (``\\r\\x8a\\x13\\x11``); those remnants pile
    up in front of the shell line printed after the receiver died (seen in the sim: no CAN,
    then ``rz done rc=143`` — the sender must stop rather than sync into the shell)."""
    assert zp._looks_like_text(b"\x13\x11" * 200 + b"rz done rc=143\r\n")
    acks = b"".join(zp.encode_hex_header(zp.ZACK, zp.pos_bytes(p))[:-1] + b"\x8a\x13\x11"
                    for p in range(1024, 9 * 1024, 1024))
    rinit = zp.encode_hex_header(zp.ZRINIT, zp.flags_bytes(zp.CANFDX | zp.CANFC32))
    t = _Timed(rinit, [zp.encode_hex_header(zp.ZRPOS), acks + b"rz done rc=143\r\n"], b"rz done")
    with pytest.raises(zp.ZmodemError) as ei:
        zp.send_files(t, [_spec("big", os.urandom(64 * 1024))], deadline=time.monotonic() + 20,
                      baud=9600, frame_timeout=1.0, abort_on_error=False)
    assert ei.value.peer_gone, ei.value
    assert len(t.written) == t.written_at_marker
