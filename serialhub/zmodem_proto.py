"""纯 Python ZMODEM 引擎（发送方 + 接收方），不含任何 I/O。

在抽象字节传输（:class:`Transport`：``read(timeout) -> bytes``，超时返回 ``b""``；
``write(data)``）之上实现 ZMODEM，供 ``zmodem_client.py`` 经 daemon 的 ``/ws`` 中继
与设备上的 lrzsz（或嵌入式 rz/sz）对传。要点：

- 帧：十六进制头（``**`` ZDLE ``B``）、二进制头 CRC-16（``*`` ZDLE ``A``）/ CRC-32
  （``*`` ZDLE ``C``）；数据子包 ≤ 1024 字节，ZDLE 转义（ZDLE、DLE、XON/XOFF 及其 8 位
  形式、CR；接收方要求 ``ESCCTL`` 时转义全部控制字符），收方丢弃未转义的 XON/XOFF。
- 发送方：接收方 ``CANFC32`` 时用 CRC-32；全双工且缓冲为 0 时流式发送 ZCRCG，每 8 KiB
  一个 ZCRCQ 要 ZACK，未确认数据不超过 :data:`WINDOW`；接收方声明了缓冲长度（或不能全双工）
  时按缓冲分段、段尾 ZCRCW 等 ZACK。随时**服从 ZRPOS**（回到该偏移重发）。
- 接收方：通告 ``CANFDX|CANOVIO|CANFC32``、缓冲 0；CRC 错 / 位置不符 → ZRPOS 并丢弃数据直到
  下一个头；ZCRCW/ZCRCQ → ZACK（lsz 每个文件第一个子包以 ZCRCW 结束，见 zmodem-shim.js）；
  ZFIN 后 ``OO`` 可选（lsz 有时不发）：非 ``OO`` 的字节即会话外文本，静默 1 s 也结束。
- 对端 5×CAN → :class:`ZmodemError`（``cancelled``）；自身失败（超时、重试耗尽）先写
  ``ABORT_SEQ`` 再抛 :class:`ZmodemError`。每帧接收有超时、重试有上限，另有总截止时间。
- 会话结束后读到、但不属于 ZMODEM 的字节（设备打印的 ``rz done``、提示符）放在
  :attr:`SessionResult.tail`，由调用方交回 daemon（``zmodem_end.tail_b64``）。
"""

from __future__ import annotations

import binascii
import re
import time
import zlib
from dataclasses import dataclass, field
from typing import BinaryIO, Callable, Optional, Protocol, Union

from .zmodem import ABORT_SEQ

__all__ = [
    "CANFC32", "CANFDX", "CANOVIO", "ESCCTL", "GARBLED", "SUBPACKET", "WINDOW",
    "ZACK", "ZBIN", "ZBIN32", "ZCRCE", "ZCRCG", "ZCRCQ", "ZCRCW", "ZDATA", "ZEOF", "ZFILE",
    "ZFIN", "ZHEX", "ZNAK", "ZRINIT", "ZRPOS", "ZRQINIT", "ZSKIP",
    "FileResult", "FileSpec", "Header", "SessionResult", "Sink", "Transport", "ZmodemError",
    "crc16", "crc32", "encode_bin_header", "encode_hex_header", "encode_subpacket",
    "file_info", "flags_bytes", "parse_header", "parse_subpacket", "pos_bytes",
    "receive_files", "send_files", "zdle_escape", "zdle_unescape",
]

# --------------------------------------------------------------------------- #
# 常量
# --------------------------------------------------------------------------- #
ZPAD, ZDLE = 0x2A, 0x18
ZBIN, ZHEX, ZBIN32 = 0x41, 0x42, 0x43          # 'A' 'B' 'C'

ZRQINIT, ZRINIT, ZSINIT, ZACK, ZFILE, ZSKIP, ZNAK, ZABORT = range(8)
ZFIN, ZRPOS, ZDATA, ZEOF, ZFERR, ZCRC, ZCHALLENGE, ZCOMPL = range(8, 16)
ZCAN, ZFREECNT, ZCOMMAND, ZSTDERR = range(16, 20)

ZCRCE, ZCRCG, ZCRCQ, ZCRCW = 0x68, 0x69, 0x6A, 0x6B   # 'h' 'i' 'j' 'k'
ZRUB0, ZRUB1 = 0x6C, 0x6D                             # 'l' → 0x7f, 'm' → 0xff

# ZRINIT ZF0 能力位
CANFDX, CANOVIO, CANBRK, CANFC32, ESCCTL = 0x01, 0x02, 0x04, 0x20, 0x40
ZCBIN = 1                                             # ZFILE ZF0：二进制传输

SUBPACKET = 1024            #: 数据子包长度（标准 1 KiB）
ACK_EVERY = 8 * 1024        #: 流式发送时每这么多字节发一个 ZCRCQ（要 ZACK）
WINDOW = 32 * 1024          #: 流式发送时未确认数据上限
MAX_SUBPACKET = 8192        #: 接收时接受的最大子包（ZMODEM-8K）
FRAME_TIMEOUT = 10.0        #: 单帧接收超时（秒）；lrz 自身超时 10 s，daemon 空闲 15 s
FIRST_TIMEOUT = 2.0         #: 首个 ZRINIT/ZRQINIT 的等待（之后改发 ZRQINIT/ZRINIT 催促）
OO_WAIT = 1.0               #: ZFIN 之后等 "OO" 的时长（lsz 可能不发）
RETRIES = 10                #: 同一步骤的连续重试上限
ERRORS_MAX = 20             #: 单个文件内 ZRPOS 重发请求上限（同 lrz）

_CAN5 = b"\x18" * 5
_XONOFF = b"\x11\x13\x91\x93"
_HEXDIGITS = frozenset(b"0123456789abcdefABCDEF")


class ZmodemError(Exception):
    """会话失败；``files`` = 失败前已完成的文件结果；``cancelled`` = 对端发了 5×CAN。"""

    def __init__(self, message: str, files: Optional[list["FileResult"]] = None,
                 cancelled: bool = False, tail: bytes = b"") -> None:
        super().__init__(message)
        self.files: list[FileResult] = list(files or [])
        self.cancelled = cancelled
        self.tail = tail        #: 取消时与 CAN 同批到达、在 CAN/BS 之后的字节（设备文本）


class _Cancelled(ZmodemError):
    """对端发来 5×CAN。"""

    def __init__(self, message: str, tail: bytes = b"") -> None:
        super().__init__(message, cancelled=True, tail=tail)


class Transport(Protocol):
    def read(self, timeout: float) -> bytes: ...   # 超时返回 b""
    def write(self, data: bytes) -> None: ...


class Sink(Protocol):
    """接收文件的落点：``write`` 追加字节，``close`` 表示文件已完整收到。"""

    def write(self, data: bytes) -> object: ...
    def close(self) -> None: ...


@dataclass
class FileSpec:
    """待发送文件：``data`` 须可 ``seek``（服从 ZRPOS）。"""

    name: str
    data: BinaryIO
    size: int
    mtime: int = 0
    mode: int = 0o100644


@dataclass
class FileResult:
    name: str
    size: int
    crc32: int
    seconds: float
    skipped: bool = False


@dataclass
class SessionResult:
    files: list[FileResult] = field(default_factory=list)
    tail: bytes = b""


Progress = Optional[Callable[[str, int, Optional[int]], None]]


# --------------------------------------------------------------------------- #
# CRC / 转义 / 帧编码
# --------------------------------------------------------------------------- #
def crc16(data: bytes, crc: int = 0) -> int:
    """CRC-16/XMODEM（poly 0x1021，init 0）。"""
    return binascii.crc_hqx(data, crc)


def crc32(data: bytes, crc: int = 0) -> int:
    """CRC-32（与 zlib 相同；ZMODEM 以小端发送）。"""
    return zlib.crc32(data, crc) & 0xFFFFFFFF


_ESC_STD = rb"[\x18\x10\x11\x13\x90\x91\x93\r\x8d]"
_ESC_CTL = rb"[\x00-\x1f\x80-\x9f]"
_RE_STD = re.compile(_ESC_STD)
_RE_CTL = re.compile(_ESC_CTL)
_ESCAPED = [bytes([ZDLE, b ^ 0x40]) for b in range(256)]


def _esc_sub(m: "re.Match[bytes]") -> bytes:
    return _ESCAPED[m.group()[0]]


def zdle_escape(data: bytes, escape_ctl: bool = False) -> bytes:
    """ZDLE 转义；``escape_ctl`` 时转义全部控制字符（接收方 ESCCTL）。"""
    return (_RE_CTL if escape_ctl else _RE_STD).sub(_esc_sub, data)


def zdle_unescape(data: bytes) -> bytes:
    """逆转义（测试用；无帧结束标记）。"""
    status, out, nxt = _take(bytearray(data), 0, None)
    if status != "ok":
        raise ValueError("bad ZDLE escape")
    return bytes(out)


def pos_bytes(pos: int) -> bytes:
    return (pos & 0xFFFFFFFF).to_bytes(4, "little")


def flags_bytes(zf0: int, bufsize: int = 0) -> bytes:
    """ZRINIT 头数据：ZP0/ZP1 = 接收缓冲长度（0 = 全流式），ZF0 = 能力位。"""
    return bytes([bufsize & 0xFF, (bufsize >> 8) & 0xFF, 0, zf0 & 0xFF])


def encode_hex_header(ftype: int, data: bytes = b"\0\0\0\0") -> bytes:
    raw = bytes([ftype]) + data
    out = b"**\x18B" + binascii.hexlify(raw + crc16(raw).to_bytes(2, "big")) + b"\r\x8a"
    return out if ftype in (ZACK, ZFIN) else out + b"\x11"


def encode_bin_header(ftype: int, data: bytes, *, use32: bool, escape_ctl: bool = False) -> bytes:
    raw = bytes([ftype]) + data
    if use32:
        return b"*\x18C" + zdle_escape(raw + crc32(raw).to_bytes(4, "little"), escape_ctl)
    return b"*\x18A" + zdle_escape(raw + crc16(raw).to_bytes(2, "big"), escape_ctl)


def encode_subpacket(data: bytes, end: int, *, use32: bool, escape_ctl: bool = False) -> bytes:
    tail = bytes([end])
    if use32:
        crc = crc32(tail, crc32(data)).to_bytes(4, "little")
    else:
        crc = crc16(tail, crc16(data)).to_bytes(2, "big")
    out = zdle_escape(data, escape_ctl) + bytes([ZDLE, end]) + zdle_escape(crc, escape_ctl)
    return out + b"\x11" if end == ZCRCW else out


@dataclass(frozen=True)
class Header:
    type: int
    data: bytes
    use32: bool = False     #: 头是 ZBIN32：随后的数据子包用 CRC-32

    @property
    def pos(self) -> int:
        return int.from_bytes(self.data, "little")

    @property
    def flags(self) -> int:  # ZF0
        return self.data[3]

    @property
    def bufsize(self) -> int:  # ZRINIT ZP0/ZP1
        return self.data[0] | (self.data[1] << 8)


class _Garbled:
    def __repr__(self) -> str:
        return "GARBLED"


#: 帧头存在但 CRC/格式错误。
GARBLED = _Garbled()


def _take(buf: bytearray, i: int, count: Optional[int]) -> tuple[str, bytearray, int]:
    """从 ``buf[i:]`` 逆转义 ``count`` 个字节（None = 到末尾）。→ (ok|need|bad, 字节, 下一位置)。

    未转义的 XON/XOFF 丢弃；遇帧结束标记 → bad（头与 CRC 内不允许）。
    """
    out = bytearray()
    n = len(buf)
    while count is None or len(out) < count:
        if i >= n:
            return ("ok" if count is None else "need"), out, i
        c = buf[i]
        if c in _XONOFF:
            i += 1
            continue
        if c != ZDLE:
            out.append(c)
            i += 1
            continue
        if i + 1 >= n:
            return "need", out, i
        d = buf[i + 1]
        if d == ZRUB0:
            out.append(0x7F)
        elif d == ZRUB1:
            out.append(0xFF)
        elif (d & 0x60) == 0x40:
            out.append(d ^ 0x40)
        else:
            return "bad", out, i + 2
        i += 2
    return "ok", out, i


def parse_header(buf: bytearray) -> tuple[Union[Header, _Garbled, None], int]:
    """在 ``buf`` 里找下一个帧头。→ ``(Header | GARBLED | None, 可丢弃的字节数)``。

    ``None`` = 需要更多数据（``consumed`` 只计候选头之前的垃圾）；十六进制头之后的
    CR/LF/XON 若已到达一并吃掉。
    """
    n = len(buf)
    i = 0
    while True:
        j = buf.find(b"*", i)
        if j < 0:
            return None, n
        k = j
        while k < n and buf[k] == ZPAD:
            k += 1
        if k >= n:
            return None, j
        if buf[k] != ZDLE:
            i = k
            continue
        if k + 1 >= n:
            return None, j
        kind = buf[k + 1]
        if kind == ZHEX:
            end = k + 2 + 14
            if n < end:
                if not all(c in _HEXDIGITS for c in buf[k + 2:n]):
                    return GARBLED, n
                return None, j
            digits = bytes(buf[k + 2:end])
            if not all(c in _HEXDIGITS for c in digits):
                return GARBLED, k + 2
            raw = binascii.unhexlify(digits)
            if crc16(raw[:5]) != int.from_bytes(raw[5:], "big"):
                return GARBLED, end
            for skip in (b"\r", b"\n\x8a", b"\x11"):
                if end < n and buf[end] in skip:
                    end += 1
            return Header(raw[0], raw[1:5]), end
        if kind in (ZBIN, ZBIN32):
            want = 9 if kind == ZBIN32 else 7
            status, raw, end = _take(buf, k + 2, want)
            if status == "need":
                return None, j
            if status == "bad":
                return GARBLED, end
            if kind == ZBIN32:
                ok = crc32(bytes(raw[:5])) == int.from_bytes(raw[5:9], "little")
            else:
                ok = crc16(bytes(raw[:5])) == int.from_bytes(raw[5:7], "big")
            if not ok:
                return GARBLED, end
            return Header(raw[0], bytes(raw[1:5]), kind == ZBIN32), end
        i = k + 1


def parse_subpacket(buf: bytearray, start: int, *, use32: bool,
                    limit: int = MAX_SUBPACKET) -> tuple[str, bytes, int, int]:
    """解析 ``buf[start:]`` 处的数据子包。→ ``(ok|need|bad, 数据, 帧结束类型, 下一位置)``。"""
    out = bytearray()
    i = start
    n = len(buf)
    while True:
        j = buf.find(b"\x18", i)
        if j < 0:
            return "need", b"", 0, start
        out += buf[i:j].translate(None, _XONOFF)
        if len(out) > limit:
            return "bad", b"", 0, j
        if j + 1 >= n:
            return "need", b"", 0, start
        d = buf[j + 1]
        if ZCRCE <= d <= ZCRCW:
            status, crc, nxt = _take(buf, j + 2, 4 if use32 else 2)
            if status != "ok":
                return ("need" if status == "need" else "bad"), b"", 0, (start if status == "need" else nxt)
            tail = bytes([d])
            if use32:
                good = crc32(tail, crc32(out)) == int.from_bytes(crc, "little")
            else:
                good = crc16(tail, crc16(out)) == int.from_bytes(crc, "big")
            return ("ok" if good else "bad"), bytes(out), d, nxt
        if d == ZRUB0:
            out.append(0x7F)
        elif d == ZRUB1:
            out.append(0xFF)
        elif (d & 0x60) == 0x40:
            out.append(d ^ 0x40)
        else:
            return "bad", b"", 0, j + 2
        i = j + 2


def file_info(spec: FileSpec, files_left: int, bytes_left: int) -> bytes:
    """ZFILE 子包内容（同 lsz）：``name NUL "size mtime(八进制) mode(八进制) 0 剩余文件 剩余字节" NUL``。"""
    meta = f"{spec.size} {spec.mtime:o} {spec.mode:o} 0 {files_left} {bytes_left}"
    return spec.name.encode("utf-8") + b"\0" + meta.encode("ascii") + b"\0"


# --------------------------------------------------------------------------- #
# 链路：缓冲 + 超时 + 5×CAN 检测
# --------------------------------------------------------------------------- #
class _Link:
    def __init__(self, t: Transport, deadline: float) -> None:
        self.t = t
        self.deadline = deadline
        self.buf = bytearray()
        self._recent = b""
        self.use32 = False          # 最近一个二进制头的 CRC 类型（决定其后子包的 CRC）

    def write(self, data: bytes) -> None:
        self.t.write(data)

    def _fill(self, until: float) -> bool:
        now = time.monotonic()
        if now >= self.deadline:
            raise ZmodemError("timed out (overall deadline)")
        data = self.t.read(max(0.0, min(until, self.deadline) - now))
        if not data:
            if time.monotonic() >= self.deadline:
                raise ZmodemError("timed out (overall deadline)")
            return False
        probe = self._recent + data
        if _CAN5 in probe:
            after = probe[probe.index(_CAN5):].lstrip(b"\x18\x08")
            raise _Cancelled("cancelled by peer (CAN)", after)
        self._recent = probe[-4:]
        self.buf += data
        return True

    def header(self, timeout: float) -> Union[Header, _Garbled, None]:
        """下一个帧头；``None`` = 超时（``timeout=0`` 为非阻塞轮询）。"""
        until = time.monotonic() + timeout
        while True:
            res, used = parse_header(self.buf)
            if used:
                del self.buf[:used]
            if res is not None:
                if isinstance(res, Header) and res.type in (ZFILE, ZDATA, ZSINIT, ZCOMMAND):
                    self.use32 = res.use32
                return res
            if not self._fill(until) and time.monotonic() >= until:
                return None

    def subpacket(self, timeout: float, use32: bool) -> Union[tuple[bytes, int], _Garbled, None]:
        """下一个数据子包 ``(数据, 帧结束类型)``；``GARBLED`` = CRC/转义错；``None`` = 超时。"""
        until = time.monotonic() + timeout
        while True:
            status, data, end, nxt = parse_subpacket(self.buf, 0, use32=use32)
            if status == "ok":
                del self.buf[:nxt]
                return data, end
            if status == "bad":
                del self.buf[:nxt]
                return GARBLED
            if not self._fill(until) and time.monotonic() >= until:
                return None

    def tail(self) -> bytes:
        """会话外字节：去掉开头残留的帧尾 CR/LF/XON。"""
        out = bytes(self.buf)
        self.buf.clear()
        return out.lstrip(b"\r\n\x8a\x11")


def _abort(link: _Link) -> None:
    try:
        link.write(ABORT_SEQ)
    except Exception:  # noqa: BLE001 - best-effort
        pass


# --------------------------------------------------------------------------- #
# 发送方
# --------------------------------------------------------------------------- #
class _Sender:
    def __init__(self, link: _Link, progress: Progress, frame_timeout: float) -> None:
        self.link = link
        self.progress = progress
        self.ft = frame_timeout
        self.use32 = False
        self.ctl = False
        self.seg = 0                # >0：分段模式（每段 ZCRCW 等 ZACK）
        self.chunk = SUBPACKET

    # -- helpers -------------------------------------------------------------
    def _hex(self, ftype: int, data: bytes = b"\0\0\0\0") -> None:
        self.link.write(encode_hex_header(ftype, data))

    def _bin(self, ftype: int, data: bytes) -> None:
        self.link.write(encode_bin_header(ftype, data, use32=self.use32, escape_ctl=self.ctl))

    def _sub(self, data: bytes, end: int) -> None:
        self.link.write(encode_subpacket(data, end, use32=self.use32, escape_ctl=self.ctl))

    # -- steps ---------------------------------------------------------------
    def handshake(self) -> None:
        tries = 0
        timeout = FIRST_TIMEOUT
        while True:
            h = self.link.header(timeout)
            if isinstance(h, Header) and h.type == ZRINIT:
                break
            if isinstance(h, Header) and h.type == ZCHALLENGE:
                self._hex(ZACK, h.data)
                continue
            if isinstance(h, Header) and h.type == ZRQINIT:
                raise ZmodemError("peer is a sender (ZRQINIT), expected a receiver")
            if isinstance(h, Header):
                continue
            tries += 1
            if tries > RETRIES:
                raise ZmodemError("receiver never sent ZRINIT")
            self._hex(ZRQINIT)
            timeout = self.ft
        # 认领前积压的重复 ZRINIT 一并吃掉，免得把它们当成“没收到 ZFILE”
        while isinstance(nxt := self.link.header(0.05), Header) and nxt.type == ZRINIT:
            h = nxt
        flags = h.flags
        self.use32 = bool(flags & CANFC32)
        self.ctl = bool(flags & ESCCTL)
        bufsize = h.bufsize
        if bufsize:
            self.seg = bufsize
            self.chunk = max(64, min(SUBPACKET, bufsize))
        elif not flags & CANFDX:
            self.seg = WINDOW

    def send_file(self, spec: FileSpec, files_left: int, bytes_left: int) -> FileResult:
        t0 = time.monotonic()
        spec.data.seek(0)
        crc = 0
        while block := spec.data.read(1 << 16):
            crc = crc32(block, crc)
        info = file_info(spec, files_left, bytes_left)
        start = self._offer(spec, info, crc)
        done = start is not None and self._data(spec, start)
        return FileResult(spec.name, spec.size, crc, time.monotonic() - t0, skipped=not done)

    def _offer(self, spec: FileSpec, info: bytes, crc: int) -> Optional[int]:
        tries = 0
        while True:
            self._bin(ZFILE, bytes([0, 0, 0, ZCBIN]))
            self._sub(info, ZCRCW)
            while True:
                h = self.link.header(self.ft)
                if not isinstance(h, Header):
                    break                                  # 超时 / 乱码 → 重发 ZFILE
                if h.type == ZRPOS:
                    return min(h.pos, spec.size)
                if h.type == ZSKIP:
                    return None
                if h.type == ZCRC:
                    self._hex(ZCRC, pos_bytes(crc))
                    continue
                if h.type in (ZFERR, ZABORT, ZFIN, ZCAN):
                    raise ZmodemError(f"receiver refused {spec.name!r} (frame {h.type})")
                if h.type in (ZRINIT, ZNAK):
                    break                                  # 对方没收到 ZFILE
                # ZACK 等：忽略，继续等
            tries += 1
            if tries > RETRIES:
                raise ZmodemError(f"no answer to ZFILE for {spec.name!r}")

    def _data(self, spec: FileSpec, start: int) -> bool:
        """发送文件数据直到对方确认 ZEOF；``False`` = 对方中途 ZSKIP。"""
        pos = acked = start
        size = spec.size
        f = spec.data
        stalls = 0

        def restart(p: int) -> None:
            nonlocal pos, acked, mark
            pos = acked = mark = max(0, min(p, size))
            self._bin(ZDATA, pos_bytes(pos))

        mark = pos           # 上一个 ZCRCQ/ZCRCW 的位置
        self._bin(ZDATA, pos_bytes(pos))
        while True:
            f.seek(pos)
            chunk = f.read(self.chunk)
            npos = pos + len(chunk)
            if npos >= size:
                end = ZCRCE
            elif self.seg and npos - mark >= self.seg:
                end = ZCRCW
            elif not self.seg and npos - mark >= ACK_EVERY:
                end = ZCRCQ
            else:
                end = ZCRCG
            self._sub(chunk, end)
            pos = npos
            if self.progress:
                self.progress(spec.name, pos, size)
            if end == ZCRCE:
                r = self._eof(pos)
                if r is True:
                    return True
                if r is False:
                    return False
                restart(r)
                continue
            if end == ZCRCW:
                r = self._wait_ack(pos)
                if r is False:
                    return False
                restart(r)
                continue
            if end == ZCRCQ:
                mark = pos
            # 流式：看一眼回传通道；窗口满则阻塞等 ZACK
            while True:
                full = pos - acked >= WINDOW
                h = self.link.header(self.ft if full else 0)
                if h is None:
                    if not full:
                        break
                    stalls += 1
                    if stalls > RETRIES:
                        raise ZmodemError("receiver stopped acknowledging data")
                    restart(acked)
                    break
                if not isinstance(h, Header):
                    continue
                if h.type == ZACK:
                    if acked < h.pos <= pos:
                        acked = h.pos
                        stalls = 0
                elif h.type == ZRPOS:
                    restart(h.pos)
                    break
                elif h.type == ZSKIP:
                    return False
                elif h.type in (ZFERR, ZABORT, ZFIN, ZCAN):
                    raise ZmodemError(f"receiver aborted {spec.name!r} (frame {h.type})")

    def _wait_ack(self, pos: int) -> Union[int, bool]:
        """ZCRCW 之后：→ 续传位置（ZACK/ZRPOS），``False`` = ZSKIP。

        超时（ZACK 丢了）也返回 ``pos``：带位置的 ZDATA 头续发，接收方位置不符会回 ZRPOS；
        一直无应答由总截止时间兜底。
        """
        while True:
            h = self.link.header(self.ft)
            if isinstance(h, Header):
                if h.type == ZACK and h.pos == pos:
                    return pos
                if h.type == ZRPOS:
                    return h.pos
                if h.type == ZSKIP:
                    return False
                if h.type in (ZFERR, ZABORT, ZFIN, ZCAN):
                    raise ZmodemError(f"receiver aborted (frame {h.type})")
                continue
            if h is None:
                return pos

    def _eof(self, pos: int) -> Union[int, bool]:
        """发 ZEOF。→ ``True`` 完成（ZRINIT），``False`` ZSKIP，``int`` = ZRPOS 续传位置。"""
        tries = 0
        self._bin(ZEOF, pos_bytes(pos))
        while True:
            h = self.link.header(self.ft)
            if isinstance(h, Header):
                if h.type == ZRINIT:
                    return True
                if h.type == ZRPOS:
                    return h.pos
                if h.type == ZSKIP:
                    return False
                if h.type in (ZFERR, ZABORT, ZFIN, ZCAN):
                    raise ZmodemError(f"receiver aborted (frame {h.type})")
                if h.type != ZNAK:
                    continue
            tries += 1
            if tries > RETRIES:
                raise ZmodemError("no answer to ZEOF")
            self._bin(ZEOF, pos_bytes(pos))

    def finish(self) -> None:
        for _ in range(3):
            self._hex(ZFIN)
            deadline = time.monotonic() + self.ft
            while (remaining := deadline - time.monotonic()) > 0:
                h = self.link.header(remaining)
                if isinstance(h, Header) and h.type == ZFIN:
                    self.link.write(b"OO")
                    return
                if h is None:
                    break
        # 每个文件都已被 ZRINIT 确认：对方不回 ZFIN 也不影响数据，视为完成


def send_files(t: Transport, files: list[FileSpec], *, deadline: float,
               progress: Progress = None, frame_timeout: float = FRAME_TIMEOUT) -> SessionResult:
    """把 ``files`` 依次发给接收方（设备上的 rz）；失败抛 :class:`ZmodemError`。"""
    link = _Link(t, deadline)
    sender = _Sender(link, progress, frame_timeout)
    results: list[FileResult] = []
    try:
        sender.handshake()
        bytes_left = sum(f.size for f in files)
        for idx, spec in enumerate(files):
            results.append(sender.send_file(spec, len(files) - idx, bytes_left))
            bytes_left -= spec.size
        sender.finish()
        return SessionResult(results, link.tail())
    except _Cancelled as exc:
        raise ZmodemError(str(exc), results, cancelled=True, tail=exc.tail) from None
    except ZmodemError as exc:
        _abort(link)
        raise ZmodemError(str(exc), results) from None


# --------------------------------------------------------------------------- #
# 接收方
# --------------------------------------------------------------------------- #
class _Receiver:
    def __init__(self, link: _Link, save: Callable[[str, Optional[int]], Optional[Sink]],
                 progress: Progress, frame_timeout: float, use_crc32: bool,
                 escape_ctl: bool, bufsize: int) -> None:
        self.link = link
        self.save = save
        self.progress = progress
        self.ft = frame_timeout
        zf0 = CANFDX | CANOVIO | (CANFC32 if use_crc32 else 0) | (ESCCTL if escape_ctl else 0)
        self.zrinit = encode_hex_header(ZRINIT, flags_bytes(zf0, bufsize))
        self.results: list[FileResult] = []

    def _hex(self, ftype: int, data: bytes = b"\0\0\0\0") -> None:
        self.link.write(encode_hex_header(ftype, data))

    def _sub(self, use32: bool) -> Union[tuple[bytes, int], _Garbled, None]:
        return self.link.subpacket(self.ft, use32)

    def run(self) -> None:
        self.link.write(self.zrinit)
        tries = 0
        timeout = FIRST_TIMEOUT
        while True:
            h = self.link.header(timeout)
            if not isinstance(h, Header):
                tries += 1
                if tries > RETRIES:
                    raise ZmodemError("sender went silent")
                self.link.write(self.zrinit if h is None else encode_hex_header(ZNAK))
                timeout = self.ft
                continue
            tries = 0
            use32 = self.link.use32
            if h.type == ZRQINIT:
                self.link.write(self.zrinit)
            elif h.type == ZSINIT:
                if isinstance(self._sub(use32), tuple):
                    self._hex(ZACK, pos_bytes(1))
                else:
                    self._hex(ZNAK)
            elif h.type == ZFILE:
                sub = self._sub(use32)
                if not isinstance(sub, tuple):
                    self._hex(ZNAK)
                    continue
                self._file(sub[0])
                self.link.write(self.zrinit)
            elif h.type == ZFIN:
                self._fin()
                return
            elif h.type == ZCOMMAND:
                raise ZmodemError("peer sent ZCOMMAND (remote commands are not supported)")
            elif h.type in (ZEOF, ZDATA, ZNAK):
                self.link.write(self.zrinit)  # 对方没收到我们的 ZRINIT（或 ZSKIP 后的残余）
            elif h.type in (ZABORT, ZFERR, ZCAN):
                raise ZmodemError(f"sender aborted (frame {h.type})")

    def _file(self, info: bytes) -> None:
        raw_name, _, rest = info.partition(b"\0")
        name = raw_name.decode("utf-8", "replace")
        meta = rest.split(b"\0", 1)[0].split()
        size: Optional[int] = None
        if meta:
            try:
                size = int(meta[0])
            except ValueError:
                size = None
        sink = self.save(name, size)
        if sink is None:
            self._hex(ZSKIP)
            return
        self.results.append(self._data(name, size, sink))

    def _data(self, name: str, size: Optional[int], sink: Sink) -> FileResult:
        t0 = time.monotonic()
        offset = 0
        crc = 0
        errors = 0
        resync = False

        def zrpos() -> None:
            nonlocal errors, resync
            errors += 1
            if errors > ERRORS_MAX:
                raise ZmodemError(f"too many errors receiving {name!r}")
            resync = True
            self._hex(ZRPOS, pos_bytes(offset))

        self._hex(ZRPOS, pos_bytes(0))
        while True:
            h = self.link.header(self.ft)
            if h is None:
                zrpos()
                continue
            if not isinstance(h, Header):
                if not resync:        # 重同步期间数据里的伪头不算错误
                    zrpos()
                continue
            if h.type == ZDATA:
                if h.pos != offset:
                    zrpos()
                    continue
                resync = False
                while True:
                    sub = self._sub(self.link.use32)
                    if not isinstance(sub, tuple):
                        zrpos()
                        break
                    data, end = sub
                    if data:
                        sink.write(data)
                        crc = crc32(data, crc)
                        offset += len(data)
                        errors = 0
                        if self.progress:
                            self.progress(name, offset, size)
                    if end in (ZCRCW, ZCRCQ):
                        self._hex(ZACK, pos_bytes(offset))
                    if end in (ZCRCW, ZCRCE):
                        break
            elif h.type == ZEOF:
                if h.pos == offset:
                    sink.close()
                    return FileResult(name, offset, crc, time.monotonic() - t0)
                zrpos()
            elif h.type == ZFILE:
                self._sub(self.link.use32)          # 对方没收到 ZRPOS：丢弃 ZFILE 子包
                self._hex(ZRPOS, pos_bytes(offset))
            elif h.type == ZNAK:
                self._hex(ZRPOS, pos_bytes(offset))
            elif h.type in (ZABORT, ZFERR, ZCAN, ZFIN):
                raise ZmodemError(f"sender aborted {name!r} (frame {h.type})")

    def _fin(self) -> None:
        """回 ZFIN，再等可选的 "OO"；其余字节留作会话外文本。"""
        self._hex(ZFIN)
        link = self.link
        until = time.monotonic() + OO_WAIT
        while True:
            while link.buf[:1] in (b"\r", b"\n", b"\x8a", b"\x11"):
                del link.buf[:1]
            if len(link.buf) >= 2:
                if link.buf[:2] == b"OO":
                    del link.buf[:2]
                    return
                if link.buf[0] == ZPAD:          # lsz 重发了 ZFIN
                    h = link.header(max(0.0, until - time.monotonic()))
                    if isinstance(h, Header) and h.type == ZFIN:
                        self._hex(ZFIN)
                        until = time.monotonic() + OO_WAIT
                        continue
                return                           # 不是 OO：设备文本
            if not link._fill(until) and time.monotonic() >= until:
                if bytes(link.buf) == b"O":      # 半个 OO 是协议字节
                    link.buf.clear()
                return


def receive_files(t: Transport, save: Callable[[str, Optional[int]], Optional[Sink]], *,
                  deadline: float, progress: Progress = None,
                  frame_timeout: float = FRAME_TIMEOUT, use_crc32: bool = True,
                  escape_ctl: bool = False, bufsize: int = 0) -> SessionResult:
    """接收发送方（设备上的 sz）发来的全部文件。

    ``save(name, size)`` 返回 :class:`Sink`（写入并在文件完整后 ``close``）或 ``None``
    （跳过该文件，回 ZSKIP）。``name`` 原样来自对端，调用方负责清洗。
    """
    link = _Link(t, deadline)
    rx = _Receiver(link, save, progress, frame_timeout, use_crc32, escape_ctl, bufsize)
    try:
        rx.run()
        return SessionResult(rx.results, link.tail())
    except _Cancelled as exc:
        raise ZmodemError(str(exc), rx.results, cancelled=True, tail=exc.tail) from None
    except ZmodemError as exc:
        _abort(link)
        raise ZmodemError(str(exc), rx.results) from None
