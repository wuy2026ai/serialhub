"""Agent-side ZMODEM over the daemon's ``/ws`` relay (no browser, no host lrzsz).

zsend / zrecv 的编排层：引擎（``zmodem_proto``）跑在**调用方进程**里，文件留在 agent 所在的
机器上，daemon 不增加任何文件访问面（且可以是远程的）。流程与
``tests/zmodem_client/zclient.mjs`` 一致（docs/ARCHITECTURE.md §14）：

1. 先连 ``/ws``（token 走 ``Authorization: Bearer``），订阅本端口并 ping/pong 确认已订阅；
   经 REST 读端口的 ``baud``（决定发送节奏与自动超时）与 ``zmodem_cleanup_eol``；
2. 记下环形缓冲游标，经普通 REST send 发 ``cmd``（``""`` = 设备自己会开始）；
3. 等本端口的 ``zmodem`` ``start`` 广播（方向须匹配：zsend = upload，zrecv = download），
   :data:`START_TIMEOUT` 内没有 → ``error: device did not start ZMODEM`` + 设备最后几行做提示；
4. ``zmodem_claim`` → 引擎经 ``zmodem_data`` / ``zmodem_write``（base64）跑完整个会话；
5. 成功：再收 :data:`TAIL_WAIT` 秒会话外字节，连同引擎交回的 tail 随 ``zmodem_end`` 的
   ``tail_b64`` 交回（设备的 ``rz done``、提示符因此照常进环）。
6. 任何非干净结束（设备取消、对方已退出、出错、超时）都走**同一个**收尾 :func:`_cleanup`：
   对方可能还活着时写**一次** ``ABORT_SEQ``（引擎不写，daemon 也不写——以 ``abort:false``
   结束）→ 等设备安静（300 ms 无数据，最多 3 s），这段文本就是提示 → 写一次
   ``zmodem_cleanup_eol`` 冲掉设备输入行缓冲里的残留 → 再等安静并**丢弃** shell 对它的回应 →
   ``zmodem_end abort:false``，``tail`` 只含设备自己的文本（设备 3 s 内都安静不下来则 tail 为空）——
   始终 ``abort:false``：``ABORT_SEQ`` 已写过或对方已退出，daemon 不能再写一份。自身的非 ZMODEM 异常
   （保存目录只读、磁盘满……）也包装成引擎的错误类型走这条路。

``zsend`` / ``zrecv`` 返回给 agent 的短文本（用户错误一律 ``error: ...``，绝不抛异常；部分完成时
先列出已传完的文件、最后一行是 ``error:``）；``send_session`` / ``receive_session`` 返回结构化
dict，供完整 MCP profile 使用。
"""
from __future__ import annotations

import base64
import binascii
import json
import math
import os
import re
import shlex
import tempfile
import time
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, Sequence, Union

from . import zmodem_proto as zp
from .client import Client
from .config import ZMODEM_CLEANUP_EOLS
from .zmodem import ABORT_SEQ, TAIL_MAX_BYTES

START_TIMEOUT = 10.0          #: 发出 cmd 后等会话开始的时长（秒）
TAIL_WAIT = 0.2               #: 会话结束后再收会话外字节的时长（秒）
CLOSE_WAIT = 3.0              #: 发 zmodem_end 后等 zmodem_closed 的时长（秒）
#: 收尾时的 zmodem_end 可能排在 daemon 尚未写完的数据之后（设备读得慢时每片要等），多等一会儿
CLEANUP_CLOSE_WAIT = 15.0
SYNC_WAIT = 30.0              #: 收尾：等 daemon 把已发出的数据写完（见 _WsTransport.sync）
SETTLE_SILENCE = 0.3          #: 收尾：连续这么久没有数据即认为设备已安静（秒）……
SETTLE_MAX = 3.0              #: ……最多等这么久（仍在说话 → abort:true）
HINT_LINES = 3                #: 失败提示里附带的设备行数
HINT_LINE_MAX = 240           #: 每行最多这么多字符
TIMEOUT_MAX_MS = 1_800_000
AUTO_FACTOR, AUTO_SLACK = 1.5, 30.0   #: 自动超时 = 字节×10/波特率×1.5 + 30 s
INLINE_MAX = 1 << 20          #: zsend 内联 content 解码后的上限
INLINE_READ = 64 * 1024       #: zrecv inline 最多读这么多字节来显示
DEFAULT_RECV_CMD = "sz {remote}"
RETRY_LATER = "retry in ~5 s"

Progress = Optional[Callable[[str, int, Optional[int]], None]]

#: A ZMODEM hex header with its CR/LF/XON trailer (protocol bytes inside device text).
_HEX_HEADER = re.compile(rb"\*+\x18B[0-9a-fA-F]{14}\r?[\n\x8a]?\x11?")
#: ANSI escape sequences (CSI … final byte, OSC … BEL/ST, two-byte ESC x).
_ANSI = re.compile(rb"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)|[@-_])")
#: Pieces of our own frames in a line the shell printed back: ZDLE, a hex-header body, the
#: hex-header trailer (``\r`` ran the line, ``\x8a\x11`` stayed).
_ECHO = re.compile(rb"\x18|\x8a\x11|B[01][0-9a-f]{13}")
_WIN_RESERVED = re.compile(r"^(con|prn|aux|nul|com[0-9¹²³]|lpt[0-9¹²³])(\..*)?$", re.I)


class ZmodemClientError(Exception):
    """用户可见的失败；``hint`` = 设备自己打印的几行（不含被我方字节引出的回应）。"""

    def __init__(self, message: str, *, cancelled: bool = False, hint: str = "",
                 files: Optional[list[Any]] = None) -> None:
        super().__init__(message)
        self.cancelled = cancelled
        self.hint = hint
        self.files = list(files or [])


class _LocalError(zp.ZmodemError):
    """A failure on our side of the link (e.g. the save directory is not writable), raised as
    the engine's error type so it takes the same cleanup path; its text is the message."""


def _cannot_write(path: str, exc: OSError) -> _LocalError:
    return _LocalError(f"cannot write {path}: {exc.strerror or exc}")


# --------------------------------------------------------------------------- #
# helpers shared by lite / CLI / MCP
# --------------------------------------------------------------------------- #
def clamp_timeout(timeout_ms: Optional[int]) -> Optional[int]:
    """``None`` stays ``None`` (= automatic from size and baud); else 1 s … 30 min."""
    if timeout_ms is None:
        return None
    return max(1000, min(TIMEOUT_MAX_MS, int(timeout_ms)))


def auto_seconds(size: int, baud: Optional[int]) -> float:
    """Automatic overall timeout for ``size`` bytes at ``baud`` (capped at the maximum)."""
    line = size * 10 / baud if baud else 0.0
    return min(TIMEOUT_MAX_MS / 1000.0, line * AUTO_FACTOR + AUTO_SLACK)


def safe_name(name: str) -> Optional[str]:
    """Basename of a remote/device-supplied name, or ``None`` if unusable on any OS.

    Rejected everywhere (simpler and safe): empty, ``.``/``..``, control characters, ``:``,
    Windows reserved device names (``CON``, ``NUL``, ``COM1``…, also with an extension) and
    trailing dots/spaces.
    """
    base = re.split(r"[\\/]", str(name))[-1]
    if base in ("", ".", "..") or any(ord(c) < 32 for c in base) or ":" in base:
        return None
    if base.endswith((".", " ")) or _WIN_RESERVED.match(base):
        return None
    return base


def inline_content(content: Union[str, bytes], name: str,
                   encoding: Optional[str] = "text") -> tuple[str, bytes]:
    """Decode a model-supplied file body. Raises ``ValueError`` with a user-facing message."""
    remote = safe_name(name)
    if remote is None:
        raise ValueError(f"bad name {name!r}")
    if encoding in (None, "text"):
        body = content if isinstance(content, bytes) else content.encode("utf-8")
    elif encoding == "base64":
        text = content.decode("ascii", "replace") if isinstance(content, bytes) else content
        try:
            body = base64.b64decode("".join(text.split()), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"bad base64 content: {exc}") from None
    else:
        raise ValueError("encoding must be text or base64")
    if len(body) > INLINE_MAX:
        raise ValueError(f"content too large ({len(body)} bytes > {INLINE_MAX}); "
                         "write it to a file and pass path")
    return remote, body


def recv_command(remote: Sequence[str], cmd: Optional[str] = None) -> str:
    """``cmd`` with ``{remote}`` replaced by the names (shell-quoted only when needed)."""
    joined = " ".join(n if re.fullmatch(r"[\w.,+@%=:/-]+", n) else shlex.quote(n)
                      for n in remote)
    return (DEFAULT_RECV_CMD if cmd is None else cmd).replace("{remote}", joined)


def cleanup_eol_bytes(setting: Optional[str]) -> bytes:
    """Bytes for a port's ``zmodem_cleanup_eol`` (unknown/missing → ``cr``)."""
    return ZMODEM_CLEANUP_EOLS.get(str(setting or "cr").lower(), b"\r")


def default_dir() -> str:
    """Current directory when writable, else the system temp directory."""
    cwd = os.getcwd()
    return cwd if os.access(cwd, os.W_OK) else tempfile.gettempdir()


def _crc(value: int) -> str:
    return f"{value & 0xFFFFFFFF:08X}"


def _clean_line(raw: bytes) -> Optional[str]:
    """A device text line, or ``None`` if it is our own protocol echo or mostly binary.

    ANSI escape sequences are removed and the rest is decoded as UTF-8 with replacement
    (a Latin-1 message keeps its readable part). Echo = a piece of one of our frames (ZDLE,
    a hex-header body or its ``\\x8a\\x11`` trailer) or raw C0 control bytes in the line (the
    shell repeating binary it was fed). Mostly non-printable / replacement → junk.
    """
    if _ECHO.search(raw):
        return None
    # 7-bit XON/XOFF only: 0x91/0x93 are also UTF-8 continuation bytes
    raw = _ANSI.sub(b"", raw).translate(None, b"\x11\x13")
    if any((c < 32 and c != 9) or c == 0x7F for c in raw):
        return None
    text = raw.decode("utf-8", "replace").strip()
    if not text:
        return ""
    bad = sum(1 for ch in text if ch == "\ufffd" or "\x80" <= ch <= "\x9f")
    if bad * 2 >= len(text):
        return None
    return text[:HINT_LINE_MAX]


def _device_lines(before: bytes, after: bytes = b"") -> list[str]:
    """Device text around a cancel (before it, then after it): every line that is the device's
    own, in order; lines that are junk or echo of our bytes are dropped, not a stop."""
    before = _HEX_HEADER.sub(b"", before)
    after = _HEX_HEADER.sub(b"", after.lstrip(b"\x18\x08"))
    return [ln for raw in re.split(rb"\r\n|\r|\n", before + b"\r\n" + after)
            if (ln := _clean_line(raw))]


# --------------------------------------------------------------------------- #
# WebSocket transport
# --------------------------------------------------------------------------- #
def _ws_url(base_url: str) -> str:
    url = base_url.rstrip("/")
    if url.startswith("https://"):
        url = "wss://" + url[len("https://"):]
    elif url.startswith("http://"):
        url = "ws://" + url[len("http://"):]
    return url + "/ws"


@contextmanager
def _connect(base_url: str, token: Optional[str]) -> Iterator[Any]:
    """Open ``/ws`` (sync client); connection errors propagate from entering the context."""
    from websockets.sync.client import connect  # noqa: PLC0415 - keep import cost off the CLI

    kw: dict[str, Any] = {"max_size": None, "compression": None, "open_timeout": 5,
                          "close_timeout": 2}
    if token:
        kw["additional_headers"] = {"Authorization": f"Bearer {token}"}
    try:
        cm = connect(_ws_url(base_url), proxy=None, **kw)
        ws = cm.__enter__()
    except TypeError:  # websockets < 15: no proxy option (and no proxy support)
        cm = connect(_ws_url(base_url), **kw)
        ws = cm.__enter__()
    try:
        yield ws
    finally:
        try:
            cm.__exit__(None, None, None)
        except Exception:  # noqa: BLE001 - already closed
            pass


class _WsTransport:
    """``zmodem_proto.Transport`` over one claimed session of the daemon relay."""

    #: A failed ``zmodem_write`` (e.g. ``Write timeout`` when the device stopped reading) is
    #: reported this long after it arrives: the device's CAN usually follows right behind it.
    ERROR_GRACE = 1.5

    def __init__(self, ws: Any, port: str, session: str) -> None:
        self.ws, self.port, self.session = ws, port, session
        self.closed: Optional[str] = None
        self._error: Optional[tuple[str, float]] = None
        self.cleaning = False   # 收尾中：daemon 对旧数据的写失败照常会来，不再当错误
        self._pending = bytearray()  # sync() 期间收到的会话数据，下次 read 先交出
        #: 已写过 ABORT_SEQ 或对方已退出：任何结束方式都不能再让 daemon 写一份（abort:false）
        self.no_daemon_abort = False

    def read(self, timeout: float) -> bytes:
        if self._pending:
            out = bytes(self._pending)
            self._pending.clear()
            return out
        until = time.monotonic() + max(0.0, timeout)
        while True:
            if self.closed == "shutdown":  # {"type":"shutdown"}: the whole daemon is going away
                raise zp.ZmodemError("daemon stopped (shutdown)")
            if self.closed is not None:
                raise zp.ZmodemError(f"daemon ended the session ({self.closed})")
            now = time.monotonic()
            overdue = bool(self._error) and now - self._error[1] >= self.ERROR_GRACE
            wait = max(0.0, until - now)
            if self._error:
                # data already queued is still delivered first (it may hold the device's CAN)
                wait = 0.0 if overdue else min(wait, self._error[1] + self.ERROR_GRACE - now)
            try:
                raw = self.ws.recv(timeout=max(0.0, wait))
            except TimeoutError:
                if overdue:
                    raise zp.ZmodemError(f"daemon: {self._error[0]}") from None
                if time.monotonic() < until:
                    continue
                return b""
            except Exception as exc:  # noqa: BLE001 - connection lost
                raise zp.ZmodemError(f"daemon connection lost: {exc}") from None
            msg = _json(raw)
            if msg.get("type") == "shutdown":  # broadcast without "port"
                self.closed = self.closed or "shutdown"
                continue
            if msg.get("port") != self.port:
                continue
            kind = msg.get("type")
            if kind == "zmodem_data" and msg.get("session") == self.session:
                return base64.b64decode(msg.get("b64") or "")
            if kind == "zmodem_closed" and msg.get("session") == self.session:
                self.closed = str(msg.get("reason"))
            elif kind == "zmodem_error" and msg.get("session") in (None, self.session):
                if self._error is None and not self.cleaning:
                    self._error = (str(msg.get("error")), time.monotonic())
            if time.monotonic() >= until and not self._error:
                return b""

    def write(self, data: bytes, *, force: bool = False) -> None:
        """Send bytes to the device; after a failed write only ``force`` (cleanup) writes go."""
        if self._error and not force:
            raise zp.ZmodemError(f"daemon: {self._error[0]}")
        try:
            self.ws.send(json.dumps({"type": "zmodem_write", "port": self.port,
                                     "session": self.session,
                                     "b64": base64.b64encode(data).decode("ascii")}))
        except Exception as exc:  # noqa: BLE001 - connection lost
            raise zp.ZmodemError(f"daemon connection lost: {exc}") from None

    def sync(self, timeout: float = SYNC_WAIT) -> bool:
        """Wait until the daemon has written everything we sent so far.

        The daemon handles one connection's messages strictly in order and awaits each
        ``zmodem_write``, so its ``pong`` to a ``ping`` sent now proves the earlier writes reached
        the device. Session data arriving meanwhile is kept for the next :meth:`read`.
        """
        try:
            self.ws.send(json.dumps({"type": "ping"}))
        except Exception:  # noqa: BLE001
            return False
        until = time.monotonic() + timeout
        while (left := until - time.monotonic()) > 0:
            try:
                msg = _json(self.ws.recv(timeout=left))
            except TimeoutError:
                return False
            except Exception:  # noqa: BLE001
                return False
            if msg.get("type") == "pong":
                return True
            if msg.get("type") == "shutdown":
                self.closed = self.closed or "shutdown"
                return False
            if msg.get("port") != self.port:
                continue
            if msg.get("type") == "zmodem_data" and msg.get("session") == self.session:
                self._pending += base64.b64decode(msg.get("b64") or "")
            elif msg.get("type") == "zmodem_closed" and msg.get("session") == self.session:
                self.closed = str(msg.get("reason"))
                return False
        return False

    def drain(self, seconds: float) -> bytes:
        """Bytes arriving within ``seconds`` (device text after the session)."""
        out = bytearray()
        until = time.monotonic() + seconds
        while (left := until - time.monotonic()) > 0:
            try:
                out += self.read(left)
            except zp.ZmodemError:
                break
        return bytes(out)

    def settle(self, silence: float = SETTLE_SILENCE,
               cap: float = SETTLE_MAX) -> tuple[bytes, bool]:
        """Read until ``silence`` s without data (or ``cap`` s total). → ``(bytes, capped)``."""
        out = bytearray()
        end = time.monotonic() + cap
        while (left := end - time.monotonic()) > 0:
            try:
                data = self.read(min(silence, left))
            except zp.ZmodemError:
                return bytes(out), False
            if not data:
                return bytes(out), False
            out += data
        return bytes(out), True

    def end(self, *, abort: bool, tail: bytes = b"",
            wait: float = CLOSE_WAIT) -> Optional[str]:
        """Send ``zmodem_end`` and wait for ``zmodem_closed``; returns its reason."""
        if self.closed is not None:
            return self.closed
        msg: dict[str, Any] = {"type": "zmodem_end", "port": self.port,
                               "session": self.session, "abort": abort}
        if tail and not abort:
            msg["tail_b64"] = base64.b64encode(tail[:TAIL_MAX_BYTES]).decode("ascii")
        try:
            self.ws.send(json.dumps(msg))
        except Exception:  # noqa: BLE001 - socket gone: daemon aborts on disconnect
            return None
        until = time.monotonic() + wait
        while self.closed is None and time.monotonic() < until:
            try:
                self.read(until - time.monotonic())
            except zp.ZmodemError:
                break
        return self.closed


def _json(raw: Any) -> dict[str, Any]:
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return msg if isinstance(msg, dict) else {}


@dataclass
class _Outcome:
    lines: list[str]        #: the device's own text (hint)
    gone: bool              #: the device's program had already quit (CAN or plain text)


def _cleanup(transport: Any, exc: zp.ZmodemError, eol: bytes) -> _Outcome:
    """The one exit path for every non-clean end.

    0. an error of ours that does not already say the peer is gone (e.g. the daemon's
       ``Write timeout``): settle first — a CAN or plain device text there means it quit;
    1. peer may still be alive → ``ABORT_SEQ`` exactly once (engine and daemon write none);
    2. wait until the daemon has written everything (``sync``), then settle (300 ms silence,
       ≤ 3 s): that text is the hint;
    3. one line terminator flushes the device's input line buffer;
    4. sync + settle again and discard the shell's reply to it;
    5. ``zmodem_end abort:false`` with only the step-2 text — always ``abort:false``: we wrote
       the one ``ABORT_SEQ`` ourselves or the peer is gone, so the daemon must not add another;
       if step 2 hit its cap (device still talking) the tail is empty but the terminator still
       goes out.

    ``transport.no_daemon_abort`` is set as soon as that holds, so an interrupted cleanup also
    ends the session without the daemon's ``ABORT_SEQ``.
    """
    gone = exc.cancelled or exc.peer_gone
    before, after = exc.before, exc.after
    if exc.peer_gone:  # what made us decide is already the program's exit (or the shell's echo)
        before, after = b"", before + after
    if transport.closed is not None:      # the daemon already ended (and aborted) the session
        return _Outcome(_device_lines(before, after), gone)
    transport.cleaning = True
    transport._error = None
    try:
        if not gone and not exc.timed_out:
            # rz/sz may take a moment to exit after cancelling: wait for a full second of quiet
            pre, _capped = transport.settle(silence=1.0)
            can = pre.find(b"\x18" * 5)
            if can >= 0:
                gone, before, after = True, before + pre[:can], after + pre[can:]
            elif zp._looks_like_text(pre):
                gone, after = True, after + pre
            else:
                after += pre
        transport.no_daemon_abort = True  # before the write: a partial one still counts
        if not gone:
            transport.write(ABORT_SEQ, force=True)
        transport.sync()                  # everything in flight has reached the device …
        text, capped = transport.settle()  # … and the device has answered it
        lines = _device_lines(before, after + text)
        if eol:
            transport.write(eol, force=True)
            transport.sync()
            transport.settle()
    except zp.ZmodemError:                # connection gone mid-cleanup: daemon aborts
        return _Outcome(_device_lines(before, after), gone)
    tail = b"" if capped else "".join(f"{ln}\r\n" for ln in lines).encode("utf-8")
    transport.end(abort=False, tail=tail, wait=CLEANUP_CLOSE_WAIT)
    return _Outcome(lines, gone)


# --------------------------------------------------------------------------- #
# session orchestration
# --------------------------------------------------------------------------- #
_DIRECTION = {"upload": "upload (device runs rz)", "download": "download (device runs sz)"}


def _recv_until(ws: Any, pred: Callable[[dict[str, Any]], bool], until: float) -> Optional[dict]:
    while (left := until - time.monotonic()) > 0:
        try:
            msg = _json(ws.recv(timeout=left))
        except TimeoutError:
            return None
        if pred(msg):
            return msg
    return None


def _device_hint(rest: Client, port: str, cursor: int, cmd: str) -> str:
    env = rest.read(port, since_seq=cursor, kinds="rx", limit=200)
    lines = [str(ln.get("text") or "").strip() for ln in env.get("lines") or []]
    lines = [ln for ln in lines if ln and not (cmd and ln.endswith(cmd.strip()))]
    return " | ".join(lines[-HINT_LINES:])


@dataclass
class _PortInfo:
    baud: Optional[int]
    eol: bytes


def _port_info(rest: Client, port: str) -> _PortInfo:
    st = rest.status(port)
    baud = st.get("baud") if st.get("ok") else None
    setting = None
    cfg = rest.call("GET", "/api/config")
    for p in ((cfg.get("config") or {}).get("ports") or []) if cfg.get("ok") else []:
        if p.get("name") == port:
            setting = p.get("zmodem_cleanup_eol")
    return _PortInfo(int(baud) if isinstance(baud, (int, float)) and baud > 0 else None,
                     cleanup_eol_bytes(setting))


class _Clock:
    """Overall deadline: fixed (``timeout_ms``) or automatic from size and baud (extendable)."""

    def __init__(self, timeout_ms: Optional[int], baud: Optional[int], size: int) -> None:
        self.t0 = time.monotonic()
        self.fixed = timeout_ms is not None
        self.baud = baud
        self.total = size
        self.done: dict[str, int] = {}
        span = timeout_ms / 1000.0 if timeout_ms is not None else auto_seconds(size, baud)
        self.deadline = self.t0 + span

    def __call__(self) -> float:
        return self.deadline

    def announce(self, size: Optional[int]) -> None:
        """zrecv: a ZFILE announced ``size`` bytes — extend an automatic deadline."""
        if size:
            self.total += size
        if not self.fixed and size:
            self.deadline = max(self.deadline, time.monotonic() + auto_seconds(size, self.baud))

    def progress(self, inner: Progress) -> Progress:
        def track(name: str, done: int, total: Optional[int]) -> None:
            self.done[name] = done
            if inner:
                inner(name, done, total)
        return track

    def message(self) -> str:
        done = sum(self.done.values())
        took = time.monotonic() - self.t0
        if not self.total:
            return f"timed out after {took:.0f}s waiting for the device; retry with a larger timeout_ms"
        msg = f"timed out at {done}/{self.total} bytes after {took:.0f}s"
        if self.baud:
            need = self.total * 10 / self.baud
            want = math.ceil((need * AUTO_FACTOR + AUTO_SLACK) * 1000)
            msg += f"; at {self.baud} baud this needs ≈{need:.0f}s; retry with timeout_ms>={want}"
            if want > TIMEOUT_MAX_MS:
                msg += f" (above the {TIMEOUT_MAX_MS} maximum: send smaller files)"
        else:
            msg += "; retry with a larger timeout_ms"
        return msg


def _run_engine(engine: Callable[..., zp.SessionResult], transport: Any, clock: Any,
                baud: Optional[int]) -> zp.SessionResult:
    """Run ``engine``; any non-ZMODEM exception becomes a :class:`_LocalError` (same cleanup
    path) that keeps the files the engine had already completed (``exc.files``)."""
    try:
        return engine(transport, clock, baud)
    except zp.ZmodemError:
        raise
    except Exception as exc:  # noqa: BLE001 - ours (not ZMODEM): same cleanup path
        raise _LocalError(f"{type(exc).__name__}: {exc}",
                          getattr(exc, "files", None)) from exc


def _run_session(base_url: str, token: Optional[str], port: str, cmd: str, direction: str,
                 timeout_ms: Optional[int], size: int,
                 engine: Callable[[zp.Transport, "_Clock", Optional[int]], zp.SessionResult]
                 ) -> zp.SessionResult:
    """Run one claimed ZMODEM session; returns the engine result with the full tail."""
    rest = Client(base_url, token, timeout=10)
    conn = _connect(base_url, token)
    try:
        ws = conn.__enter__()
    except Exception as exc:  # noqa: BLE001 - unreachable / refused / bad handshake
        raise ZmodemClientError(f"cannot connect to daemon websocket: {exc}") from None
    try:
        hello = _recv_until(ws, lambda m: True, time.monotonic() + 5)
        if not hello or hello.get("type") != "hello":
            raise ZmodemClientError(str((hello or {}).get("error") or "no hello from daemon"))
        ws.send(json.dumps({"type": "subscribe", "ports": [port]}))
        ws.send(json.dumps({"type": "ping"}))
        if not _recv_until(ws, lambda m: m.get("type") == "pong", time.monotonic() + 5):
            raise ZmodemClientError("daemon did not answer ping")
        env = rest.read(port, limit=0)
        if not env.get("ok"):
            raise ZmodemClientError(str(env.get("error") or "cannot read port"))
        cursor = int(env.get("next_seq") or 0)
        info = _port_info(rest, port)
        clock = _Clock(timeout_ms, info.baud, size)
        if cmd:
            sent = rest.send(port, cmd)
            if not sent.get("ok"):
                raise ZmodemClientError(str(sent.get("error") or "send failed"))
        start = _recv_until(
            ws, lambda m: m.get("type") == "zmodem" and m.get("event") == "start"
            and m.get("port") == port, min(clock(), time.monotonic() + START_TIMEOUT))
        if start is None:
            hint = _device_hint(rest, port, cursor, cmd)
            raise ZmodemClientError("device did not start ZMODEM"
                                    + (f"; device said: {hint}" if hint else ""))
        if start.get("direction") != direction:
            raise ZmodemClientError(
                f"device started a {_DIRECTION.get(str(start.get('direction')), '?')}, "
                f"expected a {_DIRECTION[direction]} (the daemon aborts it; {RETRY_LATER})")
        ws.send(json.dumps({"type": "zmodem_claim", "port": port}))
        reply = _recv_until(ws, lambda m: m.get("port") == port and m.get("type") in (
            "zmodem_claimed", "zmodem_error"), time.monotonic() + 5)
        if reply is None or reply.get("type") == "zmodem_error":
            code = (reply or {}).get("code")
            if code == "claimed_by_other":
                raise ZmodemClientError(f"ZMODEM session on {port!r} was claimed by another "
                                        f"client (a browser tab?); close it and {RETRY_LATER}")
            raise ZmodemClientError(f"cannot claim ZMODEM session: "
                                    f"{(reply or {}).get('error') or 'no reply'}")
        transport = _WsTransport(ws, port, str(reply.get("session")))
        try:
            result = _run_engine(engine, transport, clock, info.baud)
        except zp.ZmodemError as exc:
            try:
                out = _cleanup(transport, exc, info.eol)
            except BaseException:
                transport.end(abort=not transport.no_daemon_abort)
                raise
            hint = " | ".join(out.lines[:HINT_LINES])
            if isinstance(exc, _LocalError):
                message = str(exc)
            elif exc.timed_out:
                message = clock.message()
            elif not out.gone and any(k in str(exc) for k in (
                    "Write timeout", "stopped acknowledging")):
                message = (f"device stopped accepting data at {sum(clock.done.values())}/"
                           f"{clock.total or '?'} bytes (rz exited or flow-controlled?)")
            elif exc.cancelled:
                message = "device cancelled the transfer"
            elif out.gone:
                message = "device ended the transfer (its rz/sz exited)"
            else:
                message = f"transfer failed: {exc}"
            raise ZmodemClientError(message + (f"; device said: {hint}" if hint else ""),
                                    cancelled=out.gone, hint=hint, files=exc.files) from None
        except BaseException:             # e.g. KeyboardInterrupt in the engine
            transport.end(abort=not transport.no_daemon_abort)
            raise
        result.tail = result.tail + transport.drain(TAIL_WAIT)
        transport.end(abort=False, tail=result.tail)
        return result
    finally:
        conn.__exit__(None, None, None)


# --------------------------------------------------------------------------- #
# zsend
# --------------------------------------------------------------------------- #
def _sent_entry(f: zp.FileResult) -> dict[str, Any]:
    return {"name": f.name, "bytes": f.size, "crc32": _crc(f.crc32),
            "seconds": round(f.seconds, 1), "skipped": f.skipped}


def send_session(client_url: str, token: Optional[str], port: str,
                 paths: Sequence[str] = (), *, contents: Sequence[tuple[str, bytes]] = (),
                 cmd: str = "rz", timeout_ms: Optional[int] = None, overwrite: bool = True,
                 progress: Progress = None) -> dict[str, Any]:
    """Upload local files and/or in-memory ``(name, bytes)``.

    Returns ``{"ok": True, "files": [...]}`` or ``{"ok": False, "error", "files"}`` where
    ``files`` are the ones that did transfer before the failure.
    """
    import io  # noqa: PLC0415

    specs: list[zp.FileSpec] = []
    handles: list[Any] = []
    try:
        for path in paths:
            p = os.path.expanduser(str(path))
            if not os.path.isfile(p):
                return {"ok": False, "error": f"no such file: {path}", "files": []}
            try:
                fh = open(p, "rb")  # noqa: SIM115 - closed in finally
            except OSError as exc:
                return {"ok": False, "error": f"cannot read {path}: {exc.strerror or exc}",
                        "files": []}
            handles.append(fh)
            st = os.fstat(fh.fileno())
            specs.append(zp.FileSpec(os.path.basename(p), fh, st.st_size, int(st.st_mtime)))
        now = int(time.time())
        for name, body in contents:
            specs.append(zp.FileSpec(name, io.BytesIO(body), len(body), now))
        if not specs:
            return {"ok": False, "error": "nothing to send", "files": []}

        def engine(t: zp.Transport, clock: _Clock, baud: Optional[int]) -> zp.SessionResult:
            return zp.send_files(t, specs, deadline=clock, progress=clock.progress(progress),
                                 baud=baud, overwrite=overwrite, abort_on_error=False)

        try:
            res = _run_session(client_url, token, port, cmd, "upload",
                               clamp_timeout(timeout_ms), sum(s.size for s in specs), engine)
        except ZmodemClientError as exc:
            return {"ok": False, "error": str(exc),
                    "files": [_sent_entry(f) for f in exc.files if not f.skipped]}
        return {"ok": True, "files": [_sent_entry(f) for f in res.files]}
    finally:
        for fh in handles:
            fh.close()


def format_send(env: dict[str, Any]) -> str:
    sent = [f for f in env.get("files") or [] if not f["skipped"]]
    skipped = [f for f in env.get("files") or [] if f["skipped"]]
    lines = [f"sent {f['name']} {f['bytes']}B crc={f['crc32']} in {f['seconds']}s" for f in sent]
    lines += [f"skipped {f['name']}: device refused it (already exists? try cmd \"rz -y\")"
              for f in skipped]
    if not env.get("ok"):
        return "\n".join([*lines, f"error: {env.get('error')}"])
    if not sent:
        return "\n".join(["error: nothing was sent", *lines])
    return "\n".join(lines)


def send_ok(env: dict[str, Any]) -> bool:
    """Complete success: every file sent, none refused (the CLI's exit status)."""
    files = env.get("files") or []
    return bool(env.get("ok")) and bool(files) and not any(f["skipped"] for f in files)


def zsend_status(client_url: str, token: Optional[str], port: str, paths: Sequence[str] = (),
                 *, contents: Sequence[tuple[str, bytes]] = (), cmd: str = "rz",
                 timeout_ms: Optional[int] = None, overwrite: bool = True,
                 progress: Progress = None) -> tuple[str, bool]:
    """:func:`zsend` plus :func:`send_ok` of the result (never raises)."""
    try:
        env = send_session(client_url, token, port, paths, contents=contents, cmd=cmd,
                           timeout_ms=timeout_ms, overwrite=overwrite, progress=progress)
        return format_send(env), send_ok(env)
    except Exception as exc:  # noqa: BLE001 - the contract is "never raise"
        return f"error: {type(exc).__name__}: {exc}", False


def zsend(client_url: str, token: Optional[str], port: str, paths: Sequence[str] = (), *,
          contents: Sequence[tuple[str, bytes]] = (), cmd: str = "rz",
          timeout_ms: Optional[int] = None, overwrite: bool = True,
          progress: Progress = None) -> str:
    """Upload files to the device; one line per file or ``error: ...`` (never raises)."""
    return zsend_status(client_url, token, port, paths, contents=contents, cmd=cmd,
                        timeout_ms=timeout_ms, overwrite=overwrite, progress=progress)[0]


# --------------------------------------------------------------------------- #
# zrecv
# --------------------------------------------------------------------------- #
@dataclass
class _Saved:
    name: str
    path: str
    size: int
    crc32: int


class _SaveDir:
    """Receive callback: files land only inside ``dest_dir`` (temp name, renamed when complete)."""

    def __init__(self, dest_dir: str, overwrite: bool,
                 on_offer: Optional[Callable[[Optional[int]], None]] = None) -> None:
        self.dir = os.path.abspath(dest_dir)
        self.overwrite = overwrite
        self.on_offer = on_offer
        self.saved: list[_Saved] = []
        self.refused: list[str] = []
        self._open: list[_TempSink] = []

    def __call__(self, name: str, size: Optional[int]) -> Optional["_TempSink"]:
        base = safe_name(name)
        real = os.path.realpath(self.dir)
        if base is None or os.path.dirname(os.path.realpath(os.path.join(real, base))) != real:
            self.refused.append(name)
            return None
        if self.on_offer:
            self.on_offer(size)
        try:
            fd, tmp = tempfile.mkstemp(prefix=f".{base}.", suffix=".part", dir=self.dir)
            fh = os.fdopen(fd, "wb")
        except OSError as exc:
            raise _cannot_write(os.path.join(self.dir, base), exc) from None
        sink = _TempSink(self, base, tmp, fh, size)
        self._open.append(sink)
        return sink

    def final_path(self, base: str) -> str:
        path = os.path.join(self.dir, base)
        n = 0
        while not self.overwrite and os.path.exists(path):
            n += 1
            path = os.path.join(self.dir, f"{base}.{n}")
        return path

    def discard_partial(self) -> None:
        for sink in self._open:
            sink.discard()
        self._open.clear()


class _TempSink:
    def __init__(self, owner: _SaveDir, base: str, tmp: str, fh: Any,
                 expected: Optional[int] = None) -> None:
        self.owner, self.base, self.tmp, self.fh = owner, base, tmp, fh
        self.expected = expected
        self.crc = 0
        self.size = 0

    def write(self, data: bytes) -> None:
        try:
            self.fh.write(data)
        except OSError as exc:  # disk full …
            raise _cannot_write(self.tmp, exc) from None
        self.crc = zlib.crc32(data, self.crc)
        self.size += len(data)

    def close(self) -> None:
        path = self.owner.final_path(self.base)
        try:
            self.fh.close()
            os.replace(self.tmp, path)
        except OSError as exc:
            raise _cannot_write(path, exc) from None
        self.owner._open.remove(self)
        self.owner.saved.append(_Saved(self.base, path, self.size, self.crc))

    def discard(self) -> None:
        try:
            self.fh.close()
        finally:
            try:
                os.unlink(self.tmp)
            except OSError:
                pass


def _saved_entry(s: _Saved) -> dict[str, Any]:
    return {"name": s.name, "bytes": s.size, "crc32": _crc(s.crc32), "path": s.path}


def receive_session(client_url: str, token: Optional[str], port: str,
                    remote: Sequence[str], dest_dir: Optional[str] = None, *,
                    cmd: Optional[str] = None, timeout_ms: Optional[int] = None,
                    overwrite: bool = False, progress: Progress = None) -> dict[str, Any]:
    """Download files from the device into ``dest_dir`` (default: :func:`default_dir`).

    Returns ``{"ok": True, "files", "missing", "refused"}`` or ``{"ok": False, "error",
    "files"}`` where ``files`` are the ones saved before the failure.
    """
    dest = os.path.abspath(os.path.expanduser(dest_dir)) if dest_dir else default_dir()
    try:
        os.makedirs(dest, exist_ok=True)
    except OSError as exc:
        return {"ok": False, "error": f"cannot create {dest}: {exc.strerror or exc}",
                "files": []}
    box: dict[str, _Clock] = {}
    saver = _SaveDir(dest, overwrite, on_offer=lambda size: box["clock"].announce(size))
    command = recv_command(list(remote), cmd)

    def engine(t: zp.Transport, clock: _Clock, _baud: Optional[int]) -> zp.SessionResult:
        box["clock"] = clock
        return zp.receive_files(t, saver, deadline=clock, progress=clock.progress(progress),
                                abort_on_error=False)

    try:
        _run_session(client_url, token, port, command, "download", clamp_timeout(timeout_ms),
                     0, engine)
    except ZmodemClientError as exc:
        partial = saver._open[-1] if saver._open else None
        saver.discard_partial()
        files = [_saved_entry(s) for s in saver.saved]
        said = f"; device said: {exc.hint}" if exc.hint else ""
        if exc.cancelled and partial is not None:  # gone after a ZFILE: say where it stopped
            total = partial.expected if partial.expected is not None else "?"
            error = (f"device cancelled the transfer at {partial.size}/{total} bytes of "
                     f"{partial.base}")
            return {"ok": False, "error": error + said, "files": files}
        if exc.cancelled and not saver.saved:  # lsz: missing file → session, CAN, no file
            error = "device sent no file (missing/unreadable remote file?)"
            return {"ok": False, "error": error + said, "files": files}
        return {"ok": False, "error": str(exc), "files": files}
    except BaseException:
        saver.discard_partial()
        raise
    files = [_saved_entry(s) for s in saver.saved]
    got = {s.name for s in saver.saved}
    missing = [n for n in remote if safe_name(n) not in got]
    if not files:
        error = "device sent no file" + (f" (refused unsafe name(s): {', '.join(saver.refused)})"
                                         if saver.refused else "")
        return {"ok": False, "error": error, "files": []}
    return {"ok": True, "files": files, "missing": missing, "refused": saver.refused}


def _text_of(path: str) -> Optional[tuple[str, int]]:
    """``(text, unread_bytes)`` of the first :data:`INLINE_READ` bytes, or ``None`` if binary.

    Text = no NUL and < 5 % undecodable bytes (decoded with replacement, so a serial log with
    one bad byte is still shown)."""
    try:
        with open(path, "rb") as fh:
            raw = fh.read(INLINE_READ)
            rest = max(0, os.fstat(fh.fileno()).st_size - len(raw))
    except OSError:
        return None
    if b"\x00" in raw:
        return None
    text = raw.decode("utf-8", "replace")
    bad = text.count("�") - raw.decode("utf-8", "ignore").count("�")
    if raw and bad > 0.05 * len(raw):
        return None
    return text, rest


def format_recv(env: dict[str, Any], *, inline: bool = False, cap: Optional[int] = None) -> str:
    lines: list[str] = []
    texts: list[tuple[str, str, str, int]] = []
    for f in env.get("files") or []:
        line = f"received {f['name']} {f['bytes']}B crc={f['crc32']} -> {f['path']}"
        if inline and env.get("ok"):
            got = _text_of(f["path"])
            if got is None:
                line += " (binary, not shown)"
            else:
                texts.append((f["name"], f["path"], *got))
        lines.append(line)
    if not env.get("ok"):
        return "\n".join([*lines, f"error: {env.get('error')}"])
    lines += [f"not received {n}" for n in env.get("missing") or []]
    lines += [f"skipped {n} (unsafe name)" for n in env.get("refused") or []]
    budget = cap if cap is not None else 4000
    for name, path, text, unread in texts:
        lines.append(f"--- {name}")
        shown = text[:max(0, budget)]
        budget -= len(shown)
        left = len(text) - len(shown) + unread
        lines.append(f"{shown}…+{left} chars, full file at {path}" if left else shown)
    return "\n".join(lines)


def recv_ok(env: dict[str, Any]) -> bool:
    """Complete success: every requested file received, none refused (the CLI's exit status)."""
    return bool(env.get("ok")) and not env.get("missing") and not env.get("refused")


def zrecv_status(client_url: str, token: Optional[str], port: str,
                 remote: Union[Sequence[str], str], dest_dir: Optional[str] = None, *,
                 cmd: Optional[str] = None, timeout_ms: Optional[int] = None,
                 overwrite: bool = False, inline: bool = False, cap: Optional[int] = None,
                 progress: Progress = None) -> tuple[str, bool]:
    """:func:`zrecv` plus :func:`recv_ok` of the result (never raises)."""
    names = [remote] if isinstance(remote, str) else list(remote)
    try:
        env = receive_session(client_url, token, port, names, dest_dir, cmd=cmd,
                              timeout_ms=timeout_ms, overwrite=overwrite, progress=progress)
        return format_recv(env, inline=inline, cap=cap), recv_ok(env)
    except Exception as exc:  # noqa: BLE001 - the contract is "never raise"
        return f"error: {type(exc).__name__}: {exc}", False


def zrecv(client_url: str, token: Optional[str], port: str, remote: Union[Sequence[str], str],
          dest_dir: Optional[str] = None, *, cmd: Optional[str] = None,
          timeout_ms: Optional[int] = None, overwrite: bool = False, inline: bool = False,
          cap: Optional[int] = None, progress: Progress = None) -> str:
    """Download files; one line per file (plus inline text) or ``error: ...`` (never raises)."""
    return zrecv_status(client_url, token, port, remote, dest_dir, cmd=cmd,
                        timeout_ms=timeout_ms, overwrite=overwrite, inline=inline, cap=cap,
                        progress=progress)[0]
