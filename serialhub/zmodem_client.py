"""Agent-side ZMODEM over the daemon's ``/ws`` relay (no browser, no host lrzsz).

zsend / zrecv 的编排层：引擎（``zmodem_proto``）跑在**调用方进程**里，文件留在 agent 所在的
机器上，daemon 不增加任何文件访问面（且可以是远程的）。流程与
``tests/zmodem_client/zclient.mjs`` 一致（docs/ARCHITECTURE.md §14）：

1. 先连 ``/ws``（token 走 ``Authorization: Bearer``），订阅本端口并 ping/pong 确认已订阅；
2. 记下环形缓冲游标，经普通 REST send 发 ``cmd``（``""`` = 设备自己会开始）；
3. 等本端口的 ``zmodem`` ``start`` 广播（方向须匹配：zsend = upload，zrecv = download），
   :data:`START_TIMEOUT` 内没有 → ``error: device did not start ZMODEM`` + 设备最后几行做提示；
4. ``zmodem_claim`` → 引擎经 ``zmodem_data`` / ``zmodem_write``（base64）跑完整个会话；
5. 成功：再收 :data:`TAIL_WAIT` 秒会话外字节，连同引擎交回的 tail 随 ``zmodem_end`` 的
   ``tail_b64`` 交回（设备的 ``rz done``、提示符因此照常进环）；失败：``zmodem_end abort:true``。

``zsend`` / ``zrecv`` 返回给 agent 的短文本（用户错误一律 ``error: ...``，绝不抛异常）；
``send_session`` / ``receive_session`` 返回结构化 dict，供完整 MCP profile 使用。
"""
from __future__ import annotations

import base64
import binascii
import json
import os
import re
import shlex
import tempfile
import time
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, Sequence

from . import zmodem_proto as zp
from .client import Client
from .models import strip_ansi
from .zmodem import TAIL_MAX_BYTES

START_TIMEOUT = 10.0          #: 发出 cmd 后等会话开始的时长（秒）
TAIL_WAIT = 0.2               #: 会话结束后再收会话外字节的时长（秒）
CLOSE_WAIT = 3.0              #: 发 zmodem_end 后等 zmodem_closed 的时长（秒）
CANCEL_WAIT = 0.5             #: 对端取消（CAN）后再收设备文本（如 ``sz done rc=128``）的时长
HINT_LINES = 3                #: 失败提示里附带的设备行数
TIMEOUT_MS, TIMEOUT_MAX_MS = 120_000, 1_800_000
INLINE_MAX = 1 << 20          #: zsend 内联 content 解码后的上限
DEFAULT_RECV_CMD = "sz {remote}"

Progress = Optional[Callable[[str, int, Optional[int]], None]]


class ZmodemClientError(Exception):
    """用户可见的失败；``files`` = 失败前已完成的文件；``cancelled`` = 设备发了 CAN；
    ``hint`` = 设备随后打印的几行。"""

    def __init__(self, message: str, files: Optional[list[Any]] = None, *,
                 cancelled: bool = False, hint: str = "") -> None:
        super().__init__(message)
        self.files = list(files or [])
        self.cancelled = cancelled
        self.hint = hint


# --------------------------------------------------------------------------- #
# helpers shared by lite / CLI / MCP
# --------------------------------------------------------------------------- #
def clamp_timeout(timeout_ms: Optional[int]) -> int:
    if timeout_ms is None:
        return TIMEOUT_MS
    return max(1000, min(TIMEOUT_MAX_MS, int(timeout_ms)))


def safe_name(name: str) -> Optional[str]:
    """Basename of a remote/device-supplied name; ``None`` if unusable (empty, ``.``, ``..``)."""
    base = re.split(r"[\\/]", str(name))[-1].strip()
    if base in ("", ".", "..") or any(ord(c) < 32 for c in base):
        return None
    return base


def inline_content(content: str, name: str, encoding: Optional[str] = "text") -> tuple[str, bytes]:
    """Decode a model-supplied file body. Raises ``ValueError`` with a user-facing message."""
    remote = safe_name(name)
    if remote is None:
        raise ValueError(f"bad name {name!r}")
    if encoding in (None, "text"):
        body = content.encode("utf-8")
    elif encoding == "base64":
        try:
            body = base64.b64decode("".join(content.split()), validate=True)
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


def _crc(value: int) -> str:
    return f"{value & 0xFFFFFFFF:08X}"


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

    def __init__(self, ws: Any, port: str, session: str) -> None:
        self.ws, self.port, self.session = ws, port, session
        self.closed: Optional[str] = None

    def read(self, timeout: float) -> bytes:
        until = time.monotonic() + max(0.0, timeout)
        while True:
            if self.closed is not None:
                raise zp.ZmodemError(f"daemon ended the session ({self.closed})")
            try:
                raw = self.ws.recv(timeout=max(0.0, until - time.monotonic()))
            except TimeoutError:
                return b""
            except Exception as exc:  # noqa: BLE001 - connection lost
                raise zp.ZmodemError(f"daemon connection lost: {exc}") from None
            msg = _json(raw)
            if msg.get("port") != self.port:
                continue
            kind = msg.get("type")
            if kind == "zmodem_data" and msg.get("session") == self.session:
                return base64.b64decode(msg.get("b64") or "")
            if kind == "zmodem_closed" and msg.get("session") == self.session:
                self.closed = str(msg.get("reason"))
            elif kind == "zmodem_error" and msg.get("session") in (None, self.session):
                raise zp.ZmodemError(f"daemon: {msg.get('error')}")
            if time.monotonic() >= until:
                return b""

    def write(self, data: bytes) -> None:
        try:
            self.ws.send(json.dumps({"type": "zmodem_write", "port": self.port,
                                     "session": self.session,
                                     "b64": base64.b64encode(data).decode("ascii")}))
        except Exception as exc:  # noqa: BLE001 - connection lost
            raise zp.ZmodemError(f"daemon connection lost: {exc}") from None

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

    def end(self, *, abort: bool, tail: bytes = b"") -> Optional[str]:
        """Send ``zmodem_end`` and wait for ``zmodem_closed``; returns its reason."""
        msg: dict[str, Any] = {"type": "zmodem_end", "port": self.port,
                               "session": self.session, "abort": abort}
        # tail 会经检测器回注为 rx 行：去掉 ZDLE/CAN（0x18），设备回显的我方帧头
        # （如 lsz 退出后 shell 回显的 ZRINIT）不会被当成新会话的起始头
        tail = tail.replace(b"\x18", b"")
        if tail and not abort:
            msg["tail_b64"] = base64.b64encode(tail[:TAIL_MAX_BYTES]).decode("ascii")
        try:
            self.ws.send(json.dumps(msg))
        except Exception:  # noqa: BLE001 - socket gone: daemon aborts on disconnect
            return None
        until = time.monotonic() + CLOSE_WAIT
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


def _tail_hint(tail: bytes) -> str:
    text = strip_ansi(tail.decode("utf-8", "replace"))
    lines = [re.sub(r"[\x00-\x1f\x7f]", "", ln).strip() for ln in text.splitlines()]
    return " | ".join([ln for ln in lines if ln][:HINT_LINES])


def _run_session(base_url: str, token: Optional[str], port: str, cmd: str, direction: str,
                 timeout_ms: int,
                 engine: Callable[[zp.Transport, float], zp.SessionResult]) -> zp.SessionResult:
    """Run one claimed ZMODEM session; returns the engine result with the full tail."""
    deadline = time.monotonic() + clamp_timeout(timeout_ms) / 1000.0
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
        if cmd:
            sent = rest.send(port, cmd)
            if not sent.get("ok"):
                raise ZmodemClientError(str(sent.get("error") or "send failed"))
        start = _recv_until(
            ws, lambda m: m.get("type") == "zmodem" and m.get("event") == "start"
            and m.get("port") == port, min(deadline, time.monotonic() + START_TIMEOUT))
        if start is None:
            hint = _device_hint(rest, port, cursor, cmd)
            raise ZmodemClientError("device did not start ZMODEM"
                                    + (f"; device said: {hint}" if hint else ""))
        if start.get("direction") != direction:
            raise ZmodemClientError(
                f"device started a {_DIRECTION.get(str(start.get('direction')), '?')}, "
                f"expected a {_DIRECTION[direction]}")
        ws.send(json.dumps({"type": "zmodem_claim", "port": port}))
        reply = _recv_until(ws, lambda m: m.get("port") == port and m.get("type") in (
            "zmodem_claimed", "zmodem_error"), time.monotonic() + 5)
        if reply is None or reply.get("type") == "zmodem_error":
            code = (reply or {}).get("code")
            if code == "claimed_by_other":
                raise ZmodemClientError(f"ZMODEM session on {port!r} was claimed by another "
                                        "client (a browser tab?); close it and retry")
            raise ZmodemClientError(f"cannot claim ZMODEM session: "
                                    f"{(reply or {}).get('error') or 'no reply'}")
        transport = _WsTransport(ws, port, str(reply.get("session")))
        try:
            result = engine(transport, deadline)
        except zp.ZmodemError as exc:
            if not exc.cancelled:
                transport.end(abort=True)
                raise ZmodemClientError(f"transfer failed: {exc}", exc.files) from None
            # 设备自己取消（如 lsz 找不到文件）：它已退出，其后输出是普通文本——以 done
            # 结束并把文本交回（进环），而不是 abort（排空会把它丢掉）
            tail = (exc.tail + transport.drain(CANCEL_WAIT)).lstrip(b"\x18\x08")
            transport.end(abort=False, tail=tail)
            hint = _tail_hint(tail)
            raise ZmodemClientError("device cancelled the transfer"
                                    + (f"; device said: {hint}" if hint else ""), exc.files,
                                    cancelled=True, hint=hint) from None
        except BaseException:
            transport.end(abort=True)
            raise
        result.tail = result.tail + transport.drain(TAIL_WAIT)
        transport.end(abort=False, tail=result.tail)
        return result
    finally:
        conn.__exit__(None, None, None)


# --------------------------------------------------------------------------- #
# zsend
# --------------------------------------------------------------------------- #
def send_session(client_url: str, token: Optional[str], port: str,
                 paths: Sequence[str] = (), *, contents: Sequence[tuple[str, bytes]] = (),
                 cmd: str = "rz", timeout_ms: Optional[int] = None,
                 progress: Progress = None) -> dict[str, Any]:
    """Upload local files and/or in-memory ``(name, bytes)``; returns ``{"ok", "files"|"error"}``."""
    import io  # noqa: PLC0415

    specs: list[zp.FileSpec] = []
    handles: list[Any] = []
    try:
        for path in paths:
            p = os.path.expanduser(str(path))
            if not os.path.isfile(p):
                return {"ok": False, "error": f"no such file: {path}"}
            try:
                fh = open(p, "rb")  # noqa: SIM115 - closed in finally
            except OSError as exc:
                return {"ok": False, "error": f"cannot read {path}: {exc.strerror or exc}"}
            handles.append(fh)
            st = os.fstat(fh.fileno())
            specs.append(zp.FileSpec(os.path.basename(p), fh, st.st_size, int(st.st_mtime)))
        now = int(time.time())
        for name, body in contents:
            specs.append(zp.FileSpec(name, io.BytesIO(body), len(body), now))
        if not specs:
            return {"ok": False, "error": "nothing to send"}
        try:
            res = _run_session(client_url, token, port, cmd, "upload", clamp_timeout(timeout_ms),
                               lambda t, dl: zp.send_files(t, specs, deadline=dl,
                                                           progress=progress))
        except ZmodemClientError as exc:
            return {"ok": False, "error": str(exc)}
        files = [{"name": f.name, "bytes": f.size, "crc32": _crc(f.crc32),
                  "seconds": round(f.seconds, 1), "skipped": f.skipped} for f in res.files]
        return {"ok": True, "files": files}
    finally:
        for fh in handles:
            fh.close()


def format_send(env: dict[str, Any]) -> str:
    if not env.get("ok"):
        return f"error: {env.get('error')}"
    lines = []
    for f in env["files"]:
        if f["skipped"]:
            lines.append(f"skipped {f['name']} (device refused it)")
        else:
            lines.append(f"sent {f['name']} {f['bytes']}B crc={f['crc32']} in {f['seconds']}s")
    return "\n".join(lines)


def zsend(client_url: str, token: Optional[str], port: str, paths: Sequence[str] = (), *,
          contents: Sequence[tuple[str, bytes]] = (), cmd: str = "rz",
          timeout_ms: Optional[int] = None, progress: Progress = None) -> str:
    """Upload files to the device; one line per file or ``error: ...`` (never raises)."""
    try:
        return format_send(send_session(client_url, token, port, paths, contents=contents,
                                        cmd=cmd, timeout_ms=timeout_ms, progress=progress))
    except Exception as exc:  # noqa: BLE001 - the contract is "never raise"
        return f"error: {type(exc).__name__}: {exc}"


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

    def __init__(self, dest_dir: str, overwrite: bool) -> None:
        self.dir = dest_dir
        self.overwrite = overwrite
        self.saved: list[_Saved] = []
        self.refused: list[str] = []
        self._open: list[_TempSink] = []

    def __call__(self, name: str, size: Optional[int]) -> Optional["_TempSink"]:
        base = safe_name(name)
        real = os.path.realpath(self.dir)
        if base is None or os.path.dirname(os.path.realpath(os.path.join(real, base))) != real:
            self.refused.append(name)
            return None
        fd, tmp = tempfile.mkstemp(prefix=f".{base}.", suffix=".part", dir=self.dir)
        sink = _TempSink(self, base, tmp, os.fdopen(fd, "wb"))
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
    def __init__(self, owner: _SaveDir, base: str, tmp: str, fh: Any) -> None:
        self.owner, self.base, self.tmp, self.fh = owner, base, tmp, fh
        self.crc = 0
        self.size = 0

    def write(self, data: bytes) -> None:
        self.fh.write(data)
        self.crc = zlib.crc32(data, self.crc)
        self.size += len(data)

    def close(self) -> None:
        self.fh.close()
        path = self.owner.final_path(self.base)
        os.replace(self.tmp, path)
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


def receive_session(client_url: str, token: Optional[str], port: str,
                    remote: Sequence[str], dest_dir: str = ".", *, cmd: Optional[str] = None,
                    timeout_ms: Optional[int] = None, overwrite: bool = False,
                    progress: Progress = None) -> dict[str, Any]:
    """Download files from the device into ``dest_dir``; returns ``{"ok", "files"|"error"}``."""
    try:
        os.makedirs(dest_dir, exist_ok=True)
    except OSError as exc:
        return {"ok": False, "error": f"cannot create {dest_dir}: {exc.strerror or exc}"}
    saver = _SaveDir(dest_dir, overwrite)
    command = recv_command(list(remote), cmd)
    try:
        res = _run_session(client_url, token, port, command, "download",
                           clamp_timeout(timeout_ms),
                           lambda t, dl: zp.receive_files(t, saver, deadline=dl,
                                                          progress=progress))
    except ZmodemClientError as exc:
        saver.discard_partial()
        if exc.cancelled and not saver.saved:  # lsz: missing file → session, CAN, no file
            return {"ok": False, "error": "device sent no file"
                    + (f"; device said: {exc.hint}" if exc.hint else "")}
        return {"ok": False, "error": str(exc)}
    except BaseException:
        saver.discard_partial()
        raise
    files = [{"name": s.name, "bytes": s.size, "crc32": _crc(s.crc32), "path": s.path}
             for s in saver.saved]
    got = {s.name for s in saver.saved}
    missing = [n for n in remote if safe_name(n) not in got]
    if not files:
        hint = _tail_hint(res.tail)
        return {"ok": False, "error": "device sent no file"
                + (f"; device said: {hint}" if hint else ""), "refused": saver.refused}
    return {"ok": True, "files": files, "missing": missing, "refused": saver.refused}


def _text_of(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    return None if "\x00" in text else text


def format_recv(env: dict[str, Any], *, inline: bool = False, cap: Optional[int] = None) -> str:
    if not env.get("ok"):
        return f"error: {env.get('error')}"
    lines: list[str] = []
    texts: list[tuple[str, str, str]] = []
    for f in env["files"]:
        line = f"received {f['name']} {f['bytes']}B crc={f['crc32']} -> {f['path']}"
        if inline:
            text = _text_of(f["path"])
            if text is None:
                line += " (binary, not shown)"
            else:
                texts.append((f["name"], f["path"], text))
        lines.append(line)
    lines += [f"not received {n}" for n in env.get("missing") or []]
    lines += [f"skipped {n} (unsafe name)" for n in env.get("refused") or []]
    budget = cap if cap is not None else 4000
    for name, path, text in texts:
        lines.append(f"--- {name}")
        shown = text[:max(0, budget)]
        budget -= len(shown)
        if len(shown) < len(text):
            lines.append(f"{shown}…+{len(text) - len(shown)} chars, full file at {path}")
        else:
            lines.append(shown)
    return "\n".join(lines)


def zrecv(client_url: str, token: Optional[str], port: str, remote: Sequence[str] | str,
          dest_dir: str = ".", *, cmd: Optional[str] = None, timeout_ms: Optional[int] = None,
          overwrite: bool = False, inline: bool = False, cap: Optional[int] = None,
          progress: Progress = None) -> str:
    """Download files; one line per file (plus inline text) or ``error: ...`` (never raises)."""
    names = [remote] if isinstance(remote, str) else list(remote)
    try:
        env = receive_session(client_url, token, port, names, dest_dir, cmd=cmd,
                              timeout_ms=timeout_ms, overwrite=overwrite, progress=progress)
        return format_recv(env, inline=inline, cap=cap)
    except Exception as exc:  # noqa: BLE001 - the contract is "never raise"
        return f"error: {type(exc).__name__}: {exc}"
