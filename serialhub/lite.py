"""serialhub "lite" agent interface: six compact tools, plain-text results.

面向小模型的精简工具集（单一事实来源）：``TOOLS`` 是 Anthropic Messages 格式的
六个工具定义，``call()`` 经瘦客户端 ``Client`` 调用守护进程并**总是**返回纯文本
（用户错误返回 ``error: <message>``，绝不抛异常）。MCP lite profile
（``mcp_lite.py``）与 CLI ``serialhub tools`` / ``serialhub call`` 都复用这里。

Compact line format (send/read/wait/search)::

    <port> <first_seq>-<last_seq> next=<cursor> more=<0|1>
    <seq> <text>          rx
    <seq> ><text>         tx (sent by host)
    <seq> #<text>         mark
    <seq> <text> (x12)    12 consecutive identical lines collapsed; seq = first

Empty result: ``<port> empty next=<cursor> more=0``. Pass ``next`` back as ``since``.
"""
from __future__ import annotations

import copy
import re
import time
from typing import Any, Callable, Optional

from .client import Client
from .models import KIND_MARK, KIND_TX, strip_ansi

# --------------------------------------------------------------------------- #
# limits
# --------------------------------------------------------------------------- #
MAX_LINE_CHARS = 240          # longer lines are cut to this + "…+<dropped>"
MAX_BLOCK_CHARS = 4000        # cap on one reply's line block
QUIET_MS = 300                # send without wait: stop after this long with no new line
POLL_MS = 50                  # send: poll interval while collecting the reply
SEND_TIMEOUT_MS, SEND_TIMEOUT_MAX = 3000, 60000
WAIT_TIMEOUT_MS, WAIT_TIMEOUT_MAX = 10000, 120000
READ_LIMIT, READ_LIMIT_MAX = 50, 200
SEARCH_LIMIT, SEARCH_LIMIT_MAX = 20, 100
CONTEXT_MAX = 5
SEND_POLL_BATCH = 500         # lines per poll request while collecting a send reply
KIND_LOOKUP_MAX = 2000        # search with context: max tx/mark lines looked up for prefixes
EOL_CHOICES = ("crlf", "lf", "cr", "none")
CONTROL_ACTIONS = ("open", "close", "clear", "mark")


def _obj(props: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {"type": "object", "properties": props, "required": required,
            "additionalProperties": False}


def _s(desc: str, **extra: Any) -> dict[str, Any]:
    return {"type": "string", "description": desc, **extra}


def _i(desc: str) -> dict[str, Any]:
    return {"type": "integer", "description": desc}


_PORT = _s("Port name")
_SINCE = _i("Cursor: the next= value from last output")

TOOLS: list[dict[str, Any]] = [
    {"name": "ports",
     "description": "List ports: name, open/closed, baud, device, last seq.",
     "input_schema": _obj({}, [])},
    {"name": "send",
     "description": "Send a line and return the reply lines (after 0.3 s of silence unless wait is set).",
     "input_schema": _obj({
         "port": _PORT,
         "data": _s("Text to send, without line ending"),
         "wait": _s("Regex for reply output, not the echoed command"),
         "timeout_ms": _i("Max wait in ms, default 3000"),
         "eol": _s("Line ending; default: port setting", enum=list(EOL_CHOICES)),
     }, ["port", "data"])},
    {"name": "read",
     "description": "Read buffered lines after since, or the latest lines if since is omitted.",
     "input_schema": _obj({
         "port": _PORT,
         "since": _SINCE,
         "limit": _i("Max lines, default 50, max 200"),
         "filter": _s("Regex; only matching lines"),
     }, ["port"])},
    {"name": "wait",
     "description": "Block until a new line matches regex, or timeout.",
     "input_schema": _obj({
         "port": _PORT,
         "regex": _s("Regex to wait for"),
         "timeout_ms": _i("Max wait in ms, default 10000"),
         "since": _i("Pass previous next= to not miss lines"),
     }, ["port", "regex"])},
    {"name": "search",
     "description": "Search the whole port buffer by regex (does not move the cursor).",
     "input_schema": _obj({
         "port": _PORT,
         "regex": _s("Regex to search for"),
         "limit": _i("Max hits, default 20, max 100"),
         "context": _i("Lines around each hit, 0-5"),
     }, ["port", "regex"])},
    {"name": "control",
     "description": "Open, close, clear or mark a port.",
     "input_schema": _obj({
         "port": _PORT,
         "action": _s("Action", enum=list(CONTROL_ACTIONS)),
         "baud": _i("Baud rate (open only)"),
         "label": _s("Marker text (mark only)"),
     }, ["port", "action"])},
]

_BY_NAME = {tool["name"]: tool for tool in TOOLS}


def to_anthropic() -> list[dict[str, Any]]:
    """Tool definitions in Anthropic Messages format (a deep copy of ``TOOLS``)."""
    return copy.deepcopy(TOOLS)


def to_openai() -> list[dict[str, Any]]:
    """Tool definitions in OpenAI Chat Completions ``tools`` format."""
    return [{"type": "function",
             "function": {"name": t["name"], "description": t["description"],
                          "parameters": copy.deepcopy(t["input_schema"])}}
            for t in TOOLS]


# --------------------------------------------------------------------------- #
# errors / argument validation
# --------------------------------------------------------------------------- #
class LiteError(Exception):
    """A user-facing error; ``call()`` renders it as ``error: <message>``."""


_INT_RE = re.compile(r"^-?\d+$")


def _validate(name: str, args: Any) -> dict[str, Any]:
    """Check ``args`` against the tool's schema; lenient int coercion ("50" -> 50)."""
    if args is None:
        args = {}
    if not isinstance(args, dict):
        raise LiteError("arguments must be a JSON object")
    schema = _BY_NAME[name]["input_schema"]
    props = schema["properties"]
    unknown = sorted(set(args) - set(props))
    if unknown:
        raise LiteError(f"unknown argument(s) for {name}: {', '.join(unknown)}")
    clean: dict[str, Any] = {}
    for key, value in args.items():
        if value is None:
            continue
        spec = props[key]
        if spec["type"] == "integer":
            if isinstance(value, bool):
                raise LiteError(f"{key} must be an integer")
            if isinstance(value, float) and value.is_integer():
                value = int(value)
            elif isinstance(value, str) and _INT_RE.match(value.strip()):
                value = int(value.strip())
            elif not isinstance(value, int):
                raise LiteError(f"{key} must be an integer")
        elif spec["type"] == "string" and not isinstance(value, str):
            raise LiteError(f"{key} must be a string")
        if "enum" in spec and value not in spec["enum"]:
            raise LiteError(f"{key} must be one of: {', '.join(spec['enum'])}")
        clean[key] = value
    missing = [key for key in schema["required"] if key not in clean]
    if missing:
        raise LiteError(f"missing required argument(s) for {name}: {', '.join(missing)}")
    return clean


def _clamp(value: Optional[int], default: int, lo: int, hi: int) -> int:
    return default if value is None else max(lo, min(hi, int(value)))


def _regex(pattern: str, what: str) -> "re.Pattern[str]":
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise LiteError(f"bad {what} regex {pattern!r}: {exc}") from None


def _ok(env: dict[str, Any]) -> dict[str, Any]:
    """Unwrap a client envelope or raise its error."""
    if not env.get("ok"):
        raise LiteError(str(env.get("error") or "unknown error"))
    return env


# --------------------------------------------------------------------------- #
# compact block formatter
# --------------------------------------------------------------------------- #
# An entry is ``(seq, kind, text)``; ``None`` is a "--" separator (search windows).
Entry = Optional[tuple[int, str, str]]


def _clip(text: str) -> str:
    text = strip_ansi(text)
    if len(text) > MAX_LINE_CHARS:
        return f"{text[:MAX_LINE_CHARS]}…+{len(text) - MAX_LINE_CHARS}"
    return text


def _prefix(kind: str) -> str:
    return ">" if kind == KIND_TX else "#" if kind == KIND_MARK else ""


class _Rendered:
    """Rendered line block: ``lines`` plus the seq range and trimming facts."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.first: Optional[int] = None
        self.last: Optional[int] = None
        self.capped = False     # keep="head": lines left out at the END
        self.skipped = 0        # keep="tail": underlying lines left out at the FRONT


def _render(entries: list[Entry], *, collapse: bool, keep: str = "head") -> _Rendered:
    """Clip/collapse entries and apply the 4000-char cap.

    ``keep="head"`` keeps the oldest lines (cursor continuation: ``more=1``);
    ``keep="tail"`` keeps the newest lines and counts the trimmed front lines.
    Collapsing merges only *adjacent* identical buffer lines, so it is used for
    unfiltered blocks only (never for search hits or filtered reads).
    """
    # group: [first_seq, last_seq, kind, text, count]; None = separator
    groups: list[Optional[list[Any]]] = []
    for entry in entries:
        if entry is None:
            if groups and groups[-1] is not None:
                groups.append(None)
            continue
        seq, kind, text = entry
        text = _clip(text)
        last = groups[-1] if groups else None
        if collapse and last is not None and last[2] == kind and last[3] == text:
            last[1] = seq
            last[4] += 1
        else:
            groups.append([seq, seq, kind, text, 1])
    while groups and groups[-1] is None:
        groups.pop()

    def show(group: Optional[list[Any]]) -> str:
        if group is None:
            return "--"
        seq, _end, kind, text, count = group
        return f"{seq} {_prefix(kind)}{text}" + (f" (x{count})" if count > 1 else "")

    res = _Rendered()
    kept: list[Optional[list[Any]]] = []
    size = 0
    order = groups if keep == "head" else list(reversed(groups))
    for pos, group in enumerate(order):
        added = len(show(group)) + (1 if kept else 0)
        if size + added > MAX_BLOCK_CHARS:
            rest = order[pos:]
            if keep == "head":
                res.capped = True
            else:
                res.skipped = sum(g[4] for g in rest if g is not None)
            break
        kept.append(group)
        size += added
    if keep != "head":
        kept.reverse()
    while kept and kept[0] is None:
        kept.pop(0)
    while kept and kept[-1] is None:
        kept.pop()
    res.lines = [show(g) for g in kept]
    real = [g for g in kept if g is not None]
    if real:
        res.first, res.last = real[0][0], real[-1][1]
    return res


def _cursor_block(port: str, rendered: _Rendered, empty_next: int, more: bool = False,
                  *, dropped: int = 0, reset: bool = False) -> str:
    """``<port> <first>-<last> next=<cursor> more=<0|1>[ skipped=][ dropped=][ reset=1]``."""
    more = more or rendered.capped
    extra = ""
    if rendered.skipped:
        extra += f" skipped={rendered.skipped}"
    if dropped:
        extra += f" dropped={dropped}"
    if reset:
        extra += " reset=1"
    if rendered.last is None:
        return f"{port} empty next={empty_next} more={int(more)}{extra}"
    header = f"{port} {rendered.first}-{rendered.last} next={rendered.last} more={int(more)}{extra}"
    return "\n".join([header, *rendered.lines])


def _entries(lines: list[dict[str, Any]]) -> list[Entry]:
    return [(int(ln["seq"]), str(ln.get("kind") or "rx"), str(ln.get("text") or ""))
            for ln in lines]


def _last_seq(client: Client, port: str) -> int:
    """Current newest seq in a port's buffer (0 when empty)."""
    return int(_ok(client.read(port, limit=0)).get("next_seq") or 0)


# --------------------------------------------------------------------------- #
# tools
# --------------------------------------------------------------------------- #
def _tool_ports(client: Client, _a: dict[str, Any]) -> str:
    rows = []
    for p in _ok(client.list_ports()).get("ports") or []:
        last = (p.get("buffer") or {}).get("last_seq") or 0
        row = (f"{p.get('name')} {'open' if p.get('open') else 'closed'} "
               f"{p.get('baud')} {p.get('device')} last={last}")
        if p.get("last_error"):
            row += f" err={p['last_error']}"
        rows.append(row)
    return "\n".join(rows) or "no ports"


def _tool_read(client: Client, a: dict[str, Any]) -> str:
    port = a["port"]
    limit = _clamp(a.get("limit"), READ_LIMIT, 1, READ_LIMIT_MAX)
    flt = a.get("filter")
    if flt is not None:
        _regex(flt, "filter")
    since = a.get("since")
    env = _ok(client.read(port, since_seq=since, filter=flt, limit=limit))
    reset = False
    last = env.get("last_seq")
    if since is not None and last is not None and since > int(last):
        # Stale cursor (e.g. the daemon restarted and seq began again): serve the tail.
        env = _ok(client.read(port, filter=flt, limit=limit))
        since, reset = None, True
    keep = "tail" if since is None else "head"
    rendered = _render(_entries(env.get("lines") or []), collapse=flt is None, keep=keep)
    return _cursor_block(port, rendered, int(env.get("next_seq") or 0),
                         bool(env.get("has_more")), dropped=int(env.get("dropped") or 0),
                         reset=reset)


def _is_echo(text: str, data: str) -> bool:
    """A device echo of the command, optionally prompt-prefixed (``root@x:~# cmd``)."""
    return bool(data) and strip_ansi(text).rstrip().endswith(data.rstrip())


def _tool_send(client: Client, a: dict[str, Any]) -> str:
    port, data = a["port"], a["data"]
    pat = _regex(a["wait"], "wait") if "wait" in a else None
    timeout = _clamp(a.get("timeout_ms"), SEND_TIMEOUT_MS, 0, SEND_TIMEOUT_MAX)
    pos = _last_seq(client, port)
    # eol omitted -> None -> the daemon applies the port's configured EOL.
    sent = _ok(client.send(port, data, eol=a.get("eol")))
    start = time.monotonic()
    deadline = start + timeout / 1000.0
    last_new = start
    collected: list[dict[str, Any]] = []
    matched = extra = False
    echo_pending = bool(data.strip())  # skip the first echoed command line when matching
    while True:
        env = _ok(client.read(port, since_seq=pos, kinds="rx", limit=SEND_POLL_BATCH))
        new = env.get("lines") or []
        pos = int(env.get("next_seq") or pos)
        now = time.monotonic()
        if new:
            last_new = now
            if pat is not None:
                for idx, line in enumerate(new):
                    text = str(line.get("text") or "")
                    if echo_pending and _is_echo(text, data):
                        echo_pending = False
                        continue
                    if pat.search(strip_ansi(text)):
                        collected.extend(new[:idx + 1])
                        extra = idx + 1 < len(new) or bool(env.get("has_more"))
                        matched = True
                        break
                if matched:
                    break
            collected.extend(new)
        if pat is None and now - last_new >= QUIET_MS / 1000.0:
            break
        if now >= deadline:
            break
        if not env.get("has_more"):
            time.sleep(POLL_MS / 1000.0)
    head = f"sent {sent.get('sent_bytes', 0)}B"
    if pat is not None:
        head += " matched" if matched else " timeout"
    rendered = _render(_entries(collected), collapse=True)
    return head + "\n" + _cursor_block(port, rendered, pos, extra)


def _tool_wait(client: Client, a: dict[str, Any]) -> str:
    port = a["port"]
    _regex(a["regex"], "wait")
    timeout = _clamp(a.get("timeout_ms"), WAIT_TIMEOUT_MS, 0, WAIT_TIMEOUT_MAX)
    since = a["since"] if "since" in a else _last_seq(client, port)
    env = _ok(client.wait(port, a["regex"], timeout_ms=timeout, since_seq=since))
    line = env.get("line")
    if not (env.get("matched") and line):
        return "timeout\n" + _cursor_block(port, _Rendered(), since)
    # Show every line in (since, match] so next= never skips unseen lines;
    # over the cap, trim from the front so the matched line stays last.
    match_seq = int(line["seq"])
    got = _ok(client.read(port, since_seq=since))
    lines = [ln for ln in got.get("lines") or [] if int(ln["seq"]) <= match_seq]
    if not lines or int(lines[-1]["seq"]) != match_seq:
        lines.append(line)  # evicted meanwhile: still show the match itself
    rendered = _render(_entries(lines), collapse=True, keep="tail")
    return "matched\n" + _cursor_block(port, rendered, match_seq,
                                        dropped=int(got.get("dropped") or 0))


def _tool_search(client: Client, a: dict[str, Any]) -> str:
    """Search never moves the cursor: header is ``<port> hits=<n> more=<0|1>``."""
    port = a["port"]
    _regex(a["regex"], "search")
    limit = _clamp(a.get("limit"), SEARCH_LIMIT, 1, SEARCH_LIMIT_MAX)
    context = _clamp(a.get("context"), 0, 0, CONTEXT_MAX)
    env = _ok(client.search(port, a["regex"], context=context, limit=limit))
    hits = env.get("hits") or []
    entries: list[Entry] = []
    emitted = -1
    for hit in hits:
        window = [*hit.get("context_before", []), hit, *hit.get("context_after", [])]
        fresh = [ln for ln in window if int(ln["seq"]) > emitted]
        if not fresh:
            continue
        if entries and context and int(window[0]["seq"]) > emitted:
            entries.append(None)  # non-overlapping window: "--" like grep
        entries.extend(_entries(fresh))
        emitted = int(fresh[-1]["seq"])
    if context and entries:
        # REST context lines carry no ``kind``: look up tx/mark lines in one read.
        lo = min(e[0] for e in entries if e is not None)
        env_k = client.read(port, since_seq=lo - 1, kinds="tx,mark", limit=KIND_LOOKUP_MAX)
        kinds = {int(ln["seq"]): str(ln.get("kind")) for ln in env_k.get("lines") or []}
        entries = [e if e is None else (e[0], kinds.get(e[0], e[1]), e[2]) for e in entries]
    rendered = _render(entries, collapse=False)
    more = len(hits) >= limit or rendered.capped
    return "\n".join([f"{port} hits={len(hits)} more={int(more)}", *rendered.lines])


def _tool_control(client: Client, a: dict[str, Any]) -> str:
    port, action = a["port"], a["action"]
    if action == "open":
        env = _ok(client.open(port, baud=a.get("baud")))
        return f"open {env.get('baud')}"
    if action == "close":
        _ok(client.close(port))
        return "closed"
    if action == "clear":
        return f"cleared {_ok(client.clear(port)).get('cleared', 0)}"
    env = _ok(client.mark(port, a.get("label") or "mark"))
    return f"mark {env.get('seq')}"


_HANDLERS: dict[str, Callable[[Client, dict[str, Any]], str]] = {
    "ports": _tool_ports,
    "send": _tool_send,
    "read": _tool_read,
    "wait": _tool_wait,
    "search": _tool_search,
    "control": _tool_control,
}


def call(client: Client, name: str, args: Any) -> str:
    """Run one lite tool; always returns plain text (``error: ...`` on failure)."""
    try:
        if name not in _HANDLERS:
            raise LiteError(f"unknown tool {name!r}; tools: {', '.join(_HANDLERS)}")
        return _HANDLERS[name](client, _validate(name, args))
    except LiteError as exc:
        return f"error: {exc}"
    except Exception as exc:  # noqa: BLE001 - the contract is "never raise"
        return f"error: {type(exc).__name__}: {exc}"
