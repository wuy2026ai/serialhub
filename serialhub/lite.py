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
     "description": "Send a line to a port and return the device's reply lines.",
     "input_schema": _obj({
         "port": _PORT,
         "data": _s("Text to send, without line ending"),
         "wait": _s("Regex; stop when a reply line matches"),
         "timeout_ms": _i("Max wait in ms, default 3000"),
         "eol": _s("Line ending, default crlf", enum=list(EOL_CHOICES)),
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
         "since": _i("Also check lines after this seq"),
     }, ["port", "regex"])},
    {"name": "search",
     "description": "Search the whole port buffer by regex.",
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


def _block(port: str, entries: list[Entry], empty_next: int, more: bool = False) -> str:
    """Render entries as the compact block (collapse, clip, 4000-char cap)."""
    # group consecutive identical (kind, text): [first_seq, last_seq, kind, text, count]
    groups: list[Optional[list[Any]]] = []
    for entry in entries:
        if entry is None:
            if groups and groups[-1] is not None:
                groups.append(None)
            continue
        seq, kind, text = entry
        text = _clip(text)
        last = groups[-1] if groups else None
        if last is not None and last[2] == kind and last[3] == text:
            last[1] = seq
            last[4] += 1
        else:
            groups.append([seq, seq, kind, text, 1])
    while groups and groups[-1] is None:
        groups.pop()

    out: list[str] = []
    size = 0
    first_seq: Optional[int] = None
    last_seq: Optional[int] = None
    for group in groups:
        if group is None:
            rendered = "--"
        else:
            seq, _end, kind, text, count = group
            rendered = f"{seq} {_prefix(kind)}{text}" + (f" (x{count})" if count > 1 else "")
        added = len(rendered) + (1 if out else 0)
        if size + added > MAX_BLOCK_CHARS:
            more = True
            break
        out.append(rendered)
        size += added
        if group is not None:
            first_seq = group[0] if first_seq is None else first_seq
            last_seq = group[1]
    if out and out[-1] == "--":
        out.pop()
    if last_seq is None:
        return f"{port} empty next={empty_next} more={int(more)}"
    header = f"{port} {first_seq}-{last_seq} next={last_seq} more={int(more)}"
    return "\n".join([header, *out])


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
    if "filter" in a:
        _regex(a["filter"], "filter")
    env = _ok(client.read(port, since_seq=a.get("since"), filter=a.get("filter"),
                          limit=limit))
    return _block(port, _entries(env.get("lines") or []),
                  int(env.get("next_seq") or 0), bool(env.get("has_more")))


def _tool_send(client: Client, a: dict[str, Any]) -> str:
    port = a["port"]
    pat = _regex(a["wait"], "wait") if "wait" in a else None
    timeout = _clamp(a.get("timeout_ms"), SEND_TIMEOUT_MS, 0, SEND_TIMEOUT_MAX)
    pos = _last_seq(client, port)
    sent = _ok(client.send(port, a["data"], eol=a.get("eol", "crlf")))
    start = time.monotonic()
    deadline = start + timeout / 1000.0
    last_new = start
    collected: list[dict[str, Any]] = []
    matched = extra = False
    while True:
        env = _ok(client.read(port, since_seq=pos, kinds="rx", limit=SEND_POLL_BATCH))
        new = env.get("lines") or []
        pos = int(env.get("next_seq") or pos)
        now = time.monotonic()
        if new:
            last_new = now
            if pat is not None:
                for idx, line in enumerate(new):
                    if pat.search(strip_ansi(str(line.get("text") or ""))):
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
    return head + "\n" + _block(port, _entries(collected), pos, extra)


def _tool_wait(client: Client, a: dict[str, Any]) -> str:
    port = a["port"]
    _regex(a["regex"], "wait")
    timeout = _clamp(a.get("timeout_ms"), WAIT_TIMEOUT_MS, 0, WAIT_TIMEOUT_MAX)
    since = a["since"] if "since" in a else _last_seq(client, port)
    env = _ok(client.wait(port, a["regex"], timeout_ms=timeout, since_seq=since))
    if env.get("matched") and env.get("line"):
        return "matched\n" + _block(port, _entries([env["line"]]), since)
    return "timeout\n" + _block(port, [], since)


def _tool_search(client: Client, a: dict[str, Any]) -> str:
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
    return _block(port, entries, int(env.get("next_seq") or 0), len(hits) >= limit)


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
