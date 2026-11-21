"""Tests for the compact "lite" agent interface (``serialhub/lite.py``).

A real uvicorn server runs in a thread on a free loopback port so the lite
tools exercise the genuine HTTP client path; every port is ``loop://``.
"""

from __future__ import annotations

import json
import re
import threading
import time

import pytest

from serialhub import lite
from serialhub.client import Client

NAMES = ["ports", "send", "read", "wait", "search", "control"]


@pytest.fixture
def live(free_tcp_port, make_config):
    """Started Hub + uvicorn on a free port; yields ``(hub, Client)``."""
    import uvicorn

    from serialhub.hub import Hub
    from serialhub.server import create_app

    cfg = make_config(ports=[
        {"name": "t", "device": "loop://", "autostart": True,
         "buffer_lines": 2000, "buffer_bytes": 1 << 22},
        {"name": "t2", "device": "loop://", "autostart": False,
         "buffer_lines": 2000, "buffer_bytes": 1 << 22},
    ])
    hub = Hub(cfg)
    hub.start()
    port = free_tcp_port()
    server = uvicorn.Server(uvicorn.Config(create_app(hub, cfg), host="127.0.0.1",
                                           port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started and time.time() < deadline:
        time.sleep(0.02)
    try:
        assert server.started, "uvicorn failed to start"
        yield hub, Client(f"http://127.0.0.1:{port}", timeout=10)
    finally:
        server.should_exit = True
        thread.join(10)
        hub.stop()


def _lines(text: str) -> list[str]:
    return text.split("\n")


def _header(text: str) -> dict[str, str]:
    """Parse the ``key=value`` fields of a block header line."""
    return dict(re.findall(r"(\w+)=(\S+)", text))


def _block_header(text: str) -> str:
    """The block header is the first line starting with ``<port> ``."""
    for line in _lines(text):
        if re.match(r"^\S+ (empty|\d+-\d+) next=", line):
            return line
    raise AssertionError(f"no block header in:\n{text}")


# --------------------------------------------------------------------------- #
# schema / budget
# --------------------------------------------------------------------------- #
def test_budget_and_names() -> None:
    tools = lite.to_anthropic()
    assert [t["name"] for t in tools] == NAMES
    assert len(json.dumps(tools, ensure_ascii=False, separators=(",", ":"))) <= 3000
    for spec in tools:
        assert set(spec) == {"name", "description", "input_schema"}
        schema = spec["input_schema"]
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        assert "required" in schema
        for prop in schema["properties"].values():
            assert len(prop.get("description", "").split()) <= 8
    assert lite.to_anthropic() == lite.TOOLS


def test_openai_shape() -> None:
    tools = lite.to_openai()
    assert len(tools) == 6
    for spec, src in zip(tools, lite.TOOLS):
        assert spec["type"] == "function"
        fn = spec["function"]
        assert set(fn) == {"name", "description", "parameters"}
        assert fn["name"] == src["name"]
        assert fn["parameters"] == src["input_schema"]


# --------------------------------------------------------------------------- #
# read
# --------------------------------------------------------------------------- #
def test_read_tail_when_since_omitted(live) -> None:
    hub, client = live
    seqs = [hub.ingest("t", f"line {i}").seq for i in range(10)]
    out = lite.call(client, "read", {"port": "t", "limit": 3})
    lines = _lines(out)
    assert lines[0] == f"t {seqs[7]}-{seqs[9]} next={seqs[9]} more=0"
    assert lines[1:] == [f"{seqs[7]} line 7", f"{seqs[8]} line 8", f"{seqs[9]} line 9"]


def test_read_cursor_continuation(live) -> None:
    hub, client = live
    first = [hub.ingest("t", f"a{i}").seq for i in range(3)]
    out = lite.call(client, "read", {"port": "t", "since": 0})
    nxt = int(_header(out)["next"])
    assert nxt == first[-1]
    new = [hub.ingest("t", f"b{i}").seq for i in range(2)]
    out2 = lite.call(client, "read", {"port": "t", "since": nxt})
    assert _lines(out2)[1:] == [f"{new[0]} b0", f"{new[1]} b1"]
    assert int(_header(out2)["next"]) == new[-1]
    out3 = lite.call(client, "read", {"port": "t", "since": new[-1]})
    assert out3 == f"t empty next={new[-1]} more=0"


def test_read_limit_sets_more(live) -> None:
    hub, client = live
    seqs = [hub.ingest("t", f"x{i}").seq for i in range(5)]
    out = lite.call(client, "read", {"port": "t", "since": 0, "limit": 2})
    assert _lines(out)[0] == f"t {seqs[0]}-{seqs[1]} next={seqs[1]} more=1"


def test_read_filter(live) -> None:
    hub, client = live
    hub.ingest("t", "boot ok")
    keep = hub.ingest("t", "ERROR disk").seq
    hub.ingest("t", "idle")
    out = lite.call(client, "read", {"port": "t", "since": 0, "filter": "ERROR"})
    assert _lines(out)[1:] == [f"{keep} ERROR disk"]


def test_read_limit_clamped_to_200(live) -> None:
    hub, client = live
    for i in range(250):
        hub.ingest("t", str(i))
    out = lite.call(client, "read", {"port": "t", "since": 0, "limit": 5000})
    body = _lines(out)[1:]
    assert len(body) == 200
    assert _header(out)["more"] == "1"


def test_read_empty_buffer(live) -> None:
    _hub, client = live
    assert lite.call(client, "read", {"port": "t"}) == "t empty next=0 more=0"


# --------------------------------------------------------------------------- #
# compact format
# --------------------------------------------------------------------------- #
def test_format_prefixes_ansi_truncation_collapse(live) -> None:
    hub, client = live
    tx = hub.ingest("t", "help", kind="tx").seq
    mk = hub.mark("t", "step1")["seq"]
    ansi = hub.ingest("t", "\x1b[31mred\x1b[0m text").seq
    long_seq = hub.ingest("t", "z" * 300).seq
    dup = [hub.ingest("t", "same").seq for _ in range(3)]
    after = hub.ingest("t", "same", kind="tx").seq  # different kind: not collapsed
    out = lite.call(client, "read", {"port": "t", "since": 0})
    body = _lines(out)[1:]
    assert body == [
        f"{tx} >help",
        f"{mk} #step1",
        f"{ansi} red text",
        f"{long_seq} " + "z" * 240 + "…+60",
        f"{dup[0]} same (x3)",
        f"{after} >same",
    ]
    assert _header(out)["next"] == str(after)


def test_collapse_at_end_sets_next_to_last_collapsed(live) -> None:
    hub, client = live
    seqs = [hub.ingest("t", "tick").seq for _ in range(4)]
    out = lite.call(client, "read", {"port": "t", "since": 0})
    assert _lines(out) == [f"t {seqs[0]}-{seqs[-1]} next={seqs[-1]} more=0",
                           f"{seqs[0]} tick (x4)"]


def test_char_cap_sets_more_and_continuation_is_lossless(live) -> None:
    hub, client = live
    seqs = [hub.ingest("t", f"{i:03d}" + "y" * 200).seq for i in range(60)]
    seen: list[int] = []
    since = 0
    for _ in range(10):
        out = lite.call(client, "read", {"port": "t", "since": since, "limit": 200})
        if out.startswith("t empty"):
            break
        body = _lines(out)[1:]
        assert len("\n".join(body)) <= lite.MAX_BLOCK_CHARS
        seen.extend(int(line.split(" ", 1)[0]) for line in body)
        head = _header(out)
        since = int(head["next"])
        if head["more"] == "0":
            break
        assert since == seen[-1]
    assert seen == seqs  # no loss, no duplication


# --------------------------------------------------------------------------- #
# send
# --------------------------------------------------------------------------- #
def test_send_with_wait_matches_echo(live) -> None:
    _hub, client = live
    out = lite.call(client, "send", {"port": "t", "data": "hello", "wait": "hel+o"})
    lines = _lines(out)
    assert lines[0] == "sent 7B matched"
    assert lines[2].endswith(" hello")
    assert not any(" >" in line for line in lines[2:])  # rx only


def test_send_without_wait_quiet_period(live) -> None:
    _hub, client = live
    t0 = time.monotonic()
    out = lite.call(client, "send", {"port": "t", "data": "ping"})
    elapsed = time.monotonic() - t0
    lines = _lines(out)
    assert lines[0] == "sent 6B"
    assert lines[2].endswith(" ping")
    assert elapsed < 2.5


def test_send_timeout(live) -> None:
    _hub, client = live
    out = lite.call(client, "send", {"port": "t", "data": "foo", "wait": "never-here",
                                     "timeout_ms": 400})
    assert _lines(out)[0] == "sent 5B timeout"


def test_send_eol_variants(live) -> None:
    _hub, client = live
    assert _lines(lite.call(client, "send", {"port": "t", "data": "ab", "eol": "lf",
                                             "wait": "ab"}))[0] == "sent 3B matched"
    # a trailing lone CR is held by the daemon's line framer until the next byte
    assert _lines(lite.call(client, "send", {"port": "t", "data": "cd",
                                             "eol": "cr"}))[0] == "sent 3B"
    out1 = lite.call(client, "send", {"port": "t", "data": "ef", "eol": "none"})
    assert _lines(out1)[0] == "sent 2B"
    out2 = lite.call(client, "send", {"port": "t", "data": "gh", "wait": "efgh"})
    assert _lines(out2)[0] == "sent 4B matched"
    texts = [line.split(" ", 1)[1] for line in _lines(out1)[2:] + _lines(out2)[2:]]
    assert texts == ["cd", "efgh"]
    assert lite.call(client, "send", {"port": "t", "data": "x", "eol": "bad"}).startswith(
        "error:")


def test_send_next_cursor_skips_reply(live) -> None:
    hub, client = live
    out = lite.call(client, "send", {"port": "t", "data": "abc", "wait": "abc"})
    nxt = int(_header(_block_header(out))["next"])
    later = hub.ingest("t", "later").seq
    assert _lines(lite.call(client, "read", {"port": "t", "since": nxt}))[1:] == [
        f"{later} later"]


# --------------------------------------------------------------------------- #
# wait / search / control / ports
# --------------------------------------------------------------------------- #
def test_wait_matched_and_timeout(live) -> None:
    hub, client = live

    def later() -> None:
        time.sleep(0.3)
        hub.ingest("t", "READY now")

    threading.Thread(target=later, daemon=True).start()
    out = lite.call(client, "wait", {"port": "t", "regex": "READY", "timeout_ms": 5000})
    lines = _lines(out)
    assert lines[0].startswith("matched")
    assert re.match(r"^\d+ READY now$", lines[-1])
    assert lite.call(client, "wait", {"port": "t", "regex": "nope",
                                      "timeout_ms": 200}).startswith("timeout")


def test_wait_since(live) -> None:
    hub, client = live
    seq = hub.ingest("t", "old READY").seq
    out = lite.call(client, "wait", {"port": "t", "regex": "READY", "since": 0,
                                     "timeout_ms": 1000})
    assert _lines(out)[-1] == f"{seq} old READY"


def test_search_with_context(live) -> None:
    hub, client = live
    a = hub.ingest("t", "alpha").seq
    b = hub.ingest("t", "HIT one").seq
    c = hub.ingest("t", "gamma").seq
    hub.ingest("t", "delta")
    out = lite.call(client, "search", {"port": "t", "regex": "HIT", "context": 1})
    assert _lines(out)[1:] == [f"{a} alpha", f"{b} HIT one", f"{c} gamma"]
    out0 = lite.call(client, "search", {"port": "t", "regex": "HIT"})
    assert _lines(out0)[1:] == [f"{b} HIT one"]
    assert lite.call(client, "search", {"port": "t", "regex": "zzz"}).startswith("t empty")


def test_control_actions(live) -> None:
    hub, client = live
    assert lite.call(client, "control", {"port": "t2", "action": "open",
                                         "baud": 9600}) == "open 9600"
    assert lite.call(client, "control", {"port": "t2", "action": "close"}) == "closed"
    hub.ingest("t", "a")
    hub.ingest("t", "b")
    assert lite.call(client, "control", {"port": "t", "action": "clear"}) == "cleared 2"
    out = lite.call(client, "control", {"port": "t", "action": "mark", "label": "here"})
    assert re.match(r"^mark \d+$", out)
    assert lite.call(client, "read", {"port": "t"}).endswith(" #here")


def test_ports(live) -> None:
    hub, client = live
    seq = hub.ingest("t", "x").seq
    lines = _lines(lite.call(client, "ports", {}))
    assert lines[0] == f"t open 115200 loop:// last={seq}"
    assert lines[1] == "t2 closed 115200 loop:// last=0"


# --------------------------------------------------------------------------- #
# errors
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name,args", [
    ("nope", {}),
    ("read", {"port": "nope"}),
    ("read", {"port": "t", "filter": "("}),
    ("search", {"port": "t", "regex": "["}),
    ("wait", {"port": "t", "regex": "("}),
    ("send", {"port": "t", "data": "x", "wait": "("}),
    ("read", {}),
    ("send", {"port": "t"}),
    ("read", {"port": "t", "limit": "lots"}),
    ("read", {"port": 5}),
    ("read", {"port": "t", "bogus": 1}),
    ("control", {"port": "t", "action": "explode"}),
    ("read", "not-a-dict"),
])
def test_errors_are_text(live, name, args) -> None:
    _hub, client = live
    out = lite.call(client, name, args)
    assert out.startswith("error: "), out


def test_unreachable_daemon_is_error_text() -> None:
    out = lite.call(Client("http://127.0.0.1:1", timeout=1.0), "ports", {})
    assert out.startswith("error: ")


def test_search_context_keeps_tx_prefix(live) -> None:
    hub, client = live
    tx = hub.ingest("t", "cmd", kind="tx").seq
    hit = hub.ingest("t", "HIT").seq
    mk = hub.mark("t", "m")["seq"]
    out = lite.call(client, "search", {"port": "t", "regex": "HIT", "context": 1})
    assert _lines(out)[1:] == [f"{tx} >cmd", f"{hit} HIT", f"{mk} #m"]


def test_search_separates_windows(live) -> None:
    hub, client = live
    for text in ["a", "HIT1", "b", "c", "d", "HIT2", "e"]:
        hub.ingest("t", text)
    out = lite.call(client, "search", {"port": "t", "regex": "HIT", "context": 1})
    assert [ln.split(" ", 1)[-1] for ln in _lines(out)[1:]] == [
        "a", "HIT1", "b", "--", "d", "HIT2", "e"]


# --------------------------------------------------------------------------- #
# MCP lite profile
# --------------------------------------------------------------------------- #
def _mcp_tools(server) -> dict:
    import asyncio

    return {t.name: t for t in asyncio.run(server.list_tools())}


def test_mcp_lite_lists_exactly_six_tools() -> None:
    pytest.importorskip("mcp")
    from serialhub import mcp_lite

    tools = _mcp_tools(mcp_lite.build())
    assert list(tools) == NAMES
    for spec in lite.TOOLS:
        tool = tools[spec["name"]]
        assert tool.description == spec["description"]
        schema = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema")
        assert schema == spec["input_schema"]


def test_mcp_lite_call_matches_lite_call(live) -> None:
    pytest.importorskip("mcp")
    import asyncio

    from serialhub import mcp_lite

    hub, client = live
    hub.ingest("t", "hello mcp")
    server = mcp_lite.build(client.base_url, None)
    result = asyncio.run(server.call_tool("read", {"port": "t"}))
    content = getattr(result, "content", result)
    if isinstance(content, tuple):  # mcp 1.x: (content, structured)
        content = content[0]
    text = "".join(getattr(block, "text", "") for block in content)
    assert text == lite.call(client, "read", {"port": "t"})
    bad = asyncio.run(server.call_tool("read", {"port": "nope"}))
    bad_content = getattr(bad, "content", bad)
    if isinstance(bad_content, tuple):
        bad_content = bad_content[0]
    assert "".join(getattr(b, "text", "") for b in bad_content).startswith("error:")


def test_mcp_full_profile_unchanged() -> None:
    pytest.importorskip("mcp")
    from serialhub import mcp_server

    assert len(_mcp_tools(mcp_server.mcp)) == 18


# --------------------------------------------------------------------------- #
# CLI: tools / call
# --------------------------------------------------------------------------- #
def test_cli_tools_formats(capsys) -> None:
    from serialhub import cli

    assert cli.main(["tools"]) == 0
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert json.loads(out) == lite.to_anthropic()
    assert cli.main(["tools", "--format", "openai"]) == 0
    assert json.loads(capsys.readouterr().out) == lite.to_openai()


def test_cli_call(live, capsys) -> None:
    from serialhub import cli

    _hub, client = live
    assert cli.main(["call", "ports", "--url", client.base_url]) == 0
    assert capsys.readouterr().out.startswith("t open 115200")
    assert cli.main(["call", "read", '{"port": "t"}', "--url", client.base_url]) == 0
    assert capsys.readouterr().out.startswith("t empty")
    assert cli.main(["call", "nope", "--url", client.base_url]) == 1
    assert capsys.readouterr().out.startswith("error:")
    assert cli.main(["call", "read", "{bad json", "--url", client.base_url]) == 1
    assert capsys.readouterr().out.startswith("error:")


def test_cli_mcp_profile_dispatch(monkeypatch) -> None:
    pytest.importorskip("mcp")
    from serialhub import cli, mcp_lite, mcp_server

    seen: list = []
    monkeypatch.setattr(mcp_lite, "main", lambda **kw: seen.append(("lite", kw)))
    monkeypatch.setattr(mcp_server, "main", lambda **kw: seen.append(("full", kw)))
    assert cli.main(["mcp", "--profile", "lite", "--url", "http://h:1", "--token", "k"]) == 0
    assert cli.main(["mcp"]) == 0
    assert seen == [("lite", {"transport": "stdio", "base_url": "http://h:1", "token": "k"}),
                    ("full", {"transport": "stdio"})]


# --------------------------------------------------------------------------- #
# example agent loop (anthropic SDK mocked)
# --------------------------------------------------------------------------- #
def test_example_agent_loop_with_mocked_sdk(monkeypatch, capsys) -> None:
    import importlib.util
    import sys
    import types
    from pathlib import Path

    calls: list = []

    class _Block(types.SimpleNamespace):
        pass

    replies = [
        types.SimpleNamespace(stop_reason="tool_use", content=[
            _Block(type="text", text="checking"),
            _Block(type="tool_use", id="tu1", name="ports", input={})]),
        types.SimpleNamespace(stop_reason="end_turn", content=[
            _Block(type="text", text="all done")]),
    ]

    class _Messages:
        def create(self, **kw):
            calls.append(kw)
            return replies[len(calls) - 1]

    class _Anthropic:
        def __init__(self, **kw):
            self.messages = _Messages()

    monkeypatch.setenv("MODEL", "local-model")
    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(Anthropic=_Anthropic))
    monkeypatch.setattr(lite, "call", lambda client, name, args: "error: offline")
    path = Path(__file__).resolve().parent.parent / "examples" / "anthropic_agent.py"
    assert len(path.read_text(encoding="utf-8").splitlines()) <= 80
    spec = importlib.util.spec_from_file_location("anthropic_agent", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert len(mod.SYSTEM) <= 600
    mod.main(["list", "the", "ports"])
    assert calls[0]["tools"] == lite.to_anthropic()
    assert calls[0]["messages"][0] == {"role": "user", "content": "list the ports"}
    result = calls[1]["messages"][-1]["content"][0]
    assert result == {"type": "tool_result", "tool_use_id": "tu1",
                      "content": "error: offline", "is_error": True}
    assert "all done" in capsys.readouterr().out
