"""serialhub MCP server, "lite" profile (stdio): six compact tools.

``serialhub mcp --profile lite`` 运行本模块：注册 ``lite.TOOLS`` 中的六个工具（名称、
描述、JSON Schema 与 ``lite.TOOLS`` 完全一致），每个工具返回 ``lite.call()`` 的纯文本。
与完整 profile（``mcp_server.py``，18 个工具）并存，互不影响；同样是瘦客户端，
守护进程须已运行。URL/token 取自参数或环境变量 ``SERIALHUB_URL`` / ``SERIALHUB_TOKEN``。
"""
from __future__ import annotations

import asyncio
import copy
import inspect
import os
import sys
from typing import Any, Optional

from . import lite
from .client import Client
from .mcp_compat import MCPServer

from mcp.types import CallToolResult, TextContent  # noqa: E402 - after the compat check

#: ``CallToolResult``'s error flag: ``is_error`` (mcp 2.x) or ``isError`` (mcp 1.x).
_IS_ERROR_FIELD = "is_error" if "is_error" in getattr(CallToolResult, "model_fields", {}) \
    else "isError"
#: Set by :func:`build`: whether ``error:`` results are flagged ``isError`` on this SDK.
ERROR_FLAG_SUPPORTED = False

DEFAULT_URL = "http://127.0.0.1:8765"

INSTRUCTIONS = (
    "Serial ports via serialhub. Output lines are '<seq> <text>' ('>' = sent, '#' = mark). "
    "Always pass the header's next= value as since on the next read."
)


def _make_tool_fn(name: str, client: Client, flag_errors: bool = False) -> Any:
    """Build an async tool function whose signature mirrors the lite schema.

    String properties are annotated exactly ``str``: the SDK json-decodes any string
    argument whose annotation is not ``str`` (``data='{"a":1}'`` would become a dict,
    ``'null'`` would become ``None``). Integers stay ``Any`` so ``lite.call`` does the
    lenient validation and reports ``error: ...`` itself.
    """
    props = lite._BY_NAME[name]["input_schema"]["properties"]

    async def _tool(**kwargs: Any) -> Any:
        args = {k: v for k, v in kwargs.items() if v is not None}
        text = await asyncio.to_thread(lite.call, client, name, args)
        if flag_errors and text.startswith("error:"):
            return CallToolResult(content=[TextContent(type="text", text=text)],
                                  **{_IS_ERROR_FIELD: True})
        return text

    annotations = {p: (str if spec.get("type") == "string" else Any)
                   for p, spec in props.items()}
    _tool.__name__ = f"lite_{name}"
    _tool.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [inspect.Parameter(p, inspect.Parameter.KEYWORD_ONLY, default=None,
                           annotation=annotations[p]) for p in props],
    )
    _tool.__annotations__ = dict(annotations)
    return _tool


def _tool_registry(server: Any) -> Optional[dict]:
    """The SDK's internal ``{name: Tool}`` map (mcp 1.x and 2.x), or ``None``."""
    tools = getattr(getattr(server, "_tool_manager", None), "_tools", None)
    return tools if isinstance(tools, dict) else None


def _probe_error_flag(tool: Any) -> bool:
    """True when this SDK passes a returned ``CallToolResult`` through unchanged."""
    try:
        probe = CallToolResult(content=[TextContent(type="text", text="probe")],
                               **{_IS_ERROR_FIELD: True})
        return tool.fn_metadata.convert_result(probe) is probe
    except Exception:  # noqa: BLE001 - unknown SDK layout: don't flag
        return False


def build(base_url: Optional[str] = None, token: Optional[str] = None) -> Any:
    """Create the lite MCP server bound to one daemon URL/token."""
    global ERROR_FLAG_SUPPORTED
    url = base_url or os.environ.get("SERIALHUB_URL") or DEFAULT_URL
    tok = token or os.environ.get("SERIALHUB_TOKEN") or None
    client = Client(url, tok)
    server = MCPServer("serialhub-lite", instructions=INSTRUCTIONS)
    registry = None
    for spec in lite.TOOLS:
        _add(server, _make_tool_fn(spec["name"], client), spec)
        registry = _tool_registry(server)
        if registry is not None and spec["name"] in registry:
            # Publish the exact lite schema instead of the signature-derived one.
            registry[spec["name"]].parameters = copy.deepcopy(spec["input_schema"])
    if registry is None:
        sys.stderr.write(
            "serialhub.mcp_lite: warning: cannot reach the MCP SDK tool registry; "
            "publishing signature-derived schemas instead of the exact lite schemas.\n")
        return server
    # Flag "error: ..." results as isError only where the text stays identical.
    ERROR_FLAG_SUPPORTED = _probe_error_flag(registry[lite.TOOLS[0]["name"]])
    if ERROR_FLAG_SUPPORTED:
        for spec in lite.TOOLS:
            registry[spec["name"]].fn = _make_tool_fn(spec["name"], client, True)
    return server


def _add(server: Any, fn: Any, spec: dict[str, Any]) -> None:
    try:
        server.add_tool(fn, name=spec["name"], description=spec["description"],
                        structured_output=False)
    except TypeError:  # older mcp 1.x without structured_output
        server.add_tool(fn, name=spec["name"], description=spec["description"])


def main(transport: str = "stdio", base_url: Optional[str] = None,
         token: Optional[str] = None) -> None:
    """Run the lite MCP server. ``transport`` defaults to ``stdio``."""
    for stream in (sys.stdin, sys.stdout):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8")
        except ValueError:
            pass
    build(base_url, token).run(transport=transport)


if __name__ == "__main__":
    main()
