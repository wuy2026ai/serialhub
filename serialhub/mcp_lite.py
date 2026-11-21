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

DEFAULT_URL = "http://127.0.0.1:8765"

INSTRUCTIONS = (
    "Serial ports via serialhub. Output lines are '<seq> <text>' ('>' = sent, '#' = mark). "
    "Always pass the header's next= value as since on the next read."
)


def _make_tool_fn(name: str, client: Client) -> Any:
    """Build an async tool function whose signature mirrors the lite schema.

    Every parameter is typed ``Any`` (default ``None``) so the SDK's own argument
    model never rejects input — ``lite.call`` validates and reports ``error: ...``.
    """
    props = lite._BY_NAME[name]["input_schema"]["properties"]

    async def _tool(**kwargs: Any) -> str:
        args = {k: v for k, v in kwargs.items() if v is not None}
        return await asyncio.to_thread(lite.call, client, name, args)

    _tool.__name__ = f"lite_{name}"
    _tool.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [inspect.Parameter(p, inspect.Parameter.KEYWORD_ONLY, default=None, annotation=Any)
         for p in props],
        return_annotation=str,
    )
    _tool.__annotations__ = {**{p: Any for p in props}, "return": str}
    return _tool


def build(base_url: Optional[str] = None, token: Optional[str] = None) -> Any:
    """Create the lite MCP server bound to one daemon URL/token."""
    url = base_url or os.environ.get("SERIALHUB_URL") or DEFAULT_URL
    tok = token or os.environ.get("SERIALHUB_TOKEN") or None
    client = Client(url, tok)
    server = MCPServer("serialhub-lite", instructions=INSTRUCTIONS)
    tools = getattr(getattr(server, "_tool_manager", None), "_tools", None)
    for spec in lite.TOOLS:
        fn = _make_tool_fn(spec["name"], client)
        try:
            server.add_tool(fn, name=spec["name"], description=spec["description"],
                            structured_output=False)
        except TypeError:  # older mcp 1.x without structured_output
            server.add_tool(fn, name=spec["name"], description=spec["description"])
        # Publish the exact lite schema instead of the signature-derived one.
        if isinstance(tools, dict) and spec["name"] in tools:
            tools[spec["name"]].parameters = copy.deepcopy(spec["input_schema"])
    return server


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
