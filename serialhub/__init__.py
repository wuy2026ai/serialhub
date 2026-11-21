"""serialhub —— 常驻串口中枢（多口守护 + 每口环形缓冲 + Web UI + TCP 桥 + MCP）。

本包对外暴露版本号与关键符号。子模块（models/ringbuffer/config/serialport/hub）
互不导入本 ``__init__``，因此下列普通导入不产生循环。
"""

from __future__ import annotations

__version__ = "0.1.0"

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config, PortConfig
    from .hub import Hub
    from .models import Line
    from .ringbuffer import RingBuffer

# Lazy (PEP 562): keep ``import serialhub`` cheap for client-side CLI commands.
_LAZY = {
    "Config": "config",
    "PortConfig": "config",
    "Hub": "hub",
    "Line": "models",
    "RingBuffer": "ringbuffer",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = getattr(import_module(f".{mod}", __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """``dir(serialhub)`` 也列出懒加载的名字（否则只看得见已导入过的）。"""
    return sorted(set(globals()) | set(_LAZY))


__all__ = ["__version__", "Line", "RingBuffer", "Config", "PortConfig", "Hub"]
