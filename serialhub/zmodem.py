"""ZMODEM 会话检测与会话状态（raw 模式）。

职责：
- :class:`ZmodemDetector`：在串口读到的字节流里找 ZMODEM 十六进制起始头
  ``**`` ZDLE(0x18) ``B0`` + ``0``/``1``。``0`` = ZRQINIT（设备跑 ``sz`` → 浏览器
  ``download``），``1`` = ZRINIT（设备跑 ``rz`` → 浏览器 ``upload``）。头之前的字节
  原样还给调用方当文本；跨读分片的头靠“留住结尾的头前缀”拼接。
- :class:`ZmodemSession`：一次传输的状态。负载字节只进本会话的队列
  （:class:`PayloadQueue`，``None`` 为结束哨兵），绝不进 ``Hub.ingest`` / 环形缓冲 /
  ``Hub._broadcast``。队列本身不设 maxsize（读循环绝不阻塞），未取走的字节数超过
  :data:`QUEUE_MAX_BYTES` 时由 worker 以 abort 结束会话。

会话的开启/路由/超时/结束由 ``SerialPortWorker`` 驱动，对外经 ``Hub.zmodem_*``。
"""

from __future__ import annotations

import queue
import re
import uuid
from dataclasses import dataclass, field
from typing import Optional

from .models import now_ms

__all__ = [
    "ABORT_SEQ", "DRAIN_MAX_MS", "DRAIN_SILENCE_MS", "END_REASONS", "HEADER_RE",
    "QUEUE_MAX_BYTES", "TAIL_MAX_BYTES",
    "Detection", "PayloadQueue", "ZmodemDetector", "ZmodemSession",
]

#: 写给设备的中止序列：8×CAN + 8×BS（lrzsz 的标准取消串）。
ABORT_SEQ: bytes = b"\x18" * 8 + b"\x08" * 8

#: 十六进制起始头；组 1 为帧类型的低位：``0`` → download，``1`` → upload。
HEADER_RE = re.compile(rb"\*\*\x18B0([01])")

#: 头的固定前缀（不含帧类型位）；读分片结尾可能停在它的任一前缀上。
_PREFIX = b"**\x18B0"

_DIRECTIONS = {b"0": "download", b"1": "upload"}

#: 会话结束原因的合法取值。
END_REASONS = frozenset({"done", "abort", "timeout", "unclaimed", "closed", "disconnect"})

#: 非 ``done`` 结束后进入“排空”：丢弃读到的字节（设备还在发、OS/USB 缓冲里还有
#: 负载），直到连续这么久读不到数据……
DRAIN_SILENCE_MS = 300
#: ……或排空总时长达到上限，再回到行模式。
DRAIN_MAX_MS = 3000

#: 已认领会话的队列里“放入但未被转发方取走”的字节上限；超过 → 以 abort 结束会话
#: （客户端连着却不取数据时，避免整个文件堆在 daemon 内存里）。
QUEUE_MAX_BYTES = 64 * 1024 * 1024

#: ``zmodem_end`` 的 ``tail``（回注为 rx 行）解码后的上限；超过则整条 end 消息被拒。
TAIL_MAX_BYTES = 16 * 1024


@dataclass
class Detection:
    """检测结果：``direction`` ∈ {"download","upload"}；``initial`` = 头及其后同批字节。"""

    direction: str
    initial: bytes


class ZmodemDetector:
    """流式起始头检测器（非线程安全，只由读循环线程使用）。"""

    def __init__(self) -> None:
        self._held = bytearray()

    def feed(self, data: bytes) -> tuple[bytes, Optional[Detection]]:
        """喂入一段读到的字节，返回 ``(当作文本的字节, Detection 或 None)``。

        命中时文本 = 头之前的全部字节，``initial`` = 头及其后的全部字节，内部不再留存。
        未命中时只留住结尾那段恰为 ``**\\x18B0`` 前缀的字节（最多 5 字节），其余还给
        调用方；留住的字节由下一次 ``feed`` 或 :meth:`flush` 释放。
        """
        buf = bytes(self._held) + bytes(data)
        self._held.clear()
        match = HEADER_RE.search(buf)
        if match is not None:
            start = match.start()
            return buf[:start], Detection(_DIRECTIONS[match.group(1)], buf[start:])
        keep = _held_suffix_len(buf)
        if keep:
            self._held.extend(buf[-keep:])
            return buf[:-keep], None
        return buf, None

    def flush(self) -> bytes:
        """释放留住的字节（读超时返回空时调用，避免普通文本结尾的 ``*`` 被卡住）。"""
        out = bytes(self._held)
        self._held.clear()
        return out


def _held_suffix_len(buf: bytes) -> int:
    """``buf`` 结尾与 ``_PREFIX`` 某前缀重合的最长长度（0 表示无需留存）。"""
    for k in range(min(len(_PREFIX), len(buf)), 0, -1):
        if buf.endswith(_PREFIX[:k]):
            return k
    return 0


class PayloadQueue(queue.Queue):
    """会话负载队列：额外记录 ``queued_bytes`` = 已放入、尚未被取走的负载字节数。

    计数在 ``Queue`` 自身的互斥锁下随 put/get 增减；转发方每 ``get`` 一个分片即视为已消费。
    """

    def _init(self, maxsize: int) -> None:
        super()._init(maxsize)
        self.queued_bytes = 0

    def _put(self, item: Optional[bytes]) -> None:
        super()._put(item)
        if item:
            self.queued_bytes += len(item)

    def _get(self) -> Optional[bytes]:
        item = super()._get()
        if item:
            self.queued_bytes -= len(item)
        return item


@dataclass
class ZmodemSession:
    """一次 ZMODEM 传输的状态；字段由所属 worker 在其 ``_zm_lock`` 下读写。"""

    port: str
    direction: str
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    owner: Optional[str] = None
    rx_bytes: int = 0
    tx_bytes: int = 0
    started_ms: int = field(default_factory=now_ms)
    last_activity_ms: int = field(default_factory=now_ms)
    #: 认领前收到的字节；认领时整块回放进 ``queue``。
    pending: bytearray = field(default_factory=bytearray)
    #: 负载队列（不设 maxsize）：``bytes`` 为负载分片，``None`` 为结束哨兵。
    queue: PayloadQueue = field(default_factory=PayloadQueue)
