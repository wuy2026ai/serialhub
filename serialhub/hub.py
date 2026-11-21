"""中枢核心（§6）。

职责：把 ``Config`` + 每口 ``RingBuffer`` + 每口 ``SerialPortWorker`` + 订阅广播 +
全局单调 seq 缝合起来，对外提供端口生命周期、查询、数据读写、等待、导出、标记、
发送与 TCP 桥委托等能力，并保证全部返回结构严格符合契约 §6.1。

线程安全：``_lock`` 守护端口登记表 / 缓冲表 / 桥表 / 订阅者集合；``_seq_lock``
守护全局序号。worker 不直接改缓冲，一律经 ``ingest``。
"""

# allow: SIZE_OK - 契约 §6 要求单一 Hub 类承载全部端口/缓冲/广播/桥/等待逻辑，
# 且文件所有权（agent A）限定文件清单，禁止拆分为额外模块。

from __future__ import annotations

import os
import queue
import re
import threading
import time
from typing import TYPE_CHECKING, Any, Iterable, Optional

from serial.tools import list_ports as _list_ports

from . import discovery
from .config import Config, PortConfig, pins_view
from .exporter import VALID_FORMATS, export_to_file
from .i18n import t
from .models import Line, has_ansi, now_ms, resolve_eol, strip_ansi
from .ringbuffer import RingBuffer
from .serialport import SerialPortWorker

if TYPE_CHECKING:
    from .tcp_bridge import TcpBridge

__all__ = ["Hub"]

_SUBSCRIBER_QUEUE_SIZE = 10000
_IO_FIELDS = (
    "device", "baud", "data_bits", "parity", "stop_bits", "flow",
    "dtr", "rts", "encoding", "reconnect_ms", "reconnect_max_ms", "flush_ms",
    "framing", "pins",
)

#: ``exchange`` 缺省超时（毫秒）与一次返回的最多 rx 行数。
_EXCHANGE_TIMEOUT_MS = 10000
_EXCHANGE_LIMIT = 500

#: ``hub.set_pin`` 允许的动作。
_PIN_ACTIONS = ("set", "pulse", "reset")


def _compile(pattern: Any) -> "re.Pattern[str]":
    if isinstance(pattern, str):
        return re.compile(pattern)
    return pattern


def _compile_user(pattern: Any, what: str) -> "re.Pattern[str]":
    """编译调用方给的正则；非法 → ``ValueError``（REST 400）。"""
    if not isinstance(pattern, str):
        return pattern
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"bad {what} regex {pattern!r}: {exc}") from exc


def _is_echo(text: str, data: str) -> bool:
    """设备回显的命令本身（可带提示符前缀，如 ``root@x:~# cmd``）；与 lite send 同一规则。"""
    return bool(data) and strip_ansi(text).rstrip().endswith(data.rstrip())


def _parse_range(rng: Any) -> tuple[Optional[int], Optional[int]]:
    """解析 seq 范围：``"A-B"`` 或 ``[a, b]``；空端为 None。"""
    if rng is None:
        return None, None
    if isinstance(rng, (list, tuple)):
        if len(rng) >= 2:
            return int(rng[0]), int(rng[1])
        if len(rng) == 1:
            return int(rng[0]), None
        return None, None
    if isinstance(rng, str):
        text = rng.strip()
        if "-" in text:
            lo_s, hi_s = text.split("-", 1)
            lo = int(lo_s) if lo_s.strip() else None
            hi = int(hi_s) if hi_s.strip() else None
            return lo, hi
        if text:
            v = int(text)
            return v, v
    return None, None


class Hub:
    """串口中枢。全体后端（REST/WS/MCP/桥）依赖其返回结构。"""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._lock = threading.RLock()
        self._seq_lock = threading.Lock()
        self._seq = 0
        self._start_ms = now_ms()
        self._buffers: dict[str, RingBuffer] = {}
        self._workers: dict[str, SerialPortWorker] = {}
        self._subscribers: set[queue.Queue] = set()
        self._bridges: dict[str, "TcpBridge"] = {}
        # 每口未结束尾巴（pending，§16）：无 seq、不入环，空串即删除。由 _lock 守护。
        self._pending: dict[str, str] = {}
        # 订阅者的唤醒回调（subscribe(notify=...)，WS 转发用）；与 _subscribers 同由 _lock 守护。
        self._notifiers: dict[queue.Queue, Any] = {}
        for pc in config.ports:
            self._buffers[pc.name] = RingBuffer(pc.buffer_lines, pc.buffer_bytes)
            self._workers[pc.name] = SerialPortWorker(pc, self)

    # ---- 生命周期 ----
    def start(self) -> None:
        """打开 autostart 口，并启动 config 里声明的 tcp_bridge。"""
        with self._lock:
            ports = list(self._config.ports)
        for pc in ports:
            if pc.autostart:
                try:
                    self.open(pc.name)
                except Exception as exc:
                    self._publish_error(pc.name, exc)
            if pc.tcp_bridge:
                try:
                    self.bridge_start(
                        pc.name,
                        mode=pc.tcp_bridge.get("mode", "server"),
                        tcp_port=pc.tcp_bridge.get("port"),
                        host=pc.tcp_bridge.get("host"),
                    )
                except Exception as exc:
                    self._publish_error(pc.name, exc)

    def stop(self) -> None:
        """关闭所有桥与端口；幂等。"""
        with self._lock:
            bridge_names = list(self._bridges)
            worker_names = list(self._workers)
        for name in bridge_names:
            try:
                self.bridge_stop(name)
            except Exception as exc:
                self._publish_error(name, exc)
        for name in worker_names:
            try:
                self.close(name)
            except Exception as exc:
                self._publish_error(name, exc)

    def reload(self, new_config: Config) -> dict:
        """热重载（§6.3）：返回 {"added","removed","restarted","updated"}。"""
        added: list[str] = []
        removed: list[str] = []
        restarted: list[str] = []
        updated: list[str] = []
        old_ports = {pc.name: pc for pc in self._config.ports}
        new_ports = {pc.name: pc for pc in new_config.ports}

        for name in list(old_ports):
            if name not in new_ports:
                self.remove_port(name)
                removed.append(name)

        for name in new_ports:
            if name not in old_ports:
                self.add_port(new_ports[name])
                added.append(name)

        for name in new_ports:
            if name not in old_ports:
                continue
            old_pc, new_pc = old_ports[name], new_ports[name]
            if (old_pc.buffer_lines, old_pc.buffer_bytes) != (new_pc.buffer_lines, new_pc.buffer_bytes):
                self._rebuild_buffer(name, new_pc.buffer_lines, new_pc.buffer_bytes)
                updated.append(name)
            if any(getattr(old_pc, f) != getattr(new_pc, f) for f in _IO_FIELDS):
                worker = self._require_worker(name)
                if worker.is_open:
                    worker.close()
                    self._replace_worker(name, new_pc)
                    self.open(name)
                    restarted.append(name)
                else:
                    worker.update_config(new_pc)
            else:
                self._require_worker(name).update_config(new_pc)
            self._replace_port_config(name, new_pc)

        self._config = new_config
        return {"added": added, "removed": removed, "restarted": restarted, "updated": updated}

    def switch_config(self, path: str) -> dict:
        """运行时切换配置文件：加载+校验后整体热重载，并把该文件记为后续 reload 的来源。

        路径先归一化（``~`` 展开 + ``abspath``；相对路径相对**守护进程 cwd** 解析）。
        文件不存在或校验失败一律 ``ValueError``；因为"先加载后应用"，此时当前配置
        完全不受影响。切换只作用于内存（不写盘、不改启动参数），守护进程重启后
        仍以 ``serve --config`` 的文件为准。返回 :meth:`reload` 的摘要，并附加
        ``config``（生效的绝对路径）。
        """
        raw = str(path or "").strip()
        if not raw:
            raise ValueError(t("hub.error.switch_no_path"))
        target = os.path.abspath(os.path.expanduser(raw))
        if not os.path.isfile(target):
            raise ValueError(t("hub.error.switch_not_found", path=target))
        summary = self.reload(Config.load(target))
        summary["config"] = self._config.path
        return summary

    # ---- 端口管理 ----
    def add_port(self, pc: PortConfig, *, autostart: bool = True) -> None:
        """新增端口（已存在则 ValueError）。"""
        with self._lock:
            if pc.name in self._workers:
                raise ValueError(t("hub.error.port_exists", name=pc.name))
            self._buffers[pc.name] = RingBuffer(pc.buffer_lines, pc.buffer_bytes)
            self._workers[pc.name] = SerialPortWorker(pc, self)
            self._config.ports.append(pc)
        if autostart:
            self.open(pc.name)

    def remove_port(self, name: str) -> None:
        """关闭并移除端口；未知 name → KeyError。"""
        with self._lock:
            worker = self._workers.get(name)
        if worker is None:
            raise KeyError(name)
        try:
            self.bridge_stop(name)
        except Exception as exc:
            self._publish_error(name, exc)
        worker.close()
        with self._lock:
            self._workers.pop(name, None)
            self._buffers.pop(name, None)
            self._config.ports = [p for p in self._config.ports if p.name != name]

    def open(self, name: str, *, baud: Optional[int] = None, device: Optional[str] = None) -> dict:
        """打开端口（可选改 baud/device），返回端口 dict。"""
        pc = self._port_config(name)
        worker = self._require_worker(name)
        changed = False
        if baud is not None and int(baud) != pc.baud:
            pc.baud = int(baud)
            changed = True
        if device is not None and str(device) != pc.device:
            pc.device = str(device)
            changed = True
        if changed:
            if worker.is_open:
                worker.close()
            self._replace_worker(name, pc)
            worker = self._workers[name]
        worker.open()
        return self._port_status(name)

    def close(self, name: str) -> dict:
        """关闭端口，返回端口 dict。"""
        worker = self._require_worker(name)
        worker.close()
        return self._port_status(name)

    def is_open(self, name: str) -> bool:
        return self._require_worker(name).is_open

    # ---- 引脚（DTR/RTS）----
    def pins_status(self, name: str) -> dict:
        """归一化引脚状态；引脚控制未启用 → ValueError，未知端口 → KeyError。

        返回 ``{"port", "enabled", "dtr", "rts", "pulse_ms", "initial": {...}}``；
        ``dtr``/``rts`` 是当前电平（``None`` 表示未知），``initial`` 是 open 时
        生效的初始电平（``pins.dtr`` 优先，回退顶层 ``dtr``）。
        """
        worker = self._require_worker(name)
        cfg = worker.pins_config
        if not cfg["enabled"]:
            raise ValueError(t("hub.error.pins_disabled", name=name))
        state = worker.pin_state
        return {
            "port": name,
            "enabled": True,
            "dtr": state["dtr"],
            "rts": state["rts"],
            "pulse_ms": cfg["pulse_ms"],
            "initial": {"dtr": cfg["dtr"], "rts": cfg["rts"]},
        }

    def set_pin(
        self,
        name: str,
        action: str,
        pin: Optional[str] = None,
        value: Optional[bool] = None,
        duration_ms: Optional[int] = None,
    ) -> dict:
        """操作 DTR/RTS：``set``（需 pin+value）/``pulse``（需 pin）/``reset``。

        引脚控制未启用或参数非法 → ``ValueError``；端口未打开 → ``RuntimeError``；
        未知端口 → ``KeyError``。返回 ``{"port", "action", "pin", "value", "dtr",
        "rts", "duration_ms"}``（``reset`` 忽略并回显 ``pin=None``）。
        """
        act = str(action or "").strip().lower()
        if act not in _PIN_ACTIONS:
            raise ValueError(t("hub.error.pins_unknown_action", action=action))
        worker = self._require_worker(name)
        if act in ("set", "pulse") and not isinstance(pin, str):
            raise ValueError(t("hub.error.pins_need_pin", action=act))
        if act == "set" and not isinstance(value, bool):
            raise ValueError(t("hub.error.pins_need_value"))

        out_pin: Optional[str] = None
        out_value: Optional[bool] = None
        out_duration: Optional[int] = None
        if act == "set":
            worker.set_pin(pin, value)  # type: ignore[arg-type]
            out_pin, out_value = pin, bool(value)
        elif act == "pulse":
            worker.pulse_pin(pin, duration_ms)  # type: ignore[arg-type]
            out_pin = pin
            out_duration = (
                int(duration_ms) if duration_ms is not None else worker.pins_config["pulse_ms"]
            )
        else:
            worker.reset_pins(duration_ms)
            out_duration = (
                int(duration_ms) if duration_ms is not None else worker.pins_config["pulse_ms"]
            )
        state = worker.pin_state
        return {
            "port": name,
            "action": act,
            "pin": out_pin,
            "value": out_value,
            "dtr": state["dtr"],
            "rts": state["rts"],
            "duration_ms": out_duration,
        }

    def set_device(
        self,
        name: str,
        *,
        device: Optional[str] = None,
        match: Optional[dict] = None,
        reopen: bool = True,
    ) -> dict:
        """运行时、临时地改写某口的 device/match（仅内存，不落盘、不重启进程）。

        必须且只能给出 ``device``/``match`` 之一，否则 ``ValueError``：

        - ``device``: 非空 ``str`` → 固定设备名（同时清空 ``pc.match``）。
        - ``match``: ``dict``，键必须 ⊂ :data:`serialhub.discovery.MATCH_KEYS`
          （``vid``/``pid`` 经 :func:`serialhub.discovery.parse_usb_id` 校验）；
          置空 ``pc.device``，交由 worker 在打开/重连时重新解析真实设备名。

        ``reopen=True``（默认）立即重启 worker 并重新打开，缓冲/历史保留；
        ``reopen=False`` 只改配置映射，下次 open/重连生效。未知端口名 → ``KeyError``。
        返回与 ``open``/``status`` 相同的端口 dict（``_port_status``）。
        """
        if (device is None) == (match is None):
            raise ValueError(t("hub.error.set_device_need_one"))
        pc = self._port_config(name)
        if device is not None:
            if not isinstance(device, str) or not device:
                raise ValueError(t("hub.error.set_device_device"))
            pc.device = device
            pc.match = None
        else:
            pc.match = self._validate_match(match)
            pc.device = ""
        if reopen:
            worker = self._workers.get(name)
            if worker is not None:
                worker.close()
            self._replace_worker(name, pc)
            self.open(name)
        return self._port_status(name)

    def list_devices(self) -> dict:
        """枚举系统串口设备（``serial.tools.list_ports.comports()``）。

        返回 ``{"devices": [{"index", "device", "description", "hwid", "vid",
        "pid"}...]}``，按 ``device`` 名排序，``index`` 为排序后位置。
        """
        try:
            infos = list(_list_ports.comports())
        except Exception:
            infos = []
        infos.sort(key=lambda p: str(getattr(p, "device", "") or ""))
        devices = [
            {
                "index": i,
                "device": str(getattr(p, "device", "") or ""),
                "description": str(getattr(p, "description", "") or ""),
                "hwid": str(getattr(p, "hwid", "") or ""),
                "vid": getattr(p, "vid", None),
                "pid": getattr(p, "pid", None),
            }
            for i, p in enumerate(infos)
        ]
        return {"devices": devices}

    # ---- 查询 ----
    def list_ports(self) -> dict:
        with self._lock:
            names = list(self._workers)
        return {"system": self._system_ports(), "ports": [self._port_status(n) for n in names]}

    def status(self, name: Optional[str] = None) -> dict:
        if name is not None:
            data = self._port_status(name)
            data["server"] = self._server_info()
            return data
        with self._lock:
            names = list(self._workers)
        return {"server": self._server_info(), "ports": [self._port_status(n) for n in names]}

    # ---- 数据 ----
    def read(
        self,
        name: str,
        *,
        since_seq: Optional[int] = None,
        since_ts: Optional[int] = None,
        regex: Any = None,
        limit: Optional[int] = None,
        strip_ansi: bool = False,
        kinds: Optional[Iterable[str]] = None,
    ) -> dict:
        result = self._require_buffer(name).read(
            since_seq=since_seq,
            since_ts=since_ts,
            regex=regex,
            limit=limit,
            strip_ansi=strip_ansi,
            kinds=set(kinds) if kinds else None,
        )
        result["port"] = name
        self._add_pending(name, result)
        return result

    def search(
        self,
        name: str,
        regex: str,
        *,
        context: int = 0,
        since_seq: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict:
        result = self._require_buffer(name).search(
            regex, context=context, since_seq=since_seq, limit=limit
        )
        result["port"] = name
        return result

    def wait_for(
        self,
        name: str,
        regex: str,
        *,
        timeout_ms: int = 10000,
        since_seq: Optional[int] = None,
    ) -> dict:
        """先扫环（已到哨兵立即返回），否则订阅广播阻塞等待。超时非错误。

        扫描下界：给定 ``since_seq`` 时用其值；否则用 0（即扫全环，已存在的匹配行立即返回）。
        正则同时匹配该口的未结束尾巴（pending，§16，如无换行的 ``login: ``）：调用时已在的
        尾巴立即命中，等待中尾巴每次变化（``partial`` 事件）都重新匹配；尾巴命中时
        ``line``/``seq`` 为 None、``pending`` 为命中的文本。唤醒由读线程的广播驱动，不轮询。
        """
        buf = self._require_buffer(name)
        pat = _compile(regex)
        cursor = since_seq if since_seq is not None else 0

        q = self.subscribe()
        try:
            existing = buf.read(since_seq=cursor, regex=pat, limit=1)
            if existing["lines"]:
                return self._wait_hit(name, existing["lines"][0], cursor)
            tail = self.pending(name)
            if tail and pat.search(tail) is not None:
                return self._wait_pending_hit(name, tail, cursor)
            deadline = now_ms() + max(0, int(timeout_ms))
            while True:
                remaining = deadline - now_ms()
                if remaining <= 0:
                    break
                try:
                    event = q.get(timeout=remaining / 1000.0)
                except queue.Empty:
                    break
                if event.get("port") != name:
                    continue
                etype = event.get("type")
                if etype == "partial":
                    tail = str(event.get("text") or "")
                    if tail and pat.search(tail) is not None:
                        return self._wait_pending_hit(name, tail, cursor)
                    continue
                if etype != "line":
                    continue
                line = event["line"]
                if line["seq"] <= cursor:
                    continue
                if pat.search(line["text"]) is not None:
                    return self._wait_hit(name, line, cursor)
        finally:
            self.unsubscribe(q)
        return self._add_pending(name, {
            "port": name, "matched": False, "line": None, "seq": None, "ts_ms": None,
            "from_seq": cursor,
        })

    def exchange(
        self,
        name: str,
        data: Any,
        *,
        eol: Optional[str] = None,
        wait_regex: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        quiet_ms: Optional[int] = None,
        since_seq: Optional[int] = None,
    ) -> dict:
        """一问一答（§16）：记下游标 → 写出（同 :meth:`send`）→ 收集回复，先到者结束：

        a) ``wait_regex`` 命中游标之后的新 rx 行或（写出后变化过的）未结束尾巴 → ``regex``；
        b) 未给 ``wait_regex`` 且该口配了 ``prompt``：写出后有字节到达，且 prompt 命中尾巴
           （无尾巴时命中最后一行新 rx）→ ``prompt``；
        c) 未给 ``wait_regex``：最后一个**字节**之后静默 ``quiet_ms``（缺省 ``reply_quiet_ms``）
           → ``quiet``；写出后一个字节都没到时最多等 ``min(timeout_ms, reply_first_ms)``，再按 ``quiet``；
        d) ``timeout_ms``（缺省 10000）→ ``timeout``。

        与 lite send 一样跳过命令回显的第一行（不参与匹配，仍在 ``lines`` 里）。返回
        ``{"port","sent","sent_bytes","lines","count","has_more","pending","matched","reason",
        "next_seq","from_seq"}``；``lines`` 与 ``read`` 同形（只含 rx），至多 500 行。
        """
        worker = self._require_worker(name)
        pc = self._port_config(name)
        buf = self._require_buffer(name)
        pat = _compile_user(wait_regex, "wait") if wait_regex else None
        prompt_src = getattr(pc, "prompt", None)
        prompt = _compile_user(prompt_src, "prompt") if (prompt_src and pat is None) else None
        timeout = _EXCHANGE_TIMEOUT_MS if timeout_ms is None else max(0, int(timeout_ms))
        quiet = (int(getattr(pc, "reply_quiet_ms", 100)) if quiet_ms is None
                 else max(0, int(quiet_ms))) / 1000.0
        first = min(timeout, max(0, int(getattr(pc, "reply_first_ms", 300)))) / 1000.0
        text_sent = data if isinstance(data, str) else bytes(data).decode(pc.encoding, "replace")

        q = self.subscribe()
        try:
            newest = buf.newest_seq()
            cursor = int(since_seq) if since_seq is not None else (newest or 0)
            rx0, _ = worker.rx_activity()
            sent = self.send(name, data, eol=eol)
            start = time.monotonic()
            deadline = start + timeout / 1000.0
            state = {"echo": bool(text_sent.strip()), "seen": cursor, "last_text": None,
                     "tail": "", "tail_fresh": False, "reason": None, "match_seq": None}

            def on_line(line: dict) -> None:
                if line.get("kind") != "rx" or line["seq"] <= state["seen"]:
                    return
                state["seen"] = line["seq"]
                text = str(line.get("text") or "")
                state["last_text"] = text
                if state["echo"] and _is_echo(text, text_sent):
                    state["echo"] = False
                    return
                if pat is not None and pat.search(strip_ansi(text)) is not None:
                    state["reason"], state["match_seq"] = "regex", line["seq"]

            # 给了更早的 since_seq：游标与写出之间已入环的行先过一遍（之后的行走事件）
            if since_seq is not None:
                for line in buf.read(since_seq=cursor, kinds={"rx"})["lines"]:
                    on_line(line)
                    if state["reason"]:
                        break
            while state["reason"] is None:
                now = time.monotonic()
                wake = deadline
                if pat is None:  # 静默先于超时判定：首字节窗口 min(timeout, first) 到期按 quiet
                    rx, last = worker.rx_activity()
                    quiet_at = start + first if rx == rx0 else last + quiet
                    if now >= quiet_at:
                        state["reason"] = "quiet"
                        break
                    wake = min(wake, quiet_at)
                if now >= deadline:
                    state["reason"] = "timeout"
                    break
                try:
                    batch = [q.get(timeout=max(0.0, wake - now))]
                except queue.Empty:
                    continue
                while True:  # 一次唤醒处理完已到的全部事件
                    try:
                        batch.append(q.get_nowait())
                    except queue.Empty:
                        break
                for event in batch:
                    if event.get("port") != name:
                        continue
                    if event.get("type") == "line":
                        on_line(event["line"])
                    elif event.get("type") == "partial":
                        state["tail"] = str(event.get("text") or "")
                        state["tail_fresh"] = True
                        if pat is not None and state["tail"] and pat.search(state["tail"]):
                            state["reason"] = "regex"
                    if state["reason"]:
                        break
                if state["reason"] is None and prompt is not None:
                    target = state["tail"] if (state["tail_fresh"] and state["tail"]) \
                        else state["last_text"]
                    if target and prompt.search(strip_ansi(target)) is not None:
                        state["reason"] = "prompt"
        finally:
            self.unsubscribe(q)

        got = buf.read(since_seq=cursor, kinds={"rx"})["lines"]
        if state["match_seq"] is not None:
            got = [ln for ln in got if ln["seq"] <= state["match_seq"]]
        lines = got[:_EXCHANGE_LIMIT]
        reason = state["reason"]
        return {
            "port": name,
            "sent": sent["sent"],
            "sent_bytes": sent["sent_bytes"],
            "lines": lines,
            "count": len(lines),
            "has_more": len(got) > len(lines),
            "pending": self.pending(name),
            "matched": reason in ("regex", "prompt"),
            "reason": reason,
            "next_seq": lines[-1]["seq"] if lines else cursor,
            "from_seq": cursor,
        }

    def snapshot(self, name: str, *, tail_lines: int = 200) -> dict:
        lines = self._require_buffer(name).snapshot(tail_lines)
        dicts = [line.to_dict() for line in lines]
        return {"port": name, "tail_lines": tail_lines, "lines": dicts, "count": len(dicts)}

    def export(self, name: str, *, path: Optional[str] = None, fmt: str = "raw", rng: Any = None) -> dict:
        """导出环内行；path 缺省写到 ``<cwd>/exports/<port>-<ts>.<ext>``。"""
        if fmt not in VALID_FORMATS:
            raise ValueError(t("hub.error.unknown_format", fmt=fmt, formats=list(VALID_FORMATS)))
        lines = self._require_buffer(name).snapshot()
        lo, hi = _parse_range(rng)
        if lo is not None or hi is not None:
            lines = [
                line for line in lines
                if (lo is None or line.seq >= lo) and (hi is None or line.seq <= hi)
            ]
        if path is None:
            ext = "jsonl" if fmt == "json" else "txt"
            path = os.path.join(os.getcwd(), "exports", f"{name}-{now_ms()}.{ext}")
        target = os.path.abspath(path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        written = export_to_file(lines, target, fmt)
        return {"port": name, "path": target, "format": fmt, "bytes": written, "count": len(lines)}

    def clear(self, name: str) -> dict:
        buf = self._require_buffer(name)
        cleared = buf.stats()["count"]
        buf.clear()
        return {"port": name, "cleared": cleared}

    def mark(self, name: str, label: str) -> dict:
        line = self.ingest(name, label, kind="mark")
        return {"port": name, "label": label, "seq": line.seq, "ts_ms": line.ts_ms}

    def send(
        self,
        name: str,
        data: Any,
        *,
        eol: Optional[str] = None,
        wait_regex: Optional[str] = None,
        timeout_ms: Optional[int] = None,
    ) -> dict:
        """写串口（按口 eol 追加结尾）+ 以 kind=tx 入环；可选等待回应。"""
        worker = self._require_worker(name)
        pc = self._port_config(name)
        eol_bytes = resolve_eol(eol, pc.eol)
        if isinstance(data, str):
            payload = data.encode(pc.encoding, errors="replace")
            sent_text = data
        else:
            payload = bytes(data)
            sent_text = payload.decode(pc.encoding, errors="replace")

        from_seq: Optional[int] = None
        if wait_regex is not None:
            newest = self._require_buffer(name).newest_seq()
            from_seq = newest if newest is not None else 0

        sent_bytes = worker.write(payload + eol_bytes)
        self.ingest(name, sent_text, kind="tx")

        result: dict = {
            "port": name,
            "sent_bytes": sent_bytes,
            "sent": sent_text,
            "waited": wait_regex is not None,
            "matched": False,
            "line": None,
            "seq": None,
        }
        if wait_regex is not None:
            waited = self.wait_for(
                name, wait_regex,
                timeout_ms=timeout_ms if timeout_ms is not None else 10000,
                since_seq=from_seq,
            )
            result["matched"] = waited["matched"]
            result["line"] = waited["line"]
            result["seq"] = waited["seq"]
        return result

    # ---- ZMODEM（rz/sz raw 会话）----
    def zmodem_status(self, name: str) -> dict:
        """``{"port","active","session","direction","owner","rx_bytes","tx_bytes"}``；未知端口 → KeyError。"""
        return self._require_worker(name).zmodem_status()

    def zmodem_claim(self, name: str, client_id: str) -> "tuple[str, queue.Queue]":
        """认领该口当前会话，返回 ``(session_id, queue)``；认领前收到的字节先回放进队列。

        队列无界，元素为负载 ``bytes``，``None`` 为结束哨兵。无会话 →
        ``RuntimeError("zmodem: no active session")``；已被别的客户端认领 →
        ``RuntimeError("zmodem: claimed by another client")``。
        """
        return self._require_worker(name).zmodem_claim(client_id)

    def zmodem_write(self, name: str, session_id: str, client_id: str, data: bytes) -> int:
        """会话 owner 写原始字节（不追加 EOL、不入环）；返回写出字节数。"""
        return self._require_worker(name).zmodem_write(session_id, client_id, data)

    def zmodem_end(
        self,
        name: str,
        session_id: str,
        client_id: Optional[str],
        *,
        abort: bool = False,
        reason: str = "done",
        tail: bytes = b"",
    ) -> dict:
        """结束会话（幂等）；``abort=True`` 先向设备写 ``ABORT_SEQ``。返回结束摘要。

        ``done`` 且非 abort 时，``tail``（客户端收到的会话外字节）→ 队列中未送出的字节 → 实时字节
        依次回到行模式（作为 rx 断行，排在 end 标记之后）；其余情况忽略 ``tail``。
        """
        return self._require_worker(name).zmodem_end(
            session_id, client_id, abort=abort, reason=reason, tail=tail
        )

    # ---- 供 worker / 桥使用 ----
    def next_seq(self) -> int:
        with self._seq_lock:
            self._seq += 1
            return self._seq

    def ingest(self, port: str, text: str, *, kind: str = "rx", ts_ms: Optional[int] = None) -> Line:
        """建行→入环→广播（§6.2）。"""
        seq = self.next_seq()
        line = Line(
            seq=seq,
            port=port,
            ts_ms=ts_ms if ts_ms is not None else now_ms(),
            text=text,
            ansi=has_ansi(text),
            kind=kind,
        )
        with self._lock:
            buf = self._buffers.get(port)
        if buf is not None:
            buf.append(line)
        self._broadcast({"type": "line", "port": port, "line": line.to_dict()})
        return line

    def write_bytes(self, name: str, data: bytes) -> int:
        """原始字节写串口（不追加 EOL）。"""
        return self._require_worker(name).write(bytes(data))

    def set_pending(self, port: str, text: str) -> None:
        """worker 发布该口的未结束尾巴（§16）；有变化时广播 ``{"type":"partial","port","text"}``。

        无 seq、不入环；空串 = 无尾巴（EOL 到来、断开、关闭、ZMODEM 开始时）。
        """
        text = str(text or "")
        with self._lock:
            if self._pending.get(port, "") == text:
                return
            if text:
                self._pending[port] = text
            else:
                self._pending.pop(port, None)
        self._broadcast({"type": "partial", "port": port, "text": text})

    def pending(self, name: str) -> str:
        """该口当前的未结束尾巴（已去 ANSI，≤ 240 字符）；没有时为空串。"""
        with self._lock:
            return self._pending.get(name, "")

    def subscribe(self, notify: Any = None) -> "queue.Queue[dict]":
        """订阅广播；``notify``（可选，无参可调用）在每个事件入队后被调用（须快速、不阻塞）。

        WS 转发用它在事件循环里一次唤醒、批量取走（见 server ``_pump``），不必每个事件占一个线程。
        """
        q: "queue.Queue[dict]" = queue.Queue(maxsize=_SUBSCRIBER_QUEUE_SIZE)
        with self._lock:
            self._subscribers.add(q)
            if notify is not None:
                self._notifiers[q] = notify
        return q

    def unsubscribe(self, q: "queue.Queue[dict]") -> None:
        with self._lock:
            self._subscribers.discard(q)
            self._notifiers.pop(q, None)

    # ---- 桥 ----
    def bridge_start(
        self,
        name: str,
        *,
        mode: str = "server",
        tcp_port: Optional[int] = None,
        host: Optional[str] = None,
    ) -> dict:
        self._require_worker(name)
        with self._lock:
            existing = self._bridges.get(name)
        if existing is not None:
            existing.stop()
        from .tcp_bridge import TcpBridge  # 延迟导入：该文件由其它 agent 提供
        # server 模式下 `host` 即监听绑定地址（默认回环；传 "0.0.0.0" 可跨机访问）；
        # client 模式下 `host` 是远端地址，本地绑定固定回环。
        bind = (host or "127.0.0.1") if mode == "server" else "127.0.0.1"
        bridge = TcpBridge(self, name, mode=mode, tcp_port=tcp_port, host=host, bind=bind)
        info = bridge.start()
        with self._lock:
            self._bridges[name] = bridge
        return info

    def bridge_stop(self, name: str) -> dict:
        with self._lock:
            bridge = self._bridges.pop(name, None)
        if bridge is None:
            return {"port": name, "mode": None, "tcp_port": None, "host": None, "active": False}
        return bridge.stop()

    def bridge_status(self, name: Optional[str] = None) -> dict:
        if name is not None:
            with self._lock:
                bridge = self._bridges.get(name)
            if bridge is None:
                return {"port": name, "mode": None, "tcp_port": None, "host": None, "active": False}
            return bridge.status()
        with self._lock:
            items = [b.status() for b in self._bridges.values()]
        return {
            "port": None,
            "mode": None,
            "tcp_port": None,
            "host": None,
            "active": any(bool(i.get("active")) for i in items),
            "bridges": items,
        }

    @property
    def config(self) -> Config:
        return self._config

    # ---- 内部工具 ----
    def _broadcast(self, event: dict) -> None:
        with self._lock:
            subs = list(self._subscribers)
            notifiers = dict(self._notifiers) if self._notifiers else None
        for q in subs:
            try:
                q.put_nowait(event)
            except queue.Full:
                pass  # 订阅者消费过慢，按契约丢弃
            if notifiers is not None:
                notify = notifiers.get(q)
                if notify is not None:
                    try:
                        notify()
                    except Exception:  # noqa: BLE001 - 订阅方已关闭（如事件循环已停）
                        pass

    def _publish_error(self, name: str, exc: Exception) -> None:
        with self._lock:
            worker = self._workers.get(name)
        self._broadcast({
            "type": "status",
            "port": name,
            "open": worker.is_open if worker is not None else False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    def _require_worker(self, name: str) -> SerialPortWorker:
        with self._lock:
            worker = self._workers.get(name)
        if worker is None:
            raise KeyError(name)
        return worker

    def _require_buffer(self, name: str) -> RingBuffer:
        with self._lock:
            buf = self._buffers.get(name)
        if buf is None:
            raise KeyError(name)
        return buf

    def _port_config(self, name: str) -> PortConfig:
        for pc in self._config.ports:
            if pc.name == name:
                return pc
        raise KeyError(name)

    @staticmethod
    def _validate_match(match: Any) -> dict:
        """校验运行时 match 规格；非法 → ``ValueError``（消息点名违规键）。

        规则与 :mod:`serialhub.discovery` 一致：必须是 mapping，键 ⊂
        ``MATCH_KEYS``，``vid``/``pid`` 经 :func:`discovery.parse_usb_id` 校验。
        """
        if not isinstance(match, dict):
            raise ValueError(t("hub.error.set_device_match_type"))
        for key in match:
            if key not in discovery.MATCH_KEYS:
                raise ValueError(t("hub.error.set_device_match_key", bad_key=key))
        for key in ("vid", "pid"):
            if key in match:
                try:
                    discovery.parse_usb_id(match[key])
                except ValueError as exc:
                    raise ValueError(
                        t("hub.error.set_device_usb", bad_key=key, value=match[key])
                    ) from exc
        return dict(match)

    def _replace_worker(self, name: str, pc: PortConfig) -> None:
        with self._lock:
            self._workers[name] = SerialPortWorker(pc, self)

    def _replace_port_config(self, name: str, pc: PortConfig) -> None:
        with self._lock:
            self._config.ports = [pc if p.name == name else p for p in self._config.ports]

    def _rebuild_buffer(self, name: str, max_lines: int, max_bytes: int) -> None:
        with self._lock:
            old = self._buffers.get(name)
        new = RingBuffer(max_lines, max_bytes)
        if old is not None:
            for line in old.snapshot():
                new.append(line)
        with self._lock:
            self._buffers[name] = new

    def _wait_hit(self, name: str, line: dict, cursor: int) -> dict:
        return self._add_pending(name, {
            "port": name,
            "matched": True,
            "line": line,
            "seq": line["seq"],
            "ts_ms": line["ts_ms"],
            "from_seq": cursor,
        })

    def _wait_pending_hit(self, name: str, tail: str, cursor: int) -> dict:
        """正则命中未结束尾巴：没有行、没有 seq；``pending`` 是命中的那段文本。"""
        return {
            "port": name, "matched": True, "line": None, "seq": None, "ts_ms": None,
            "from_seq": cursor, "pending": tail,
        }

    def _add_pending(self, name: str, result: dict) -> dict:
        """有未结束尾巴时追加 ``pending`` 字段（没有时不加，保持旧形状）。"""
        tail = self.pending(name)
        if tail:
            result["pending"] = tail
        return result

    def _server_info(self) -> dict:
        return {
            "host": self._config.server.host,
            "port": self._config.server.port,
            "version": self._version(),
            "uptime_ms": now_ms() - self._start_ms,
        }

    def _bridge_view(self, name: str, pc: PortConfig) -> Optional[dict]:
        with self._lock:
            bridge = self._bridges.get(name)
        if bridge is not None:
            s = bridge.status()
            return {
                "mode": s.get("mode"),
                "port": s.get("tcp_port"),
                "host": s.get("host"),
                "active": bool(s.get("active")),
            }
        if pc.tcp_bridge:
            return {
                "mode": pc.tcp_bridge.get("mode"),
                "port": pc.tcp_bridge.get("port"),
                "host": pc.tcp_bridge.get("host"),
                "active": False,
            }
        return None

    def _port_status(self, name: str) -> dict:
        worker = self._require_worker(name)
        pc = self._port_config(name)
        buf = self._require_buffer(name)
        st = worker.status()
        return {
            "name": name,
            "device": pc.device,
            "description": pc.description,
            "open": st["open"],
            "baud": pc.baud,
            "data_bits": pc.data_bits,
            "parity": pc.parity,
            "stop_bits": pc.stop_bits,
            "flow": pc.flow,
            "framing": pc.framing,
            "flush_ms": pc.flush_ms,
            "pins": pins_view(pc),
            "rx_lines": st["rx_lines"],
            "tx_lines": st["tx_lines"],
            "last_error": st["last_error"],
            "reconnect_count": st["reconnect_count"],
            "buffer": buf.stats(),
            "tcp_bridge": self._bridge_view(name, pc),
            **({"pending": tail} if (tail := self.pending(name)) else {}),
        }

    @staticmethod
    def _version() -> str:
        try:
            from . import __version__
            return __version__
        except Exception:
            return "0.1.0"

    @staticmethod
    def _system_ports() -> list[dict]:
        result: list[dict] = []
        try:
            for info in _list_ports.comports():
                result.append({
                    "device": info.device,
                    "description": info.description,
                    "hwid": info.hwid,
                })
        except Exception:
            return []
        return result
