"""串口工作线程（§7）。

职责：每个逻辑口一个守护线程，负责持久打开、断线指数退避重连、读取字节并按
``\\n``/``\\r``/``\\r\\n`` 断行，逐行经 ``hub.ingest`` 入环广播。断帧策略由
``pc.framing`` 决定：

- ``"line"``（默认）：只按行结束符分行；无结束符的残行一直留在缓冲，直到该口断开/
  被 ``close`` 时才作为一行吐出（吐出前去掉结尾单个 ``\\r``）。
- ``"idle"``：保留 ``flush_ms`` 时间片分帧，且仅当 ``flush_ms > 0`` 时生效。

解码：每个 worker 一个增量解码器（``codecs.getincrementaldecoder``），(重)连接时
``reset()``；不完整的多字节序列留到下一个产出边界，绝不产生 U+FFFD。

引脚控制（DTR/RTS）：由 ``pc.pins`` 决定，**默认 enabled=False**，此时 worker 任何时刻
都不调用 ``setDTR``/``setRTS``（含 open 时），所有运行时引脚 API 抛 ``ValueError``
（消息含 "pins disabled" 与端口名）。启用后 open 时置初始电平
（``pins.dtr`` 优先，回退顶层 ``pc.dtr``），并按 :meth:`SerialPortWorker.set_pin` /
:meth:`~SerialPortWorker.pulse_pin` / :meth:`~SerialPortWorker.reset_pins` 操作引脚；
睡眠一律在 ``_state_lock`` 之外，序列途中端口关闭则安静退出。

ZMODEM（``pc.zmodem``，默认开）：读到的字节先过 :class:`~serialhub.zmodem.ZmodemDetector`，
只把头之前的部分断行；命中起始头即把头前残行立即成行、开启会话并切到 raw 模式——此后
读到的全部字节只进会话（认领前进 ``pending``，认领后进会话的无界队列），不断行、不入环、
不广播。每轮读循环检查认领/空闲超时；会话结束（含超时、关口、断线）后回到行模式。
非 ``done`` 结束后先“排空”：丢弃读到的字节（检测器也停用），直到连续
``DRAIN_SILENCE_MS`` 无数据或总计 ``DRAIN_MAX_MS``，再以干净的行缓冲/解码器恢复。
``done`` 结束时，客户端交回的会话外尾巴（``tail``，如 lsz 的 ``OO`` 之后设备打印的
``sz done``/提示符）与会话队列里还没送出的字节按此顺序重新注入读循环，当作 rx 断行，
再接实时字节；注入只在读循环线程里处理，并在 end 标记写入之后才放行。
会话进行中读循环只读 ``in_waiting`` 字节、不在 ``_io_lock`` 下阻塞，owner 的写不被饿死。
会话期间普通 :meth:`SerialPortWorker.write` 被拒绝，只有会话 owner 经
:meth:`SerialPortWorker.zmodem_write` 写原始字节。``_zm_lock`` 是叶子锁：持有期间
不做串口 I/O、不调 hub、不阻塞。

任何串口错误都记入 ``last_error`` 并广播 status 事件，绝不崩溃进程。

仅被 ``Hub`` 使用；不直接改缓冲（一律经 ``hub.ingest``）。
"""

# allow: SIZE_OK - 契约 §7 要求单文件内实现完整 SerialPortWorker 读写/断行/重连循环。

from __future__ import annotations

import codecs
import logging
import queue
import select
import socket
import threading
from typing import TYPE_CHECKING, Optional

import serial

from . import discovery
from .config import DEFAULT_PULSE_MS, normalize_pins
from .i18n import t
from .models import now_ms
from . import zmodem as _zmodem  # 排空/队列上限常量按模块属性读取（便于测试调小）
from .zmodem import ABORT_SEQ, END_REASONS, Detection, ZmodemDetector, ZmodemSession

if TYPE_CHECKING:  # 仅类型标注，避免与 hub 形成运行期循环导入
    from .hub import Hub

__all__ = ["SerialPortWorker"]

_log = logging.getLogger(__name__)

_CR = 0x0D
_LF = 0x0A

#: ZMODEM 会话中读循环无数据时的轮询间隔（秒）。
_ACTIVE_POLL_S = 0.005

#: 允许被引脚 API 操作的引脚（DTR/RTS）。
_PIN_NAMES = ("dtr", "rts")


def _find_eol(buf: bytearray) -> Optional[tuple[int, int]]:
    """定位最早的行结束符，返回 (行内容结束下标, 需删除到的下标)。

    ``\\r\\n`` 视为一个结束符；结尾孤立 ``\\r`` 暂缓（等待可能的 ``\\n``）。
    """
    n = len(buf)
    for i in range(n):
        b = buf[i]
        if b == _LF:
            return i, i + 1
        if b == _CR:
            if i + 1 < n:
                return i, (i + 2 if buf[i + 1] == _LF else i + 1)
            return None
    return None


class SerialPortWorker(threading.Thread):
    """单口串口读写线程。基类为 ``Thread``，实际读循环由内部线程承载以支持开/关复用。"""

    def __init__(self, pc, hub: "Hub") -> None:
        super().__init__(name=f"serialhub-{getattr(pc, 'name', 'port')}", daemon=True)
        self._pc = pc
        self._hub = hub
        self._serial: Optional[serial.SerialBase] = None
        self._open: bool = False
        self._want_open: bool = False
        self._last_error: Optional[str] = None
        self._reconnect_count: int = 0
        self._rx_lines: int = 0
        self._tx_lines: int = 0
        self._encoding: str = getattr(pc, "encoding", None) or "utf-8"
        self._flush_ms: int = max(0, int(getattr(pc, "flush_ms", 50)))
        # 断帧策略：默认 "line"（只按 EOL 分行）；"idle" 才启用 flush_ms 时间片分帧。
        self._framing: str = str(getattr(pc, "framing", None) or "line")
        # 引脚控制配置（归一化形状）与生效初始电平（pins.dtr 优先，回退 pc.dtr）。
        self._pins: dict = normalize_pins(getattr(pc, "pins", None), strict=False)
        self._pin_initial: dict = self._resolve_initial(pc)
        # 当前电平：None 表示未知（未打开或引脚控制未启用）。
        self._pin_state: dict = {"dtr": None, "rts": None}
        # 每个 worker 一个增量解码器，跨分片的多字节字符留到下一段，不产生 U+FFFD。
        self._decoder: "codecs.IncrementalDecoder" = self._make_decoder()
        # 是否用 match 自动发现设备：为真时每次连接/重连前重新解析真实设备名。
        self._uses_match: bool = bool(getattr(pc, "match", None))
        self._state_lock = threading.RLock()
        self._io_lock = threading.Lock()
        self._counters_lock = threading.Lock()
        self._stop = threading.Event()
        self._loop_thread: Optional[threading.Thread] = None
        # ZMODEM：检测器只由读循环线程使用；会话指针与会话字段由 _zm_lock 守护。
        self._zm_enabled: bool = bool(getattr(pc, "zmodem", True))
        self._zm_claim_ms: int = int(getattr(pc, "zmodem_claim_ms", 5000))
        self._zm_idle_ms: int = int(getattr(pc, "zmodem_idle_ms", 15000))
        self._zm_detector = ZmodemDetector()
        self._zm_lock = threading.Lock()
        self._zm_session: Optional[ZmodemSession] = None
        self._zm_last_end: Optional[dict] = None
        # 非 done 结束后的排空状态：开始时刻 / 最近一次读到字节的时刻（None = 不在排空）。
        self._zm_drain_started: Optional[int] = None
        self._zm_drain_last_rx: int = 0
        # done 结束后回注入行模式的字节（tail → 队列残余 → 期间读到的实时字节），只由读循环取走；
        # _zm_inject_hold 为真时（end 标记尚未写入）读到的字节也先追加到这里，保证顺序。
        self._zm_inject = bytearray()
        self._zm_inject_hold = False

    # ---- 属性 ----
    @property
    def is_open(self) -> bool:
        with self._state_lock:
            return self._open

    @property
    def last_error(self) -> Optional[str]:
        with self._state_lock:
            return self._last_error

    @property
    def reconnect_count(self) -> int:
        with self._state_lock:
            return self._reconnect_count

    @property
    def rx_lines(self) -> int:
        with self._counters_lock:
            return self._rx_lines

    @property
    def tx_lines(self) -> int:
        with self._counters_lock:
            return self._tx_lines

    @property
    def resolved_device(self) -> str:
        """当前解析到的真实设备名（match 端口为发现的 COM，固定端口即配置值）。"""
        with self._state_lock:
            return self._pc.device

    @property
    def pin_state(self) -> dict:
        """当前 DTR/RTS 电平（只读）；``None`` 表示未知（未打开/未启用）。"""
        with self._state_lock:
            return {"dtr": self._pin_state["dtr"], "rts": self._pin_state["rts"]}

    @property
    def pins_config(self) -> dict:
        """归一化引脚配置；``dtr``/``rts`` 为 open 时生效的初始电平（已回退顶层值）。"""
        with self._state_lock:
            return {
                "enabled": bool(self._pins.get("enabled")),
                "dtr": bool(self._pin_initial["dtr"]),
                "rts": bool(self._pin_initial["rts"]),
                "pulse_ms": int(self._pins.get("pulse_ms", DEFAULT_PULSE_MS)),
            }

    # ---- 引脚（DTR/RTS）----
    def set_pin(self, pin: str, value: bool) -> dict:
        """设置 ``pin`` 电平；未启用 → ValueError，端口未打开 → RuntimeError。"""
        ser = self._require_pins(pin)
        self._apply_pin(ser, pin, bool(value))
        return self.pin_state

    def pulse_pin(self, pin: str, duration_ms: Optional[int] = None) -> dict:
        """把 ``pin`` 翻到相反电平，保持 ``duration_ms``（默认 ``pins.pulse_ms``）后恢复。

        睡眠在 ``_state_lock`` 之外；期间端口关闭则安静退出（不抛异常）。
        """
        ser = self._require_pins(pin)
        ms = self._resolve_duration(duration_ms)
        base = self._base_level(pin)
        self._apply_pin(ser, pin, not base)
        if not self._pin_sleep(ms):
            self._restore_quietly(ser, pin, base)
            return self.pin_state
        self._apply_pin(ser, pin, base)
        return self.pin_state

    def reset_pins(self, duration_ms: Optional[int] = None) -> dict:
        """经典 CH340/ESP 自动复位序列：``rts=True,dtr=False`` → ``rts=False,dtr=True``
        → 恢复初始电平，每段间隔 ``duration_ms``（默认 ``pins.pulse_ms``）。

        睡眠在 ``_state_lock`` 之外；序列途中端口关闭则安静退出（不抛异常）。
        """
        ser = self._require_pins()
        ms = self._resolve_duration(duration_ms)
        initial = dict(self._pin_initial)
        try:
            self._apply_pin(ser, "rts", True)
            self._apply_pin(ser, "dtr", False)
            if not self._pin_sleep(ms):
                return self.pin_state
            self._apply_pin(ser, "rts", False)
            self._apply_pin(ser, "dtr", True)
            if not self._pin_sleep(ms):
                return self.pin_state
        except RuntimeError:
            if not self.is_open:
                return self.pin_state  # 途中端口关闭：安静退出（错误已记 last_error）
            raise
        for pin in _PIN_NAMES:
            if not self.is_open:
                return self.pin_state
            try:
                self._apply_pin(ser, pin, bool(initial[pin]))
            except RuntimeError:
                return self.pin_state
        return self.pin_state

    def _resolve_initial(self, pc) -> dict:
        """生效初始电平：``pins.dtr``/``pins.rts`` 优先，回退顶层 ``pc.dtr``/``pc.rts``。"""
        return {
            pin: (
                self._pins[pin]
                if self._pins.get(pin) is not None
                else bool(getattr(pc, pin, True))
            )
            for pin in _PIN_NAMES
        }

    def _require_pins(self, pin: Optional[str] = None) -> "serial.SerialBase":
        """校验引脚控制已启用、``pin`` 合法且端口已打开；返回串口对象。"""
        with self._state_lock:
            enabled = bool(self._pins.get("enabled"))
            ser = self._serial
            opened = self._open
        if not enabled:
            raise ValueError(t("hub.error.pins_disabled", name=self._pc.name))
        if pin is not None and pin not in _PIN_NAMES:
            raise ValueError(t("hub.error.pins_unknown_pin", pin=pin, name=self._pc.name))
        if ser is None or not opened:
            raise RuntimeError(t("hub.error.pins_not_open", name=self._pc.name))
        return ser

    def _apply_pin(self, ser: "serial.SerialBase", pin: str, value: bool) -> None:
        """调用 ``setDTR``/``setRTS`` 并记录电平（不在 ``_state_lock`` 下写串口）。"""
        setter = ser.setDTR if pin == "dtr" else ser.setRTS
        try:
            with self._io_lock:
                setter(bool(value))
        except Exception as exc:
            with self._state_lock:
                self._last_error = f"{pin}: {type(exc).__name__}: {exc}"
            self._broadcast_status()
            raise RuntimeError(
                t("hub.error.pins_failed", pin=pin, name=self._pc.name, error=exc)
            ) from exc
        with self._state_lock:
            self._pin_state[pin] = bool(value)

    def _base_level(self, pin: str) -> bool:
        """脉冲的基准电平：当前已知电平，未知时退回初始电平。"""
        with self._state_lock:
            current = self._pin_state[pin]
        return bool(current) if current is not None else bool(self._pin_initial[pin])

    def _resolve_duration(self, duration_ms: Optional[int]) -> int:
        """``duration_ms`` 缺省 → ``pins.pulse_ms``；非法/负数 → ValueError。"""
        if duration_ms is None:
            with self._state_lock:
                return int(self._pins.get("pulse_ms", DEFAULT_PULSE_MS))
        try:
            ms = int(duration_ms)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                t("hub.error.pins_bad_duration", value=duration_ms)
            ) from exc
        if ms < 0:
            raise ValueError(t("hub.error.pins_bad_duration", value=ms))
        return ms

    def _pin_sleep(self, ms: int) -> bool:
        """在锁外等待 ``ms`` 毫秒；返回 False 表示端口已关闭/已停止（应安静退出）。"""
        if ms > 0:
            self._stop.wait(ms / 1000.0)
        return self.is_open and not self._stop.is_set()

    def _restore_quietly(self, ser: "serial.SerialBase", pin: str, value: bool) -> None:
        """尽力恢复电平；端口已关或写入失败时静默放弃（用于脉冲被中断的收尾）。"""
        if not self.is_open:
            return
        try:
            self._apply_pin(ser, pin, value)
        except RuntimeError:
            pass

    # ---- 生命周期 ----
    def open(self) -> None:
        """立即尝试打开（失败置 last_error，不抛），并确保读循环在运行。"""
        with self._state_lock:
            self._want_open = True
            self._stop.clear()
            self._try_connect()
            if self._loop_thread is None or not self._loop_thread.is_alive():
                self._loop_thread = threading.Thread(
                    target=self._run_loop,
                    name=f"serialhub-{self._pc.name}-io",
                    daemon=True,
                )
                self._loop_thread.start()

    def close(self) -> None:
        """停止读循环并释放端口（幂等）；进行中的 ZMODEM 会话以 ``closed`` 结束。"""
        with self._state_lock:
            self._want_open = False
        self._stop.set()
        thread = self._loop_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=3.0)
        self._zm_end_current("closed", abort=True)  # 关口前尽力让设备退出 rz/sz
        self._close_serial()
        self._broadcast_status()

    def update_config(self, pc) -> None:
        """热重载时替换端口配置（线程安全，供 Hub 调用）。"""
        with self._state_lock:
            self._pc = pc
            encoding = getattr(pc, "encoding", None) or "utf-8"
            if encoding != self._encoding:
                self._encoding = encoding
                self._decoder = self._make_decoder()  # 换编码即换解码器
            self._flush_ms = max(0, int(getattr(pc, "flush_ms", 50)))
            self._framing = str(getattr(pc, "framing", None) or "line")
            self._uses_match = bool(getattr(pc, "match", None))
            self._pins = normalize_pins(getattr(pc, "pins", None), strict=False)
            self._pin_initial = self._resolve_initial(pc)
        with self._zm_lock:
            self._zm_enabled = bool(getattr(pc, "zmodem", True))
            self._zm_claim_ms = int(getattr(pc, "zmodem_claim_ms", 5000))
            self._zm_idle_ms = int(getattr(pc, "zmodem_idle_ms", 15000))

    def _make_decoder(self) -> "codecs.IncrementalDecoder":
        """按当前 ``encoding`` 建增量解码器；未知编码回退 utf-8（不让配置炸掉读循环）。"""
        try:
            factory = codecs.getincrementaldecoder(self._encoding)
        except LookupError:
            factory = codecs.getincrementaldecoder("utf-8")
        return factory(errors="replace")  # type: ignore[no-any-return]

    def write(self, data: bytes) -> int:
        """线程安全写串口（原始字节，不追加 EOL）；未打开 → RuntimeError。

        ZMODEM 会话进行中 → RuntimeError（普通写入会破坏传输；会话内只走
        :meth:`zmodem_write`）。
        """
        with self._zm_lock:
            active = self._zm_session is not None
        if active:
            raise RuntimeError(f"zmodem: session active on port '{self._pc.name}'")
        count = self._write_raw(data)
        with self._counters_lock:
            self._tx_lines += 1
        return count

    def _write_raw(self, data: bytes) -> int:
        """写原始字节并 flush（不计 tx_lines）；未打开 → RuntimeError，写失败记 last_error 后重抛。

        在读循环线程里（如截止时间触发的中止串）不做 flush（tcdrain）：tcdrain 要等对端读走数据，
        而对端（或中间的 socat）此刻可能正卡在把数据写给我们，读循环再等它就是死锁。
        """
        drain = threading.current_thread() is not self._loop_thread
        with self._state_lock:
            ser = self._serial
            if ser is None or not self._open:
                raise RuntimeError(f"port '{self._pc.name}' is not open")
        try:
            with self._io_lock:
                written = ser.write(data)
            # flush（tcdrain）放在 _io_lock 外：它要等对端读走数据，若持锁等，读循环读不了，
            # 正在发送的设备（lsz）写满缓冲后不再读输入 → tcdrain 永不返回（死锁）。
            try:
                if drain:
                    ser.flush()
            except Exception as exc:  # flush 失败记录但不致命
                with self._state_lock:
                    self._last_error = f"flush: {type(exc).__name__}: {exc}"
        except Exception as exc:
            with self._state_lock:
                self._last_error = f"write: {type(exc).__name__}: {exc}"
            self._broadcast_status()
            raise
        return int(written) if written is not None else len(data)

    def status(self) -> dict:
        with self._state_lock:
            return {
                "open": self._open,
                "last_error": self._last_error,
                "reconnect_count": self._reconnect_count,
                "rx_lines": self.rx_lines,
                "tx_lines": self.tx_lines,
            }

    # ---- 连接 ----
    def _resolve_device(self) -> Optional[str]:
        """match 端口：重新解析真实设备名并写回 ``pc.device``。

        每次连接/重连前调用（先把 ``pc.device`` 清空以强制重解析），这样设备拔插
        后换到别的 COM 仍能找回。无命中或解析异常 → 置 ``last_error``、返回 None，
        不抛异常、不崩溃，交由外层退避重试。
        """
        pc = self._pc
        pc.device = ""
        try:
            found = discovery.resolve(pc.match or {})
        except Exception as exc:
            self._last_error = f"discovery: {type(exc).__name__}: {exc}"
            self._open = False
            self._serial = None
            self._broadcast_status()
            return None
        device = found.get("device")
        if not device:
            self._last_error = f"no device matched: {discovery.describe(pc.match or {})}"
            self._open = False
            self._serial = None
            self._broadcast_status()
            return None
        pc.device = device
        return device

    def _try_connect(self) -> bool:
        """在 _state_lock 下尝试建立连接；成功 True，失败 False（置 last_error）。"""
        if self._open and self._serial is not None:
            return True
        pc = self._pc
        if self._uses_match:
            device = self._resolve_device()
            if device is None:
                return False
        else:
            device = pc.device
        try:
            ser = serial.serial_for_url(
                device,
                baudrate=pc.baud,
                bytesize=pc.data_bits,
                parity=pc.parity,
                stopbits=pc.stop_bits,
                rtscts=pc.flow == "rtscts",
                xonxoff=pc.flow == "xonxoff",
                dsrdtr=pc.flow == "dsrdtr",
                timeout=0.1,
                write_timeout=1,
            )
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"
            self._open = False
            self._serial = None
            self._broadcast_status()
            return False
        # 新连接（读循环重连或 open() 直接打开）：上一个连接的回注字节作废、没有残余负载
        # 可排空——否则 closed 结束后的排空会吞掉设备的第一段输出（开机横幅）。
        with self._zm_lock:
            self._zm_inject.clear()
            self._zm_drain_started = None
        self._serial = ser
        self._open = True
        self._pin_state = {"dtr": None, "rts": None}
        # 默认（pins.enabled=False）：绝不触碰 DTR/RTS —— 顶层 dtr/rts 只在引脚控制
        # 启用后才在 open 时生效（迁移说明见 config.example.yaml）。
        pin_error: Optional[str] = None
        if self._pins.get("enabled"):
            pin_error = self._apply_initial_pins(ser)
        if self._uses_match:
            self._last_error = pin_error  # 连接成功清掉“未发现/重连”错误（保留引脚错误）
        self._broadcast_status()
        return True

    def _apply_initial_pins(self, ser: "serial.SerialBase") -> Optional[str]:
        """open 时置初始电平；单脚失败只记 ``last_error``，不关端口。返回错误文本或 None。"""
        error: Optional[str] = None
        for pin in _PIN_NAMES:
            setter = ser.setDTR if pin == "dtr" else ser.setRTS
            try:
                setter(bool(self._pin_initial[pin]))
            except Exception as exc:  # 某些 URL 不支持 DTR/RTS：记录但不致命
                error = f"{pin}: {type(exc).__name__}: {exc}"
                self._last_error = error
            else:
                self._pin_state[pin] = bool(self._pin_initial[pin])
        return error

    def _close_serial(self) -> None:
        with self._io_lock:
            with self._state_lock:
                ser = self._serial
                self._serial = None
                self._open = False
                self._pin_state = {"dtr": None, "rts": None}  # 关闭后电平未知
            if ser is not None:
                try:
                    ser.close()
                except Exception as exc:
                    with self._state_lock:
                        self._last_error = f"close: {type(exc).__name__}: {exc}"

    def _broadcast_status(self) -> None:
        with self._state_lock:
            event = {
                "type": "status",
                "port": self._pc.name,
                "open": self._open,
                "last_error": self._last_error,
                "reconnect_count": self._reconnect_count,
            }
        self._hub._broadcast(event)

    # ---- 读循环 ----
    def _run_loop(self) -> None:
        pc = self._pc
        backoff = max(1, int(pc.reconnect_ms))
        backoff_max = max(backoff, int(pc.reconnect_max_ms))
        buf = bytearray()
        residual_since: Optional[int] = None

        try:
            while not self._stop.is_set():
                with self._state_lock:
                    want = self._want_open
                if not want:
                    self._stop.wait(0.05)
                    continue

                if not self.is_open:
                    if self._try_connect():
                        backoff = max(1, int(self._pc.reconnect_ms))
                        buf.clear()
                        residual_since = None
                        self._decoder.reset()  # (重)连接：丢弃上一个连接的半字符状态
                    else:
                        with self._state_lock:
                            self._reconnect_count += 1
                        self._stop.wait(backoff / 1000.0)
                        backoff = min(backoff * 2, backoff_max)
                        continue

                data = self._read_once()
                if data is None:
                    self._zm_end_current("closed", abort=False)  # 串口已断，无法再写中止串
                    buf.extend(self._zm_detector.flush())
                    self._flush_line_residual(buf)  # line 模式：断开前吐出残行
                    buf.clear()
                    residual_since = None
                    with self._state_lock:
                        self._reconnect_count += 1
                    self._stop.wait(backoff / 1000.0)
                    backoff = min(backoff * 2, backoff_max)
                    continue

                now = now_ms()
                if data or self._zm_inject_ready():
                    text, detection = self._zm_split(data)
                else:
                    # 读超时（空读）：释放检测器留住的头前缀，普通文本最多迟一轮。
                    text, detection = self._zm_detector.flush(), None
                if text:
                    residual_since = self._frame(buf, text, residual_since, now)
                if detection is not None:
                    if buf:
                        self._drain_residual(buf)  # 头前残行（如 sz 打印的 "rz\r"）立即成行
                    residual_since = None
                    self._zm_begin(detection)
                self._zm_check_deadlines()
                if self._zm_drain_done():
                    # 排空结束：以干净状态回到行模式（丢弃残行/半字符/留住的前缀）。
                    buf.clear()
                    residual_since = None
                    self._decoder.reset()
                    self._zm_detector.flush()

                # 时间片分帧只在 framing == "idle" 且 flush_ms > 0 时生效（flush_ms=0
                # 不再等价于“每轮都吐”）；默认的 "line" 模式残行一直留到断开/关闭。
                if (
                    self._framing == "idle"
                    and self._flush_ms > 0
                    and buf
                    and residual_since is not None
                    and (now_ms() - residual_since) >= self._flush_ms
                ):
                    self._drain_residual(buf)
        finally:
            # close()/stop() 退出循环：line 模式的残行（含检测器留住的字节）作为最后一行吐出。
            buf.extend(self._zm_detector.flush())
            self._flush_line_residual(buf)

    def _frame(
        self, buf: bytearray, text: bytes, residual_since: Optional[int], now: int
    ) -> Optional[int]:
        """把文本字节并入行缓冲并吐出整行；返回更新后的残行起始时刻。"""
        if not buf:
            residual_since = now
        buf.extend(text)
        self._emit_complete_lines(buf)
        if not buf:
            return None
        return residual_since if residual_since is not None else now

    # ---- ZMODEM ----
    def zmodem_status(self) -> dict:
        """当前会话快照：``{"port","active","session","direction","owner","rx_bytes","tx_bytes"}``。"""
        with self._zm_lock:
            s = self._zm_session
            if s is None:
                return {
                    "port": self._pc.name, "active": False, "session": None,
                    "direction": None, "owner": None, "rx_bytes": 0, "tx_bytes": 0,
                }
            return {
                "port": s.port, "active": True, "session": s.id,
                "direction": s.direction, "owner": s.owner,
                "rx_bytes": s.rx_bytes, "tx_bytes": s.tx_bytes,
            }

    def zmodem_claim(self, client_id: str) -> tuple[str, "queue.Queue[Optional[bytes]]"]:
        """认领当前会话：先把 ``pending`` 回放进队列，返回 ``(session_id, queue)``。

        同一 ``client_id`` 重复认领返回同一会话；无会话 / 已被他人认领 → RuntimeError。
        首次认领成功后广播 ``{"type":"zmodem","event":"claimed","port","session"}``（只有元数据，
        不含 owner），其他浏览器据此在点击前就显示“已被占用”。
        """
        with self._zm_lock:
            s = self._zm_session
            if s is None:
                raise RuntimeError("zmodem: no active session")
            if s.owner is not None and s.owner != client_id:
                raise RuntimeError("zmodem: claimed by another client")
            newly = s.owner is None
            if newly:
                s.owner = client_id
                if s.pending:
                    s.queue.put_nowait(bytes(s.pending))
                    s.pending.clear()
                s.last_activity_ms = now_ms()
        if newly:  # 锁外广播（_zm_lock 是叶子锁）
            self._hub._broadcast({
                "type": "zmodem", "event": "claimed", "port": s.port, "session": s.id,
            })
        return s.id, s.queue

    def zmodem_write(self, session_id: str, client_id: str, data: bytes) -> int:
        """会话 owner 写原始字节到设备；计入 ``tx_bytes`` 并刷新活动时间。

        按 ``max(64, baud // 20)`` 字节（8N1 下约 0.5 s 线上时间）切片依次写，片间释放
        ``_io_lock``：单次 ``ser.write`` 不会超过 ``write_timeout=1``（低波特率下一整块
        8 KiB 会触发 ``SerialTimeoutException``）。某片失败照常抛出。
        """
        self._zm_require_owner(session_id, client_id)
        data = bytes(data)
        piece = max(64, int(self._pc.baud) // 20)
        n = 0
        for off in range(0, len(data), piece):
            n += self._write_raw(data[off:off + piece])
        with self._zm_lock:
            s = self._zm_session
            if s is not None and s.id == session_id:
                s.tx_bytes += n
                s.last_activity_ms = now_ms()
        return n

    def zmodem_end(
        self,
        session_id: str,
        client_id: Optional[str],
        *,
        abort: bool = False,
        reason: str = "done",
        tail: bytes = b"",
    ) -> dict:
        """结束会话（幂等）：返回结束摘要 ``{"port","session","reason","rx_bytes","tx_bytes"}``。

        ``client_id`` 非 None 且会话已被别人认领 → RuntimeError。会话已结束：若正是最近
        结束的那个则返回同一摘要，否则返回 ``reason=None`` 的空摘要（不抛）。
        ``reason="done"`` 且非 ``abort`` 时，``tail``（客户端收到的会话外字节）与队列里尚未
        送出的字节依次回注行模式（见模块说明）；其余情况忽略 ``tail``。
        """
        if reason not in END_REASONS:
            raise ValueError(
                f"zmodem: invalid end reason {reason!r} (allowed: {sorted(END_REASONS)})"
            )
        with self._zm_lock:
            s = self._zm_session
            if s is None or s.id != session_id:
                return self._zm_stale_summary_locked(session_id)
            if client_id is not None and s.owner is not None and s.owner != client_id:
                raise RuntimeError("zmodem: claimed by another client")
            summary = self._zm_detach_locked(s, reason)  # 校验与摘除同一临界区
            if reason == "done" and not abort:
                self._zm_stage_inject_locked(s, bytes(tail))
        return self._zm_after_end(s, summary, abort=abort)

    def _zm_stage_inject_locked(self, session: ZmodemSession, tail: bytes) -> None:
        """（持 ``_zm_lock``，会话刚摘除）把 tail + 队列残余放进回注区，并暂扣到 end 标记写入后。

        队列残余原样当文本回注：客户端若在协议中途发 ``done``，没取走的帧也会进环（客户端的责任）。

        从队列取走的字节不会再被转发（随后放入的 None 让转发方停止），因此不会重复投递。
        """
        self._zm_inject.extend(tail)
        while True:
            try:
                item = session.queue.get_nowait()
            except queue.Empty:
                break
            if item:
                self._zm_inject.extend(item)
        if session.owner is not None:  # 未认领的 pending 是没人看过的协议帧，不当文本回注
            self._zm_inject.extend(session.pending)
        session.pending.clear()
        self._zm_inject_hold = True

    def _zm_require_owner(self, session_id: str, client_id: str) -> None:
        with self._zm_lock:
            s = self._zm_session
            if s is None or s.id != session_id:
                raise RuntimeError("zmodem: no active session")
            if s.owner != client_id:
                raise RuntimeError("zmodem: claimed by another client")

    def _zm_split(self, data: bytes) -> tuple[bytes, Optional[Detection]]:
        """读到的字节分流：会话中 → 全部进会话；回注暂扣中 → 追加到回注区；排空中 → 丢弃；
        否则（回注字节在前）过检测器。

        会话判定、回注取用与排空判定在同一 ``_zm_lock`` 临界区：读到的字节与 end 的摘除/回注
        之间只有一种先后。
        """
        with self._zm_lock:
            s = self._zm_session
            if s is not None:
                if data:
                    s.rx_bytes += len(data)
                    s.last_activity_ms = now_ms()
                    if s.owner is None:
                        s.pending.extend(data)
                    else:
                        s.queue.put_nowait(bytes(data))  # 无界队列不阻塞；与回放/哨兵保序
                return b"", None
            if self._zm_inject_hold:
                self._zm_inject.extend(data)  # end 标记写入前：先排在回注字节之后
                return b"", None
            if self._zm_inject:
                data = bytes(self._zm_inject) + bytes(data)
                self._zm_inject.clear()
            if self._zm_drain_started is not None:
                if data:
                    self._zm_drain_last_rx = now_ms()
                return b"", None  # 会话残余负载：绝不进环
            enabled = self._zm_enabled
        if not data:
            return b"", None
        if not enabled:
            return data, None
        return self._zm_detector.feed(data)

    def _zm_inject_ready(self) -> bool:
        """有待回注、且已放行的字节（读循环空读时也要处理）。"""
        with self._zm_lock:
            return bool(self._zm_inject) and not self._zm_inject_hold

    def _zm_begin(self, detection: Detection) -> None:
        """开启会话：``initial``（头及其后字节）进 pending，写 start 标记并广播 start 事件。"""
        session = ZmodemSession(port=self._pc.name, direction=detection.direction)
        session.pending.extend(detection.initial)
        session.rx_bytes = len(detection.initial)
        with self._zm_lock:
            self._zm_session = session
        self._hub.mark(session.port, f"[zmodem] start {session.direction}")
        self._hub._broadcast({
            "type": "zmodem", "event": "start", "port": session.port,
            "session": session.id, "direction": session.direction,
        })

    def _zm_check_deadlines(self) -> None:
        """每轮读循环调用：认领超时 → ``unclaimed``，双向空闲超时 → ``timeout``，队列里未被
        取走的字节超过 ``QUEUE_MAX_BYTES``（客户端连着却不取）→ ``abort``（均中止）。

        判定与摘除会话在同一 ``_zm_lock`` 临界区内，期间落地的认领/写入不会被误杀。
        """
        with self._zm_lock:
            s = self._zm_session
            if s is None:
                return
            now = now_ms()
            reason: Optional[str] = None
            if s.owner is None and now - s.started_ms >= self._zm_claim_ms:
                reason = "unclaimed"
            elif now - s.last_activity_ms >= self._zm_idle_ms:
                reason = "timeout"
            elif s.queue.queued_bytes > _zmodem.QUEUE_MAX_BYTES:
                reason = "abort"
            if reason is None:
                return
            queued = s.queue.queued_bytes
            summary = self._zm_detach_locked(s, reason)
        if reason == "abort":
            _log.warning(
                "zmodem: port %s session %s aborted: %d undelivered bytes queued (limit %d)",
                s.port, s.id, queued, _zmodem.QUEUE_MAX_BYTES,
            )
        self._zm_after_end(s, summary, abort=True)

    def _zm_end_current(self, reason: str, *, abort: bool) -> None:
        with self._zm_lock:
            s = self._zm_session
            if s is None:
                return
            summary = self._zm_detach_locked(s, reason)
        self._zm_after_end(s, summary, abort=abort)

    def _zm_drain_done(self) -> bool:
        """排空中且已静默 ``DRAIN_SILENCE_MS`` 或总计 ``DRAIN_MAX_MS`` → 退出排空并返回 True。"""
        with self._zm_lock:
            started = self._zm_drain_started
            if started is None:
                return False
            now = now_ms()
            if (
                now - self._zm_drain_last_rx < _zmodem.DRAIN_SILENCE_MS
                and now - started < _zmodem.DRAIN_MAX_MS
            ):
                return False
            self._zm_drain_started = None
            return True

    def _zm_stale_summary_locked(self, session_id: str) -> dict:
        """（持 ``_zm_lock``）已结束/未知会话的幂等摘要：最近结束的那个原样返回，否则 reason=None。"""
        last = self._zm_last_end
        if last is not None and last["session"] == session_id:
            return dict(last)
        return {
            "port": self._pc.name, "session": session_id, "reason": None,
            "rx_bytes": 0, "tx_bytes": 0,
        }

    def _zm_detach_locked(self, session: ZmodemSession, reason: str) -> dict:
        """（持 ``_zm_lock``，且 ``session`` 是当前会话）摘除会话、记摘要；非 done 进入排空。"""
        self._zm_session = None
        summary = {
            "port": session.port, "session": session.id, "reason": reason,
            "rx_bytes": session.rx_bytes, "tx_bytes": session.tx_bytes,
        }
        self._zm_last_end = summary
        if reason != "done":
            now = now_ms()
            self._zm_drain_started = now
            self._zm_drain_last_rx = now
        return summary

    def _zm_after_end(self, session: ZmodemSession, summary: dict, *, abort: bool) -> dict:
        """（锁外）会话摘除后的收尾：可选写中止串、队列放 None、写 end 标记并广播。"""
        if abort and self.is_open:
            try:
                self._write_raw(ABORT_SEQ)  # 在读循环线程里时不 tcdrain（见 _write_raw）
            except Exception:  # noqa: BLE001 - 写失败已由 _write_raw 记入 last_error 并广播
                pass
        session.queue.put_nowait(None)
        try:
            self._hub.mark(
                session.port,
                f"[zmodem] end reason={summary['reason']} "
                f"rx={summary['rx_bytes']} tx={summary['tx_bytes']}",
            )
            self._hub._broadcast({"type": "zmodem", "event": "end", **summary})
        finally:
            with self._zm_lock:
                self._zm_inject_hold = False  # end 标记已在环里：放行回注字节
        return dict(summary)

    def _drain_residual(self, buf: bytearray) -> None:
        """把缓冲里的残行立即吐出（idle 时间片 / line 断开都复用它）。"""
        raw = bytes(buf)
        buf.clear()
        if raw.endswith(b"\r"):
            raw = raw[:-1]  # 结尾孤立 \r 只是行结束符残留，不作为内容
        self._emit(raw)

    def _flush_line_residual(self, buf: bytearray) -> None:
        """line 模式下把残行作为一行吐出；idle 模式不动缓冲（保持时间片语义）。

        只有显式的 ``"idle"`` 才跳过（时间片语义）；其它取值（例如绕过 config
        校验直接构造的 PortConfig）一律按 ``"line"`` 兜底，避免断开/关闭时丢数据。
        """
        if self._framing == "idle" or not buf:
            return
        self._drain_residual(buf)

    def _read_once(self) -> Optional[bytes]:
        """读一次；返回 None 表示连接已断开（错误已记录/广播）。"""
        with self._state_lock:
            ser = self._serial
            opened = self._open
        if ser is None or not opened:
            return None
        with self._zm_lock:
            active = self._zm_session is not None
        try:
            with self._io_lock:
                sock = getattr(ser, "_socket", None)  # pyserial socket:// 实现才有
                if sock is not None and getattr(ser, "is_open", False):
                    # 会话中不阻塞（timeout=0），否则沿用端口读超时。
                    data: bytes = self._read_socket(
                        ser, sock, 0 if active else (ser.timeout or 0)
                    )
                elif active:
                    # 会话中：只取已到达的字节，不在 _io_lock 下阻塞 0.1 s，owner 的写不被饿死。
                    waiting = ser.in_waiting
                    data = ser.read(waiting) if waiting else b""
                else:
                    data = ser.read(4096)
        except Exception as exc:
            with self._state_lock:
                self._last_error = f"read: {type(exc).__name__}: {exc}"
            self._close_serial()
            self._broadcast_status()
            return None
        if active and not data:
            self._stop.wait(_ACTIVE_POLL_S)  # 锁外短暂让出，避免空转
        return data

    @staticmethod
    def _read_socket(ser: "serial.SerialBase", sock: "socket.socket", timeout: float) -> bytes:
        """socket:// 专用读：select + 单次 recv(4096)。

        pyserial 的 ``read()`` 在循环里累积字节，recv 返回 b""（对端关闭）时抛
        SerialException，把同一次调用里已累积的字节一并丢掉（对端发完即关会丢尾部）。
        这里一次只 recv 一段，立即返回；EOF 时才抛异常，此前的数据已在上一轮交付。
        """
        try:
            ready, _, _ = select.select([sock], [], [], timeout)
            if not ready:
                return b""
            chunk = sock.recv(4096)
        except (BlockingIOError, InterruptedError):
            return b""
        except OSError as exc:
            raise serial.SerialException(f"read failed: {exc}") from exc
        if not chunk:
            raise serial.SerialException("socket disconnected")
        return chunk

    def _emit_complete_lines(self, buf: bytearray) -> None:
        while True:
            idx = _find_eol(buf)
            if idx is None:
                break
            end, drop = idx
            raw = bytes(buf[:end])
            del buf[:drop]
            self._emit(raw)

    def _emit(self, raw: bytes) -> None:
        """增量解码后入环（kind=rx）。

        ``raw`` 只是本次产出边界的字节；不完整的多字节序列留在解码器里等下一段，
        因此不会产生 U+FFFD。若本次解出为空文本而 ``raw`` 非空，说明整段都是暂存的
        半个字符：不产出空行，留到下一个产出边界拼成完整字符。
        """
        text = self._decoder.decode(raw)
        if raw and not text:
            return
        self._hub.ingest(self._pc.name, text, kind="rx")
        with self._counters_lock:
            self._rx_lines += 1

    # Thread.run 覆盖为读循环体（内部线程以 target=self._run_loop 启动）。
    def run(self) -> None:  # pragma: no cover - 仅当以 Thread 方式启动时使用
        self._run_loop()
