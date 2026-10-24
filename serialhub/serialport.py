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

任何串口错误都记入 ``last_error`` 并广播 status 事件，绝不崩溃进程。

仅被 ``Hub`` 使用；不直接改缓冲（一律经 ``hub.ingest``）。
"""

# allow: SIZE_OK - 契约 §7 要求单文件内实现完整 SerialPortWorker 读写/断行/重连循环。

from __future__ import annotations

import codecs
import threading
from typing import TYPE_CHECKING, Optional

import serial

from . import discovery
from .config import DEFAULT_PULSE_MS, normalize_pins
from .i18n import t
from .models import now_ms

if TYPE_CHECKING:  # 仅类型标注，避免与 hub 形成运行期循环导入
    from .hub import Hub

__all__ = ["SerialPortWorker"]

_CR = 0x0D
_LF = 0x0A

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
        """停止读循环并释放端口（幂等）。"""
        with self._state_lock:
            self._want_open = False
        self._stop.set()
        thread = self._loop_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=3.0)
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

    def _make_decoder(self) -> "codecs.IncrementalDecoder":
        """按当前 ``encoding`` 建增量解码器；未知编码回退 utf-8（不让配置炸掉读循环）。"""
        try:
            factory = codecs.getincrementaldecoder(self._encoding)
        except LookupError:
            factory = codecs.getincrementaldecoder("utf-8")
        return factory(errors="replace")  # type: ignore[no-any-return]

    def write(self, data: bytes) -> int:
        """线程安全写串口（原始字节，不追加 EOL）；未打开 → RuntimeError。"""
        with self._state_lock:
            ser = self._serial
            if ser is None or not self._open:
                raise RuntimeError(f"port '{self._pc.name}' is not open")
        try:
            with self._io_lock:
                written = ser.write(data)
                try:
                    ser.flush()
                except Exception as exc:  # flush 失败记录但不致命
                    with self._state_lock:
                        self._last_error = f"flush: {type(exc).__name__}: {exc}"
        except Exception as exc:
            with self._state_lock:
                self._last_error = f"write: {type(exc).__name__}: {exc}"
            self._broadcast_status()
            raise
        count = int(written) if written is not None else len(data)
        with self._counters_lock:
            self._tx_lines += 1
        return count

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
                    self._flush_line_residual(buf)  # line 模式：断开前吐出残行
                    buf.clear()
                    residual_since = None
                    with self._state_lock:
                        self._reconnect_count += 1
                    self._stop.wait(backoff / 1000.0)
                    backoff = min(backoff * 2, backoff_max)
                    continue

                now = now_ms()
                if data:
                    if not buf:
                        residual_since = now
                    buf.extend(data)
                    self._emit_complete_lines(buf)
                    if not buf:
                        residual_since = None
                    elif residual_since is None:
                        residual_since = now

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
            # close()/stop() 退出循环：line 模式的残行作为最后一行吐出。
            self._flush_line_residual(buf)

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
        try:
            with self._io_lock:
                return ser.read(4096)  # type: ignore[no-any-return]
        except Exception as exc:
            with self._state_lock:
                self._last_error = f"read: {type(exc).__name__}: {exc}"
            self._close_serial()
            self._broadcast_status()
            return None

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
