"""配置模型与加载/校验（§5）。

职责：
- ``ServerConfig`` / ``PortConfig`` / ``LoggingConfig`` / ``UIConfig`` / ``Config`` 数据类。
- ``Config.load``：按扩展名选择 YAML/JSON，顶层 ``defaults`` 合并进每个 port，随后全量校验。
- ``to_dict(redact=True)``、``port``、``port_names``、``merge_overrides``。

校验失败一律抛 ``ValueError``，消息含字段路径（如 ``ports[console].data_bits``）。
"""

# allow: SIZE_OK - 契约 §5 要求全部配置数据类与加载/校验/序列化逻辑同处 config.py。

from __future__ import annotations

import copy
import json
import os
import re
from dataclasses import dataclass, field, fields, replace
from typing import Any, Optional

import yaml

from .discovery import MATCH_KEYS, parse_usb_id

__all__ = [
    "ServerConfig",
    "PortConfig",
    "LoggingConfig",
    "UIConfig",
    "Config",
    "normalize_pins",
    "pins_view",
]

_VALID_DATA_BITS = {5, 6, 7, 8}
_VALID_PARITY = {"N", "E", "O", "M", "S"}
_VALID_STOP_BITS = {1, 1.5, 2}
_VALID_FLOW = {"none", "rtscts", "xonxoff", "dsrdtr"}
_VALID_FRAMING = {"line", "idle"}

#: ``pins`` 配置允许出现的键（未知键 → ValueError）。
_VALID_PINS_KEYS = {"enabled", "dtr", "rts", "pulse_ms"}
#: ``pins.pulse_ms`` 缺省值（毫秒）。
DEFAULT_PULSE_MS = 100
#: ``pins`` 省略/None 时的归一化形状（enabled=False ⇒ 引脚控制完全关闭）。
_DEFAULT_PINS = {"enabled": False, "dtr": None, "rts": None, "pulse_ms": DEFAULT_PULSE_MS}

_PORT_FIELDS = {
    "name", "device", "baud", "data_bits", "parity", "stop_bits", "flow",
    "dtr", "rts", "eol", "encoding", "reconnect_ms", "reconnect_max_ms",
    "flush_ms", "framing", "pins", "buffer_lines", "buffer_bytes", "autostart",
    "tcp_bridge", "description", "match",
    "zmodem", "zmodem_claim_ms", "zmodem_idle_ms", "zmodem_cleanup_eol",
    "prompt", "reply_quiet_ms", "reply_first_ms",
}

#: ``reply_quiet_ms`` / ``reply_first_ms`` 缺省值（毫秒，§16 ``/exchange``）。
DEFAULT_REPLY_QUIET_MS = 100
DEFAULT_REPLY_FIRST_MS = 300

#: ``zmodem_cleanup_eol`` 的合法取值：非干净结束后，非浏览器客户端（zsend/zrecv）写给设备
#: 冲掉其输入行缓冲的行结束符（``ctrl-u-cr`` = 先 ^U 清行再 CR；``none`` = 不写）。
ZMODEM_CLEANUP_EOLS = {"cr": b"\r", "lf": b"\n", "crlf": b"\r\n", "ctrl-u-cr": b"\x15\r",
                       "none": b""}

#: match 规格里允许出现的键（与 discovery.MATCH_KEYS 同源）。
_VALID_MATCH_KEYS = set(MATCH_KEYS)


@dataclass
class ServerConfig:
    """HTTP/WebSocket 服务端配置。"""

    host: str = "127.0.0.1"
    port: int = 8765
    token: Optional[str] = None
    cors: bool = True
    allow_shutdown: bool = True   # 允许 POST /api/shutdown（Web UI 的 Exit 按钮）停止服务


@dataclass
class PortConfig:
    """单个串口配置。"""

    name: str
    device: str = ""           # 固定设备名；留空时由 match 自动解析（见 serialhub.discovery）
    description: str = ""      # 备注：自由文本，透传到 list_ports/status，便于辨识设备
    baud: int = 115200
    data_bits: int = 8
    parity: str = "N"
    stop_bits: float = 1
    flow: str = "none"
    dtr: bool = True
    rts: bool = True
    eol: str = "\r\n"
    encoding: str = "utf-8"
    reconnect_ms: int = 1000
    reconnect_max_ms: int = 10000
    flush_ms: int = 50
    # 断帧策略： "line"（默认）只按 \n/\r/\r\n 分行，残行留到断开/关闭时才吐出；
    # "idle" 额外按 flush_ms 时间片把无结束符的残行切出（仅 flush_ms > 0 时生效）。
    framing: str = "line"
    # 串口芯片引脚控制（DTR/RTS/reset）。缺省/None ⇒ enabled=False：worker 任何时刻
    # 都不调用 setDTR/setRTS（dtr/rts 顶层字段仅在启用引脚控制后才生效）。
    # 归一化形状：{"enabled": bool, "dtr": bool|None, "rts": bool|None, "pulse_ms": int}
    pins: Optional[dict] = None
    buffer_lines: int = 20000
    buffer_bytes: int = 8 * 1024 * 1024
    autostart: bool = True
    tcp_bridge: Optional[dict] = None
    match: Optional[dict] = None   # 自动发现规格（device 为空时使用）：VID/PID、描述子串等
    # ZMODEM（rz/sz）检测：命中起始头后该口切到 raw 模式，直到会话结束。
    zmodem: bool = True
    zmodem_claim_ms: int = 5000    # 会话开始后这么久无人认领 → 中止（unclaimed）
    zmodem_idle_ms: int = 15000    # 会话内双向都无字节这么久 → 中止（timeout）
    # zsend/zrecv 非干净结束后写给设备的行结束符（见 ZMODEM_CLEANUP_EOLS）
    zmodem_cleanup_eol: str = "cr"
    # 一问一答（§16 POST /exchange、lite send）：prompt 为设备提示符正则（缺省 None = 不按提示符结束），
    # 命中未结束的尾巴（pending）或最后一行 rx 即判定回复结束；否则最后一个字节后静默
    # reply_quiet_ms 结束；写出后一个字节都没收到时最多等 reply_first_ms。
    prompt: Optional[str] = None
    reply_quiet_ms: int = DEFAULT_REPLY_QUIET_MS
    reply_first_ms: int = DEFAULT_REPLY_FIRST_MS


@dataclass
class LoggingConfig:
    """滚动落盘配置。"""

    enabled: bool = False
    dir: str = "logs"
    max_mb: int = 16
    backups: int = 3


@dataclass
class UIConfig:
    """Web UI 默认项。"""

    theme: str = "dark"
    timestamp: bool = True
    max_render_lines: int = 5000


@dataclass
class Config:
    """完整配置。"""

    server: ServerConfig
    ports: list[PortConfig]
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    path: Optional[str] = None
    defaults: dict = field(default_factory=dict)

    # ---- 加载 ----
    @staticmethod
    def load(path: str) -> "Config":
        """按扩展名加载：``.yaml/.yml`` → YAML，否则 JSON。合并 defaults 后校验。"""
        if not path:
            raise ValueError("config path must be a non-empty string")
        with open(path, "r", encoding="utf-8") as fh:
            ext = os.path.splitext(path)[1].lower()
            if ext in (".yaml", ".yml"):
                raw = yaml.safe_load(fh)
            else:
                raw = json.load(fh)
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise ValueError("config root must be a mapping/object")
        return Config.from_dict(raw, path=os.path.abspath(path))

    @staticmethod
    def from_dict(raw: dict, *, path: Optional[str] = None) -> "Config":
        """从已解析的字典构建并校验配置。"""
        server_raw = raw.get("server") or {}
        if not isinstance(server_raw, dict):
            raise ValueError("server must be a mapping")
        server = _build(ServerConfig, server_raw, "server")
        if not isinstance(server.allow_shutdown, bool):
            raise ValueError(
                f"server.allow_shutdown must be a boolean, got {server.allow_shutdown!r}"
            )

        logging = _build(LoggingConfig, raw.get("logging") or {}, "logging")
        ui = _build(UIConfig, raw.get("ui") or {}, "ui")

        defaults_raw = raw.get("defaults") or {}
        if not isinstance(defaults_raw, dict):
            raise ValueError("defaults must be a mapping")
        buffer_defaults = raw.get("buffer_defaults") or {}

        ports_raw = raw.get("ports") or []
        if not isinstance(ports_raw, list):
            raise ValueError("ports must be a list")

        ports: list[PortConfig] = []
        seen: set[str] = set()
        for index, entry in enumerate(ports_raw):
            if not isinstance(entry, dict):
                raise ValueError(f"ports[{index}] must be a mapping")
            merged = dict(defaults_raw)
            merged.update(entry)
            if "buffer_lines" not in merged and isinstance(buffer_defaults, dict) and "lines" in buffer_defaults:
                merged["buffer_lines"] = buffer_defaults["lines"]
            if "buffer_bytes" not in merged and isinstance(buffer_defaults, dict) and "bytes" in buffer_defaults:
                merged["buffer_bytes"] = buffer_defaults["bytes"]
            kwargs = {k: v for k, v in merged.items() if k in _PORT_FIELDS}
            name = kwargs.get("name")
            path_label = f"ports[{name}]" if name else f"ports[{index}]"
            if not name or not isinstance(name, str):
                raise ValueError(f"{path_label}.name must be a non-empty string")
            device_raw = kwargs.get("device")
            if device_raw is not None and not isinstance(device_raw, str):
                raise ValueError(f"{path_label}.device must be a string")
            if not device_raw and not kwargs.get("match"):
                raise ValueError(
                    f"{path_label} must have a non-empty 'device' or a 'match' spec"
                )
            pc = _build(PortConfig, kwargs, path_label)
            _validate_port(pc, path_label)
            if pc.name in seen:
                raise ValueError(f"{path_label}.name duplicate: '{pc.name}'")
            seen.add(pc.name)
            ports.append(pc)

        return Config(
            server=server,
            ports=ports,
            logging=logging,
            ui=ui,
            path=path,
            defaults=dict(defaults_raw),
        )

    # ---- 查询 ----
    def port(self, name: str) -> PortConfig:
        """按名返回 PortConfig；未知 name → KeyError。"""
        for pc in self.ports:
            if pc.name == name:
                return pc
        raise KeyError(name)

    def port_names(self) -> list[str]:
        return [pc.name for pc in self.ports]

    # ---- 序列化 ----
    def to_dict(self, *, redact: bool = True) -> dict:
        """导出为 dict；redact=True 时把 server.token 置为 '***'（存在时）或 None。"""
        token = self.server.token
        if redact and token:
            token = "***"
        return {
            "server": {
                "host": self.server.host,
                "port": self.server.port,
                "token": token,
                "cors": self.server.cors,
                "allow_shutdown": self.server.allow_shutdown,
            },
            "defaults": copy.deepcopy(self.defaults),
            "ports": [_port_to_dict(pc) for pc in self.ports],
            "logging": {
                "enabled": self.logging.enabled,
                "dir": self.logging.dir,
                "max_mb": self.logging.max_mb,
                "backups": self.logging.backups,
            },
            "ui": {
                "theme": self.ui.theme,
                "timestamp": self.ui.timestamp,
                "max_render_lines": self.ui.max_render_lines,
            },
            "path": self.path,
        }

    # ---- 覆盖 ----
    def merge_overrides(self, **kw: Any) -> "Config":
        """返回应用了命令行覆盖（host/port/token/cors/autostart）的新 Config。"""
        server = replace(self.server)
        if kw.get("host") is not None:
            server.host = str(kw["host"])
        if kw.get("port") is not None:
            server.port = int(kw["port"])
        if kw.get("token") is not None:
            server.token = str(kw["token"])
        if kw.get("cors") is not None:
            server.cors = bool(kw["cors"])

        ports = [replace(pc) for pc in self.ports]
        for pc in ports:
            pc.pins = copy.deepcopy(pc.pins)  # 覆盖后不与原 Config 共享可变 dict
        if kw.get("autostart") is not None:
            flag = bool(kw["autostart"])
            for pc in ports:
                pc.autostart = flag

        return Config(
            server=server,
            ports=ports,
            logging=self.logging,
            ui=self.ui,
            path=self.path,
            defaults=copy.deepcopy(self.defaults),
        )


def _build(cls: type, data: dict, path_label: str) -> Any:
    """按给定字段构造 dataclass 实例，忽略未知键。"""
    known = {f.name for f in fields(cls)}
    kwargs = {k: v for k, v in data.items() if k in known}
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise ValueError(f"{path_label}: {exc}") from exc


def _as_int(value: Any, label: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer, got {value!r}") from exc


def _as_float(value: Any, label: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number, got {value!r}") from exc


def _validate_port(pc: PortConfig, path_label: str) -> None:
    """校验端口参数；非法 → ValueError（消息含字段路径）。"""
    pc.parity = str(pc.parity).upper()
    pc.flow = str(pc.flow).lower()
    pc.data_bits = _as_int(pc.data_bits, f"{path_label}.data_bits")
    pc.baud = _as_int(pc.baud, f"{path_label}.baud")
    pc.stop_bits = _as_float(pc.stop_bits, f"{path_label}.stop_bits")
    pc.buffer_lines = _as_int(pc.buffer_lines, f"{path_label}.buffer_lines")
    pc.buffer_bytes = _as_int(pc.buffer_bytes, f"{path_label}.buffer_bytes")
    pc.reconnect_ms = _as_int(pc.reconnect_ms, f"{path_label}.reconnect_ms")
    pc.reconnect_max_ms = _as_int(pc.reconnect_max_ms, f"{path_label}.reconnect_max_ms")
    pc.flush_ms = _as_int(pc.flush_ms, f"{path_label}.flush_ms")
    pc.framing = str(pc.framing).lower()
    pc.pins = normalize_pins(pc.pins, f"{path_label}.pins")
    pc.zmodem_claim_ms = _as_int(pc.zmodem_claim_ms, f"{path_label}.zmodem_claim_ms")
    pc.zmodem_idle_ms = _as_int(pc.zmodem_idle_ms, f"{path_label}.zmodem_idle_ms")
    pc.zmodem_cleanup_eol = str(pc.zmodem_cleanup_eol).lower()
    pc.reply_quiet_ms = _as_int(pc.reply_quiet_ms, f"{path_label}.reply_quiet_ms")
    pc.reply_first_ms = _as_int(pc.reply_first_ms, f"{path_label}.reply_first_ms")
    if pc.reply_quiet_ms < 0:
        raise ValueError(f"{path_label}.reply_quiet_ms must be >= 0, got {pc.reply_quiet_ms!r}")
    if pc.reply_first_ms < 0:
        raise ValueError(f"{path_label}.reply_first_ms must be >= 0, got {pc.reply_first_ms!r}")
    if pc.prompt is not None:
        if not isinstance(pc.prompt, str) or not pc.prompt:
            raise ValueError(f"{path_label}.prompt must be a non-empty regex string, got {pc.prompt!r}")
        try:
            re.compile(pc.prompt)
        except re.error as exc:
            raise ValueError(f"{path_label}.prompt is not a valid regex: {exc}") from exc

    if pc.data_bits not in _VALID_DATA_BITS:
        raise ValueError(f"{path_label}.data_bits must be one of {sorted(_VALID_DATA_BITS)}, got {pc.data_bits!r}")
    if pc.parity not in _VALID_PARITY:
        raise ValueError(f"{path_label}.parity must be one of {sorted(_VALID_PARITY)}, got {pc.parity!r}")
    if pc.stop_bits not in _VALID_STOP_BITS:
        raise ValueError(f"{path_label}.stop_bits must be one of {sorted(_VALID_STOP_BITS)}, got {pc.stop_bits!r}")
    if pc.flow not in _VALID_FLOW:
        raise ValueError(f"{path_label}.flow must be one of {sorted(_VALID_FLOW)}, got {pc.flow!r}")
    if pc.buffer_lines <= 0:
        raise ValueError(f"{path_label}.buffer_lines must be > 0, got {pc.buffer_lines!r}")
    if pc.buffer_bytes <= 0:
        raise ValueError(f"{path_label}.buffer_bytes must be > 0, got {pc.buffer_bytes!r}")
    if pc.reconnect_ms <= 0:
        raise ValueError(f"{path_label}.reconnect_ms must be > 0, got {pc.reconnect_ms!r}")
    if pc.reconnect_max_ms < pc.reconnect_ms:
        raise ValueError(f"{path_label}.reconnect_max_ms must be >= reconnect_ms, got {pc.reconnect_max_ms!r}")
    if pc.flush_ms < 0:
        raise ValueError(f"{path_label}.flush_ms must be >= 0, got {pc.flush_ms!r}")
    if pc.framing not in _VALID_FRAMING:
        raise ValueError(
            f"{path_label}.framing must be one of {sorted(_VALID_FRAMING)}, got {pc.framing!r}"
        )
    if not isinstance(pc.zmodem, bool):
        raise ValueError(f"{path_label}.zmodem must be a boolean, got {pc.zmodem!r}")
    if pc.zmodem_claim_ms <= 0:
        raise ValueError(f"{path_label}.zmodem_claim_ms must be > 0, got {pc.zmodem_claim_ms!r}")
    if pc.zmodem_idle_ms <= 0:
        raise ValueError(f"{path_label}.zmodem_idle_ms must be > 0, got {pc.zmodem_idle_ms!r}")
    if pc.zmodem_cleanup_eol not in ZMODEM_CLEANUP_EOLS:
        raise ValueError(f"{path_label}.zmodem_cleanup_eol must be one of "
                         f"{sorted(ZMODEM_CLEANUP_EOLS)}, got {pc.zmodem_cleanup_eol!r}")
    if not pc.name:
        raise ValueError(f"{path_label}.name must be non-empty")
    if not pc.device and not pc.match:
        raise ValueError(
            f"{path_label} must have a non-empty 'device' or a 'match' spec"
        )
    if pc.match is not None:
        _validate_match(pc.match, f"{path_label}.match")


def normalize_pins(raw: Any, path_label: str = "pins", *, strict: bool = True) -> dict:
    """归一化 ``pins`` 配置为冻结形状。

    返回 ``{"enabled": bool, "dtr": bool|None, "rts": bool|None, "pulse_ms": int}``；
    ``raw`` 为 ``None``/省略 ⇒ ``enabled=False`` 的默认形状。

    ``strict=True``（配置加载路径）：未知键、非法类型、负 ``pulse_ms`` → ``ValueError``，
    消息含字段路径（如 ``ports[a].pins.bogus is not a valid pins key``）。
    ``strict=False``（运行时/直接构造的 ``PortConfig``）：尽力归一化，绝不抛异常。
    """
    if raw is None:
        return dict(_DEFAULT_PINS)
    if not isinstance(raw, dict):
        if strict:
            raise ValueError(f"{path_label} must be a mapping, got {raw!r}")
        return dict(_DEFAULT_PINS)

    for key in raw:
        if key not in _VALID_PINS_KEYS:
            if strict:
                raise ValueError(f"{path_label}.{key} is not a valid pins key")
            return dict(_DEFAULT_PINS)

    out = dict(_DEFAULT_PINS)
    enabled = raw.get("enabled", False)
    if not isinstance(enabled, bool):
        if strict:
            raise ValueError(f"{path_label}.enabled must be a boolean, got {enabled!r}")
    else:
        out["enabled"] = enabled

    for key in ("dtr", "rts"):
        if key not in raw or raw[key] is None:
            continue
        value = raw[key]
        if not isinstance(value, bool):
            if strict:
                raise ValueError(
                    f"{path_label}.{key} must be a boolean or null, got {value!r}"
                )
            continue
        out[key] = value

    if raw.get("pulse_ms") is not None:
        if strict:
            pulse = _as_int(raw["pulse_ms"], f"{path_label}.pulse_ms")
            if pulse < 0:
                raise ValueError(f"{path_label}.pulse_ms must be >= 0, got {pulse!r}")
            out["pulse_ms"] = pulse
        else:
            try:
                pulse = int(raw["pulse_ms"])
            except (TypeError, ValueError):
                pulse = DEFAULT_PULSE_MS
            out["pulse_ms"] = pulse if pulse >= 0 else DEFAULT_PULSE_MS
    return out


def pins_view(pc: Any) -> dict:
    """``pc.pins`` 的容错只读视图（归一化形状，绝不抛异常）。

    供 ``hub._port_status()[``"pins"``]`` 使用：即便 ``PortConfig`` 是直接构造的、
    未经 :func:`normalize_pins` 校验，也总是给出冻结形状的 dict。
    """
    return normalize_pins(getattr(pc, "pins", None), strict=False)


def _validate_match(match: Any, path_label: str) -> None:
    """校验 match 规格；未知键 / 非法 vid,pid,index → ValueError（含字段路径）。"""
    if not isinstance(match, dict):
        raise ValueError(f"{path_label} must be a mapping")
    for key in match:
        if key not in _VALID_MATCH_KEYS:
            raise ValueError(
                f"{path_label}.{key} is not a valid match key "
                f"(allowed: {sorted(_VALID_MATCH_KEYS)})"
            )
    for key in ("description", "hwid", "device", "serial"):
        if key in match and not isinstance(match[key], str):
            raise ValueError(f"{path_label}.{key} must be a string")
    for key in ("vid", "pid"):
        if key in match:
            try:
                parse_usb_id(match[key])
            except ValueError as exc:
                raise ValueError(f"{path_label}.{key}: {exc}") from exc
    if "index" in match:
        idx = _as_int(match["index"], f"{path_label}.index")
        if idx < 0:
            raise ValueError(f"{path_label}.index must be >= 0, got {idx!r}")


def _port_to_dict(pc: PortConfig) -> dict:
    return {
        "name": pc.name,
        "device": pc.device,
        "description": pc.description,
        "match": copy.deepcopy(pc.match),
        "baud": pc.baud,
        "data_bits": pc.data_bits,
        "parity": pc.parity,
        "stop_bits": pc.stop_bits,
        "flow": pc.flow,
        "dtr": pc.dtr,
        "rts": pc.rts,
        "eol": pc.eol,
        "encoding": pc.encoding,
        "reconnect_ms": pc.reconnect_ms,
        "reconnect_max_ms": pc.reconnect_max_ms,
        "flush_ms": pc.flush_ms,
        "framing": pc.framing,
        "pins": copy.deepcopy(pc.pins),
        "buffer_lines": pc.buffer_lines,
        "buffer_bytes": pc.buffer_bytes,
        "autostart": pc.autostart,
        "tcp_bridge": copy.deepcopy(pc.tcp_bridge),
        "zmodem": pc.zmodem,
        "zmodem_claim_ms": pc.zmodem_claim_ms,
        "zmodem_idle_ms": pc.zmodem_idle_ms,
        "zmodem_cleanup_eol": pc.zmodem_cleanup_eol,
        "prompt": pc.prompt,
        "reply_quiet_ms": pc.reply_quiet_ms,
        "reply_first_ms": pc.reply_first_ms,
    }
