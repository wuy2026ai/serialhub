# serialhub 架构与接口契约（v1，冻结）

> 本文件是**并行开发唯一事实来源**。所有模块必须严格按此处签名实现，不得改名/改返回结构。
> 语言 Python 3.10+（本机 3.12）。运行时依赖仅：`pyserial, fastapi, uvicorn, pyyaml, websockets`（+ 可选 `mcp`）。
> 禁止引入上述之外的新依赖（标准库随意）。类型注解齐全；Windows/Linux 均可运行。

---

## 1. 进程模型

```
┌─────────────────────────────── serialhub serve（常驻守护，单进程） ───────────────────────────────┐
│  Hub（serialhub/hub.py）                                                                           │
│   ├── SerialPortWorker × N  （每口一个线程：持久打开/读行/写/断线重连）                             │
│   ├── RingBuffer × N        （每口环形缓冲，含 seq/ts_ms）                                          │
│   ├── 全局单调序号 seq（线程安全）                                                                  │
│   ├── 订阅广播（WebSocket + wait_for 唤醒）                                                        │
│   └── TcpBridge × M         （tcp_bridge.py，把某口暴露为 TCP / 或作客户端透传）                    │
│  server.py（FastAPI + uvicorn，同进程）→ REST /api/* + WebSocket /ws + 静态 Web UI(/static)        │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
              ▲ HTTP/WS（Bearer token 可选）
              │
      serialhub <cmd>（cli.py，轻量客户端）──► 打印单行 JSON envelope {ok, data|error}
              ▲
              │ 同 serialhub/client.py
      serialhub mcp --stdio（mcp_server.py）──► 暴露 MCP tools（结构化 JSON），供 agent 调用
```

- **MCP 是瘦客户端**：它（与 CLI）通过 HTTP 调用常驻守护；守护必须先 `serve`。这与本仓 ecosystem 既有做法一致（MCP 包一层客户端协议，保持单一事实来源）。
- CLI 与 MCP 共用 `serialhub/client.py`（stdlib `urllib.request`，无额外依赖）。

---

## 2. 文件归属（并行不重叠）

| 文件 | 归属 | 说明 |
|---|---|---|
| `serialhub/__init__.py` | A | 版本、导出关键符号 |
| `serialhub/__main__.py` | A | `from .cli import main; main()`（若 cli 未就绪则延迟导入） |
| `serialhub/models.py` | A | `Line`、ANSI 工具、EOL 工具、`now_ms()` |
| `serialhub/ringbuffer.py` | A | `RingBuffer` |
| `serialhub/config.py` | A | `Config`/`PortConfig`/...、加载/校验/热重载合并 |
| `serialhub/serialport.py` | A | `SerialPortWorker` |
| `serialhub/hub.py` | A | `Hub`（核心） |
| `serialhub/exporter.py` | A | 导出 raw/noansi/json |
| `serialhub/server.py` | B | `create_app(hub, config)`（FastAPI）+ mount static |
| `serialhub/static/index.html` `.js` `.css` | B | Web UI（xterm.js） |
| `serialhub/tcp_bridge.py` | D | `TcpBridge` |
| `serialhub/client.py` | C | HTTP 客户端（stdlib） |
| `serialhub/cli.py` | C | 命令行（含 serve 入口） |
| `serialhub/mcp_compat.py` | C | 兼容 mcp 1.x(FastMCP)/2.x(MCPServer) |
| `serialhub/mcp_server.py` | C | MCP tools |
| `tools/mcp_smoke.py` | C | MCP stdio 冒烟客户端 |
| `tests/*` | E | 单测 |
| `README.md` / `docs/*` / `examples/*` | F | 文档 |

> B/C/D 实现时**按本契约编码**，不要依赖 A 的中间状态；A 完成后统一联调。

---

## 3. 数据模型（models.py，A 实现，所有人依赖）

```python
from dataclasses import dataclass, field
from typing import Optional
import re, time

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b[@-Z\\-_]")

def strip_ansi(text: str) -> str: ...      # 去除 ANSI 控制序列
def has_ansi(text: str) -> bool: ...
def now_ms() -> int: ...                   # int(time.time()*1000)

KIND_RX = "rx"; KIND_TX = "tx"; KIND_MARK = "mark"

@dataclass(frozen=True, slots=True)
class Line:
    seq: int          # 全局单调递增（跨所有口唯一）
    port: str         # 逻辑口名
    ts_ms: int        # 接收时刻（epoch 毫秒）
    text: str         # 一行文本，**不含行尾 EOL**；可能含 ANSI 转义
    ansi: bool = False# text 是否含 ANSI
    kind: str = "rx"  # rx=收到 | tx=本进程发出（回显） | mark=标记

    def to_dict(self, strip: bool = False) -> dict:
        """{'seq','port','ts_ms','text','ansi','kind'}；strip=True 时 text 去 ANSI。"""
```

EOL 工具：
```python
def resolve_eol(eol: Optional[str], default: str = "\r\n") -> bytes:
    """crlf|lf|cr|none|\r\n|\n|\r|Literal → bytes。'none'→b''。"""
```

---

## 4. RingBuffer（ringbuffer.py，A 实现）

```python
class RingBuffer:
    def __init__(self, max_lines: int = 20000, max_bytes: int = 8*1024*1024): ...

    def append(self, line: Line) -> None:        # 越界自动淘汰最老（行数/字节双阈值）
    def clear(self) -> None:
    def stats(self) -> dict:
        # {'count','bytes','first_seq','last_seq','dropped_total'}
    def snapshot(self, tail: Optional[int] = None) -> list[Line]:
        # tail=None→全部；否则最后 tail 行（新→旧排列? 一律旧→新升序）
    def oldest_seq(self) -> Optional[int]: ...
    def newest_seq(self) -> Optional[int]: ...
    def read(self, *, since_seq=None, since_ts=None, regex=None, limit=None,
             strip_ansi: bool=False, kinds: Optional[set[str]]=None) -> dict:
        # 见 §4.1 语义
    def search(self, regex: str, *, context: int=0, since_seq=None, limit=None) -> dict:
        # 见 §4.2
```

### 4.1 `read` 语义（**无重复消费**关键）
- 游标：优先 `since_seq`（返回 `seq > since_seq`）；否则 `since_ts`（返回 `ts_ms > since_ts`）；都没有→返回末尾 `limit`（默认按调用方给的 limit，None→全部保留行）。
- `regex` 非空→仅返回匹配行；`kinds` 非空→仅返回这些 kind；`strip_ansi=True`→text 去 ANSI。
- `limit` 截断后第一个未返回的行即下次 `next_seq`。
- 返回：
```python
{
  "lines": [Line.to_dict(strip_ansi) ...],   # 旧→新
  "count": int,          # 本次返回行数
  "next_seq": int,       # = 最后返回行的 seq；无返回则 = 传入游标（或最新 seq）
  "dropped": int,        # 因缓冲淘汰而永久丢失的行数（游标早于最老保留行时 >0）
  "has_more": bool,      # 缓冲中仍有 seq>next_seq 且满足条件的行未返回
  "first_seq": int|None, # 当前缓冲最老 seq
  "last_seq": int|None,  # 当前缓冲最新 seq
}
```
`dropped` 定义：若传入 `since_seq` 且 `since_seq < first_seq - 1`，则 `dropped = first_seq - 1 - since_seq`；否则 0。

### 4.2 `search` 语义
```python
{
  "hits": [
    {"seq","ts_ms","text","kind",
     "match": "<命中的子串>",
     "context_before": [{"seq","ts_ms","text"}...],   # context>0 时
     "context_after":  [{"seq","ts_ms","text"}...]}
  ],
  "count": int, "next_seq": int, "regex": str
}
```
`since_seq` 仅搜 `seq>since_seq`；`limit` 限制命中数。

---

## 5. Config（config.py，A 实现）

```python
@dataclass
class ServerConfig:
    host: str = "127.0.0.1"; port: int = 8765
    token: Optional[str] = None; cors: bool = True

@dataclass
class PortConfig:
    name: str; device: str
    baud: int = 115200
    data_bits: int = 8            # 5..8
    parity: str = "N"             # N|E|O|M|S
    stop_bits: float = 1          # 1|1.5|2
    flow: str = "none"            # none|rtscts|xonxoff|dsrdtr
    dtr: bool = True; rts: bool = True
    eol: str = "\r\n"
    encoding: str = "utf-8"
    reconnect_ms: int = 1000; reconnect_max_ms: int = 10000
    flush_ms: int = 50                  # 时间片断帧阈值（仅 framing=="idle" 且 >0 时生效）
    framing: str = "line"               # line|idle：分行策略（默认只按 \n/\r/\r\n）
    pins: Optional[dict] = None         # DTR/RTS 引脚控制；None/省略 ⇒ enabled=False（见 §5.1 语义）
    buffer_lines: int = 20000; buffer_bytes: int = 8*1024*1024
    autostart: bool = True
    tcp_bridge: Optional[dict] = None   # {mode:'server'|'client', port:int, host:str|None}

@dataclass
class LoggingConfig:
    enabled: bool = False; dir: str = "logs"; max_mb: int = 16; backups: int = 3

@dataclass
class UIConfig:
    theme: str = "dark"; timestamp: bool = True; max_render_lines: int = 5000

@dataclass
class Config:
    server: ServerConfig; ports: list[PortConfig]
    logging: LoggingConfig; ui: UIConfig
    path: Optional[str] = None           # 来源文件（热重载用）
    defaults: dict = field(default_factory=dict)

    @staticmethod
    def load(path: str) -> "Config":
        """按扩展名 .yaml/.yml→yaml, 否则 json；顶层 defaults 逐项合并进每个 port；校验后返回。"""
    def to_dict(self, *, redact: bool = True) -> dict:
        """redact=True 时把 server.token 置为 None 或以 '***' 替代。"""
    def port(self, name: str) -> PortConfig:      # 未知 name → KeyError
    def port_names(self) -> list[str]: ...
    def merge_overrides(self, **kw) -> "Config":  # 命令行覆盖 host/port/...
```

校验（`load` 内）：name 唯一非空；device 非空；`data_bits∈{5,6,7,8}`；`parity∈{N,E,O,M,S}`（大小写归一）；`stop_bits∈{1,1.5,2}`；`flow∈{none,rtscts,xonxoff,dsrdtr}`；`framing∈{line,idle}`；`flush_ms>=0`；`buffer_lines>0`；`buffer_bytes>0`。非法→`ValueError`（消息含字段路径，如 `ports[console].framing`）。

**`pins`（引脚控制，默认关闭）**：`PortConfig.pins: Optional[dict]`，归一化形状（`config.normalize_pins`）
`{"enabled": bool(默认 False), "dtr": bool|None(默认 None), "rts": bool|None(默认 None), "pulse_ms": int(默认 100)}`；
`None`/省略 ⇒ `enabled=False`。只允许这四个键，未知键 → `ValueError`（`ports[<name>].pins.<key> is not a valid pins key`）；
`enabled`/`dtr`/`rts` 非布尔、`pulse_ms` 非整数或为负 → `ValueError`（含字段路径）。
`defaults.pins` 可被端口级 `pins` **整体替换**（`merged = dict(defaults); merged.update(entry)`，非深合并）。
`config.to_dict()["ports"][i]["pins"]` 为归一化形状；`hub._IO_FIELDS` 含 `pins`（改 pins 会重启该口）。
`enabled=False`（默认）时 worker **任何时刻都不调用 `setDTR`/`setRTS`**（含 open 时）——这是相对旧版
“开端口即按顶层 `dtr`/`rts` 置位”的有意行为变化；`enabled=True` 时 open 置初始电平
（`pins.dtr` 优先，`None` 回退顶层 `dtr`；`rts` 同理）。

---

## 6. Hub（hub.py，A 实现）—— 全体后端依赖

```python
class Hub:
    def __init__(self, config: Config) -> None: ...

    # 生命周期
    def start(self) -> None:                      # 打开 autostart 口 + 起 config 里声明的 tcp_bridge
    def stop(self) -> None:                       # 关所有口+桥；幂等
    def reload(self, new_config: Config) -> dict: # 热重载（见 §6.3）
    def switch_config(self, path: str) -> dict:    # 运行时切换配置文件（见 §6.3）

    # 端口
    def add_port(self, pc: PortConfig, *, autostart: bool = True) -> None
    def remove_port(self, name: str) -> None
    def open(self, name: str, *, baud: int|None = None, device: str|None = None) -> dict
    def close(self, name: str) -> dict
    def is_open(self, name: str) -> bool

    # 查询
    def list_ports(self) -> dict
    def status(self, name: str|None = None) -> dict
    def list_devices(self) -> dict                # 系统串口枚举（GET /api/devices）
    def set_device(self, name, *, device=None, match=None, reopen=True) -> dict   # 运行时改口（临时，不落盘）

    # 数据
    def read(self, name, *, since_seq=None, since_ts=None, regex=None,
             limit=None, strip_ansi=False, kinds=None) -> dict
    def search(self, name, regex, *, context=0, since_seq=None, limit=None) -> dict
    def wait_for(self, name, regex, *, timeout_ms=10000, since_seq=None) -> dict
    def snapshot(self, name, *, tail_lines=200) -> dict
    def export(self, name, *, path=None, fmt="raw", rng=None) -> dict
    def clear(self, name) -> dict
    def mark(self, name, label) -> dict
    def send(self, name, data, *, eol=None, wait_regex=None, timeout_ms=None) -> dict

    # 供 SerialPortWorker / TcpBridge 使用
    def next_seq(self) -> int:                    # 线程安全自增
    def ingest(self, port, text, *, kind="rx", ts_ms=None) -> Line   # 建行→入环→广播
    def write_bytes(self, name, data: bytes) -> int   # 原始字节写串口（不追加 EOL）
    def subscribe(self) -> "queue.Queue[dict]": ...   # 广播队列（事件见下）
    def unsubscribe(self, q) -> None: ...

    # 引脚（DTR/RTS；仅 pins.enabled=true 时可用）
    def pins_status(self, name: str) -> dict
        # {"port","enabled":True,"dtr":bool|None,"rts":bool|None,"pulse_ms":int,
        #  "initial":{"dtr":bool,"rts":bool}}；未启用 → ValueError（含 "pins disabled"）；未知口 → KeyError
    def set_pin(self, name, action, pin=None, value=None, duration_ms=None) -> dict
        # action∈{set,pulse,reset}；set 需 pin+value(bool)，pulse 需 pin，reset 忽略 pin
        # 返回 {"port","action","pin","value","dtr","rts","duration_ms"}
        # 未启用/参数非法 → ValueError；端口未打开 → RuntimeError；未知口 → KeyError

    # 桥
    def bridge_start(self, name, *, mode="server", tcp_port=None, host=None) -> dict
    def bridge_stop(self, name) -> dict
    def bridge_status(self, name=None) -> dict

    @property
    def config(self) -> Config: ...
```

### 6.1 返回结构（REST/MCP 直接透传其 `data`）
**`list_ports()`**
```python
{"system":[{"device","description","hwid"}...],       # serial.tools.list_ports
 "ports":[{"name","device","description","open":bool,"baud","data_bits","parity","stop_bits",
           "flow","framing","flush_ms","pins":{"enabled","dtr","rts","pulse_ms"},
           "rx_lines","tx_lines","last_error","reconnect_count",
           "buffer":{"count","bytes","first_seq","last_seq","dropped_total"},
           "tcp_bridge":{"mode","port","host","active"}|None}...]}
```
`pins` 是**配置视图**（`config.pins_view(pc)`，归一化形状），不是当前电平；当前 DTR/RTS 电平要读
`hub.pins_status(name)`（REST `GET /api/ports/{name}/pins`），端口关闭时其 `dtr`/`rts` 为 `null`。
`framing`/`flush_ms` 为该口生效的断帧配置。
**`status(name=None)`**：`name` 给定→单个 port dict（同 ports[] 元素）加 `"server"` 字段；否则
```python
{"server":{"host","port","version","uptime_ms"},"ports":[<port dict>...]}
```
**`read(...)`**：同 §4.1，另加 `"port": name`。
**`search(...)`**：同 §4.2，另加 `"port": name`。
**`wait_for(...)`**
```python
{"port":name,"matched":bool,"line":{Line dict}|None,"seq":int|None,"ts_ms":int|None,"from_seq":int}
```
（`from_seq` = 本次等待起点游标）
**`send(...)`**
```python
{"port":name,"sent_bytes":int,"sent":str,"waited":bool,"matched":bool,
 "line":{Line dict}|None,"seq":int|None}
```
**`snapshot(...)`**：`{"port":name,"tail_lines":n,"lines":[...],"count":n}`
**`export(...)`**：`{"port":name,"path":str,"format":"raw|noansi|json","bytes":int,"count":int}`
**`mark(...)`**：`{"port":name,"label":str,"seq":int,"ts_ms":int}`
**`clear(...)`**：`{"port":name,"cleared":int}`
**`bridge_start/stop/status`**：`{"port":name,"mode","tcp_port","host","active":bool}`

### 6.2 广播事件（subscribe 队列元素）
- `{"type":"line","port":name,"line":{Line dict}}`（每次 ingest 都发；含 rx/tx/mark）
- `{"type":"status","port":name,"open":bool,...}`（开/关/重连变化时）
- `{"type":"mark","port":name,"label":...,"seq":...}`（可选，line 已足够）

### 6.3 `reload(new_config)`
- 更新 `self._config`；对每个口：若缓冲区上限变化→用新上限重建 RingBuffer 并**保留现有行**；若设备/参数变化且已打开→重启该 worker；新增口→add_port；移除的口→close+remove；桥按需重建。
- 返回 `{"added":[...],"removed":[...],"restarted":[...],"updated":[...]}`。
- `switch_config(path)`：`Config.load` 校验通过后调用上面的 reload，并把 `self._config.path` 记为后续 reload 的来源；路径先 `expanduser + abspath`（相对**守护进程 cwd**），文件不存在/校验失败 → `ValueError`，且因"先加载后应用"当前配置不变；切换只在内存（不落盘，重启回到 `serve --config`）。返回 reload 摘要 + `config`（生效的绝对路径）。

---

## 7. SerialPortWorker（serialport.py，A 实现，仅 Hub 使用）

```python
class SerialPortWorker(threading.Thread):
    def __init__(self, pc: PortConfig, hub: Hub) -> None: ...
    @property
    def is_open(self) -> bool: ...
    @property
    def last_error(self) -> Optional[str]: ...
    @property
    def reconnect_count(self) -> int: ...
    @property
    def rx_lines(self) -> int: ...
    @property
    def tx_lines(self) -> int: ...
    def open(self) -> None:       # 立即尝试打开（失败置 last_error，不抛）
    def close(self) -> None:      # 停止线程并释放端口
    def write(self, data: bytes) -> int:   # 线程安全；未开→抛 RuntimeError
    def status(self) -> dict: ...
    # 引脚（DTR/RTS）
    @property
    def pin_state(self) -> dict: ...       # {"dtr": bool|None, "rts": bool|None}（关闭后为 None）
    @property
    def pins_config(self) -> dict: ...     # {"enabled","dtr","rts","pulse_ms"}，dtr/rts 为 open 时生效的初始电平
    def set_pin(self, pin: str, value: bool) -> dict
    def pulse_pin(self, pin: str, duration_ms: Optional[int] = None) -> dict
    def reset_pins(self, duration_ms: Optional[int] = None) -> dict
```
要点：
- 用 `serial.serial_for_url(pc.device, baudrate=pc.baud, bytesize=pc.data_bits, parity=pc.parity,
  stopbits=pc.stop_bits, rtscts=pc.flow=="rtscts", xonxoff=pc.flow=="xonxoff",
  dsrdtr=pc.flow=="dsrdtr", timeout=0.1, write_timeout=1)`；随后**仅当 `pc.pins.enabled` 为真**时按初始电平 `setDTR`/`setRTS`（`pins.dtr/rts` 优先，`None` 回退顶层 `dtr`/`rts`）；单脚失败只记 `last_error`，不关端口。
- 引脚 API：`pin∈{"dtr","rts"}`，其他 → `ValueError`；引脚控制未启用 → `ValueError`（消息含 `pins disabled` 与端口名）；端口未打开 → `RuntimeError`。
  `pulse` = 把该脚翻到相反电平、保持 `duration_ms`（默认 `pins.pulse_ms`）再恢复；`reset` = 经典 CH340/ESP 自动复位序列
  （`rts=True,dtr=False` → `rts=False,dtr=True` → 恢复初始电平），每段间隔 `duration_ms`。睡眠在 `_state_lock` 之外，
  序列途中端口关闭则安静退出；`_close_serial()` 后 `pin_state` 两脚均为 `None`。
- 读循环：`read(4096)`；累积字节→按 `\n`/`\r` 断行（`\r\n` 视为一个）；**断帧由 `pc.framing` 决定**：
  `"line"`（默认）只按 EOL 分行，无结束符的残行一直保留，直到该口断开/被 `close` 时才作为一行吐出（吐出前去掉结尾单个 `\r`）；
  `"idle"` 才额外按 `flush_ms` 时间片把残行切出，且仅当 `flush_ms > 0` 时生效（`flush_ms=0` 不再等于“每轮都吐”）。
- 每个产出边界→`hub.ingest(name, text, kind="rx")`；解码用**每 worker 一个增量解码器**
  （`codecs.getincrementaldecoder(pc.encoding)(errors="replace")`，(重)连接时 `reset()`），
  跨读/跨分片的多字节字符（中文等）留到下一段拼成完整字符，绝不产生 U+FFFD。
- 断线：捕获串口异常→关闭→`hub` 广播 status→按 `reconnect_ms` 指数退避至 `reconnect_max_ms` 重试；**旧缓冲不丢**。
- `close()` 唤醒读循环并 join（带超时）。

---

## 8. Exporter（exporter.py，A 实现）

```python
def export_text(lines: list[Line], fmt: str = "raw") -> str:
    """raw: 原样 text+\\n；noansi: 去 ANSI text+\\n；json: 每行一个 JSON 对象(JSONL)。"""
def export_to_file(lines, path, fmt="raw") -> int:   # 返回写入字节数
```

---

## 9. REST / WebSocket 契约（server.py，B 实现）

- 基址 `http://{server.host}:{server.port}`；若 `server.token` 非空→`/api/*` 与 `/ws` 需
  `Authorization: Bearer <token>`（WS 亦可用 `?token=`）。失败→HTTP 401 `{"ok":false,"error":"unauthorized"}`。
- 所有 JSON 响应：成功 `{"ok":true, ...}`，失败 `{"ok":false,"error":"..."}`（4xx/5xx）。
- `create_app(hub: Hub, config: Config) -> fastapi.FastAPI` 供 `cli serve` 调用。
- 端点（`{}` 中为路径参数）：
  | 方法 | 路径 | 参数/体 | 返回 |
  |---|---|---|---|
  | GET | `/health` | — | `{"ok":true,"status":"ok","version","uptime_ms"}` |
  | GET | `/api/ports` | — | `{"ok":true,**hub.list_ports()}` |
  | GET | `/api/status` | `name?` | `{"ok":true,**hub.status(name)}` |
  | POST | `/api/ports/{name}/open` | `{baud?,device?}` | `{"ok":true,**hub.open(...)}` |
  | POST | `/api/ports/{name}/close` | — | `{"ok":true,**hub.close(...)}` |
  | GET | `/api/devices` | — | `{"ok":true,**hub.list_devices()}` |
  | POST | `/api/ports/{name}/device` | `{device?,match?,reopen?}` | `{"ok":true,**hub.set_device(...)}`（运行时改口，临时） |
  | GET | `/api/ports/{name}/pins` | — | `{"ok":true,**hub.pins_status(...)}`（引脚未启用 → 400） |
  | POST | `/api/ports/{name}/pins` | `{action?,pin?,value?,duration_ms?}` | `{"ok":true,**hub.set_pin(...)}` |
  | GET | `/api/ports/{name}/read` | `since_seq?,since_ts?,filter?,limit?,strip_ansi?,kinds?` | `{"ok":true,**hub.read(...)}` |
  | GET | `/api/ports/{name}/search` | `regex,context?,since_seq?,limit?` | `{"ok":true,**hub.search(...)}` |
  | GET | `/api/ports/{name}/wait` | `regex,timeout_ms?,since_seq?` | `{"ok":true,**hub.wait_for(...)}` |
  | POST | `/api/ports/{name}/send` | `{data,eol?,wait_regex?,timeout_ms?}` | `{"ok":true,**hub.send(...)}` |
  | GET | `/api/ports/{name}/snapshot` | `tail_lines?` | `{"ok":true,**hub.snapshot(...)}` |
  | POST | `/api/ports/{name}/export` | `{path?,format?,range?}` | `{"ok":true,**hub.export(...)}` |
  | POST | `/api/ports/{name}/clear` | — | `{"ok":true,**hub.clear(...)}` |
  | POST | `/api/ports/{name}/mark` | `{label}` | `{"ok":true,**hub.mark(...)}` |
  | POST | `/api/bridge` | `{action,port,mode?,tcp_port?,host?}` | `{"ok":true,**bridge_*}` |
  | POST | `/api/reload` | `{config?,path?}` | 无路径：`{"ok":true,**hub.reload(Config.load(当前生效的 path))}`；给出路径：`{"ok":true,**hub.switch_config(path)}`（含 `config`） |
  | GET | `/api/config` | — | `{"ok":true,"config":当前生效配置.to_dict(redact=True)}`（跟随运行时切换，即 `hub.config`） |
  | WS  | `/ws` | — | 服务端推送 `hub.subscribe()` 事件原样（`type`∈line/status/mark/hello） |
- 参数命名：`filter`（read 的正则）与 `regex`（search/wait/send 的正则）不同名，勿混。
- 错误映射：`ValueError`→400、`KeyError`→404、`RuntimeError`→409、请求体非法→422。
  引脚端点：引脚未启用或参数非法 → 400（消息含 `pins disabled`）；`POST`（`set`/`pulse`/`reset`）在端口未打开时 → 409；
  `GET /api/ports/{name}/pins` 只读，引脚已启用时端口关闭也返回 200（`dtr`/`rts` 为 `null`，并给出配置的 `initial` 电平）。
- 布尔参数接受 `true/false/1/0`。`kinds` 逗号分隔。
- 静态 UI：`GET /` 返回 `serialhub/static/index.html`；`/static/*` 挂载静态目录。UI 通过 `/ws` 与 `/api/*` 工作。

**Web UI（B）必须满足**：深色为主可切浅色；xterm.js（CDN 或本地，需能离线降级）；多口标签页；ANSI 彩色渲染；每行时间戳开关 + 序号；实时过滤（regex）+ 搜索高亮/跳转；暂停/继续、自动滚屏、清屏；导出 raw/noansi/json；发送栏（选口/输入/结尾选项/历史）；端口头手动开/关按钮（`POST /api/ports/{n}/open|close`，请求中禁用）；引脚控件（DTR/RTS/Reset 脉冲，仅该口 `pins.enabled` 为真时显示）；顶部统计（行速率/字节速率/缓冲占用）；连接状态与波特率显示；顶栏配置文件切换（chip 显示当前配置文件，输入路径切换 / ↻ 重读当前文件；切换后收敛页签、面板、发送栏，并对已移除端口的迟到 WS 事件忽略）。

---

## 10. CLI 契约（cli.py，C 实现）

- 入口：`main(argv=None) -> int`。`serialhub serve` 运行守护（不打印 JSON）；其余子命令打印**单行 JSON**：
  `{"ok":true,"data":<...>}` 或 `{"ok":false,"error":"<msg>"}`；退出码 0=ok，1=失败。
- 全局参数（`--url` 默认 `http://127.0.0.1:8765`；`--token` 或环境变量 `SERIALHUB_TOKEN`）。
- 子命令 → REST 映射：
  | CLI | REST |
  |---|---|
  | `serve --config F [--host][--port][--no-autostart][--log-level]` | 启 uvicorn + Hub |
  | `list` | GET /api/ports |
  | `discover` | GET /api/devices |
  | `status [NAME]` | GET /api/status |
  | `open NAME [--baud][--device]` | POST /api/ports/{n}/open |
  | `set-device NAME [--device COMx][--match SPEC][--no-reopen]` | POST /api/ports/{n}/device（临时改口） |
  | `close NAME` | POST /api/ports/{n}/close |
  | `pins NAME [--set PIN=on\|off]...[--pulse dtr\|rts][--reset][--duration-ms N]` | GET/POST /api/ports/{n}/pins |
  | `read NAME [--since-seq][--since-ts][--filter][--limit][--strip-ansi][--kinds]` | GET .../read |
  | `search NAME REGEX [--context][--since-seq][--limit]` | GET .../search |
  | `wait NAME REGEX [--timeout-ms][--since-seq]` | GET .../wait |
  | `send NAME DATA [--eol][--wait-regex][--timeout-ms]` | POST .../send |
  | `snapshot NAME [--tail-lines]` | GET .../snapshot |
  | `export NAME [--path][--format][--range]` | POST .../export |
  | `mark NAME LABEL` | POST .../mark |
  | `clear NAME` | POST .../clear |
  | `bridge {start,stop,status} --port NAME [--mode][--tcp-port][--host]` | POST /api/bridge |
  | `reload [--config F]` | POST /api/reload |
  | `ports` | `list` 别名 |
  | `mcp [--stdio]` | 运行 MCP（见 §11） |
- `send` 的 `DATA` 为 `-` 时读 stdin（去尾随换行）。`--eol` 接受 `crlf|lf|cr|none`；默认取该口 `Config` 的 `eol`（客户端不知道时用 `\r\n`）。
- `--kinds` 逗号分隔（rx,tx,mark）。
- `pins` 无动作参数 → 读状态（`GET`）；`--set PIN=on|off` 可重复（接受 `on/off/true/false/1/0/yes/no`）；
  `--pulse` 与 `--reset` 互斥（`{"ok":false,...}` + exit 1）；同一调用里的多个动作按顺序执行，
  返回最后一个动作的结果并附 `actions[]`（`data.actions`）。引脚未启用/端口未开由 REST 的 400/409 转成错误信封。

---

## 11. MCP 契约（mcp_server.py + client.py，C 实现）

- 传输：stdio（默认）。`serialhub mcp --stdio` 启动。
- 兼容垫片 `mcp_compat.py`：
  ```python
  try:
      from mcp.server.fastmcp import FastMCP as MCPServer      # mcp 1.x
  except Exception:
      from mcp.server.mcpserver import MCPServer                # mcp 2.x
  from mcp.types import ToolAnnotations
  ```
- `client.py`（stdlib `urllib.request`）：
  ```python
  class Client:
      def __init__(self, base_url: str, token: str|None = None, timeout: float = 30.0): ...
      def call(self, method: str, path: str, *, params: dict|None=None,
               body: dict|None=None, timeout: float|None=None) -> dict:
          """返回 REST 的 JSON（已含 ok）。连接失败→{'ok':False,'error':'...'}。
          按需把 params 转 query（None 值省略；bool→'true'/'false'）。"""
  ```
- 工具（共 **18** 个，名字即 MCP 工具名；返回 `{"ok":true,...}` / `{"ok":false,"error"}`）：
  | 工具 | 参数 | 返回关键字段 |
  |---|---|---|
  | `list_ports` | — | `system[]`,`ports[]` |
  | `open` | `port, baud=115200, device=None` | `port` dict |
  | `close` | `port` | `port` dict |
  | `status` | `port=None` | `server`,`ports[]` 或单口 |
  | `read` | `port, since_seq=None, since_ts=None, filter=None, limit=None, strip_ansi=False` | `lines[{seq,ts_ms,text}]`,`next_seq`,`count`,`dropped`,`has_more` |
  | `search` | `port, regex, context=0, since_seq=None, limit=None` | `hits[]`,`count`,`next_seq` |
  | `wait_for` | `port, regex, timeout_ms=10000, since_seq=None` | `matched`,`line`,`seq`,`ts_ms`,`from_seq` |
  | `send` | `port, data, eol="\r\n", wait_regex=None, timeout_ms=None` | `sent_bytes`,`waited`,`matched`,`line`,`seq` |
  | `snapshot` | `port, tail_lines=200` | `lines[]`,`count` |
  | `export` | `port, path=None, format="raw", range=None` | `path`,`bytes`,`format`,`count` |
  | `tcp_bridge` | `action, port, tcp_port=None, mode="server", host=None` | `active`,`mode`,`tcp_port` |
  | `mark` | `port, label` | `seq`,`ts_ms` |
  | `clear` | `port` | `cleared` |
  | `reload` | `config?` | `added/removed/restarted/updated`（切换时含 `config`） |
  | `set_device` | `port, device=None, match=None, reopen=True` | `port` dict（改后的 `device`/`open`；临时不落盘） |
  | `discover` | — | `devices[]`（`{index,device,description,hwid,vid,pid}`） |
  | `set_pin` | `port, action="set", pin=None, value=None, duration_ms=None` | `action`,`pin`,`value`,`dtr`,`rts`,`duration_ms`（需 `pins.enabled=true` 且端口已打开） |
  | `switch_config` | `config` | `added/removed/restarted/updated`,`config`（运行时切换配置文件；临时、不落盘） |
- `wait_for` 用长超时（`timeout_ms/1000 + 10`s）。工具在 worker 线程调用 `Client`（`asyncio.to_thread`），事件循环不阻塞。
- `tools/mcp_smoke.py`：自写 stdio JSON-RPC 客户端，`initialize`+`tools/list`（默认）；`--full` 再跑 `list_ports`。

---

## 12. TcpBridge（tcp_bridge.py，D 实现）

```python
class TcpBridge:
    def __init__(self, hub: "Hub", port: str, *, mode: str = "server",
                 tcp_port: int | None = None, host: str | None = None,
                 bind: str = "127.0.0.1") -> None: ...
    def start(self) -> dict:   # {"port","mode","tcp_port","host","active":True}
    def stop(self) -> dict:    # active:False
    def status(self) -> dict:  # 含 active/clients 计数
```
- **server**：`bind:tcp_port` 监听（默认 bind=127.0.0.1，可由 Hub 参数放宽到 0.0.0.0）；多客户端。
  客户端写入的字节→`hub.write_bytes(port, data)`，并 `hub.ingest(port, text, kind="tx")` 供观测；
  `hub.subscribe()` 收到该口 `type=="line"` 事件→把 `line.text`+该口 `eol` 编码后写回客户端。
- **client**：连接 `host:tcp_port`；远端→串口（write_bytes+ingest tx），串口→远端（同订阅）。
- 线程实现（threading + socket），`stop()` 需唤醒并 join 所有线程（带超时），幂等。
- 缺 `tcp_port`（server）或 `host`（client）→ raise ValueError。

---

## 13. 统一约定
- 所有时间：epoch 毫秒（`now_ms()`）。所有 seq：全局 int。
- 所有对外 JSON 一律 `{"ok": bool, ...}`；错误必须带 `"error": "<可读消息>"`。
- 禁止吞异常（空 `except`）；错误转成 `ok:false` 或记录到 `last_error`。
- 线程安全：Hub 的 seq/缓冲/队列用锁；worker 不直接改缓冲，一律经 `hub.ingest`。
- 类型注解齐全；`from __future__ import annotations`；文件头 docstring 说明职责。
- `__init__.py` 暴露 `__version__ = "0.1.0"`。

---

## 14. ZMODEM raw 会话（zmodem.py + serialport.py + hub.py，2026-11 增补）

> 新增章节，不改动上面冻结的 §1–§13。

**配置**（每口，`PortConfig`）：`zmodem: bool = True`（关掉后起始头按普通文本处理）、
`zmodem_claim_ms: int = 5000`、`zmodem_idle_ms: int = 15000`（均须 > 0），随 `to_dict()` 往返。

**检测**（`serialhub/zmodem.py`）：
- 起始头正则 `rb"\*\*\x18B0([01])"`：`0`（ZRQINIT，设备跑 `sz`）→ `"download"`，`1`（ZRINIT，设备跑 `rz`）→ `"upload"`。
- `ZmodemDetector.feed(data) -> (text_bytes, Detection|None)`：命中时 `text` = 头之前的字节，
  `Detection.initial` = 头及其后同批字节。未命中时只留住结尾恰为 `**\x18B0` 前缀的字节（≤5 字节），
  `flush()` 释放；worker 在空读（0.1 s 读超时）时调用 `flush()`，故普通文本最多迟一轮读，`line`/`idle` 两种断帧都适用。
- `ABORT_SEQ = b"\x18"*8 + b"\x08"*8`。

**会话**（worker 驱动，每口至多一个）：
- 命中 → 头前残行立即作为一行吐出（lrzsz `sz` 先打印 `rz\r`）→ 新建 `ZmodemSession`（`id` = uuid4 hex 前 8 位），
  `initial` 进 `pending` → `hub.mark(port, "[zmodem] start <direction>")` → 广播
  `{"type":"zmodem","event":"start","port","session","direction"}`。
- 会话中读到的**全部**字节只进会话：未认领进 `pending`，认领后进会话的**无界** `queue.Queue`（`None` = 结束哨兵）。
  不断行、不解码、不经 `Hub.ingest`/环形缓冲/`Hub._broadcast`；检测器停用。
- 会话中普通 `worker.write`（因此 `hub.send`/`hub.write_bytes`/TCP 桥写入）→ `RuntimeError("zmodem: session active on port '<name>'")`。
- 每轮读循环检查：未认领超过 `zmodem_claim_ms` → `unclaimed`；双向都无字节超过 `zmodem_idle_ms` → `timeout`；二者都先写 `ABORT_SEQ`。
- `close()`/`Hub.stop()` → `closed`（尽力写 `ABORT_SEQ` 后关口）；串口读错误断线 → `closed`（不写）。
- 结束：队列放 `None` → `hub.mark(port, "[zmodem] end reason=<r> rx=<n> tx=<n>")` → 广播
  `{"type":"zmodem","event":"end","port","session","reason","rx_bytes","tx_bytes"}`，
  `reason ∈ done|abort|timeout|unclaimed|closed|disconnect`（`END_REASONS`）；`done` 立即回到行模式。
- 非 `done` 结束 → **排空**：读到的字节一律丢弃（检测器停用），直到连续 `DRAIN_SILENCE_MS = 300` 无数据
  或总计 `DRAIN_MAX_MS = 3000`（`zmodem.py` 模块常量），再以清空的行缓冲/重置的解码器回到行模式——
  设备处理 CAN 之前还在发的负载、OS/USB 缓冲里的残余都不会进环。
- 会话中读循环只读 `in_waiting` 字节（无数据时在 `_io_lock` 外等 5 ms），不在锁下阻塞 0.1 s，owner 的写不被饿死。
- TCP 桥：`write_bytes` 失败（如会话进行中）时不再 `ingest` tx 行。

**Hub API**（未知端口一律 `KeyError`）：
```python
def zmodem_status(self, name) -> dict  # {"port","active","session","direction","owner","rx_bytes","tx_bytes"}
def zmodem_claim(self, name, client_id: str) -> tuple[str, queue.Queue]
    # 先把 pending 回放进队列；同一 client 重复认领返回同一会话；
    # RuntimeError("zmodem: no active session") / RuntimeError("zmodem: claimed by another client")
def zmodem_write(self, name, session_id, client_id, data: bytes) -> int
    # 仅 owner；原始写，tx_bytes += n，刷新活动时间；会话不符 → 上述两种 RuntimeError
def zmodem_end(self, name, session_id, client_id: Optional[str], *, abort=False, reason="done",
               tail: bytes = b"") -> dict
    # 幂等；返回 {"port","session","reason","rx_bytes","tx_bytes"}；abort 先写 ABORT_SEQ。
    # reason="done" 且非 abort：tail → 队列中未送出的字节 → 实时字节 依次回到行模式（见下）；否则忽略 tail。
    # reason ∉ END_REASONS → ValueError。
    # client_id 非 None 且会话被他人认领 → RuntimeError；已结束的会话返回同一摘要，未知 id 返回 reason=None。
```

**锁**：worker 新增 `_zm_lock`（叶子锁）守护会话指针与会话字段；持有时不做串口 I/O、不调 hub、不阻塞
（只对无界队列 `put_nowait`，保证数据、认领回放与结束哨兵的先后顺序）。超时判定 / owner 校验与摘除会话
（`_zm_session = None`）在同一临界区内完成，期间落地的认领或别的客户端的结束不会误杀新会话。
`_write_raw` 只在 `_io_lock` 下 `write`，`flush()`（tcdrain）在锁外：tcdrain 要等对端读走数据，持锁等会让读循环
读不了，正在发送的设备（`lsz`）写满缓冲后不再读输入，tcdrain 永不返回（死锁）。

**WebSocket 转发**（`server.py` 的 `/ws`）：每条连接有稳定的 `client_id`（uuid4 hex），会话按端口记在本连接上。
客户端 → 服务端（JSON 文本帧）：
- `{"type":"zmodem_claim","port"}` → 回 `{"type":"zmodem_claimed","port","session","direction"}`，随后**只向本连接**推
  `{"type":"zmodem_data","port","session","b64"}`（会话队列的字节，已到的分片合并，≤ 64 KiB/帧），
  会话结束（队列 `None`）时推 `{"type":"zmodem_closed","port","session","reason"}`（`reason` 同 end 广播）。
  同一连接重复认领同一会话只回 `zmodem_claimed`，不另起转发。
- `{"type":"zmodem_write","port","session","b64"}` → `hub.zmodem_write`（线程里执行，按收到顺序逐条写）。
- `{"type":"zmodem_end","port","session","abort":bool,"tail_b64"?}` → `hub.zmodem_end(..., abort, reason="abort" if abort else "done", tail=…)`。
  `tail_b64`（可选，仅非 abort 时生效）= 客户端收到、但不属于 ZMODEM 会话的字节：zmodem.js 会话结束后
  Sentry 经 `to_terminal` 交出的内容（lsz 的 `OO` 之后设备打印的 `sz done`、提示符等）。
- 任一失败（无会话、被他人认领、未知端口、base64 非法……）→ `{"type":"zmodem_error","port","error"}`，连接不断。
- 连接断开时 → 对本连接认领过的**每个**会话调用 `hub.zmodem_end(..., abort=True, reason="disconnect")`（已结束的是幂等空操作）。
  认领一返回就记账（在发 `zmodem_claimed` 之前），转发任务因发送失败退出也不会抹掉记录，故中止不会被漏掉。
- `abort` 只有字面量 `true` 才算中止（字符串 `"false"` 等按 `done` 处理）。
- `{"type":"zmodem","event":"start"|"end",...}` 广播照常经订阅推给所有连接（受 `subscribe` 过滤）。
- 会话期间 REST `POST /api/ports/{name}/send` → 409 `{"ok":false,"error":"zmodem: session active on port '<name>'"}`。

**`done` 结束后的文本回注**：会话只在客户端发 `zmodem_end` 时结束，设备在 `OO` 之后打印的内容可能已经进了会话。
worker 在摘除会话的同一 `_zm_lock` 临界区里把 `tail` + 队列中尚未送出的字节（会话已认领时还有 `pending`；
从未认领的会话的 `pending` 是没人看过的协议帧，直接丢弃）放进回注区，
取走的队列项不会再被转发（随后的 `None` 让转发任务停止，不重复投递），并置“暂扣”；end 标记写入并广播后才放行。
暂扣期间读到的实时字节追加在回注区之后。回注区只由读循环线程取走：与新读到的字节拼接（回注在前）后照常过检测器、
增量解码与断行，作为 rx 行——因此顺序恒为 end 标记 → tail → 队列残余 → 实时字节。重连时回注区清空。
队列残余原样当文本回注：客户端若在协议中途就发 `done`，没取走的 ZMODEM 帧也会进环——只有确认会话已结束才发 `done`，否则发 `abort`。
非 `done` 结束忽略 `tail`，其后的设备输出照旧落在排空窗口里被丢弃（如中止后的 `sz done rc=…`）。

**zmodem.js（0.1.10）与 lrzsz 的兼容垫片**：`lsz` 每个文件的第一个 ZDATA 子包以 ZCRCW 结尾并等 ZACK，
随后发新的 ZDATA 头；zmodem.js 只对 ZCRCQ 回 ZACK、ZDATA 帧后只接受 ZEOF，下载会在 1024 字节处卡住。
补丁在 `serialhub/static/vendor/zmodem-shim.js`（serialhub 自写、无依赖，`applyLrzszShim(Zmodem)`：回 ZACK 并接受下一个
ZDATA 头），唯一一份：浏览器以普通 `<script>` 加载（在 `zmodem.min.js` 之后，立即给 `window.Zmodem` 打补丁），
zclient.mjs 以 CommonJS 导入同一文件。补丁还处理 lsz 在 ZFIN 之后不发 `OO` 就退出的情况（0 字节文件时常见；
zmodem.js 原样会对设备的下一行抛 “Only thing after ZFIN should be OO”）：ZFIN 之后的非 `OO` 输入直接结束会话并作为
会话外字节交回（进 `tail_b64`），重复的 ZFIN 头照常应答，完全没有输入则 1 s 后结束——与 lrz 一样把 `OO` 视为可选。
回归测试 `tests/integration/test_zmodem_shim.py`（`tests/zmodem_client/shim_check.mjs` 重放抓到的 lsz 字节）。浏览器 UI 还须像 zclient 一样：zmodem.js 报 `session_end` 后继续喂 Sentry
约 200 ms，收集 `to_terminal` 字节，随 `zmodem_end` 的 `tail_b64` 交回。

**测试客户端**：`tests/zmodem_client/zclient.mjs`（Node，依赖锁在 `package-lock.json`，`node_modules/` 不入库；
`tests/integration/simenv.py` 的 `node_client()` 每个测试会话最多跑一次 `npm ci --prefer-offline`，失败/超时（120 s）则 skip 并记住）：
`node zclient.mjs --ws URL --port NAME (--send FILE… | --recv-dir DIR) [--close-after-bytes N] [--timeout-ms MS]`，
订阅就绪后往 stderr 打 `{"ready":true}`，结束时 stdout 打一行 `{"ok","files":[{"name","size"}],"error"}`，退出码 0/1。
