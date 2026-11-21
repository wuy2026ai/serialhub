"""serialhub HTTP / WebSocket server (server.py — 归属 B).

职责：把常驻 :class:`~serialhub.hub.Hub` 暴露为
    * REST ``/api/*``（契约 §9）
    * WebSocket ``/ws``（转发 ``hub.subscribe()`` 事件原样）
    * 静态 Web UI（``GET /`` + ``/static/*``）
并提供一个 ``create_app(hub, config)`` 工厂，交给 ``cli serve`` 用 uvicorn 运行。

设计约束：
    * **不**绑定监听地址、**不**调用 ``uvicorn.run``（那是 CLI 的职责）。
    * ``Hub`` / ``Config`` 仅用于类型注解（``TYPE_CHECKING``），运行时按 duck typing
      访问，因此即便 ``hub.py`` / ``config.py`` 尚未就绪，本模块也能被安全导入。
    * 所有 JSON 响应统一 ``{"ok": true, ...}`` / ``{"ok": false, "error": "..."}``。
    * ``POST /api/shutdown`` 要退出进程时调用 ``app.state.request_exit()``——由运行
      uvicorn 的一方（``cli serve`` / 测试）注入（通常是 ``server.should_exit = True``）；
      未注入时该端点返回 503，不会误杀宿主进程。
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import queue
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Set
from urllib.parse import urlsplit

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .i18n import resolve_locale, set_locale, t
from .zmodem import TAIL_MAX_BYTES

if TYPE_CHECKING:  # 仅供类型检查；运行时不需要 hub.py / config.py 存在
    from .config import Config
    from .hub import Hub

try:  # __init__.py 可能尚未落地，导入失败时兜底
    from . import __version__ as VERSION
except Exception:  # noqa: BLE001 - 包初始化期间允许缺失
    VERSION = "0.1.0"

STATIC_DIR = Path(__file__).resolve().parent / "static"

#: ``POST /api/shutdown`` 先回响应、再等这么久才真正停机（让响应与 WS 广播送达）。
SHUTDOWN_DELAY_S = 0.2


class I18nMiddleware:
    """Pure-ASGI middleware: pick the request locale from ``Accept-Language``.

    It runs in the same task as the wrapped app (no ``BaseHTTPMiddleware`` task
    group), so the locale set here is visible to endpoints and to the
    ``KeyError``/``ValueError``/``RuntimeError`` exception handlers, and it is
    reset afterwards — requests without the header keep the default locale.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        headers = scope.get("headers") or []
        accept = next(
            (value.decode("latin-1") for key, value in headers if key == b"accept-language"),
            None,
        )
        set_locale(resolve_locale(accept_language=accept))
        try:
            await self.app(scope, receive, send)
        finally:
            set_locale(None)


# --------------------------------------------------------------------------- #
# 请求体模型
# --------------------------------------------------------------------------- #
class OpenBody(BaseModel):
    baud: Optional[int] = None
    device: Optional[str] = None


class SetDeviceBody(BaseModel):
    device: Optional[str] = None
    match: Optional[dict] = None
    reopen: bool = True


class PinBody(BaseModel):
    """``POST /api/ports/{name}/pins`` 请求体（action=set/pulse/reset）。"""

    action: str = "set"
    pin: Optional[str] = None
    value: Optional[bool] = None
    duration_ms: Optional[int] = None


class SendBody(BaseModel):
    data: str
    eol: Optional[str] = None
    wait_regex: Optional[str] = None
    timeout_ms: Optional[int] = None


class ExchangeBody(BaseModel):
    """``POST /api/ports/{name}/exchange`` 请求体（§16：写出并收集回复，一次请求）。"""

    data: str
    eol: Optional[str] = None
    wait_regex: Optional[str] = None
    timeout_ms: Optional[int] = None
    quiet_ms: Optional[int] = None
    since_seq: Optional[int] = None


class ExportBody(BaseModel):
    path: Optional[str] = None
    format: str = "raw"
    range: Optional[str] = None  # noqa: A003 - 与 REST 契约字段名保持一致


class MarkBody(BaseModel):
    label: str


class BridgeBody(BaseModel):
    action: str
    port: str
    mode: Optional[str] = None
    tcp_port: Optional[int] = None
    host: Optional[str] = None


class ReloadBody(BaseModel):
    """``POST /api/reload`` 请求体：给出 ``config``（或别名 ``path``）时切换配置文件。"""

    config: Optional[str] = None
    path: Optional[str] = None  # 别名，便于直接 curl 时少想一个字段名


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #
def uptime_ms(app: FastAPI) -> int:
    """自 ``app.state.started_at`` 起始的毫秒数（helper，供 /health 等使用）。"""
    started = getattr(app.state, "started_at", None)
    if started is None:
        return 0
    return int((time.time() - started) * 1000)


def _parse_kinds(kinds: Optional[str]) -> Optional[Set[str]]:
    """``"rx,tx,mark"`` → ``{"rx","tx","mark"}``；空 → None。"""
    if not kinds:
        return None
    items = {part.strip() for part in kinds.split(",") if part.strip()}
    return items or None


def _http_token(request: Request) -> Optional[str]:
    auth = request.headers.get("authorization", "")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.query_params.get("token")


def _host_port(netloc: str, scheme: str = "http") -> Optional[tuple[str, int]]:
    """``"Host:8765"`` → ``("host", 8765)``；省略端口时按 scheme 取默认端口；非法 → None。"""
    try:
        parts = urlsplit(f"{scheme}://{netloc}")
        host = (parts.hostname or "").lower()
        port = parts.port or (443 if scheme in ("https", "wss") else 80)
    except ValueError:
        return None
    return (host, port) if host else None


def _same_origin(request: Request) -> bool:
    """CSRF 防护：有 ``Origin`` 时其 host:port 必须等于 ``Host``；无 ``Origin``（非浏览器）放行。"""
    origin = request.headers.get("origin")
    if origin is None:
        return True
    try:
        parts = urlsplit(origin.strip())
    except ValueError:
        return False
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return False  # 含 "null"（沙箱 iframe / file:// 页面）
    want = _host_port(request.headers.get("host", ""), parts.scheme)
    return want is not None and _host_port(parts.netloc, parts.scheme) == want


def _ws_token(websocket: WebSocket) -> Optional[str]:
    auth = websocket.headers.get("authorization", "")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return websocket.query_params.get("token")


# --------------------------------------------------------------------------- #
# 工厂
# --------------------------------------------------------------------------- #
def create_app(hub: "Hub", config: "Config") -> FastAPI:
    """构建并返回 FastAPI 应用（不启动服务器）。"""
    app = FastAPI(title="serialhub", version=VERSION, docs_url=None, redoc_url=None)
    app.state.started_at = time.time()
    app.state.request_exit = None  # 由运行 uvicorn 的一方注入（见模块说明）
    app.state.shutting_down = False

    server_cfg = getattr(config, "server", None)
    token: Optional[str] = getattr(server_cfg, "token", None) or None

    # ---- CORS -------------------------------------------------------------- #
    if getattr(server_cfg, "cors", False):
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            allow_credentials=False,
        )

    # ---- 鉴权中间件（仅 /api/*，WS 在握手时单独校验） ----------------------- #
    if token:
        @app.middleware("http")
        async def _auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
            if request.url.path.startswith("/api/") and _http_token(request) != token:
                return JSONResponse(
                    status_code=401, content={"ok": False, "error": t("server.error.unauthorized")}
                )
            return await call_next(request)

    # ---- 统一异常 → {"ok": false, "error": ...} ----------------------------- #
    @app.exception_handler(KeyError)
    async def _on_key_error(_req: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"ok": False, "error": t("server.error.unknown_port", name=str(exc))})

    @app.exception_handler(ValueError)
    async def _on_value_error(_req: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})

    @app.exception_handler(RuntimeError)
    async def _on_runtime_error(_req: Request, exc: RuntimeError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"ok": False, "error": str(exc)})

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(_req: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"ok": False, "error": t("server.error.invalid_request")})

    @app.exception_handler(Exception)
    async def _on_unhandled(_req: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500, content={"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        )

    def _allow_shutdown() -> bool:
        active = getattr(hub, "config", None) or config  # 跟随运行时切换
        return bool(getattr(getattr(active, "server", None), "allow_shutdown", True))

    # ---- 基础 / 健康检查 ---------------------------------------------------- #
    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "status": "ok",
            "version": VERSION,
            "uptime_ms": uptime_ms(app),
            "allow_shutdown": _allow_shutdown(),
        }

    # ---- 端口列表 / 状态 ---------------------------------------------------- #
    # 约定（§16）：可能阻塞的端点（wait/send 的等待、open/close 的线程 join、引脚脉冲的睡眠、
    # 导出/重载的磁盘 I/O）写成普通 ``def``，由 FastAPI 放进线程池执行，绝不阻塞事件循环
    # （否则一个 wait 会让所有其它 REST/WS 客户端一起卡住）；其余纯内存端点保持 ``async def``。
    @app.get("/api/ports")
    async def api_ports() -> dict[str, Any]:
        return {"ok": True, **hub.list_ports()}

    @app.get("/api/status")
    async def api_status(name: Optional[str] = None) -> dict[str, Any]:
        return {"ok": True, **hub.status(name)}

    @app.post("/api/ports/{name}/open")
    def api_open(name: str, body: Optional[OpenBody] = None) -> dict[str, Any]:
        b = body or OpenBody()
        return {"ok": True, **hub.open(name, baud=b.baud, device=b.device)}

    @app.post("/api/ports/{name}/close")
    def api_close(name: str) -> dict[str, Any]:
        return {"ok": True, **hub.close(name)}

    # ---- 运行时改设备（临时，不落盘） -------------------------------------- #
    @app.get("/api/devices")
    async def api_devices() -> dict[str, Any]:
        return {"ok": True, **hub.list_devices()}

    @app.post("/api/ports/{name}/device")
    def api_set_device(
        name: str, body: Optional[SetDeviceBody] = None
    ) -> dict[str, Any]:
        b = body or SetDeviceBody()
        return {
            "ok": True,
            **hub.set_device(name, device=b.device, match=b.match, reopen=b.reopen),
        }

    # ---- 引脚：DTR/RTS 状态与操作（需 pins.enabled=true） ------------------- #
    @app.get("/api/ports/{name}/pins")
    async def api_pins(name: str) -> dict[str, Any]:
        return {"ok": True, **hub.pins_status(name)}

    @app.post("/api/ports/{name}/pins")
    def api_set_pin(name: str, body: Optional[PinBody] = None) -> dict[str, Any]:
        b = body or PinBody()
        return {
            "ok": True,
            **hub.set_pin(
                name, b.action, pin=b.pin, value=b.value, duration_ms=b.duration_ms
            ),
        }

    # ---- 数据：read / search / wait ---------------------------------------- #
    @app.get("/api/ports/{name}/read")
    async def api_read(
        name: str,
        since_seq: Optional[int] = None,
        since_ts: Optional[int] = None,
        filter_: Optional[str] = Query(None, alias="filter"),
        limit: Optional[int] = None,
        strip_ansi: bool = False,
        kinds: Optional[str] = None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            **hub.read(
                name,
                since_seq=since_seq,
                since_ts=since_ts,
                regex=filter_,
                limit=limit,
                strip_ansi=strip_ansi,
                kinds=_parse_kinds(kinds),
            ),
        }

    @app.get("/api/ports/{name}/search")
    async def api_search(
        name: str,
        regex: str,
        context: int = 0,
        since_seq: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            **hub.search(name, regex, context=context, since_seq=since_seq, limit=limit),
        }

    @app.get("/api/ports/{name}/wait")
    def api_wait(
        name: str,
        regex: str,
        timeout_ms: int = 10000,
        since_seq: Optional[int] = None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            **hub.wait_for(name, regex, timeout_ms=timeout_ms, since_seq=since_seq),
        }

    # ---- 数据：send / snapshot / export / clear / mark --------------------- #
    @app.post("/api/ports/{name}/send")
    def api_send(name: str, body: SendBody) -> dict[str, Any]:
        return {
            "ok": True,
            **hub.send(
                name,
                body.data,
                eol=body.eol,
                wait_regex=body.wait_regex,
                timeout_ms=body.timeout_ms,
            ),
        }

    @app.post("/api/ports/{name}/exchange")
    def api_exchange(name: str, body: ExchangeBody) -> dict[str, Any]:
        return {
            "ok": True,
            **hub.exchange(
                name,
                body.data,
                eol=body.eol,
                wait_regex=body.wait_regex,
                timeout_ms=body.timeout_ms,
                quiet_ms=body.quiet_ms,
                since_seq=body.since_seq,
            ),
        }

    @app.get("/api/ports/{name}/snapshot")
    async def api_snapshot(name: str, tail_lines: int = 200) -> dict[str, Any]:
        return {"ok": True, **hub.snapshot(name, tail_lines=tail_lines)}

    @app.post("/api/ports/{name}/export")
    def api_export(name: str, body: Optional[ExportBody] = None) -> dict[str, Any]:
        b = body or ExportBody()
        return {"ok": True, **hub.export(name, path=b.path, fmt=b.format, rng=b.range)}

    @app.post("/api/ports/{name}/clear")
    async def api_clear(name: str) -> dict[str, Any]:
        return {"ok": True, **hub.clear(name)}

    @app.post("/api/ports/{name}/mark")
    async def api_mark(name: str, body: MarkBody) -> dict[str, Any]:
        return {"ok": True, **hub.mark(name, body.label)}

    # ---- 桥 / 热重载 / 配置 ------------------------------------------------ #
    @app.post("/api/bridge")
    async def api_bridge(body: BridgeBody) -> dict[str, Any]:
        action = (body.action or "").lower()
        if action == "start":
            return {
                "ok": True,
                **hub.bridge_start(
                    body.port,
                    mode=body.mode or "server",
                    tcp_port=body.tcp_port,
                    host=body.host,
                ),
            }
        if action == "stop":
            return {"ok": True, **hub.bridge_stop(body.port)}
        if action == "status":
            return {"ok": True, **hub.bridge_status(body.port)}
        raise ValueError(t("server.error.unknown_bridge_action", action=body.action))

    @app.post("/api/reload")
    def api_reload(body: Optional[ReloadBody] = None) -> dict[str, Any]:
        """热重载；``{"config": "<path>"}`` 时在运行时切换到该配置文件（仅内存，不落盘）。"""
        from .config import Config as RuntimeConfig  # 延迟导入：config.py 可能尚未落地

        b = body or ReloadBody()
        wanted = str(b.config or b.path or "").strip()
        if wanted:
            return {"ok": True, **hub.switch_config(wanted)}
        # 未指定路径：沿用"当前生效"的配置源（可能是之前切换过的那个文件）
        path = getattr(getattr(hub, "config", None), "path", None) or getattr(config, "path", None)
        if not path:
            raise ValueError(t("server.error.no_reload_path"))
        return {"ok": True, **hub.reload(RuntimeConfig.load(path))}

    @app.get("/api/config")
    async def api_config() -> dict[str, Any]:
        active = getattr(hub, "config", None) or config  # 跟随运行时切换
        return {"ok": True, "config": active.to_dict(redact=True)}

    # ---- 停止服务（Web UI 的 Exit 按钮 / CLI shutdown） --------------------- #
    async def _shutdown_sequence() -> None:
        """先让响应送出，再：广播 shutdown → 停桥/关口（中止 ZMODEM、吐残行）→ 停 uvicorn。"""
        await asyncio.sleep(SHUTDOWN_DELAY_S)
        try:
            hub._broadcast({"type": "shutdown"})
            await asyncio.sleep(0.1)  # 让各 WS 转发任务把广播发出去
            await asyncio.to_thread(hub.stop)
        except Exception:  # noqa: BLE001 - 无论如何都要退出
            pass
        finally:
            request_exit = getattr(app.state, "request_exit", None)
            if callable(request_exit):
                request_exit()

    @app.post("/api/shutdown")
    async def api_shutdown(request: Request) -> JSONResponse:
        if not _allow_shutdown():
            return JSONResponse(
                status_code=403, content={"ok": False, "error": t("server.error.shutdown_disabled")}
            )
        if not _same_origin(request):
            return JSONResponse(
                status_code=403, content={"ok": False, "error": t("server.error.bad_origin")}
            )
        if not callable(getattr(app.state, "request_exit", None)):
            return JSONResponse(
                status_code=503,
                content={"ok": False, "error": t("server.error.shutdown_unavailable")},
            )
        if not app.state.shutting_down:  # 重复请求只回同一响应，不再起第二个停机流程
            app.state.shutting_down = True
            app.state.shutdown_task = asyncio.create_task(_shutdown_sequence())
        return JSONResponse(status_code=200, content={"ok": True, "data": {"stopping": True}})

    # ---- WebSocket --------------------------------------------------------- #
    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket) -> None:
        if token and _ws_token(websocket) != token:
            await websocket.accept()
            await websocket.send_json({"ok": False, "error": t("server.error.unauthorized")})
            await websocket.close(code=1008)
            return

        await websocket.accept()

        # 1) 连接问候：推送当前端口概览
        try:
            listing = hub.list_ports()
            ports = listing.get("ports", []) if isinstance(listing, dict) else []
        except Exception:  # noqa: BLE001 - 问候失败不应中断连接
            ports = []
        await websocket.send_json({"type": "hello", "ports": ports})

        # 2) 订阅广播
        q = hub.subscribe()
        subscribed: Optional[Set[str]] = None  # None 表示全部端口
        # ZMODEM：本连接的稳定身份；port -> (session_id, 会话转发任务)；
        # zm_claimed 记下本连接认领过的每个会话（从不删除），断开时逐个幂等 end。
        client_id = uuid.uuid4().hex
        zm_sessions: dict[str, tuple[str, asyncio.Task]] = {}
        zm_claimed: list[tuple[str, str]] = []

        async def _pump() -> None:
            """后台任务：把阻塞队列的事件喂给 WebSocket。"""
            while True:
                try:
                    event = await asyncio.to_thread(q.get, True, 1.0)
                except queue.Empty:
                    continue
                except Exception:  # noqa: BLE001
                    break
                if subscribed is not None and isinstance(event, dict):
                    ev_port = event.get("port")
                    if ev_port is not None and ev_port not in subscribed:
                        continue
                try:
                    await websocket.send_json(event)
                except Exception:  # noqa: BLE001 - 客户端已断开
                    break

        async def _zm_error(port: Any, exc: BaseException, session: Any = None,
                            *, claim: bool = False) -> None:
            """``zmodem_error``：带稳定的 ``code``；客户端消息带了 ``session`` 时原样回显。"""
            if isinstance(exc, KeyError):
                error = t("server.error.unknown_port", name=str(exc))
            else:
                error = str(exc)
            if isinstance(exc, (KeyError, ValueError)):
                code = "bad_request"
            elif "no active session" in error:
                code = "no_session"
            elif "claimed by another client" in error:
                code = "claimed_by_other" if claim else "not_owner"
            else:
                code = "error"
            msg = {"type": "zmodem_error", "port": port, "code": code, "error": error}
            if session is not None:
                msg["session"] = session
            await websocket.send_json(msg)

        async def _zm_pump(port: str, sid: str, zq: "queue.Queue[Optional[bytes]]") -> None:
            """会话转发任务：负载只发给本连接（base64），遇 None 哨兵发 zmodem_closed。"""
            sentinel_seen = False
            try:
                ended = False
                while not ended:
                    try:
                        chunk = await asyncio.to_thread(zq.get, True, 1.0)
                    except queue.Empty:
                        continue
                    if chunk is None:
                        break
                    buf = bytearray(chunk)
                    while len(buf) < 65536:  # 合并已到的分片，减少帧数
                        try:
                            more = zq.get_nowait()
                        except queue.Empty:
                            break
                        if more is None:
                            ended = True
                            break
                        buf.extend(more)
                    await websocket.send_json({
                        "type": "zmodem_data", "port": port, "session": sid,
                        "b64": base64.b64encode(bytes(buf)).decode("ascii"),
                    })
                sentinel_seen = True
                # 会话已摘除：对旧 id 调 end 幂等，取回结束原因
                summary = hub.zmodem_end(port, sid, client_id)
                await websocket.send_json({
                    "type": "zmodem_closed", "port": port, "session": sid,
                    "reason": summary.get("reason") or "closed",
                })
            except Exception:  # noqa: BLE001 - 客户端已断开
                pass
            finally:
                # 只有真正看到结束哨兵才摘记录；发送失败（客户端已断）时留着，交给断开路径中止
                if sentinel_seen and zm_sessions.get(port, (None,))[0] == sid:
                    del zm_sessions[port]

        async def _zm_claim(port: Any) -> None:
            try:
                sid, zq = hub.zmodem_claim(port, client_id)
                st = hub.zmodem_status(port)
            except Exception as exc:  # noqa: BLE001 - 报错给客户端，连接不断
                await _zm_error(port, exc, claim=True)
                return
            # 先记账再 await：回复发送失败时断开路径也能中止这个已认领的会话
            if (port, sid) not in zm_claimed:
                zm_claimed.append((port, sid))
            if zm_sessions.get(port, (None,))[0] != sid:  # 重复认领不另起转发任务
                zm_sessions[port] = (sid, asyncio.create_task(_zm_pump(port, sid, zq)))
            direction = st.get("direction") if st.get("session") == sid else None
            await websocket.send_json({
                "type": "zmodem_claimed", "port": port, "session": sid, "direction": direction,
            })

        async def _zm_write(port: Any, sid: Any, b64: Any) -> None:
            try:
                data = base64.b64decode(str(b64 or ""), validate=True)
                # 阻塞写放线程里；本循环逐条 await，写入顺序与收到顺序一致
                await asyncio.to_thread(hub.zmodem_write, port, str(sid), client_id, data)
            except binascii.Error as exc:
                await _zm_error(port, ValueError(f"zmodem: bad base64: {exc}"), sid)
            except Exception as exc:  # noqa: BLE001 - 报错给客户端，连接不断
                await _zm_error(port, exc, sid)

        async def _zm_end(port: Any, sid: Any, abort: bool, tail_b64: Any) -> None:
            try:
                # tail：客户端收到的会话外字节（lsz 的 OO 之后设备打印的内容），只在非 abort 时回注
                tail = b"" if abort else base64.b64decode(str(tail_b64 or ""), validate=True)
                if len(tail) > TAIL_MAX_BYTES:  # 拒收整条 end，会话保留，客户端可重发
                    raise ValueError(
                        f"zmodem: tail too large ({len(tail)} > {TAIL_MAX_BYTES} bytes)"
                    )
                await asyncio.to_thread(
                    hub.zmodem_end, port, str(sid), client_id,
                    abort=abort, reason="abort" if abort else "done", tail=tail,
                )
            except binascii.Error as exc:
                await _zm_error(port, ValueError(f"zmodem: bad base64: {exc}"), sid)
            except Exception as exc:  # noqa: BLE001 - 报错给客户端，连接不断
                await _zm_error(port, exc, sid)

        def _zm_abort_all(owned: list[tuple[str, str]]) -> None:
            for port, sid in owned:
                try:
                    hub.zmodem_end(port, sid, client_id, abort=True, reason="disconnect")
                except Exception:  # noqa: BLE001 - best-effort
                    pass

        async def _handle_client_message(raw: str) -> None:
            nonlocal subscribed
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                return
            if not isinstance(msg, dict):
                return
            mtype = msg.get("type")
            if mtype == "ping":
                await websocket.send_json({"type": "pong"})
            elif mtype == "subscribe":
                names = msg.get("ports")
                if isinstance(names, list):
                    resolved = {str(n) for n in names}
                    subscribed = resolved or None
            elif mtype == "mark":
                port = msg.get("port")
                label = str(msg.get("label", ""))
                if port:
                    try:
                        hub.mark(port, label)
                    except Exception:  # noqa: BLE001 - best-effort
                        pass
            elif mtype == "zmodem_claim":
                await _zm_claim(msg.get("port"))
            elif mtype == "zmodem_write":
                await _zm_write(msg.get("port"), msg.get("session"), msg.get("b64"))
            elif mtype == "zmodem_end":
                await _zm_end(msg.get("port"), msg.get("session"), msg.get("abort") is True,
                              msg.get("tail_b64"))

        pump_task = asyncio.create_task(_pump())
        try:
            while True:
                raw = await websocket.receive_text()
                await _handle_client_message(raw)
        except WebSocketDisconnect:
            pass
        except Exception:  # noqa: BLE001
            pass
        finally:
            pump_task.cancel()
            # 断开时仍持有的会话一律中止（对已结束的会话调用是幂等的）。写中止串放线程里，
            # 不阻塞事件循环；即使本协程被取消，线程里的收尾也会跑完。
            owned = list(zm_claimed)  # 已结束的会话再 end 是幂等的空操作
            for _sid, task in zm_sessions.values():
                task.cancel()
            zm_sessions.clear()
            try:
                hub.unsubscribe(q)
            except Exception:  # noqa: BLE001
                pass
            if owned:
                await asyncio.to_thread(_zm_abort_all, owned)

    # ---- 静态 UI ----------------------------------------------------------- #
    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    async def index() -> Any:
        index_file = STATIC_DIR / "index.html"
        if index_file.is_file():
            return FileResponse(str(index_file))
        return JSONResponse(status_code=404, content={"ok": False, "error": t("server.error.ui_not_built")})

    # Outermost user middleware (added last ⇒ insert(0)): sets the per-request
    # locale before auth/exception handlers/endpoints run, so a request with an
    # ``Accept-Language`` header gets localized errors while header-less requests
    # (e.g. the existing test suite) stay on the default ``en``.
    app.add_middleware(I18nMiddleware)

    return app


__all__ = ["create_app", "uptime_ms"]
