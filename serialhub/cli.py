"""serialhub command-line interface.

``serialhub <cmd>`` is a lightweight client for a running ``serialhub serve``
daemon: every non-``serve`` subcommand prints exactly one line of JSON —
``{"ok": true, "data": <result>}`` or ``{"ok": false, "error": "<message>"}`` —
and exits 0 on success / 1 on failure. ``serve`` runs the daemon in-process.
The lite-interface commands are deliberately different: ``tools`` prints a raw
JSON array of tool definitions and ``call`` prints plain text (``serialhub.lite``);
so do ``zsend`` / ``zrecv`` (ZMODEM file transfer, ``serialhub.zmodem_client``),
which print the same one-line-per-file text as the lite tools.

``hub``/``server``/``tcp_bridge`` (and uvicorn) are imported lazily inside the
``serve`` handler only, so ``serialhub list`` and the MCP server start with
minimal dependencies.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Optional

from .client import DEFAULT_URL, Client
from .i18n import t

DEFAULT_EOL = "\r\n"
_EOL_TOKENS = {"crlf", "lf", "cr", "none"}
_EOL_ESCAPES = {"\\r\\n": "\r\n", "\\n": "\n", "\\r": "\r", "\\t": "\t"}
#: ``--set PIN=on|off`` 的真/假取值（大小写不敏感）。
_PIN_TRUE = {"on", "true", "1", "yes"}
_PIN_FALSE = {"off", "false", "0", "no"}
_PIN_CHOICES = ("dtr", "rts")


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
def _resolve_eol(value: Optional[str]) -> str:
    """Normalize ``--eol``: named tokens pass through, escapes become literal."""
    if value is None:
        return DEFAULT_EOL
    if value in _EOL_TOKENS:
        return value
    return _EOL_ESCAPES.get(value, value)


def _read_send_data(data: str) -> str:
    """Resolve the ``DATA`` argument; ``-`` reads stdin minus one trailing newline."""
    if data != "-":
        return data
    text = sys.stdin.read()
    if text.endswith("\r\n"):
        return text[:-2]
    if text.endswith(("\n", "\r")):
        return text[:-1]
    return text


def _parse_match_arg(text: str) -> dict[str, Any]:
    """Parse ``--match``: a JSON object, or compact ``k=v`` pairs.

    Examples: ``'{"description": "CH340"}'`` or ``description=CH340,vid=0x1A86``.
    Raises ``ValueError`` on malformed input; the caller turns it into an error
    envelope.
    """
    text = text.strip()
    if text.startswith("{"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(t("cli.error.match_json", text=text)) from exc
        if not isinstance(parsed, dict):
            raise ValueError(t("cli.error.match_not_object", text=text))
        return parsed
    spec: dict[str, Any] = {}
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        key, sep, value = part.partition("=")
        if not sep or not key.strip():
            raise ValueError(t("cli.error.match_pairs", text=text))
        spec[key.strip()] = value.strip()
    if not spec:
        raise ValueError(t("cli.error.match_pairs", text=text))
    return spec


def _global_options(args: argparse.Namespace) -> tuple[str, Optional[str]]:
    url = getattr(args, "url", None) or DEFAULT_URL
    token = getattr(args, "token", None) or os.environ.get("SERIALHUB_TOKEN") or None
    return url, token


def _parse_pin_set(text: str) -> tuple[str, bool]:
    """解析 ``--set`` 参数：``dtr=on`` / ``rts=0`` → ``("dtr", True)``。

    接受 on/off/true/false/1/0/yes/no（大小写不敏感）；格式非法 → ``ValueError``。
    """
    key, sep, raw = str(text).partition("=")
    pin = key.strip().lower()
    token = raw.strip().lower()
    if not sep or pin not in _PIN_CHOICES or token not in (_PIN_TRUE | _PIN_FALSE):
        raise ValueError(t("cli.error.pins_set_format", text=text))
    return pin, token in _PIN_TRUE


def _pins_command(client: Any, args: argparse.Namespace) -> dict[str, Any]:
    """``pins`` 子命令：无动作 → GET 状态；否则按 --set/--pulse/--reset 依次调用。

    ``--pulse`` 与 ``--reset`` 互斥（错误信封）；任一步失败立即返回该错误信封。
    """
    sets: list[str] = list(getattr(args, "set_pins", None) or [])
    pulse: Optional[str] = getattr(args, "pulse", None)
    do_reset: bool = bool(getattr(args, "reset", False))
    name = args.name
    if pulse and do_reset:
        return {"ok": False, "error": t("cli.error.pins_pulse_reset")}
    if not sets and not pulse and not do_reset:
        return client.pins(name)

    actions: list[tuple[str, str, Optional[bool]]] = []
    for item in sets:
        try:
            pin, value = _parse_pin_set(item)
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        actions.append(("set", pin, value))
    if pulse:
        actions.append(("pulse", pulse, None))
    if do_reset:
        actions.append(("reset", None, None))  # type: ignore[arg-type]

    results: list[dict[str, Any]] = []
    for action, pin, value in actions:
        env = client.set_pin(name, action=action, pin=pin, value=value,
                             duration_ms=args.duration_ms)
        if not env.get("ok"):
            return env
        results.append(env)
    if len(results) == 1:
        return results[0]
    merged = dict(results[-1])
    merged["actions"] = results
    return merged


def _print_env(env: dict[str, Any]) -> int:
    """Emit the single-line JSON envelope and return the process exit code."""
    if env.get("ok"):
        data = {k: v for k, v in env.items() if k != "ok"}
        print(json.dumps({"ok": True, "data": data}, ensure_ascii=False))
        return 0
    print(json.dumps({"ok": False, "error": str(env.get("error") or "unknown error")},
                     ensure_ascii=False))
    return 1


# --------------------------------------------------------------------------- #
# argument parser
# --------------------------------------------------------------------------- #
def _build_parser() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--url", default=argparse.SUPPRESS,
                        help=t("cli.opt.url"))
    parent.add_argument("--token", default=argparse.SUPPRESS,
                        help=t("cli.opt.token"))

    parser = argparse.ArgumentParser(prog="serialhub", description=t("cli.description"),
                                     parents=[parent])
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    def add(name: str, **kw: Any) -> argparse.ArgumentParser:
        return sub.add_parser(name, parents=[parent], **kw)

    add("list", help=t("cli.cmd.list"))
    add("ports", help=t("cli.cmd.ports"))

    p = add("status", help=t("cli.cmd.status"))
    p.add_argument("name", nargs="?", default=None)

    p = add("open", help=t("cli.cmd.open"))
    p.add_argument("name")
    p.add_argument("--baud", type=int, default=None)
    p.add_argument("--device", default=None)

    p = add("close", help=t("cli.cmd.close"))
    p.add_argument("name")

    p = add("set-device", help=t("cli.cmd.set_device"))
    p.add_argument("name")
    p.add_argument("--device", default=None)
    p.add_argument("--match", default=None, help=t("cli.arg.match"))
    p.add_argument("--no-reopen", action="store_true", help=t("cli.arg.no_reopen"))

    add("discover", help=t("cli.cmd.discover"))

    p = add("pins", help=t("cli.cmd.pins"))
    p.add_argument("name")
    p.add_argument("--set", dest="set_pins", action="append", default=None,
                   metavar="PIN=ON|OFF", help=t("cli.arg.pins_set"))
    p.add_argument("--pulse", default=None, choices=list(_PIN_CHOICES),
                   help=t("cli.arg.pins_pulse"))
    p.add_argument("--reset", action="store_true", help=t("cli.arg.pins_reset"))
    p.add_argument("--duration-ms", type=int, default=None,
                   help=t("cli.arg.duration_ms"))

    p = add("read", help=t("cli.cmd.read"))
    p.add_argument("name")
    p.add_argument("--since-seq", type=int, default=None)
    p.add_argument("--since-ts", type=int, default=None)
    p.add_argument("--filter", default=None, help=t("cli.arg.filter"))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--strip-ansi", action="store_true")
    p.add_argument("--kinds", default=None, help=t("cli.arg.kinds"))

    p = add("search", help=t("cli.cmd.search"))
    p.add_argument("name")
    p.add_argument("regex")
    p.add_argument("--context", type=int, default=0)
    p.add_argument("--since-seq", type=int, default=None)
    p.add_argument("--limit", type=int, default=None)

    p = add("wait", help=t("cli.cmd.wait"))
    p.add_argument("name")
    p.add_argument("regex")
    p.add_argument("--timeout-ms", type=int, default=10000)
    p.add_argument("--since-seq", type=int, default=None)

    p = add("send", help=t("cli.cmd.send"))
    p.add_argument("name")
    p.add_argument("data", help=t("cli.arg.send_data"))
    p.add_argument("--eol", default=None, help=t("cli.arg.eol"))
    p.add_argument("--wait-regex", default=None)
    p.add_argument("--timeout-ms", type=int, default=None)

    p = add("snapshot", help=t("cli.cmd.snapshot"))
    p.add_argument("name")
    p.add_argument("--tail-lines", type=int, default=200)

    p = add("export", help=t("cli.cmd.export"))
    p.add_argument("name")
    p.add_argument("--path", default=None)
    p.add_argument("--format", default="raw", choices=["raw", "noansi", "json"])
    p.add_argument("--range", default=None, help=t("cli.arg.range"))

    p = add("mark", help=t("cli.cmd.mark"))
    p.add_argument("name")
    p.add_argument("label")

    p = add("clear", help=t("cli.cmd.clear"))
    p.add_argument("name")

    p = add("bridge", help=t("cli.cmd.bridge"))
    p.add_argument("action", choices=["start", "stop", "status"])
    p.add_argument("--port", required=True)
    p.add_argument("--mode", default="server", choices=["server", "client"])
    p.add_argument("--tcp-port", type=int, default=None)
    p.add_argument("--host", default=None)

    p = add("reload", help=t("cli.cmd.reload"))
    p.add_argument("--config", default=None, help=t("cli.arg.config_reload"))

    add("shutdown", help=t("cli.cmd.shutdown"), description=t("cli.desc.shutdown"))

    p = add("serve", help=t("cli.cmd.serve"))
    p.add_argument("--config", required=True)
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    p.add_argument("--no-autostart", action="store_true")
    p.add_argument("--log-level", default=None)

    p = add("mcp", help=t("cli.cmd.mcp"))
    p.add_argument("--stdio", action="store_true", help=t("cli.arg.stdio"))
    p.add_argument("--transport", default="stdio", help=t("cli.arg.transport"))
    p.add_argument("--profile", default="full", choices=["full", "lite"],
                   help=t("cli.arg.profile"))

    # lite 接口：输出刻意不同于其它子命令的单行 JSON 信封（原始 JSON 数组 / 纯文本）。
    p = add("tools", help=t("cli.cmd.tools"), description=t("cli.desc.tools"))
    p.add_argument("--format", default="anthropic", choices=["anthropic", "openai"],
                   help=t("cli.arg.tools_format"))

    p = add("call", help=t("cli.cmd.call"), description=t("cli.desc.call"))
    p.add_argument("tool", help=t("cli.arg.call_tool"))
    p.add_argument("args", nargs="?", default=None, help=t("cli.arg.call_args"))

    # ZMODEM：同样输出纯文本（每个文件一行），出错以 "error:" 开头、退出码 1。
    p = add("zsend", help=t("cli.cmd.zsend"), description=t("cli.desc.zsend"))
    p.add_argument("port")
    p.add_argument("files", nargs="*", help=t("cli.arg.zsend_files"))
    p.add_argument("--content", default=None, help=t("cli.arg.zsend_content"))
    p.add_argument("--name", dest="content_name", default=None, help=t("cli.arg.zsend_name"))
    p.add_argument("--encoding", default="text", choices=["text", "base64"],
                   help=t("cli.arg.zsend_encoding"))
    p.add_argument("--cmd", default="rz", help=t("cli.arg.zsend_cmd"))
    p.add_argument("--no-overwrite", action="store_true", help=t("cli.arg.zsend_no_overwrite"))
    p.add_argument("--timeout-ms", type=int, default=None, help=t("cli.arg.zm_timeout"))

    p = add("zrecv", help=t("cli.cmd.zrecv"), description=t("cli.desc.zrecv"))
    p.add_argument("port")
    p.add_argument("remote", nargs="+", help=t("cli.arg.zrecv_remote"))
    p.add_argument("--dir", default=None, help=t("cli.arg.zrecv_dir"))
    p.add_argument("--cmd", default=None, help=t("cli.arg.zrecv_cmd"))
    p.add_argument("--overwrite", action="store_true", help=t("cli.arg.zrecv_overwrite"))
    p.add_argument("--inline", action="store_true", help=t("cli.arg.zrecv_inline"))
    p.add_argument("--timeout-ms", type=int, default=None, help=t("cli.arg.zm_timeout"))

    return parser


# --------------------------------------------------------------------------- #
# handlers
# --------------------------------------------------------------------------- #
def _dispatch(command: str, args: argparse.Namespace) -> int:
    url, token = _global_options(args)
    client = Client(url, token)
    if command in ("list", "ports"):
        env = client.list_ports()
    elif command == "status":
        env = client.status(args.name)
    elif command == "open":
        env = client.open(args.name, baud=args.baud, device=args.device)
    elif command == "close":
        env = client.close(args.name)
    elif command == "set-device":
        if (args.device is None) == (args.match is None):
            env = {"ok": False, "error": t("cli.error.set_device_one")}
        else:
            try:
                match = _parse_match_arg(args.match) if args.match is not None else None
            except ValueError as exc:
                env = {"ok": False, "error": str(exc)}
            else:
                env = client.set_device(args.name, device=args.device, match=match,
                                        reopen=not args.no_reopen)
    elif command == "discover":
        env = client.discover()
    elif command == "pins":
        env = _pins_command(client, args)
    elif command == "read":
        env = client.read(args.name, since_seq=args.since_seq, since_ts=args.since_ts,
                          filter=args.filter, limit=args.limit,
                          strip_ansi=args.strip_ansi, kinds=args.kinds)
    elif command == "search":
        env = client.search(args.name, args.regex, context=args.context,
                            since_seq=args.since_seq, limit=args.limit)
    elif command == "wait":
        env = client.wait(args.name, args.regex, timeout_ms=args.timeout_ms,
                          since_seq=args.since_seq)
    elif command == "send":
        env = client.send(args.name, _read_send_data(args.data),
                          eol=_resolve_eol(args.eol), wait_regex=args.wait_regex,
                          timeout_ms=args.timeout_ms)
    elif command == "snapshot":
        env = client.snapshot(args.name, tail_lines=args.tail_lines)
    elif command == "export":
        env = client.export(args.name, path=args.path, fmt=args.format, rng=args.range)
    elif command == "mark":
        env = client.mark(args.name, args.label)
    elif command == "clear":
        env = client.clear(args.name)
    elif command == "bridge":
        env = client.bridge(args.action, args.port, mode=args.mode,
                            tcp_port=args.tcp_port, host=args.host)
    elif command == "reload":
        target = getattr(args, "config", None)
        if target:
            target = os.path.abspath(os.path.expanduser(str(target)))
        env = client.reload(target)
    elif command == "shutdown":
        env = client.shutdown()
        if env.get("ok") and isinstance(env.get("data"), dict):
            env = {"ok": True, **env["data"]}  # 端点自带 data 包装：拆开，避免 data.data
    else:
        env = {"ok": False, "error": f"unknown command: {command}"}
    return _print_env(env)


def _cmd_serve(args: argparse.Namespace) -> int:
    """Run the daemon in-process. Heavy imports stay here (lazy)."""
    import uvicorn  # noqa: PLC0415 - lazy by design

    from .config import Config
    from .hub import Hub
    from .server import create_app

    cfg = Config.load(args.config)
    overrides: dict[str, Any] = {}
    if args.host is not None:
        overrides["host"] = args.host
    if args.port is not None:
        overrides["port"] = args.port
    if overrides:
        cfg = cfg.merge_overrides(**overrides)
    if args.no_autostart:
        for port_cfg in cfg.ports:
            port_cfg.autostart = False

    hub = Hub(cfg)
    hub.start()
    app = create_app(hub, cfg)
    server = uvicorn.Server(uvicorn.Config(app, host=cfg.server.host, port=cfg.server.port,
                                           log_level=args.log_level or "info"))
    # POST /api/shutdown (Web UI "Exit" / `serialhub shutdown`) ends server.run() cleanly.
    app.state.request_exit = lambda: setattr(server, "should_exit", True)
    try:
        server.run()
    except KeyboardInterrupt:
        pass
    finally:
        hub.stop()
    return 0


def _cmd_mcp(args: argparse.Namespace) -> int:
    transport = "stdio" if args.stdio else args.transport
    if getattr(args, "profile", "full") == "lite":
        from . import mcp_lite  # noqa: PLC0415 - lazy by design

        mcp_lite.main(transport=transport, base_url=getattr(args, "url", None),
                      token=getattr(args, "token", None))
        return 0

    from . import mcp_server  # noqa: PLC0415 - lazy by design

    mcp_server.main(transport=transport)
    return 0


def _cmd_tools(args: argparse.Namespace) -> int:
    """Print the lite tool definitions as one compact JSON array (no daemon needed)."""
    from . import lite  # noqa: PLC0415

    tools = lite.to_openai() if args.format == "openai" else lite.to_anthropic()
    print(json.dumps(tools, ensure_ascii=False, separators=(",", ":")))
    return 0


def _cmd_call(args: argparse.Namespace) -> int:
    """Run one lite tool and print its plain-text result; exit 1 on ``error:``."""
    from . import lite  # noqa: PLC0415

    tool_args: Any = {}
    if args.args:
        try:
            tool_args = json.loads(args.args)
        except json.JSONDecodeError as exc:
            print(f"error: {t('cli.error.call_args_json', error=exc)}")
            return 1
    url, token = _global_options(args)
    if getattr(args, "url", None) is None:  # same env fallback as `mcp --profile lite`
        url = os.environ.get("SERIALHUB_URL") or url
    text = lite.call(Client(url, token), args.tool, tool_args)
    print(text)
    return 1 if text.startswith("error:") else 0


def _progress() -> Any:
    """stderr progress for zsend/zrecv, only when stderr is a TTY (else ``None``)."""
    if not sys.stderr.isatty():
        return None
    last = [0.0]

    def show(name: str, done: int, total: Optional[int]) -> None:
        now = time.monotonic()
        if now - last[0] < 0.2 and done != total:
            return
        last[0] = now
        pct = f" {done * 100 // total}%" if total else ""
        sys.stderr.write(f"\r{name} {done}/{total if total is not None else '?'}B{pct}\033[K")
        if done == total:
            sys.stderr.write("\n")
        sys.stderr.flush()

    return show


def _cmd_zmodem(args: argparse.Namespace) -> int:
    """``zsend`` / ``zrecv``: print the plain-text result; exit 1 on ``error:``."""
    from . import zmodem_client as zc  # noqa: PLC0415 - websockets only when used

    url, token = _global_options(args)
    if getattr(args, "url", None) is None:
        url = os.environ.get("SERIALHUB_URL") or url
    if args.command == "zsend":
        contents: list[tuple[str, bytes]] = []
        if args.content is not None:
            if args.files:
                text = f"error: {t('cli.error.zsend_one')}"
            elif not args.content_name:
                text = f"error: {t('cli.error.zsend_name')}"
            else:
                # stdin 按字节读：Windows 上文本模式会改写换行
                body = sys.stdin.buffer.read() if args.content == "-" else args.content
                try:
                    contents.append(zc.inline_content(body, args.content_name, args.encoding))
                    text = ""
                except ValueError as exc:
                    text = f"error: {exc}"
        elif not args.files:
            text = f"error: {t('cli.error.zsend_one')}"
        else:
            text = ""
        if not text:
            text = zc.zsend(url, token, args.port, args.files, contents=contents,
                            cmd=args.cmd, timeout_ms=zc.clamp_timeout(args.timeout_ms),
                            overwrite=not args.no_overwrite, progress=_progress())
    else:
        text = zc.zrecv(url, token, args.port, args.remote, args.dir,
                        cmd=args.cmd, timeout_ms=zc.clamp_timeout(args.timeout_ms),
                        overwrite=args.overwrite, inline=args.inline, progress=_progress())
    print(text)
    bad = ("error:", "not received ", "skipped ")
    return 1 if any(line.startswith(bad) for line in text.split("\n")) else 0


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _build_parser().parse_args(argv)
    if args.command == "serve":
        return _cmd_serve(args)
    if args.command == "mcp":
        return _cmd_mcp(args)
    if args.command == "tools":
        return _cmd_tools(args)
    if args.command == "call":
        try:
            return _cmd_call(args)
        except BrokenPipeError:
            return 1
    if args.command in ("zsend", "zrecv"):
        try:
            return _cmd_zmodem(args)
        except BrokenPipeError:
            return 1
    try:
        return _dispatch(args.command, args)
    except BrokenPipeError:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
