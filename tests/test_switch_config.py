"""运行时切换配置文件（``hub.switch_config`` / ``POST /api/reload {config}``）的测试。

覆盖面：
- ``Hub.switch_config``：加载+校验→整体热重载、记住生效路径、非法路径不改状态。
- REST ``POST /api/reload``：``config``/``path`` 两种字段、切换后裸 reload 跟随新文件、
  ``GET /api/config`` 反映切换结果、坏路径 400。
- CLI ``reload --config`` 的参数解析；``Client.reload`` 的请求体；MCP ``reload`` 工具签名。

全部使用 ``loop://`` 虚拟串口，不碰真实硬件。
"""

from __future__ import annotations

import json
import os
import time

import pytest

from serialhub.config import Config
from serialhub.hub import Hub

SETTLE = 0.3


def _write_yaml(tmp_path, name: str, ports: list[str], *, baud: int = 115200) -> str:
    """写一份最小合法 YAML 配置（端口都用 loop://），返回绝对路径。"""
    lines = ["server:", "  host: 127.0.0.1", "  port: 8765", "defaults:",
             "  reconnect_ms: 200", "  reconnect_max_ms: 500", "  flush_ms: 20", "ports:"]
    for port in ports:
        lines += [f"  - name: {port}", "    device: loop://", "    autostart: true",
                  f"    baud: {baud}", "    buffer_lines: 64", "    buffer_bytes: 1048576"]
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def _write_json(tmp_path, name: str, ports: list[str], *, baud: int = 115200) -> str:
    path = tmp_path / name
    path.write_text(json.dumps({
        "server": {"host": "127.0.0.1", "port": 8765},
        "defaults": {"reconnect_ms": 200, "reconnect_max_ms": 500, "flush_ms": 20},
        "ports": [{"name": p, "device": "loop://", "autostart": True, "baud": baud,
                   "buffer_lines": 64, "buffer_bytes": 1 << 20} for p in ports],
    }, indent=2), encoding="utf-8")
    return str(path)


def _wait_open(hub: Hub, name: str, timeout: float = 3.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if hub.is_open(name):
            return True
        time.sleep(0.03)
    return hub.is_open(name)


# --------------------------------------------------------------------------- #
# Hub.switch_config
# --------------------------------------------------------------------------- #
def test_switch_config_applies_and_remembers_path(hub, tmp_path) -> None:
    # Given 初始配置（t、t2）且 t 已打开
    assert hub.config.port_names() == ["t", "t2"]
    assert hub.is_open("t") is True

    # When 切换到另一份配置文件（保留 t 并改 baud、去掉 t2、新增 t3）
    target = _write_yaml(tmp_path, "other.yaml", ["t", "t3"], baud=9600)
    out = hub.switch_config(target)

    # Then 变更摘要、生效路径、端口集合、真实生效的 baud 都对
    assert out["config"] == target
    assert "t3" in out["added"]
    assert "t2" in out["removed"]
    assert "t" in out["restarted"]          # baud 是 IO 字段 → 重启该口
    assert hub.config.path == target
    assert hub.config.port_names() == ["t", "t3"]
    assert hub.config.port("t").baud == 9600
    assert hub.config.port("t3").baud == 9600
    assert _wait_open(hub, "t3") is True
    with pytest.raises(KeyError):
        hub.status("t2")                    # 已从配置中移除


def test_switch_config_json_file_and_defaults_apply(hub, tmp_path) -> None:
    target = _write_json(tmp_path, "profile.json", ["j1"], baud=57600)
    out = hub.switch_config(target)
    assert out["config"] == target
    assert hub.config.port_names() == ["j1"]
    assert hub.config.port("j1").baud == 57600
    assert hub.config.port("j1").framing == "line"   # 未写 → 新默认值
    assert _wait_open(hub, "j1") is True


@pytest.mark.parametrize("case", ["missing", "empty", "invalid"])
def test_switch_config_failure_leaves_current_config_untouched(hub, tmp_path, case: str) -> None:
    before = (hub.config.path, hub.config.port_names(), hub.config.port("t").baud)
    if case == "missing":
        target = str(tmp_path / "nope.yaml")
    elif case == "empty":
        target = "   "
    else:
        bad = tmp_path / "bad.yaml"
        bad.write_text("ports:\n  - name: x\n    device: loop://\n    data_bits: 9\n",
                       encoding="utf-8")
        target = str(bad)

    with pytest.raises(ValueError):
        hub.switch_config(target)

    assert (hub.config.path, hub.config.port_names(), hub.config.port("t").baud) == before
    assert hub.is_open("t") is True


# --------------------------------------------------------------------------- #
# REST：POST /api/reload {config} + GET /api/config
# --------------------------------------------------------------------------- #
def test_rest_reload_switches_then_bare_reload_follows(client, cfg, tmp_path) -> None:
    target = _write_yaml(tmp_path, "rest.yaml", ["t", "r1"])

    resp = client.post("/api/reload", json={"config": target})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True and body["config"] == target
    assert "r1" in body["added"] and "t2" in body["removed"]

    active = client.get("/api/config").json()["config"]
    assert active["path"] == target
    assert {p["name"] for p in active["ports"]} == {"t", "r1"}

    # 修改被切换到的文件，再"裸"reload：应读新文件（而不是启动时那份）
    target2 = _write_yaml(tmp_path, "rest.yaml", ["t", "r2"])
    assert target2 == target
    bare = client.post("/api/reload")
    assert bare.status_code == 200
    assert "r2" in bare.json()["added"]
    assert {p["name"] for p in client.get("/api/ports").json()["ports"]} == {"t", "r2"}


def test_rest_reload_accepts_path_alias(client, tmp_path) -> None:
    target = _write_json(tmp_path, "alias.json", ["a1"])
    body = client.post("/api/reload", json={"path": target}).json()
    assert body["ok"] is True
    assert body["config"] == target
    assert body["added"] == ["a1"]


def test_rest_reload_missing_file_is_400_and_keeps_config(client, cfg, tmp_path) -> None:
    resp = client.post("/api/reload", json={"config": str(tmp_path / "absent.yaml")})
    assert resp.status_code == 400
    assert resp.json()["ok"] is False
    assert "not found" in resp.json()["error"].lower()
    # 原配置不变
    assert {p["name"] for p in client.get("/api/ports").json()["ports"]} == {"t", "t2"}


def test_rest_reload_invalid_config_is_400(client, tmp_path) -> None:
    bad = tmp_path / "invalid.yaml"
    bad.write_text("ports:\n  - name: x\n    device: loop://\n    parity: Q\n", encoding="utf-8")
    resp = client.post("/api/reload", json={"config": str(bad)})
    assert resp.status_code == 400
    assert resp.json()["ok"] is False


# --------------------------------------------------------------------------- #
# CLI / Client / MCP 接线
# --------------------------------------------------------------------------- #
def test_cli_reload_config_arg_is_optional() -> None:
    from serialhub.cli import _build_parser

    plain = _build_parser().parse_args(["reload"])
    assert plain.config is None
    with_file = _build_parser().parse_args(["reload", "--config", "other.yaml"])
    assert with_file.config == "other.yaml"


def test_client_reload_sends_config_body(monkeypatch) -> None:
    from serialhub.client import Client

    seen: list[dict] = []

    def fake_call(self, method, path, *, params=None, body=None, timeout=None):
        seen.append({"method": method, "path": path, "body": body})
        return {"ok": True}

    monkeypatch.setattr(Client, "call", fake_call)
    Client().reload("/tmp/x.yaml")
    Client().reload()
    assert seen[0] == {"method": "POST", "path": "/api/reload",
                       "body": {"config": "/tmp/x.yaml"}}
    assert seen[1]["body"] is None


def test_mcp_reload_tool_takes_config() -> None:
    pytest.importorskip("mcp")
    import inspect

    from serialhub import mcp_server

    params = inspect.signature(mcp_server.reload).parameters
    assert "config" in params
    assert params["config"].default is None


def test_config_load_roundtrip_sets_absolute_path(tmp_path) -> None:
    target = _write_yaml(tmp_path, "abs.yaml", ["z"])
    loaded = Config.load(target)
    assert loaded.path == target
    assert loaded.to_dict()["path"] == target


# --------------------------------------------------------------------------- #
# AI 侧专用接口：Client.switch_config + MCP switch_config 工具（第 18 个工具）
# --------------------------------------------------------------------------- #
def test_client_switch_config_sends_config_body(monkeypatch) -> None:
    from serialhub.client import Client

    seen: list[dict] = []

    def fake_call(self, method, path, *, params=None, body=None, timeout=None):
        seen.append(body)
        return {"ok": True}

    monkeypatch.setattr(Client, "call", fake_call)
    Client().switch_config("/tmp/profile.yaml")
    assert seen == [{"config": "/tmp/profile.yaml"}]


def test_mcp_switch_config_forwards_absolute_path_and_rejects_empty(monkeypatch) -> None:
    pytest.importorskip("mcp")
    import asyncio

    from serialhub import mcp_server
    from serialhub.client import Client

    seen: list[str] = []

    def fake_switch(self, path):
        seen.append(path)
        return {"ok": True, "config": path}

    monkeypatch.setattr(Client, "switch_config", fake_switch)

    out = asyncio.run(mcp_server.switch_config("profiles/b.yaml"))
    assert out["ok"] is True
    assert seen and os.path.isabs(seen[0]) and seen[0].endswith("b.yaml")

    empty = asyncio.run(mcp_server.switch_config("   "))
    assert empty["ok"] is False
    assert "non-empty" in empty["error"]
    assert len(seen) == 1  # 空路径不发请求
