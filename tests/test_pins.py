"""串口引脚控制（DTR/RTS/reset）测试（§5/§6/§7/§9/§11）——只用 ``loop://`` 虚拟口。

覆盖 T3 冻结契约：

- ``PortConfig.pins`` 缺省/None ⇒ ``enabled=False``：worker 任何时刻（含 open）都不调用
  ``setDTR``/``setRTS``，运行时引脚 API 一律 ``ValueError``（消息含 "pins disabled" 与端口名）。
- ``pins.enabled=true``：open 时置初始电平（``pins.dtr/rts`` 优先，回退顶层 ``dtr/rts``），
  ``set_pin``/``pulse_pin``/``reset_pins`` 与只读 ``pin_state`` 行为，端口未打开 → ``RuntimeError``。
- Hub ``pins_status``/``set_pin``、``_port_status`` 的附加键、reload 重启该口。
- REST ``GET/POST /api/ports/{name}/pins``（400/404/409 映射）、CLI ``pins``、MCP ``set_pin``。

禁止真实串口：所有设备都是 ``loop://``；``serial.serial_for_url`` 只在测试内被包一层
计数桩（底层仍是真实 ``loop://`` 实现），用于证明"默认禁用时一次都没碰过引脚"。
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any, Optional

import pytest
import serial

from serialhub.cli import _parse_pin_set
from serialhub.cli import main as cli_main
from serialhub.client import Client
from serialhub.config import Config, PortConfig, pins_view
from serialhub.server import create_app

DEFAULT_PINS = {"enabled": False, "dtr": None, "rts": None, "pulse_ms": 100}
POLL_TIMEOUT = 3.0


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _port(**over: Any) -> dict:
    """单个 loop:// 测试口的基线配置，可用关键字覆盖任意字段。"""
    base = {
        "name": "t",
        "device": "loop://",
        "autostart": True,
        "buffer_lines": 64,
        "buffer_bytes": 1 << 20,
        "reconnect_ms": 200,
        "reconnect_max_ms": 500,
        "dtr": True,
        "rts": True,
    }
    base.update(over)
    return base


class _SerialSpy:
    """包住真实 ``loop://`` 串口，记录每次 ``setDTR``/``setRTS`` 调用。"""

    def __init__(self, real: Any) -> None:
        self._real = real
        self.calls: list[tuple[str, bool]] = []

    def setDTR(self, value: Any) -> Any:  # noqa: N802 - pyserial API
        self.calls.append(("dtr", bool(value)))
        return self._real.setDTR(value)

    def setRTS(self, value: Any) -> Any:  # noqa: N802 - pyserial API
        self.calls.append(("rts", bool(value)))
        return self._real.setRTS(value)

    @property
    def dtr(self) -> Any:
        return self._real.dtr

    @property
    def rts(self) -> Any:
        return self._real.rts

    def __getattr__(self, item: str) -> Any:  # read/write/flush/close/... 原样透传
        return getattr(self._real, item)


class _NoDtrSpy(_SerialSpy):
    """``setDTR`` 永远失败的桩：验证 open 时置初始电平失败只记 last_error、不关端口。"""

    def setDTR(self, value: Any) -> Any:  # noqa: N802 - pyserial API
        self.calls.append(("dtr", bool(value)))
        raise NotImplementedError("no DTR on this url")


def _install_serial_spy(monkeypatch) -> list[_SerialSpy]:
    """把 ``serial.serial_for_url`` 换成计数桩（底层仍是真实 ``loop://``）。"""
    original = serial.serial_for_url
    spies: list[_SerialSpy] = []

    def fake_for_url(device: str, **kwargs: Any) -> _SerialSpy:
        spy = _SerialSpy(original(device, **kwargs))
        spies.append(spy)
        return spy

    monkeypatch.setattr("serial.serial_for_url", fake_for_url)
    return spies


def _enabled_hub(hub_factory, make_config, monkeypatch, *, spy_cls=_SerialSpy, **pins: Any):
    """启用引脚控制的 loop:// 口（``t``）+ 计数桩。"""
    original = serial.serial_for_url
    spies: list[_SerialSpy] = []

    def fake_for_url(device: str, **kwargs: Any) -> _SerialSpy:
        spy = spy_cls(original(device, **kwargs))
        spies.append(spy)
        return spy

    monkeypatch.setattr("serial.serial_for_url", fake_for_url)
    cfg = make_config(ports=[_port(pins={"enabled": True, **pins})])
    hub = hub_factory(cfg)
    return hub, spies


# --------------------------------------------------------------------------- #
# 配置：默认值、归一化与校验
# --------------------------------------------------------------------------- #
def test_pins_omitted_defaults_to_disabled() -> None:
    cfg = Config.from_dict({"ports": [{"name": "a", "device": "loop://"}]})
    assert cfg.port("a").pins == DEFAULT_PINS
    assert cfg.to_dict()["ports"][0]["pins"] == DEFAULT_PINS


def test_pins_none_equals_disabled() -> None:
    cfg = Config.from_dict({"ports": [{"name": "a", "device": "loop://", "pins": None}]})
    assert cfg.port("a").pins == DEFAULT_PINS
    assert cfg.to_dict()["ports"][0]["pins"] == DEFAULT_PINS


def test_pins_normalized_shape_and_roundtrip() -> None:
    cfg = Config.from_dict({
        "ports": [{
            "name": "a", "device": "loop://",
            "pins": {"enabled": True, "dtr": False, "rts": True, "pulse_ms": 25},
        }],
    })
    assert cfg.port("a").pins == {"enabled": True, "dtr": False, "rts": True, "pulse_ms": 25}
    exported = cfg.to_dict()["ports"][0]["pins"]
    assert exported == cfg.port("a").pins
    again = Config.from_dict(cfg.to_dict())
    assert again.port("a").pins == cfg.port("a").pins


def test_pins_tolerates_missing_dtr_rts_and_enabled() -> None:
    cfg = Config.from_dict({
        "ports": [{"name": "a", "device": "loop://", "pins": {"enabled": True}}],
    })
    assert cfg.port("a").pins == {"enabled": True, "dtr": None, "rts": None, "pulse_ms": 100}
    cfg2 = Config.from_dict({
        "ports": [{"name": "a", "device": "loop://", "pins": {"dtr": False}}],
    })
    assert cfg2.port("a").pins == {"enabled": False, "dtr": False, "rts": None, "pulse_ms": 100}


def test_pins_inherited_from_defaults_but_replaced_wholesale_by_port() -> None:
    cfg = Config.from_dict({
        "defaults": {"pins": {"enabled": True, "pulse_ms": 7}},
        "ports": [{"name": "a", "device": "loop://"}],
    })
    assert cfg.port("a").pins == {"enabled": True, "dtr": None, "rts": None, "pulse_ms": 7}
    # 端口级 pins 是整体覆盖（与 match/tcp_bridge 的合并语义一致），不做深合并。
    cfg2 = Config.from_dict({
        "defaults": {"pins": {"enabled": True, "pulse_ms": 7}},
        "ports": [{"name": "a", "device": "loop://", "pins": {"pulse_ms": 3}}],
    })
    assert cfg2.port("a").pins == {"enabled": False, "dtr": None, "rts": None, "pulse_ms": 3}


@pytest.mark.parametrize("pins,needle", [
    ({"bogus": 1}, "ports[a].pins.bogus is not a valid pins key"),
    ({"pulse_ms": -1}, "ports[a].pins.pulse_ms must be >= 0"),
    ({"pulse_ms": "abc"}, "ports[a].pins.pulse_ms must be an integer"),
    ({"dtr": "yes"}, "ports[a].pins.dtr must be a boolean or null"),
    ({"rts": 1}, "ports[a].pins.rts must be a boolean or null"),
    ({"enabled": "true"}, "ports[a].pins.enabled must be a boolean"),
    ("on", "ports[a].pins must be a mapping"),
])
def test_pins_validation_errors(pins: Any, needle: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        Config.from_dict({"ports": [{"name": "a", "device": "loop://", "pins": pins}]})
    assert needle in str(excinfo.value)


def test_pins_view_tolerates_directly_constructed_port_config() -> None:
    assert pins_view(PortConfig(name="a", device="loop://")) == DEFAULT_PINS
    junk = PortConfig(name="a", device="loop://", pins={"enabled": True, "junk": 1})
    assert pins_view(junk) == DEFAULT_PINS
    coerced = PortConfig(name="a", device="loop://", pins={"enabled": True, "pulse_ms": -3})
    assert pins_view(coerced)["pulse_ms"] == 100  # 负值回退默认，绝不抛异常
    assert pins_view(coerced)["enabled"] is True


# --------------------------------------------------------------------------- #
# 默认禁用：worker 绝不触碰 DTR/RTS
# --------------------------------------------------------------------------- #
def test_disabled_port_never_calls_dtr_rts(monkeypatch, hub_factory, make_config) -> None:
    spies = _install_serial_spy(monkeypatch)
    hub = hub_factory(make_config(ports=[_port()]))
    assert hub.is_open("t") is True
    time.sleep(0.05)  # 让读循环跑几轮
    assert spies and spies[0].calls == []  # open 时一次都没调用
    assert hub._workers["t"].pin_state == {"dtr": None, "rts": None}
    assert hub.status("t")["pins"] == DEFAULT_PINS

    # 断线重连（任何时刻）也不得触碰引脚。
    hub._workers["t"]._close_serial()
    deadline = time.time() + POLL_TIMEOUT
    while not hub.is_open("t") and time.time() < deadline:
        time.sleep(0.02)
    assert hub.is_open("t") is True
    assert all(spy.calls == [] for spy in spies)


def test_disabled_runtime_pin_apis_raise_value_error(monkeypatch, hub_factory, make_config) -> None:
    spies = _install_serial_spy(monkeypatch)
    hub = hub_factory(make_config(ports=[_port()]))
    worker = hub._workers["t"]

    for call in (
        lambda: worker.set_pin("dtr", True),
        lambda: worker.pulse_pin("dtr", 1),
        lambda: worker.reset_pins(1),
    ):
        with pytest.raises(ValueError) as excinfo:
            call()
        message = str(excinfo.value)
        assert "pins disabled" in message
        assert "t" in message

    with pytest.raises(ValueError, match="pins disabled"):
        hub.pins_status("t")
    with pytest.raises(ValueError, match="pins disabled"):
        hub.set_pin("t", "set", "dtr", True)
    assert spies[0].calls == []


def test_disabled_rest_pins_endpoints_return_400(client) -> None:
    for name in ("t", "t2"):
        got = client.get(f"/api/ports/{name}/pins")
        assert got.status_code == 400
        body = got.json()
        assert body["ok"] is False
        assert "pins disabled" in body["error"] and name in body["error"]

    posted = client.post("/api/ports/t/pins", json={"action": "set", "pin": "dtr", "value": False})
    assert posted.status_code == 400
    assert "pins disabled" in posted.json()["error"]

    reset = client.post("/api/ports/t/pins", json={"action": "reset"})
    assert reset.status_code == 400
    assert "pins disabled" in reset.json()["error"]


# --------------------------------------------------------------------------- #
# 启用：open 初始电平
# --------------------------------------------------------------------------- #
def test_enabled_open_applies_pins_initial_levels(monkeypatch, hub_factory, make_config) -> None:
    hub, spies = _enabled_hub(hub_factory, make_config, monkeypatch,
                              dtr=False, rts=True, pulse_ms=10)
    assert hub.is_open("t") is True
    assert hub._workers["t"].pin_state == {"dtr": False, "rts": True}
    assert spies[0].calls == [("dtr", False), ("rts", True)]
    assert spies[0].dtr is False and spies[0].rts is True  # 真串口电平确实变了
    assert hub.status("t")["pins"] == {
        "enabled": True, "dtr": False, "rts": True, "pulse_ms": 10,
    }


def test_enabled_open_falls_back_to_top_level_dtr_rts(monkeypatch, hub_factory, make_config) -> None:
    hub, spies = _enabled_hub(hub_factory, make_config, monkeypatch)
    # pins 只给了 enabled：初始电平回退顶层 dtr/rts（_port 里都是 True）。
    assert hub._workers["t"].pin_state == {"dtr": True, "rts": True}
    assert spies[0].calls == [("dtr", True), ("rts", True)]
    assert hub.pins_status("t")["initial"] == {"dtr": True, "rts": True}

    other = hub_factory(make_config(ports=[_port(dtr=False, rts=False, pins={"enabled": True})]))
    try:
        assert other._workers["t"].pin_state == {"dtr": False, "rts": False}
        assert other.pins_status("t")["initial"] == {"dtr": False, "rts": False}
    finally:
        other.stop()


def test_enabled_initial_level_failure_records_error_but_keeps_port_open(
    monkeypatch, hub_factory, make_config,
) -> None:
    hub, spies = _enabled_hub(hub_factory, make_config, monkeypatch, spy_cls=_NoDtrSpy)
    assert hub.is_open("t") is True  # 引脚失败不关端口
    worker = hub._workers["t"]
    assert worker.pin_state == {"dtr": None, "rts": True}  # 失败的那脚电平未知
    assert "NotImplementedError" in (worker.last_error or "")
    assert "dtr" in (worker.last_error or "")


# --------------------------------------------------------------------------- #
# 启用：set / pulse / reset
# --------------------------------------------------------------------------- #
def test_set_pin_updates_level_and_serial(monkeypatch, hub_factory, make_config) -> None:
    hub, spies = _enabled_hub(hub_factory, make_config, monkeypatch, pulse_ms=10)
    worker = hub._workers["t"]
    assert worker.set_pin("dtr", False) == {"dtr": False, "rts": True}
    assert spies[0].calls[-1] == ("dtr", False)
    assert spies[0].dtr is False
    assert worker.set_pin("dtr", True) == {"dtr": True, "rts": True}
    assert spies[0].dtr is True


def test_set_pin_unknown_pin_raises(monkeypatch, hub_factory, make_config) -> None:
    hub, _ = _enabled_hub(hub_factory, make_config, monkeypatch)
    with pytest.raises(ValueError) as excinfo:
        hub._workers["t"].set_pin("xyz", True)
    assert "xyz" in str(excinfo.value)
    with pytest.raises(ValueError):
        hub._workers["t"].pulse_pin("cts", 5)


def test_pulse_pin_flips_then_restores(monkeypatch, hub_factory, make_config) -> None:
    hub, spies = _enabled_hub(hub_factory, make_config, monkeypatch, pulse_ms=200)
    worker = hub._workers["t"]
    seen: dict[str, Any] = {}

    def _run() -> None:
        seen["state"] = worker.pulse_pin("dtr")

    thread = threading.Thread(target=_run)
    thread.start()
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline and worker.pin_state["dtr"] is not False:
        time.sleep(0.005)
    assert worker.pin_state["dtr"] is False  # 保持在相反电平
    thread.join(3)
    assert not thread.is_alive()
    assert worker.pin_state["dtr"] is True  # 结束后恢复
    assert seen["state"] == {"dtr": True, "rts": True}
    assert spies[0].calls == [("dtr", True), ("rts", True), ("dtr", False), ("dtr", True)]


def test_pulse_pin_default_duration_uses_pins_pulse_ms(monkeypatch, hub_factory, make_config) -> None:
    hub, spies = _enabled_hub(hub_factory, make_config, monkeypatch, pulse_ms=60)
    worker = hub._workers["t"]
    started = time.monotonic()
    state = worker.pulse_pin("rts")
    elapsed = time.monotonic() - started
    assert elapsed >= 0.04  # 至少等了大半 pulse_ms
    assert state == {"dtr": True, "rts": True}
    assert ("rts", False) in spies[0].calls and ("rts", True) in spies[0].calls


def test_reset_pins_sequence_and_restore(monkeypatch, hub_factory, make_config) -> None:
    hub, spies = _enabled_hub(hub_factory, make_config, monkeypatch,
                              dtr=False, rts=False, pulse_ms=20)
    worker = hub._workers["t"]
    assert worker.reset_pins(20) == {"dtr": False, "rts": False}
    assert spies[0].calls == [
        ("dtr", False), ("rts", False),   # open 初始电平
        ("rts", True), ("dtr", False),    # 第 1 段：rts=True, dtr=False
        ("rts", False), ("dtr", True),    # 第 2 段：rts=False, dtr=True
        ("dtr", False), ("rts", False),   # 恢复初始电平
    ]
    assert spies[0].dtr is False and spies[0].rts is False


def test_reset_pins_aborts_quietly_when_port_closes(monkeypatch, hub_factory, make_config) -> None:
    hub, _ = _enabled_hub(hub_factory, make_config, monkeypatch, pulse_ms=300)
    worker = hub._workers["t"]
    errors: list[BaseException] = []

    def _run() -> None:
        try:
            worker.reset_pins(300)
        except BaseException as exc:  # noqa: BLE001 - 测试断言用
            errors.append(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    time.sleep(0.08)  # 序列进行中
    hub.close("t")
    thread.join(4)
    assert not thread.is_alive()
    assert errors == []  # 中途关闭 → 安静退出


# --------------------------------------------------------------------------- #
# Hub 层：pins_status / set_pin / _port_status / reload
# --------------------------------------------------------------------------- #
def test_hub_pins_status_and_set_pin_shapes(monkeypatch, hub_factory, make_config) -> None:
    hub, _ = _enabled_hub(hub_factory, make_config, monkeypatch, dtr=True, rts=True, pulse_ms=15)
    assert hub.pins_status("t") == {
        "port": "t", "enabled": True, "dtr": True, "rts": True, "pulse_ms": 15,
        "initial": {"dtr": True, "rts": True},
    }
    set_out = hub.set_pin("t", "set", "dtr", False)
    assert set_out == {
        "port": "t", "action": "set", "pin": "dtr", "value": False,
        "dtr": False, "rts": True, "duration_ms": None,
    }
    pulse_out = hub.set_pin("t", "pulse", "rts", duration_ms=10)
    assert pulse_out["action"] == "pulse" and pulse_out["pin"] == "rts"
    assert pulse_out["duration_ms"] == 10 and pulse_out["rts"] is True
    reset_out = hub.set_pin("t", "reset")  # duration 缺省 = pins.pulse_ms
    assert reset_out["action"] == "reset" and reset_out["pin"] is None
    assert reset_out["duration_ms"] == 15
    assert (reset_out["dtr"], reset_out["rts"]) == (True, True)  # 恢复初始电平


def test_hub_pins_status_when_enabled_but_closed(monkeypatch, hub_factory, make_config) -> None:
    _install_serial_spy(monkeypatch)
    hub = hub_factory(make_config(ports=[_port(autostart=False, pins={"enabled": True})]))
    assert hub.is_open("t") is False
    status = hub.pins_status("t")  # 只读状态：未打开也能读配置/初始电平
    assert status["enabled"] is True
    assert status["dtr"] is None and status["rts"] is None
    assert status["initial"] == {"dtr": True, "rts": True}
    with pytest.raises(RuntimeError) as excinfo:
        hub.set_pin("t", "set", "dtr", True)
    assert "not open" in str(excinfo.value)
    with pytest.raises(RuntimeError):
        hub.set_pin("t", "pulse", "dtr", 5)
    with pytest.raises(RuntimeError):
        hub.set_pin("t", "reset")


def test_hub_set_pin_validation_errors(monkeypatch, hub_factory, make_config) -> None:
    hub, _ = _enabled_hub(hub_factory, make_config, monkeypatch)
    with pytest.raises(ValueError, match="unknown pins action"):
        hub.set_pin("t", "wobble")
    with pytest.raises(ValueError, match="requires a pin"):
        hub.set_pin("t", "set")
    with pytest.raises(ValueError, match="requires a pin"):
        hub.set_pin("t", "pulse")
    with pytest.raises(ValueError, match="boolean"):
        hub.set_pin("t", "set", "dtr")
    with pytest.raises(ValueError, match="boolean"):
        hub.set_pin("t", "set", "dtr", 1)  # int 不是 bool
    with pytest.raises(ValueError, match="unknown pin"):
        hub.set_pin("t", "set", "xyz", True)
    with pytest.raises(ValueError, match="duration_ms"):
        hub.set_pin("t", "pulse", "dtr", duration_ms=-5)
    with pytest.raises(ValueError, match="duration_ms"):
        hub.set_pin("t", "reset", duration_ms="soon")


def test_hub_pins_unknown_port_raises_key_error(hub) -> None:
    with pytest.raises(KeyError):
        hub.pins_status("nope")
    with pytest.raises(KeyError):
        hub.set_pin("nope", "set", "dtr", True)


def test_port_status_has_framing_flush_ms_and_pins(hub) -> None:
    status = hub.status("t")
    assert status["framing"] == "line"
    assert status["flush_ms"] == 20
    assert status["pins"] == DEFAULT_PINS


def test_reload_pins_change_restarts_open_port(monkeypatch, hub_factory, make_config) -> None:
    spies = _install_serial_spy(monkeypatch)
    start = make_config(ports=[_port(pins={"enabled": True, "dtr": True, "rts": True})])
    hub = hub_factory(start)
    assert hub.is_open("t") is True
    result = hub.reload(make_config(ports=[
        _port(pins={"enabled": True, "dtr": False, "rts": True}),
    ]))
    assert result["restarted"] == ["t"]
    assert hub._workers["t"].pin_state == {"dtr": False, "rts": True}
    assert spies[-1].calls == [("dtr", False), ("rts", True)]


# --------------------------------------------------------------------------- #
# REST / client
# --------------------------------------------------------------------------- #
@pytest.fixture
def pins_client(hub_factory, make_config, monkeypatch):
    """启用引脚控制的 loop:// 口 + FastAPI TestClient。"""
    _install_serial_spy(monkeypatch)
    cfg = make_config(ports=[
        _port(pins={"enabled": True, "dtr": True, "rts": True, "pulse_ms": 20}),
    ])
    hub = hub_factory(cfg)
    from fastapi.testclient import TestClient

    with TestClient(create_app(hub, cfg)) as tc:
        yield tc, hub


def test_rest_get_pins_enabled(pins_client) -> None:
    tc, _hub = pins_client
    response = tc.get("/api/ports/t/pins")
    assert response.status_code == 200
    body = response.json()
    assert body == {
        "ok": True, "port": "t", "enabled": True, "dtr": True, "rts": True,
        "pulse_ms": 20, "initial": {"dtr": True, "rts": True},
    }


def test_rest_post_pin_actions_roundtrip(pins_client) -> None:
    tc, _hub = pins_client
    set_response = tc.post("/api/ports/t/pins",
                           json={"action": "set", "pin": "dtr", "value": False})
    assert set_response.status_code == 200
    assert set_response.json() == {
        "ok": True, "port": "t", "action": "set", "pin": "dtr", "value": False,
        "dtr": False, "rts": True, "duration_ms": None,
    }
    assert tc.get("/api/ports/t/pins").json()["dtr"] is False

    pulse = tc.post("/api/ports/t/pins",
                    json={"action": "pulse", "pin": "dtr", "duration_ms": 10})
    assert pulse.status_code == 200
    # pulse 翻到相反电平再恢复：set 之后 dtr=False，脉冲结束后仍是 False。
    assert pulse.json()["dtr"] is False and pulse.json()["duration_ms"] == 10

    reset = tc.post("/api/ports/t/pins", json={"action": "reset", "duration_ms": 10})
    assert reset.status_code == 200
    body = reset.json()
    assert body["action"] == "reset" and body["pin"] is None and body["duration_ms"] == 10
    assert (body["dtr"], body["rts"]) == (True, True)  # reset 末段恢复初始电平


def test_rest_pin_error_mapping(pins_client) -> None:
    tc, _hub = pins_client
    assert tc.post("/api/ports/t/pins", json={"action": "nope"}).status_code == 400
    assert tc.post("/api/ports/t/pins",
                   json={"action": "set", "pin": "xyz", "value": True}).status_code == 400
    assert tc.post("/api/ports/t/pins", json={"action": "set", "pin": "dtr"}).status_code == 400
    assert tc.get("/api/ports/nope/pins").status_code == 404
    assert tc.post("/api/ports/nope/pins", json={"action": "reset"}).status_code == 404


def test_rest_post_pins_409_when_closed(monkeypatch, hub_factory, make_config) -> None:
    _install_serial_spy(monkeypatch)
    cfg = make_config(ports=[_port(autostart=False, pins={"enabled": True})])
    hub = hub_factory(cfg)
    from fastapi.testclient import TestClient

    with TestClient(create_app(hub, cfg)) as tc:
        response = tc.post("/api/ports/t/pins",
                           json={"action": "set", "pin": "dtr", "value": False})
        assert response.status_code == 409
        assert "not open" in response.json()["error"]


def test_client_pins_methods_exist_and_degrade_gracefully() -> None:
    client = Client("http://127.0.0.1:1", timeout=0.5)
    assert client.pins("t")["ok"] is False
    assert client.set_pin("t", "set", "dtr", True)["ok"] is False
    assert client.set_pin("t", "reset", duration_ms=5)["ok"] is False


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
class _StubClient:
    """记录调用的 Client 替身（CLI 走 HTTP，这里只验证参数与信封）。"""

    instances: list["_StubClient"] = []

    def __init__(self, url: str, token: Optional[str] = None) -> None:
        self.url = url
        self.token = token
        self.calls: list[tuple] = []
        _StubClient.instances.append(self)

    def pins(self, name: str) -> dict:
        self.calls.append(("pins", name))
        return {"ok": True, "port": name, "enabled": True, "dtr": True, "rts": True,
                "pulse_ms": 100, "initial": {"dtr": True, "rts": True}}

    def set_pin(self, name: str, action: str = "set", pin: Optional[str] = None,
                value: Optional[bool] = None,
                duration_ms: Optional[int] = None) -> dict:
        self.calls.append(("set_pin", name, action, pin, value, duration_ms))
        return {"ok": True, "port": name, "action": action, "pin": pin, "value": value,
                "dtr": False, "rts": True, "duration_ms": duration_ms}


@pytest.fixture
def stub_cli(monkeypatch):
    _StubClient.instances = []
    monkeypatch.setattr("serialhub.cli.Client", _StubClient)
    return _StubClient


def _run_cli(capsys, argv: list[str]) -> tuple[int, dict]:
    code = cli_main(argv)
    out = capsys.readouterr().out.strip()
    return code, json.loads(out)


def test_cli_pins_without_action_reads_status(stub_cli, capsys) -> None:
    code, env = _run_cli(capsys, ["pins", "t"])
    assert code == 0 and env["ok"] is True
    assert env["data"]["enabled"] is True and env["data"]["dtr"] is True
    assert stub_cli.instances[-1].calls == [("pins", "t")]


def test_cli_pins_set_is_repeatable_and_parses_tokens(stub_cli, capsys) -> None:
    code, env = _run_cli(capsys, ["pins", "t", "--set", "dtr=off", "--set", "rts=1"])
    assert code == 0 and env["ok"] is True
    assert stub_cli.instances[-1].calls == [
        ("set_pin", "t", "set", "dtr", False, None),
        ("set_pin", "t", "set", "rts", True, None),
    ]
    assert len(env["data"]["actions"]) == 2


def test_cli_pins_pulse_and_reset_are_mutually_exclusive(stub_cli, capsys) -> None:
    code, env = _run_cli(capsys, ["pins", "t", "--pulse", "dtr", "--reset"])
    assert code == 1 and env["ok"] is False
    assert "mutually exclusive" in env["error"]
    assert stub_cli.instances[-1].calls == []  # 未发出任何请求


def test_cli_pins_pulse_and_reset_forward_duration(stub_cli, capsys) -> None:
    code, env = _run_cli(capsys, ["pins", "t", "--pulse", "rts", "--duration-ms", "40"])
    assert code == 0 and env["ok"] is True
    assert stub_cli.instances[-1].calls == [("set_pin", "t", "pulse", "rts", None, 40)]

    code2, env2 = _run_cli(capsys, ["pins", "t", "--reset", "--duration-ms", "250"])
    assert code2 == 0 and env2["ok"] is True
    assert stub_cli.instances[-1].calls == [("set_pin", "t", "reset", None, None, 250)]


def test_cli_pins_bad_set_value_is_error_envelope(stub_cli, capsys) -> None:
    code, env = _run_cli(capsys, ["pins", "t", "--set", "dtr=maybe"])
    assert code == 1 and env["ok"] is False
    assert "invalid --set value" in env["error"]
    assert stub_cli.instances[-1].calls == []


def test_cli_pins_propagates_server_error(stub_cli, capsys, monkeypatch) -> None:
    def failing(self, name, action="set", pin=None, value=None, duration_ms=None):
        return {"ok": False, "error": "pins disabled for port 't'"}

    monkeypatch.setattr(_StubClient, "set_pin", failing)
    code, env = _run_cli(capsys, ["pins", "t", "--set", "dtr=on"])
    assert code == 1 and env["ok"] is False
    assert "pins disabled" in env["error"]


@pytest.mark.parametrize("text,expected", [
    ("dtr=on", ("dtr", True)),
    ("DTR=ON", ("dtr", True)),
    ("rts = false", ("rts", False)),
    ("dtr=1", ("dtr", True)),
    ("rts=0", ("rts", False)),
    ("dtr=yes", ("dtr", True)),
    ("rts=no", ("rts", False)),
])
def test_parse_pin_set_accepts_tokens(text: str, expected: tuple) -> None:
    assert _parse_pin_set(text) == expected


@pytest.mark.parametrize("text", ["dtr", "dtr=maybe", "xyz=on", "=on", "dtr=", "", "dtr=on=off"])
def test_parse_pin_set_rejects_bad_values(text: str) -> None:
    with pytest.raises(ValueError):
        _parse_pin_set(text)


# --------------------------------------------------------------------------- #
# MCP
# --------------------------------------------------------------------------- #
def test_mcp_exposes_set_pin_tool() -> None:
    pytest.importorskip("mcp")
    from serialhub import mcp_server

    server = mcp_server.mcp
    manager = getattr(server, "_tool_manager", None)
    tools = getattr(manager, "_tools", None)
    if isinstance(tools, dict) and tools:
        names = set(tools.keys())
    else:
        names = {tool.name for tool in asyncio.run(server.list_tools())}
    assert "set_pin" in names
    assert len(names) == 17
