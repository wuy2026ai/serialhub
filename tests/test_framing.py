"""断帧策略与增量解码测试（§7）——只用 ``loop://`` 虚拟口，禁止真实串口。

覆盖 T1 冻结契约：

- ``PortConfig.framing`` ∈ ``{"line", "idle"}``，默认 ``"line"``。
- ``"line"``：只在 ``\\n``/``\\r``/``\\r\\n`` 处产出整行；无结束符的残行一直留在缓冲，
  直到该口断开/被 ``close`` 时才作为一行吐出（吐出前去掉结尾单个 ``\\r``）。
- ``"idle"``：保留 ``flush_ms`` 时间片断帧，且仅当 ``flush_ms > 0`` 时生效。
- 增量解码：跨两次 write（间隔 > ``flush_ms``）的多字节字符不得被截断成 U+FFFD。
- ``config.to_dict()`` 往返、非法 framing 的报错路径、``hub.reload`` 改动 framing 重启该口。
"""

from __future__ import annotations

import time

import pytest

from serialhub.config import Config

#: 大于下面用到的任何 3×flush_ms（最大 3×30=90ms），也远大于一个读循环轮次。
SETTLE = 0.35
#: 等待断言的轮询上限（秒）。
POLL_TIMEOUT = 3.0


def _port(**over):
    """单个 loop:// 测试口的基线配置，可用关键字覆盖任意字段。"""
    base = {
        "name": "b",
        "device": "loop://",
        "autostart": True,
        "buffer_lines": 64,
        "buffer_bytes": 1 << 20,
        "reconnect_ms": 200,
        "reconnect_max_ms": 500,
        "flush_ms": 20,
    }
    base.update(over)
    return base


def _rx_texts(hub, name: str = "b") -> list[str]:
    return [line["text"] for line in hub.read(name, kinds={"rx"})["lines"]]


def _wait_rx(hub, name: str = "b", *, count: int = 1, timeout: float = POLL_TIMEOUT) -> list[str]:
    """轮询到至少 ``count`` 行 rx 为止；超时返回当前结果（由调用方断言）。"""
    deadline = time.time() + timeout
    texts = _rx_texts(hub, name)
    while len(texts) < count and time.time() < deadline:
        time.sleep(0.02)
        texts = _rx_texts(hub, name)
    return texts


def _write_and_wait(hub, data: bytes, *, count: int = 1, name: str = "b") -> list[str]:
    """写原始字节到 loop:// 口（原样回读），等至少 ``count`` 行 rx 后返回全部文本。"""
    hub.write_bytes(name, data)
    return _wait_rx(hub, name, count=count)


# ---- framing="line"（默认）：只按 EOL 分行 -------------------------------- #
def test_line_framing_holds_residual_until_eol(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="line", flush_ms=20)]))
    assert hub.is_open("b") is True

    hub.write_bytes("b", b"abc")
    time.sleep(SETTLE)  # ≈ 17×flush_ms：残行不得被时间片吐出
    assert _rx_texts(hub) == []

    # 补齐换行后才成一行，且两段拼在一起（不被时间片切成 'abc' / 'def'）
    assert _write_and_wait(hub, b"def\r\n") == ["abcdef"]


def test_line_framing_is_the_default(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port()]))  # 不写 framing
    assert hub.config.port("b").framing == "line"

    hub.write_bytes("b", b"partial")
    time.sleep(SETTLE)
    assert _rx_texts(hub) == []


def test_line_framing_splits_only_at_eol(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="line")]))

    # 两条完整行 + 一条以孤立 \r 结尾的残行（可能在等 \n）
    assert _write_and_wait(hub, b"one\r\ntwo\nthree\r", count=2) == ["one", "two"]
    time.sleep(SETTLE)
    assert _rx_texts(hub) == ["one", "two"]

    assert _write_and_wait(hub, b"\n", count=3) == ["one", "two", "three"]


def test_line_framing_flushes_residual_on_close(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="line")]))

    hub.write_bytes("b", b"xyz\r")  # 结尾孤立 \r：等可能的 \n，暂不吐出
    time.sleep(SETTLE)
    assert _rx_texts(hub) == []

    hub.close("b")
    assert _rx_texts(hub) == ["xyz"]  # close 时才吐出，且去掉结尾单个 \r


def test_unvalidated_framing_value_falls_back_to_line_on_close(hub_factory, make_config) -> None:
    """绕过 Config 校验直接构造的非法 framing 不得在 close 时静默丢数据。

    只有显式的 ``"idle"`` 才跳过 close 时的残行吐出；其它取值一律按 ``"line"``
    兜底（库使用者可能直接改 dataclass 后再 ``hub.add_port``）。
    """
    cfg = make_config(ports=[_port(framing="line")])
    cfg.port("b").framing = "bogus"  # 模拟绕过 Config.from_dict / _validate_port
    hub = hub_factory(cfg)

    hub.write_bytes("b", b"tail-bogus")
    time.sleep(SETTLE)
    assert _rx_texts(hub) == []  # 非法值不等于 idle，不按时间片吐出

    hub.close("b")
    assert _rx_texts(hub) == ["tail-bogus"]  # 兜底按 line 处理：残行不丢


def test_line_framing_flushes_residual_on_disconnect(hub_factory, make_config, monkeypatch) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="line")]))

    hub.write_bytes("b", b"tail")
    time.sleep(SETTLE)
    assert _rx_texts(hub) == []

    # 让下一次 read 抛错 = 断线：读循环须先把残行吐出，再重连（重连会清缓冲）
    def boom(*_args, **_kwargs):
        raise OSError("simulated disconnect")

    monkeypatch.setattr(hub._workers["b"]._serial, "read", boom)
    assert _wait_rx(hub) == ["tail"]


# ---- framing="idle"：保留时间片断帧 --------------------------------------- #
def test_idle_framing_flushes_residual_after_flush_ms(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="idle", flush_ms=30)]))

    hub.write_bytes("b", b"partial")
    assert _wait_rx(hub) == ["partial"]


def test_idle_framing_still_splits_at_eol(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="idle", flush_ms=200)]))

    assert _write_and_wait(hub, b"a\r\nb\n", count=2) == ["a", "b"]


@pytest.mark.parametrize("framing", ["line", "idle"])
def test_flush_ms_zero_never_flushes_residual(hub_factory, make_config, framing) -> None:
    """flush_ms=0 曾等价于“每轮都吐”；现在只有 idle 且 flush_ms>0 才走时间片。"""
    hub = hub_factory(make_config(ports=[_port(framing=framing, flush_ms=0)]))

    hub.write_bytes("b", b"abc")
    time.sleep(SETTLE)
    assert _rx_texts(hub) == []

    assert _write_and_wait(hub, b"def\r\n") == ["abcdef"]


# ---- 增量解码：多字节字符跨产出边界不产生 U+FFFD -------------------------- #
def test_incremental_decode_keeps_split_multibyte_char_intact(hub_factory, make_config) -> None:
    """idle 时间片切在 UTF-8 字符中间：字符完整留给下一行，绝不出现 U+FFFD。"""
    hub = hub_factory(make_config(ports=[_port(framing="idle", flush_ms=20)]))

    # "中" = e4 b8 ad；首段只送出 e4 b8（不完整）
    assert _write_and_wait(hub, b"A\xe4\xb8") == ["A"]
    # 补上剩余字节：跨两个产出边界的字符必须拼成完整的「中!」
    assert _write_and_wait(hub, b"\xad!\r\n", count=2) == ["A", "中!"]
    assert "\ufffd" not in "".join(_rx_texts(hub))


def test_split_multibyte_char_yields_no_placeholder_line(hub_factory, make_config) -> None:
    """首段整段都是半个字符时不吐空行：字节留给下一行（Lead 复现的 U+FFFD 场景）。"""
    hub = hub_factory(make_config(ports=[_port(framing="idle", flush_ms=20)]))

    hub.write_bytes("b", b"\xe4\xb8")
    time.sleep(SETTLE)  # 时间片至少切一次
    assert _rx_texts(hub) == []

    assert _write_and_wait(hub, b"\xad!\r\n") == ["中!"]


def test_line_framing_multibyte_across_reads_is_intact(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="line")]))

    hub.write_bytes("b", b"deb\xe4\xb8")
    time.sleep(SETTLE)
    assert _rx_texts(hub) == []

    assert _write_and_wait(hub, b"\xad\r\n") == ["deb中"]


# ---- 配置：校验 / defaults 合并 / to_dict 往返 ---------------------------- #
def test_framing_default_is_line_in_config_and_to_dict() -> None:
    cfg = Config.from_dict({"ports": [{"name": "a", "device": "loop://"}]})
    assert cfg.port("a").framing == "line"
    assert cfg.to_dict()["ports"][0]["framing"] == "line"


def test_framing_idle_from_defaults_and_roundtrip() -> None:
    cfg = Config.from_dict({
        "defaults": {"framing": "idle"},
        "ports": [{"name": "a", "device": "loop://"}],
    })
    assert cfg.port("a").framing == "idle"
    assert cfg.to_dict()["ports"][0]["framing"] == "idle"


def test_framing_invalid_value_names_field_path() -> None:
    with pytest.raises(ValueError) as excinfo:
        Config.from_dict({"ports": [{"name": "a", "device": "loop://", "framing": "x"}]})
    assert str(excinfo.value) == "ports[a].framing must be one of ['idle', 'line'], got 'x'"


# ---- reload：framing 是 IO 字段，改动即重启该口 --------------------------- #
def test_reload_framing_change_restarts_port(hub_factory, make_config) -> None:
    hub = hub_factory(make_config(ports=[_port(framing="line", flush_ms=20)]))
    assert hub.is_open("b") is True

    result = hub.reload(make_config(ports=[_port(framing="idle", flush_ms=20)]))
    assert result["restarted"] == ["b"]
    assert result["added"] == [] and result["removed"] == []
    assert hub.config.port("b").framing == "idle"

    # 新 framing 确实生效：残行被时间片吐出
    hub.write_bytes("b", b"partial")
    assert _wait_rx(hub) == ["partial"]

    # framing 未变 → 不重启该口
    again = hub.reload(make_config(ports=[_port(framing="idle", flush_ms=20)]))
    assert again == {"added": [], "removed": [], "restarted": [], "updated": []}
