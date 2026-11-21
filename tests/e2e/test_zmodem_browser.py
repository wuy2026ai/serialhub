"""浏览器端 ZMODEM：Playwright(Chromium) 打开 Web UI，经 /ws 用 zmodem.js 与 mock 设备（真实 lrzsz）收发文件。

缺 playwright / chromium / socat / lrzsz 时 skip。
"""

from __future__ import annotations

import os
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Iterator

import httpx
import pytest

from tests.integration.simenv import LiveServer, MockDevice, PtyPair, need_tools

pytestmark = pytest.mark.e2e

sync_api = pytest.importorskip("playwright.sync_api")

PORT = "a"


@pytest.fixture(autouse=True)
def _tools():
    need_tools("socat", "lrz", "lsz")


@pytest.fixture(scope="module")
def browser():
    try:
        pw = sync_api.sync_playwright().start()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"playwright unavailable: {exc}")
    try:
        b = pw.chromium.launch()
    except Exception as exc:  # noqa: BLE001 - 没装 chromium（playwright install chromium）
        pw.stop()
        pytest.skip(f"chromium unavailable: {exc}")
    try:
        yield b
    finally:
        b.close()
        pw.stop()


@contextmanager
def _sim(tmp_path: Path) -> Iterator[dict[str, Any]]:
    files, recv = tmp_path / "files", tmp_path / "recv"
    files.mkdir()
    recv.mkdir()
    with ExitStack() as st:
        pair = st.enter_context(PtyPair())
        dev = st.enter_context(MockDevice(pair.dev_path, PORT, files, recv, interval=1.0))
        srv = st.enter_context(LiveServer([{"name": PORT, "device": pair.host_path,
                                             "autostart": True}]))
        r = httpx.get(f"{srv.base_url}/api/ports/{PORT}/wait",
                      params={"regex": "^PKT name=", "timeout_ms": 5000}, timeout=10).json()
        assert r["ok"] and r["matched"], "device not streaming"
        yield {"srv": srv, "files": files, "recv": recv, "dev": dev}


@contextmanager
def _page(browser, url: str):
    ctx = browser.new_context(accept_downloads=True, viewport={"width": 1400, "height": 1000},
                              locale="en-US")
    page = ctx.new_page()
    try:
        page.goto(url)
        page.wait_for_function("document.querySelector('.xterm-rows') && "
                               "document.querySelector('.xterm-rows').innerText.includes('PKT name=')",
                               timeout=10000)
        yield page
    finally:
        ctx.close()


def _rows(page) -> str:
    return page.eval_on_selector(".pane:not([hidden]) .xterm-rows", "e => e.innerText")


def _wait_rows(page, needle: str, timeout: float = 10.0) -> None:
    page.wait_for_function(
        "n => { const e = document.querySelector('.pane:not([hidden]) .xterm-rows');"
        " return !!e && e.innerText.includes(n); }",
        arg=needle, timeout=int(timeout * 1000))


def _dialog(page):
    return page.locator(".pane:not([hidden]) [data-zm]")


def _wait_dialog_closed(page, timeout: float = 15.0) -> None:
    _dialog(page).wait_for(state="hidden", timeout=int(timeout * 1000))


def _download(page, path: str, timeout: float = 30.0) -> list[tuple[str, bytes]]:
    """点 Download (sz)…，输入设备路径，点 Receive，返回每个下载 (文件名, 内容)。"""
    got: list[tuple[str, bytes]] = []
    page.on("download", lambda d: got.append((d.suggested_filename, Path(d.path()).read_bytes())))
    page.click("#btnSz")
    page.fill(".pane:not([hidden]) [data-zm-path]", path)
    page.press(".pane:not([hidden]) [data-zm-path]", "Enter")
    dlg = _dialog(page)
    page.wait_for_selector(".pane:not([hidden]) [data-zm][data-phase='offer'][data-direction='download']")
    assert "Device is sending files" in dlg.inner_text()
    dlg.locator("[data-zm-primary]").click()
    _wait_dialog_closed(page, timeout)
    return got


def _upload(page, files: list[Path], timeout: float = 30.0) -> None:
    page.click("#btnRz")
    page.wait_for_selector(".pane:not([hidden]) [data-zm][data-phase='offer'][data-direction='upload']")
    dlg = _dialog(page)
    assert "Device is waiting for files" in dlg.inner_text()
    with page.expect_file_chooser() as fc:
        dlg.locator("[data-zm-primary]").click()
    fc.value.set_files([str(f) for f in files])
    _wait_dialog_closed(page, timeout)


def _wait_dev(dev, needle: str, count: int, timeout: float = 10.0) -> None:
    """等设备 stdout 中 ``needle`` 出现 ``count`` 次（lsz/lrz 已退出、设备回到命令行）。

    浏览器端会话结束时 lsz 可能还没退出，此时发出的下一条命令会被 lsz 读走，故连续传输之间先等这一步。
    """
    deadline = time.time() + timeout
    while dev.output().count(needle) < count:
        assert time.time() < deadline, f"device output: {dev.output()!r}"
        time.sleep(0.05)


def _wait_file(path: Path, data: bytes, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and path.read_bytes() == data:
            return
        time.sleep(0.05)
    raise AssertionError(f"{path} never matched ({path.stat().st_size if path.exists() else 'missing'})")


def test_browser_download_and_upload(browser, tmp_path):
    with _sim(tmp_path) as env, _page(browser, env["srv"].base_url) as page:
        blob = bytearray(os.urandom(300_000))
        blob[150_000:150_006] = b"**\x18B00"  # 负载中的假起始头
        (env["files"] / "blob.bin").write_bytes(bytes(blob))

        got = _download(page, "blob.bin")
        assert [n for n, _ in got] == ["blob.bin"]
        assert got[0][1] == bytes(blob)
        _wait_rows(page, "sz done rc=0")  # tail_b64：OO 之后设备打印的行回到终端
        rows = _rows(page)
        assert "[zmodem] start download" in rows
        assert "[zmodem] end reason=done" in rows

        up = tmp_path / "up.bin"
        up_data = os.urandom(100 * 1024)
        up.write_bytes(up_data)
        _upload(page, [up])
        _wait_file(env["recv"] / "up.bin", up_data)
        _wait_rows(page, "rz done rc=0")
        rows = _rows(page)
        assert "[zmodem] start upload" in rows
        assert rows.count("[zmodem] end reason=done") >= 1
        assert "�" not in rows  # 负载字节没有进终端


def test_browser_empty_and_unicode_names(browser, tmp_path):
    with _sim(tmp_path) as env, _page(browser, env["srv"].base_url) as page:
        name = "测试 file.bin"
        data = os.urandom(5000)
        (env["files"] / name).write_bytes(data)
        (env["files"] / "empty.bin").write_bytes(b"")

        got = _download(page, name)
        assert got == [(name, data)]
        _wait_dev(env["dev"], "sz done rc=0", 1)
        got = _download(page, "empty.bin")
        assert got == [("empty.bin", b"")]
        _wait_dev(env["dev"], "sz done rc=0", 2)

        up_dir = tmp_path / "up"
        up_dir.mkdir()
        (up_dir / name).write_bytes(data)
        (up_dir / "empty.bin").write_bytes(b"")
        _upload(page, [up_dir / name, up_dir / "empty.bin"])
        _wait_file(env["recv"] / name, data)
        _wait_file(env["recv"] / "empty.bin", b"")
        assert "�" not in _rows(page)


def test_browser_cancel_second_tab_and_reload(browser, tmp_path):
    with _sim(tmp_path) as env, _page(browser, env["srv"].base_url) as page, \
            _page(browser, env["srv"].base_url) as other:
        (env["files"] / "big.bin").write_bytes(os.urandom(16 << 20))

        # 1) 第二个标签页：本页认领后，另一页点 Receive → “被另一个浏览器占用”，按钮禁用
        page.click("#btnSz")
        page.fill(".pane:not([hidden]) [data-zm-path]", "big.bin")
        page.press(".pane:not([hidden]) [data-zm-path]", "Enter")
        page.wait_for_selector(".pane:not([hidden]) [data-zm][data-phase='offer']")
        other.wait_for_selector(".pane:not([hidden]) [data-zm][data-phase='offer']")
        _dialog(page).locator("[data-zm-primary]").click()
        page.wait_for_function(
            "() => [...document.querySelectorAll('.pane:not([hidden]) .zm-file')]"
            ".some(e => Number(e.dataset.bytes) > 65536)", timeout=15000)
        _dialog(other).locator("[data-zm-primary]").click()
        other.wait_for_selector(".pane:not([hidden]) [data-zm][data-phase='inuse']")
        assert "in use by another browser" in _dialog(other).inner_text()
        assert _dialog(other).locator("[data-zm-primary]").is_disabled()
        assert _dialog(other).locator("[data-zm-cancel]").is_disabled()

        # 2) 传输中点 Cancel → zmodem_end abort:true；两页的对话框都关闭，文本恢复
        _dialog(page).locator("[data-zm-cancel]").click()
        _wait_dialog_closed(page)
        _wait_dialog_closed(other)
        _wait_rows(page, "[zmodem] end reason=abort")
        deadline = time.time() + 10
        while "sz done rc=" not in env["dev"].output() and time.time() < deadline:
            time.sleep(0.1)
        assert "sz done rc=0" not in env["dev"].output()
        _wait_line_after_now(env["srv"])

        # 3) 传输中刷新页面 → 断开中止；刷新后的页面干净，文本照常
        page.click("#btnSz")
        page.fill(".pane:not([hidden]) [data-zm-path]", "big.bin")
        page.press(".pane:not([hidden]) [data-zm-path]", "Enter")
        page.wait_for_selector(".pane:not([hidden]) [data-zm][data-phase='offer']")
        _dialog(page).locator("[data-zm-primary]").click()
        page.wait_for_function(
            "() => [...document.querySelectorAll('.pane:not([hidden]) .zm-file')]"
            ".some(e => Number(e.dataset.bytes) > 65536)", timeout=15000)
        page.reload()
        _wait_rows(page, "PKT name=", 15)
        _wait_line_after_now(env["srv"])
        assert _dialog(page).is_hidden()
        _wait_rows(other, "[zmodem] end reason=disconnect")
        assert _dialog(other).is_hidden()
        assert "�" not in _rows(page) + _rows(other)


def _wait_line_after_now(srv, timeout: float = 10.0) -> None:
    """排空结束后设备文本恢复：等到最后一个 end 标记之后出现新的 PKT 行。"""
    lines = httpx.get(f"{srv.base_url}/api/ports/{PORT}/read", timeout=5).json()["lines"]
    end_seq = max(ln["seq"] for ln in lines
                  if ln["kind"] == "mark" and ln["text"].startswith("[zmodem] end"))
    r = httpx.get(f"{srv.base_url}/api/ports/{PORT}/wait",
                  params={"regex": "^PKT name=", "timeout_ms": int(timeout * 1000),
                          "since_seq": end_seq + 1},
                  timeout=timeout + 5).json()
    assert r["ok"] and r["matched"], "device text did not resume"
