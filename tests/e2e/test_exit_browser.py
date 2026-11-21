"""浏览器端 Exit 按钮：Playwright(Chromium) 打开 Web UI，确认后 POST /api/shutdown 停掉服务。

只用 ``loop://`` 端口；缺 playwright / chromium 时 skip。
设置环境变量 ``SERIALHUB_SHOTS=<dir>`` 时顺带保存截图（头部 dark/light + 停止横幅）。
"""

from __future__ import annotations

import os
import socket
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import pytest

from tests.integration.simenv import LiveServer

pytestmark = pytest.mark.e2e

sync_api = pytest.importorskip("playwright.sync_api")

CONFIRM_TEXT = ("Stop the serialhub service? All serial ports will be closed and this page "
                "will stop working until the service is started again.")
LOOP = [{"name": "a", "device": "loop://", "autostart": True}]
SHOTS: Optional[Path] = Path(os.environ["SERIALHUB_SHOTS"]) if os.environ.get("SERIALHUB_SHOTS") else None


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
def _page(browser, url: str, theme: str = "dark"):
    ctx = browser.new_context(viewport={"width": 1400, "height": 900}, locale="en-US",
                              color_scheme=theme)
    ctx.add_init_script(f"try {{ localStorage.setItem('serialhub.theme', '{theme}'); }} catch (e) {{}}")
    page = ctx.new_page()
    try:
        page.goto(url)
        page.wait_for_function("document.querySelector('#connPill').dataset.state === 'open'",
                               timeout=10000)
        yield page
    finally:
        ctx.close()


def _port_free(port: int) -> bool:
    """Nobody listens and a restarted serialhub could bind it (uvicorn sets SO_REUSEADDR,
    so TIME_WAIT leftovers of closed client connections do not count)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
        s.listen(1)
        return True
    except OSError:
        return False
    finally:
        s.close()


def _shot(page, name: str, selector: Optional[str] = None) -> None:
    if SHOTS is None:
        return
    SHOTS.mkdir(parents=True, exist_ok=True)
    target = page.locator(selector) if selector else page
    target.screenshot(path=str(SHOTS / name))


def _server_down(srv: LiveServer, timeout: float = 5.0) -> bool:
    thread = srv._thread
    if thread is not None:
        thread.join(timeout)
        if thread.is_alive():
            return False
    deadline = time.time() + 2
    while time.time() < deadline and not _port_free(srv.port):
        time.sleep(0.05)
    return _port_free(srv.port)


def test_exit_click_confirm_stops_service_and_all_tabs(browser):
    with LiveServer(LOOP) as srv, _page(browser, srv.base_url) as page, \
            _page(browser, srv.base_url, theme="light") as other:
        btn = page.locator("#exitBtn")
        btn.wait_for(state="visible")
        other.locator("#exitBtn").wait_for(state="visible")
        _shot(page, "header-dark.png", ".topbar")
        _shot(other, "header-light.png", ".topbar")
        seen: list[str] = []

        def _accept(dialog) -> None:
            seen.append(dialog.message)
            dialog.accept()

        page.once("dialog", _accept)
        btn.click()
        page.locator("#stoppedBanner").wait_for(state="visible", timeout=5000)
        assert seen == [CONFIRM_TEXT]
        assert "Service stopped" in page.locator("#stoppedBanner").inner_text()
        assert btn.is_disabled()
        assert page.locator("#connPill").get_attribute("data-state") == "stopped"
        # the second tab learns from the WS {"type":"shutdown"} broadcast
        other.locator("#stoppedBanner").wait_for(state="visible", timeout=5000)
        assert other.locator("#connPill").get_attribute("data-state") == "stopped"
        assert _server_down(srv)
        _shot(page, "stopped-dark.png")
        _shot(other, "stopped-light.png")
        # no reconnect loop afterwards: the pill stays "stopped"
        page.wait_for_timeout(2500)
        assert page.locator("#connPill").get_attribute("data-state") == "stopped"
        assert page.locator("#stoppedReload").is_visible()


def test_exit_dismiss_does_nothing(browser):
    with LiveServer(LOOP) as srv, _page(browser, srv.base_url) as page:
        page.once("dialog", lambda d: d.dismiss())
        page.locator("#exitBtn").click()
        page.wait_for_timeout(800)
        assert page.locator("#stoppedBanner").is_hidden()
        assert page.locator("#exitBtn").is_enabled()
        assert srv._thread is not None and srv._thread.is_alive()
        assert page.locator("#connPill").get_attribute("data-state") == "open"


def test_exit_button_hidden_when_disabled(browser):
    with LiveServer(LOOP, server={"allow_shutdown": False}) as srv, \
            _page(browser, srv.base_url) as page:
        # the button starts hidden, so first prove the page has consumed /health: the uptime
        # stat leaves its "–" placeholder in the same step that decides the button
        page.wait_for_function(
            "document.querySelector('#statUptime').textContent.trim() !== '\\u2013'",
            timeout=10000)
        assert page.locator("#exitBtn").is_hidden()
