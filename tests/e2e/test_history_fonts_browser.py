"""Browser: history on (re)load without duplicates; no dependency on the Google Fonts host.

Only ``loop://`` ports (a send is echoed back as an rx line). Skips without playwright/chromium.
"""

from __future__ import annotations

import re
import time

import httpx
import pytest

from tests.integration.simenv import LiveServer

pytestmark = pytest.mark.e2e

sync_api = pytest.importorskip("playwright.sync_api")

LOOP = [{"name": "a", "device": "loop://", "autostart": True}]


@pytest.fixture(scope="module")
def browser():
    try:
        pw = sync_api.sync_playwright().start()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"playwright unavailable: {exc}")
    try:
        b = pw.chromium.launch()
    except Exception as exc:  # noqa: BLE001
        pw.stop()
        pytest.skip(f"chromium unavailable: {exc}")
    try:
        yield b
    finally:
        b.close()
        pw.stop()


def _send(srv, text: str) -> None:
    r = httpx.post(f"{srv.base_url}/api/ports/a/send", json={"data": text, "eol": "\r\n"}, timeout=5)
    assert r.json()["ok"]


def _wait_rows(page, needle: str, timeout: int = 8000) -> None:
    page.wait_for_function(
        "n => { const e = document.querySelector('.xterm-rows'); return !!e && e.innerText.includes(n); }",
        arg=needle, timeout=timeout)


def _buffer_text(page) -> str:
    return page.evaluate(
        "() => { const b = state.runtimes.get('a').term.buffer.active; const o = [];"
        " for (let i = 0; i < b.length; i++) o.push(b.getLine(i).translateToString(true));"
        " return o.join('\\n'); }")


def test_reload_shows_history_exactly_once(browser):
    with LiveServer(LOOP) as srv:
        for i in range(5):
            _send(srv, f"HIST{i}")
        # wait until the daemon has them all in its ring buffer
        deadline = time.time() + 5
        while time.time() < deadline:
            r = httpx.get(f"{srv.base_url}/api/ports/a/read", params={"filter": "^HIST"}, timeout=5).json()
            if r.get("count", 0) >= 5:
                break
            time.sleep(0.05)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900}, locale="en-US")
        page = ctx.new_page()
        try:
            page.goto(srv.base_url)
            _wait_rows(page, "HIST4")           # history shown without any new device output
            for i in range(5):
                _send(srv, f"LIVE{i}")
            _wait_rows(page, "LIVE4")
            page.reload()
            _wait_rows(page, "LIVE4")
            _send(srv, "AFTER")
            _wait_rows(page, "AFTER")
            page.wait_for_timeout(300)
            text = _buffer_text(page)
            # loop:// shows each send twice (tx + echoed rx), each with its own seq
            seqs = [int(m) for m in re.findall(r"^(?:\S+ )?\d\d:\d\d:\d\d\.\d+ +(\d+) ", text, re.M)]
            assert len(seqs) == len(set(seqs)) == 22, (seqs, text)
            assert seqs == sorted(seqs)
            for tag in [f"HIST{i}" for i in range(5)] + [f"LIVE{i}" for i in range(5)] + ["AFTER"]:
                assert text.count(tag) == 2, (tag, text)
            assert text.index("HIST0") < text.index("LIVE0") < text.index("AFTER")
        finally:
            ctx.close()


def test_renders_fast_with_fonts_host_blocked(browser):
    with LiveServer(LOOP) as srv:
        ctx = browser.new_context(viewport={"width": 1400, "height": 900}, locale="en-US")
        ctx.route("**/fonts.googleapis.com/**", lambda r: r.abort())
        ctx.route("**/fonts.gstatic.com/**", lambda r: r.abort())
        page = ctx.new_page()
        external: list[str] = []
        page.on("request", lambda r: external.append(r.url) if not r.url.startswith(srv.base_url)
                and not r.url.startswith("data:") else None)
        try:
            t0 = time.time()
            page.goto(srv.base_url)
            page.wait_for_function("document.querySelector('.xterm-rows')", timeout=2000)
            assert time.time() - t0 < 2.0
            page.wait_for_function("document.querySelector('#connPill').dataset.state === 'open'",
                                   timeout=2000)
            assert not [u for u in external if "fonts.g" in u], external
        finally:
            ctx.close()
