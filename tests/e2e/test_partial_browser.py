"""Browser: the unterminated tail (prompt without EOL) is shown live and replaced by the real line."""

from __future__ import annotations

import os
import time

import pytest

from tests.integration.simenv import LiveServer, PtyPair, need_tools

pytestmark = pytest.mark.e2e

sync_api = pytest.importorskip("playwright.sync_api")

PORT = "p"


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


def _rows(page) -> list[str]:
    return page.evaluate(
        "() => { const b = state.runtimes.get('p').term.buffer.active; const o = [];"
        " for (let i = 0; i < b.length; i++) o.push(b.getLine(i).translateToString(true));"
        " return o; }")


def _last(page) -> str:
    rows = [r.rstrip() for r in _rows(page) if r.strip()]
    return rows[-1] if rows else ""


def _wait_last(page, pred, timeout: float = 1.0) -> str:
    end = time.time() + timeout
    while True:
        last = _last(page)
        if pred(last):
            return last
        if time.time() > end:
            raise AssertionError(f"last row {last!r}; rows={[r for r in _rows(page) if r.strip()]}")
        time.sleep(0.02)


def test_pending_prompt_shown_and_replaced(browser):
    need_tools("socat")
    with PtyPair() as pair, LiveServer([{"name": PORT, "device": pair.host_path, "autostart": True}]) as srv:
        fd = os.open(pair.dev_path, os.O_RDWR | os.O_NOCTTY)
        try:
            ctx = browser.new_context(viewport={"width": 1400, "height": 900}, locale="en-US")
            page = ctx.new_page()
            try:
                page.goto(srv.base_url)
                page.wait_for_function("typeof state !== 'undefined' && state.runtimes.get('p') && state.runtimes.get('p').term",
                                       timeout=10000)
                time.sleep(0.5)
                os.write(fd, b"=> ")
                _wait_last(page, lambda r: r.endswith("=>"))
                os.write(fd, b"help\r\n")
                _wait_last(page, lambda r: r.endswith("=> help"))
                time.sleep(0.3)
                text = [r.rstrip() for r in _rows(page) if r.strip()]
                assert sum(1 for r in text if r.endswith("=> help")) == 1, text
                assert not any(r.endswith("=>") for r in text), text
                assert text[-1].endswith("=> help"), text
                os.write(fd, b"=> ")
                _wait_last(page, lambda r: r.endswith("=>"))
                page.reload()
                page.wait_for_function("typeof state !== 'undefined' && state.runtimes.get('p') && state.runtimes.get('p').term",
                                       timeout=10000)
                _wait_last(page, lambda r: r.endswith("=>"), timeout=5.0)
                # growth in place: no duplicate rows
                os.write(fd, b"ab")
                _wait_last(page, lambda r: r.endswith("=> ab"))
                text = [r.rstrip() for r in _rows(page) if r.strip()]
                assert sum(1 for r in text if "=>" in r and r.endswith("=> ab")) == 1, text
                assert not any(r.endswith("=>") for r in text), text
            finally:
                ctx.close()
        finally:
            os.close(fd)
