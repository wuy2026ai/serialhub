"""Browser: daemon restart (boot id), out-of-order seq dedupe window, pending-tail redraw, zmodem fail-fast.

Only ``loop://`` ports (a send is echoed back as an rx line). Skips without playwright/chromium.
"""

from __future__ import annotations

import time

import httpx
import pytest

import serialhub.server as server_mod
from tests.integration.simenv import LiveServer

pytestmark = pytest.mark.e2e

sync_api = pytest.importorskip("playwright.sync_api")

LOOP = [{"name": "a", "device": "loop://", "autostart": True}]
SEP = "--- service restarted ---"


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


def _send(srv, text: str, eol: str = "\r\n") -> None:
    r = httpx.post(f"{srv.base_url}/api/ports/a/send", json={"data": text, "eol": eol}, timeout=5)
    assert r.json()["ok"]


def _wait_text(page, needle: str, timeout: int = 8000) -> None:
    page.wait_for_function(
        "n => { const e = document.querySelector('.xterm-rows'); return !!e && e.innerText.includes(n); }",
        arg=needle, timeout=timeout)


def _buffer_text(page) -> str:
    return page.evaluate(
        "() => { const b = state.runtimes.get('a').term.buffer.active; const o = [];"
        " for (let i = 0; i < b.length; i++) o.push(b.getLine(i).translateToString(true));"
        " return o.join('\\n'); }")


def _rows(page) -> list[str]:
    return [r.rstrip() for r in _buffer_text(page).split("\n")]


def _last(page) -> str:
    rows = [r for r in _rows(page) if r.strip()]
    return rows[-1] if rows else ""


def _wait_last(page, pred, timeout: float = 2.0) -> None:
    end = time.time() + timeout
    while True:
        last = _last(page)
        if pred(last):
            return
        if time.time() > end:
            raise AssertionError(f"last row {last!r}; rows={[r for r in _rows(page) if r.strip()]}")
        time.sleep(0.02)


def _open_page(browser, srv):
    ctx = browser.new_context(viewport={"width": 1400, "height": 900}, locale="en-US")
    page = ctx.new_page()
    page.goto(srv.base_url)
    page.wait_for_function("typeof state !== 'undefined' && state.runtimes.get('a') && state.runtimes.get('a').term",
                           timeout=10000)
    page.wait_for_function("document.querySelector('#connPill').dataset.state === 'open'", timeout=5000)
    return ctx, page


# ---- U1: daemon restart on the same port ---------------------------------- #
def test_restart_same_port_new_lines_appear_and_separator_once(browser):
    srv = LiveServer(LOOP).start()
    srv2 = None
    ctx = None
    old_boot = server_mod.BOOT_ID   # restored in finally (moved for the "restarted" daemon)
    try:
        ctx, page = _open_page(browser, srv)
        _send(srv, "OLDLINE")
        _wait_text(page, "OLDLINE")
        # the daemon must advertise its boot id in the hello (D4); the page records it
        page.wait_for_function("state.boot != null", timeout=5000)

        srv.stop()   # daemon dies; its ring buffer (and the seq counter) dies with it
        # A real restart is a new process => a new boot id. Both LiveServers share
        # this pytest process, so move the module global by hand (it is read at
        # request time) to give the new daemon its own identity.
        server_mod.BOOT_ID = old_boot + "-r2"
        srv2 = LiveServer(LOOP)
        srv2.port = srv.port          # same port, same config: a fresh, EMPTY buffer
        srv2.base_url = f"http://127.0.0.1:{srv2.port}"
        srv2.start()

        _wait_text(page, SEP, timeout=15000)   # reconnect shows the restart separator
        _send(srv2, "NEWLINE")                 # seq restarts from 0: dropped without the reset
        _wait_text(page, "NEWLINE", timeout=5000)
        page.wait_for_timeout(400)             # a late second hello must not duplicate the marker
        text = _buffer_text(page)
        assert text.count(SEP) == 1, text
        assert text.index("OLDLINE") < text.index(SEP) < text.index("NEWLINE"), text
    finally:
        server_mod.BOOT_ID = old_boot
        if ctx:
            ctx.close()
        if srv2:
            srv2.stop()
        srv.stop()


# ---- U2: dedupe window instead of a single high-water mark ----------------- #
def test_late_lower_seq_still_rendered_and_true_duplicate_dropped(browser):
    with LiveServer(LOOP) as srv:
        ctx, page = _open_page(browser, srv)
        try:
            out = page.evaluate("""() => {
              const rt = state.runtimes.get('a');
              rt.lastSeq = null; rt.seenSeqs = new Set(); rt.seenOrder = []; rt.lines = [];
              onLine('a', { seq: 40, port: 'a', ts_ms: Date.now(), text: 'RX40' });
              // tx line got its seq just before the rx line but is broadcast just after it
              onLine('a', { seq: 39, port: 'a', ts_ms: Date.now(), text: 'TX39', kind: 'tx' });
              onLine('a', { seq: 40, port: 'a', ts_ms: Date.now(), text: 'RX40DUP' });  // true duplicate
              return { lines: rt.lines.map((l) => l.seq + ':' + l.text), lastSeq: rt.lastSeq };
            }""")
            assert out["lines"] == ["40:RX40", "39:TX39"], out
            assert out["lastSeq"] == 40, out
        finally:
            ctx.close()


# ---- U3: the pending tail is redrawn on resume and on filter change -------- #
def test_pending_tail_redrawn_on_resume_and_filter_change(browser):
    with LiveServer(LOOP) as srv:
        ctx, page = _open_page(browser, srv)
        try:
            # an unterminated tail arrives while paused: it is remembered but not drawn.
            # (the send's own tx line is a complete row "▸ <ts> <seq> TAIL"; the tail
            # row is the bare prompt text with no prefix, so match it exactly)
            page.click("#btnPause")
            _send(srv, "TAIL", eol="none")
            page.wait_for_function("state.runtimes.get('a').pend.want === 'TAIL'", timeout=5000)
            page.click("#btnPause")   # resume
            _wait_last(page, lambda r: r == "TAIL")

            # a filter that matches nothing must take the unfiltered tail off the row
            page.fill("#filterInput", "nomatch")
            _wait_last(page, lambda r: r != "TAIL")

            # clearing the filter redraws the tail from the remembered value
            page.fill("#filterInput", "")
            _wait_last(page, lambda r: r == "TAIL")
        finally:
            ctx.close()


# ---- U4: zmodem lazy load fails fast, local only, with a clear toast ------- #
def test_zmodem_load_fails_fast_local_only_with_toast(browser):
    with LiveServer(LOOP) as srv:
        ctx = browser.new_context(viewport={"width": 1400, "height": 900}, locale="en-US")
        stalled: list = []
        ctx.route("**/static/vendor/zmodem.min.js", lambda route: stalled.append(route))  # never settles
        cdn: list[str] = []
        ctx.route("**/cdn.jsdelivr.net/**", lambda route: (cdn.append(route.request.url), route.abort()))
        page = ctx.new_page()
        try:
            page.goto(srv.base_url)
            page.wait_for_function("typeof state !== 'undefined' && state.runtimes.get('a') && state.runtimes.get('a').term",
                                   timeout=10000)
            t0 = time.time()
            page.click("#btnRz")
            page.wait_for_function(
                "() => Array.from(document.querySelectorAll('.toast .t-msg')).some((e) => e.textContent.includes('zmodem.js'))",
                timeout=4000)
            assert time.time() - t0 <= 3.0     # fail fast: well under the daemon's 5 s claim window
            assert stalled, "the stalled local vendor script was never requested"
            assert not cdn, "a CDN fallback was attempted for the zmodem scripts"
        finally:
            ctx.close()
