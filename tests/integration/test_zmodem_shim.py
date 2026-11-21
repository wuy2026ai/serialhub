"""zmodem-shim.js 回归：lsz 在 ZFIN 之后不发 “OO” 就退出（0 字节文件时常见），会话仍须正常结束。"""

from __future__ import annotations

import json
import subprocess

import pytest

from .simenv import ZCLIENT_DIR, _ensure_node_modules

pytestmark = pytest.mark.sim


def _run(*args: str) -> dict:
    _ensure_node_modules()
    r = subprocess.run(["node", str(ZCLIENT_DIR / "shim_check.mjs"), *args],
                       capture_output=True, text=True, encoding="utf-8", timeout=20)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def test_missing_oo_then_device_text():
    assert _run() == {"ended": True, "tail": "sz done rc=0\r\n", "error": None}


def test_missing_oo_and_silence_ends_by_timer():
    assert _run("--no-text") == {"ended": True, "tail": "", "error": None}


def test_timer_end_keeps_buffered_byte():
    assert _run("--one-byte") == {"ended": True, "tail": "s", "error": None}


def test_unpatched_zmodem_js_fails():
    """不打补丁时 zmodem.js 0.1.10 确实会抛错（证明上面两条测的是补丁）。"""
    res = _run("--no-shim")
    assert res["ended"] is False
    assert "Only thing after ZFIN" in res["error"]
