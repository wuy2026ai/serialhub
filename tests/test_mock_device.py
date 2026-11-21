"""``tools/mock_device.py``: discarding a killed rz's in-flight bytes (no serial port needed)."""

from __future__ import annotations

import importlib.util
import time
from pathlib import Path

MOCK = Path(__file__).resolve().parents[1] / "tools" / "mock_device.py"


def _mod():
    spec = importlib.util.spec_from_file_location("mock_device", MOCK)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _TimedSerial:
    """``read()`` hands out ``(at_seconds, bytes)`` chunks once their time has come."""

    def __init__(self, script: list[tuple[float, bytes]]) -> None:
        self.t0 = time.monotonic()
        self.script = list(script)

    def read(self, _n: int) -> bytes:
        if self.script and time.monotonic() - self.t0 >= self.script[0][0]:
            return self.script.pop(0)[1]
        time.sleep(0.005)
        return b""


def _device(script):
    mod = _mod()
    return mod.MockDevice(_TimedSerial(script), "a", 0, ".", ".")


def test_late_in_flight_data_is_still_discarded(capsys) -> None:
    """In-flight bytes that start > 200 ms after rz died are still in-flight, not commands."""
    dev = _device([(0.4, b"\x18Ijunk\x18h\r\x8a"), (0.5, b"more\x18k")])
    dev.DISCARD_QUIET_S = 0.5
    dev._discard_in_flight()
    assert "rz killed discarded=16" in capsys.readouterr().out
    assert dev.carry == b""


def test_in_flight_burst_after_a_long_gap_is_discarded(capsys) -> None:
    """The host may resume writing well over 1.5 s later (paced writes, a loaded machine):
    everything up to the client's lone cleanup terminator is in-flight data."""
    dev = _device([(0.05, b"\x18Iabc"), (1.9, b"\x95\xca\x18\xd3\r\x8a"), (2.2, b"\r")])
    dev._discard_in_flight()
    assert "rz killed discarded=11" in capsys.readouterr().out
    assert dev.carry == b"\r"


def test_lone_terminator_is_kept_for_the_shell(capsys) -> None:
    """No in-flight bytes at all: the client's single cleanup terminator is not swallowed."""
    dev = _device([(0.5, b"\r")])
    t0 = time.monotonic()
    dev._discard_in_flight()
    assert time.monotonic() - t0 < 1.0
    assert "rz killed discarded=0" in capsys.readouterr().out
    assert dev.carry == b"\r"


def test_terminator_after_in_flight_window(capsys) -> None:
    dev = _device([(0.05, b"\x18Iabc"), (0.6, b"\r")])
    dev._discard_in_flight()
    assert "rz killed discarded=5" in capsys.readouterr().out
    assert dev.carry == b"\r"


def test_discard_bounded_without_terminator(capsys) -> None:
    """No cleanup terminator (``zmodem_cleanup_eol: none``): stop after a quiet spell."""
    dev = _device([(0.05, b"\x18Iabc")])
    dev.DISCARD_QUIET_S = 0.5
    t0 = time.monotonic()
    dev._discard_in_flight()
    assert time.monotonic() - t0 < 0.5 + 0.4
    assert "rz killed discarded=5" in capsys.readouterr().out


def test_terminator_glued_to_in_flight_data(capsys) -> None:
    """A stalled host can deliver the last in-flight bytes and the terminator as one chunk:
    ZDLE data never holds a bare CR, so a trailing CR is the terminator…"""
    dev = _device([(0.05, b"\x18Iabc\x18M\r")])
    dev._discard_in_flight()
    assert "rz killed discarded=7" in capsys.readouterr().out
    assert dev.carry == b"\r"


def test_hex_header_cr_is_not_the_terminator(capsys) -> None:
    """… but the CR ending a hex header is not."""
    dev = _device([(0.05, b"**\x18B0b000000000000\r"), (0.1, b"\x8a\x11"), (0.3, b"\r")])
    dev._discard_in_flight()
    assert "rz killed discarded=21" in capsys.readouterr().out
    assert dev.carry == b"\r"
