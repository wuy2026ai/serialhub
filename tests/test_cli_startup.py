"""Client-side CLI commands must not import the server stack."""
from __future__ import annotations

import json
import subprocess
import sys

HEAVY = ("fastapi", "uvicorn", "mcp", "websockets")

_SCRIPT = """
import io, sys, contextlib
import serialhub.cli as cli
p = cli._build_parser()
p.parse_args(["call", "ports"])
p.parse_args(["tools"])
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    rc = cli.main(["tools"])
assert rc == 0
bad = [m for m in %r if m in sys.modules]
assert not bad, bad
sys.stdout.write(buf.getvalue())
"""


def test_call_and_tools_do_not_import_server_stack():
    out = subprocess.run(
        [sys.executable, "-c", _SCRIPT % (HEAVY,)],
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr
    tools = json.loads(out.stdout)
    assert isinstance(tools, list) and tools


def test_serialhub_lazy_exports_still_work():
    out = subprocess.run(
        [sys.executable, "-c",
         "import serialhub; print(serialhub.Hub.__name__, serialhub.Line.__name__)"],
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ["Hub", "Line"]
