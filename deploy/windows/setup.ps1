# One-time setup on Windows: python venv + deps. Requires Python 3.10+ (python.org or `winget install Python.Python.3.12`).
$ErrorActionPreference = "Stop"
$Root = (Resolve-Path "$PSScriptRoot\..\..").Path
py -3 -m venv "$Root\.venv"
& "$Root\.venv\Scripts\pip.exe" install -q -r "$Root\requirements.txt" "mcp>=1.9"
& "$Root\.venv\Scripts\pip.exe" install -q -e $Root
Write-Host "Done. Serial devices found:"
& "$Root\.venv\Scripts\python.exe" -m serial.tools.list_ports -v
