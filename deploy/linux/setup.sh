#!/usr/bin/env bash
# One-time setup on Debian 12: python venv + deps + serial permissions.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
sudo apt-get install -y python3-venv
python3 -m venv "$ROOT/.venv"
"$ROOT/.venv/bin/pip" install -q -r "$ROOT/requirements.txt" "mcp>=1.9"
"$ROOT/.venv/bin/pip" install -q -e "$ROOT"
if ! id -nG "$USER" | grep -qw dialout; then
  sudo usermod -aG dialout "$USER"
  echo "Added $USER to 'dialout' - log out and back in before starting."
fi
echo "Done. Serial devices found:"; "$ROOT/.venv/bin/python" -m serial.tools.list_ports -v || true
