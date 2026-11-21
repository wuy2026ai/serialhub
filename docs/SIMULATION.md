# Serial simulation (no hardware)

Simulates a serial device with a PTY pair (`socat`) and `tools/mock_device.py`.
ZMODEM simulation needs lrzsz and therefore runs on Linux/macOS only.

## Prerequisites

- macOS: `brew install socat lrzsz`
- Debian/Ubuntu: `apt install socat lrzsz`
- Windows: no lrzsz. Use a com0com virtual pair and run `mock_device.py --device COMy`
  on the second COM; point serialhub at the first. ZMODEM cannot be simulated there.

## Run by hand

```bash
D=$(mktemp -d)
socat -d -d pty,raw,echo=0,link=$D/host pty,raw,echo=0,link=$D/dev &
python tools/mock_device.py --device $D/dev --name dev --interval 0.5 \
    --files-dir ./some-files --recv-dir $D &
cat > $D/c.yaml <<EOF2
server: {host: 127.0.0.1, port: 8799}
ports:
  - {name: dev, device: $D/host, autostart: true}
EOF2
serialhub serve --config $D/c.yaml &
# Web UI: http://127.0.0.1:8799/   or non-interactively:
curl -s "http://127.0.0.1:8799/api/ports/dev/read?limit=3"
curl -s -X POST -H 'content-type: application/json' \
    -d '{"data":"echo hi"}' http://127.0.0.1:8799/api/ports/dev/send
# cleanup: kill the three background jobs, rm -rf $D
```

Device commands (CR/LF terminated): `ping`, `echo <text>`, `stream on|off`, `ls`,
`rz` (device receives, files land in `--recv-dir`), `sz <file...>` (device sends files
from `--files-dir`). Packets: `PKT name=.. seq=.. t=.. payload=<16 hex> crc=<CRC16-CCITT-FALSE>`.

## Tests

```bash
.venv/bin/python -m pytest tests/integration -m sim -v
```
Tests skip when `socat` (or lrzsz / node where required) is missing.
