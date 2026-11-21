# Two serial ports, two browser ports

| Config | Serial port (edit `device:`) | Web UI |
|---|---|---|
| `com-a.yaml` | Debian `/dev/ttyUSB0` · Windows `COM3` | http://HOST:8765/ |
| `com-b.yaml` | Debian `/dev/ttyUSB1` · Windows `COM4` | http://HOST:8766/ |

Add a third port: copy `com-b.yaml` to `com-c.yaml`, change `name`, `device`, `port` (e.g. 8767). The start scripts pick up every `com-*.yaml`.

## Debian 12
    cd deploy/linux
    ./setup.sh                 # once: venv, deps, adds you to 'dialout'
    ./start.sh                 # local only;  ./start.sh --host 0.0.0.0  for LAN
    ./stop.sh
Tip: use stable names like `/dev/serial/by-id/usb-...` for `device:`. For start at boot see `serialhub@.service`.

## Windows
    cd deploy\windows
    powershell -ExecutionPolicy Bypass -File setup.ps1     # once
    powershell -ExecutionPolicy Bypass -File start.ps1     # add -ListenHost 0.0.0.0 for LAN
    powershell -ExecutionPolicy Bypass -File stop.ps1

## Notes
- With `--host 0.0.0.0` anyone on the network can use the ports; set `token:` in the yaml to require auth, and allow 8765/8766 in the firewall.
- CLI against one instance: `serialhub --url http://127.0.0.1:8766 read com-b --limit 20`
- `demo/` has `loop://` versions (no hardware) for a quick test.
