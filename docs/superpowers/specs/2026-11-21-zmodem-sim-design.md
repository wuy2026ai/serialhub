# ZMODEM (rz/sz) in the Web UI + serial simulation environment — design

Date: 2026-11-21 · Status: user opted out of further questions ("just dive in"); calls below are Claude's.

## Intent
The user runs serialhub (one instance per COM port, see `deploy/`) on Debian 12 and Windows to
reach embedded devices from a browser. They want Xshell/SecureCRT-style file transfer:
type `sz file` on the device → file downloads in the browser; type `rz` → browser picks a file
and uploads it. They also want a simulated device (mock TTL serial sending/receiving packets)
so serialhub and the transfer can be tested without hardware.

## Goals
1. **Simulation environment**: a virtual serial pair (socat PTYs on macOS/Linux) with a Python
   mock device on the far end that streams packets, answers commands, and runs the real
   lrzsz `rz`/`sz` binaries — so transfers are tested against the same code a real board runs.
2. **Integration tests** on the sim: packets arrive in the ring buffer, sends reach the device,
   two ports run independently, ZMODEM both directions byte-exact.
3. **ZMODEM in the browser**: auto-detect a ZMODEM session on a port, switch that port to
   raw byte mode, let one browser tab run the transfer (zmodem.js), return to line mode after.

## Non-goals
ZMODEM via CLI/MCP/TCP bridge; XMODEM/YMODEM; resuming partial transfers; Windows sim
automation (document com0com only).

## Anti-goals (failure even if it "works")
- Binary transfer bytes ending up in the ring buffer / line history or the text terminal.
- A dropped or reordered byte (the broadcast queue drops on overflow — must not carry data).
- Device left hung in rz/sz when no browser is around or the tab closes.
- Normal text traffic broken or delayed for ports that never use ZMODEM.

## Approaches
- **A. Browser-side ZMODEM (chosen)**: daemon detects the ZMODEM start header, switches the
  port to raw mode, relays bytes over the existing `/ws` to the claiming tab; zmodem.js
  (Apache-2.0, mature, used by ttyd) runs the protocol in the browser. Files never touch the
  host disk; works identically on Windows and Debian with no extra binaries.
- B. Daemon-side ZMODEM in Python: no maintained Python ZMODEM library; writing one is the
  riskiest part of the project. Rejected.
- C. Daemon spawns host lrzsz: no lrzsz on Windows hosts. Rejected.

## Decided details
- Detection: hex header `**` ZDLE(0x18) `B0` then `0` (ZRQINIT → device is sending →
  "download") or `1` (ZRINIT → device is receiving → "upload"). Text before the header still
  becomes lines; the header and everything after go raw.
- Raw data travels on a per-session unbounded queue delivered only to the claiming WebSocket;
  bytes received before the claim are buffered and replayed on claim.
- Safety: unclaimed for 5 s → abort; no bytes either way for 15 s → abort; claimer WS
  disconnect → abort; port close → end. Abort = 8×CAN + 8×BS written to the device.
- Ring buffer gets `mark` lines: `[zmodem] start <direction>` and
  `[zmodem] end reason=<r> rx=<n> tx=<n>` — never the payload.
- Per-port config `zmodem: true|false` (default true) to disable detection.
- UI: per-port dialog (accept/choose files/cancel), progress, downloads saved via Blob; toolbar
  buttons "Upload (rz)" and "Download (sz)…" send the command for the user. en + zh strings.
