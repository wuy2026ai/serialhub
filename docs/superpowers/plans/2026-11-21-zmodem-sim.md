# ZMODEM Web UI + Serial Simulation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A mock-device serial simulator with integration tests, and browser rz/sz (ZMODEM) file transfer in serialhub's Web UI.

**Architecture:** socat creates a PTY pair; `tools/mock_device.py` owns one end (streams packets, answers commands, spawns real lrzsz). serialhub's worker detects the ZMODEM start header, flips the port into raw mode and hands bytes to the one WebSocket that claimed the session; zmodem.js runs the protocol in the browser (and in a Node harness for automated tests).

**Tech Stack:** Python 3.10+ (pyserial, FastAPI, pytest), socat 1.8, lrzsz 0.12/0.13, Node ≥ 20 + zmodem.js 0.1.10, Playwright (python) for the one e2e test.

**Spec:** `docs/superpowers/specs/2026-11-21-zmodem-sim-design.md`

## Global Constraints
- Repo: `/Volumes/Res/AI/Tools/cjznb-serialhub`, branch `deploy-two-ports`; venv `.venv` (`.venv/bin/python -m pytest`).
- Existing suite (`.venv/bin/python -m pytest -q`) must stay green after every task.
- Sim/integration tests live under `tests/integration/`, are marked `@pytest.mark.sim`, and **skip** (not fail) when `socat`, lrzsz (`lrz`/`rz`) or `node` is missing. Register the markers `sim` and `e2e` in `pyproject.toml` `[tool.pytest.ini_options]`.
- lrzsz binary lookup: `shutil.which("lrz") or shutil.which("rz")` (and `lsz`/`sz`) — Homebrew ships both names, Debian ships `rz`/`sz`.
- ZMODEM header regex (bytes): `rb"\*\*\x18B0([01])"`; group `0` → `"download"` (device runs sz), `1` → `"upload"` (device runs rz).
- Abort sequence written to device: `b"\x18" * 8 + b"\x08" * 8`.
- Timeouts (config defaults, per port): `zmodem_claim_ms = 5000`, `zmodem_idle_ms = 15000`; `zmodem = True`.
- Payload bytes never go to `Hub.ingest`, the ring buffer, `Hub._broadcast`, or the text terminal.
- zmodem.js pinned to `0.1.10` (Apache-2.0); vendored file + CDN fallback, mirroring how `index.html` loads xterm. Add it (and crc-32 if bundled) to `THIRD_PARTY_NOTICES.md`.
- UI strings in both `serialhub/static/i18n/en.js` and `zh.js` (follow `_template.js`).
- Code comments/docstrings: match the file you edit (this repo writes Chinese comments in Python).

## Review Focus
1. Header split across two `ser.read()` chunks (e.g. `**\x18` | `B00…`) — must still detect; ordinary text ending in `*` must not be delayed beyond the next chunk.
2. Payload containing the literal header bytes mid-transfer (random 1 MiB) — must not restart a session (detector is off while a session is active).
3. Owner tab closes / WS drops mid-transfer — device gets abort, port returns to line mode, next `echo` works.
4. Two browser tabs both try to claim — exactly one wins; the other gets `zmodem_error` and the UI says "in use by another browser".
5. Empty (0-byte) file and a file name with spaces/non-ASCII (`测试 file.bin`) round-trip in both directions.

Tests pinning each are listed in Tasks 2–4.

---

### Task 1: Simulator (PTY pair + mock device) and baseline integration tests

**Files:**
- Create: `tools/mock_device.py`
- Create: `tests/integration/__init__.py`, `tests/integration/simenv.py`, `tests/integration/test_sim_baseline.py`
- Modify: `pyproject.toml` (markers)
- Create: `docs/SIMULATION.md` (how to run the sim by hand on macOS/Debian; Windows: com0com pair + mock_device on the second COM, lrzsz unavailable there → ZMODEM sim on Linux/macOS only)

**Interfaces:**
- Produces `tools/mock_device.py` CLI: `python tools/mock_device.py --device PATH [--baud 115200] [--name dev] [--interval 0.5] [--files-dir DIR] [--recv-dir DIR]`.
  - Every `--interval` s (0 = off) writes `PKT name=<name> seq=<n> t=<epoch_ms> payload=<16 hex> crc=<CRC16-CCITT of payload bytes, 4 hex>\r\n`.
  - Reads CR/LF-terminated commands: `ping` → `pong`; `echo <text>` → `<text>`; `stream on|off`; `ls` → one line per file in recv-dir; `rz` → pause packets, spawn lrz `-y` with stdin/stdout = the serial fd, cwd = recv-dir, then print `rz done rc=<code>`; `sz <file…>` → same with lsz, cwd = files-dir, print `sz done rc=<code>`; unknown → `ERR unknown: <cmd>`. All replies end `\r\n`.
- Produces `tests/integration/simenv.py`:
  - `need_tools(*names: str) -> None` — `pytest.skip` when any is missing.
  - `class PtyPair` context manager: starts `socat -d -d pty,raw,echo=0,link=<tmp>/hostN pty,raw,echo=0,link=<tmp>/devN`, waits until both links exist; attrs `host_path`, `dev_path`.
  - `class MockDevice` context manager: `MockDevice(dev_path, name, files_dir, recv_dir, interval=0.2)` runs `tools/mock_device.py` as a subprocess.
  - `class LiveServer` context manager: builds `Config` with given ports (`[{"name":..., "device": host_path, "autostart": True}]`) on a free port, runs `create_app` under uvicorn in a thread; attrs `base_url`, `ws_url`; `stop()` joins cleanly.

- [ ] **Step 1: Write failing tests** in `test_sim_baseline.py` (all `@pytest.mark.sim`):
  - `test_packets_arrive`: within 3 s `GET /api/ports/a/read` returns ≥ 3 lines matching `^PKT name=a seq=\d+ .* crc=[0-9A-F]{4}$`, seqs strictly increasing, CRC recomputes correctly.
  - `test_send_roundtrip`: `POST /api/ports/a/send {"data":"echo hello-42"}` → `wait` for `^hello-42$` matches within 2 s.
  - `test_two_ports_independent`: two PtyPairs + devices `a`,`b` on one server → port `a` buffer only has `name=a` packets, `b` only `name=b`; `echo` on `b` never appears on `a`.
  - `test_device_unplug_reconnect`: kill device's PtyPair socat, restart on same link paths → packets resume on the same port name within 10 s (old buffer kept).
- [ ] **Step 2:** `.venv/bin/python -m pytest tests/integration/test_sim_baseline.py -v` → FAIL (modules missing).
- [ ] **Step 3:** Implement `tools/mock_device.py` and `simenv.py` per Interfaces. Mock device: open pyserial with `timeout=0.05`; packet thread + command loop guarded by one write lock; while rz/sz child runs, the parent must not read or write the fd.
- [ ] **Step 4:** Re-run → PASS; full suite green.
- [ ] **Step 5:** Manual smoke: `python tools/mock_device.py --device <devlink>` + `serialhub serve` on the host link shows packets in the Web UI; note the commands in `docs/SIMULATION.md`.
- [ ] **Step 6: Commit** `test(sim): socat PTY simulator, mock device and baseline integration tests`

### Task 2: ZMODEM detector and raw-mode session in the worker/hub

**Files:**
- Create: `serialhub/zmodem.py`
- Modify: `serialhub/serialport.py` (`_run_loop`, `_emit` path, `write`, `close`), `serialhub/hub.py` (new methods), `serialhub/config.py` (`PortConfig.zmodem`, `zmodem_claim_ms`, `zmodem_idle_ms` + validation + `_port_to_dict`), `docs/ARCHITECTURE.md` (new section)
- Test: `tests/test_zmodem_detect.py`, `tests/test_zmodem_session.py`

**Interfaces:**
- `serialhub/zmodem.py`:
  - `ABORT_SEQ: bytes`
  - `@dataclass class Detection: direction: str  # "download"|"upload"; initial: bytes  # header + following bytes`
  - `class ZmodemDetector: feed(data: bytes) -> tuple[bytes, Optional[Detection]]` — returns bytes to treat as text (everything before the header), and a Detection if found. Holds back at most the trailing bytes that are a proper prefix of `**\x18B0`; `flush() -> bytes` releases them (called when the read returns empty so held text isn't stuck).
  - `class ZmodemSession`: fields `id: str` (uuid4 hex[:8]), `port`, `direction`, `owner: Optional[str]`, `rx_bytes`, `tx_bytes`, `started_ms`, `last_activity_ms`, `pending: bytearray`, `queue: queue.Queue[Optional[bytes]]` (unbounded; `None` = end sentinel).
- Hub methods (all raise the repo's usual not-found error for unknown ports):
  - `zmodem_status(name) -> dict` → `{"port", "active": bool, "session", "direction", "owner", "rx_bytes", "tx_bytes"}`
  - `zmodem_claim(name, client_id: str) -> tuple[str, queue.Queue]` — returns `(session_id, queue)`; replays `pending` into the queue first; raises `RuntimeError("zmodem: no active session")` / `RuntimeError("zmodem: claimed by another client")`.
  - `zmodem_write(name, session_id: str, client_id: str, data: bytes) -> int` — owner only; raw write, `tx_bytes += n`, refreshes activity.
  - `zmodem_end(name, session_id: str, client_id: Optional[str], *, abort: bool = False, reason: str = "done") -> dict` — idempotent; abort writes `ABORT_SEQ`; puts `None` on queue.
- Broadcast events (via `_broadcast`, metadata only): `{"type":"zmodem","event":"start","port","session","direction"}` and `{"type":"zmodem","event":"end","port","session","reason","rx_bytes","tx_bytes"}`; reasons ∈ `done|abort|timeout|unclaimed|closed|disconnect`.
- Marks: `hub.mark(port, "[zmodem] start <direction>")`, `hub.mark(port, "[zmodem] end reason=<r> rx=<n> tx=<n>")`.

- [ ] **Step 1: Failing tests** (`test_zmodem_detect.py`): header in one chunk; split at every offset of the header; text-before-header returned; `*` ending ordinary text released by `flush()`; `**\x18B02` (other frame type) not detected; direction mapping 0→download, 1→upload.
- [ ] **Step 2: Failing tests** (`test_zmodem_session.py`, using a fake serial / `loop://` fixture from `conftest.py` and calling worker ingestion directly): detection broadcasts `start` and writes the start mark; bytes after detection appear only in the claimed queue (ring buffer gets no new rx lines); pre-claim bytes replayed; second `zmodem_claim` with a different client raises; unclaimed after `zmodem_claim_ms` (set 200 ms in test) → `ABORT_SEQ` written, `end` reason `unclaimed`, line mode resumes (next text becomes a line); idle timeout (set 300 ms) → reason `timeout`; detector disabled while active (feeding a header mid-session does not emit a second `start`); `zmodem: false` config → header stays text.
- [ ] **Step 3:** Run both → FAIL.
- [ ] **Step 4:** Implement. In `_run_loop`, when `zmodem` enabled and no session: pass data through the detector, line-frame only the text part; on Detection, flush line residual, open session via hub, push `initial` into it. When a session is active: route all read bytes to the session (append to `pending` until claimed, else queue), skip framing, and check claim/idle deadlines each loop iteration (reads time out at 0.1 s already). Port close/stop ends any session with `closed`.
- [ ] **Step 5:** Run → PASS; full suite green.
- [ ] **Step 6: Commit** `feat(zmodem): detect ZMODEM sessions and switch port to raw mode`

### Task 3: WebSocket protocol + Node zmodem.js harness + end-to-end sim tests

**Files:**
- Modify: `serialhub/server.py` (`ws_endpoint` message handling + per-session pump task)
- Create: `tests/zmodem_client/package.json` (deps: `zmodem.js@0.1.10`; package-lock committed), `tests/zmodem_client/zclient.mjs`, `tests/integration/test_zmodem_sim.py`
- Modify: `tests/integration/simenv.py` (`node_client(...)` helper, runs `npm ci` once per session into `tests/zmodem_client/node_modules`, gitignored)
- Modify: `docs/ARCHITECTURE.md` (WS messages)

**Interfaces:**
- Client → server over `/ws` (JSON text frames):
  - `{"type":"zmodem_claim","port"}` → server replies `{"type":"zmodem_claimed","port","session","direction"}` then streams `{"type":"zmodem_data","port","session","b64"}` to **this socket only**, ending with `{"type":"zmodem_closed","port","session","reason"}`; failure → `{"type":"zmodem_error","port","error"}`.
  - `{"type":"zmodem_write","port","session","b64"}`
  - `{"type":"zmodem_end","port","session","abort": bool}`
- WS disconnect while owning a session → `hub.zmodem_end(..., abort=True, reason="disconnect")`. Each socket gets a stable `client_id` (uuid4).
- `zclient.mjs` CLI: `node zclient.mjs --ws URL --port NAME (--send FILE… | --recv-dir DIR) [--no-claim] [--close-after-bytes N]`; waits for the `zmodem` start broadcast for NAME, claims, feeds bytes to `Zmodem.Sentry`, accepts offers (download) or sends files (upload), sends `zmodem_end`, prints one JSON line `{"ok":bool,"files":[{"name","size"}],"error"}` and exits 0/1. `--close-after-bytes` drops the socket mid-transfer (Review Focus 3).

- [ ] **Step 1: Failing tests** in `test_zmodem_sim.py` (`@pytest.mark.sim`, need socat+lrzsz+node):
  - `test_download_1mib_random`: device `files_dir` has 1 MiB `os.urandom` file with `b"**\x18B00"` spliced at offset 500 000; send `sz blob.bin`; zclient `--recv-dir` gets identical bytes; ring buffer has start/end marks and **no** line containing payload; then `echo after` → line `after` within 2 s.
  - `test_upload_files`: zclient `--send` a 300 KiB random file + 0-byte file + `测试 file.bin`; send `rz`; device recv_dir has all three byte-identical; `rz done rc=0` line appears.
  - `test_owner_disconnect_aborts`: `--close-after-bytes 65536` on a 1 MiB download → end broadcast reason `disconnect`, device prints `sz done rc=` with non-zero rc within 20 s, `echo ok` works after.
  - `test_unclaimed_aborts`: no client; `sz blob.bin` → end reason `unclaimed` within 7 s; `ping` → `pong`.
  - `test_second_claim_rejected`: two raw WS clients claim the same session → one `zmodem_claimed`, one `zmodem_error`.
  - `test_other_port_unaffected`: during a download on port `a`, port `b` keeps receiving packets with no seq gaps.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement server handling (session pump: `asyncio.to_thread(q.get, True, 1.0)` loop like `_pump`, base64 encode, stop on `None`) and `zclient.mjs`.
- [ ] **Step 4:** Run → PASS (each test < 30 s); full suite green.
- [ ] **Step 5: Commit** `feat(zmodem): websocket session relay + Node zmodem.js harness and sim e2e tests`

### Task 4: Browser UI (zmodem.js dialog, progress, rz/sz buttons) + e2e

**Files:**
- Create: `serialhub/static/vendor/zmodem.min.js` (browser build of zmodem.js 0.1.10 including its crc-32 dep — build once with `npx esbuild` from `tests/zmodem_client/node_modules`, record the exact command in `THIRD_PARTY_NOTICES.md`), `serialhub/static/zmodem-ui.js`
- Modify: `serialhub/static/index.html` (load vendor file with CDN fallback `https://cdn.jsdelivr.net/npm/zmodem.js@0.1.10/dist/zmodem.js` + `zmodem-ui.js`; dialog markup; two per-port toolbar buttons), `serialhub/static/app.js` (`handleWsMessage`: route `zmodem`, `zmodem_claimed`, `zmodem_data`, `zmodem_closed`, `zmodem_error` to `zmodem-ui.js`; expose a `wsSend(obj)` helper), `serialhub/static/style.css`, `serialhub/static/i18n/{en,zh}.js`, `THIRD_PARTY_NOTICES.md`, `README.md` + `README.zh-CN.md` (feature row + usage), `deploy/README.md` (one line)
- Test: `tests/e2e/test_zmodem_browser.py` (`@pytest.mark.e2e`, skip if `playwright` or chromium missing)

**Interfaces:**
- Consumes Task 3 WS messages and Task 2 `start`/`end` broadcasts.
- `zmodem-ui.js` exports on `window.ZmodemUI`: `onBroadcast(msg)`, `onClaimed(msg)`, `onData(msg)`, `onClosed(msg)`, `onError(msg)`, `init({wsSend, sendCommand(port, text), t})`.
- Behaviour: `start` broadcast → that port's panel shows a dialog: download → "Device is sending files" [Receive] [Cancel]; upload → "Device is waiting for files" [Choose files…] [Cancel]. Only a click claims. Progress row per file (name, bytes/total, rate); download saves each file via Blob + `<a download>`. Cancel or claim error → `zmodem_end abort:true`. If another client claimed (`end`/`start` seen but not ours) → "Transfer in use by another browser", buttons disabled. Toolbar: "Upload (rz)" sends `rz`; "Download (sz)…" prompts for a device path then sends `sz <path>`.

- [ ] **Step 1: Failing e2e test** `test_browser_download_and_upload`: LiveServer + sim device; Playwright opens `/`, clicks "Download (sz)…", enters `blob.bin`, clicks Receive, awaits the download event and compares bytes; then clicks "Upload (rz)", sets the file chooser to a 100 KiB file, waits for the device recv_dir file to match; asserts the text terminal shows the two `[zmodem]` marks and no binary garbage (no U+FFFD in rendered rows).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement per Interfaces; strings in en + zh.
- [ ] **Step 4:** Run e2e → PASS; full suite green; open the page manually against the sim and check both themes and the zh locale.
- [ ] **Step 5: Commit** `feat(webui): rz/sz file transfer dialog with zmodem.js`
