#!/usr/bin/env node
// Test harness: drive one serialhub /ws ZMODEM session with zmodem.js.
//
//   node zclient.mjs --ws URL --port NAME (--send FILE... | --recv-dir DIR)
//                    [--close-after-bytes N]
//
// Waits for the {"type":"zmodem","event":"start"} broadcast for NAME, claims the
// session, feeds the relayed bytes to Zmodem.Sentry, accepts every offer
// (download, --recv-dir) or sends the given files (upload, --send), then sends
// zmodem_end and waits for zmodem_closed.
//
// stderr: {"ready":true} once subscribed (start the device transfer after it).
// stdout: exactly one JSON line {"ok":bool,"files":[{"name","size"}],"error"}.
// Exit 0 when ok, 1 otherwise. --close-after-bytes drops the socket once that
// many session bytes have been received (simulates a tab closing mid-transfer).

import { Buffer } from "node:buffer";
import { readFileSync, statSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import Zmodem from "zmodem.js";

const CHUNK = 8192;

// lrzsz compatibility shim for zmodem.js 0.1.10 (receive side).
// lsz ends the *first* ZDATA subpacket of every file with ZCRCW (frame end, ACK
// expected) and then waits for ZACK before sending a fresh ZDATA header.
// zmodem.js only ACKs ZCRCQ and only accepts ZEOF after a ZDATA frame, so the
// download stalls after 1024 bytes. Send the ZACK and accept the next ZDATA.
// The browser UI (Task 4) needs the same shim.
{
  const Receive = Zmodem.Session.Receive.prototype;
  const consumeData = Receive._consume_ZDATA_data;
  Receive._consume_ZDATA_data = function (subpacket) {
    consumeData.call(this, subpacket);
    if (!(subpacket.ack_expected() && subpacket.frame_end())) return;
    this._send_header("ZACK", Zmodem.ENCODELIB.pack_u32_le(this._file_offset));
    const handlers = this._next_header_handler; // { ZEOF } installed by accept()
    handlers.ZDATA = function (hdr) {
      this._consume_ZDATA(hdr);
      this._next_subpacket_handler = this._consume_ZDATA_data;
      this._next_header_handler = handlers;
    };
  };
}

function parseArgs(argv) {
  const opts = { send: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--ws") opts.ws = argv[++i];
    else if (a === "--port") opts.port = argv[++i];
    else if (a === "--recv-dir") opts.recvDir = argv[++i];
    else if (a === "--close-after-bytes") opts.closeAfter = Number(argv[++i]);
    else if (a === "--send") {
      while (i + 1 < argv.length && !argv[i + 1].startsWith("--")) opts.send.push(argv[++i]);
    } else throw new Error(`unknown argument: ${a}`);
  }
  if (!opts.ws || !opts.port) throw new Error("--ws and --port are required");
  if (!opts.send.length === !opts.recvDir) throw new Error("need exactly one of --send / --recv-dir");
  return opts;
}

let opts;
try {
  opts = parseArgs(process.argv.slice(2));
} catch (e) {
  process.stdout.write(JSON.stringify({ ok: false, files: [], error: String(e.message) }) + "\n");
  process.exit(1);
}

const files = [];
const pendingWrites = [];
let sessionId = null;
let zmEnded = false; // zmodem.js reported session_end
let finished = false;
let rxBytes = 0;

const ws = new WebSocket(opts.ws);

function send(obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function finish(ok, error = null) {
  if (finished) return;
  finished = true;
  process.stdout.write(JSON.stringify({ ok, files, error }) + "\n");
  try { ws.close(); } catch { /* already closed */ }
  process.exitCode = ok ? 0 : 1;
  setTimeout(() => process.exit(ok ? 0 : 1), 200).unref();
}

function endSession(abort) {
  send({ type: "zmodem_end", port: opts.port, session: sessionId, abort });
}

function fail(err) {
  if (sessionId && !zmEnded) endSession(true);
  finish(false, String(err && err.message ? err.message : err));
}

function receive(zsession) {
  zsession.on("offer", (offer) => {
    const name = offer.get_details().name;
    const chunks = [];
    pendingWrites.push(
      offer.accept({ on_input: (p) => chunks.push(Buffer.from(p)) }).then(() => {
        const data = Buffer.concat(chunks);
        writeFileSync(join(opts.recvDir, basename(name)), data);
        files.push({ name, size: data.length });
      }),
    );
  });
  zsession.start();
}

async function upload(zsession) {
  const paths = opts.send;
  let remaining = paths.reduce((n, p) => n + statSync(p).size, 0);
  for (let i = 0; i < paths.length; i++) {
    const data = readFileSync(paths[i]);
    const name = basename(paths[i]);
    const xfer = await zsession.send_offer({
      name,
      size: data.length,
      mtime: statSync(paths[i]).mtime,
      files_remaining: paths.length - i,
      bytes_remaining: remaining,
    });
    remaining -= data.length;
    if (!xfer) continue; // receiver skipped the file
    let off = 0;
    for (; off + CHUNK < data.length; off += CHUNK) xfer.send(data.subarray(off, off + CHUNK));
    await xfer.end(data.subarray(off));
    files.push({ name, size: data.length });
  }
  await zsession.close();
}

function startSentry() {
  return new Zmodem.Sentry({
    to_terminal: () => {},
    on_retract: () => {},
    sender: (octets) =>
      send({
        type: "zmodem_write", port: opts.port, session: sessionId,
        b64: Buffer.from(octets).toString("base64"),
      }),
    on_detect: (detection) => {
      const zsession = detection.confirm();
      zsession.on("session_end", () => {
        zmEnded = true;
        endSession(false);
      });
      if (zsession.type === "receive") {
        if (!opts.recvDir) return fail("device offered files but --send was given");
        receive(zsession);
      } else {
        if (!opts.send.length) return fail("device wants files but --recv-dir was given");
        upload(zsession).catch(fail);
      }
    },
  });
}

let sentry = null;

ws.addEventListener("message", (ev) => {
  let msg;
  try { msg = JSON.parse(ev.data); } catch { return; }
  if (msg.type === "hello") {
    send({ type: "ping" }); // pong ⇒ the server has subscribed this socket
  } else if (msg.type === "pong") {
    process.stderr.write(JSON.stringify({ ready: true }) + "\n");
  } else if (msg.port !== opts.port) {
    // other ports
  } else if (msg.type === "zmodem" && msg.event === "start" && !sessionId) {
    send({ type: "zmodem_claim", port: opts.port });
  } else if (msg.type === "zmodem_claimed" && !sessionId) {
    sessionId = msg.session;
    sentry = startSentry();
  } else if (msg.type === "zmodem_data" && msg.session === sessionId) {
    const bytes = Buffer.from(msg.b64, "base64");
    rxBytes += bytes.length;
    if (opts.closeAfter && rxBytes >= opts.closeAfter) {
      finish(false, `closed after ${rxBytes} bytes`);
      return;
    }
    try { sentry.consume(Array.from(bytes)); } catch (e) { fail(e); }
  } else if (msg.type === "zmodem_closed" && msg.session === sessionId) {
    if (!zmEnded || msg.reason !== "done") return fail(`session closed: ${msg.reason}`);
    Promise.all(pendingWrites).then(() => finish(true), fail);
  } else if (msg.type === "zmodem_error") {
    if (!zmEnded) fail(msg.error);
  }
});

ws.addEventListener("error", () => fail("websocket error"));
ws.addEventListener("close", () => fail("websocket closed"));
