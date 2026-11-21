#!/usr/bin/env node
// Test harness: drive one serialhub /ws ZMODEM session with zmodem.js.
//
//   node zclient.mjs --ws URL --port NAME (--send FILE... | --recv-dir DIR)
//                    [--close-after-bytes N] [--timeout-ms MS]
//
// Waits for the {"type":"zmodem","event":"start"} broadcast for NAME, claims the
// session, feeds the relayed bytes to Zmodem.Sentry, accepts every offer
// (download, --recv-dir) or sends the given files (upload, --send), then sends
// zmodem_end (with the post-session bytes as tail_b64) and waits for
// zmodem_closed.
//
// stderr: {"ready":true} once subscribed (start the device transfer after it).
// stdout: exactly one JSON line {"ok":bool,"files":[{"name","size"}],"error"}.
// Exit 0 when ok, 1 otherwise. --close-after-bytes drops the socket once that
// many session bytes have been received (simulates a tab closing mid-transfer).
// --timeout-ms (default 20000) fails the run if it has not finished by then.

import { Buffer } from "node:buffer";
import { readFileSync, statSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import Zmodem from "zmodem.js";
import { applyLrzszShim } from "./zmodem-shim.mjs";

const CHUNK = 8192;

// lsz download fix for zmodem.js 0.1.10; see zmodem-shim.mjs.
applyLrzszShim(Zmodem);

// After zmodem.js reports session_end, keep feeding the Sentry this long and
// collect what it hands to to_terminal (device output after lsz's "OO", e.g.
// "sz done rc=0" or the prompt); it goes back to the hub as zmodem_end.tail_b64
// so the hub can put it into the ring buffer. The browser UI must do the same.
const TAIL_MS = 200;

function parseArgs(argv) {
  const opts = { send: [], timeoutMs: 20000 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--ws") opts.ws = argv[++i];
    else if (a === "--port") opts.port = argv[++i];
    else if (a === "--recv-dir") opts.recvDir = argv[++i];
    else if (a === "--close-after-bytes") opts.closeAfter = Number(argv[++i]);
    else if (a === "--timeout-ms") opts.timeoutMs = Number(argv[++i]);
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
const tail = []; // to_terminal bytes after session_end

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
  const msg = { type: "zmodem_end", port: opts.port, session: sessionId, abort };
  if (!abort && tail.length) msg.tail_b64 = Buffer.from(tail).toString("base64");
  send(msg);
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
    to_terminal: (octets) => {
      if (zmEnded) for (const b of octets) tail.push(b);
    },
    on_retract: () => {},
    sender: (octets) =>
      send({
        type: "zmodem_write", port: opts.port, session: sessionId,
        b64: Buffer.from(octets).toString("base64"),
      }),
    on_detect: (detection) => {
      const zsession = detection.confirm();
      zsession.on("session_end", () => {
        if (zsession.aborted()) return fail("zmodem session aborted");
        zmEnded = true;
        setTimeout(() => endSession(false), TAIL_MS);
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
    fail(msg.error);
  }
});

setTimeout(() => fail(`timed out after ${opts.timeoutMs} ms`), opts.timeoutMs).unref();

ws.addEventListener("error", () => fail("websocket error"));
ws.addEventListener("close", () => fail("websocket closed"));
