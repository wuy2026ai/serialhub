#!/usr/bin/env node
// Regression check for serialhub/static/vendor/zmodem-shim.js: replays lsz's
// bytes for a 0-byte download in which lsz exits after ZFIN without "OO"
// (captured from the browser e2e test), then prints one JSON line:
//   {"ended":bool,"tail":"<to_terminal text after the session>","error":str|null}
//   node shim_check.mjs [--no-text] [--no-shim]
// --no-text omits the device's "sz done" line (the session must still end, by timer).

import Zmodem from "zmodem.js";
import lrzszShim from "../../serialhub/static/vendor/zmodem-shim.js";

const args = new Set(process.argv.slice(2));
if (!args.has("--no-shim")) lrzszShim.applyLrzszShim(Zmodem);

const CHUNKS = [
  // ZRQINIT (hex header)
  [42, 42, 24, 66, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 13, 138, 17],
  // ZFILE "empty.bin", size 0
  [42, 24, 67, 4, 0, 0, 0, 0, 221, 81, 162, 51, 101, 109, 112, 116, 121, 46, 98, 105, 110, 0, 48,
    32, 49, 53, 51, 48, 48, 50, 50, 55, 48, 50, 49, 32, 49, 48, 48, 54, 52, 52, 32, 48, 32, 49, 32,
    48, 0, 24, 107, 175, 126, 124, 11, 17],
  // empty ZDATA subpacket (ZCRCW) + ZEOF
  [42, 24, 67, 10, 0, 0, 0, 0, 188, 239, 146, 140, 24, 104, 231, 6, 107, 24, 209, 42, 24, 67, 11, 0,
    0, 0, 0, 12, 198, 242, 177],
  // ZFIN (hex header) — lsz then exits without "OO"
  [42, 42, 24, 66, 48, 56, 48, 48, 48, 48, 48, 48, 48, 48, 48, 50, 50, 100, 13, 138],
];
if (!args.has("--no-text")) CHUNKS.push(Array.from(Buffer.from("sz done rc=0\r\n")));

let ended = false;
const tail = [];
const done = (error) => {
  process.stdout.write(JSON.stringify({ ended, tail: Buffer.from(tail).toString(), error }) + "\n");
  process.exit(0);
};

const sentry = new Zmodem.Sentry({
  to_terminal: (octets) => { if (ended) tail.push(...octets); },
  on_retract: () => {},
  sender: () => {},
  on_detect: (detection) => {
    const zsession = detection.confirm();
    zsession.on("offer", (offer) => offer.accept());
    zsession.on("session_end", () => { ended = true; });
    zsession.start();
  },
});

try {
  for (const c of CHUNKS) sentry.consume(c);
} catch (e) {
  done(String(e && e.message ? e.message : e));
}
setTimeout(() => done(null), 1500);
