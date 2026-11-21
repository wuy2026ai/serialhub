/* lrzsz compatibility shim for zmodem.js 0.1.10 (receive side).
 *
 * lsz ends the *first* ZDATA subpacket of every file with ZCRCW (frame end, ACK
 * expected), then waits for ZACK and starts a fresh ZDATA header. zmodem.js
 * only ACKs ZCRCQ and, after a ZDATA frame, only accepts ZEOF, so every
 * download stalls after 1024 bytes. This patch sends the ZACK and accepts the
 * next ZDATA header.
 *
 * Second, lsz does not always send the closing "OO" after the ZFIN exchange
 * (its saybibi() returns without it on a timeout/ZCAN, and with tiny files it
 * often exits straight away). zmodem.js then throws "Only thing after ZFIN
 * should be OO" on the device's next output, or never ends the session. lrz
 * itself treats OO as optional, so do the same: after ZFIN, non-"OO" input
 * ends the session and is handed back as post-session (terminal) bytes; a
 * repeated ZFIN header is answered again; and with no input at all the
 * session ends after ZFIN_OO_WAIT_MS.
 *
 * Single source of truth, written by serialhub (not part of zmodem.js):
 *   - browser: loaded as a classic <script> after zmodem.min.js; it defines
 *     window.applyLrzszShim and patches window.Zmodem right away.
 *   - Node (tests/zmodem_client/zclient.mjs): imported as CommonJS,
 *     `applyLrzszShim(Zmodem)`.
 * applyLrzszShim is idempotent.
 */
(function (root) {
  "use strict";

  var ZFIN_OO_WAIT_MS = 1000;

  function applyLrzszShim(Zmodem) {
    var Receive = Zmodem.Session.Receive.prototype;
    if (Receive._lrzszShim) return;
    Receive._lrzszShim = true;

    var consumeFirst = Receive._consume_first;
    Receive._consume_first = function () {
      var buf = this._input_buffer;
      if (this._got_ZFIN && !this._bytes_after_OO && buf.length >= 2 &&
          !(buf[0] === 79 && buf[1] === 79)) {
        if (buf[0] === 42) {          // "*": lsz repeated its ZFIN header
          this._got_ZFIN = false;
          return consumeFirst.call(this);
        }
        this._bytes_after_OO = buf.splice(0, buf.length);   // no "OO": device text
        this._on_session_end();
        return;
      }
      return consumeFirst.call(this);
    };

    var consumeZFIN = Receive._consume_ZFIN;
    Receive._consume_ZFIN = function () {
      consumeZFIN.apply(this, arguments);
      var self = this;
      clearTimeout(this._lrzszOOTimer);
      this._lrzszOOTimer = setTimeout(function () {
        if (self._bytes_after_OO || self._aborted) return;
        // a lone "O" (half of OO) is protocol; anything else buffered is device text
        var rest = self._input_buffer.splice(0, self._input_buffer.length);
        if (rest.length === 1 && rest[0] === 79) rest = [];
        self._bytes_after_OO = rest;
        self._on_session_end();
        // the Sentry only forwards trailing bytes from inside consume(): hand
        // these over the same way it forwards stray bytes ("garbage" → to_terminal)
        if (rest.length) self._Happen("garbage", rest.slice(0));
      }, ZFIN_OO_WAIT_MS);
    };

    var consumeData = Receive._consume_ZDATA_data;
    Receive._consume_ZDATA_data = function (subpacket) {
      consumeData.call(this, subpacket);
      if (!(subpacket.ack_expected() && subpacket.frame_end())) return;
      this._send_header("ZACK", Zmodem.ENCODELIB.pack_u32_le(this._file_offset));
      var handlers = this._next_header_handler; // { ZEOF } installed by accept()
      handlers.ZDATA = function (hdr) {
        this._consume_ZDATA(hdr);
        this._next_subpacket_handler = this._consume_ZDATA_data;
        this._next_header_handler = handlers;
      };
    };
  }

  if (typeof module === "object" && module.exports) {
    module.exports = { applyLrzszShim: applyLrzszShim };
  } else if (root) {
    root.applyLrzszShim = applyLrzszShim;
    if (root.Zmodem) applyLrzszShim(root.Zmodem);
  }
})(typeof window !== "undefined" ? window : this);
