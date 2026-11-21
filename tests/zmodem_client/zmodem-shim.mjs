// lrzsz compatibility shim for zmodem.js 0.1.10 (receive side).
//
// lsz ends the *first* ZDATA subpacket of every file with ZCRCW (frame end, ACK
// expected), then waits for ZACK and starts a fresh ZDATA header. zmodem.js
// only ACKs ZCRCQ and, after a ZDATA frame, only accepts ZEOF, so every
// download stalls after 1024 bytes. This patch sends the ZACK and accepts the
// next ZDATA header.
//
// Plain ES module with no imports: Node (tests/zmodem_client/zclient.mjs) and a
// browser build (Task 4) both call applyLrzszShim(Zmodem) once at startup.

export function applyLrzszShim(Zmodem) {
  const Receive = Zmodem.Session.Receive.prototype;
  if (Receive._lrzszShim) return;
  Receive._lrzszShim = true;
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
