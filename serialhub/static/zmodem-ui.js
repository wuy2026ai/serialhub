/* =============================================================================
   serialhub Web UI — ZMODEM (rz/sz) file transfer, per port.
   Protocol: docs/ARCHITECTURE.md §14 (WebSocket relay). Reference client with
   the same session handling: tests/zmodem_client/zclient.mjs.

   app.js routes the zmodem* WebSocket messages here and calls init() once:
     init({ wsSend(obj), sendCommand(port, text) -> Promise<{ok}>, t(key, vars),
            paneFor(port) -> pane element | null, activePort() -> name,
            wsBuffered() -> bytes queued on the socket })
     onBroadcast(msg)   {"type":"zmodem","event":"start"|"end",...}
     onClaimed(msg) / onData(msg) / onClosed(msg) / onError(msg)
     onDisconnect()     the socket dropped (the server aborts what we owned)

   Each port's pane holds one dialog ([data-zm]). A `start` broadcast opens it;
   only a click (Receive / Choose files…) claims the session. Payload bytes are
   fed to zmodem.js (window.Zmodem, patched by vendor/zmodem-shim.js) and never
   reach the terminal.
   ========================================================================== */
"use strict";

(function () {
  const TAIL_MS = 200;          // keep feeding the Sentry after session_end (see zclient.mjs)
  const CHUNK = 8192;           // upload subpacket payload per zmodem_write
  const MAX_BUFFERED = 256 * 1024; // upload pacing: wait while the socket holds more than this
  const START_WAIT_MS = 6000;   // after sending rz/sz: give up waiting for the device's start
  const DONE_LINGER_MS = 1200;  // keep the finished dialog visible briefly

  let env = null;               // init() options
  const ports = new Map();      // port -> per-port transfer state

  /* -------------------------------------------------------------- helpers */
  const t = (k, v) => (env ? env.t(k, v) : k);

  function fmtSize(n) {
    if (n == null || !isFinite(n)) return "?";
    if (n < 1024) return n + " B";
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KiB";
    return (n / 1024 / 1024).toFixed(1) + " MiB";
  }

  function toB64(octets) {
    const u8 = octets instanceof Uint8Array ? octets : Uint8Array.from(octets);
    let s = "";
    for (let i = 0; i < u8.length; i += 0x8000) {
      s += String.fromCharCode.apply(null, u8.subarray(i, i + 0x8000));
    }
    return btoa(s);
  }

  function fromB64(b64) {
    const s = atob(b64 || "");
    const out = new Array(s.length);
    for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
    return out;
  }

  // POSIX-shell quoting for the device path in `sz <path>` (only when needed).
  function shellQuote(path) {
    if (/^[A-Za-z0-9_.\/~:@%+=,-]+$/.test(path)) return path;
    return "'" + path.replace(/'/g, "'\\''") + "'";
  }

  function baseName(name) {
    const parts = String(name || "").split(/[\\/]/);
    return parts[parts.length - 1] || "download.bin";
  }

  function saveBlob(name, chunks) {
    const url = URL.createObjectURL(new Blob(chunks, { type: "application/octet-stream" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = baseName(name);
    a.hidden = true;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 60000);
  }

  const zmodemReady = () => !!(window.Zmodem && window.Zmodem.Sentry);

  /* ------------------------------------------------------------ the dialog */
  function dialogFor(port) {
    const pane = env && env.paneFor(port);
    const root = pane && pane.querySelector("[data-zm]");
    if (!root) return null;
    if (!root._zmWired) {
      root._zmWired = true;
      root.querySelector("[data-zm-primary]").addEventListener("click", () => onPrimary(port));
      root.querySelector("[data-zm-cancel]").addEventListener("click", () => onCancel(port));
      root.querySelector("[data-zm-path]").addEventListener("keydown", (e) => {
        if (e.key === "Enter") { e.preventDefault(); onPrimary(port); }
        if (e.key === "Escape") { e.preventDefault(); onCancel(port); }
      });
      const input = root.querySelector("[data-zm-input]");
      input.addEventListener("change", () => onFilesChosen(port, input));
      input.addEventListener("cancel", () => onFilesChosen(port, input));
    }
    return root;
  }

  function setText(el, key, vars) {
    // static keys keep data-i18n so a language switch re-translates them
    if (vars) el.removeAttribute("data-i18n");
    else el.setAttribute("data-i18n", key);
    el.textContent = key ? t(key, vars) : "";
  }

  /* Paint the dialog of `port` from its state (s == null → hidden). */
  function render(port) {
    const root = dialogFor(port);
    if (!root) return;
    const s = ports.get(port);
    if (!s) { root.hidden = true; return; }
    root.hidden = false;
    root.dataset.phase = s.phase;
    root.dataset.direction = s.direction || "";

    const title = root.querySelector("[data-zm-title]");
    const msg = root.querySelector("[data-zm-msg]");
    const primary = root.querySelector("[data-zm-primary]");
    const cancel = root.querySelector("[data-zm-cancel]");
    const pathRow = root.querySelector("[data-zm-pathrow]");

    const titleKey = s.phase === "prompt" ? "zm.promptTitle"
      : s.direction === "upload" ? "zm.uploadDialog"
      : s.direction === "download" ? "zm.downloadDialog" : "zm.waitTitle";
    setText(title, titleKey);

    const msgKey = {
      prompt: "zm.promptMsg", waiting: "zm.waitingDevice", offer: "zm.offerHint",
      claiming: "zm.claiming", active: s.direction === "upload" && !s.files ? "zm.pickFiles" : "",
      finishing: "zm.finishing", inuse: "zm.inUse", done: "zm.done",
    }[s.phase] || "";
    setText(msg, msgKey);
    msg.classList.toggle("is-warn", s.phase === "inuse");

    pathRow.hidden = s.phase !== "prompt";
    const primaryKey = s.phase === "prompt" ? "zm.request"
      : s.direction === "upload" ? "zm.choose" : "zm.receive";
    setText(primary.querySelector("[data-zm-primary-label]"), primaryKey);
    const needFiles = s.phase === "active" && s.direction === "upload" && !s.files;
    primary.hidden = !(s.phase === "prompt" || s.phase === "offer" || s.phase === "inuse" || needFiles);
    primary.disabled = s.phase === "inuse";
    cancel.hidden = s.phase === "done";
    cancel.disabled = s.phase === "inuse" || s.phase === "finishing";
    renderFiles(root, s);
  }

  function renderFiles(root, s) {
    const box = root.querySelector("[data-zm-files]");
    const rows = s.rows || [];
    box.hidden = rows.length === 0;
    while (box.childElementCount > rows.length) box.removeChild(box.lastChild);
    rows.forEach((r, i) => {
      let el = box.children[i];
      if (!el) {
        el = document.createElement("div");
        el.className = "zm-file";
        el.innerHTML = '<div class="zm-file-line"><span class="zm-fname"></span>' +
          '<span class="zm-fbytes"></span><span class="zm-frate"></span></div>' +
          '<div class="zm-bar"><i></i></div>';
        box.appendChild(el);
      }
      const secs = Math.max(0.001, ((r.end || performance.now()) - r.t0) / 1000);
      el.dataset.bytes = String(r.bytes);
      el.dataset.done = r.done ? "1" : "";
      el.querySelector(".zm-fname").textContent = r.name;
      el.querySelector(".zm-fname").title = r.name;
      el.querySelector(".zm-fbytes").textContent =
        fmtSize(r.bytes) + " / " + fmtSize(r.size);
      el.querySelector(".zm-frate").textContent = t("zm.rate", { rate: fmtSize(Math.round(r.bytes / secs)) });
      const pct = r.size ? Math.min(100, (r.bytes / r.size) * 100) : (r.done ? 100 : 0);
      el.querySelector(".zm-bar i").style.width = pct.toFixed(1) + "%";
      el.classList.toggle("is-done", !!r.done);
    });
  }

  function scheduleRender(port) {
    const s = ports.get(port);
    if (!s || s.raf) return;
    s.raf = requestAnimationFrame(() => { s.raf = 0; render(port); });
  }

  /* Retire a state object: its timers stop and alive(s) turns false, so a
     still-running upload loop / sender / tail timer of it goes quiet. */
  function dispose(s) {
    s.finished = true;
    clearTimeout(s.waitTimer);
    clearTimeout(s.lingerTimer);
    if (s.raf) { cancelAnimationFrame(s.raf); s.raf = 0; }
  }

  function close(port) {
    const s = ports.get(port);
    if (s) dispose(s);
    ports.delete(port);
    render(port);
  }

  // The session this state belongs to is still ours to drive.
  const alive = (s) => ports.get(s.port) === s && !s.finished && !s.endSent && !s.failed;

  /* ------------------------------------------------------- toolbar actions */
  function requireReady(port) {
    if (!port) { env.toast(t("zm.noPort"), "warn"); return false; }
    if (!zmodemReady()) { env.toast(t("zm.unavailable"), "error"); return false; }
    const s = ports.get(port);
    if (s && s.phase !== "prompt" && s.phase !== "waiting") {
      env.toast(t("zm.busy", { port }), "warn");
      return false;
    }
    return true;
  }

  function requestUpload() {
    const port = env.activePort();
    if (!requireReady(port)) return;
    runCommand(port, "rz");
  }

  function requestDownload() {
    const port = env.activePort();
    if (!requireReady(port)) return;
    ports.set(port, { port, phase: "prompt", direction: null });
    render(port);
    const input = dialogFor(port) && dialogFor(port).querySelector("[data-zm-path]");
    if (input) { input.value = ""; input.focus(); }
  }

  function runCommand(port, text) {
    const s = { port, phase: "waiting", direction: null };
    ports.set(port, s);
    render(port);
    s.waitTimer = setTimeout(() => {
      if (ports.get(port) === s && s.phase === "waiting") {
        close(port);
        env.toast(t("zm.noStart", { port }), "warn");
      }
    }, START_WAIT_MS);
    Promise.resolve(env.sendCommand(port, text)).then((r) => {
      // the server already toasted the error (e.g. 409 while a session is active)
      if (!(r && r.ok) && ports.get(port) === s && s.phase === "waiting") close(port);
    });
  }

  /* ------------------------------------------------------- dialog buttons */
  function onPrimary(port) {
    const s = ports.get(port);
    if (!s) return;
    if (s.phase === "prompt") {
      const input = dialogFor(port).querySelector("[data-zm-path]");
      const path = input.value.trim();
      if (!path) { input.focus(); env.toast(t("zm.needPath"), "warn"); return; }
      runCommand(port, "sz " + shellQuote(path));
    } else if (s.phase === "offer") {
      s.phase = "claiming";
      render(port);
      env.wsSend({ type: "zmodem_claim", port });
    } else if (s.phase === "active" && s.direction === "upload" && !s.files) {
      openPicker(port);
    }
  }

  // Upload: the picker opens once the claim succeeded (still inside the
  // click's transient user activation); [Choose files…] stays as a fallback.
  function openPicker(port) {
    const root = dialogFor(port);
    if (!root) return;
    const input = root.querySelector("[data-zm-input]");
    input.value = "";
    input.click();
  }

  function onCancel(port) {
    const s = ports.get(port);
    if (!s) return;
    if (s.phase === "prompt" || s.phase === "waiting") { close(port); return; }
    if (s.phase === "inuse" || s.phase === "finishing" || s.phase === "done") return;
    s.cancelled = true;
    abort(s);
    if (s.phase === "offer") {             // nothing of ours to wait for
      env.toast(t("zm.cancelled", { port }), "info");
      close(port);
    } else { s.phase = "finishing"; render(port); }
  }

  function onFilesChosen(port, input) {
    const s = ports.get(port);
    if (!s || s.direction !== "upload" || s.files) return;
    const list = Array.from(input.files || []);
    input.value = "";
    if (!list.length) {            // picker dismissed
      if (s.phase === "claiming" || s.phase === "active") onCancel(port);
      return;
    }
    s.files = list;
    if (s.resolveFiles) s.resolveFiles(list);
    render(port);
  }

  /* ------------------------------------------------------ session handling */
  function abort(s) {
    if (s.endSent) return;
    s.endSent = true;
    env.wsSend({ type: "zmodem_end", port: s.port, session: s.sid || s.session, abort: true });
  }

  function fail(s, err) {
    if (ports.get(s.port) !== s || s.failed || s.finished) return;
    s.failed = true;
    if (!s.zmEnded) abort(s);
    env.toast(t("zm.failed", { reason: String(err && err.message ? err.message : err) }), "error");
    close(s.port);
  }

  function endDone(s) {
    if (!alive(s)) return;
    s.endSent = true;
    const msg = { type: "zmodem_end", port: s.port, session: s.sid, abort: false };
    // a late "OO" (after the shim already ended the session) is protocol, not text
    if (s.tail[0] === 79 && s.tail[1] === 79) s.tail.splice(0, 2);
    if (s.tail.length) msg.tail_b64 = toB64(s.tail);
    env.wsSend(msg);
  }

  function startSentry(s) {
    s.tail = [];
    s.rows = [];
    return new window.Zmodem.Sentry({
      to_terminal: (octets) => {
        if (s.zmEnded) for (const b of octets) s.tail.push(b);
      },
      on_retract: () => {},
      sender: (octets) => {
        if (alive(s)) {
          env.wsSend({ type: "zmodem_write", port: s.port, session: s.sid, b64: toB64(octets) });
        }
      },
      on_detect: (detection) => {
        if (s.zsession) return;    // one zmodem.js session per relay session
        const zsession = detection.confirm();
        s.zsession = zsession;
        zsession.on("session_end", () => {
          if (zsession.aborted()) return fail(s, t("zm.aborted"));
          s.zmEnded = true;
          s.phase = "finishing";
          scheduleRender(s.port);
          setTimeout(() => endDone(s), TAIL_MS);
        });
        if (zsession.type === "receive") receive(s, zsession);
        else upload(s, zsession).catch((e) => fail(s, e));
      },
    });
  }

  function receive(s, zsession) {
    s.pendingSaves = [];
    zsession.on("offer", (offer) => {
      const d = offer.get_details();
      const row = { name: baseName(d.name), size: d.size, bytes: 0, t0: performance.now() };
      s.rows.push(row);
      scheduleRender(s.port);
      const chunks = [];
      s.pendingSaves.push(offer.accept({
        on_input: (payload) => {
          chunks.push(Uint8Array.from(payload));
          row.bytes += payload.length;
          scheduleRender(s.port);
        },
      }).then(() => {
        row.done = true;
        row.end = performance.now();
        saveBlob(row.name, chunks);
        scheduleRender(s.port);
      }));
    });
    zsession.start();
  }

  function waitDrain(s) {
    return new Promise((resolve) => {
      (function poll() {
        if (!alive(s) || env.wsBuffered() <= MAX_BUFFERED) resolve();
        else setTimeout(poll, 15);
      })();
    });
  }

  async function upload(s, zsession) {
    const files = s.files || await new Promise((resolve) => { s.resolveFiles = resolve; });
    // every await below may outlive the session (server end, reconnect): re-check
    if (!alive(s)) return;
    let remaining = files.reduce((n, f) => n + f.size, 0);
    for (let i = 0; i < files.length; i++) {
      const f = files[i];
      const data = new Uint8Array(await f.arrayBuffer());
      if (!alive(s)) return;
      const row = { name: f.name, size: f.size, bytes: 0, t0: performance.now() };
      s.rows.push(row);
      scheduleRender(s.port);
      const xfer = await zsession.send_offer({
        name: f.name,
        size: data.length,
        mtime: new Date(f.lastModified),
        files_remaining: files.length - i,
        bytes_remaining: remaining,
      });
      remaining -= data.length;
      if (!alive(s)) return;
      if (!xfer) { row.skipped = row.done = true; scheduleRender(s.port); continue; }
      let off = 0;
      for (; off + CHUNK < data.length; off += CHUNK) {
        if (!alive(s)) return;
        xfer.send(data.subarray(off, off + CHUNK));
        row.bytes = off + CHUNK;
        scheduleRender(s.port);
        if (env.wsBuffered() > MAX_BUFFERED) await waitDrain(s);
      }
      if (!alive(s)) return;
      await xfer.end(data.subarray(off));
      row.bytes = data.length;
      row.done = true;
      row.end = performance.now();
      scheduleRender(s.port);
    }
    if (alive(s)) await zsession.close();
  }

  /* The relay session ended (zmodem_closed, or the end broadcast as fallback).
     The toast always shows; the dialog only while this state is still current
     (a new `start` may already have replaced it). */
  function finish(s, reason) {
    if (s.finished) return;
    s.finished = true;
    const ok = reason === "done" && s.zmEnded && !s.failed;
    const current = () => ports.get(s.port) === s;
    const saves = ok && s.pendingSaves ? Promise.all(s.pendingSaves) : Promise.resolve();
    saves.then(() => {
      if (ok) {
        const n = (s.rows || []).filter((r) => !r.skipped).length;
        env.toast(t(s.direction === "upload" ? "zm.sent" : "zm.received", { n, port: s.port }), "ok");
        if (!current()) return;
        s.phase = "done";
        render(s.port);
        s.lingerTimer = setTimeout(() => { if (current()) close(s.port); }, DONE_LINGER_MS);
      } else {
        if (s.cancelled) env.toast(t("zm.cancelled", { port: s.port }), "info");
        else if (!s.failed) env.toast(t("zm.failed", { reason: reasonText(reason) }), "error");
        if (current()) close(s.port);
      }
    }, (e) => {
      env.toast(t("zm.failed", { reason: String(e && e.message ? e.message : e) }), "error");
      if (current()) close(s.port);
    });
  }

  function reasonText(reason) {
    const key = "zm.reason." + reason;
    const txt = t(key);
    return txt === key ? String(reason) : txt;
  }

  /* ------------------------------------------------------ message routing */
  function onBroadcast(msg) {
    const port = msg.port;
    if (!port) return;
    const s = ports.get(port);
    if (msg.event === "start") {
      if (s && s.sid && !s.finished) {
        // ours has not seen zmodem_closed yet; one session per port, so if its
        // end broadcast arrived the old one is over — finish it now
        if (!s.endReason) return;
        finish(s, s.endReason);
      }
      if (s) dispose(s);
      ports.set(port, {
        port, phase: "offer", session: msg.session, direction: msg.direction,
      });
      render(port);
    } else if (msg.event === "claimed") {
      // another browser took it (ours is still `claiming` and learns from the reply)
      if (s && s.session === msg.session && s.phase === "offer") {
        s.phase = "inuse";
        render(port);
      }
    } else if (msg.event === "end") {
      if (!s || s.session !== msg.session) return;
      if (s.sid) {
        // ours: zmodem_closed normally follows; fall back to the broadcast
        s.endReason = msg.reason || "closed";
        setTimeout(() => finish(s, s.endReason), 1500);
        return;
      }
      if (s.cancelled) env.toast(t("zm.cancelled", { port }), "info");
      else if (s.phase === "offer" && msg.reason === "unclaimed") {
        env.toast(t("zm.unclaimed", { port }), "warn");
      }
      close(port);
    }
  }

  function onClaimed(msg) {
    const s = ports.get(msg.port);
    if (!s || s.phase !== "claiming" || s.sid) return;
    if (s.session && msg.session !== s.session) {
      // claimed a newer session than the dialog showed: the old one is gone
      s.session = msg.session;
    }
    s.sid = msg.session;
    if (msg.direction) s.direction = msg.direction;
    s.phase = "active";
    render(msg.port);
    try { s.sentry = startSentry(s); }
    catch (e) { fail(s, e); return; }
    if (s.direction === "upload") openPicker(msg.port);
  }

  function onData(msg) {
    const s = ports.get(msg.port);
    if (!s || !s.sentry || msg.session !== s.sid || s.failed) return;
    try { s.sentry.consume(fromB64(msg.b64)); }
    catch (e) { fail(s, e); }
  }

  function onClosed(msg) {
    const s = ports.get(msg.port);
    if (!s || msg.session !== s.sid) return;
    finish(s, msg.reason);
  }

  /* zmodem_error carries `code`, and echoes `session` for write/end. Claim
     errors have no session; anything else must match the live session, so a
     late error of an old session never touches a new one. */
  function onError(msg) {
    const s = ports.get(msg.port);
    if (!s) return;
    const err = String(msg.error || "");
    if (msg.session == null) {
      if (s.phase !== "claiming") return;
      if (msg.code === "claimed_by_other") { s.phase = "inuse"; render(msg.port); return; }
      if (msg.code === "no_session") { close(msg.port); return; }
      fail(s, err);
    } else if (s.sid && msg.session === s.sid && alive(s)) {
      fail(s, err);
    }
  }

  function onDisconnect() {
    Array.from(ports.values()).forEach((s) => {
      if (s.phase === "prompt") return;
      if (s.sid && !s.endSent) env.toast(t("zm.failed", { reason: reasonText("disconnect") }), "error");
      close(s.port);
    });
  }

  function init(opts) {
    env = Object.assign({
      wsBuffered: () => 0,
      toast: () => {},
      activePort: () => null,
      paneFor: () => null,
    }, opts);
    const rz = document.getElementById("btnRz");
    const sz = document.getElementById("btnSz");
    if (rz) rz.addEventListener("click", requestUpload);
    if (sz) sz.addEventListener("click", requestDownload);
    if (window.__zmodemReady) {
      window.__zmodemReady.then((ok) => { if (!ok) [rz, sz].forEach((b) => b && (b.disabled = true)); });
    }
  }

  window.ZmodemUI = {
    init, onBroadcast, onClaimed, onData, onClosed, onError, onDisconnect,
    _shellQuote: shellQuote,
  };
})();
