/* =============================================================================
   serialhub Web UI — vanilla JS, no build step, no npm.
   Sections:
     1. utilities            6. terminal / panes / rendering
     2. state                7. stats (rates + sparkline)
     3. toasts               8. websocket bus
     4. API client           9. filter + search
     5. theme               10. sendbar + export + toolbar wiring + init
   ========================================================================== */
"use strict";

/* 1. utilities ============================================================= */
const ANSI_RE = /\x1b\[[0-9;?]*[ -\/]*[@-~]|\x1b[@-Z\\-_]/g;
const encoder = new TextEncoder();

const $ = (sel, root) => (root || document).querySelector(sel);
const $$ = (sel, root) => Array.prototype.slice.call((root || document).querySelectorAll(sel));

const stripAnsi = (s) => String(s == null ? "" : s).replace(ANSI_RE, "");
const byteLen = (s) => encoder.encode(s || "").length;
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}
function fmtTime(ms) {
  const d = new Date(ms || Date.now());
  const p2 = (n) => String(n).padStart(2, "0");
  const p3 = (n) => String(n).padStart(3, "0");
  return `${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}.${p3(d.getMilliseconds())}`;
}
function fmtBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return n + "B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + "K";
  if (n < 1024 * 1024 * 1024) return (n / 1048576).toFixed(1) + "M";
  return (n / 1073741824).toFixed(1) + "G";
}
function fmtRate(n) { return n >= 1000 ? (n / 1000).toFixed(1) + "k" : String(Math.round(n)); }
function fmtDuration(ms) {
  const s = Math.floor((ms || 0) / 1000);
  const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60), ss = s % 60;
  if (d) return `${d}d${h}h`;
  if (h) return `${h}h${m}m`;
  if (m) return `${m}m${ss}s`;
  return `${ss}s`;
}
function compileRe(pattern) {
  if (!pattern) return null;
  try { return new RegExp(pattern, "i"); } catch (_e) { return null; }
}
function safeGet(key, fallback) {
  try { const v = localStorage.getItem(key); return v == null ? fallback : v; } catch (_e) { return fallback; }
}
function safeSet(key, val) {
  try { localStorage.setItem(key, val); } catch (_e) { /* storage disabled */ }
}

/* 1b. i18n ================================================================ */
/* Locale dictionaries live in static/i18n/<code>.js and self-register with
   window.SERIALHUB_I18N (defined inline in index.html). app.js holds no
   dictionary of its own: the active dictionary is derived from that registry,
   so adding a language = one new file + one <script> line in index.html.
   Fallback chain for t(key, vars): active locale -> en -> the key itself. */
const i18nRegistry = window.SERIALHUB_I18N || {
  _d: {}, _meta: {},
  register() {},
  has() { return false; },
  langs() { return []; },
  meta() { return {}; },
  dict() { return {}; },
};

/* Dictionary for `lang`, or the `en` baseline when it is missing or empty. */
function dictFor(lang) {
  const d = i18nRegistry.dict(lang);
  return (d && Object.keys(d).length) ? d : i18nRegistry.dict("en");
}

function t(key, vars) {
  const dict = dictFor(state.lang);
  let s = dict[key];
  if (s == null) s = i18nRegistry.dict("en")[key];
  if (s == null) s = key;
  if (vars) {
    s = s.replace(/\{(\w+)\}/g, (m, k) => (vars[k] == null ? m : String(vars[k])));
  }
  return s;
}

/* stored -> browser prefers zh -> 'zh' -> else 'en'; a stored value that is no
   longer registered (e.g. its locale file was removed) is ignored. */
function detectLang() {
  const saved = safeGet("serialhub.lang", "");
  if (saved && i18nRegistry.has(saved)) return saved;
  const list = (navigator.languages && navigator.languages.length)
    ? navigator.languages : [navigator.language || "en"];
  return String(list[0] || "").toLowerCase().startsWith("zh") ? "zh" : "en";
}

/* Re-apply all static text to the DOM subtree rooted at `root` (default doc).
   data-i18n -> textContent, data-i18n-title -> title,
   data-i18n-aria -> aria-label, data-i18n-ph -> placeholder. */
function applyI18n(root) {
  root = root || document;
  $$("[data-i18n]", root).forEach((el) => { el.textContent = t(el.getAttribute("data-i18n")); });
  $$("[data-i18n-title]", root).forEach((el) => { el.title = t(el.getAttribute("data-i18n-title")); });
  $$("[data-i18n-aria]", root).forEach((el) => el.setAttribute("aria-label", t(el.getAttribute("data-i18n-aria"))));
  $$("[data-i18n-ph]", root).forEach((el) => { el.placeholder = t(el.getAttribute("data-i18n-ph")); });
}

/* 2. state ================================================================= */
const state = {
  config: null,
  ports: new Map(),        // name -> server-side port summary
  runtimes: new Map(),     // name -> client runtime (terminal + buffers)
  active: null,
  ws: null,
  wsBackoff: 500,
  wsBackoffMax: 12000,
  wsTimer: null,
  stopped: false,   // true once the daemon was told to exit (or broadcast "shutdown")
  timers: [],       // setInterval ids, cancelled when the service stops
  wsState: "idle",
  token: safeGet("serialhub.token", ""),
  theme: safeGet("serialhub.theme", ""),
  lang: detectLang(),
  timestamp: safeGet("serialhub.timestamp", "") !== "false",
  autoscroll: safeGet("serialhub.autoscroll", "") !== "false",
  paused: false,
  maxRenderLines: 5000,
  termFallback: false,
  sendHistory: [],
  sendIndex: -1,
};

/* 3. toasts ================================================================ */
const toastHost = () => $("#toasts");
function toast(message, kind, ttl) {
  const el = document.createElement("div");
  el.className = "toast " + (kind || "info");
  el.innerHTML = `<span class="t-msg">${escapeHtml(message)}</span>`;
  const close = document.createElement("button");
  close.className = "t-close";
  close.textContent = "\u00d7";
  close.setAttribute("aria-label", t("toastDismiss"));
  close.addEventListener("click", () => dismiss());
  el.appendChild(close);
  function dismiss() {
    el.classList.add("out");
    setTimeout(() => el.remove(), 200);
  }
  toastHost().appendChild(el);
  setTimeout(dismiss, ttl || (kind === "error" ? 8000 : 4000));
}

/* 4. API client ============================================================ */
function apiUrl(path, params) {
  const u = new URL(path, location.origin);
  if (params) {
    for (const k of Object.keys(params)) {
      const v = params[k];
      if (v === undefined || v === null || v === "") continue;
      u.searchParams.set(k, String(v));
    }
  }
  return u.toString();
}
async function api(method, path, opts) {
  opts = opts || {};
  const headers = {};
  if (state.token) headers["Authorization"] = "Bearer " + state.token;
  const init = { method, headers };
  if (opts.body !== undefined) {
    headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(opts.body);
  }
  let res;
  try {
    res = await fetch(apiUrl(path, opts.params), init);
  } catch (e) {
    toast(t("toastNetworkError", { msg: e.message }), "error");
    return { ok: false, error: String(e) };
  }
  let data;
  try { data = await res.json(); } catch (_e) { data = { ok: false, error: "invalid JSON (HTTP " + res.status + ")" }; }
  if (!data.ok) {
    const msg = data.error || ("HTTP " + res.status);
    if (res.status === 401) toast(t("toastUnauthorized"), "error");
    else toast(msg, "error");
  }
  return data;
}

/* 5. theme ================================================================= */
const XTERM_THEME = {
  dark: {
    background: "#0b0d11", foreground: "#e8edf3", cursor: "#e8834a", cursorAccent: "#0b0d11",
    selectionBackground: "rgba(232,131,74,0.28)",
    black: "#1a1f28", red: "#ef6d6d", green: "#4fd1b0", yellow: "#e6b34a",
    blue: "#6aa6f5", magenta: "#c792ea", cyan: "#4fd1b0", white: "#c9d1dc",
    brightBlack: "#66707e", brightRed: "#ff8787", brightGreen: "#6ee7c8", brightYellow: "#f3ce74",
    brightBlue: "#8bbcf7", brightMagenta: "#d9abf5", brightCyan: "#6ee7c8", brightWhite: "#ffffff",
  },
  light: {
    background: "#ffffff", foreground: "#1b1f26", cursor: "#cf5a1e", cursorAccent: "#ffffff",
    selectionBackground: "rgba(207,90,30,0.20)",
    black: "#1b1f26", red: "#cf3b3b", green: "#109c81", yellow: "#a8790c",
    blue: "#2f6fd0", magenta: "#8a4fb0", cyan: "#109c81", white: "#5a6472",
    brightBlack: "#878f9c", brightRed: "#e05555", brightGreen: "#14b596", brightYellow: "#c08e14",
    brightBlue: "#3f82e0", brightMagenta: "#9d5fc4", brightCyan: "#14b596", brightWhite: "#1b1f26",
  },
};
function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  safeSet("serialhub.theme", theme);
  state.theme = theme;
  state.runtimes.forEach((rt) => {
    if (rt.term) rt.term.options.theme = XTERM_THEME[theme] || XTERM_THEME.dark;
  });
}
function initTheme() {
  if (!state.theme) {
    const prefersLight = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches;
    state.theme = prefersLight ? "light" : "dark";
  }
  applyTheme(state.theme);
}

/* 5b. language ============================================================ */
function refreshPauseBtn() {
  const span = $("[data-pause-label]", $("#btnPause"));
  if (span) span.textContent = t(state.paused ? "resume" : "pause");
}
function updateLangSwitcher() {
  $$(".lang-btn").forEach((b) => {
    const on = b.dataset.lang === state.lang;
    b.classList.toggle("on", on);
    b.setAttribute("aria-pressed", String(on));
  });
}
/* Build one segmented-control button per REGISTERED locale, so dropping a new
   static/i18n/<code>.js file adds its button automatically. */
function buildLangSwitcher() {
  const host = $("#langSwitch");
  if (!host) return;
  host.innerHTML = "";
  i18nRegistry.langs().forEach((code) => {
    const meta = i18nRegistry.meta(code);
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "lang-btn";
    btn.dataset.lang = code;
    btn.textContent = meta.label || code.toUpperCase();
    if (meta.name) btn.title = meta.name;
    btn.addEventListener("click", () => applyLang(code));
    host.appendChild(btn);
  });
}
function applyLang(lang) {
  if (!i18nRegistry.has(lang)) lang = i18nRegistry.has("en") ? "en" : (i18nRegistry.langs()[0] || "en");
  state.lang = lang;
  safeSet("serialhub.lang", lang);
  const meta = i18nRegistry.meta(lang);
  document.documentElement.lang = meta.htmlLang || lang;
  document.title = t("docTitle");
  applyI18n(document);
  refreshPauseBtn();
  $("#connText").textContent = t(conn.key, conn.vars);
  updateConfigIndicator();
  state.runtimes.forEach((rt) => { applyI18n(rt.pane); updatePortHeader(rt); });
  if (openDevPop && deviceCache) renderDeviceList(openDevPop.rt, deviceCache);
  updateLangSwitcher();
}

/* 6. terminal / panes / rendering ========================================== */
const termHosts = new WeakMap();

function createRuntime(name) {
  const tpl = $("#paneTemplate").content.firstElementChild.cloneNode(true);
  applyI18n(tpl);   // translate the freshly cloned pane into the current language
  const rt = {
    name,
    pane: tpl,
    term: null, fit: null, ro: null,
    pend: { shown: "", want: "", raf: 0 },   // unterminated tail ("partial"), see pendSet
    lastSeq: null, histBusy: false, liveQ: [], clearGen: 0,   // history/dedupe
    lines: [],          // retained records for filter/search/fallback
    deferred: [],       // lines buffered while paused
    hits: [], hitIdx: -1,
    filter: "", filterRe: null,
    lineCount: 0, byteCount: 0,
    last: { lines: 0, bytes: 0 }, hist: [],
    writeIndex: 0,
    state: "closed",
    el: {
      stateDot: $(".state-dot", tpl),
      stateLabel: $(".state-label", tpl),
      name: $(".port-name", tpl),
      meta: $(".port-meta", tpl),
      spark: $(".spark", tpl),
      rateLines: $(".rate-lines", tpl),
      rateBytes: $(".rate-bytes", tpl),
      bufPct: $(".buf-pct", tpl),
      termHost: $(".term-host", tpl),
      fallback: $(".log-fallback", tpl),
      portToggle: $("[data-port-toggle]", tpl),
      portToggleLabel: $("[data-port-toggle-label]", tpl),
      pins: $("[data-port-pins]", tpl),
      pinDtr: $('[data-pin="dtr"]', tpl),
      pinRts: $('[data-pin="rts"]', tpl),
      pinReset: $("[data-pin-reset]", tpl),
      devChip: $(".dev-chip", tpl),
      devName: $(".dev-name", tpl),
      devDesc: $(".dev-desc", tpl),
      devAuto: $(".dev-auto", tpl),
      devBtn: $("[data-dev-open]", tpl),
      devPop: $("[data-dev-pop]", tpl),
      devList: $("[data-dev-list]", tpl),
      devRefresh: $("[data-dev-refresh]", tpl),
      devMatch: $("[data-dev-match]", tpl),
      devApply: $("[data-dev-apply]", tpl),
      devMapOnly: $("[data-dev-maponly]", tpl),
    },
    deviceSource: null,   // "device" | "match" | null (null => derive from config)
    pins: null,           // last GET/POST /pins payload for this port
    pinsProbed: false,    // capability probe already answered (or moot)
    pinsProbing: false,   // a probe request is in flight
    pinsProbeAt: 0,       // throttle for re-probes
    pinsBusy: false,      // a pin action is in flight
    portBusy: false,      // an open/close request is in flight
  };
  rt.el.name.textContent = name;
  wireDeviceControl(rt);
  wirePortControl(rt);
  $("#panes").appendChild(tpl);
  termHosts.set(rt.el.termHost, rt);
  createTerminal(rt);
  refreshPortToggle(rt);   // seed the toggle in the active language
  updatePinsUI(rt);
  return rt;
}

/* clipboard: xterm renders its own selection (no native DOM selection), so we
   wire Ctrl+C / Ctrl+Insert to copy it; keys without a selection pass through
   untouched so the serial console still receives control bytes (e.g. ^C). */
function fallbackCopy(text) {
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.top = "-1000px";
    ta.style.left = "-1000px";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    const sel = document.getSelection();
    const prev = sel && sel.rangeCount ? sel.getRangeAt(0) : null;
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    if (sel) { sel.removeAllRanges(); if (prev) sel.addRange(prev); }
    return !!ok;
  } catch (_e) { return false; }
}
function copyText(text) {
  return new Promise((resolve) => {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text)
        .then(() => resolve(true))
        .catch(() => resolve(fallbackCopy(text)));
    } else {
      resolve(fallbackCopy(text));
    }
  });
}
function copySelection(text) {
  if (!text) return;
  copyText(text).then((ok) => { if (ok) toast(t("copied", { n: text.length }), "ok"); });
}
function isCopyChord(event) {
  if (!event.ctrlKey || event.altKey || event.metaKey) return false;
  if (event.code === "KeyC" || event.key === "c" || event.key === "C") return true;   // Ctrl+C
  if (event.code === "Insert" || event.key === "Insert") return true;                 // Ctrl+Insert
  return false;
}
function attachCopyHandler(rt, term) {
  term.attachCustomKeyEventHandler((event) => {
    if (event.type !== "keydown") return true;              // never touch keyup/keypress
    if (isCopyChord(event) && term.hasSelection()) {
      copySelection(term.getSelection());
      return false;                                         // handled — do NOT send to serial
    }
    return true;                                            // everything else passes through
  });
  // Right-click with a live selection copies it too (discoverability).
  rt.el.termHost.addEventListener("contextmenu", (event) => {
    if (term.hasSelection()) { event.preventDefault(); copySelection(term.getSelection()); }
  });
}

function createTerminal(rt) {
  if (state.termFallback || typeof window.Terminal === "undefined") {
    rt.el.termHost.hidden = true;
    rt.el.fallback.hidden = false;
    return;
  }
  const term = new window.Terminal({
    // same local-first stack as --mono in style.css (no webfont is fetched)
    fontFamily: '"IBM Plex Mono", ui-monospace, "SF Mono", "SFMono-Regular", Menlo, Consolas, "Cascadia Mono", "Cascadia Code", "JetBrains Mono", "Liberation Mono", monospace',
    fontSize: 12.5,
    lineHeight: 1.28,
    letterSpacing: 0,
    cursorBlink: false,
    cursorStyle: "bar",
    scrollback: Math.max(1000, state.maxRenderLines),
    convertEol: false,
    allowProposedApi: true,
    theme: XTERM_THEME[state.theme] || XTERM_THEME.dark,
  });
  rt.term = term;
  attachCopyHandler(rt, term);
  if (window.FitAddon && window.FitAddon.FitAddon) {
    rt.fit = new window.FitAddon.FitAddon();
    term.loadAddon(rt.fit);
  }
  term.open(rt.el.termHost);
  refit(rt);
  if (rt.fit) {
    const ro = new ResizeObserver(() => refit(rt));
    ro.observe(rt.el.termHost);
    rt.ro = ro;
  }
}

function refit(rt) {
  if (!rt.fit) return;
  try {
    const box = rt.el.termHost.getBoundingClientRect();
    if (box.width > 8 && box.height > 8) rt.fit.fit();
  } catch (_e) { /* fit can throw while hidden */ }
}

function ensurePort(name) {
  if (state.runtimes.has(name)) return state.runtimes.get(name);
  const rt = createRuntime(name);
  state.runtimes.set(name, rt);
  scheduleTabsRefresh();
  return rt;
}

/* Tear down a pane that no longer belongs to the active config: close any open
   popover bound to it, dispose the xterm instance + its ResizeObserver, detach
   the pane from the DOM and drop the runtime (buffers die with it). */
function destroyRuntime(name) {
  const rt = state.runtimes.get(name);
  if (!rt) return;
  if (openDevPop && openDevPop.rt === rt) closeDevicePop();
  if (rt.ro) { try { rt.ro.disconnect(); } catch (_e) { /* ignore */ } rt.ro = null; }
  if (rt.fit) { try { rt.fit.dispose(); } catch (_e) { /* ignore */ } rt.fit = null; }
  if (rt.term) { try { rt.term.dispose(); } catch (_e) { /* ignore */ } rt.term = null; }
  termHosts.delete(rt.el.termHost);
  if (rt.pane && rt.pane.parentNode) rt.pane.parentNode.removeChild(rt.pane);
  state.runtimes.delete(name);
}

/* 6b. device control (resolved COM chip + runtime change popover) ========== */
/* Reuses the backend contract: GET /api/devices lists system ports; POST
   /api/ports/{name}/device swaps the device (fixed `device`) or the match spec
   (`match`) at runtime. The chip mirrors the resolved COM + the port's note and
   tags itself "auto" when the port is match-driven. */
let openDevPop = null;      // { rt, pop, btn } — the single open popover
let deviceCache = null;     // last GET /api/devices payload (for re-render on lang switch)

function wireDeviceControl(rt) {
  const popId = "dev-pop-" + String(rt.name).replace(/[^A-Za-z0-9_-]/g, "_");
  rt.el.devPop.id = popId;
  rt.el.devBtn.setAttribute("aria-controls", popId);
  rt.el.devBtn.addEventListener("click", () => toggleDevicePop(rt));
  rt.el.devRefresh.addEventListener("click", () => refreshDeviceList(rt));
  rt.el.devApply.addEventListener("click", () => applyMatchSpec(rt));
  rt.el.devMatch.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); applyMatchSpec(rt); }
  });
}

/* "match" when the port is auto-discovered (a match spec is configured and no
   explicit device was applied this session), else "device". An explicit runtime
   change stamps rt.deviceSource and wins. */
function deviceSource(name) {
  const rt = state.runtimes.get(name);
  if (rt && rt.deviceSource) return rt.deviceSource;
  const pc = state.portConfig(name);
  if (pc && pc.match && Object.keys(pc.match).length) return "match";
  return "device";
}

function updateDeviceChip(rt) {
  const meta = state.ports.get(rt.name) || {};
  const dev = meta.device || "";
  rt.el.devName.textContent = dev || "\u2013";
  rt.el.devDesc.textContent = meta.description || "";
  const auto = deviceSource(rt.name) === "match";
  rt.el.devAuto.hidden = !auto;
  const parts = [];
  if (dev) parts.push(dev);
  if (meta.description) parts.push(meta.description);
  if (auto) parts.push(t("device.auto"));
  rt.el.devChip.title = parts.join("  \u00b7  ");
}

function closeDevicePop() {
  if (!openDevPop) return;
  openDevPop.pop.hidden = true;
  openDevPop.btn.setAttribute("aria-expanded", "false");
  openDevPop = null;
}

function toggleDevicePop(rt) {
  if (openDevPop && openDevPop.rt === rt) { closeDevicePop(); return; }
  closeDevicePop();
  rt.el.devPop.hidden = false;
  rt.el.devBtn.setAttribute("aria-expanded", "true");
  openDevPop = { rt, pop: rt.el.devPop, btn: rt.el.devBtn };
  refreshDeviceList(rt);
  rt.el.devMatch.focus();
}

/* Parse a match spec exactly like the CLI's `--match`: a JSON object, or
   compact `k=v` pairs (values stay strings, so `vid=0x1A86` passes through). */
function parseMatch(text) {
  const s = String(text == null ? "" : text).trim();
  if (!s) throw new Error(t("device.match_invalid"));
  if (s.charAt(0) === "{") {
    let obj;
    try { obj = JSON.parse(s); } catch (_e) { throw new Error(t("device.match_invalid")); }
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) throw new Error(t("device.match_invalid"));
    return obj;
  }
  const spec = {};
  s.split(",").forEach((part) => {
    part = part.trim();
    if (!part) return;
    const eq = part.indexOf("=");
    if (eq <= 0) throw new Error(t("device.match_invalid"));
    const key = part.slice(0, eq).trim();
    if (!key) throw new Error(t("device.match_invalid"));
    spec[key] = part.slice(eq + 1).trim();
  });
  if (!Object.keys(spec).length) throw new Error(t("device.match_invalid"));
  return spec;
}

async function refreshDeviceList(rt) {
  rt.el.devList.innerHTML = '<div class="dev-empty">\u2026</div>';
  const data = await api("GET", "/api/devices");
  if (!data.ok) {
    rt.el.devList.innerHTML = '<div class="dev-empty">' + escapeHtml(t("device.loadFailed")) + "</div>";
    return;
  }
  deviceCache = data.devices || [];
  renderDeviceList(rt, deviceCache);
}

function renderDeviceList(rt, devices) {
  const cur = (state.ports.get(rt.name) || {}).device;
  const list = rt.el.devList;
  list.innerHTML = "";
  if (!devices.length) {
    list.innerHTML = '<div class="dev-empty">' + escapeHtml(t("device.noDevices")) + "</div>";
    return;
  }
  devices.forEach((d) => {
    const row = document.createElement("button");
    row.type = "button";
    row.setAttribute("role", "option");
    const isCur = d.device === cur;
    row.className = "dev-row" + (isCur ? " is-current" : "");
    row.setAttribute("aria-selected", String(isCur));
    if (isCur) row.setAttribute("aria-current", "true");
    row.innerHTML = '<span class="dr-dev">' + escapeHtml(d.device || "") + "</span>" +
      '<span class="dr-desc">' + escapeHtml(d.description || "") + "</span>" +
      (isCur ? '<span class="dr-cur">' + escapeHtml(t("device.current")) + "</span>" : "");
    row.addEventListener("click", () => applyDevice(rt, { device: d.device }));
    list.appendChild(row);
  });
}

function applyMatchSpec(rt) {
  let spec;
  try { spec = parseMatch(rt.el.devMatch.value); }
  catch (e) { toast(t("device.error", { msg: e.message }), "error"); return; }
  applyDevice(rt, { match: spec });
}

/* Fold a device-change response (the full port status dict) into shared state. */
function mergePortStatus(name, data) {
  const prev = state.ports.get(name) || {};
  const merged = Object.assign({}, prev);
  Object.keys(data).forEach((k) => { if (k !== "ok") merged[k] = data[k]; });
  state.ports.set(name, merged);
  const rt = state.runtimes.get(name);
  if (rt && data.open !== undefined) {
    rt.state = data.open ? "open" : (data.last_error ? "error" : "closed");
  }
}

async function applyDevice(rt, spec) {
  if (rt.devBusy) return;
  rt.devBusy = true;
  rt.el.devPop.classList.add("busy");
  const body = Object.assign({ reopen: !rt.el.devMapOnly.checked }, spec);
  const data = await api("POST", "/api/ports/" + encodeURIComponent(rt.name) + "/device", { body });
  rt.devBusy = false;
  rt.el.devPop.classList.remove("busy");
  if (!data.ok) return;   // server error already toasted; keep the popover open
  rt.deviceSource = (spec.device != null) ? "device" : "match";
  mergePortStatus(rt.name, data);
  updateDeviceChip(rt);
  updatePortHeader(rt);
  updateTabState(rt.name);
  updateGlobalStats();
  closeDevicePop();
  toast(t("device.changed", { device: data.device || "" }), "ok");
}

/* 6c. port open/close + pin control ======================================== */
/* Reuses the backend contract: POST /api/ports/{name}/open (empty JSON body =
   keep the configured baud) and POST /api/ports/{name}/close both return the
   full port status dict, which we fold back in with mergePortStatus(). Pins
   (GET/POST /api/ports/{name}/pins) are optional per port: the controls stay
   hidden unless the server reports pins.enabled. */
function portPath(rt) { return "/api/ports/" + encodeURIComponent(rt.name) + "/pins"; }
function portOpenPath(rt, verb) { return "/api/ports/" + encodeURIComponent(rt.name) + "/" + verb; }

function portIsOpen(rt) {
  if (rt.state === "open") return true;
  const meta = state.ports.get(rt.name);
  return !!(meta && meta.open);
}

function wirePortControl(rt) {
  if (rt.el.portToggle) rt.el.portToggle.addEventListener("click", () => togglePort(rt));
  if (rt.el.pinDtr) rt.el.pinDtr.addEventListener("click", () => pinAction(rt, { action: "set", pin: "dtr", value: !pinState(rt).dtr }));
  if (rt.el.pinRts) rt.el.pinRts.addEventListener("click", () => pinAction(rt, { action: "set", pin: "rts", value: !pinState(rt).rts }));
  if (rt.el.pinReset) rt.el.pinReset.addEventListener("click", () => pinAction(rt, { action: "reset", pin: null, value: null, duration_ms: null }));
}

/* Open <-> close. The button is disabled for the duration of the request so a
   double click cannot fire two conflicting transitions. */
async function togglePort(rt) {
  if (rt.portBusy) return;
  rt.portBusy = true;
  refreshPortToggle(rt);
  const data = await api("POST", portOpenPath(rt, portIsOpen(rt) ? "close" : "open"), { body: {} });
  rt.portBusy = false;
  if (!data.ok) { refreshPortToggle(rt); return; }   // server error already toasted
  mergePortStatus(rt.name, data);
  // a freshly (re)opened port may only now be able to answer /pins
  if (!pinsFromStatus(rt)) rt.pinsProbed = false;
  updatePortHeader(rt);
  updateTabState(rt.name);
  updateGlobalStats();
  if (pinState(rt).enabled) refreshPins(rt);   // live levels: unknown when closed, initial after open
  toast(t(data.open ? "port.opened" : "port.closed", { port: rt.name }), "ok");
}

/* Label, aria-pressed, title and disabled state all derive from the state. */
function refreshPortToggle(rt) {
  const btn = rt.el.portToggle;
  if (!btn) return;
  const open = portIsOpen(rt);
  const label = open ? t("port.close") : t("port.open");
  if (rt.el.portToggleLabel) rt.el.portToggleLabel.textContent = label;
  btn.classList.toggle("is-open", open);
  btn.setAttribute("aria-pressed", String(open));
  btn.disabled = !!rt.portBusy;
  btn.setAttribute("aria-busy", rt.portBusy ? "true" : "false");
  const title = t(open ? "port.closeTitle" : "port.openTitle", { port: rt.name });
  btn.title = title;
  btn.setAttribute("aria-label", title);
}

/* ---- pins ---- */
function pinsFromStatus(rt) {
  const meta = state.ports.get(rt.name);
  const p = meta && meta.pins;
  return (p && typeof p === "object") ? p : null;
}
/* Last known pin record: rt.pins (fresh GET/POST) wins over the status dict. */
function pinState(rt) {
  const meta = pinsFromStatus(rt) || {};
  const rec = rt.pins || {};
  const pick = (k) => {
    const v = rec[k] !== undefined && rec[k] !== null ? rec[k] : meta[k];
    return v === undefined || v === null ? null : !!v;
  };
  return {
    enabled: !!(rec.enabled || meta.enabled),
    dtr: pick("dtr"),
    rts: pick("rts"),
    pulse_ms: rec.pulse_ms != null ? rec.pulse_ms : (meta.pulse_ms != null ? meta.pulse_ms : null),
  };
}

/* Purely a renderer: reads state, never triggers requests. */
function updatePinsUI(rt) {
  const host = rt.el.pins;
  if (!host) return;
  const st = pinState(rt);
  host.hidden = !st.enabled;
  if (!st.enabled) return;
  const open = portIsOpen(rt);
  /* A closed port's line levels are not observable: GET /pins reports null for
     them, while the status dict's pins view keeps the CONFIGURED initial levels.
     Show "unknown" instead of a stale/configured value, and disable the actions. */
  const enabled = open && !rt.pinsBusy;
  [[rt.el.pinDtr, t("pins.dtr"), open ? st.dtr : null], [rt.el.pinRts, t("pins.rts"), open ? st.rts : null]].forEach(([btn, name, val]) => {
    if (!btn) return;
    const stateTxt = val === null ? "\u2013" : t(val ? "pins.on" : "pins.off");
    const nameEl = $(".pin-name", btn), valEl = $(".pin-val", btn);
    if (nameEl) nameEl.textContent = name;
    if (valEl) valEl.textContent = stateTxt;
    const title = t("pins.setTitle", { pin: name, state: stateTxt });
    btn.title = title;
    btn.setAttribute("aria-label", title);
    btn.setAttribute("aria-pressed", val === null ? "mixed" : String(!!val));
    btn.disabled = !enabled;
  });
  if (rt.el.pinReset) {
    const btn = rt.el.pinReset;
    btn.disabled = !enabled;
    const title = t("pins.resetTitle");
    btn.title = title;
    btn.setAttribute("aria-label", title);
  }
  host.classList.toggle("busy", !!rt.pinsBusy);
}

/* Silent GET /pins (raw fetch, so it never raises a user-visible toast). */
async function fetchPinsRaw(rt) {
  const headers = {};
  if (state.token) headers["Authorization"] = "Bearer " + state.token;
  try {
    const res = await fetch(apiUrl(portPath(rt)), { headers });
    let data = null;
    try { data = await res.json(); } catch (_e) { data = null; }
    return { status: res.status, ok: res.ok, data };
  } catch (_e) {
    return { status: 0, ok: false, data: null };   // transient network failure
  }
}

/* Optional-capability probe: only needed when the port status carries no `pins`
   view (older server). 400 / 404 are definitive, 409 means the port is closed. */
async function probePins(rt) {
  if (rt.pinsProbed || rt.pinsProbing) return;
  const now = Date.now();
  if (now - rt.pinsProbeAt < 10000) return;      // throttle repeated retries
  rt.pinsProbeAt = now;
  rt.pinsProbing = true;
  const r = await fetchPinsRaw(rt);
  rt.pinsProbing = false;
  if (r.status === 0 || r.status === 409) return; // transient / port not open yet
  rt.pinsProbed = true;                          // 200 / 400 / 404 are all definitive
  if (r.ok && r.data && r.data.ok && r.data.enabled) {
    rt.pins = r.data;
    updatePinsUI(rt);
  }
}

/* Authoritative LIVE levels (GET /pins returns null while the port is closed and
   the reloaded initial levels after an open) — the status dict's pins view only
   mirrors the configured initial levels, so refresh after an open/close. */
async function refreshPins(rt) {
  if (rt.pinsProbing || !pinState(rt).enabled || !portIsOpen(rt)) return;
  rt.pinsProbing = true;
  const r = await fetchPinsRaw(rt);
  rt.pinsProbing = false;
  if (r.ok && r.data && r.data.ok && r.data.enabled) {
    rt.pins = r.data;
    updatePinsUI(rt);
  }
}

function maybeProbePins(rt) {
  if (rt.pinsProbed || !portIsOpen(rt)) return;
  if (pinsFromStatus(rt)) { rt.pinsProbed = true; return; }
  probePins(rt);
}

async function pinAction(rt, body) {
  if (rt.pinsBusy) return;
  rt.pinsBusy = true;
  updatePinsUI(rt);
  const data = await api("POST", portPath(rt), { body });
  rt.pinsBusy = false;
  if (!data.ok) { updatePinsUI(rt); return; }    // server error already toasted
  const rec = Object.assign({ enabled: true }, rt.pins || {});
  ["enabled", "dtr", "rts", "pulse_ms", "initial"].forEach((k) => {
    if (data[k] !== undefined) rec[k] = data[k];
  });
  if (body.action === "set" && body.pin) rec[body.pin] = !!body.value;
  rt.pins = rec;
  if (data.open !== undefined) {                 // some builds echo the port status
    mergePortStatus(rt.name, data);
    updateTabState(rt.name);
    updateGlobalStats();
  }
  updatePinsUI(rt);
  if (body.action === "set") {
    toast(t("pins.set", { pin: t("pins." + body.pin), state: t(body.value ? "pins.on" : "pins.off") }), "ok");
  } else {
    toast(t("pins.pulsed"), "ok");
  }
}

let tabsTimer = null;
function scheduleTabsRefresh() {
  if (tabsTimer) return;
  tabsTimer = setTimeout(() => { tabsTimer = null; buildTabs(); fillSendPorts(); }, 0);
}

function buildRecord(line) {
  const text = line.text == null ? "" : String(line.text);
  return {
    seq: line.seq,
    ts: line.ts_ms || Date.now(),
    port: line.port,
    kind: line.kind || "rx",
    text,
    plain: stripAnsi(text),
    hasAnsi: !!line.ansi || ANSI_RE.test(text),
    row: -1,
  };
}

function buildPrefix(rec) {
  let p = "";
  if (state.timestamp) p += `\x1b[2m${fmtTime(rec.ts)}\x1b[0m `;
  p += `\x1b[38;2;232;131;74m` + String(rec.seq).padStart(4, " ") + `\x1b[0m `;
  if (rec.kind === "tx") return `\x1b[38;2;230;179;74m\u25b8\x1b[0m ` + p;
  if (rec.kind === "mark") return `\x1b[38;2;106;166;245m\u2691\x1b[0m ` + p;
  return p;
}

function renderRecord(rt, rec) {
  if (rt.filterRe && !rt.filterRe.test(rec.plain)) return;
  rec.row = rt.writeIndex++;
  if (rt.term) {
    rt.term.write(buildPrefix(rec) + rec.text + "\r\n");
    if (state.autoscroll && state.active === rt.name) rt.term.scrollToBottom();
  } else {
    renderFallback(rt, rec);
  }
}

function renderFallback(rt, rec) {
  const pre = rt.el.fallback;
  const row = document.createElement("div");
  row.className = "lf-" + (rec.kind === "tx" ? "tx" : rec.kind === "mark" ? "lf-seq" : "rx");
  let html = "";
  if (state.timestamp) html += `<span class="lf-ts">${fmtTime(rec.ts)}</span> `;
  html += `<span class="lf-seq">${rec.seq}</span> ` + escapeHtml(rec.plain);
  row.innerHTML = html;
  pre.appendChild(row);
  while (pre.childElementCount > state.maxRenderLines) pre.removeChild(pre.firstChild);
  if (state.autoscroll && state.active === rt.name) pre.scrollTop = pre.scrollHeight;
}

function trimArr(arr, n) { while (arr.length > n) arr.shift(); }

/* Pending tail ("partial" WS event / REST `pending`): the unterminated last line (a prompt such as
   "=> ") shown live at the bottom of the terminal. rt.pend.shown is what is currently on the cursor
   row (no trailing \r\n). Updates are erased with \r\x1b[2K and rewritten, at most once per animation
   frame. Wrapped tails are avoided by capping the displayed text to cols-1 (ellipsis + the tail's end),
   so the tail always occupies exactly one row. A "line" event erases it first. */
function pendFit(rt, text) {
  let s = String(text || "").replace(/\t/g, " ").replace(/[\x00-\x1f\x7f]/g, "");
  const cols = Math.max(4, (rt.term && rt.term.cols) || 80);
  if (s.length > cols - 1) s = "\u2026" + s.slice(-(cols - 2));
  return s;
}
function pendTarget(rt) {
  if (state.stopped || state.paused || rt.filterRe || rt.histBusy) return null;   // null: leave as is
  if (window.ZmodemUI && window.ZmodemUI.busy(rt.name)) return "";                // never draw during a transfer
  return pendFit(rt, rt.pend.want);
}
function pendFlush(rt) {
  rt.pend.raf = 0;
  if (!rt.term) return;
  const target = pendTarget(rt);
  if (target === null || target === rt.pend.shown) return;
  rt.term.write("\r\x1b[2K" + target);
  rt.pend.shown = target;
  if (state.autoscroll && state.active === rt.name) rt.term.scrollToBottom();
}
function pendSchedule(rt) {
  if (!rt.term || rt.pend.raf) return;
  rt.pend.raf = requestAnimationFrame(() => pendFlush(rt));
}
function pendSet(name, text) {
  if (!portStillKnown(name)) return;
  const rt = ensurePort(name);
  if (rt.histBusy) { rt.liveQ.push({ __partial: text || "" }); return; }   // keep order with queued lines
  const want = text || "";
  if (want === rt.pend.want && (rt.pend.raf || want === rt.pend.shown)) return;
  rt.pend.want = want;
  pendSchedule(rt);
}
/* Erase the shown tail now (call right before writing a line / when the terminal is wiped). */
function pendErase(rt) {
  rt.pend.want = "";
  if (rt.term && rt.pend.shown) rt.term.write("\r\x1b[2K");
  rt.pend.shown = "";
}
/* The terminal was cleared/reset: nothing is shown any more. */
function pendForget(rt) {
  rt.pend.want = ""; rt.pend.shown = "";
  if (rt.pend.raf) { cancelAnimationFrame(rt.pend.raf); rt.pend.raf = 0; }
}

function onLine(name, line) {
  if (!portStillKnown(name)) return;   // late line from a port a config switch removed
  const rt = ensurePort(name);
  if (rt.histBusy) { rt.liveQ.push(line); return; }   // history fetch in flight: replay after it
  const seq = line.seq;
  if (typeof seq === "number") {
    if (rt.lastSeq != null && seq <= rt.lastSeq) return;   // already shown (history overlap)
    rt.lastSeq = seq;
  }
  pendErase(rt);   // the completed line replaces the tail
  const rec = buildRecord(line);
  rt.lineCount += 1;
  rt.byteCount += byteLen(rec.text);
  rt.lines.push(rec);
  trimArr(rt.lines, state.maxRenderLines);
  if (state.paused) {
    rt.deferred.push(rec);
    trimArr(rt.deferred, state.maxRenderLines);
  } else {
    renderRecord(rt, rec);
  }
  if (state.active === name) scheduleCounters();
}
/* History on (re)connect: GET /api/ports/<n>/read returns the ring buffer.
   First load: last HISTORY_LINES lines; reconnect: since_seq=<last seen>.
   Live WS lines that arrive meanwhile are queued (rt.liveQ) and replayed after
   the history is written; rt.lastSeq drops overlaps, so no duplicates/gaps
   (the WS is subscribed before the hello, i.e. before this snapshot). */
const HISTORY_LINES = 500;
async function fetchRead(name, params) {
  const headers = state.token ? { Authorization: "Bearer " + state.token } : {};
  try {
    const res = await fetch(apiUrl(`/api/ports/${encodeURIComponent(name)}/read`, params), { headers });
    const d = await res.json();
    return d && d.ok ? d : null;
  } catch (_e) { return null; }
}

function renderBatch(rt, recs) {
  if (!rt.term) { recs.forEach((rec) => renderRecord(rt, rec)); return; }
  const parts = [];
  for (const rec of recs) {
    if (rt.filterRe && !rt.filterRe.test(rec.plain)) continue;
    rec.row = rt.writeIndex++;
    parts.push(buildPrefix(rec) + rec.text + "\r\n");
  }
  if (!parts.length) return;
  rt.term.write(parts.join(""));
  if (state.autoscroll && state.active === rt.name) rt.term.scrollToBottom();
}

async function loadHistory(rt) {
  if (state.stopped || rt.histBusy) return;
  if (window.ZmodemUI && window.ZmodemUI.busy(rt.name)) return;   // never inject during a transfer
  rt.histBusy = true; rt.liveQ = [];
  const gen = rt.clearGen;
  let lines = [];
  let pending = "";
  try {
    const since = rt.lastSeq;
    let r = await fetchRead(rt.name, since == null ? { limit: HISTORY_LINES } : { since_seq: since });
    if (r && since != null && r.last_seq != null && r.last_seq < since) {   // daemon restarted: seq reset
      rt.lastSeq = null;
      r = await fetchRead(rt.name, { limit: HISTORY_LINES });
    }
    if (r) { lines = r.lines || []; pending = typeof r.pending === "string" ? r.pending : ""; } else rt.lastSeq = null;   // fetch failed: stop de-duplicating
  } catch (_e) { rt.lastSeq = null; }
  try {
    if (lines.length > state.maxRenderLines) lines = lines.slice(-state.maxRenderLines);
    const base = rt.lastSeq;
    const fresh = lines.filter((l) => base == null || l.seq > base);
    fresh.forEach((l) => { if (typeof l.seq === "number" && (rt.lastSeq == null || l.seq > rt.lastSeq)) rt.lastSeq = l.seq; });
    if (gen === rt.clearGen && portStillKnown(rt.name)) pendErase(rt);
    if (fresh.length && gen === rt.clearGen && portStillKnown(rt.name)) {   // "clear" while fetching: drop
      const recs = fresh.map(buildRecord);
      rt.lines.push(...recs);
      trimArr(rt.lines, state.maxRenderLines);
      if (state.paused) { rt.deferred.push(...recs); trimArr(rt.deferred, state.maxRenderLines); }
      else renderBatch(rt, recs);
    }
  } finally {
    rt.histBusy = false;
    if (gen === rt.clearGen && portStillKnown(rt.name)) { rt.pend.want = pending; pendSchedule(rt); }   // shown once, after the history lines
    const q = rt.liveQ; rt.liveQ = [];
    q.forEach((l) => (l && l.__partial !== undefined ? pendSet(rt.name, l.__partial) : onLine(rt.name, l)));
  }
}

function onMark(name, msg) {
  onLine(name, { seq: msg.seq, port: name, ts_ms: msg.ts_ms || Date.now(),
    text: t("markLine", { label: msg.label || "" }), ansi: false, kind: "mark" });
}

/* 7. stats (rates + sparkline) ============================================ */
let counterTimer = null;
function scheduleCounters() {
  if (counterTimer) return;
  counterTimer = requestAnimationFrame(() => { counterTimer = null; paintCounters(); });
}
function paintCounters() {
  const rt = state.active ? state.runtimes.get(state.active) : null;
  if (!rt) return;
  rt.el.rateLines.textContent = fmtRate(rt.hist.length ? rt.hist[rt.hist.length - 1].l : 0);
  rt.el.rateBytes.textContent = fmtRate(rt.hist.length ? rt.hist[rt.hist.length - 1].b : 0);
}

function tickRates() {
  state.runtimes.forEach((rt) => {
    const dl = rt.lineCount - rt.last.lines;
    const db = rt.byteCount - rt.last.bytes;
    rt.last = { lines: rt.lineCount, bytes: rt.byteCount };
    rt.hist.push({ l: dl, b: db });
    trimArr(rt.hist, 60);
    updatePortHeader(rt);
    drawSpark(rt);
  });
  paintCounters();
}

function updatePortHeader(rt) {
  const meta = state.ports.get(rt.name);
  const bits = [];
  if (meta && meta.device) bits.push(meta.device);
  if (meta && meta.baud) bits.push(String(meta.baud) + " baud");
  if (rt.state) bits.push(t("state_" + rt.state));
  rt.el.meta.textContent = bits.join("  \u00b7  ");
  rt.el.meta.title = bits.join(" \u00b7 ");
  rt.el.stateLabel.textContent = t("state_" + rt.state);
  rt.el.stateDot.className = "state-dot " + (rt.state === "open" ? "open" : rt.state === "reconnecting" ? "reconnecting" : rt.state === "error" ? "error" : "");
  const buf = meta && meta.buffer ? meta.buffer : null;
  const capLines = (state.portConfig(rt.name) && state.portConfig(rt.name).buffer_lines) || 20000;
  const pct = buf && capLines ? clamp((buf.count / capLines) * 100, 0, 100) : 0;
  rt.el.bufPct.textContent = Math.round(pct) + "%";
  rt.el.bufPct.title = buf ? t("bufHint", { n: buf.count, bytes: fmtBytes(buf.bytes) }) : "";
  updateDeviceChip(rt);
  refreshPortToggle(rt);
  updatePinsUI(rt);
  maybeProbePins(rt);
}

function drawSpark(rt) {
  const c = rt.el.spark;
  if (!c || c.offsetParent === null) return;
  const ctx = c.getContext("2d");
  const W = c.width, H = c.height;
  ctx.clearRect(0, 0, W, H);
  const hist = rt.hist.slice(-40);
  if (!hist.length) return;
  let maxB = 1;
  for (const h of hist) maxB = Math.max(maxB, h.b);
  const css = getComputedStyle(document.documentElement);
  const col = (css.getPropertyValue("--rx").trim() || "#4fd1b0");
  const gap = 2, n = hist.length;
  const bw = Math.max(1, (W - gap * (n - 1)) / n);
  ctx.fillStyle = col;
  let x = W - (bw * n + gap * (n - 1));
  for (const h of hist) {
    const bh = Math.max(1, (h.b / maxB) * (H - 6));
    ctx.globalAlpha = 0.35 + 0.65 * (h.b / maxB);
    ctx.fillRect(x, H - bh, Math.max(1, bw - 0.5), bh);
    x += bw + gap;
  }
  ctx.globalAlpha = 1;
}

/* 8. websocket bus ======================================================== */
const CONN_DEFAULT_KEY = { idle: "connIdle", connecting: "connConnecting", open: "connLive", error: "connError", stopped: "connStopped" };
const conn = { state: "idle", key: "connIdle", vars: null };
function setConn(stateName, key, vars) {
  state.wsState = stateName;
  conn.state = stateName;
  conn.key = key || CONN_DEFAULT_KEY[stateName] || "connIdle";
  conn.vars = vars || null;
  const pill = $("#connPill");
  pill.setAttribute("data-state", stateName === "unauthorized" ? "error" : stateName);
  $("#connText").textContent = t(conn.key, conn.vars);
}

function wsUrl() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  let url = `${proto}://${location.host}/ws`;
  if (state.token) url += "?token=" + encodeURIComponent(state.token);
  return url;
}

function connectWS() {
  if (state.stopped) return;
  if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) return;
  if (state.wsTimer) { clearTimeout(state.wsTimer); state.wsTimer = null; }
  setConn("connecting");
  let ws;
  try { ws = new WebSocket(wsUrl()); }
  catch (e) { toast(t("toastWsError", { msg: e.message }), "error"); return scheduleReconnect(); }
  state.ws = ws;

  ws.addEventListener("open", () => {
    state.wsBackoff = 500;
    setConn("open");
    ws.send(JSON.stringify({ type: "subscribe", ports: [] })); // [] => all ports
  });
  ws.addEventListener("message", (ev) => {
    let msg;
    try { msg = JSON.parse(ev.data); } catch (_e) { return; }
    handleWsMessage(msg);
  });
  ws.addEventListener("close", () => {
    if (state.ws === ws && state.zmInited) window.ZmodemUI.onDisconnect();
    if (state.stopped) return;   // service exited on purpose: no reconnect loop
    setConn("idle", "connReconnecting");
    scheduleReconnect();
  });
  ws.addEventListener("error", () => { if (!state.stopped) setConn("error"); });
}

function scheduleReconnect() {
  if (state.wsTimer || state.stopped) return;
  const wait = state.wsBackoff;
  state.wsBackoff = Math.min(state.wsBackoffMax, Math.round(state.wsBackoff * 1.7));
  state.wsTimer = setTimeout(() => { state.wsTimer = null; connectWS(); }, wait);
}

function restartWS() {
  if (state.wsTimer) { clearTimeout(state.wsTimer); state.wsTimer = null; }
  state.wsBackoff = 500;
  if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) {
    try { state.ws.close(); } catch (_e) { /* ignore */ }
  } else {
    connectWS();
  }
}

/* Send one JSON message on the live socket (no-op while disconnected). */
function wsSend(obj) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) state.ws.send(JSON.stringify(obj));
}

/* ZMODEM scripts are loaded on demand (first zmodem message or rz/sz click).
   Messages that arrive while they load are queued and replayed in order. */
let zmQueue = null;
function ensureZmodem() {
  if (window.ZmodemUI && state.zmInited) return Promise.resolve();
  if (state.zmPromise) return state.zmPromise;
  state.zmPromise = (window.__loadZmodem ? window.__loadZmodem() : Promise.resolve()).then(() => {
    if (!window.ZmodemUI) return;
    window.ZmodemUI.init({
      wsSend, sendCommand, t, toast,
      paneFor: (name) => { const rt = state.runtimes.get(name); return rt ? rt.pane : null; },
      activePort: () => $("#sendPort").value || state.active,
      wsBuffered: () => (state.ws ? state.ws.bufferedAmount : 0),
    });
    state.zmInited = true;
    const q = zmQueue || []; zmQueue = null;
    q.forEach(([fn, m]) => window.ZmodemUI[fn](m));
  });
  return state.zmPromise;
}
function zmRoute(fn, msg) {
  if (state.zmInited) { window.ZmodemUI[fn](msg); return; }
  (zmQueue = zmQueue || []).push([fn, msg]);
  ensureZmodem();
}

/* Messages that arrive before init() finished (WS opens in parallel with the
   config fetch / xterm load) are held here and replayed in order. */
let preReady = [];
function handleWsMessage(msg) {
  if (preReady) { preReady.push(msg); return; }
  dispatchWsMessage(msg);
}
function dispatchWsMessage(msg) {
  if (!msg || typeof msg !== "object") return;
  if (msg.ok === false && msg.error === "unauthorized") {
    setConn("unauthorized", "connUnauthorized");
    toast(t("toastWsUnauthorized"), "error");
    return;
  }
  switch (msg.type) {
    case "hello": onHello(msg.ports || []); break;
    case "line": if (msg.line) onLine(msg.port, msg.line); break;
    case "partial": pendSet(msg.port, typeof msg.text === "string" ? msg.text : ""); break;
    case "status": onStatus(msg); break;
    case "mark": onMark(msg.port, msg); break;
    case "pong": break;
    case "shutdown": enterStopped(); break;   // the daemon is exiting (any tab's Exit)
    // ZMODEM: broadcasts (start/end) + this connection's session relay
    case "zmodem": zmRoute("onBroadcast", msg); break;
    case "zmodem_claimed": zmRoute("onClaimed", msg); break;
    case "zmodem_data": zmRoute("onData", msg); break;
    case "zmodem_closed": zmRoute("onClosed", msg); break;
    case "zmodem_error": zmRoute("onError", msg); break;
    default: break;
  }
}

function onHello(ports) {
  const seen = new Set();
  ports.forEach((p) => {
    if (!p || !p.name || !portInActiveConfig(p.name)) return;
    seen.add(p.name);
    state.ports.set(p.name, p);
    const rt = ensurePort(p.name);
    rt.state = p.open ? "open" : (p.last_error ? "error" : "closed");
    updatePortHeader(rt);
    pendErase(rt);   // reconnect: drop the old tail; loadHistory re-reads it from REST
    loadHistory(rt);
  });
  if (!state.active && ports.length) activate(ports[0].name);
  updateGlobalStats();
}

function onStatus(msg) {
  const name = msg.port;
  if (!name || !portStillKnown(name)) return;
  const rt = ensurePort(name);
  const prev = state.ports.get(name) || {};
  const merged = Object.assign({}, prev);
  ["open", "baud", "device", "last_error", "reconnect_count", "rx_lines", "tx_lines", "buffer", "pins"].forEach((k) => {
    if (msg[k] !== undefined) merged[k] = msg[k];
  });
  state.ports.set(name, merged);
  rt.state = msg.open === undefined ? rt.state : (msg.open ? "open" : (msg.last_error ? "error" : "closed"));
  updatePortHeader(rt);
  updateTabState(name);
  updateGlobalStats();
}

/* 9. filter + search ====================================================== */
function getFilter(name) {
  const rt = state.runtimes.get(name);
  return rt ? rt.filter : "";
}
function applyFilter(pattern) {
  const rt = state.active ? state.runtimes.get(state.active) : null;
  if (!rt) return;
  rt.filter = pattern;
  rt.filterRe = pattern ? compileRe(pattern) : null;
  const fld = $("#filterInput");
  fld.classList.toggle("invalid", !!pattern && !rt.filterRe);
}
function rebuildView(rt) {
  // Re-render the retained buffer under the current filter state.
  if (rt.term) { rt.term.clear(); rt.term.reset(); pendForget(rt); }
  else { rt.el.fallback.innerHTML = ""; }
  rt.writeIndex = 0;
  rt.lines.forEach((rec) => renderRecord(rt, rec));
}

function runSearch(pattern) {
  const rt = state.active ? state.runtimes.get(state.active) : null;
  if (!rt) return;
  rt.hits = []; rt.hitIdx = -1;
  const re = compileRe(pattern);
  if (re) {
    for (let i = 0; i < rt.lines.length; i++) {
      const rec = rt.lines[i];
      if (re.test(rec.plain)) rt.hits.push(rec);
    }
  }
  renderSearchResults(rt, "local", pattern);
  if (rt.hits.length) { rt.hitIdx = 0; jumpHit(); }
  updateSearchCount();
}
function updateSearchCount() {
  const rt = state.active ? state.runtimes.get(state.active) : null;
  const el = $("#searchCount");
  if (!rt || !rt.hits.length) { el.textContent = "0/0"; return; }
  el.textContent = (rt.hitIdx + 1) + "/" + rt.hits.length;
}
function jumpHit() {
  const rt = state.active ? state.runtimes.get(state.active) : null;
  if (!rt || rt.hitIdx < 0 || !rt.hits[rt.hitIdx]) return;
  const rec = rt.hits[rt.hitIdx];
  markActiveMatch(rt, rec.seq);
  if (rt.term) {
    try { rt.term.scrollToLine(Math.max(0, rec.row)); } catch (_e) { rt.term.scrollToBottom(); }
  } else {
    const node = rt.el.fallback.children[Math.max(0, rec.row)];
    if (node) node.scrollIntoView({ block: "center" });
  }
}
function nextHit(delta) {
  const rt = state.active ? state.runtimes.get(state.active) : null;
  if (!rt || !rt.hits.length) return;
  rt.hitIdx = (rt.hitIdx + delta + rt.hits.length) % rt.hits.length;
  jumpHit(); updateSearchCount();
}
function renderSearchResults(rt, source, pattern) {
  const box = $("#searchResults");
  if (!rt.hits.length) { box.hidden = true; box.innerHTML = ""; return; }
  const re = compileRe(pattern != null ? pattern : $("#searchInput").value);
  box.innerHTML = "";
  rt.hits.slice(0, 200).forEach((rec, i) => {
    const row = document.createElement("div");
    row.className = "match-row";
    row.dataset.seq = rec.seq;
    let text = escapeHtml(rec.plain);
    if (re) text = text.replace(re, (m) => `<mark>${m}</mark>`);
    row.innerHTML = `<span class="mr-seq">${rec.seq}</span>` +
      (source === "local" ? `<span class="mr-ts">${fmtTime(rec.ts)}</span>` : "") +
      `<span class="mr-text">${text}</span>`;
    row.addEventListener("click", () => {
      rt.hitIdx = i; jumpHit(); updateSearchCount(); markActiveMatch(rt, rec.seq);
    });
    box.appendChild(row);
  });
  box.hidden = false;
}
function markActiveMatch(rt, seq) {
  $$(".match-row", $("#searchResults")).forEach((r) => r.classList.toggle("active", Number(r.dataset.seq) === seq));
}

async function serverSearch() {
  const rt = state.active ? state.runtimes.get(state.active) : null;
  const pattern = $("#searchInput").value;
  if (!rt || !pattern) return;
  const data = await api("GET", `/api/ports/${encodeURIComponent(rt.name)}/search`, {
    params: { regex: pattern, limit: 200 },
  });
  if (!data.ok) return;
  const hits = (data.hits || []).map((h) => ({
    seq: h.seq, ts: h.ts_ms, plain: h.text,
  }));
  rt.hits = hits;
  rt.hitIdx = -1;
  const box = $("#searchResults");
  box.innerHTML = "";
  if (!hits.length) { box.hidden = true; toast(t("toastNoMatches"), "info"); updateSearchCount(); return; }
  const re = compileRe(pattern);
  hits.forEach((h, i) => {
    const row = document.createElement("div");
    row.className = "match-row";
    row.dataset.seq = h.seq;
    let text = escapeHtml(h.plain);
    if (re) text = text.replace(re, (m) => `<mark>${m}</mark>`);
    row.innerHTML = `<span class="mr-seq">${h.seq}</span><span class="mr-ts">${fmtTime(h.ts)}</span><span class="mr-text">${text}</span>`;
    row.addEventListener("click", () => {
      const local = rt.lines.find((l) => l.seq === h.seq);
      if (local) { rt.hitIdx = rt.lines.indexOf(local); jumpHit(); }
      else toast(t("toastOutsideBuffer", { seq: h.seq }), "warn");
    });
    box.appendChild(row);
  });
  box.hidden = false;
  updateSearchCount();
}

/* 10. tabs, activate, sendbar, export, toolbar, polling, init ============= */
function updateTabState(name) {
  const tab = $('.tab[data-port="' + CSS.escape(name) + '"]');
  if (!tab) return;
  const rt = state.runtimes.get(name);
  const dot = $(".t-dot", tab);
  const cls = rt && rt.state === "open" ? "open" : rt && rt.state === "reconnecting" ? "reconnecting" : "closed";
  dot.className = "t-dot " + cls;
}
function buildTabs() {
  const tabs = $("#tabs");
  tabs.innerHTML = "";
  state.runtimes.forEach((rt, name) => {
    const btn = document.createElement("button");
    btn.className = "tab";
    btn.dataset.port = name;
    btn.setAttribute("role", "tab");
    btn.setAttribute("aria-selected", String(state.active === name));
    const baud = (state.ports.get(name) || {}).baud;
    btn.innerHTML = `<span class="t-dot"></span><span>${escapeHtml(name)}</span>` +
      (baud ? `<span class="t-baud">${baud}</span>` : "");
    btn.addEventListener("click", () => activate(name));
    tabs.appendChild(btn);
    updateTabState(name);
  });
}
function activate(name) {
  if (!state.runtimes.has(name)) ensurePort(name);
  closeDevicePop();
  state.active = name;
  state.runtimes.forEach((rt, n) => {
    const on = n === name;
    rt.pane.hidden = !on;
    if (on) requestAnimationFrame(() => { refit(rt); if (rt.term) rt.term.scrollToBottom(); });
  });
  $$(".tab").forEach((t) => t.setAttribute("aria-selected", String(t.dataset.port === name)));
  $("#filterInput").value = getFilter(name);
  $("#sendPort").value = name;
  const rt = state.runtimes.get(name);
  if (rt) { renderSearchResults(rt, "local"); updateSearchCount(); }
  paintCounters();
}

function send() {
  const name = $("#sendPort").value;
  const input = $("#sendInput");
  const data = input.value;
  if (!name || data === "") return;
  const eol = $("#sendEol").value;
  api("POST", `/api/ports/${encodeURIComponent(name)}/send`, { body: { data, eol } })
    .then((r) => { if (r.ok) { pushHistory(data); input.value = ""; } });
}
/* Send a device command (rz / sz <path>) to `name` with the sendbar's line
   ending (CRLF when it is set to "none" — the device must see a line). */
function sendCommand(name, text) {
  const sel = $("#sendEol").value;
  const eol = sel === "none" ? "crlf" : sel;
  return api("POST", `/api/ports/${encodeURIComponent(name)}/send`, { body: { data: text, eol } });
}
function pushHistory(data) {
  state.sendHistory = state.sendHistory.filter((h) => h !== data);
  state.sendHistory.unshift(data);
  state.sendHistory = state.sendHistory.slice(0, 50);
  state.sendIndex = -1;
  safeSet("serialhub.sendHistory", JSON.stringify(state.sendHistory));
}
function historyNav(delta) {
  if (!state.sendHistory.length) return;
  if (state.sendIndex === -1 && delta < 0) state.sendIndex = 0;
  else state.sendIndex = clamp(state.sendIndex + (delta < 0 ? 1 : -1), 0, state.sendHistory.length - 1);
  $("#sendInput").value = state.sendHistory[state.sendIndex] || "";
}

async function exportPort(fmt) {
  const name = $("#sendPort").value || state.active;
  if (!name) return;
  const r = await api("POST", `/api/ports/${encodeURIComponent(name)}/export`, { body: { format: fmt } });
  if (r.ok) toast(t("toastExported", { n: r.count, fmt: r.format, file: r.path }), "ok");
}

function updateGlobalStats() {
  const ports = Array.from(state.ports.values());
  $("#statPorts").textContent = String(Math.max(ports.length, state.runtimes.size));
  $("#statOpen").textContent = String(ports.filter((p) => p.open).length);
}

async function pollStatus() {
  if (state.stopped) return;
  const health = await api("GET", "/health");
  if (state.stopped) return;
  if (health.ok) {
    $("#statUptime").textContent = fmtDuration(health.uptime_ms);
    updateExitButton(health.allow_shutdown);
  }
  const st = await api("GET", "/api/status");
  if (!st.ok) return;
  (st.ports || []).forEach((p) => {
    if (!p || !p.name || !portInActiveConfig(p.name)) return;
    state.ports.set(p.name, p);
    const rt = ensurePort(p.name);
    rt.state = p.open ? "open" : (p.last_error ? "error" : "closed");
    updatePortHeader(rt);
    updateTabState(p.name);
  });
  updateGlobalStats();
}

/* Fold a GET /api/config payload into shared state (path, ui knobs, i18n'd chip). */
function applyConfig(cfg) {
  state.config = cfg;
  const ui = cfg.ui || {};
  if (ui.max_render_lines) state.maxRenderLines = ui.max_render_lines;
  if (!safeGet("serialhub.timestamp", "")) state.timestamp = ui.timestamp !== false;
  updateConfigIndicator();
}

async function loadConfig(pending) {
  const r = await (pending || api("GET", "/api/config"));
  if (!r.ok || !r.config) return;
  applyConfig(r.config);
  // build port runtimes from config (ensures tabs before WS hello)
  (state.config.ports || []).forEach((p) => { if (p && p.name) ensurePort(p.name); });
  updateEmptyState();
}
state.portConfig = function (name) {
  const cfg = state.config;
  if (!cfg) return null;
  const list = cfg.ports || [];
  return list.find((p) => p.name === name) || null;
};

/* 10b. runtime config switch =============================================== */
/* GET /api/config carries the active file's absolute path; POST /api/reload with
   {"config": path} switches it (memory-only, no write) and with no body re-reads
   the active file. A switch can ADD or REMOVE ports, so after a successful call
   we re-read the config and reconcile the whole port surface: panes/runtimes for
   disappeared ports are destroyed (xterm disposed), new ones are created, the
   active tab falls back to the first remaining port, and tabs / the send-port
   dropdown / the top stats are rebuilt from the new set. */
let configBusy = false;

function configBaseName(path) {
  const s = String(path || "");
  const cut = Math.max(s.lastIndexOf("/"), s.lastIndexOf("\\"));
  return cut >= 0 ? s.slice(cut + 1) : s;
}

function updateConfigIndicator() {
  const el = $("#configCurrent");
  if (!el) return;
  const path = (state.config && state.config.path) || "";
  el.textContent = path ? configBaseName(path) : t("config.none");
  el.classList.toggle("is-none", !path);
  const label = path ? t("config.currentTitle", { path }) : t("config.none");
  el.title = label;
  el.setAttribute("aria-label", label);
}

function setConfigBusy(busy) {
  configBusy = busy;
  const ctl = $("#configCtl");
  if (ctl) {
    ctl.classList.toggle("busy", busy);
    ctl.setAttribute("aria-busy", busy ? "true" : "false");
  }
  ["#configInput", "#configReload", "#configSwitch"].forEach((sel) => {
    const el = $(sel);
    if (el) el.disabled = busy;
  });
  const input = $("#configInput");
  if (input) input.setAttribute("aria-busy", busy ? "true" : "false");
}

function updateEmptyState() {
  const el = $("#emptyState");
  if (el) el.hidden = state.runtimes.size > 0;
}

/* Re-derive the port surface from state.config. Only names present in the new
   config survive; the Map is re-ordered to config order so tabs follow the file. */
function reconcilePorts() {
  const names = [];
  ((state.config && state.config.ports) || []).forEach((p) => {
    if (p && p.name && names.indexOf(p.name) < 0) names.push(p.name);
  });
  const wanted = new Set(names);
  Array.from(state.runtimes.keys()).forEach((name) => {
    if (!wanted.has(name)) destroyRuntime(name);
  });
  Array.from(state.ports.keys()).forEach((name) => {
    if (!wanted.has(name)) state.ports.delete(name);
  });
  names.forEach((name) => ensurePort(name));
  const ordered = new Map();
  names.forEach((name) => {
    const rt = state.runtimes.get(name);
    if (rt) ordered.set(name, rt);
  });
  state.runtimes.forEach((rt, name) => { if (!ordered.has(name)) ordered.set(name, rt); });
  state.runtimes = ordered;
  // the active tab may have just disappeared: fall back to the first remaining
  if (!state.active || !state.runtimes.has(state.active)) {
    const first = state.runtimes.keys().next();
    if (!first.done) activate(first.value);
    else {
      state.active = null;
      $("#sendPort").value = "";
      $("#filterInput").value = "";
      $("#searchResults").hidden = true;
      updateSearchCount();
    }
  }
  updateEmptyState();
  buildTabs();
  fillSendPorts();
  updateGlobalStats();
}

async function reloadConfig(switchTo) {
  if (configBusy) return;
  const input = $("#configInput");
  const wanted = switchTo ? String(input.value || "").trim() : "";
  if (switchTo && !wanted) {
    input.focus();
    toast(t("config.needPath"), "warn");
    return;
  }
  setConfigBusy(true);
  const data = await api("POST", "/api/reload", switchTo ? { body: { config: wanted } } : {});
  if (!data.ok) {
    setConfigBusy(false);      // api() already toasted the server error
    return;
  }
  const r = await api("GET", "/api/config");
  if (r.ok && r.config) {
    applyConfig(r.config);
    reconcilePorts();
    await pollStatus();        // seed open/baud/buffer for ports added by the switch
  } else if (data.config) {
    state.config = Object.assign({}, state.config || {}, { path: data.config });
    updateConfigIndicator();
  }
  setConfigBusy(false);
  if (switchTo) input.value = "";
  const delta = t("config.delta", {
    added: (data.added || []).length,
    removed: (data.removed || []).length,
    restarted: (data.restarted || []).length,
  });
  // a switch echoes the new path; a no-body reload does not, so fall back to the
  // path we just re-read (or the one we sent) to keep the toast self-explanatory
  const activePath = data.config || (state.config && state.config.path) || wanted;
  toast(t(switchTo ? "config.switched" : "config.reloaded", { path: activePath }) + "  " + delta, "ok");
}

/* A port that is no longer in the active config must never resurrect a pane from
   a late WebSocket line/status (the daemon may still flush old-port events). */
function portInActiveConfig(name) {
  const cfg = state.config;
  if (!cfg) return true;               // config not loaded yet: trust the event
  const list = cfg.ports || [];
  return list.some((p) => p && p.name === name);
}
function portStillKnown(name) {
  return state.runtimes.has(name) || state.ports.has(name) || portInActiveConfig(name);
}

/* 10c. exit (stop the daemon) ============================================== */
/* Exit → confirm → POST /api/shutdown. The daemon replies, then ~200 ms later
   broadcasts {"type":"shutdown"} on /ws, closes every port and exits. Success
   (or that broadcast, in any open tab) switches the page into a terminal
   "stopped" state: persistent banner, stopped pill, and no WS reconnect /
   status polling — the page only works again after a reload once the service
   is restarted. */
function updateExitButton(allowed) {
  const btn = $("#exitBtn");
  if (!btn) return;
  btn.hidden = allowed !== true;
}

function enterStopped() {
  if (state.stopped) return;
  state.stopped = true;
  if (state.wsTimer) { clearTimeout(state.wsTimer); state.wsTimer = null; }
  (state.timers || []).forEach((id) => clearInterval(id));
  state.timers = [];
  const btn = $("#exitBtn");
  if (btn) btn.disabled = true;
  $("#stoppedBanner").hidden = false;
  setConn("stopped", "connStopped");
  // the daemon closes every port on exit; the socket goes away before those
  // status events would arrive, so show the ports as released right here
  Array.from(state.runtimes.keys()).forEach((name) => onStatus({ port: name, open: false }));
  if (state.ws) {
    try { state.ws.close(); } catch (_e) { /* ignore */ }
  }
}

async function requestExit() {
  const btn = $("#exitBtn");
  if (state.stopped || btn.disabled) return;
  if (!window.confirm(t("exitConfirm"))) return;
  btn.disabled = true;
  const r = await api("POST", "/api/shutdown");   // api() toasts any error
  if (r.ok) enterStopped();
  else if (!state.stopped) btn.disabled = false;
}

function bindUI() {
  $("#themeToggle").addEventListener("click", () => applyTheme(state.theme === "dark" ? "light" : "dark"));
  $("#exitBtn").addEventListener("click", requestExit);
  $("#stoppedReload").addEventListener("click", () => location.reload());
  // language buttons are created (with their own click handlers) by
  // buildLangSwitcher() from the registry — nothing to bind here.

  const tokenInput = $("#tokenInput");
  tokenInput.value = state.token;
  tokenInput.addEventListener("change", () => {
    state.token = tokenInput.value.trim();
    safeSet("serialhub.token", state.token);
    toast(t("toastTokenUpdated"), "info");
    restartWS();
  });

  // config switch: Enter in the input == clicking Switch; ↻ reloads in place.
  const configInput = $("#configInput");
  if (configInput) {
    configInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); reloadConfig(true); }
    });
  }
  if ($("#configSwitch")) $("#configSwitch").addEventListener("click", () => reloadConfig(true));
  if ($("#configReload")) $("#configReload").addEventListener("click", () => reloadConfig(false));

  $("#btnPause").addEventListener("click", (e) => {
    state.paused = !state.paused;
    e.currentTarget.classList.toggle("on", state.paused);
    refreshPauseBtn();
    if (!state.paused) {
      state.runtimes.forEach((rt) => { rt.deferred.forEach((rec) => renderRecord(rt, rec)); rt.deferred = []; });
      toast(t("toastResumed"), "info");
    } else {
      toast(t("toastPaused"), "info");
    }
  });

  $("#btnAuto").addEventListener("click", (e) => {
    state.autoscroll = !state.autoscroll;
    e.currentTarget.classList.toggle("on", state.autoscroll);
    safeSet("serialhub.autoscroll", String(state.autoscroll));
  });

  $("#btnTs").addEventListener("click", (e) => {
    state.timestamp = !state.timestamp;
    e.currentTarget.classList.toggle("on", state.timestamp);
    safeSet("serialhub.timestamp", String(state.timestamp));
    const rt = state.active ? state.runtimes.get(state.active) : null;
    if (rt) rebuildView(rt);
  });

  $("#btnClear").addEventListener("click", () => {
    const rt = state.active ? state.runtimes.get(state.active) : null;
    if (!rt) return;
    rt.lines = []; rt.deferred = []; rt.hits = []; rt.hitIdx = -1; rt.writeIndex = 0; rt.clearGen++;
    if (rt.term) { rt.term.clear(); rt.term.reset(); pendForget(rt); }
    else rt.el.fallback.innerHTML = "";
    $("#searchResults").hidden = true;
    updateSearchCount();
  });

  let filterTimer = null;
  $("#filterInput").addEventListener("input", (e) => {
    clearTimeout(filterTimer);
    const val = e.target.value;
    filterTimer = setTimeout(() => applyFilter(val), 200);
  });

  $("#searchInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); runSearch(e.target.value); }
    if (e.key === "Escape") { e.target.value = ""; $("#searchResults").hidden = true; }
  });
  $("#btnPrev").addEventListener("click", () => nextHit(-1));
  $("#btnNext").addEventListener("click", () => nextHit(1));
  $("#btnServerSearch").addEventListener("click", serverSearch);

  $$("[data-export]").forEach((b) => b.addEventListener("click", () => exportPort(b.dataset.export)));

  $("#sendBtn").addEventListener("click", send);
  const sendInput = $("#sendInput");
  sendInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); send(); }
    else if (e.key === "ArrowUp") { e.preventDefault(); historyNav(-1); }
    else if (e.key === "ArrowDown") { e.preventDefault(); historyNav(1); }
  });
  $("#sendPort").addEventListener("change", (e) => { if (state.runtimes.has(e.target.value)) activate(e.target.value); });

  document.addEventListener("click", (e) => {
    const box = $("#searchResults");
    if (box.hidden) return;
    if (!e.target.closest("#searchResults") && !e.target.closest("#searchInput") &&
        !e.target.closest("#btnPrev") && !e.target.closest("#btnNext") && !e.target.closest("#btnServerSearch")) {
      box.hidden = true;
    }
  });

  // device popover: Esc closes; a click outside the popover (or its trigger) closes.
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDevicePop(); });
  document.addEventListener("click", (e) => {
    if (!openDevPop) return;
    if (e.target.closest(".dev-pop") || e.target.closest(".dev-btn")) return;
    closeDevicePop();
  });
}

function fillSendPorts() {
  const sel = $("#sendPort");
  sel.innerHTML = "";
  state.runtimes.forEach((_rt, name) => {
    const o = document.createElement("option");
    o.value = name; o.textContent = name;
    sel.appendChild(o);
  });
  if (state.active) sel.value = state.active;
}

function detectFallback() {
  if (typeof window.Terminal === "undefined") {
    state.termFallback = true;
    $("#banner").hidden = false;
  }
}

async function init() {
  try { state.sendHistory = JSON.parse(safeGet("serialhub.sendHistory", "[]")) || []; }
  catch (_e) { state.sendHistory = []; }

  // Start the config fetch and the WebSocket right away, in parallel with the
  // xterm load; WS messages are held in preReady until the UI is set up.
  const cfgPromise = api("GET", "/api/config");
  connectWS();

  // Wait for xterm.js (local vendor -> CDN fallback) to settle before we
  // decide whether to render with the terminal or the plain-text fallback.
  if (window.__xtermReady) { try { await window.__xtermReady; } catch (_e) { /* ignore */ } }
  if (window.__fitReady) { try { await window.__fitReady; } catch (_e) { /* ignore */ } }

  initTheme();
  buildLangSwitcher();
  applyLang(state.lang);
  detectFallback();
  $("#btnAuto").classList.toggle("on", state.autoscroll);
  $("#btnTs").classList.toggle("on", state.timestamp);
  bindUI();
  // ZMODEM UI loads lazily; rz/sz buttons trigger the load, then re-click.
  ["btnRz", "btnSz"].forEach((id) => {
    const b = document.getElementById(id);
    if (!b) return;
    b.addEventListener("click", function once(e) {
      if (state.zmInited) return;
      b.removeEventListener("click", once);
      e.stopImmediatePropagation();
      ensureZmodem().then(() => b.click());
    });
  });

  await loadConfig(cfgPromise);
  updateEmptyState();
  fillSendPorts();

  // Show configured ports immediately, before the WebSocket hello arrives.
  if (!state.active && state.runtimes.size) activate(state.runtimes.keys().next().value);
  buildTabs();

  // WS messages queued while we were initialising
  const held = preReady; preReady = null;
  held.forEach(dispatchWsMessage);
  // interval ids are kept so the "service stopped" state can cancel them
  state.timers = [
    setInterval(tickRates, 1000),
    setInterval(pollStatus, 3000),
    // keep the active port header fresh as buffer/state change server-side
    setInterval(() => {
      if (state.active) {
        const rt = state.runtimes.get(state.active);
        if (rt) { updatePortHeader(rt); }
      }
    }, 2000),
  ];
  setTimeout(pollStatus, 400);
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();
