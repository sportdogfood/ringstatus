var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// health-records.mjs
var SHOWS_BASE = "appCaSmL819KK7UFJ";
var HEALTH_TABLES = { shows: "tblJA5CUon5s1sWib", days: "tbl14MaCCDw6dAywH", health: "tblcYgKFwKtILSdU8", tokens: "tbl8pJcHIaRchfDTP", warnings: "tblki6YL1Fe3nnfT0" };
function healthStore(env, request = fetch) {
  async function call(table, method = "GET", body, query = "") {
    if (!env.WEF_SHOWS_AIRTABLE_TOKEN) throw Error("Health logging credential unavailable");
    const r = await request(`https://api.airtable.com/v0/${SHOWS_BASE}/${table}${query}`, { method, headers: { Authorization: "Bearer " + env.WEF_SHOWS_AIRTABLE_TOKEN, "Content-Type": "application/json" }, ...body ? { body: JSON.stringify(body) } : {}, signal: AbortSignal.timeout(3e4) });
    if (!r.ok) throw Error("Health log HTTP " + r.status);
    return r.json();
  }
  __name(call, "call");
  async function list(table, filter) {
    let rows = [], offset;
    do {
      const q = new URLSearchParams({ pageSize: "100", ...filter ? { filterByFormula: filter } : {}, ...offset ? { offset } : {} });
      const p = await call(table, "GET", void 0, "?" + q);
      rows.push(...p.records);
      offset = p.offset;
    } while (offset);
    return rows;
  }
  __name(list, "list");
  async function save(table, key, fields) {
    const value = String(fields[key]).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
    const filter = `{${key}}='${value}'`, rows = await list(table, filter);
    if (rows.length > 1) throw Error("Duplicate health log identity");
    const result = await call(table, rows.length ? "PATCH" : "POST", { records: [{ ...rows.length ? { id: rows[0].id } : {}, fields }] });
    const check = await list(table, filter);
    if (check.length !== 1 || check[0].id !== result.records?.[0]?.id) throw Error("Health log readback failed");
    for (const [k, v] of Object.entries(fields)) {
      const actual = check[0].fields[k];
      if (v === null || Array.isArray(v) && v.length === 0) {
        if (actual != null && !(Array.isArray(actual) && actual.length === 0)) throw Error("Health log readback mismatch");
      } else if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}T/.test(v)) {
        if (Date.parse(actual) !== Date.parse(v)) throw Error("Health log readback mismatch");
      } else if (JSON.stringify(actual) !== JSON.stringify(v)) throw Error("Health log readback mismatch");
    }
    return check[0];
  }
  __name(save, "save");
  return { list, save };
}
__name(healthStore, "healthStore");
async function recordRenewal(env, fields, request = fetch) {
  try {
    const safe = {};
    for (const key of ["refresh-id", "started-at", "completed-at", "lane", "endpoint", "reason", "result", "http-status", "token-expires-at", "error"]) if (fields[key] !== void 0) safe[key] = fields[key];
    await healthStore(env, request).save(HEALTH_TABLES.tokens, "refresh-id", safe);
    return true;
  } catch {
    console.error(JSON.stringify({ message: "Token renewal log save failed", refresh_id: fields["refresh-id"] }));
    return false;
  }
}
__name(recordRenewal, "recordRenewal");

// schedule-health.mjs
var profiles = ["original-header", "profile-fb-header1", "profile-fb-header2", "windows-clean", "android-clean"];
var localDate = /* @__PURE__ */ __name((now) => new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(now)), "localDate");
async function scheduleHealth(request, env, { original, alternate, renew, store = healthStore(env), sleep = /* @__PURE__ */ __name((ms) => new Promise((r) => setTimeout(r, ms)), "sleep"), now = /* @__PURE__ */ __name(() => (/* @__PURE__ */ new Date()).toISOString(), "now") } = {}) {
  const at = now(), attempts = [], errors = [];
  let show, day, data, status = null, successful = false, warning, renewalFailure;
  let prior;
  try {
    const rows = await store.list(HEALTH_TABLES.health, "{health-key}='decoder'");
    if (rows.length > 1) throw Error("Duplicate decoder health record");
    prior = rows[0]?.fields;
    const defaults = await store.list(HEALTH_TABLES.shows, "AND({is_default}=TRUE(),{customer_id}=15)");
    if (defaults.length !== 1 || !Number.isSafeInteger(defaults[0].fields.show_id) || defaults[0].fields.show_id <= 0) throw Error("Exactly one valid default show required");
    show = defaults[0];
    const ping = new URL("/schedule", request.url);
    ping.search = new URLSearchParams({ date: "00/00/00", show_id: String(show.fields.show_id), customer_id: "15" });
    for (let round = 0; round < 2 && !successful; round++) {
      if (round) {
        await sleep(1e3);
        try {
          await renew("schedule-health-recovery", (failure) => {
            renewalFailure = failure;
          });
        } catch {
          errors.push("Token renewal failed");
          break;
        }
      }
      for (const profile of profiles) {
        if (attempts.length || round) await sleep(1e3);
        let response;
        const details = {}, report = /* @__PURE__ */ __name((fields) => Object.assign(details, fields), "report");
        try {
          response = round === 0 && profile === profiles[0] ? await original(new Request(ping), report) : await alternate(new Request(ping), profile, report);
          status = response.status;
          data = await response.json().catch(() => null);
          successful = response.ok && response.headers.get("x-rs-decoded") === "1" && String(data?.show?.show_id) === String(show.fields.show_id) && /^\d{4}-\d{2}-\d{2}$/.test(data?.show_date || "") && Array.isArray(data?.rings);
          attempts.push({ round: round + 1, profile, lane: response.headers.get("x-rs-source-lane") || "cloudflare-fallback", status, usable: successful, ...details });
        } catch {
          attempts.push({ round: round + 1, profile, status: null, usable: false, reason: "Request failed", ...details });
        }
        if (successful) break;
      }
    }
    const date = successful ? data.show_date : localDate(at);
    const days = await store.list(HEALTH_TABLES.days);
    const matching = days.filter((r) => r.fields.date === date && r.fields.show?.includes(show.id));
    if (matching.length > 1) throw Error("Duplicate matching show-day");
    day = matching[0];
  } catch (e) {
    errors.push(e.message);
  }
  const links = { show: show ? [show.id] : [], "show-day": day ? [day.id] : [] };
  const ok = successful && errors.length === 0;
  const message = ok ? `Decoder recovered: show ${show.fields.show_id}; ${data.rings.length} rings.` : `Decoder schedule health failed${show ? "; show " + show.fields.show_id : ""}.`;
  try {
    const transition = !ok && prior?.status !== "Outage" ? "Outage" : ok && prior?.status === "Outage" ? "Recovery" : null;
    if (transition) {
      const id = `decoder|${transition}|${transition === "Recovery" ? prior["outage-started-at"] || at : at}`;
      warning = await store.save(HEALTH_TABLES.warnings, "warning-id", { "warning-id": id, at, kind: transition, endpoint: request.url, ...links, message, recipient: "Owner \u2014 existing decoder monitoring chat", "delivery-status": "Pending" });
    }
    await store.save(HEALTH_TABLES.health, "health-key", { "health-key": "decoder", service: "decoder", endpoint: request.url, ...links, status: ok ? "Healthy" : "Outage", "checked-at": at, "http-status": status, "last-success-at": ok ? at : prior?.["last-success-at"] || null, "outage-started-at": ok ? null : prior?.status === "Outage" ? prior["outage-started-at"] || at : at, "recovered-at": ok && prior?.status === "Outage" ? at : prior?.["recovered-at"] || null, details: JSON.stringify({ attempts, errors, renewalFailure, rings: ok ? data.rings.length : null }) });
  } catch {
    errors.push("Health or warning log save failed");
  }
  return { ok: ok && errors.length === 0, worker: "wef-schedules-decoded", show_id: show ? String(show.fields.show_id) : null, rings: successful ? data.rings.length : 0, schedule_status: status, attempts, errors, ...warning ? { warning_id: warning.id } : {} };
}
__name(scheduleHealth, "scheduleHealth");

// public-live.mjs
var root = "/iphonev2/index.php/esp/liveclassv2/";
var sgl = "https://sgl.wellingtoninternational.com";
var routes = /* @__PURE__ */ new Map([
  ["/homepage/getLiveClassStatus", ["https://sglapi.wellingtoninternational.com", { customer_id: /^15$/, datetimestamp: /^\d+$/ }]],
  [root + "ListAjax", [sgl, { from_wp_api: /^true$/ }]],
  ["/iphone.php/esp/webservice/LiveScoreWidget", [sgl, {}]],
  [root + "getLiveClassData", [sgl, { show_id: /^[1-9]\d*$/, cid: /^[1-9]\d*$/, t: /^\d+$/ }]]
]);
function publicRoute(url) {
  const route = routes.get(url.pathname);
  if (!route) return null;
  const [origin, params] = route;
  const valid = [...url.searchParams.keys()].length === Object.keys(params).length && Object.entries(params).every(([key, pattern]) => pattern.test(url.searchParams.get(key) || ""));
  return { origin, valid };
}
__name(publicRoute, "publicRoute");
function fetchPublic(url, origin, sourceFetch = fetch) {
  const target = new URL(url.pathname + url.search, origin);
  return sourceFetch(target, { redirect: "manual", signal: AbortSignal.timeout(25e3), headers: {
    Accept: "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    Expires: "0",
    Origin: "https://www.wellingtoninternational.com",
    Referer: "https://www.wellingtoninternational.com/",
    "sgl-request-origin": "SGL-API",
    authority: target.host,
    method: "GET",
    path: target.pathname + target.search,
    scheme: "https",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    priority: "u=1, i",
    "sec-ch-ua": '"Not-A.Brand";v="99", "Microsoft Edge";v="157", "Chromium";v="157"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"iOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site"
  } });
}
__name(fetchPublic, "fetchPublic");

// worker.js
var SGL_BUNDLE_SALT = "5874a2ec29e1d30258abe7a00220fd5d";
async function decodeSglPayload(body, authorization, salt, keepBlobs = false) {
  const payload = JSON.parse(body);
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw Error("Invalid payload");
  if (payload._t) throw Error("Upstream token refresh required");
  const keys = /* @__PURE__ */ new Map();
  async function visit(value) {
    if (!value || typeof value !== "object") return;
    if (Array.isArray(value)) {
      for (const row of value) await visit(row);
      return;
    }
    if (Object.hasOwn(value, "_")) {
      if (!authorization || typeof value._ !== "string") throw Error("Missing authentication or blob");
      const bytes = Uint8Array.from(atob(value._), (c) => c.charCodeAt(0));
      if (bytes.length < 36) throw Error("Invalid blob length");
      const minute = new TextDecoder().decode(bytes.slice(0, 8));
      if (!/^\d{8}$/.test(minute)) throw Error("Invalid blob minute");
      if (!keys.has(minute)) {
        const material = new TextEncoder().encode(salt + "|" + authorization + "|" + minute);
        const digest = await crypto.subtle.digest("SHA-256", material);
        keys.set(minute, await crypto.subtle.importKey("raw", digest, { name: "AES-GCM" }, false, ["decrypt"]));
      }
      const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv: bytes.slice(8, 20) }, keys.get(minute), bytes.slice(20));
      const fields = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(plaintext));
      if (!fields || typeof fields !== "object" || Array.isArray(fields)) throw Error("Invalid decoded fields");
      for (const [key, field] of Object.entries(fields)) {
        if (["__proto__", "constructor", "prototype", "_", "_t"].includes(key)) throw Error("Invalid decoded key");
        value[key] = field;
      }
      if (!keepBlobs) delete value._;
    }
    for (const child of Object.values(value)) await visit(child);
  }
  __name(visit, "visit");
  await visit(payload);
  if (Object.keys(payload).length === 0) throw Error("Empty source payload");
  return JSON.stringify(payload);
}
__name(decodeSglPayload, "decodeSglPayload");
var reply = /* @__PURE__ */ __name((data, status = 200, extra = {}) => new Response(JSON.stringify(data), {
  status,
  headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", ...extra }
}), "reply");
function diagnostic(error, env, seen = /* @__PURE__ */ new Set()) {
  if (error && typeof error === "object") {
    if (seen.has(error)) return "[circular cause]";
    seen.add(error);
    const message = (error.name || "Error") + ": " + (error.message || String(error));
    return diagnostic(message, env) + (error.cause !== void 0 ? " Cause: " + diagnostic(error.cause, env, seen) : "");
  }
  let text = String(error);
  for (const secret of [env.SGL_TOKEN, env.SGL_COOKIE]) if (secret) text = text.split(secret).join("[redacted]");
  return text.replace(/Bearer\s+[^\s"'<>]+/gi, "Bearer [redacted]").slice(0, 2e3);
}
__name(diagnostic, "diagnostic");
var worker_default = {
  async fetch(request, env) {
    const url = new URL(request.url), pathname = url.pathname.replace(/\/+$/, "") || "/";
    if (request.method !== "GET") return reply({ message: "GET only" }, 405);
    if (pathname === "/health") return reply({ ok: true, worker: "wef-schedules-decoded" });
    const isRingDetail = /^\/ring\/[1-9]\d*$/.test(pathname);
    const isPeople = /^\/people\/\d+$/.test(pathname);
    const isClasses = /^\/classes(?:\/\d+)?$/.test(pathname);
    const isEntries = pathname === "/entries";
    if (!["/ring", "/schedule"].includes(pathname) && !isRingDetail && !isPeople && !isClasses && !isEntries)
      return reply({ message: "Route not found" }, 404);
    const allowed = isRingDetail ? ["show_date", "show_id", "customer_id"] : pathname === "/ring" ? ["customer_id"] : pathname === "/schedule" ? ["date", "show_id", "customer_id"] : isPeople ? ["pid", "show_id", "customer_id"] : isEntries ? ["show_id", "customer_id", "page", "sort_on", "sort_type", "search_text"] : ["show_id", "customer_id", "cgid"];
    if ([...url.searchParams.keys()].some((k) => !allowed.includes(k)) || url.searchParams.get("customer_id") !== "15" || pathname !== "/ring" && !/^\d+$/.test(url.searchParams.get("show_id") || "") || isRingDetail && !/^\d{4}-\d{2}-\d{2}$/.test(url.searchParams.get("show_date") || "") || isPeople && url.searchParams.get("pid") !== pathname.split("/")[2] || isEntries && (!/^[1-9]\d*$/.test(url.searchParams.get("page") || "") || url.searchParams.has("sort_on") && url.searchParams.get("sort_on") !== "number" || url.searchParams.has("sort_type") && url.searchParams.get("sort_type") !== "asc" || url.searchParams.has("search_text") && url.searchParams.get("search_text") !== "") || pathname === "/schedule" && !/^(?:\d{4}-\d{2}-\d{2}|00\/00\/00)$/.test(url.searchParams.get("date") || ""))
      return reply({ message: "Invalid source parameters" }, 400);
    if (!env.SGL_TOKEN) return reply({ message: "Source authentication not configured" }, 503);
    const upstream = new URL(pathname, "https://sglapi.wellingtoninternational.com");
    upstream.search = url.search;
    const authorization = "Bearer " + env.SGL_TOKEN;
    const headers = {
      Accept: "application/json, text/plain, */*",
      "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      Expires: "0",
      Authorization: authorization,
      "sgl-request-origin": "SGL-API",
      Origin: "https://www.wellingtoninternational.com",
      Referer: "https://www.wellingtoninternational.com/"
    };
    if (isRingDetail || pathname === "/schedule" || isClasses || isPeople) Object.assign(headers, {
      path: upstream.pathname + upstream.search,
      "accept-encoding": "gzip, deflate, br, zstd",
      "accept-language": "en-US,en;q=0.9",
      priority: "u=1, i",
      "sec-ch-ua": '"Not-A.Brand";v="99", "Microsoft Edge";v="157", "Chromium";v="157"',
      "sec-ch-ua-mobile": "?1",
      "sec-ch-ua-platform": '"iOS"',
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-site"
    });
    if (env.SGL_HEADER_PROFILE === "profile-fb-header1") {
      headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36 Edg/158.0.0.0";
      Object.assign(headers, { "sec-ch-ua": '"Chromium";v="158", "Not.A/Brand";v="24", "Microsoft Edge";v="158"', "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"', "sec-fetch-site": "cross-site", "sec-fetch-dest": "document", "sec-fetch-mode": "navigate", "sec-fetch-user": "?1" });
    } else if (env.SGL_HEADER_PROFILE === "profile-fb-header2") {
      for (const name of ["path", "priority", "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"]) delete headers[name];
      headers["sec-fetch-site"] = "cross-site";
    }
    if (env.SGL_HEADER_PROFILE === "windows-clean") {
      headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36 Edg/158.0.0.0";
      headers["sec-ch-ua"] = '"Chromium";v="158", "Not.A/Brand";v="24", "Microsoft Edge";v="158"';
      headers["sec-ch-ua-mobile"] = "?0";
      headers["sec-ch-ua-platform"] = '"Windows"';
      for (const name of ["authority", "method", "path", "scheme"]) delete headers[name];
    } else if (env.SGL_HEADER_PROFILE === "android-clean") {
      headers["User-Agent"] = "Mozilla/5.0 (Linux; Android 16; Pixel 10) AppleWebKit/537.36 (KHTML, like Gecko) Edg/157.0.0.0 Mobile Safari/537.36";
      delete headers.path;
    }
    if (env.SGL_COOKIE) headers.Cookie = env.SGL_COOKIE;
    let upstreamStatus = null, stage = "fetch";
    try {
      const response = await (env.SGL_FETCH || fetch)(upstream.toString(), { method: "GET", headers, redirect: "manual", signal: AbortSignal.timeout(25e3), cf: { cacheTtl: 0, cacheEverything: false } });
      upstreamStatus = response.status;
      if (response.status >= 300 && response.status < 400) {
        console.error(JSON.stringify({ message: "Source redirect blocked", upstreamStatus }));
        return reply({ message: "Source redirect blocked", statusCode: 502 }, 502);
      }
      if (!response.ok) {
        console.error(JSON.stringify({ message: "Source request failed", upstreamStatus }));
        return reply({ message: "Source request failed", statusCode: response.status }, response.status);
      }
      stage = "decode";
      const body = await decodeSglPayload(await response.text(), authorization, env.SGL_BUNDLE_SALT || SGL_BUNDLE_SALT, pathname === "/schedule");
      return new Response(body, { headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", "X-RS-Decoded": "1" } });
    } catch (error) {
      console.error(JSON.stringify({ message: "Source fetch or protected response decoding failed", stage, upstreamStatus, error: diagnostic(error, env) }));
      return reply({ message: "Source fetch or protected response decoding failed", statusCode: 502 }, 502);
    }
  }
};

// tcp-fetch.mjs
import { connect } from "cloudflare:sockets";
var encoder = new TextEncoder();
function unchunk(bytes) {
  const chunks = [];
  let offset = 0, total = 0;
  while (offset < bytes.length) {
    let end = offset;
    while (end + 1 < bytes.length && !(bytes[end] === 13 && bytes[end + 1] === 10)) end++;
    if (end + 1 >= bytes.length) throw Error("Incomplete chunk size");
    const sizeText = new TextDecoder().decode(bytes.slice(offset, end)).split(";")[0];
    if (!/^[0-9a-f]+$/i.test(sizeText)) throw Error("Invalid chunk size");
    const size = parseInt(sizeText, 16);
    offset = end + 2;
    if (size === 0) {
      const out = new Uint8Array(total);
      let i = 0;
      for (const c of chunks) {
        out.set(c, i);
        i += c.length;
      }
      return out;
    }
    if (offset + size + 2 > bytes.length || bytes[offset + size] !== 13 || bytes[offset + size + 1] !== 10) throw Error("Incomplete chunk body");
    chunks.push(bytes.slice(offset, offset + size));
    total += size;
    offset += size + 2;
  }
  throw Error("Missing terminal chunk");
}
__name(unchunk, "unchunk");
async function tcpFetch(input, options = {}) {
  const url = new URL(input);
  if (url.protocol !== "https:" || !["sglapi.wellingtoninternational.com", "sgl.wellingtoninternational.com"].includes(url.hostname)) throw Error("Unapproved source destination");
  const socket = connect({ hostname: url.hostname, port: 443 }, { secureTransport: "on" });
  socket.closed.catch(() => {
  });
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    socket.close().catch(() => {
    });
  }, 2e4);
  try {
    await socket.opened;
    const headers = new Headers(options.headers);
    headers.set("Host", url.hostname);
    headers.set("Connection", "close");
    headers.set("Accept-Encoding", "identity");
    headers.delete("cf-worker");
    let wire = "GET " + url.pathname + url.search + " HTTP/1.1\r\n";
    for (const [k, v] of headers) {
      if (/[\r\n]/.test(k + v)) throw Error("Invalid header");
      wire += k + ": " + v + "\r\n";
    }
    wire += "\r\n";
    const writer = socket.writable.getWriter();
    await writer.write(encoder.encode(wire));
    writer.releaseLock();
    const reader = socket.readable.getReader();
    const chunks = [];
    let size = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > 5 * 1024 * 1024) throw Error("Source response too large");
      chunks.push(value);
    }
    if (timedOut) throw Error("Source socket timed out");
    const raw = new Uint8Array(size);
    let cursor = 0;
    for (const c of chunks) {
      raw.set(c, cursor);
      cursor += c.length;
    }
    let boundary = -1;
    for (let i = 0; i + 3 < raw.length; i++) if (raw[i] === 13 && raw[i + 1] === 10 && raw[i + 2] === 13 && raw[i + 3] === 10) {
      boundary = i;
      break;
    }
    if (boundary < 0) throw Error("Missing HTTP headers");
    const lines = new TextDecoder().decode(raw.slice(0, boundary)).split("\r\n");
    const status = Number(lines.shift().match(/^HTTP\/1\.[01] (\d{3}) /)?.[1]);
    if (!status) throw Error("Invalid HTTP status");
    const responseHeaders = new Headers();
    for (const line of lines) {
      const i = line.indexOf(":");
      if (i <= 0) throw Error("Invalid HTTP header");
      responseHeaders.append(line.slice(0, i), line.slice(i + 1).trim());
    }
    let body = raw.slice(boundary + 4);
    if (responseHeaders.get("transfer-encoding")?.toLowerCase() === "chunked") {
      body = unchunk(body);
      responseHeaders.delete("transfer-encoding");
    } else if (responseHeaders.has("content-length") && Number(responseHeaders.get("content-length")) !== body.length) throw Error("Incomplete HTTP body");
    const compression = responseHeaders.get("content-encoding");
    if (compression && compression !== "identity") throw Error("Unexpected response compression");
    responseHeaders.delete("content-length");
    return new Response(body, { status, headers: responseHeaders });
  } finally {
    clearTimeout(timer);
    await socket.close().catch(() => {
    });
  }
}
__name(tcpFetch, "tcpFetch");

// fallback.mjs
var cached;
var renewal;
var TokenRenewalError = class extends Error {
  static {
    __name(this, "TokenRenewalError");
  }
  constructor(reason, status = null) {
    super(reason);
    this.status = status;
  }
};
async function readAnonymousToken(response) {
  let data;
  try {
    data = await response.json();
  } catch {
    throw new TokenRenewalError("Config response was not valid JSON", response.status);
  }
  const token = data?._t;
  if (token == null || token === "") throw new TokenRenewalError("Config response is missing _t", response.status);
  let claims;
  try {
    claims = JSON.parse(atob(token.split(".")[1].replaceAll("-", "+").replaceAll("_", "/")));
  } catch {
    throw new TokenRenewalError("Config token is malformed", response.status);
  }
  if (claims?.anon !== true) throw new TokenRenewalError("Config token is not anonymous", response.status);
  if (!Number.isFinite(claims.exp)) throw new TokenRenewalError("Config token expiry is missing or invalid", response.status);
  if (claims.exp * 1e3 <= Date.now()) throw new TokenRenewalError("Config token is expired", response.status);
  return { token, expires: claims.exp * 1e3 };
}
__name(readAnonymousToken, "readAnonymousToken");
async function sourceToken(env) {
  if (cached && cached.expires > Date.now() + 864e5 && cached.cookie === env.SGL_COOKIE) return cached.token;
  if (!env.SGL_COOKIE) throw new TokenRenewalError("Fallback cookie not configured");
  renewal ??= (async () => {
    const started = (/* @__PURE__ */ new Date()).toISOString(), id = crypto.randomUUID();
    let status = null;
    try {
      const response = await tcpFetch("https://sglapi.wellingtoninternational.com/utilities/get-config", { headers: {
        Accept: "application/json, text/plain, */*",
        Cookie: env.SGL_COOKIE,
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
        Origin: "https://www.wellingtoninternational.com",
        Referer: "https://www.wellingtoninternational.com/",
        "sgl-request-origin": "SGL-API",
        "Cache-Control": "no-cache",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Site": "same-site"
      } });
      status = response.status;
      if (!response.ok) throw new TokenRenewalError("Config request returned an HTTP error", status);
      const { token, expires } = await readAnonymousToken(response);
      cached = { token, expires, cookie: env.SGL_COOKIE };
      await recordRenewal(env, { "refresh-id": id, "started-at": started, "completed-at": (/* @__PURE__ */ new Date()).toISOString(), lane: "cloudflare-fallback", endpoint: "https://sglapi.wellingtoninternational.com/utilities/get-config", reason: env.REFRESH_REASON || "missing-or-near-expiry", result: "Succeeded", "http-status": status, "token-expires-at": new Date(cached.expires).toISOString() });
      await new Promise((resolve) => setTimeout(resolve, 1e3));
      return token;
    } catch (error) {
      const failure = error instanceof TokenRenewalError ? error : new TokenRenewalError("Config transport failed before a response was available", status);
      await recordRenewal(env, { "refresh-id": id, "started-at": started, "completed-at": (/* @__PURE__ */ new Date()).toISOString(), lane: "cloudflare-fallback", endpoint: "https://sglapi.wellingtoninternational.com/utilities/get-config", reason: env.REFRESH_REASON || "missing-or-near-expiry", result: "Failed", "http-status": failure.status, error: failure.message });
      throw failure;
    }
  })().finally(() => {
    renewal = void 0;
  });
  return renewal;
}
__name(sourceToken, "sourceToken");
async function renewFallbackToken(env, reason, report) {
  cached = void 0;
  try {
    return await sourceToken({ ...env, REFRESH_REASON: reason });
  } catch (error) {
    if (report && error instanceof TokenRenewalError) report({ reason: error.message, token_http_status: error.status });
    throw error;
  }
}
__name(renewFallbackToken, "renewFallbackToken");
async function fetchFallback(request, env, { profile, report } = {}) {
  try {
    const url = new URL(request.url), feed = publicRoute(url);
    if (feed) {
      if (!feed.valid) throw Error("Invalid public source parameters");
      const response2 = await fetchPublic(url, feed.origin, tcpFetch);
      if (response2.status >= 300 && response2.status < 400) throw Error("Public source redirect blocked");
      return new Response(response2.body, { status: response2.status, headers: { "Content-Type": response2.headers.get("content-type") || "application/json", "Cache-Control": "no-store" } });
    }
    const token = await sourceToken(env);
    let sourceStatus = null;
    const sourceFetch = report ? async (...args) => {
      report({ stage: "schedule-request" });
      const response2 = await tcpFetch(...args);
      sourceStatus = response2.status;
      return response2;
    } : tcpFetch;
    const response = await worker_default.fetch(request, { SGL_TOKEN: token, SGL_COOKIE: env.SGL_COOKIE, SGL_FETCH: sourceFetch, SGL_HEADER_PROFILE: profile });
    if (report && !response.ok) report({ schedule_upstream_status: sourceStatus, reason: sourceStatus === null ? "Schedule request failed before a response was available" : sourceStatus >= 300 && sourceStatus < 400 ? "Schedule redirect was blocked" : sourceStatus >= 400 ? "Schedule returned an HTTP error" : "Schedule response could not be decoded" });
    return response;
  } catch (error) {
    if (report) report(error instanceof TokenRenewalError ? { stage: "token-before-schedule", reason: error.message, token_http_status: error.status } : { stage: "fallback", reason: "Fallback failed" });
    console.error(JSON.stringify({ message: "Direct source fallback failed", path: new URL(request.url).pathname }));
    return Response.json({ message: "Source fallback unavailable", statusCode: 502 }, { status: 502, headers: { "Cache-Control": "no-store" } });
  }
}
__name(fetchFallback, "fetchFallback");

// shared-worker.mjs
import { DurableObject } from "cloudflare:workers";

// sgl-pacing.mjs
function createPacer({ notBefore = 0, now = Date.now, sleep = /* @__PURE__ */ __name((ms) => new Promise((resolve) => setTimeout(resolve, ms)), "sleep"), persist }) {
  if (!Number.isFinite(notBefore) || notBefore < 0) throw Error("Invalid pacing timestamp");
  let tail = Promise.resolve(), storageFailed = false;
  return (run) => {
    const result = tail.then(async () => {
      if (storageFailed) throw Error("Pacing storage unavailable");
      while (now() < notBefore) await sleep(notBefore - now());
      try {
        return await run({ startedAt: now() });
      } finally {
        notBefore = now() + 1e3;
        try {
          await persist(notBefore);
        } catch (error) {
          storageFailed = true;
          throw error;
        }
      }
    });
    tail = result.then(() => {
    }, () => {
    });
    return result;
  };
}
__name(createPacer, "createPacer");

// shared-worker.mjs
var reply2 = /* @__PURE__ */ __name((data, status = 200, extra = {}) => new Response(JSON.stringify(data), {
  status,
  headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", ...extra }
}), "reply");
var sourceHandler = {
  async fetch(request, env) {
    const url = new URL(request.url), pathname = url.pathname.replace(/\/+$/, "") || "/";
    if (request.method !== "GET") return reply2({ message: "GET only" }, 405);
    if (pathname === "/health") {
      const result = await scheduleHealth(request, env, {
        original: /* @__PURE__ */ __name((ping, report) => this.fetch(ping, { ...env, SGL_HEALTH_PROBE: true, SGL_HEALTH_REPORT: report }), "original"),
        alternate: /* @__PURE__ */ __name((ping, profile, report) => fetchFallback(ping, env, { profile, report }), "alternate"),
        renew: /* @__PURE__ */ __name((reason, report) => renewFallbackToken(env, reason, report), "renew")
      });
      return reply2(result, result.ok ? 200 : 503);
    }
    const publicFeed = publicRoute(url);
    if (publicFeed && !publicFeed.valid) return reply2({ message: "Invalid source parameters" }, 400);
    if (!publicFeed) {
      const isRingDetail = /^\/ring\/[1-9]\d*$/.test(pathname);
      const isPeople = /^\/people\/\d+$/.test(pathname);
      const isClasses = /^\/classes(?:\/\d+)?$/.test(pathname);
      const isEntries = pathname === "/entries";
      if (!["/ring", "/schedule"].includes(pathname) && !isRingDetail && !isPeople && !isClasses && !isEntries)
        return reply2({ message: "Route not found" }, 404);
      const allowed = isRingDetail ? ["show_date", "show_id", "customer_id"] : pathname === "/ring" ? ["customer_id"] : pathname === "/schedule" ? ["date", "show_id", "customer_id"] : isPeople ? ["pid", "show_id", "customer_id"] : isEntries ? ["show_id", "customer_id", "page", "sort_on", "sort_type", "search_text"] : ["show_id", "customer_id", "cgid"];
      if ([...url.searchParams.keys()].some((k) => !allowed.includes(k)) || url.searchParams.get("customer_id") !== "15" || pathname !== "/ring" && !/^\d+$/.test(url.searchParams.get("show_id") || "") || isRingDetail && !/^\d{4}-\d{2}-\d{2}$/.test(url.searchParams.get("show_date") || "") || isPeople && url.searchParams.get("pid") !== pathname.split("/")[2] || isEntries && (!/^[1-9]\d*$/.test(url.searchParams.get("page") || "") || url.searchParams.has("sort_on") && url.searchParams.get("sort_on") !== "number" || url.searchParams.has("sort_type") && url.searchParams.get("sort_type") !== "asc" || url.searchParams.has("search_text") && url.searchParams.get("search_text") !== "") || pathname === "/schedule" && !/^(?:\d{4}-\d{2}-\d{2}|00\/00\/00)$/.test(url.searchParams.get("date") || ""))
        return reply2({ message: "Invalid source parameters" }, 400);
    }
    const fallbackEligible = true;
    const fallback = /* @__PURE__ */ __name(async (reason) => {
      console.error(JSON.stringify({ message: "Using direct source fallback", path: pathname, reason }));
      env.SGL_HEALTH_REPORT?.({ runner_fallback_reason: reason });
      const recover = !env.SGL_HEALTH_PROBE && (pathname === "/schedule" || /^\/classes\/[1-9]\d*$/.test(pathname));
      const profiles2 = recover ? ["original-header", "profile-fb-header1", "profile-fb-header2", "windows-clean", "android-clean"] : ["original-header"];
      let response, profile;
      for (let round = 0; round < (recover ? 2 : 1); round++) {
        if (round) {
          await new Promise((resolve) => setTimeout(resolve, 1e3));
          try {
            await renewFallbackToken(env, "source-header-recovery");
          } catch {
            break;
          }
        }
        for (let index = 0; index < profiles2.length; index++) {
          profile = profiles2[index];
          if (index || round) await new Promise((resolve) => setTimeout(resolve, 1e3));
          response = await fetchFallback(request, env, { profile: index || round ? profile : void 0, report: env.SGL_HEALTH_REPORT });
          const payload = response.ok ? await response.clone().json().catch(() => null) : null;
          const usable = response.ok && (publicFeed || response.headers.get("x-rs-decoded") === "1" && payload && typeof payload === "object" && !Array.isArray(payload) && Object.keys(payload).length > 0);
          if (usable || !recover) {
            const headers2 = new Headers(response.headers);
            headers2.set("X-RS-Source-Lane", "cloudflare-fallback");
            if (recover) headers2.set("X-RS-Header-Profile", profile);
            return new Response(response.body, { status: response.status, headers: headers2 });
          }
          if (index < profiles2.length - 1) await response.body?.cancel();
        }
      }
      if (response.ok) return reply2({ message: "Source fallback returned unusable data", statusCode: 502 }, 502, { "X-RS-Source-Lane": "cloudflare-fallback", "X-RS-Header-Profile": profile });
      const headers = new Headers(response.headers);
      headers.set("X-RS-Source-Lane", "cloudflare-fallback");
      headers.set("X-RS-Header-Profile", profile);
      return new Response(response.body, { status: response.status, headers });
    }, "fallback");
    if (fallbackEligible && env.RUNNER_KEY && request.headers.get("X-RS-Fallback-Test") === env.RUNNER_KEY) return fallback("authorized-verification");
    try {
      const target = new URL(env.RUNNER_ORIGIN);
      if (target.protocol !== "https:") throw Error("Invalid runner origin");
      target.pathname = pathname;
      target.search = url.search;
      const response = await fetch(target, {
        headers: { Authorization: "Bearer " + env.RUNNER_KEY },
        redirect: "manual",
        cache: "no-store",
        signal: AbortSignal.timeout(3e4)
      });
      env.SGL_HEALTH_REPORT?.({ runner_http_status: response.status });
      if (fallbackEligible) {
        if (!response.ok) {
          await response.body?.cancel();
          return fallback("runner-http-" + response.status);
        }
        if (!publicFeed) {
          if (response.headers.get("x-rs-decoded") !== "1") {
            await response.body?.cancel();
            return fallback("runner-undecoded");
          }
          const payload = await response.clone().json().catch(() => null);
          if (!payload || typeof payload !== "object" || Array.isArray(payload) || Object.keys(payload).length === 0) {
            await response.body?.cancel();
            return fallback("runner-empty-or-invalid");
          }
        }
      }
      if (response.status >= 300 && response.status < 400) return reply2({ message: "Runner redirect blocked" }, 502);
      if (!publicFeed && response.ok && response.headers.get("x-rs-decoded") !== "1") return reply2({ message: "Undecoded response" }, 502);
      return new Response(response.body, { status: response.status, headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
        ...fallbackEligible ? { "X-RS-Source-Lane": "runner" } : {},
        ...!publicFeed && response.ok ? { "X-RS-Decoded": "1" } : {}
      } });
    } catch {
      return fallbackEligible ? fallback("runner-unavailable") : reply2({ message: "Runner unavailable" }, 502);
    }
  }
};
var SglPacer = class extends DurableObject {
  static {
    __name(this, "SglPacer");
  }
  constructor(ctx, env) {
    super(ctx, env);
    this.ready = ctx.blockConcurrencyWhile(async () => {
      const notBefore = await ctx.storage.get("nextAllowed") ?? 0;
      this.enqueue = createPacer({ notBefore, persist: /* @__PURE__ */ __name((value) => ctx.storage.put("nextAllowed", value), "persist") });
    });
  }
  async fetch(request) {
    await this.ready;
    return this.enqueue(async ({ startedAt }) => {
      const response = await sourceHandler.fetch(request, this.env);
      const body = response.body === null ? null : await response.arrayBuffer();
      const headers = new Headers(response.headers);
      headers.set("X-RS-Queue-Start", String(startedAt));
      headers.set("X-RS-Queue-End", String(Date.now()));
      return new Response(body, { status: response.status, headers });
    });
  }
};
var shared_worker_default = {
  async fetch(request, env) {
    if (request.method !== "GET") return sourceHandler.fetch(request, env);
    try {
      if (!env.SGL_PACER) throw Error("Shared pacing not configured");
      return await env.SGL_PACER.get(env.SGL_PACER.idFromName("sgl-source")).fetch(request);
    } catch {
      console.error(JSON.stringify({ message: "Shared source queue unavailable" }));
      return reply2({ message: "Source queue unavailable", statusCode: 503 }, 503);
    }
  }
};
export {
  SglPacer,
  shared_worker_default as default,
  sourceHandler
};
//# sourceMappingURL=shared-worker.js.map

