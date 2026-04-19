// ringstatus-proxy Worker (FULL DROP)
// Supports:
//   GET  /health
//   GET  /rider?q=Jessica -> Airtable-backed rider lookup
//   GET  /horse?q=Knox    -> Airtable-backed horse lookup
//   GET  /docs/*   -> proxies UPSTREAM_BASE/docs/*
//   GET  /items/*  -> proxies UPSTREAM_BASE/items/*
//   POST /docs/commit-bulk  -> commits docs/* files to GitHub repo/branch via Git API
//   POST /items/commit-bulk -> commits items/* files to GitHub repo/branch via Git API
//
// Required env vars (Worker Settings -> Variables):
//   UPSTREAM_BASE   (e.g. https://raw.githubusercontent.com/sportdogfood/ringstatus-data/main)
//   GITHUB_REPO     (e.g. sportdogfood/ringstatus-data)
//   GITHUB_BRANCH   (e.g. main)
//   GITHUB_TOKEN    (GitHub PAT with contents:read/write to that repo)  [SECRET]
// Optional:
//   CACHE_TTL       (seconds; default 300)
//   AIRTABLE_RIDER_CACHE_TTL (seconds; default 0; 0 disables)
//
// Optional Airtable rider lookup env vars:
//   AIRTABLE_TOKEN       (Airtable PAT) [SECRET]
//   AIRTABLE_BASE_ID
//   AIRTABLE_RIDER_TABLE
//   AIRTABLE_RIDER_VIEW

const DEFAULT_TTL = 300;
const DEFAULT_AIRTABLE_RIDER_CACHE_TTL = 0;

// ---------- small utils ----------
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
};

function withCors(res) {
  const h = new Headers(res.headers);
  for (const [k, v] of Object.entries(corsHeaders)) h.set(k, v);
  return new Response(res.body, { status: res.status, headers: h });
}

function json(data, status = 200, extraHeaders = {}) {
  const h = new Headers({ "Content-Type": "application/json; charset=utf-8", ...extraHeaders, ...corsHeaders });
  return new Response(JSON.stringify(data, null, 2) + "\n", { status, headers: h });
}

function text(body, status = 200, extraHeaders = {}) {
  const h = new Headers({ "Content-Type": "text/plain; charset=utf-8", ...extraHeaders, ...corsHeaders });
  return new Response(String(body), { status, headers: h });
}

function safePath(rel) {
  if (typeof rel !== "string" || !rel.length) return false;
  if (rel.includes("..")) return false;
  if (!/^[a-z0-9][\w\-./]+$/i.test(rel)) return false;
  // conservative extension allowlist
  if (!/\.(json|txt|html|xml|csv|ndjson|md|tmpl|css|js)$/i.test(rel)) return false;
  return true;
}

function normalizeNoLeadingSlash(p) {
  return String(p || "").replace(/^\/+/, "");
}

async function readJsonBody(req) {
  const raw = await req.text();
  if (!raw) return null;
  try {
    const first = JSON.parse(raw);
    // tolerate double-stringified bodies
    if (typeof first === "string") {
      try { return JSON.parse(first); } catch { return first; }
    }
    // tolerate stringified json field
    if (first && typeof first.json === "string") {
      try { first.json = JSON.parse(first.json); } catch {}
    }
    return first;
  } catch {
    return null;
  }
}

function contentTypeForPath(path) {
  const p = String(path || "").toLowerCase();

  if (p.endsWith(".html")) return "text/html; charset=utf-8";
  if (p.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (p.endsWith(".css")) return "text/css; charset=utf-8";
  if (p.endsWith(".json")) return "application/json; charset=utf-8";
  if (p.endsWith(".xml")) return "application/xml; charset=utf-8";
  if (p.endsWith(".csv")) return "text/csv; charset=utf-8";
  if (p.endsWith(".txt")) return "text/plain; charset=utf-8";
  if (p.endsWith(".md")) return "text/markdown; charset=utf-8";
  if (p.endsWith(".tmpl")) return "text/plain; charset=utf-8";
  if (p.endsWith(".ndjson")) return "application/x-ndjson; charset=utf-8";

  return "application/octet-stream";
}

// ---------- Airtable rider lookup ----------
function cleanName(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalize(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function firstText(...values) {
  for (const value of values) {
    if (value == null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function firstNumber(...values) {
  for (const value of values) {
    if (value == null || value === "") continue;
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function displayPlacing(value) {
  const text = firstText(value);
  if (!text) return "";

  const n = Number(text);
  if (Number.isFinite(n) && (n <= 0 || n >= 99999)) return "";

  return text;
}

function parseClockToSeconds(value) {
  const s = String(value || "").trim().toUpperCase();
  if (!s) return null;

  let m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const hh = Number(m[1]);
    const mm = Number(m[2]);
    const ss = Number(m[3] || 0);
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59 && ss >= 0 && ss <= 59) {
      return hh * 3600 + mm * 60 + ss;
    }
  }

  m = s.match(/^(\d{1,2}):(\d{2})\s*([AP])M?$/);
  if (m) {
    let hh = Number(m[1]);
    const mm = Number(m[2]);
    const ap = m[3];
    if (!(hh >= 1 && hh <= 12 && mm >= 0 && mm <= 59)) return null;
    if (hh === 12) hh = 0;
    if (ap === "P") hh += 12;
    return hh * 3600 + mm * 60;
  }

  return null;
}

function formatDisplayTime(value) {
  const sec = parseClockToSeconds(value);
  if (sec == null) return cleanName(value);

  let hh = Math.floor(sec / 3600);
  const mm = Math.floor((sec % 3600) / 60);
  const ap = hh >= 12 ? "PM" : "AM";
  hh %= 12;
  if (hh === 0) hh = 12;

  return `${hh}:${String(mm).padStart(2, "0")} ${ap}`;
}

function statusRank(status) {
  const v = normalize(status);
  if (v === "underway" || v === "in progress") return 0;
  if (v === "not started" || v === "upcoming") return 1;
  if (v === "completed") return 2;
  return 9;
}

function statusCategory(status) {
  const v = normalize(status);
  if (v === "underway" || v === "in progress") return "Underway";
  if (v === "completed") return "Completed";
  if (v === "not started" || v === "upcoming") return "Not Started";
  return cleanName(status) || "Unknown";
}

function isGoneIn(value) {
  const v = normalize(value);
  if (!v || v === "0" || v === "false" || v === "no" || v === "n") return false;
  return v === "1" || v === "true" || v === "yes" || v === "y";
}

function tripCategory({ statusCategory: category, minutesToGo, goneIn, hasResult }) {
  if (category === "Completed") return "Completed";
  if (category === "Underway") return "Underway";
  if (category === "Not Started") return "Not Started";
  if (hasResult && isGoneIn(goneIn)) return "Completed";
  if (typeof minutesToGo === "number" && Number.isFinite(minutesToGo) && minutesToGo <= 0) return "Underway";
  return category || "Unknown";
}

function nextTripRank(record) {
  if (record.statusCategory === "Underway") return 0;
  if (record.tripCategory === "Underway") return 0;
  if (record.statusCategory === "Not Started") return 1;
  if (record.tripCategory === "Not Started") return 1;
  if (record.statusCategory === "Completed") return 2;
  if (record.tripCategory === "Completed") return 2;
  return 9;
}

function resultRank(record) {
  if (record.hasResult) return 0;
  if (record.tripCategory === "Completed" || normalize(record.status) === "completed") return 1;
  return 9;
}

function parseLookupQuery(url) {
  const rawQ = cleanName(url.searchParams.get("q") || url.searchParams.get("name") || "");
  let mode = normalize(url.searchParams.get("mode") || "") || "next";
  let classQuery = cleanName(url.searchParams.get("class") || url.searchParams.get("class_number") || "");
  const limit = Math.min(10, Math.max(1, Number(url.searchParams.get("limit") || 5) || 5));
  const raw = ["1", "true", "yes", "y"].includes(normalize(url.searchParams.get("raw") || ""));

  const tokens = rawQ.split(/\s+/).filter(Boolean);
  if (tokens.length > 1) {
    const tail = normalize(tokens[tokens.length - 1]);
    if (/^\d{1,4}$/.test(tail) && !classQuery) {
      classQuery = tail;
      tokens.pop();
      mode = "class";
    } else if (["all", "al"].includes(tail)) {
      mode = "all";
      tokens.pop();
    } else if (["next", "ne"].includes(tail)) {
      mode = "next";
      tokens.pop();
    } else if (["result", "results", "res", "re"].includes(tail)) {
      mode = "result";
      tokens.pop();
    }
  }

  if (!["next", "all", "class", "result"].includes(mode)) mode = "next";
  if (classQuery) mode = mode === "all" ? "all" : "class";

  return {
    raw: rawQ,
    name: cleanName(tokens.join(" ") || rawQ),
    nameNorm: normalize(tokens.join(" ") || rawQ),
    mode,
    classQuery,
    limit,
    raw,
  };
}

function airtableConfig(env) {
  return {
    token: String(env.AIRTABLE_TOKEN || "").trim(),
    baseId: String(env.AIRTABLE_BASE_ID || "").trim(),
    table: String(env.AIRTABLE_RIDER_TABLE || "").trim(),
    view: String(env.AIRTABLE_RIDER_VIEW || "").trim(),
  };
}

function requireAirtableConfig(env) {
  const cfg = airtableConfig(env);
  const missing = [];
  if (!cfg.token) missing.push("AIRTABLE_TOKEN");
  if (!cfg.baseId) missing.push("AIRTABLE_BASE_ID");
  if (!cfg.table) missing.push("AIRTABLE_RIDER_TABLE");
  if (missing.length) throw new Error(`Missing env ${missing.join(", ")}`);
  return cfg;
}

async function airtableListAll(env) {
  const cfg = requireAirtableConfig(env);
  const out = [];
  let offset = "";

  do {
    const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(cfg.baseId)}/${encodeURIComponent(cfg.table)}`);
    url.searchParams.set("pageSize", "100");
    if (cfg.view) url.searchParams.set("view", cfg.view);
    if (offset) url.searchParams.set("offset", offset);

    const res = await fetch(url.toString(), {
      headers: {
        Authorization: `Bearer ${cfg.token}`,
        Accept: "application/json",
      },
    });

    const textBody = await res.text();
    let payload = null;
    try { payload = JSON.parse(textBody); } catch {}

    if (!res.ok) {
      const type = payload?.error?.type || res.status;
      const message = payload?.error?.message || textBody.slice(0, 200);
      throw new Error(`Airtable list failed ${type}: ${message}`);
    }

    const records = Array.isArray(payload?.records) ? payload.records : [];
    out.push(...records);
    offset = payload?.offset || "";
  } while (offset);

  return out;
}

async function getAirtableRiderRecords(env) {
  const ttl = Math.max(0, Number(env.AIRTABLE_RIDER_CACHE_TTL || DEFAULT_AIRTABLE_RIDER_CACHE_TTL) || 0);
  if (!ttl || typeof caches === "undefined") return airtableListAll(env);

  const cfg = airtableConfig(env);
  const cacheKey = new Request(
    `https://ringstatus-proxy.internal/airtable-rider/${encodeURIComponent(cfg.baseId)}/` +
      `${encodeURIComponent(cfg.table)}/${encodeURIComponent(cfg.view || "default")}`
  );
  const cache = caches.default;
  const cached = await cache.match(cacheKey);
  if (cached) return JSON.parse(await cached.text());

  const records = await airtableListAll(env);
  await cache.put(
    cacheKey,
    new Response(JSON.stringify(records), {
      headers: { "Cache-Control": `public, max-age=${ttl}` },
    })
  );
  return records;
}

function normalizeAirtableRiderRecord(record) {
  const f = record?.fields || {};
  const goRaw = firstText(
    f.goTimeDisplay,
    f.latestGO,
    f.estimatedGO,
    f.goTime,
    f.rs_go_time,
    f.rs_go_time_from_start,
    f.estimated_go_time
  );
  const startRaw = firstText(
    f.startTimeDisplay,
    f.latestStart,
    f.startTime,
    f.rs_start_time,
    f.estimated_start_time
  );
  const endRaw = firstText(
    f.endTimeDisplay,
    f.endTime,
    f.rs_end_time,
    f.estimated_end_time
  );
  const status = firstText(f.status, f.latestStatus, f.rs_status);
  const classNumber = firstText(f.classNumber, f.class_number);
  const sortTime =
    firstNumber(f.sortTime, f.time_sort) ??
    parseClockToSeconds(goRaw) ??
    parseClockToSeconds(startRaw) ??
    999999;
  const lastScore = firstText(
    f.lastScore,
    f.score,
    f.score1,
    f.score_one,
    f.score_1,
    f.score2,
    f.score_2,
    f.score3,
    f.score_3
  );
  const lastTime = firstText(
    f.lastTime,
    f.time_one,
    f.time1,
    f.time_1,
    f.time,
    f.time_two,
    f.time2,
    f.time_2
  );
  const lastPlace = displayPlacing(firstText(
    f.lastPlace,
    f.lastPlacing,
    f.latestPlacing,
    f.placing,
    f.place
  ));
  const category = statusCategory(status);
  const minutesToGo = firstNumber(f.minutesToGo, f.rs_min_till_go);
  const minutesTillStart = firstNumber(f.minutesTillStart, f.rs_mins_till_start);
  const minutesSinceStart = firstNumber(f.minutesSinceStart, f.rs_mins_since_start);
  const classLengthMinutes = firstNumber(f.classLengthMinutes, f.rs_length);
  const minutesToEnd =
    firstNumber(f.minutesToEnd, f.rs_mins_till_end) ??
    (minutesTillStart != null && classLengthMinutes != null ? minutesTillStart + classLengthMinutes : null);
  const goMinutesFromStart = firstNumber(f.goMinutesFromStart, f.rs_go_mins_from_start);
  const minutesToActualGo = firstNumber(f.minutesToActualGo, f.rs_min_to_actual_go);
  const goneIn = firstText(f.goneIn, f.gone_in, f.rs_gone_in);
  const hasResult = Boolean(lastScore || lastTime || lastPlace);

  const riderName = firstText(
    f.teamName,
    f.team_name,
    f.riderName,
    f.rider_name,
    f["Rider Name"]
  );
  const riderFullName = firstText(
    f.riderFullName,
    f.rider_full_name,
    f.rider_name,
    f["Rider Name"]
  );

  const riderSearch = [
    riderName,
    riderFullName,
    f.teamName,
    f.team_name,
    f.riderName,
    f.rider_name,
    f["Rider Name"],
    ...(Array.isArray(f.riderSearch) ? f.riderSearch : []),
  ]
    .map(cleanName)
    .filter(Boolean);
  const horseName = firstText(f.horseName, f.horse_name, f.barnName, f.barn_name);
  const horseSearch = [
    horseName,
    f.horseName,
    f.horse_name,
    f.barnName,
    f.barn_name,
    f.horseId,
    f.h_eid,
    ...(Array.isArray(f.horseSearch) ? f.horseSearch : []),
  ]
    .map(cleanName)
    .filter(Boolean);

  return {
    id: record?.id || "",
    scheduledDate: firstText(f.scheduledDate, f.scheduled_date, f.dt),
    entryClassId: firstText(f.entryClassId, f.entryxclasses_uuid, f.entry_x_classes_uuid),
    entryId: firstText(f.entryId, f.entry_id),
    horseId: firstText(f.horseId, f.h_eid),
    riderName,
    riderFullName,
    riderSearch: [...new Set(riderSearch)],
    horseName,
    horseSearch: [...new Set(horseSearch)],
    groomName: firstText(f.groomName, f.groom_name),
    ringNumber: firstText(f.ringNumber, f.ring_number),
    ringName: firstText(f.ringName, f.ring_name, f.ring_nickname),
    classGroupId: firstText(f.classGroupId, f.class_group_id),
    classId: firstText(f.classId, f.class_id),
    className: firstText(f.className, f.class_name, f.groupName, f.group_name),
    classNumber,
    classType: firstText(f.classType, f.class_type),
    status,
    statusCategory: category,
    tripCategory: tripCategory({ statusCategory: category, minutesToGo, goneIn, hasResult }),
    startTime: startRaw,
    startTimeDisplay: formatDisplayTime(startRaw),
    endTime: endRaw,
    endTimeDisplay: formatDisplayTime(endRaw),
    goTime: goRaw,
    goTimeDisplay: formatDisplayTime(goRaw),
    goTimeFromStart: firstText(f.goTimeFromStart, f.rs_go_time_from_start),
    goTimeFromStartDisplay: formatDisplayTime(firstText(f.goTimeFromStart, f.rs_go_time_from_start)),
    minutesToGo,
    minutesToEnd,
    minutesTillStart,
    minutesSinceStart,
    goMinutesFromStart,
    minutesToActualGo,
    classLengthMinutes,
    orderOfGo: firstText(f.orderOfGo, f.rs_order_of_go),
    runningOrderOfGo: firstText(f.runningOrderOfGo, f.rs_running_order_of_go, f.runningOOG),
    completedTrips: firstNumber(f.completedTrips, f.completed_trips, f.rs_completed_trips),
    totalTrips: firstNumber(f.totalTrips, f.total_trips),
    goneIn,
    lastPlace,
    lastTime,
    lastScore,
    lastPlacing: lastPlace,
    hasResult,
    sortStatus: firstNumber(f.sortStatus) ?? statusRank(status),
    sortTime,
  };
}

function sortNextRecords(records) {
  return [...records].sort((a, b) => {
    const sr = nextTripRank(a) - nextTripRank(b);
    if (sr !== 0) return sr;

    const am = typeof a.minutesToGo === "number" && Number.isFinite(a.minutesToGo) ? Math.max(0, a.minutesToGo) : 999999;
    const bm = typeof b.minutesToGo === "number" && Number.isFinite(b.minutesToGo) ? Math.max(0, b.minutesToGo) : 999999;
    const mr = am - bm;
    if (mr !== 0) return mr;

    const tr = (a.sortTime ?? 999999) - (b.sortTime ?? 999999);
    if (tr !== 0) return tr;
    return String(a.classNumber || "").localeCompare(String(b.classNumber || ""));
  });
}

function sortResultRecords(records) {
  return [...records].sort((a, b) => {
    const rr = resultRank(a) - resultRank(b);
    if (rr !== 0) return rr;
    const tr = (b.sortTime ?? -1) - (a.sortTime ?? -1);
    if (tr !== 0) return tr;
    return String(a.classNumber || "").localeCompare(String(b.classNumber || ""));
  });
}

function lookupMatches(record, query, lane) {
  const values = lane === "horse" ? record.horseSearch : record.riderSearch;
  return values.some((value) => normalize(value) === query.nameNorm);
}

function replyLabelForRecord(record, index, mode, isNextUpcoming) {
  if (mode === "result") return record.hasResult || record.statusCategory === "Completed" ? "Completed" : "Result";
  if (record.statusCategory === "Underway" || record.tripCategory === "Underway") return "Now";
  if (record.statusCategory === "Not Started" || record.tripCategory === "Not Started") return isNextUpcoming ? "Next" : "Not Started";
  if (record.statusCategory === "Completed" || record.tripCategory === "Completed") return "Completed";
  return index === 0 ? "Next" : record.tripCategory;
}

function decorateLookupRecords(records, mode) {
  let foundNextUpcoming = false;
  return records.map((record, index) => {
    const isNextUpcoming = (record.statusCategory === "Not Started" || record.tripCategory === "Not Started") && !foundNextUpcoming;
    if (isNextUpcoming) foundNextUpcoming = true;

    return {
      ...record,
      replyLabel: replyLabelForRecord(record, index, mode, isNextUpcoming),
    };
  });
}

async function airtableLookupResponse(url, env, lane) {
  const query = parseLookupQuery(url);
  if (!query.nameNorm) return json({ ok: false, error: "Missing q" }, 400);

  const rawRecords = await getAirtableRiderRecords(env);
  const sourceFieldNames = [...new Set(rawRecords.flatMap((record) => Object.keys(record?.fields || {})))];
  const records = rawRecords.map((record) => {
    const normalized = normalizeAirtableRiderRecord(record);
    return query.raw ? { ...normalized, rawFields: record?.fields || {} } : normalized;
  });
  let matches = records.filter((record) => lookupMatches(record, query, lane));

  if (query.classQuery) {
    matches = matches.filter((record) => String(record.classNumber || "").startsWith(String(query.classQuery)));
  }

  const sorted = query.mode === "result" ? sortResultRecords(matches) : sortNextRecords(matches);
  const decorated = decorateLookupRecords(sorted, query.mode);
  const selected = decorated[0] || null;
  const limit = query.mode === "all" || query.mode === "class" ? query.limit : 1;

  return json({
    ok: true,
    query,
    source: {
      lane,
      table: String(env.AIRTABLE_RIDER_TABLE || "").trim(),
      view: String(env.AIRTABLE_RIDER_VIEW || "").trim(),
      records: records.length,
      fields: query.raw ? sourceFieldNames : undefined,
    },
    matches: decorated.length,
    selected,
    records: decorated.slice(0, limit),
    allMatches: decorated.slice(0, query.limit),
  }, 200, { "Cache-Control": "no-store" });
}

async function riderLookupResponse(url, env) {
  return airtableLookupResponse(url, env, "rider");
}

async function horseLookupResponse(url, env) {
  return airtableLookupResponse(url, env, "horse");
}

// ---------- proxy GET ----------
async function proxyGet(upstreamUrl, ttlSec, requestedPath) {
  const r = await fetch(upstreamUrl, {
    method: "GET",
    cf: { cacheTtl: ttlSec, cacheEverything: true },
  });

  const h = new Headers(r.headers);
  const contentType = contentTypeForPath(requestedPath);
  const p = String(requestedPath || "").toLowerCase();

  // force correct content-type based on requested file extension
  h.set("content-type", contentType);

  // strip upstream headers that break standalone app rendering
  if (p.endsWith(".html") || p.endsWith(".js")) {
    h.delete("content-security-policy");
    h.delete("x-frame-options");
  }

  for (const [k, v] of Object.entries(corsHeaders)) h.set(k, v);

  return new Response(r.body, { status: r.status, headers: h });
}

// ---------- GitHub commit-bulk (docs/items) ----------
async function ghApi(env, path, init) {
  const url = `https://api.github.com${path}`;
  const headers = new Headers(init?.headers || {});
  headers.set("Accept", "application/vnd.github+json");
  headers.set("Authorization", `Bearer ${env.GITHUB_TOKEN}`);
  headers.set("User-Agent", "ringstatus-proxy");
  if (init?.json) headers.set("Content-Type", "application/json; charset=utf-8");

  const res = await fetch(url, {
    ...init,
    headers,
    body: init?.json ? JSON.stringify(init.json) : init?.body,
  });

  const txt = await res.text();
  let j = null;
  try { j = JSON.parse(txt); } catch {}
  return { ok: res.ok, status: res.status, json: j, text: txt };
}

function requireGitEnv(env) {
  const need = ["GITHUB_REPO", "GITHUB_BRANCH", "GITHUB_TOKEN"];
  for (const k of need) {
    if (!env[k] || !String(env[k]).trim()) throw new Error(`Missing env ${k}`);
  }
}

function validateBulkFiles(files, requiredPrefix) {
  if (!Array.isArray(files) || files.length === 0) throw new Error("files[] required");

  const allowDocsExt = /\.(html|xml|json|js|css)$/i;
  const allowItemsExt = /\.(json|txt|csv|ndjson|md|tmpl)$/i;

  const allowExt = requiredPrefix === "docs/" ? allowDocsExt : allowItemsExt;

  for (let i = 0; i < files.length; i++) {
    const f = files[i];
    if (!f || !f.path || !f.content_base64) throw new Error(`files[${i}] requires path and content_base64`);
    const p = String(f.path);
    if (!p.toLowerCase().startsWith(requiredPrefix)) throw new Error(`files[${i}].path must start with '${requiredPrefix}'`);

    const clean = p.slice(requiredPrefix.length);
    if (!safePath(clean)) throw new Error(`files[${i}].path not safe`);
    if (!allowExt.test(p)) throw new Error(`files[${i}].path extension not allowed`);
  }
}

async function commitBulkToGit(env, body, requiredPrefix) {
  requireGitEnv(env);

  const { message, overwrite, force, files } = body || {};
  const shouldForce = overwrite === true || force === true;

  validateBulkFiles(files, requiredPrefix);

  const repo = env.GITHUB_REPO;
  const branch = env.GITHUB_BRANCH;

  // 1) get current ref
  const refPath = `/repos/${repo}/git/refs/heads/${encodeURIComponent(branch)}`;
  const ref = await ghApi(env, refPath, { method: "GET" });
  if (!ref.ok) throw new Error(`ref ${ref.status}: ${ref.text.slice(0, 200)}`);
  const baseCommit = ref.json?.object?.sha;
  if (!baseCommit) throw new Error("Missing base commit sha");

  // 2) get base tree
  const commit = await ghApi(env, `/repos/${repo}/git/commits/${baseCommit}`, { method: "GET" });
  if (!commit.ok) throw new Error(`commit ${commit.status}: ${commit.text.slice(0, 200)}`);
  const baseTree = commit.json?.tree?.sha;
  if (!baseTree) throw new Error("Missing base tree sha");

  // 3) create blobs
  const blobShas = [];
  for (const f of files) {
    const blob = await ghApi(env, `/repos/${repo}/git/blobs`, {
      method: "POST",
      json: { content: f.content_base64, encoding: "base64" },
    });
    if (!blob.ok) throw new Error(`blob ${blob.status}: ${blob.text.slice(0, 200)}`);
    blobShas.push({ path: f.path, sha: blob.json?.sha });
  }

  // 4) create tree
  const tree = await ghApi(env, `/repos/${repo}/git/trees`, {
    method: "POST",
    json: {
      base_tree: baseTree,
      tree: blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha })),
    },
  });
  if (!tree.ok) throw new Error(`tree ${tree.status}: ${tree.text.slice(0, 200)}`);
  const newTree = tree.json?.sha;
  if (!newTree) throw new Error("Missing new tree sha");

  // 5) create commit
  const commitMsg = message || `${requiredPrefix} bulk publish`;
  const newCommit = await ghApi(env, `/repos/${repo}/git/commits`, {
    method: "POST",
    json: { message: commitMsg, tree: newTree, parents: [baseCommit] },
  });
  if (!newCommit.ok) throw new Error(`commit-post ${newCommit.status}: ${newCommit.text.slice(0, 200)}`);

  const newSha = newCommit.json?.sha;
  if (!newSha) throw new Error("Missing new commit sha");

  // 6) move ref
  const patch = await ghApi(env, refPath, {
    method: "PATCH",
    json: { sha: newSha, force: shouldForce },
  });
  if (!patch.ok) throw new Error(`ref-patch ${patch.status}: ${patch.text.slice(0, 200)}`);

  return {
    ok: true,
    commit: { sha: newSha, url: `https://github.com/${repo}/commit/${newSha}` },
    committed_paths: files.map(f => f.path),
  };
}

// ---------- Worker entry ----------
export default {
  async fetch(req, env, ctx) {
    try {
      const url = new URL(req.url);
      const path = url.pathname;
      const method = req.method.toUpperCase();

      if (method === "OPTIONS") return text("OK", 204);

      const ttl = Number(env.CACHE_TTL || DEFAULT_TTL) || DEFAULT_TTL;

      // health
      if (method === "GET" && path === "/health") return text("OK", 200);

      // Airtable-backed rider lookup for SMS and diagnostics.
      if (method === "GET" && (path === "/rider" || path === "/riders")) {
        return await riderLookupResponse(url, env);
      }

      // Airtable-backed horse lookup using the same table/view as rider lookup.
      if (method === "GET" && (path === "/horse" || path === "/horses")) {
        return await horseLookupResponse(url, env);
      }

      // proxy /api/* -> SGL_API (strip /api prefix) added as latest
      if (path.startsWith("/api/")) {
        const incomingUrl = new URL(req.url);
        const forwardPath = path.replace(/^\/api/, "") || "/";

        const upstreamUrl = new URL(`https://sgl-api.example${forwardPath}`);
        upstreamUrl.search = incomingUrl.search;

        const proxiedReq = new Request(upstreamUrl.toString(), req);
        const res = await env.SGL_API.fetch(proxiedReq);

        return withCors(res);
      }

      // proxy GET /docs/*
      if (method === "GET" && path.startsWith("/docs/")) {
        const rel = normalizeNoLeadingSlash(path.slice("/docs/".length));
        if (!safePath(rel)) return json({ ok: false, error: "Invalid path" }, 400);

        const upstream = `${env.UPSTREAM_BASE}/docs/${rel}`;
        return await proxyGet(upstream, ttl, rel);
      }

      // proxy GET /items/*
      if (method === "GET" && path.startsWith("/items/")) {
        const rel = normalizeNoLeadingSlash(path.slice("/items/".length));
        if (!safePath(rel)) return json({ ok: false, error: "Invalid path" }, 400);

        const upstream = `${env.UPSTREAM_BASE}/items/${rel}`;
        return await proxyGet(upstream, ttl, rel);
      }

      // commit-bulk docs
      if (method === "POST" && path === "/docs/commit-bulk") {
        const body = await readJsonBody(req);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, 400);

        const out = await commitBulkToGit(env, body, "docs/");
        return json(out, 200);
      }

      // commit-bulk items
      if (method === "POST" && path === "/items/commit-bulk") {
        const body = await readJsonBody(req);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, 400);

        const out = await commitBulkToGit(env, body, "items/");
        return json(out, 200);
      }

      return json({ ok: false, error: "Not found" }, 404);
    } catch (e) {
      return json({ ok: false, error: String(e?.message || e).slice(0, 300) }, 500);
    }
  },
};
