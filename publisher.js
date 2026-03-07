/**
 * publisher.js (FULL DROP) — RingStatus Data Publisher + Per-Tenant Manifest (NO table_view2)
 *
 * INTENT
 * - Code/templates live in repo: sportdogfood/ringstatus
 * - Data snapshots live in repo: sportdogfood/ringstatus-data (via Cloudflare Worker commit gateway)
 *
 * RUNS OUTSIDE AIRTABLE (Task Scheduler heartbeat or manual):
 * - Reads publish_queue (optionally via a view; default: all_active)
 * - Processes only rows where dirty=true
 * - Each row publishes ONE lane (table_view1 -> paths1). No table_view2 is used.
 * - allowed_fields is taken from publish_queue row (comma/newline separated)
 * - Preflight GET of published JSON and SKIP commit if no change
 * - Commits changed paths via /docs/commit-bulk on ringstatus-proxy
 * - Clears dirty; stamps last_publish_epoch ONLY when a commit happens
 *
 * MANIFEST
 * - Any dataset_key starting with "manifest" publishes a tenant manifest to its paths1.
 * - Tenant is inferred from manifest path: docs/{tenant}/manifest.json
 * - Manifest includes ONLY datasets whose paths1 are under docs/{tenant}/...
 *
 * Requires env:
 *   AIRTABLE_TOKEN
 *   AIRTABLE_BASE_ID
 *
 * Optional env:
 *   PUBLISH_QUEUE_TABLE (default: publish_queue)
 *   PUBLISH_QUEUE_VIEW  (default: all_active)   // set to empty to disable view filtering
 *   PUBLISH_URI         (default: https://ringstatus-proxy.gombcg.workers.dev/docs/commit-bulk)
 *   PUBLISHED_BASE      (default: https://ringstatus-proxy.gombcg.workers.dev/)
 *   FORCE_PUSH          (default: 1)
 *   DRY_RUN             (default: 0)
 *   SHOWTIME_URL        (optional; used only to stamp epoch; falls back to local time)
 */

//////////////////////
// 0) Env + constants
//////////////////////
const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;

if (!AIRTABLE_TOKEN) throw new Error("Missing env AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing env AIRTABLE_BASE_ID");

const PUBLISH_QUEUE_TABLE = process.env.PUBLISH_QUEUE_TABLE || "publish_queue";
const PUBLISH_QUEUE_VIEW  = (process.env.PUBLISH_QUEUE_VIEW ?? "all_active").trim(); // allow blank => no view

// RingStatus proxy defaults (data repo = ringstatus-data behind the Worker)
const PUBLISH_URI    = process.env.PUBLISH_URI    || "https://ringstatus-proxy.gombcg.workers.dev/docs/commit-bulk";
const PUBLISHED_BASE = process.env.PUBLISHED_BASE || "https://ringstatus-proxy.gombcg.workers.dev/";

const FORCE_PUSH     = String(process.env.FORCE_PUSH ?? "1") === "1";
const DRY_RUN        = String(process.env.DRY_RUN ?? "0") === "1";
const SHOWTIME_URL   = process.env.SHOWTIME_URL || "";

// publish_queue field names (must match Airtable field names)
const PQ_DATASET_KEY        = "dataset_key";
const PQ_DIRTY              = "dirty";
const PQ_DIRTY_REASON       = "dirty_reason";
const PQ_DIRTY_EPOCH        = "dirty_epoch";         // optional
const PQ_LAST_PUBLISH_EPOCH = "last_publish_epoch";
const PQ_TABLE_NAME         = "table_name";
const PQ_VIEW1              = "table_view1";         // ONLY view used
const PQ_PATHS1             = "paths1";              // ONLY paths used
const PQ_ALLOWED_FIELDS     = "allowed_fields";      // comma/newline list

const CONTENT_TYPE = "application/json";

//////////////////////
// 1) Dataset defaults (fallback only if allowed_fields is blank)
//////////////////////
const DEFAULT_ALLOWED_FIELDS = {
  watch_schedule: [
    "sid",
    "dt",
    "ring_number",
    "ringName",
    "class_groupxclasses_id",
    "class_group_id",
    "group_name",
    "class_id",
    "class_number",
    "class_name",
    "latestStart",
    "latestStatus",
    "total_trips",
    "rollup_entries",
    "rollup_trips",
    "rollup_horses",
  ],
};

//////////////////////
// 2) Helpers
//////////////////////
function nowEpochSecFallback() {
  return Math.floor(Date.now() / 1000);
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = 15000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: ac.signal });
  } finally {
    clearTimeout(t);
  }
}

function normalizePath(p) {
  const s = (p ?? "").toString().trim();
  return s.replace(/^\/+/, "");
}

function parseListFlexible(s) {
  // supports: comma, newline, space-separated
  if (!s) return [];
  return String(s)
    .replace(/\r/g, "")
    .split(/[, \n]+/)
    .map(x => x.trim())
    .filter(Boolean);
}

function parsePaths(s) {
  return parseListFlexible(s).map(normalizePath);
}

function toBase64Utf8(str) {
  return Buffer.from(String(str), "utf8").toString("base64");
}

function stableStringify(obj) {
  const seen = new WeakSet();
  function sorter(x) {
    if (x === null || typeof x !== "object") return x;
    if (seen.has(x)) return null;
    seen.add(x);
    if (Array.isArray(x)) return x.map(sorter);
    const out = {};
    for (const k of Object.keys(x).sort()) out[k] = sorter(x[k]);
    return out;
  }
  return JSON.stringify(sorter(obj));
}

function isManifestKey(k) {
  return String(k || "").trim().toLowerCase().startsWith("manifest");
}

function inferTenantFromManifestPath(paths) {
  // expects docs/{tenant}/manifest.json (or docs/{tenant}/anything/manifest.json)
  if (!paths || !paths.length) return null;
  for (const raw of paths) {
    const p = String(raw || "");
    const m = p.match(/^docs\/([^\/]+)\//i);
    if (m) return m[1];
  }
  return null;
}

async function getEpochSec() {
  if (!SHOWTIME_URL) return nowEpochSecFallback();
  try {
    const res = await fetchWithTimeout(SHOWTIME_URL, { method: "GET" }, 12000);
    const txt = await res.text();
    if (!res.ok) return nowEpochSecFallback();

    let j;
    try { j = JSON.parse(txt); } catch { return nowEpochSecFallback(); }

    const tzd = j && j.time_zone_date_time ? j.time_zone_date_time : null;
    const iso = tzd && (tzd.time_obj || tzd.date_obj) ? (tzd.time_obj || tzd.date_obj) : null;
    if (iso) {
      const d = new Date(iso);
      const ms = d.getTime();
      if (Number.isFinite(ms) && ms > 0) return Math.floor(ms / 1000);
    }
    return nowEpochSecFallback();
  } catch {
    return nowEpochSecFallback();
  }
}

//////////////////////
// 3) Airtable REST
//////////////////////
const AT_BASE = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

function atHeaders() {
  return {
    Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
  };
}

async function airtableListAll({ table, view, fields = null }) {
  // fields:
  // - null => do not send fields[] (safe for schema drift)
  // - []   => do not send fields[] (treat as null)
  // - [..] => send fields[] to reduce payload
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(`${AT_BASE}/${encodeURIComponent(table)}`);
    if (view) url.searchParams.set("view", view);

    if (Array.isArray(fields) && fields.length) {
      for (const f of fields) url.searchParams.append("fields[]", f);
    }

    if (offset) url.searchParams.set("offset", offset);

    const res = await fetchWithTimeout(url.toString(), { method: "GET", headers: atHeaders() }, 20000);
    const j = await res.json().catch(() => ({}));

    if (!res.ok) {
      const msg = (j && j.error && j.error.message) ? j.error.message : JSON.stringify(j).slice(0, 400);
      const type = (j && j.error && j.error.type) ? j.error.type : "";
      const err = new Error(`Airtable list failed (${table}/${view || "NO_VIEW"}): ${res.status} ${type} ${msg}`);
      err._airtable_status = res.status;
      err._airtable_type = type;
      err._airtable_message = msg;
      throw err;
    }

    if (Array.isArray(j.records)) out.push(...j.records);
    offset = j.offset || null;
    if (!offset) break;
  }

  return out;
}

async function airtablePatchRecord({ table, recordId, fields }) {
  const url = `${AT_BASE}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetchWithTimeout(
    url,
    { method: "PATCH", headers: atHeaders(), body: JSON.stringify({ fields }) },
    20000
  );
  const j = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = (j && j.error && j.error.message) ? j.error.message : JSON.stringify(j).slice(0, 400);
    const type = (j && j.error && j.error.type) ? j.error.type : "";
    throw new Error(`Airtable patch failed (${table}/${recordId}): ${res.status} ${type} ${msg}`);
  }
  return j;
}

//////////////////////
// 4) Preflight GET
//////////////////////
async function preflightGetJson(url) {
  try {
    const res = await fetchWithTimeout(url, { method: "GET" }, 15000);
    const txt = await res.text();
    if (!res.ok) return { ok: false, status: res.status, reason: txt.slice(0, 200) };
    try {
      const json = JSON.parse(txt);
      return { ok: true, status: res.status, json };
    } catch {
      return { ok: false, status: res.status, reason: "invalid_json" };
    }
  } catch (e) {
    return { ok: false, status: "fetch_error", reason: String(e?.message || e).slice(0, 200) };
  }
}

//////////////////////
// 5) Commit-bulk (RingStatus proxy)
//////////////////////
function isNonFastForward422(status, text) {
  if (status !== 422) return false;
  const t = String(text || "");
  return (
    t.includes("not a fast forward") ||
    t.includes("Update is not a fast forward") ||
    t.includes("ref-patch 422")
  );
}

async function sleepReal(ms) {
  const w = Math.max(0, Math.min(Number(ms) || 0, 2000));
  if (!w) return;
  await new Promise(r => setTimeout(r, w));
}

async function commitBulk({ message, files, force = true }) {
  const body = { message, force, files };

  const RETRY_MAX_ATTEMPTS = 5;
  const RETRY_BASE_DELAY_MS = 800;

  let lastStatus = 0;
  let lastText = "";

  for (let attempt = 1; attempt <= RETRY_MAX_ATTEMPTS; attempt++) {
    const res = await fetchWithTimeout(
      PUBLISH_URI,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) },
      45000
    );

    lastStatus = res.status;
    lastText = await res.text();

    if (res.ok) return { ok: true, status: res.status, text: lastText };

    if (isNonFastForward422(res.status, lastText) && attempt < RETRY_MAX_ATTEMPTS) {
      const delay = RETRY_BASE_DELAY_MS * attempt + Math.floor(Math.random() * 600);
      await sleepReal(delay);
      continue;
    }

    return { ok: false, status: res.status, text: lastText };
  }

  return { ok: false, status: lastStatus, text: lastText };
}

//////////////////////
// 6) Publish primitives
//////////////////////
function buildRowsFromRecords(records, allowedFields) {
  // Preserve view order (Airtable view controls sorting)
  return records.map(r => {
    const src = r.fields || {};
    const obj = {};
    for (const f of allowedFields) obj[f] = (f in src) ? src[f] : null;
    return obj;
  });
}

async function publishContentToPaths({ datasetKey, contentObj, paths, epochSec }) {
  if (!paths.length) return { ok: true, skipped: true, reason: "no_paths", committed: 0 };

  const contentText = JSON.stringify(contentObj, null, 2) + "\n";
  const changedFiles = [];
  let anyChange = false;

  for (const p of paths) {
    const publishedUrl = `${PUBLISHED_BASE}${normalizePath(p)}`;
    const pre = await preflightGetJson(publishedUrl);

    if (pre.ok) {
      const same = stableStringify(pre.json) === stableStringify(contentObj);
      if (!same) {
        anyChange = true;
        changedFiles.push({
          path: normalizePath(p),
          content_type: CONTENT_TYPE,
          content_base64: toBase64Utf8(contentText),
        });
      }
    } else {
      // If preflight fails, commit for safety.
      anyChange = true;
      changedFiles.push({
        path: normalizePath(p),
        content_type: CONTENT_TYPE,
        content_base64: toBase64Utf8(contentText),
      });
    }
  }

  if (!anyChange) return { ok: true, skipped: true, reason: "no_change", committed: 0 };

  if (DRY_RUN) return { ok: true, skipped: true, reason: "dry_run", committed: 0, wouldCommit: changedFiles.length };

  const msg = `chore: publish ${datasetKey} @${epochSec}`;
  const res = await commitBulk({ message: msg, files: changedFiles, force: FORCE_PUSH });

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      errorText: String(res.text || "").slice(0, 500),
      committed: 0,
    };
  }

  return { ok: true, skipped: false, reason: "published", committed: changedFiles.length, status: res.status };
}

async function publishDataset({
  datasetKey,
  tableName,
  viewName,
  paths,
  allowedFields,
  epochSec,
}) {
  if (!tableName || !viewName) return { ok: true, skipped: true, reason: "missing_table_or_view", committed: 0 };
  if (!paths.length) return { ok: true, skipped: true, reason: "no_paths", committed: 0 };
  if (!allowedFields.length) return { ok: true, skipped: true, reason: "no_allowed_fields", committed: 0 };

  // Try with fields[] first; if Airtable complains about unknown field, retry without fields[].
  let records;
  try {
    records = await airtableListAll({ table: tableName, view: viewName, fields: allowedFields });
  } catch (e) {
    const status = e && e._airtable_status;
    const type   = e && e._airtable_type;
    const msg    = (e && e._airtable_message) ? String(e._airtable_message) : String(e?.message || e);

    if (status === 422 && String(type).toUpperCase() === "UNKNOWN_FIELD_NAME") {
      console.log(`warn: ${datasetKey} unknown field in fields[]; retrying without fields[] | ${msg}`);
      records = await airtableListAll({ table: tableName, view: viewName, fields: null });
    } else {
      throw e;
    }
  }

  const rows = buildRowsFromRecords(records, allowedFields);

  return await publishContentToPaths({
    datasetKey: `${datasetKey} (${tableName}/${viewName})`,
    contentObj: rows,
    paths,
    epochSec,
  });
}

function pickAllowedFields(datasetKey, pqAllowedFieldsRaw) {
  const fromQueue = parseListFlexible(pqAllowedFieldsRaw);
  if (fromQueue.length) return fromQueue;

  const def = DEFAULT_ALLOWED_FIELDS[String(datasetKey || "").trim()] || [];
  return def.slice();
}

//////////////////////
// 7) Manifest
//////////////////////
function buildTenantManifestFromQueue(pqRecords, epochSec, tenant) {
  const datasets = [];

  if (!tenant) {
    return { tenant: null, epoch: epochSec, datasets: [] };
  }

  const tenantPrefix = `docs/${tenant}/`.toLowerCase();

  for (const r of pqRecords) {
    const f = r.fields || {};
    const key = String(f[PQ_DATASET_KEY] || "").trim();
    if (!key) continue;
    if (isManifestKey(key)) continue;

    const paths = parsePaths(f[PQ_PATHS1]).filter(Boolean);
    // ONLY include paths under docs/{tenant}/...
    const tenantPaths = paths.filter(p => String(p).toLowerCase().startsWith(tenantPrefix));
    if (!tenantPaths.length) continue;

    const rawVer = f[PQ_LAST_PUBLISH_EPOCH];
    const version = (rawVer === undefined || rawVer === null || rawVer === "") ? null : Number(rawVer);

    for (const p of tenantPaths) {
      datasets.push({
        key,
        path: normalizePath(p),
        version: Number.isFinite(version) ? version : null,
      });
    }
  }

  datasets.sort((a, b) => (a.key.localeCompare(b.key) || a.path.localeCompare(b.path)));

  return {
    tenant,
    epoch: epochSec,
    datasets,
  };
}

//////////////////////
// 8) Dirty clearing (success vs error)
//////////////////////
async function clearDirtySuccess({ recordId, committedAny, epochSec, reason }) {
  const fields = {
    [PQ_DIRTY]: false,
    [PQ_DIRTY_REASON]: reason,
  };
  if (committedAny) fields[PQ_LAST_PUBLISH_EPOCH] = epochSec;

  await airtablePatchRecord({
    table: PUBLISH_QUEUE_TABLE,
    recordId,
    fields,
  });
}

async function stampDirtyError({ recordId, msg }) {
  await airtablePatchRecord({
    table: PUBLISH_QUEUE_TABLE,
    recordId,
    fields: { [PQ_DIRTY_REASON]: `error: ${msg}` },
  }).catch(() => {});
}

//////////////////////
// 9) Main
//////////////////////
async function main() {
  const epochSec = await getEpochSec();
  console.log(`publisher start | epoch=${epochSec} dry_run=${DRY_RUN}`);

  // IMPORTANT: do NOT pass fields[] here to avoid 422 when schema changes.
  const pqRecords = await airtableListAll({
    table: PUBLISH_QUEUE_TABLE,
    view: PUBLISH_QUEUE_VIEW || null,
    fields: null,
  });

  const dirty = pqRecords.filter(r => Boolean(r.fields && r.fields[PQ_DIRTY]));
  console.log(`queue visible=${pqRecords.length} dirty=${dirty.length}`);

  for (const r of dirty) {
    const f = r.fields || {};

    const datasetKey = String(f[PQ_DATASET_KEY] || "").trim() || "unknown";
    const tableName  = String(f[PQ_TABLE_NAME] || "").trim();
    const viewName   = String(f[PQ_VIEW1] || "").trim();
    const paths      = parsePaths(f[PQ_PATHS1]);

    console.log(`job=${datasetKey} table=${tableName || "-"} view=${viewName || "-"} paths=${paths.length}`);

    try {
      // MANIFEST job
      if (isManifestKey(datasetKey)) {
        const tenant = inferTenantFromManifestPath(paths);
        const manifest = buildTenantManifestFromQueue(pqRecords, epochSec, tenant);

        const resM = await publishContentToPaths({
          datasetKey,
          contentObj: manifest,
          paths,
          epochSec,
        });

        if (!resM.ok) throw new Error(`manifest publish failed (${resM.status || "?"}) ${resM.errorText || ""}`);

        console.log(`job done: ${datasetKey} | ${resM.skipped ? "skip" : "commit"}(${resM.committed || 0})`);

        const committedAny = !resM.skipped && (resM.committed || 0) > 0;
        const reason = resM.reason === "no_change" ? "skipped: no change" : (DRY_RUN ? "dry_run" : "published");
        await clearDirtySuccess({ recordId: r.id, committedAny, epochSec, reason });
        continue;
      }

      // Normal dataset job
      const allowedFields = pickAllowedFields(datasetKey, f[PQ_ALLOWED_FIELDS]);

      const res = await publishDataset({
        datasetKey,
        tableName,
        viewName,
        paths,
        allowedFields,
        epochSec,
      });

      if (!res.ok) throw new Error(`publish failed (${res.status || "?"}) ${res.errorText || ""}`);

      console.log(`job done: ${datasetKey} | ${res.skipped ? "skip" : "commit"}(${res.committed || 0})`);

      const committedAny = !res.skipped && (res.committed || 0) > 0;
      const reason = res.reason === "no_change" ? "skipped: no change" : (DRY_RUN ? "dry_run" : "published");
      await clearDirtySuccess({ recordId: r.id, committedAny, epochSec, reason });
    } catch (e) {
      const msg = String(e?.message || e).slice(0, 240);
      console.log(`job error: ${datasetKey} | ${msg}`);
      await stampDirtyError({ recordId: r.id, msg });
      // keep dirty=true on errors
    }
  }

  console.log("publisher done");
}

main().catch(err => {
  console.error("publisher fatal:", err?.message || err);
  process.exitCode = 1;
});
