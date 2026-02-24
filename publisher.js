/**
 * publisher.js (FULL DROP)
 *
 * Runs OUTSIDE Airtable:
 * - Reads publish_queue view (default: all_active)
 * - Processes only records where dirty=true
 * - For each dirty dataset:
 *    - exports rows from table_name + table_view1/table_view2
 *    - writes to paths1/paths2 (comma-separated paths)
 *    - preflight GETs published JSON and skips commit if no change
 *    - commits changed paths via commit-bulk (items.clearroundtravel.com)
 *    - clears dirty and stamps last_publish_epoch
 *
 * Requires env:
 *   AIRTABLE_TOKEN
 *   AIRTABLE_BASE_ID
 *
 * Optional env (defaults provided):
 *   PUBLISH_QUEUE_TABLE (default: publish_queue)
 *   PUBLISH_QUEUE_VIEW  (default: all_active)
 *   PUBLISH_URI         (default: https://items.clearroundtravel.com/docs/commit-bulk)
 *   PUBLISHED_BASE      (default: https://items.clearroundtravel.com/)
 *   FORCE_PUSH          (default: 1)
 *   DRY_RUN             (default: 0)
 *   SHOWTIME_URL        (optional; used only to stamp epoch; falls back to local time)
 */

//////////////////////
// 0) Env + constants
//////////////////////
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;

if (!AIRTABLE_TOKEN) throw new Error("Missing env AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing env AIRTABLE_BASE_ID");

const PUBLISH_QUEUE_TABLE = process.env.PUBLISH_QUEUE_TABLE || "publish_queue";
const PUBLISH_QUEUE_VIEW  = process.env.PUBLISH_QUEUE_VIEW  || "all_active";

const PUBLISH_URI    = process.env.PUBLISH_URI    || "https://items.clearroundtravel.com/docs/commit-bulk";
const PUBLISHED_BASE = process.env.PUBLISHED_BASE || "https://items.clearroundtravel.com/";
const FORCE_PUSH     = String(process.env.FORCE_PUSH ?? "1") === "1";
const DRY_RUN        = String(process.env.DRY_RUN ?? "0") === "1";

const SHOWTIME_URL   = process.env.SHOWTIME_URL || "";

// publish_queue field names (match Airtable visible names)
const PQ_DATASET_KEY        = "dataset_key";
const PQ_DIRTY              = "dirty";
const PQ_DIRTY_REASON       = "dirty_reason";
const PQ_DIRTY_EPOCH        = "dirty_epoch";
const PQ_LAST_PUBLISH_EPOCH = "last_publish_epoch";
const PQ_TABLE_NAME         = "table_name";
const PQ_VIEW1              = "table_view1";
const PQ_VIEW2              = "table_view2";
const PQ_PATHS1             = "paths1";
const PQ_PATHS2             = "paths2";
const PQ_ALLOWED_FIELDS     = "allowed_fields";

// Commit bulk payload defaults
const CONTENT_TYPE = "application/json";

//////////////////////
// 1) Dataset defaults (only used if publish_queue.allowed_fields is blank)
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
    "class_type",
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
    const res = await fetch(url, { ...opts, signal: ac.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

function normalizePath(p) {
  const s = (p ?? "").toString().trim();
  return s.replace(/^\/+/, "");
}

function parseCommaList(s) {
  if (!s) return [];
  return String(s)
    .split(",")
    .map(x => x.trim())
    .filter(Boolean);
}

function parsePaths(s) {
  // expects comma-separated; tolerates newlines
  if (!s) return [];
  return String(s)
    .replace(/\r/g, "")
    .split(/[, \n]+/)
    .map(x => x.trim())
    .filter(Boolean)
    .map(normalizePath);
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
    "Authorization": `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
  };
}

async function airtableListAll({ table, view, fields = [] }) {
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(`${AT_BASE}/${encodeURIComponent(table)}`);
    if (view) url.searchParams.set("view", view);

    for (const f of fields) url.searchParams.append("fields[]", f);

    // Keep Airtable from returning huge cell objects (optional)
    // url.searchParams.set("cellFormat", "json");

    if (offset) url.searchParams.set("offset", offset);

    const res = await fetchWithTimeout(url.toString(), { method: "GET", headers: atHeaders() }, 20000);
    const j = await res.json().catch(() => ({}));
    if (!res.ok) {
      const msg = (j && j.error && j.error.message) ? j.error.message : JSON.stringify(j).slice(0, 300);
      throw new Error(`Airtable list failed (${table}/${view}): ${res.status} ${msg}`);
    }

    if (Array.isArray(j.records)) out.push(...j.records);
    offset = j.offset || null;
    if (!offset) break;
  }

  return out;
}

async function airtableUpdateRecord({ table, recordId, fields }) {
  const url = `${AT_BASE}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetchWithTimeout(
    url,
    { method: "PATCH", headers: atHeaders(), body: JSON.stringify({ fields }) },
    20000
  );
  const j = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = (j && j.error && j.error.message) ? j.error.message : JSON.stringify(j).slice(0, 300);
    throw new Error(`Airtable patch failed (${table}/${recordId}): ${res.status} ${msg}`);
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
// 5) Commit-bulk
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
      30000
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
// 6) Core publish logic
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

async function publishDatasetSlot({
  datasetKey,
  tableName,
  viewName,
  paths,
  allowedFields,
  epochSec,
}) {
  if (!tableName || !viewName) return { ok: true, skipped: true, reason: "missing_table_or_view" };
  if (!paths.length) return { ok: true, skipped: true, reason: "no_paths" };
  if (!allowedFields.length) return { ok: true, skipped: true, reason: "no_allowed_fields" };

  const records = await airtableListAll({ table: tableName, view: viewName, fields: allowedFields });
  const rows = buildRowsFromRecords(records, allowedFields);
  const contentText = JSON.stringify(rows, null, 2);

  // Preflight per path; only include changed paths in commit
  const changedFiles = [];
  let anyChange = false;

  for (const p of paths) {
    const publishedUrl = `${PUBLISHED_BASE}${normalizePath(p)}`;
    const pre = await preflightGetJson(publishedUrl);

    if (pre.ok) {
      const same = stableStringify(pre.json) === stableStringify(rows);
      if (!same) {
        anyChange = true;
        changedFiles.push({
          path: normalizePath(p),
          content_type: CONTENT_TYPE,
          content_base64: toBase64Utf8(contentText),
        });
      }
    } else {
      // If preflight fails, proceed to commit for safety
      anyChange = true;
      changedFiles.push({
        path: normalizePath(p),
        content_type: CONTENT_TYPE,
        content_base64: toBase64Utf8(contentText),
      });
    }
  }

  if (!anyChange) return { ok: true, skipped: true, reason: "no_change", count: rows.length };

  if (DRY_RUN) return { ok: true, skipped: true, reason: "dry_run", wouldCommit: changedFiles.length, count: rows.length };

  const msg = `chore: publish ${datasetKey} (${tableName}/${viewName}) @${epochSec}`;
  const res = await commitBulk({ message: msg, files: changedFiles, force: FORCE_PUSH });

  if (!res.ok) return { ok: false, status: res.status, errorText: String(res.text || "").slice(0, 300), count: rows.length };

  return { ok: true, skipped: false, committed: changedFiles.length, status: res.status, count: rows.length };
}

function pickAllowedFields(datasetKey, pqAllowedFieldsRaw) {
  const fromQueue = parseCommaList(pqAllowedFieldsRaw);
  if (fromQueue.length) return fromQueue;

  const def = DEFAULT_ALLOWED_FIELDS[String(datasetKey || "").trim()] || [];
  return def.slice();
}

async function main() {
  const epochSec = await getEpochSec();
  console.log(`publisher start | epoch=${epochSec} dry_run=${DRY_RUN}`);

  // Read queue (view determines which records are visible)
  const pqRecords = await airtableListAll({
    table: PUBLISH_QUEUE_TABLE,
    view: PUBLISH_QUEUE_VIEW,
    fields: [
      PQ_DATASET_KEY,
      PQ_DIRTY,
      PQ_DIRTY_REASON,
      PQ_DIRTY_EPOCH,
      PQ_LAST_PUBLISH_EPOCH,
      PQ_TABLE_NAME,
      PQ_VIEW1,
      PQ_VIEW2,
      PQ_PATHS1,
      PQ_PATHS2,
      PQ_ALLOWED_FIELDS,
    ],
  });

  const dirty = pqRecords.filter(r => Boolean(r.fields && r.fields[PQ_DIRTY]));
  console.log(`queue visible=${pqRecords.length} dirty=${dirty.length}`);

  for (const r of dirty) {
    const f = r.fields || {};
    const datasetKey = String(f[PQ_DATASET_KEY] || "").trim() || "unknown";
    const tableName  = String(f[PQ_TABLE_NAME] || "").trim();
    const allowedFields = pickAllowedFields(datasetKey, f[PQ_ALLOWED_FIELDS]);

    const slot1 = {
      viewName: String(f[PQ_VIEW1] || "").trim(),
      paths: parsePaths(f[PQ_PATHS1]),
    };
    const slot2 = {
      viewName: String(f[PQ_VIEW2] || "").trim(),
      paths: parsePaths(f[PQ_PATHS2]),
    };

    console.log(`job=${datasetKey} table=${tableName} v1=${slot1.viewName} p1=${slot1.paths.length} v2=${slot2.viewName} p2=${slot2.paths.length}`);

    try {
      // Slot 1
      const res1 = await publishDatasetSlot({
        datasetKey,
        tableName,
        viewName: slot1.viewName,
        paths: slot1.paths,
        allowedFields,
        epochSec,
      });

      // Slot 2 (only if configured)
      let res2 = { ok: true, skipped: true, reason: "no_slot2" };
      if (slot2.viewName && slot2.paths.length) {
        res2 = await publishDatasetSlot({
          datasetKey,
          tableName,
          viewName: slot2.viewName,
          paths: slot2.paths,
          allowedFields,
          epochSec,
        });
      }

      const summary = `ok v1=${res1.skipped ? "skip" : "commit"}(${res1.committed || 0}) v2=${res2.skipped ? "skip" : "commit"}(${res2.committed || 0})`;
      console.log(`job done: ${datasetKey} | ${summary}`);

      // Clear dirty regardless of commit vs skip (because the job evaluated + is now consistent)
      await airtableUpdateRecord({
        table: PUBLISH_QUEUE_TABLE,
        recordId: r.id,
        fields: {
          [PQ_DIRTY]: false,
          [PQ_LAST_PUBLISH_EPOCH]: epochSec,
          [PQ_DIRTY_REASON]: res1.reason === "no_change" && res2.reason === "no_change"
            ? "skipped: no change"
            : (DRY_RUN ? "dry_run" : "published"),
        },
      });
    } catch (e) {
      const msg = String(e?.message || e).slice(0, 240);
      console.log(`job error: ${datasetKey} | ${msg}`);

      // Keep dirty=true, stamp reason for visibility
      await airtableUpdateRecord({
        table: PUBLISH_QUEUE_TABLE,
        recordId: r.id,
        fields: {
          [PQ_DIRTY_REASON]: `error: ${msg}`,
        },
      }).catch(() => {});
    }
  }

  console.log("publisher done");
}

main().catch(err => {
  console.error("publisher fatal:", err?.message || err);
  process.exitCode = 1;
});
