/**
 * publisher.js (FULL DROP) — with manifest support
 *
 * Runs OUTSIDE Airtable:
 * - Reads publish_queue (optionally via view; default: all_active)
 * - Processes only records where dirty=true
 * - For each dirty dataset:
 *    - exports rows from table_name + table_view1/table_view2
 *    - writes to paths1/paths2 (comma/newline-separated paths)
 *    - preflight GETs published JSON and skips commit if no change
 *    - commits changed paths via commit-bulk
 *    - clears dirty and stamps last_publish_epoch ONLY when a commit occurs
 *
 * NEW:
 * - dataset_key="manifest" publishes a tenant manifest JSON to its paths.
 *   Manifest is derived from other publish_queue rows (visible in the same queue fetch).
 *
 * Requires env:
 *   AIRTABLE_TOKEN
 *   AIRTABLE_BASE_ID
 *
 * Optional env:
 *   PUBLISH_QUEUE_TABLE (default: publish_queue)
 *   PUBLISH_QUEUE_VIEW  (default: all_active)  // set to empty to disable view filtering
 *   PUBLISH_URI         (default: https://items.clearroundtravel.com/docs/commit-bulk)
 *   PUBLISHED_BASE      (default: https://items.clearroundtravel.com/)
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

function parseCommaList(s) {
  if (!s) return [];
  return String(s)
    .split(",")
    .map(x => x.trim())
    .filter(Boolean);
}

function parsePaths(s) {
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

function inferTenantFromFirstPath(paths) {
  // expects docs/{tenant}/...
  if (!paths || !paths.length) return null;
  const p = String(paths[0] || "");
  const m = p.match(/^docs\/([^\/]+)\//i);
  return m ? m[1] : null;
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
    if (offset) url.searchParams.set("offset", offset);

    const res = await fetchWithTimeout(url.toString(), { method: "GET", headers: atHeaders() }, 20000);
    const j = await res.json().catch(() => ({}));
    if (!res.ok) {
      const msg = (j && j.error && j.error.message) ? j.error.message : JSON.stringify(j).slice(0, 300);
      throw new Error(`Airtable list failed (${table}/${view || "NO_VIEW"}): ${res.status} ${msg}`);
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

async function publishContentToPaths({ datasetKey, contentObj, paths, epochSec }) {
  if (!paths.length) return { ok: true, skipped: true, reason: "no_paths" };

  const contentText = JSON.stringify(contentObj, null, 2);
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
      anyChange = true;
      changedFiles.push({
        path: normalizePath(p),
        content_type: CONTENT_TYPE,
        content_base64: toBase64Utf8(contentText),
      });
    }
  }

  if (!anyChange) return { ok: true, skipped: true, reason: "no_change", committed: 0 };

  if (DRY_RUN) return { ok: true, skipped: true, reason: "dry_run", wouldCommit: changedFiles.length };

  const msg = `chore: publish ${datasetKey} @${epochSec}`;
  const res = await commitBulk({ message: msg, files: changedFiles, force: FORCE_PUSH });
  if (!res.ok) return { ok: false, status: res.status, errorText: String(res.text || "").slice(0, 300), committed: 0 };

  return { ok: true, skipped: false, committed: changedFiles.length, status: res.status };
}

async function publishDatasetSlot({
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

  const records = await airtableListAll({ table: tableName, view: viewName, fields: allowedFields });
  const rows = buildRowsFromRecords(records, allowedFields);

  return await publishContentToPaths({
    datasetKey: `${datasetKey} (${tableName}/${viewName})`,
    contentObj: rows,
    paths,
    epochSec,
  });
}

function pickAllowedFields(datasetKey, pqAllowedFieldsRaw) {
  const fromQueue = parseCommaList(pqAllowedFieldsRaw);
  if (fromQueue.length) return fromQueue;

  const def = DEFAULT_ALLOWED_FIELDS[String(datasetKey || "").trim()] || [];
  return def.slice();
}

function buildManifestFromQueue(pqRecords, epochSec, tenantHint) {
  // include all non-manifest rows that have at least one path
  const datasets = [];

  for (const r of pqRecords) {
    const f = r.fields || {};
    const key = String(f[PQ_DATASET_KEY] || "").trim();
    if (!key) continue;
    if (key.toLowerCase() === "manifest") continue;

    const p1 = parsePaths(f[PQ_PATHS1]);
    const p2 = parsePaths(f[PQ_PATHS2]);
    const allPaths = [...p1, ...p2].filter(Boolean);

    if (!allPaths.length) continue;

    // If tenantHint is present and we see any tenant-style paths in this row,
    // keep only those paths matching docs/{tenant}/..., otherwise keep legacy paths too.
    let filteredPaths = allPaths;
    if (tenantHint) {
      const tenantPrefix = `docs/${tenantHint}/`;
      const hasAnyTenantPaths = allPaths.some(p => String(p).toLowerCase().startsWith("docs/"));
      if (hasAnyTenantPaths) {
        const match = allPaths.filter(p => String(p).toLowerCase().startsWith(tenantPrefix.toLowerCase()));
        if (match.length) filteredPaths = match;
      }
    }

    const version = (f[PQ_LAST_PUBLISH_EPOCH] === undefined || f[PQ_LAST_PUBLISH_EPOCH] === null || f[PQ_LAST_PUBLISH_EPOCH] === "")
      ? null
      : Number(f[PQ_LAST_PUBLISH_EPOCH]);

    for (const p of filteredPaths) {
      datasets.push({
        key,
        path: normalizePath(p),
        version: Number.isFinite(version) ? version : null,
      });
    }
  }

  datasets.sort((a, b) => (a.key.localeCompare(b.key) || a.path.localeCompare(b.path)));

  return {
    tenant: tenantHint || null,
    epoch: epochSec,
    datasets,
  };
}

async function clearDirtySuccess({ recordId, committedAny, epochSec, reason }) {
  const fields = {
    [PQ_DIRTY]: false,
    [PQ_DIRTY_REASON]: reason,
  };

  // Stamp last_publish_epoch ONLY when a commit occurred.
  if (committedAny) fields[PQ_LAST_PUBLISH_EPOCH] = epochSec;

  await airtableUpdateRecord({
    table: PUBLISH_QUEUE_TABLE,
    recordId,
    fields,
  });
}

async function stampDirtyError({ recordId, msg }) {
  await airtableUpdateRecord({
    table: PUBLISH_QUEUE_TABLE,
    recordId,
    fields: { [PQ_DIRTY_REASON]: `error: ${msg}` },
  }).catch(() => {});
}

async function main() {
  const epochSec = await getEpochSec();
  console.log(`publisher start | epoch=${epochSec} dry_run=${DRY_RUN}`);

  // Read queue
  const pqRecords = await airtableListAll({
    table: PUBLISH_QUEUE_TABLE,
    view: PUBLISH_QUEUE_VIEW || null,
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
      // MANIFEST job (special)
      if (datasetKey.toLowerCase() === "manifest") {
        const manifestPaths = [...slot1.paths, ...slot2.paths].filter(Boolean);
        const tenantHint = inferTenantFromFirstPath(manifestPaths);
        const manifest = buildManifestFromQueue(pqRecords, epochSec, tenantHint);

        const resM = await publishContentToPaths({
          datasetKey: "manifest",
          contentObj: manifest,
          paths: manifestPaths,
          epochSec,
        });

        if (!resM.ok) throw new Error(`manifest publish failed (${resM.status || "?"}) ${resM.errorText || ""}`);

        const committedAny = !resM.skipped && (resM.committed || 0) > 0;
        const reason = resM.reason === "no_change" ? "skipped: no change" : (DRY_RUN ? "dry_run" : "published");
        console.log(`job done: ${datasetKey} | ${resM.skipped ? "skip" : "commit"}(${resM.committed || 0})`);

        await clearDirtySuccess({ recordId: r.id, committedAny, epochSec, reason });
        continue;
      }

      // Normal dataset job
      const allowedFields = pickAllowedFields(datasetKey, f[PQ_ALLOWED_FIELDS]);

      const res1 = await publishDatasetSlot({
        datasetKey,
        tableName,
        viewName: slot1.viewName,
        paths: slot1.paths,
        allowedFields,
        epochSec,
      });

      let res2 = { ok: true, skipped: true, reason: "no_slot2", committed: 0 };
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

      if (!res1.ok) throw new Error(`slot1 failed (${res1.status || "?"}) ${res1.errorText || ""}`);
      if (!res2.ok) throw new Error(`slot2 failed (${res2.status || "?"}) ${res2.errorText || ""}`);

      const c1 = res1.committed || 0;
      const c2 = res2.committed || 0;

      const summary = `ok v1=${res1.skipped ? "skip" : "commit"}(${c1}) v2=${res2.skipped ? "skip" : "commit"}(${c2})`;
      console.log(`job done: ${datasetKey} | ${summary}`);

      const committedAny = (!res1.skipped && c1 > 0) || (!res2.skipped && c2 > 0);
      const bothNoChange = (res1.reason === "no_change" || res1.skipped) && (res2.reason === "no_change" || res2.skipped);
      const reason = bothNoChange ? "skipped: no change" : (DRY_RUN ? "dry_run" : "published");

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
