/**
 * publisher.js (FULL DROP)
 *
 * Fast publisher:
 * - dirty + use_differ=false  -> grouped into one bulk commit call
 * - dirty + use_differ=true   -> processed in differ lane
 *
 * Exact queue paths only.
 * No auto-generated paths.
 * Diff runs only for explicit *trips-catch-changes.json paths.
 */

//////////////////////
// 0) Env + constants
//////////////////////
const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;

if (!AIRTABLE_TOKEN) throw new Error("Missing env AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing env AIRTABLE_BASE_ID");

const PUBLISH_QUEUE_TABLE = process.env.PUBLISH_QUEUE_TABLE || "publish_queue";
const PUBLISH_QUEUE_VIEW  = (process.env.PUBLISH_QUEUE_VIEW ?? "all_active").trim();

const PUBLISH_URI    = process.env.PUBLISH_URI    || "https://ringstatus-proxy.gombcg.workers.dev/docs/commit-bulk";
const PUBLISHED_BASE = process.env.PUBLISHED_BASE || "https://ringstatus-proxy.gombcg.workers.dev/";
const PUBLISH_DIFFS_TABLE = process.env.PUBLISH_DIFFS_TABLE || "publish_diffs";

const FORCE_PUSH   = String(process.env.FORCE_PUSH ?? "1") === "1";
const DRY_RUN      = String(process.env.DRY_RUN ?? "0") === "1";
const SHOWTIME_URL = process.env.SHOWTIME_URL || "";
const PUBLISHER_LANE = String(process.env.PUBLISHER_LANE || "all").trim().toLowerCase(); // all | bulk | differ

const PQ_DATASET_KEY        = "dataset_key";
const PQ_DIRTY              = "dirty";
const PQ_DIRTY_REASON       = "dirty_reason";
const PQ_LAST_PUBLISH_EPOCH = "last_publish_epoch";
const PQ_TABLE_NAME         = "table_name";
const PQ_VIEW1              = "table_view1";
const PQ_PATHS1             = "paths1";
const PQ_ALLOWED_FIELDS     = "allowed_fields";
const PQ_USE_DIFFER         = "use_differ";

const CONTENT_TYPE = "application/json";

//////////////////////
// 1) Diff config
//////////////////////
const DIFF_PATH_REGEX = /\/trips-catch-changes\.json$/i;

const DIFF_TRIPS_FIELDS = parseListFlexible(
  process.env.DIFF_TRIPS_FIELDS ||
  "estimated_start_time,estimated_go_time,latestStatus,completed_trips"
);

const DIFF_NUMERIC_FIELDS = new Set(
  parseListFlexible(
    process.env.DIFF_NUMERIC_FIELDS ||
    "completed_trips,lastGoneIn,lastOOG"
  )
);

const DIFF_TOLERANT_TIME_FIELDS = new Set(
  parseListFlexible(
    process.env.DIFF_TOLERANT_TIME_FIELDS ||
    "estimated_start_time,estimated_go_time"
  )
);

const DIFF_TIME_TOLERANCE_SEC = Number(process.env.DIFF_TIME_TOLERANCE_SEC || "180");

//////////////////////
// 2) Dataset defaults
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
// 3) Helpers
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
  const s = String(p ?? "").trim();
  return s.replace(/^\/+/, "");
}

function parseListFlexible(s) {
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

function buildRowsFromRecords(records, allowedFields) {
  return records.map(r => {
    const src = r.fields || {};
    const obj = {};
    for (const f of allowedFields) obj[f] = (f in src) ? src[f] : null;
    return obj;
  });
}

function safeJsonParseArray(txt) {
  try {
    const v = JSON.parse(txt);
    return Array.isArray(v) ? v : [];
  } catch {
    return [];
  }
}

function normalizeTextValue(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

function pickRowValue(row, fieldName) {
  if (!row || typeof row !== "object") return undefined;
  return row[fieldName];
}

function pickIdValue(newRow, oldRow, fieldName) {
  const a = pickRowValue(newRow, fieldName);
  if (a !== null && a !== undefined && String(a).trim() !== "") return a;
  const b = pickRowValue(oldRow, fieldName);
  if (b !== null && b !== undefined && String(b).trim() !== "") return b;
  return null;
}

function toAirtableNumber(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function isBlankLike(v) {
  return v === null || v === undefined || String(v).trim() === "";
}

function numericCompareValue(v) {
  if (isBlankLike(v)) return 0;
  const n = Number(String(v).trim());
  return Number.isFinite(n) ? n : 0;
}

function parseHmsToSeconds(v) {
  if (isBlankLike(v)) return 0;
  const s = String(v).trim();
  if (s === "00:00:00") return 0;
  const m = s.match(/^(\d{1,2}):(\d{2}):(\d{2})$/);
  if (!m) return null;
  return (Number(m[1]) * 3600) + (Number(m[2]) * 60) + Number(m[3]);
}

function computeFieldDiff(fieldName, oldRaw, newRaw, cfg) {
  if (cfg.numericFields.has(fieldName)) {
    const oldNum = numericCompareValue(oldRaw);
    const newNum = numericCompareValue(newRaw);
    if (oldNum === newNum) return null;
    return {
      useValueFields: true,
      oldOut: String(oldNum),
      newOut: String(newNum)
    };
  }

  if (cfg.tolerantTimeFields.has(fieldName)) {
    const oldSec = parseHmsToSeconds(oldRaw);
    const newSec = parseHmsToSeconds(newRaw);

    if (oldSec !== null && newSec !== null) {
      if (Math.abs(oldSec - newSec) < cfg.timeToleranceSec) return null;
      return {
        useValueFields: false,
        oldOut: normalizeTextValue(oldRaw),
        newOut: normalizeTextValue(newRaw)
      };
    }

    const oldText = normalizeTextValue(oldRaw);
    const newText = normalizeTextValue(newRaw);
    if (oldText === newText) return null;

    return {
      useValueFields: false,
      oldOut: oldText,
      newOut: newText
    };
  }

  const oldText = normalizeTextValue(oldRaw);
  const newText = normalizeTextValue(newRaw);
  if (oldText === newText) return null;

  return {
    useValueFields: false,
    oldOut: oldText,
    newOut: newText
  };
}

function pickAllowedFields(datasetKey, pqAllowedFieldsRaw) {
  const fromQueue = parseListFlexible(pqAllowedFieldsRaw);
  if (fromQueue.length) return fromQueue;

  const def = DEFAULT_ALLOWED_FIELDS[String(datasetKey || "").trim()] || [];
  return def.slice();
}

function boolCell(v) {
  return v === true || String(v).trim().toLowerCase() === "true" || String(v).trim() === "1";
}

function laneAllowsRecord(useDiffer) {
  if (PUBLISHER_LANE === "bulk") return !useDiffer;
  if (PUBLISHER_LANE === "differ") return useDiffer;
  return true;
}

//////////////////////
// 4) Airtable REST
//////////////////////
const AT_BASE = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

function atHeaders() {
  return {
    Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
  };
}

async function airtableListAll({ table, view, fields = null }) {
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

async function airtableCreateRecords({ table, records }) {
  if (!Array.isArray(records) || !records.length) return { records: [] };

  const out = [];

  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const url = `${AT_BASE}/${encodeURIComponent(table)}`;

    const res = await fetchWithTimeout(
      url,
      { method: "POST", headers: atHeaders(), body: JSON.stringify({ records: chunk }) },
      20000
    );

    const j = await res.json().catch(() => ({}));
    if (!res.ok) {
      const msg = (j && j.error && j.error.message) ? j.error.message : JSON.stringify(j).slice(0, 400);
      const type = (j && j.error && j.error.type) ? j.error.type : "";
      throw new Error(`Airtable create failed (${table}): ${res.status} ${type} ${msg}`);
    }

    if (Array.isArray(j.records)) out.push(...j.records);
  }

  return { records: out };
}

//////////////////////
// 5) Preflight GET
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
// 6) Commit-bulk
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
    console.log("COMMIT_BULK_RESPONSE", res.status, lastText);

    if (res.ok) {
      let json = null;
      try { json = JSON.parse(lastText); } catch {}
      return { ok: true, status: res.status, text: lastText, json };
    }

    if (isNonFastForward422(res.status, lastText) && attempt < RETRY_MAX_ATTEMPTS) {
      const delay = RETRY_BASE_DELAY_MS * attempt + Math.floor(Math.random() * 600);
      await sleepReal(delay);
      continue;
    }

    return { ok: false, status: res.status, text: lastText };
  }

  return { ok: false, status: lastStatus, text: lastText };
}

const GITHUB_COMMITS_API_BASE = "https://api.github.com/repos/sportdogfood/ringstatus-data/commits";

async function getLatestCommitShaForPath(repoPath) {
  const path = normalizePath(repoPath);
  const url = new URL(GITHUB_COMMITS_API_BASE);
  url.searchParams.set("path", path);
  url.searchParams.set("per_page", "1");

  const res = await fetchWithTimeout(
    url.toString(),
    {
      method: "GET",
      headers: {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ringstatus-publisher"
      }
    },
    15000
  );

  const txt = await res.text();
  if (!res.ok) throw new Error(`GitHub commits lookup failed (${res.status}) ${txt.slice(0, 200)}`);

  let arr = [];
  try { arr = JSON.parse(txt); } catch {}
  return Array.isArray(arr) && arr[0] && arr[0].sha ? arr[0].sha : null;
}

async function getRawFileTextAtSha(sha, repoPath) {
  if (!sha || !repoPath) return null;
  const path = normalizePath(repoPath);
  const url = `https://raw.githubusercontent.com/sportdogfood/ringstatus-data/${sha}/${path}`;

  const res = await fetchWithTimeout(
    url,
    {
      method: "GET",
      headers: {
        "User-Agent": "ringstatus-publisher"
      }
    },
    15000
  );

  if (!res.ok) return null;
  return await res.text();
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
// 8) Prepare jobs
//////////////////////
async function buildContentObjectForRow(row, pqRecords, epochSec) {
  const f = row.fields || {};
  const datasetKey = String(f[PQ_DATASET_KEY] || "").trim() || "unknown";
  const tableName  = String(f[PQ_TABLE_NAME] || "").trim();
  const viewName   = String(f[PQ_VIEW1] || "").trim();
  const paths      = parsePaths(f[PQ_PATHS1]);
  const allowedFields = pickAllowedFields(datasetKey, f[PQ_ALLOWED_FIELDS]);

  if (isManifestKey(datasetKey)) {
    const tenant = inferTenantFromManifestPath(paths);
    return {
      datasetKey,
      tableName,
      viewName,
      paths,
      useDiffer: boolCell(f[PQ_USE_DIFFER]),
      contentObj: buildTenantManifestFromQueue(pqRecords, epochSec, tenant)
    };
  }

  if (!tableName || !viewName) throw new Error("missing_table_or_view");
  if (!paths.length) throw new Error("no_paths");
  if (!allowedFields.length) throw new Error("no_allowed_fields");

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

  return {
    datasetKey,
    tableName,
    viewName,
    paths,
    useDiffer: boolCell(f[PQ_USE_DIFFER]),
    contentObj: buildRowsFromRecords(records, allowedFields)
  };
}

async function prepareBulkRow(row, pqRecords, epochSec) {
  const built = await buildContentObjectForRow(row, pqRecords, epochSec);
  const contentText = JSON.stringify(built.contentObj, null, 2) + "\n";
  const changedFiles = [];

  for (const p of built.paths) {
    const normalized = normalizePath(p);
    const publishedUrl = `${PUBLISHED_BASE}${normalized}`;
    const pre = await preflightGetJson(publishedUrl);

    if (pre.ok) {
      const same = stableStringify(pre.json) === stableStringify(built.contentObj);
      if (!same) {
        changedFiles.push({
          path: normalized,
          content_type: CONTENT_TYPE,
          content_base64: toBase64Utf8(contentText),
        });
      }
    } else {
      changedFiles.push({
        path: normalized,
        content_type: CONTENT_TYPE,
        content_base64: toBase64Utf8(contentText),
      });
    }
  }

  return {
    recordId: row.id,
    datasetKey: built.datasetKey,
    paths: built.paths,
    changedFiles,
    changedAny: changedFiles.length > 0
  };
}

async function prepareDifferRow(row, pqRecords, epochSec) {
  const built = await buildContentObjectForRow(row, pqRecords, epochSec);
  const contentText = JSON.stringify(built.contentObj, null, 2) + "\n";
  const primaryPath = normalizePath(built.paths[0] || "");
  const changedFiles = [];

  for (const p of built.paths) {
    const normalized = normalizePath(p);
    const publishedUrl = `${PUBLISHED_BASE}${normalized}`;
    const pre = await preflightGetJson(publishedUrl);

    if (pre.ok) {
      const same = stableStringify(pre.json) === stableStringify(built.contentObj);
      if (!same) {
        changedFiles.push({
          path: normalized,
          content_type: CONTENT_TYPE,
          content_base64: toBase64Utf8(contentText),
        });
      }
    } else {
      changedFiles.push({
        path: normalized,
        content_type: CONTENT_TYPE,
        content_base64: toBase64Utf8(contentText),
      });
    }
  }

  let shaPrev = null;
  let oldBlob = null;

  if (changedFiles.length > 0 && DIFF_PATH_REGEX.test(primaryPath)) {
    shaPrev = await getLatestCommitShaForPath(primaryPath);
    oldBlob = await getRawFileTextAtSha(shaPrev, primaryPath);
  }

  return {
    recordId: row.id,
    datasetKey: built.datasetKey,
    paths: built.paths,
    changedFiles,
    changedAny: changedFiles.length > 0,
    repoPath: primaryPath,
    shaPrev,
    oldBlob,
    newBlob: contentText
  };
}

//////////////////////
// 9) Diff builder
//////////////////////
function buildDiffRows({ datasetKey, repoPath, shaPrev, shaNew, epochSec, oldBlob, newBlob }) {
  if (!DIFF_PATH_REGEX.test(String(repoPath || ""))) return [];

  const cfg = {
    keyField: "entryxclasses_uuid",
    watched: DIFF_TRIPS_FIELDS,
    numericFields: DIFF_NUMERIC_FIELDS,
    tolerantTimeFields: DIFF_TOLERANT_TIME_FIELDS,
    timeToleranceSec: DIFF_TIME_TOLERANCE_SEC,
    textIdFields: ["entryxclasses_uuid"],
    numericIdFields: ["entry_id", "class_id"]
  };

  const { keyField, watched, textIdFields, numericIdFields } = cfg;
  const oldRows = safeJsonParseArray(oldBlob);
  const newRows = safeJsonParseArray(newBlob);

  const oldMap = new Map(
    oldRows.filter(r => r && r[keyField] != null).map(r => [String(r[keyField]), r])
  );
  const newMap = new Map(
    newRows.filter(r => r && r[keyField] != null).map(r => [String(r[keyField]), r])
  );

  const out = [];
  const changedAtIso = new Date(Number(epochSec) * 1000).toISOString();

  for (const [rowKey, newRow] of newMap.entries()) {
    if (!oldMap.has(rowKey)) continue;

    const oldRow = oldMap.get(rowKey);

    for (const fieldName of watched) {
      const oldRaw = pickRowValue(oldRow, fieldName);
      const newRaw = pickRowValue(newRow, fieldName);

      const diff = computeFieldDiff(fieldName, oldRaw, newRaw, cfg);
      if (!diff) continue;

      const fields = {
        dataset_key: datasetKey,
        path: repoPath || "",
        sha_prev: shaPrev || "",
        sha_new: shaNew || "",
        field_name: fieldName,
        changed_at: changedAtIso,
        file_ref: `${shaNew || ""}:${repoPath || ""}:${rowKey}:${fieldName}`,
        published_epoch: Number(epochSec)
      };

      if (diff.useValueFields) {
        fields.old_value = diff.oldOut;
        fields.new_value = diff.newOut;
      } else {
        fields.old_text = diff.oldOut;
        fields.new_text = diff.newOut;
      }

      for (const textIdField of textIdFields) {
        const idValue = pickIdValue(newRow, oldRow, textIdField);
        if (idValue !== null && idValue !== undefined && String(idValue).trim() !== "") {
          fields[textIdField] = String(idValue).trim();
        }
      }

      for (const numericIdField of numericIdFields) {
        const idValue = pickIdValue(newRow, oldRow, numericIdField);
        const n = toAirtableNumber(idValue);
        if (n !== null) {
          fields[numericIdField] = n;
        }
      }

      out.push({ fields });
    }
  }

  for (const [rowKey, oldRow] of oldMap.entries()) {
    if (newMap.has(rowKey)) continue;

    const fields = {
      dataset_key: datasetKey,
      path: repoPath || "",
      sha_prev: shaPrev || "",
      sha_new: shaNew || "",
      field_name: "__deleted__",
      old_text: "present",
      new_text: "missing",
      changed_at: changedAtIso,
      file_ref: `${shaNew || ""}:${repoPath || ""}:${rowKey}:__deleted__`,
      published_epoch: Number(epochSec)
    };

    for (const textIdField of textIdFields) {
      const idValue = pickIdValue(null, oldRow, textIdField);
      if (idValue !== null && idValue !== undefined && String(idValue).trim() !== "") {
        fields[textIdField] = String(idValue).trim();
      }
    }

    for (const numericIdField of numericIdFields) {
      const idValue = pickIdValue(null, oldRow, numericIdField);
      const n = toAirtableNumber(idValue);
      if (n !== null) {
        fields[numericIdField] = n;
      }
    }

    out.push({ fields });
  }

  return out;
}

//////////////////////
// 10) Queue writeback
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
// 11) Main
//////////////////////
async function main() {
  const epochSec = await getEpochSec();
  console.log(`publisher start | epoch=${epochSec} dry_run=${DRY_RUN} lane=${PUBLISHER_LANE}`);

  const pqRecords = await airtableListAll({
    table: PUBLISH_QUEUE_TABLE,
    view: PUBLISH_QUEUE_VIEW || null,
    fields: null,
  });

  const dirty = pqRecords.filter(r => Boolean(r.fields && r.fields[PQ_DIRTY]));
  const todo = dirty.filter(r => laneAllowsRecord(boolCell(r.fields?.[PQ_USE_DIFFER])));

  const bulkRows = todo.filter(r => !boolCell(r.fields?.[PQ_USE_DIFFER]));
  const differRows = todo.filter(r => boolCell(r.fields?.[PQ_USE_DIFFER]));

  console.log(`queue visible=${pqRecords.length} dirty=${dirty.length} bulk=${bulkRows.length} differ=${differRows.length}`);

  // BULK LANE
  const bulkPrepared = [];
  for (const r of bulkRows) {
    const f = r.fields || {};
    const datasetKey = String(f[PQ_DATASET_KEY] || "").trim() || "unknown";
    const tableName  = String(f[PQ_TABLE_NAME] || "").trim();
    const viewName   = String(f[PQ_VIEW1] || "").trim();
    const paths      = parsePaths(f[PQ_PATHS1]);

    console.log(`bulk job=${datasetKey} table=${tableName || "-"} view=${viewName || "-"} paths=${paths.length}`);

    try {
      const prepared = await prepareBulkRow(r, pqRecords, epochSec);
      bulkPrepared.push(prepared);
    } catch (e) {
      const msg = String(e?.message || e).slice(0, 240);
      console.log(`bulk job error: ${datasetKey} | ${msg}`);
      await stampDirtyError({ recordId: r.id, msg });
    }
  }

  const bulkChangedFiles = bulkPrepared.flatMap(x => x.changedFiles);
  let bulkCommitOk = true;
  let bulkCommitSha = null;
  let bulkCommitErr = "";

  if (bulkChangedFiles.length > 0) {
    if (DRY_RUN) {
      console.log(`bulk commit skipped: dry_run files=${bulkChangedFiles.length}`);
    } else {
      const bulkCommit = await commitBulk({
        message: `chore: publish bulk @${epochSec}`,
        files: bulkChangedFiles,
        force: FORCE_PUSH
      });

      if (!bulkCommit.ok) {
        bulkCommitOk = false;
        bulkCommitErr = String(bulkCommit.text || "").slice(0, 300);
      } else {
        bulkCommitSha = bulkCommit?.json?.commit?.sha || null;
      }
    }
  }

  if (bulkChangedFiles.length > 0 && bulkCommitOk) {
    console.log(`bulk done: commit(${bulkChangedFiles.length}) | sha=${bulkCommitSha || "-"}`);
  }

  for (const prepared of bulkPrepared) {
    if (!prepared.changedAny) {
      await clearDirtySuccess({
        recordId: prepared.recordId,
        committedAny: false,
        epochSec,
        reason: DRY_RUN ? "dry_run" : "skipped: no change"
      });
      continue;
    }

    if (!bulkCommitOk && !DRY_RUN) {
      await stampDirtyError({
        recordId: prepared.recordId,
        msg: `bulk commit failed ${bulkCommitErr}`
      });
      continue;
    }

    await clearDirtySuccess({
      recordId: prepared.recordId,
      committedAny: !DRY_RUN,
      epochSec,
      reason: DRY_RUN ? "dry_run" : "published"
    });
  }

  // DIFFER LANE
  for (const r of differRows) {
    const f = r.fields || {};
    const datasetKey = String(f[PQ_DATASET_KEY] || "").trim() || "unknown";
    const tableName  = String(f[PQ_TABLE_NAME] || "").trim();
    const viewName   = String(f[PQ_VIEW1] || "").trim();
    const paths      = parsePaths(f[PQ_PATHS1]);

    console.log(`differ job=${datasetKey} table=${tableName || "-"} view=${viewName || "-"} paths=${paths.length}`);

    try {
      const prepared = await prepareDifferRow(r, pqRecords, epochSec);

      if (!prepared.changedAny) {
        await clearDirtySuccess({
          recordId: prepared.recordId,
          committedAny: false,
          epochSec,
          reason: DRY_RUN ? "dry_run" : "skipped: no change"
        });
        continue;
      }

      let shaNew = null;

      if (!DRY_RUN) {
        const res = await commitBulk({
          message: `chore: publish ${prepared.datasetKey} @${epochSec}`,
          files: prepared.changedFiles,
          force: FORCE_PUSH
        });

        if (!res.ok) {
          throw new Error(`differ commit failed (${res.status || "?"}) ${String(res.text || "").slice(0, 300)}`);
        }

        shaNew = res?.json?.commit?.sha || null;
        console.log(`differ done: ${prepared.datasetKey} | commit(${prepared.changedFiles.length}) | sha=${shaNew || "-"}`);
      } else {
        console.log(`differ dry_run: ${prepared.datasetKey} files=${prepared.changedFiles.length}`);
      }

      if (!DRY_RUN && DIFF_PATH_REGEX.test(String(prepared.repoPath || ""))) {
        const diffRows = buildDiffRows({
          datasetKey: prepared.datasetKey,
          repoPath: prepared.repoPath,
          shaPrev: prepared.shaPrev,
          shaNew,
          epochSec,
          oldBlob: prepared.oldBlob,
          newBlob: prepared.newBlob
        });

        console.log(`FIELD_DIFF path=${prepared.repoPath || "-"} rows=${diffRows.length}`);

        if (diffRows.length > 0) {
          await airtableCreateRecords({
            table: PUBLISH_DIFFS_TABLE,
            records: diffRows
          });
        }
      }

      await clearDirtySuccess({
        recordId: prepared.recordId,
        committedAny: !DRY_RUN,
        epochSec,
        reason: DRY_RUN ? "dry_run" : "published + differ"
      });
    } catch (e) {
      const msg = String(e?.message || e).slice(0, 240);
      console.log(`differ job error: ${datasetKey} | ${msg}`);
      await stampDirtyError({ recordId: r.id, msg });
    }
  }

  console.log("publisher done");
}

main().catch(err => {
  console.error("publisher fatal:", err?.message || err);
  process.exitCode = 1;
});