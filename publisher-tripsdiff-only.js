/**
 * publisher.js (FULL DROP) — RingStatus Data Publisher + Per-Tenant Manifest (NO table_view2)
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

const FORCE_PUSH     = String(process.env.FORCE_PUSH ?? "1") === "1";
const DRY_RUN        = String(process.env.DRY_RUN ?? "0") === "1";
const SHOWTIME_URL   = process.env.SHOWTIME_URL || "";

const PQ_DATASET_KEY        = "dataset_key";
const PQ_DIRTY              = "dirty";
const PQ_DIRTY_REASON       = "dirty_reason";
const PQ_DIRTY_EPOCH        = "dirty_epoch";
const PQ_LAST_PUBLISH_EPOCH = "last_publish_epoch";
const PQ_TABLE_NAME         = "table_name";
const PQ_VIEW1              = "table_view1";
const PQ_PATHS1             = "paths1";
const PQ_ALLOWED_FIELDS     = "allowed_fields";

const CONTENT_TYPE = "application/json";

//////////////////////
// 1) Dataset defaults
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
// 6) Publish primitives
//////////////////////
function buildRowsFromRecords(records, allowedFields) {
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
  const newBlob = contentText;
  const changedFiles = [];
  const shaPrev = paths[0] ? await getLatestCommitShaForPath(paths[0]) : null;
  const oldBlob = (shaPrev && paths[0]) ? await getRawFileTextAtSha(shaPrev, paths[0]) : null;
  console.log(`PREV_SHA path=${paths[0] || "-"} sha=${shaPrev || "-"}`);
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

  if (DRY_RUN) return { ok: true, skipped: true, reason: "dry_run", committed: 0, wouldCommit: changedFiles.length };

  const msg = `chore: publish ${datasetKey} @${epochSec}`;
  const res = await commitBulk({ message: msg, files: changedFiles, force: FORCE_PUSH });
  const shaNew = res?.json?.commit?.sha || null;
  console.log(`BLOB_STATE path=${paths[0] || "-"} shaPrev=${shaPrev || "-"} shaNew=${shaNew || "-"} oldLen=${oldBlob ? oldBlob.length : 0} newLen=${newBlob ? newBlob.length : 0}`);

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      errorText: String(res.text || "").slice(0, 500),
      committed: 0,
    };
  }

  return {
    ok: true,
    skipped: false,
    reason: "published",
    committed: changedFiles.length,
    status: res.status,
    shaPrev,
    shaNew,
    oldBlob,
    newBlob,
    repoPath: (paths[0] || null)
  };
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
// 8) Field diffs — trips only
//////////////////////
function buildDiffRows({ datasetKey, repoPath, shaPrev, shaNew, epochSec, oldBlob, newBlob }) {
  const cfg =
    /schedules\/trips\.json$/i.test(repoPath || "")
      ? {
          keyField: "entryxclasses_uuid",
          watched: [
            "estimated_start_time",
            "estimated_go_time",
            "latestStatus",
            "completed_trips",
            "lastOOG",
            "lastGoneIn"
          ],
          numericFields: new Set([
            "lastGoneIn",
            "completed_trips",
            "lastOOG"
          ]),
          textIdFields: ["entryxclasses_uuid"],
          numericIdFields: ["entry_id", "class_id"]
        }
      : null;

  if (!cfg) return [];

  const { keyField, watched, numericFields, textIdFields, numericIdFields } = cfg;
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

      const oldNorm = normalizeTextValue(oldRaw);
      const newNorm = normalizeTextValue(newRaw);

      if (oldNorm === newNorm) continue;

      const useValueFields = numericFields.has(fieldName);

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

      if (useValueFields) {
        fields.old_value = oldNorm;
        fields.new_value = newNorm;
      } else {
        fields.old_text = oldNorm;
        fields.new_text = newNorm;
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

  return out;
}

//////////////////////
// 9) Dirty clearing
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
// 10) Main
//////////////////////
async function main() {
  const epochSec = await getEpochSec();
  console.log(`publisher start | epoch=${epochSec} dry_run=${DRY_RUN}`);

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

      console.log(`job done: ${datasetKey} | ${res.skipped ? "skip" : "commit"}(${res.committed || 0}) | sha=${res.shaNew || "-"}`);
      console.log(`DIFF_READY path=${res.repoPath || "-"} shaPrev=${res.shaPrev || "-"} shaNew=${res.shaNew || "-"} oldLen=${res.oldBlob ? res.oldBlob.length : 0} newLen=${res.newBlob ? res.newBlob.length : 0}`);

      const diffRows = buildDiffRows({
        datasetKey,
        repoPath: res.repoPath,
        shaPrev: res.shaPrev,
        shaNew: res.shaNew,
        epochSec,
        oldBlob: res.oldBlob,
        newBlob: res.newBlob
      });

      console.log(`FIELD_DIFF path=${res.repoPath || "-"} rows=${diffRows.length}`);

      if (!res.skipped && diffRows.length > 0) {
        await airtableCreateRecords({
          table: "publish_diffs",
          records: diffRows
        });
      }

      const committedAny = !res.skipped && (res.committed || 0) > 0;
      const reason = res.reason === "no_change" ? "skipped: no change" : (DRY_RUN ? "dry_run" : "published");
      await clearDirtySuccess({ recordId: r.id, committedAny, epochSec, reason });
    } catch (e) {
      const msg = String(e?.message || e).slice(0, 240);
      console.log(`job error: ${datasetKey} | ${msg}`);
      await stampDirtyError({ recordId: r.id, msg });
    }
  }

  console.log("publisher done");
}

main().catch(err => {
  console.error("publisher fatal:", err?.message || err);
  process.exitCode = 1;
});