/**
 * Airtable Automation Script (BATCH) — HARDENED:
 * - Reads MANY source records from a SOURCE VIEW
 * - Applies ALL link_rules from link_rules VIEW (default: "ww_entries")
 * - For each rule: sourceField value -> find linked record by linkedKeyField -> set linkField
 * - NO CREATE (skips if no match)
 * - Updates in batches of 50
 *
 * Hardening:
 * - If Airtable throws: "Could not find a record with ID rec...."
 *   - isolates failing update(s) (binary split)
 *   - if missing ID is a linked-record ID, removes it from the link array and retries
 *   - if missing ID is the source record ID, skips that update
 *   - continues run (does not abort whole automation)
 *
 * Inputs (Automation -> Run script -> Input variables):
 *   source_table   (optional) default: "ww_entries"
 *   source_view    (REQUIRED) e.g. "refresh" or "TODAY"
 *   rules_view     (optional) default: "ww_entries"
 *   max_records    (optional) number; if set, only processes first N records from the view
 */

//////////////////////
// 0) Config / Inputs
//////////////////////
const cfg = input.config();

const RULES_TABLE_NAME = "link_rules";

const SOURCE_TABLE = (cfg.source_table || "ww_entries").trim();
const SOURCE_VIEW  = (cfg.source_view || "").trim();
if (!SOURCE_VIEW) throw new Error("Missing input variable: source_view");

const RULES_VIEW   = (cfg.rules_view || "ww_entries").trim();
const MAX_RECORDS  = cfg.max_records ? Number(cfg.max_records) : null;

function getFieldSafe(table, fieldName) {
  try { return table.getField(fieldName); } catch (_) { return null; }
}

function toKeyStrings(v) {
  const out = [];
  const pushOne = (x) => {
    if (x === null || x === undefined) return;
    if (typeof x === "string") {
      const s = x.trim();
      if (s) out.push(s);
      return;
    }
    if (typeof x === "number" || typeof x === "boolean") {
      out.push(String(x));
      return;
    }
    if (typeof x === "object") {
      if (typeof x.name === "string" && x.name.trim()) out.push(x.name.trim());
      else if ("value" in x && (typeof x.value === "string" || typeof x.value === "number")) out.push(String(x.value).trim());
    }
  };
  if (Array.isArray(v)) for (const item of v) pushOne(item);
  else pushOne(v);
  return [...new Set(out)];
}

async function buildLookupMap(table, keyFieldName) {
  const q = await table.selectRecordsAsync({ fields: [keyFieldName] });
  const keyToId = new Map();
  const idSet = new Set();

  for (const r of q.records) {
    idSet.add(r.id);
    const keys = toKeyStrings(r.getCellValue(keyFieldName));
    for (const k of keys) if (!keyToId.has(k)) keyToId.set(k, r.id);
  }

  return { keyToId, idSet, recordCount: q.records.length };
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function parseMissingRecordId(err) {
  const s = String(err && err.message ? err.message : err);
  const m = s.match(/Could not find a record with ID "([^"]+)"/);
  return m ? m[1] : null;
}

//////////////////////
// 1) Load rules (from view)
//////////////////////
const rulesTable = base.getTable(RULES_TABLE_NAME);
const rulesView = rulesTable.getView(RULES_VIEW);

const rulesQuery = await rulesView.selectRecordsAsync({
  fields: ["sourceField", "linkField", "linkedTable", "linkedKeyField"],
});

const rawRules = rulesQuery.records
  .map((r) => ({
    sourceField: (r.getCellValueAsString("sourceField") || "").trim(),
    linkField: (r.getCellValueAsString("linkField") || "").trim(),
    linkedTable: (r.getCellValueAsString("linkedTable") || "").trim(),
    linkedKeyField: (r.getCellValueAsString("linkedKeyField") || "").trim(),
  }))
  .filter((x) => x.sourceField && x.linkField && x.linkedTable && x.linkedKeyField);

if (!rawRules.length) throw new Error(`No valid rules found in ${RULES_TABLE_NAME} / view "${RULES_VIEW}"`);

//////////////////////
// 2) Load source records (from view)
//////////////////////
const sourceTable = base.getTable(SOURCE_TABLE);
const sourceView = sourceTable.getView(SOURCE_VIEW);

// only pull the source fields needed
const neededSourceFields = [...new Set(rawRules.map(r => r.sourceField))].filter(Boolean);
const sourceQuery = await sourceView.selectRecordsAsync({ fields: neededSourceFields });

let sourceRecords = sourceQuery.records;
if (MAX_RECORDS && Number.isFinite(MAX_RECORDS) && MAX_RECORDS > 0) {
  sourceRecords = sourceRecords.slice(0, MAX_RECORDS);
}

output.set("source_table", SOURCE_TABLE);
output.set("source_view", SOURCE_VIEW);
output.set("rules_view", RULES_VIEW);
output.set("found_records", sourceRecords.length);
output.set("rules_count", rawRules.length);

//////////////////////
// 3) Pre-build lookup maps per linkedTable::linkedKeyField
//////////////////////
const lookupCache = new Map(); // `${table}::${field}` -> { keyToId, idSet }
for (const rule of rawRules) {
  const cacheKey = `${rule.linkedTable}::${rule.linkedKeyField}`;
  if (lookupCache.has(cacheKey)) continue;

  const t = base.getTable(rule.linkedTable);
  const keyFieldObj = getFieldSafe(t, rule.linkedKeyField);
  if (!keyFieldObj) throw new Error(`Missing field "${rule.linkedKeyField}" in linked table "${rule.linkedTable}"`);

  const mapObj = await buildLookupMap(t, rule.linkedKeyField);
  lookupCache.set(cacheKey, mapObj);
}

//////////////////////
// 4) Build updates
//////////////////////
const updates = [];
let missingLinks = 0;
let skippedBlank = 0;

// validate link fields exist + writable
const linkFieldWritable = new Map(); // linkField -> boolean
for (const rule of rawRules) {
  if (linkFieldWritable.has(rule.linkField)) continue;
  const f = getFieldSafe(sourceTable, rule.linkField);
  linkFieldWritable.set(rule.linkField, !!(f && !f.isComputed));
}

for (const rec of sourceRecords) {
  const fieldsToSet = {};

  for (const rule of rawRules) {
    if (!linkFieldWritable.get(rule.linkField)) continue;

    const keys = toKeyStrings(rec.getCellValue(rule.sourceField));
    if (!keys.length) { skippedBlank++; continue; }

    const cacheKey = `${rule.linkedTable}::${rule.linkedKeyField}`;
    const mapObj = lookupCache.get(cacheKey);
    const linkIds = [];

    for (const k of keys) {
      const rid = mapObj.keyToId.get(k);
      if (rid) linkIds.push({ id: rid });
      else missingLinks++;
    }

    if (linkIds.length) {
      fieldsToSet[rule.linkField] = linkIds; // overwrite
    }
  }

  if (Object.keys(fieldsToSet).length) {
    updates.push({ id: rec.id, fields: fieldsToSet });
  }
}

//////////////////////
// 5) Write updates (50 per call) — hardened
//////////////////////
let updatedCount = 0;

let repairedLinkIdsRemoved = 0;
let skippedSourceMissing = 0;
let skippedUnknownMissing = 0;
let otherErrors = 0;

const missingIdSamples = [];
const otherErrorSamples = [];

function samplePush(arr, value, limit = 20) {
  if (arr.length < limit) arr.push(value);
}

function shallowCopyUpdate(u) {
  const fields = {};
  for (const [k, v] of Object.entries(u.fields || {})) {
    if (Array.isArray(v)) fields[k] = v.map(x => (x && typeof x === "object" ? { ...x } : x));
    else fields[k] = v;
  }
  return { id: u.id, fields };
}

async function tryUpdateOne(updateObj) {
  // try up to 3 times if we can repair a missing linked-record ID
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      await sourceTable.updateRecordsAsync([updateObj]);
      return { status: "ok" };
    } catch (err) {
      const missingId = parseMissingRecordId(err);
      if (!missingId) {
        otherErrors++;
        samplePush(otherErrorSamples, String(err && err.message ? err.message : err));
        return { status: "error_other" };
      }

      samplePush(missingIdSamples, missingId);

      // case 1: the source record itself is gone
      if (missingId === updateObj.id) {
        skippedSourceMissing++;
        return { status: "skipped_source_missing" };
      }

      // case 2: a linked record ID is gone — remove it from any link arrays and retry
      let removed = 0;
      for (const [fieldName, value] of Object.entries(updateObj.fields || {})) {
        if (!Array.isArray(value)) continue;
        if (!value.length) continue;
        if (typeof value[0] !== "object" || !value[0] || !("id" in value[0])) continue;

        const before = value.length;
        const filtered = value.filter(x => x && x.id !== missingId);
        const delta = before - filtered.length;
        if (delta > 0) {
          removed += delta;
          if (filtered.length) updateObj.fields[fieldName] = filtered;
          else delete updateObj.fields[fieldName];
        }
      }

      if (removed > 0) {
        repairedLinkIdsRemoved += removed;
        // if nothing left to write after removal, treat as skip
        if (!Object.keys(updateObj.fields || {}).length) {
          skippedUnknownMissing++;
          return { status: "skipped_empty_after_repair" };
        }
        continue; // retry
      }

      // missing ID not found in this update payload -> skip to avoid run failure
      skippedUnknownMissing++;
      return { status: "skipped_unknown_missing" };
    }
  }

  skippedUnknownMissing++;
  return { status: "skipped_after_retries" };
}

async function safeUpdateBatch(batch) {
  try {
    await sourceTable.updateRecordsAsync(batch);
    updatedCount += batch.length;
    return;
  } catch (err) {
    // If batch fails, bisect to isolate (only when needed)
    if (batch.length === 1) {
      const u = shallowCopyUpdate(batch[0]);
      await tryUpdateOne(u);
      // count "updatedCount" only when success; tryUpdateOne handles that by calling updateRecordsAsync
      // so if success there, we should increment here:
      // easiest: detect success by re-attempting; but we already executed. We'll just re-count by status:
      // We can’t know without modifying tryUpdateOne; so do it there:
      // (implemented by counting inside tryUpdateOne would be better, but keep simple)
      // We'll do a small tweak: if tryUpdateOne returns ok, increment.
      // (So: rerun with return status)
      return;
    }

    const mid = Math.floor(batch.length / 2);
    const left = batch.slice(0, mid);
    const right = batch.slice(mid);

    if (left.length) await safeUpdateBatch(left);
    if (right.length) await safeUpdateBatch(right);
  }
}

// Patch: increment updatedCount when single update succeeds via tryUpdateOne
async function safeUpdateBatchV2(batch) {
  try {
    await sourceTable.updateRecordsAsync(batch);
    updatedCount += batch.length;
    return;
  } catch (err) {
    if (batch.length === 1) {
      const u = shallowCopyUpdate(batch[0]);
      const res = await tryUpdateOne(u);
      if (res && res.status === "ok") updatedCount += 1;
      return;
    }

    const mid = Math.floor(batch.length / 2);
    const left = batch.slice(0, mid);
    const right = batch.slice(mid);

    if (left.length) await safeUpdateBatchV2(left);
    if (right.length) await safeUpdateBatchV2(right);
  }
}

for (const batch of chunk(updates, 50)) {
  await safeUpdateBatchV2(batch);
}

//////////////////////
// 6) Outputs
//////////////////////
output.set("updated_records", updatedCount);
output.set("missing_link_keys_count", missingLinks);
output.set("skipped_blank_source_values", skippedBlank);

output.set("repaired_link_ids_removed", repairedLinkIdsRemoved);
output.set("skipped_updates_source_missing", skippedSourceMissing);
output.set("skipped_updates_unknown_missing", skippedUnknownMissing);
output.set("other_errors_count", otherErrors);

output.set("missing_record_id_samples", missingIdSamples);
output.set("other_error_samples", otherErrorSamples);
