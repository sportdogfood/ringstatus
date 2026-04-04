/**
 * Airtable Automation Script: Wellington liveclassv2 ingest (SAFE VIEW REBUILD)
 *
 * CHANGE:
 * - If payload parses but produces 0 create candidates:
 *     ✅ DO NOT FAIL
 *     ✅ DO NOT DELETE anything
 *     ✅ Just log + output "noop" and exit the script cleanly
 *
 * - DO NOT DELETE anything until:
 *    1) payload fetch/parse succeeds
 *    2) we successfully CREATE the new records
 * - After successful CREATE, we DELETE only the old records (view-scoped),
 *   excluding the newly created record IDs.
 *
 * Inputs (Automation -> Run script -> Input variables):
 *   - target_table (string, REQUIRED)  e.g. "liveclass_latest"
 *   - target_view  (string, REQUIRED)  e.g. "latest" (view-scoped wipe)
 *   - endpoint_url (string, optional)  default below
 *   - run_tag      (string, optional)
 */

const DEFAULT_URL =
  "https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true";

//////////////////////
// 0) Inputs
//////////////////////
let cfg = {};
if (typeof input !== "undefined" && input && typeof input.config === "function") cfg = input.config();

const targetTableName = (cfg.target_table || "").trim();
const targetViewName = (cfg.target_view || "").trim();
const endpointUrl = (cfg.endpoint_url || DEFAULT_URL).trim();
const runTag = (cfg.run_tag || "").trim();

if (!targetTableName) throw new Error('Missing required input: "target_table"');
if (!targetViewName) throw new Error('Missing required input: "target_view"');

//////////////////////
// OUTPUT helper
//////////////////////
function outSet(k, v) {
  if (typeof output === "undefined" || !output || typeof output.set !== "function") return;
  output.set(k, v);
}

outSet("target_table", targetTableName);
outSet("target_view", targetViewName);
outSet("endpoint_url", endpointUrl);
outSet("run_tag_input", runTag);

//////////////////////
// Helpers
//////////////////////
const BATCH_SIZE = 50;
const nowIso = new Date().toISOString();
const effectiveRunTag = runTag || `liveclassv2_${nowIso}`;

function isObj(v) {
  return v && typeof v === "object" && !Array.isArray(v);
}

function toDateOnly(v) {
  if (v === null || v === undefined) return "";
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  const s = String(v).trim();
  if (!s) return "";
  if (s.includes("T")) return s.split("T")[0];
  if (s.includes(" ")) return s.split(" ")[0];
  return s;
}

function yyyymmdd(v) {
  const d = toDateOnly(v);
  return d ? d.replaceAll("-", "") : "";
}

function parseNum(v) {
  if (v === null || v === undefined || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function safeJoin(arr) {
  if (!Array.isArray(arr)) return "";
  return arr
    .filter((x) => x !== null && x !== undefined && String(x).trim() !== "")
    .map((x) => String(x).trim())
    .join(", ");
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function getTableMeta(tbl) {
  const writableFields = tbl.fields.filter((f) => !f.isComputed);
  const writableByName = new Map(writableFields.map((f) => [f.name, f]));
  const writableNames = new Set(writableFields.map((f) => f.name));
  return { writableByName, writableNames };
}

function coerceForField(field, v) {
  if (!field) return undefined;
  if (v === undefined || v === null) return undefined;

  const isEmptyString = typeof v === "string" && v === "";

  switch (field.type) {
    case "singleLineText":
    case "multilineText":
    case "richText":
    case "url":
    case "email":
    case "phoneNumber":
      return String(v);

    case "number":
    case "currency":
    case "percent":
    case "rating":
    case "duration": {
      if (isEmptyString) return undefined;
      const n = parseNum(v);
      return n === undefined ? undefined : n;
    }

    case "checkbox":
      if (isEmptyString) return undefined;
      return Boolean(v);

    case "date": {
      if (isEmptyString) return undefined;
      const d = toDateOnly(v);
      return d ? d : undefined;
    }

    case "dateTime": {
      if (isEmptyString) return undefined;
      if (v instanceof Date) return v.toISOString();
      if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}T/.test(v)) return v;
      return typeof v === "string" ? v : undefined;
    }

    case "singleSelect":
      if (isEmptyString) return undefined;
      return { name: String(v) };

    case "multipleSelects":
      if (isEmptyString) return undefined;
      if (Array.isArray(v)) {
        const arr = v.filter((x) => x !== null && x !== undefined && x !== "");
        return arr.length ? arr.map((x) => ({ name: String(x) })) : undefined;
      }
      return [{ name: String(v) }];

    default:
      return v;
  }
}

function buildWritableFields(flat, meta) {
  const out = {};
  for (const [k, raw] of Object.entries(flat)) {
    if (!meta.writableNames.has(k)) continue;
    const field = meta.writableByName.get(k);
    const coerced = coerceForField(field, raw);
    if (coerced === undefined) continue;
    out[k] = coerced;
  }
  if (effectiveRunTag && meta.writableNames.has("run_tag")) out.run_tag = String(effectiveRunTag);
  if (nowIso && meta.writableNames.has("ingested_at")) out.ingested_at = String(nowIso);
  return out;
}

function ensureWritablePrimary(tbl, meta, fields, fallbackVal) {
  try {
    const pf = tbl.primaryField;
    if (!pf) return;
    if (pf.isComputed) return;
    if (!meta.writableNames.has(pf.name)) return;
    if (fields[pf.name] !== undefined) return;
    if (fallbackVal === undefined || fallbackVal === null || fallbackVal === "") return;
    fields[pf.name] = String(fallbackVal);
  } catch {}
}

async function fetchJson(url) {
  const res = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Fetch failed ${res.status} ${res.statusText} :: ${body.slice(0, 800)}`);
  }

  const txt = await res.text();
  try {
    return JSON.parse(txt);
  } catch {
    throw new Error(`Response was not valid JSON. First 800 chars:\n${txt.slice(0, 800)}`);
  }
}

//////////////////////
// 1) Resolve table/view + meta
//////////////////////
const targetTable = base.getTable(targetTableName);
if (!targetTable) throw new Error(`Table not found: ${targetTableName}`);

const targetView = targetTable.getView(targetViewName);
if (!targetView) throw new Error(`View not found: ${targetViewName} (table: ${targetTableName})`);

const targetMeta = getTableMeta(targetTable);

//////////////////////
// 2) FETCH + BUILD create payload (NO DELETES YET)
//////////////////////
const payload = await fetchJson(endpointUrl);
if (!Array.isArray(payload)) throw new Error("Unexpected payload: expected top-level array.");

outSet("payload_top_level_count", payload.length);
outSet("effective_run_tag", effectiveRunTag);
outSet("ingested_at", nowIso);

const toCreate = [];

for (const showObj of payload) {
  if (!isObj(showObj)) continue;

  const show_id_raw = showObj.show_id;
  const show_id_num = parseNum(show_id_raw);
  const show_id_str = show_id_raw == null ? "" : String(show_id_raw).trim();
  if (!show_id_str) continue;

  const show_name = showObj.show_name || "";

  const items = Array.isArray(showObj.json_data) ? showObj.json_data : [];
  for (const item of items) {
    if (!isObj(item)) continue;

    const day = toDateOnly(item.day || "");
    const ring_number_num = parseNum(item.ring_number);
    const ring_number_str = item.ring_number == null ? "" : String(item.ring_number).trim();

    const show_key = show_id_str;
    const show_day_key = day ? `${show_id_str}-${yyyymmdd(day)}` : "";
    const show_ring_key = ring_number_str ? `${show_id_str}-${ring_number_str}` : "";

    const class_numbers_val =
      item.class_numbers !== undefined && item.class_numbers !== null && item.class_numbers !== ""
        ? item.class_numbers
        : safeJoin(item.classNumbers);

    const rawFields = {
      // derived
      show_key,
      show_day_key,
      show_ring_key,

      // core
      show_id: show_id_num !== undefined ? show_id_num : show_id_str,
      day: day,
      ring_number: ring_number_num !== undefined ? ring_number_num : ring_number_str,

      class_group_id: parseNum(item.class_group_id) ?? (item.class_group_id ?? ""),
      group_name: item.group_name ?? "",
      estimated_start_time: item.estimated_start_time ?? "",
      ring_id: parseNum(item.ring_id) ?? (item.ring_id ?? ""),

      classes: safeJoin(item.classes),
      class_numbers: class_numbers_val ?? "",

      status: item.status ?? "",
      gone: item.gone ?? null,
      total: item.total ?? null,
      is_live: item.is_live ?? false,
      has_JSON: item.has_JSON ?? false,
      curr_updated_at: item.curr_updated_at ?? null,

      // optional
      show_name: show_name,
      run_tag: effectiveRunTag,
      ingested_at: nowIso,
    };

    const fields = buildWritableFields(rawFields, targetMeta);

    ensureWritablePrimary(targetTable, targetMeta, fields, show_ring_key || show_day_key || show_key);

    if (fields && Object.keys(fields).length) toCreate.push({ fields });
  }
}

outSet("create_candidate_count", toCreate.length);

//////////////////////
// ✅ NOOP MODE (do not fail; do not delete; allow automation to continue)
//////////////////////
if (!toCreate.length) {
  outSet("run_status", "NOOP");
  outSet("noop_reason", "Payload parsed but produced 0 create candidates (skipped create + delete)");
  outSet("created_count", 0);
  outSet("deleted_count", 0);
  console.log("NOOP: payload parsed but produced 0 create candidates. Skipping create/delete.");
  // clean exit: script ends successfully here
} else {
  //////////////////////
  // 3) CREATE new records FIRST (batched 50)
  //    If create fails, we STOP and DO NOT DELETE old records.
  //////////////////////
  let created = 0;
  const newRecordIds = [];

  try {
    for (const batch of chunk(toCreate, BATCH_SIZE)) {
      const ids = await targetTable.createRecordsAsync(batch); // returns created record IDs
      if (Array.isArray(ids)) newRecordIds.push(...ids);
      created += Array.isArray(ids) ? ids.length : batch.length;
    }
  } catch (err) {
    outSet("run_status", "CREATE_FAILED");
    outSet("create_error", String(err?.message || err));
    outSet("created_before_error", created);
    outSet("new_ids_sample", newRecordIds.slice(0, 10).join(", "));
    throw err;
  }

  outSet("run_status", "CREATED");
  outSet("created_count", created);
  outSet("new_ids_count", newRecordIds.length);
  outSet("new_ids_sample", newRecordIds.slice(0, 10).join(", "));

  //////////////////////
  // 4) DELETE old records visible in the view (batched 50)
  //    Exclude the newly created records (by ID)
  //////////////////////
  const newIdSet = new Set(newRecordIds);

  const delQuery = await targetView.selectRecordsAsync({ fields: [] });
  const allViewIds = delQuery.records.map((r) => r.id);

  // delete everything in the view except newly created records
  const delIds = allViewIds.filter((id) => !newIdSet.has(id));

  let deleted = 0;
  for (const ids of chunk(delIds, BATCH_SIZE)) {
    await targetTable.deleteRecordsAsync(ids);
    deleted += ids.length;
  }

  outSet("run_status", "SUCCESS");
  outSet("deleted_count", deleted);
  outSet("source_url", endpointUrl);
}
