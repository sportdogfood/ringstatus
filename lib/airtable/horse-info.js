/**
 * Airtable Automation Script: ww_horses UPSERT by horse
 *
 * Inputs:
 *   search_by   ("search_by_entry_id" | "search_by_horse" | "search_by_entry_no")
 *   search_uri  (URL string OR one of:
 *                "missing entry id"
 *                "missing entry_number"
 *                "missing horse name")
 *
 * Target table: ww_horses
 * Key: horse
 *
 * Writes to ww_horses (if fields exist and are writable):
 *   horse            (key)
 *   entry_id         (Number)
 *   entry_number     (Number)   <-- from payload.number
 *   horse_id         (Number)
 *   trainer_id       (Number)
 *   rider_id         (Number)
 *   rider_list       (Long text)
 *   app_show_id      (Number)   <-- from payload.show_id
 *   run_id           (Number)   <-- YYYYMMDD
 *   last_run         (Date/time ISO)
 *   new_horse        (Checkbox) <-- set TRUE on create only
 *
 * Writes to automation_errs on known missing-input sentinels and hard failures.
 */

const HORSES_TABLE = "ww_horses";
const ERRS_TABLE = "automation_errs";
const KEY_FIELD = "horse";
const AUTOMATION_NAME = "horse_detail";

// ----------------------------
// INPUTS
// ----------------------------
const { search_by, search_uri } = input.config();

if (!search_by || typeof search_by !== "string") {
  throw new Error("Missing required input variable: search_by");
}
if (!search_uri || typeof search_uri !== "string") {
  throw new Error("Missing required input variable: search_uri");
}

// ----------------------------
// DERIVED RUN FIELDS
// ----------------------------
const now = new Date();
const run_id = Number(
  `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}`
);
const last_run = now.toISOString();

// ----------------------------
// OUTPUT HELPERS
// ----------------------------
function outSet(k, v) {
  if (typeof output === "undefined" || !output || typeof output.set !== "function") return;
  output.set(k, v);
}
function outSetJson(k, v) {
  outSet(k, JSON.stringify(v ?? null));
}

outSet("search_by", search_by);
outSet("search_uri", search_uri);
outSet("run_id", run_id);
outSet("last_run", last_run);

// ----------------------------
// HELPERS
// ----------------------------
function isObj(v) {
  return v && typeof v === "object" && !Array.isArray(v);
}

function parseNum(v) {
  if (v === null || v === undefined || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function normalizeKey(v) {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

function getTableMeta(tbl) {
  const fieldsByName = new Map(tbl.fields.map((f) => [f.name, f]));
  const writableFields = tbl.fields.filter((f) => !f.isComputed);
  const writableByName = new Map(writableFields.map((f) => [f.name, f]));
  const writableNames = new Set(writableFields.map((f) => f.name));
  return { fieldsByName, writableFields, writableByName, writableNames };
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

    case "checkbox": {
      if (isEmptyString) return undefined;
      if (typeof v === "boolean") return v;
      const n = parseNum(v);
      if (n !== undefined) return n !== 0;
      const s = String(v).trim().toLowerCase();
      if (["true", "t", "yes", "y", "1"].includes(s)) return true;
      if (["false", "f", "no", "n", "0"].includes(s)) return false;
      return undefined;
    }

    case "date":
    case "dateTime":
      return String(v);

    default:
      return v;
  }
}

function buildWritableFields(sourceObj, meta, extra = {}) {
  const out = {};

  const merged = { ...sourceObj, ...extra };

  for (const [k, raw] of Object.entries(merged)) {
    if (!meta.writableNames.has(k)) continue;
    const f = meta.writableByName.get(k);
    const coerced = coerceForField(f, raw);
    if (coerced === undefined) continue;
    out[k] = coerced;
  }

  return out;
}

function slugifyErrorType(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "unknown_error";
}

async function createAutomationErr({
  error_type,
  message,
  app_show_id = "",
}) {
  const errsTbl = base.getTable(ERRS_TABLE);
  const meta = getTableMeta(errsTbl);

  const automation_key = `${AUTOMATION_NAME}|${app_show_id || ""}|${run_id}|${slugifyErrorType(error_type)}`;

  const payload = {
    automation_key,
    automation_name: AUTOMATION_NAME,
    error_type,
    app_sql_date: "",
    app_show_id,
    run_id,
    last_run,
    message: message || "",
  };

  const fields = buildWritableFields(payload, meta);

  if (Object.keys(fields).length === 0) {
    console.log(`automation_errs: no writable matching fields found for error "${error_type}"`);
    return "";
  }

  const recId = await errsTbl.createRecordAsync(fields);

  outSet("error_type", error_type);
  outSet("automation_key", automation_key);
  outSet("automation_err_record_id", recId);

  return recId;
}

async function stopWithAutomationErr({ error_type, message, app_show_id = "" }) {
  await createAutomationErr({ error_type, message, app_show_id });
  outSet("upsert_action", "errored");
  throw new Error(message || error_type);
}

function selectPayloadRow(payload, search_by) {
  if (search_by === "search_by_entry_id") {
    if (!isObj(payload) || !isObj(payload.entry)) return { row: null, app_show_id: "" };
    return {
      row: payload.entry,
      app_show_id: parseNum(payload.entry.show_id) ?? ""
    };
  }

  if (search_by === "search_by_horse" || search_by === "search_by_entry_no") {
    if (!isObj(payload) || !Array.isArray(payload.entries) || !isObj(payload.entries[0])) {
      return { row: null, app_show_id: "" };
    }
    return {
      row: payload.entries[0],
      app_show_id: parseNum(payload.entries[0].show_id) ?? parseNum(payload.show_id) ?? ""
    };
  }

  return { row: null, app_show_id: "" };
}

// ----------------------------
// EARLY SENTINEL CHECKS
// ----------------------------
const sentinelMap = {
  "missing entry id": "missing entry id",
  "missing entry_number": "missing entry_number",
  "missing horse name": "missing horse name",
};

const trimmedUri = String(search_uri).trim();
if (Object.prototype.hasOwnProperty.call(sentinelMap, trimmedUri)) {
  await createAutomationErr({
    error_type: sentinelMap[trimmedUri],
    message: `Input search_uri was sentinel value: "${trimmedUri}"`,
    app_show_id: ""
  });
  outSet("upsert_action", "skipped");
  return;
}

// ----------------------------
// VALIDATE search_by
// ----------------------------
const allowedSearchBy = new Set([
  "search_by_entry_id",
  "search_by_horse",
  "search_by_entry_no",
]);

if (!allowedSearchBy.has(search_by)) {
  await stopWithAutomationErr({
    error_type: "invalid search_by",
    message: `Invalid search_by: ${search_by}`,
    app_show_id: ""
  });
}

// ----------------------------
// FETCH
// ----------------------------
let payload;
let res;

try {
  console.log(`Fetching search_uri: ${search_uri}`);
  res = await fetch(search_uri);
} catch (err) {
  await stopWithAutomationErr({
    error_type: "fetch failed",
    message: `Fetch threw error for search_uri: ${String(err?.message || err)}`,
    app_show_id: ""
  });
}

if (!res.ok) {
  const txt = await res.text();
  await stopWithAutomationErr({
    error_type: "fetch failed",
    message: `Fetch failed (${res.status}): ${txt.slice(0, 1000)}`,
    app_show_id: ""
  });
}

try {
  payload = await res.json();
} catch (err) {
  await stopWithAutomationErr({
    error_type: "invalid json",
    message: `Response was not valid JSON: ${String(err?.message || err)}`,
    app_show_id: ""
  });
}

// ----------------------------
// SELECT PAYLOAD ROW
// ----------------------------
const { row, app_show_id } = selectPayloadRow(payload, search_by);

if (!row) {
  const error_type =
    search_by === "search_by_entry_id"
      ? "missing payload.entry"
      : "missing payload.entries[0]";

  await stopWithAutomationErr({
    error_type,
    message: `Payload did not contain expected row for ${search_by}`,
    app_show_id
  });
}

// ----------------------------
// NORMALIZE SELECTED RECORD
// ----------------------------
const horseKey = normalizeKey(row.horse);
if (!horseKey) {
  await stopWithAutomationErr({
    error_type: "missing horse key",
    message: `Selected payload row missing usable horse value; cannot upsert ${HORSES_TABLE}`,
    app_show_id
  });
}

const flat = {
  horse: horseKey,
  entry_id: parseNum(row.entry_id),
  entry_number: parseNum(row.number),
  horse_id: parseNum(row.horse_id),
  trainer_id: parseNum(row.trainer_id),
  rider_id: parseNum(row.rider_id),
  rider_list: row.rider_list ?? "",
  app_show_id: parseNum(row.show_id),
  run_id,
  last_run,
};

outSetJson("selected_row_json", row);
outSetJson("flattened_horse_json", flat);

// ----------------------------
// LOAD TABLE + META
// ----------------------------
const horsesTbl = base.getTable(HORSES_TABLE);
const horsesMeta = getTableMeta(horsesTbl);

if (!horsesMeta.fieldsByName.has(KEY_FIELD)) {
  throw new Error(`Missing key field "${KEY_FIELD}" in table "${HORSES_TABLE}"`);
}

const keyFieldObj = horsesMeta.fieldsByName.get(KEY_FIELD);
const readFields = Array.from(new Set([keyFieldObj, ...horsesMeta.writableFields]));
const existing = await horsesTbl.selectRecordsAsync({ fields: readFields });

let existingRec = null;
for (const rec of existing.records) {
  const cell = rec.getCellValue(keyFieldObj);
  const k = normalizeKey(isObj(cell) && cell.name ? cell.name : cell);
  if (k === horseKey) {
    existingRec = rec;
    break;
  }
}

// ----------------------------
// UPSERT
// ----------------------------
const baseWriteFields = buildWritableFields(flat, horsesMeta);

if (!baseWriteFields || Object.keys(baseWriteFields).length === 0) {
  await stopWithAutomationErr({
    error_type: "no writable fields",
    message: `No writable fields matched in "${HORSES_TABLE}"`,
    app_show_id: flat.app_show_id ?? ""
  });
}

let wh_record_id = "";
let upsert_action = "noop";

if (!existingRec) {
  const createFields = { ...baseWriteFields };

  if (horsesMeta.writableNames.has("new_horse")) {
    const nf = horsesMeta.writableByName.get("new_horse");
    const coerced = coerceForField(nf, true);
    if (coerced !== undefined) createFields.new_horse = coerced;
  }

  const ids = await horsesTbl.createRecordsAsync([{ fields: createFields }]);
  wh_record_id = ids[0] || "";
  upsert_action = "created";
} else {
  const updateFields = { ...baseWriteFields };
  delete updateFields.new_horse; // never touch on update

  await horsesTbl.updateRecordsAsync([{ id: existingRec.id, fields: updateFields }]);
  wh_record_id = existingRec.id;
  upsert_action = "updated";
}

// ----------------------------
// OUTPUTS
// ----------------------------
outSet("wh_record_id", wh_record_id);
outSet("upsert_action", upsert_action);

outSet("horse", flat.horse ?? "");
outSet("entry_id", flat.entry_id ?? "");
outSet("entry_number", flat.entry_number ?? "");
outSet("horse_id", flat.horse_id ?? "");
outSet("trainer_id", flat.trainer_id ?? "");
outSet("rider_id", flat.rider_id ?? "");
outSet("rider_list", flat.rider_list ?? "");
outSet("app_show_id", flat.app_show_id ?? "");
outSet("run_id", flat.run_id ?? "");
outSet("last_run", flat.last_run ?? "");

outSetJson("written_fields_json", baseWriteFields);
