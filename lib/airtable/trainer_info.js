/**
 * Airtable Automation Script: trainers ingest from /people/{id} endpoint (UPSERT by trainer_id)
 *
 * INPUT variables (Automation -> Run script):
 *   - uri (string, REQUIRED)  e.g. https://broad-tooth-b8ed.gombcg.workers.dev/people/9590?pid=9590&customer_id=15
 *   - table_name (string, optional) default "trainers"
 *   - key_field  (string, optional) default "trainer_id"
 *
 * TARGET TABLE:
 *   - ww_trainers
 *   - key = trainer_id (Number)  <=> people.people_id (Number)
 *
 * SOURCE SHAPE:
 *   payload.people { ... }
 *   payload.trips  [ ... ]
 *
 * WRITES (only if field exists + is writable):
 *   - trainer_id  (Number) = people.people_id
 *   - people_id   (Number) = people.people_id
 *   - Every key from payload.people mapped by SAME field name (usef, lf_name, name, city, state, country, fei_id, etc.)
 *   - horses_list (Text)  = dedup CSV of trips[].horse_id
 *   - riders_list (Text)  = dedup CSV of trips[].rider_id
 *   - total_trips (Number/Text) = payload.total_trips
 *
 * OUTPUTS (for downstream steps):
 *   trainer_record_id, trainer_id, people_id, horses_list, riders_list,
 *   horses_count, riders_count, total_trips, wrote_fields, skipped_fields, warning
 */

// --------------------
// INPUT
// --------------------
let cfg = {};
if (typeof input !== "undefined" && typeof input.config === "function") cfg = input.config();

const URI = (cfg.uri || "").trim();
const TABLE_NAME = (cfg.table_name || "ww_trainers").trim();
const KEY_FIELD = (cfg.key_field || "trainer_id").trim();

if (!URI) throw new Error("Missing required input variable: uri");

// --------------------
// OUTPUT helper
// --------------------
function setOutputs(obj) {
  if (typeof output === "undefined" || !output || typeof output.set !== "function") return;
  for (const [k, v] of Object.entries(obj)) output.set(k, v);
}

// --------------------
// Airtable helpers
// --------------------
function isWritableField(field) {
  // computed fields are not writable; everything else is.
  return field && !field.isComputed;
}

function coerceForField(field, value) {
  if (value === undefined) return undefined; // omit
  if (value === null) return null;

  const t = field.type;

  // numbers
  if (t === "number" || t === "currency" || t === "percent") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  // checkbox
  if (t === "checkbox") {
    if (value === true || value === 1 || value === "1") return true;
    return false;
  }

  // default: keep strings as-is, but normalize non-strings to string (except empty)
  if (typeof value === "string") return value;
  return String(value);
}

function buildWritableFieldMap(table) {
  const map = new Map();
  for (const f of table.fields) {
    if (isWritableField(f)) map.set(f.name, f);
  }
  return map;
}

// --------------------
// Fetch + parse
// --------------------
let payload;
{
  const res = await fetch(URI, { headers: { Accept: "application/json" } });
  const text = await res.text();
  if (!res.ok) throw new Error(`Fetch failed (${res.status}): ${text.slice(0, 400)}`);

  try {
    payload = JSON.parse(text);
  } catch (e) {
    throw new Error(`Response was not valid JSON. First 400 chars:\n${text.slice(0, 400)}`);
  }
}

const people = payload?.people;
if (!people || typeof people !== "object") throw new Error('Missing expected object: payload.people');

const peopleIdNum = Number(people.people_id);
if (!Number.isFinite(peopleIdNum)) throw new Error(`people.people_id is not numeric: ${people.people_id}`);

// Optional mismatch note (does NOT stop)
let warning = "";
try {
  const u = new URL(URI);
  const pid = u.searchParams.get("pid");
  if (pid && Number(pid) !== peopleIdNum) {
    warning = `pid(${pid}) != people_id(${peopleIdNum}); using people_id from payload as key.`;
    console.log(warning);
  }
} catch (_) {}

// --------------------
// Build horses_list (loop trips once) + riders_list (loop trips again)
// --------------------
const trips = Array.isArray(payload?.trips) ? payload.trips : [];
const horseSet = new Set();
for (const t of trips) {
  const hid = Number(t?.horse_id);
  if (Number.isFinite(hid) && hid > 0) horseSet.add(hid);
}

const riderSet = new Set();
for (const t of trips) {
  const rid = Number(t?.rider_id);
  if (Number.isFinite(rid) && rid > 0) riderSet.add(rid);
}

const horsesArr = Array.from(horseSet).sort((a, b) => a - b);
const ridersArr = Array.from(riderSet).sort((a, b) => a - b);

const horses_list = horsesArr.join(",");
const riders_list = ridersArr.join(",");

// --------------------
// Table + upsert
// --------------------
const table = base.getTable(TABLE_NAME);
const writableFields = buildWritableFieldMap(table);

if (!table.fields.find((f) => f.name === KEY_FIELD)) {
  throw new Error(`Key field "${KEY_FIELD}" not found in table "${TABLE_NAME}"`);
}

const query = await table.selectRecordsAsync({ fields: [KEY_FIELD] });

let existing = null;
for (const r of query.records) {
  const v = r.getCellValue(KEY_FIELD);
  if (Number(v) === peopleIdNum) {
    existing = r;
    break;
  }
}

// Build fieldsToWrite:
// - start with trainer_id + people_id
// - then map every key in people{} to same-named Airtable fields (if writable)
// - then horses_list / riders_list / total_trips (if fields exist)
const fieldsToWrite = {};
const wrote_fields = [];
const skipped_fields = [];

function setIfExists(fieldName, value) {
  const field = writableFields.get(fieldName);
  if (!field) {
    skipped_fields.push(fieldName);
    return;
  }
  const coerced = coerceForField(field, value);
  if (coerced === undefined) return; // omit
  fieldsToWrite[fieldName] = coerced;
  wrote_fields.push(fieldName);
}

// Ensure key + mirror id
setIfExists("trainer_id", peopleIdNum);
setIfExists("people_id", peopleIdNum);

// Map all people keys by same name
for (const [k, v] of Object.entries(people)) {
  setIfExists(k, v);
}

// Lists + total trips
setIfExists("horses_list", horses_list);
setIfExists("riders_list", riders_list);
setIfExists("total_trips", payload?.total_trips);

// Write one record
let recordId;
if (existing) {
  await table.updateRecordAsync(existing.id, fieldsToWrite);
  recordId = existing.id;
  console.log(`Updated trainer record: ${recordId}`);
} else {
  recordId = await table.createRecordAsync(fieldsToWrite);
  console.log(`Created trainer record: ${recordId}`);
}

// --------------------
// Outputs
// --------------------
setOutputs({
  trainer_record_id: recordId,
  trainer_id: peopleIdNum,
  people_id: peopleIdNum,
  horses_list,
  riders_list,
  horses_count: horsesArr.length,
  riders_count: ridersArr.length,
  total_trips: payload?.total_trips ?? null,
  wrote_fields: wrote_fields.join(","),
  skipped_fields: skipped_fields.join(","),
  warning: warning || "",
});
