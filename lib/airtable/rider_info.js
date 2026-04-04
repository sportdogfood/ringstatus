/**
 * Airtable Automation Script: ww_riders UPSERT from people_uri
 *
 * Input:
 *   people_uri (string) e.g. https://.../people/79378?pid=79378&customer_id=15
 *
 * Target table: ww_riders
 * Key: rider_id
 *
 * Writes (if fields exist and are writable):
 *   rider_id (Number)
 *   usef (Single line text)
 *   lf_name (Single line text)
 *   name (Single line text)
 *   city (Single line text)
 *   state (Single line text)
 *   fei_id (Single line text)
 *
 *   horse_ids_list (Long text)
 *   horses_list (Long text)
 *   riders_ids_list (Long text)
 *   rider_names_list (Long text)
 *
 *   empty_trips_show_id (Number)
 *   empty_trips_at (Date/time)
 *   trips_lists_show_id (Number)
 *   trips_lists_at (Date/time)
 *
 * Behavior:
 *   - rider_id is the only key used for lookup/create in ww_riders
 *   - endpoint payload uses payload.people.people_id
 *   - payload.people.people_id is mapped into rider_id
 *   - people_id is not written at all
 *   - If trips has rows, rebuild and write the 4 list fields
 *   - If trips is empty, DO NOT blank existing list fields
 *   - If trips is empty, write:
 *       empty_trips_show_id = payload.show_id
 *       empty_trips_at = now
 *   - If trips has rows, write:
 *       trips_lists_show_id = payload.show_id
 *       trips_lists_at = now
 *
 * Required payload:
 *   - payload.people must exist
 *   - payload.people.people_id must exist
 */

const TABLE_NAME = "ww_riders";
const KEY_FIELD = "rider_id";

// ----------------------------
// INPUTS
// ----------------------------
const { people_uri, run_tag } = input.config();

if (!people_uri || typeof people_uri !== "string") {
  throw new Error("Missing required input variable: people_uri");
}

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

outSet("people_uri", people_uri);
outSet("run_tag", run_tag ?? "");

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

function cleanText(v) {
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

function resolveKeyReadWrite(meta, keyName, tableName) {
  if (!meta.fieldsByName.has(keyName)) {
    throw new Error(`Missing key field "${keyName}" in table "${tableName}"`);
  }

  const keyReadName = keyName;

  if (meta.writableNames.has(keyName)) {
    return { keyReadName, keyWriteName: keyName };
  }

  const preferred = [
    `${keyName}_value`,
    `${keyName}_raw`,
    `${keyName}_key`,
    `key_${keyName}`,
    "key",
    "uuid",
  ];

  for (const nm of preferred) {
    if (meta.writableNames.has(nm)) {
      return { keyReadName, keyWriteName: nm };
    }
  }

  const rx = /rider[_\s-]*id.*(value|raw|key|uuid)?/i;
  const candidate = meta.writableFields.find((f) => rx.test(String(f.name)));
  if (candidate) {
    return { keyReadName, keyWriteName: candidate.name };
  }

  throw new Error(
    `Key field "${keyName}" in table "${tableName}" is computed/read-only. Add a writable key field (e.g. "${keyName}_value") and rerun.`
  );
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
      return v instanceof Date ? v.toISOString() : String(v);

    default:
      return v;
  }
}

function buildWritableFields(flat, meta, keyWriteName) {
  const out = {};

  if (keyWriteName && meta.writableNames.has(keyWriteName) && flat[KEY_FIELD] !== undefined) {
    const f = meta.writableByName.get(keyWriteName);
    const coercedKey = coerceForField(f, flat[KEY_FIELD]);
    if (coercedKey !== undefined) out[keyWriteName] = coercedKey;
  }

  for (const [k, raw] of Object.entries(flat)) {
    if (k === KEY_FIELD) continue;
    if (!meta.writableNames.has(k)) continue;
    const f = meta.writableByName.get(k);
    const coerced = coerceForField(f, raw);
    if (coerced === undefined) continue;
    out[k] = coerced;
  }

  if (run_tag && meta.writableNames.has("run_tag")) out.run_tag = String(run_tag);

  return out;
}

function pushUnique(set, arr, value) {
  const s = cleanText(value);
  if (!s || set.has(s)) return;
  set.add(s);
  arr.push(s);
}

function valuesEqual(a, b) {
  if (a === b) return true;
  if ((a ?? null) === null && (b ?? null) === null) return true;
  return String(a ?? "") === String(b ?? "");
}

function getComparableCellValue(rec, field) {
  if (!field) return undefined;
  const raw = rec.getCellValue(field);

  if (raw === null || raw === undefined) return undefined;

  switch (field.type) {
    case "singleLineText":
    case "multilineText":
    case "richText":
    case "url":
    case "email":
    case "phoneNumber":
    case "number":
    case "currency":
    case "percent":
    case "rating":
    case "duration":
    case "checkbox":
      return raw;

    case "date":
    case "dateTime":
      return rec.getCellValueAsString(field);

    default:
      return rec.getCellValueAsString(field);
  }
}

function makeNowIsoNoMs() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

// ----------------------------
// FETCH PAYLOAD
// ----------------------------
console.log(`Fetching people_uri: ${people_uri}`);
const res = await fetch(people_uri);
if (!res.ok) {
  const txt = await res.text();
  throw new Error(`Fetch failed (${res.status}): ${txt.slice(0, 1000)}`);
}
const payload = await res.json();

if (!isObj(payload) || !isObj(payload.people)) {
  throw new Error("Payload missing required object: payload.people");
}

const p = payload.people;
const riderId = parseNum(p.people_id);

if (riderId === undefined) {
  throw new Error("Payload missing required key: payload.people.people_id");
}

const trips = Array.isArray(payload.trips) ? payload.trips : [];
const showIdNum = parseNum(payload.show_id);
const hasTrips = trips.length > 0;
const nowIso = makeNowIsoNoMs();

// ----------------------------
// DEDUPE LISTS FROM trips
// ----------------------------
const horseIdSeen = new Set();
const horseSeen = new Set();
const riderIdSeen = new Set();
const riderNameSeen = new Set();

const horseIdsArr = [];
const horsesArr = [];
const riderIdsArr = [];
const riderNamesArr = [];

for (const t of trips) {
  if (!isObj(t)) continue;

  pushUnique(horseIdSeen, horseIdsArr, t.horse_id);
  pushUnique(horseSeen, horsesArr, t.horse);
  pushUnique(riderIdSeen, riderIdsArr, t.rider_id);
  pushUnique(riderNameSeen, riderNamesArr, t.rider_name);
}

const horse_ids_list = horseIdsArr.join(", ");
const horses_list = horsesArr.join(", ");
const riders_ids_list = riderIdsArr.join(", ");
const rider_names_list = riderNamesArr.join(", ");

// ----------------------------
// FLATTEN -> TARGET FIELDS
// ----------------------------
const flat = {
  rider_id: riderId,
  usef: p.usef,
  lf_name: p.lf_name,
  name: p.name,
  city: p.city,
  state: p.state,
  fei_id: p.fei_id ?? "",
};

if (hasTrips) {
  flat.horse_ids_list = horse_ids_list;
  flat.horses_list = horses_list;
  flat.riders_ids_list = riders_ids_list;
  flat.rider_names_list = rider_names_list;

  if (showIdNum !== undefined) flat.trips_lists_show_id = showIdNum;
  flat.trips_lists_at = nowIso;
} else {
  if (showIdNum !== undefined) flat.empty_trips_show_id = showIdNum;
  flat.empty_trips_at = nowIso;
}

const keyVal = normalizeKey(flat.rider_id);
if (!keyVal) throw new Error("Missing required key value: rider_id");

// ----------------------------
// LOAD TABLE + META
// ----------------------------
const tbl = base.getTable(TABLE_NAME);
const meta = getTableMeta(tbl);
const keyMeta = resolveKeyReadWrite(meta, KEY_FIELD, TABLE_NAME);

const keyReadFieldObj = meta.fieldsByName.get(keyMeta.keyReadName);
const fieldsToRead = Array.from(new Set([keyReadFieldObj, ...meta.writableFields]));
const existing = await tbl.selectRecordsAsync({ fields: fieldsToRead });

let existingRec = null;
for (const rec of existing.records) {
  const cell = rec.getCellValue(keyReadFieldObj);
  const k = normalizeKey(isObj(cell) && cell.name ? cell.name : cell);
  if (k === keyVal) {
    existingRec = rec;
    break;
  }
}

// ----------------------------
// UPSERT
// ----------------------------
const writeFields = buildWritableFields(flat, meta, keyMeta.keyWriteName);
if (!writeFields || Object.keys(writeFields).length === 0) {
  throw new Error(`No writable fields matched in "${TABLE_NAME}". Ensure the target fields exist and are not computed.`);
}

let wr_record_id = "";
let upsert_action = "noop";

if (!existingRec) {
  const ids = await tbl.createRecordsAsync([{ fields: writeFields }]);
  wr_record_id = ids[0] || "";
  upsert_action = "created";
} else {
  const updateFields = { ...writeFields };
  delete updateFields[KEY_FIELD];
  if (keyMeta.keyWriteName) delete updateFields[keyMeta.keyWriteName];

  const changedFields = {};
  for (const [fieldName, newValue] of Object.entries(updateFields)) {
    if (!meta.fieldsByName.has(fieldName)) continue;
    const fieldObj = meta.fieldsByName.get(fieldName);
    const oldValue = getComparableCellValue(existingRec, fieldObj);

    if (!valuesEqual(oldValue, newValue)) {
      changedFields[fieldName] = newValue;
    }
  }

  if (Object.keys(changedFields).length > 0) {
    await tbl.updateRecordsAsync([{ id: existingRec.id, fields: changedFields }]);
    upsert_action = "updated";
  } else {
    upsert_action = "noop";
  }

  wr_record_id = existingRec.id;
}

// ----------------------------
// OUTPUTS FOR DOWNSTREAM STEPS
// ----------------------------
outSet("upsert_action", upsert_action);
outSet("wr_record_id", wr_record_id);

outSet("rider_id", flat.rider_id ?? "");
outSet("usef", flat.usef ?? "");
outSet("lf_name", flat.lf_name ?? "");
outSet("name", flat.name ?? "");
outSet("city", flat.city ?? "");
outSet("state", flat.state ?? "");
outSet("fei_id", flat.fei_id ?? "");

outSet("horse_ids_list", flat.horse_ids_list ?? "");
outSet("horses_list", flat.horses_list ?? "");
outSet("riders_ids_list", flat.riders_ids_list ?? "");
outSet("rider_names_list", flat.rider_names_list ?? "");

outSet("empty_trips_show_id", flat.empty_trips_show_id ?? "");
outSet("empty_trips_at", flat.empty_trips_at ?? "");
outSet("trips_lists_show_id", flat.trips_lists_show_id ?? "");
outSet("trips_lists_at", flat.trips_lists_at ?? "");

outSet("key_read_field", keyMeta.keyReadName);
outSet("key_write_field", keyMeta.keyWriteName);
outSetJson("flattened_people_json", flat);
outSetJson("written_fields_json", writeFields);
outSetJson("horse_ids_array_json", horseIdsArr);
outSetJson("horses_array_json", horsesArr);
outSetJson("rider_ids_array_json", riderIdsArr);
outSetJson("rider_names_array_json", riderNamesArr);
