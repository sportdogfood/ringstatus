/**
 * Airtable Automation Script
 * Evaluate parent / sibling / target rows in watch_trips / TODAY
 *
 * Logic:
 * - Read records from table: watch_trips
 * - Read only records in view: TODAY
 * - Build sibling sets by:
 *     class_group_id + class_type + entry_number + ring_number
 *
 * Forced-solo rows are identified by either:
 * - checkbox flags: is_classic / is_usf / is_handy / is_warmup
 * - class_name fallback text detection
 *
 * Rules:
 * - Forced solo row:
 *   - is_parent = false
 *   - is_sibling = false
 *   - parent_class_number = that row's numeric class_number when available, else null
 *   - is_target = true
 *   - entry_sequence:
 *       - 3 when classic
 *       - 0 when usf
 *       - 4 when handy
 *       - 5 when warmup
 *   - targets_last_at = NOW
 *
 * - For the remaining non-forced rows inside the same sibling set:
 *   - Solo set = 1 distinct numeric class_number
 *     - is_parent = false
 *     - is_sibling = false
 *     - parent_class_number = that class_number
 *     - is_target = true
 *     - entry_sequence = 1
 *     - targets_last_at = NOW
 *
 *   - Multi-class sibling set = 2+ distinct numeric class_number values
 *     - is_sibling = true on all non-forced rows in the set
 *     - parent_class_number = lowest numeric class_number among NON-forced rows
 *     - is_parent = true only on the non-forced row(s) whose class_number equals that lowest value
 *     - is_target = true only on those parent row(s)
 *     - entry_sequence = 1 on parent row(s)
 *     - entry_sequence = 2 on non-parent sibling row(s)
 *     - targets_last_at = NOW only on parent row(s)
 *
 * - Rows with incomplete grouping fields or non-numeric class_number are reset:
 *   - is_parent = false
 *   - is_sibling = false
 *   - is_target = false
 *   - parent_class_number = null
 *   - entry_sequence = null
 *   - targets_last_at is left unchanged
 *
 * - Exception:
 *   - If a row is forced solo, it is still marked solo
 *     even if it does not participate in a valid grouped set
 */

const TABLE_NAME = "watch_trips";
const VIEW_NAME = "TODAY";

const FIELD_CLASS_GROUP_ID = "class_group_id";
const FIELD_CLASS_TYPE = "class_type";
const FIELD_ENTRY_NUMBER = "entry_number";
const FIELD_RING_NUMBER = "ring_number";
const FIELD_CLASS_NUMBER = "class_number";
const FIELD_CLASS_NAME = "class_name";

const FIELD_IS_PARENT = "is_parent";
const FIELD_PARENT_CLASS_NUMBER = "parent_class_number";
const FIELD_IS_TARGET = "is_target";
const FIELD_IS_SIBLING = "is_sibling";
const FIELD_TARGETS_LAST_AT = "targets_last_at";
const FIELD_ENTRY_SEQUENCE = "entry_sequence";

const FIELD_IS_CLASSIC = "is_classic";
const FIELD_IS_USF = "is_usf";
const FIELD_IS_HANDY = "is_handy";
const FIELD_IS_WARMUP = "is_warmup";

const NOW_ISO = new Date().toISOString();

function chunk50(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 50) out.push(arr.slice(i, i + 50));
  return out;
}

function normalizeKeyStr(v) {
  if (v === undefined || v === null) return "";
  return String(v).trim();
}

function normalizeLower(v) {
  return normalizeKeyStr(v).toLowerCase();
}

function normalizeClassType(v) {
  return normalizeLower(v);
}

function normalizeClassNumber(v) {
  if (v === undefined || v === null) return undefined;
  if (typeof v === "number" && Number.isFinite(v)) return v;

  const s = String(v).trim();
  if (!s) return undefined;
  if (/^\d+$/.test(s)) return Number(s);

  return undefined;
}

function isChecked(v) {
  return v === true || v === 1 || v === "1" || String(v || "").toLowerCase() === "checked";
}

function hasWordBoundary(text, phrase) {
  return new RegExp(`\\b${phrase}\\b`, "i").test(text);
}

function detectForcedSoloType(rec) {
  const isClassic = isChecked(rec.getCellValue(FIELD_IS_CLASSIC));
  const isUsf = isChecked(rec.getCellValue(FIELD_IS_USF));
  const isHandy = isChecked(rec.getCellValue(FIELD_IS_HANDY));
  const isWarmup = isChecked(rec.getCellValue(FIELD_IS_WARMUP));

  if (isClassic) return "classic";
  if (isUsf) return "usf";
  if (isHandy) return "handy";
  if (isWarmup) return "warmup";

  const className = normalizeLower(rec.getCellValue(FIELD_CLASS_NAME));
  if (!className) return null;

  // Fallback text detection when the checkbox flags were not populated before this run.
  if (hasWordBoundary(className, "warm up") || hasWordBoundary(className, "warmup")) return "warmup";
  if (hasWordBoundary(className, "handy")) return "handy";
  if (hasWordBoundary(className, "ushja")) return "usf";
  if (hasWordBoundary(className, "classic")) return "classic";

  return null;
}

function getForcedSoloSequence(rec) {
  const type = detectForcedSoloType(rec);
  if (type === "classic") return 3;
  if (type === "usf") return 0;
  if (type === "handy") return 4;
  if (type === "warmup") return 5;
  return null;
}

function buildSiblingKey(rec) {
  const classGroupId = normalizeKeyStr(rec.getCellValue(FIELD_CLASS_GROUP_ID));
  const classType = normalizeClassType(rec.getCellValue(FIELD_CLASS_TYPE));
  const entryNumber = normalizeKeyStr(rec.getCellValue(FIELD_ENTRY_NUMBER));
  const ringNumber = normalizeKeyStr(rec.getCellValue(FIELD_RING_NUMBER));

  if (!classGroupId || !classType || !entryNumber || !ringNumber) return "";
  return `${classGroupId}|${classType}|${entryNumber}|${ringNumber}`;
}

const table = base.getTable(TABLE_NAME);
const view = table.getView(VIEW_NAME);

const requiredFields = [
  FIELD_CLASS_GROUP_ID,
  FIELD_CLASS_TYPE,
  FIELD_ENTRY_NUMBER,
  FIELD_RING_NUMBER,
  FIELD_CLASS_NUMBER,
  FIELD_CLASS_NAME,
  FIELD_IS_PARENT,
  FIELD_PARENT_CLASS_NUMBER,
  FIELD_IS_TARGET,
  FIELD_IS_SIBLING,
  FIELD_TARGETS_LAST_AT,
  FIELD_ENTRY_SEQUENCE,
  FIELD_IS_CLASSIC,
  FIELD_IS_USF,
  FIELD_IS_HANDY,
  FIELD_IS_WARMUP,
];

for (const fieldName of requiredFields) {
  if (!table.fields.some(f => f.name === fieldName)) {
    throw new Error(`Missing required field in ${TABLE_NAME}: ${fieldName}`);
  }
}

const query = await view.selectRecordsAsync({
  fields: requiredFields,
});

const records = query.records;

// Default reset for every record in TODAY.
// Note: targets_last_at is intentionally NOT cleared here.
const updatesById = new Map();
for (const rec of records) {
  updatesById.set(rec.id, {
    id: rec.id,
    fields: {
      [FIELD_IS_PARENT]: false,
      [FIELD_IS_SIBLING]: false,
      [FIELD_IS_TARGET]: false,
      [FIELD_PARENT_CLASS_NUMBER]: null,
      [FIELD_ENTRY_SEQUENCE]: null,
    },
  });
}

// Build FULL sibling groups first.
const groups = new Map();
const groupedRecordIds = new Set();

for (const rec of records) {
  const siblingKey = buildSiblingKey(rec);
  const classNumber = normalizeClassNumber(rec.getCellValue(FIELD_CLASS_NUMBER));
  const forcedSoloSequence = getForcedSoloSequence(rec);
  const isForcedSolo = forcedSoloSequence !== null;

  if (!siblingKey) continue;
  if (!Number.isFinite(classNumber)) continue;

  if (!groups.has(siblingKey)) groups.set(siblingKey, []);
  groups.get(siblingKey).push({
    id: rec.id,
    classNumber,
    isForcedSolo,
    forcedSoloSequence,
  });

  groupedRecordIds.add(rec.id);
}

// Counters
let siblingSetCount = 0;
let soloSetCount = 0;
let multiClassSetCount = 0;

let forcedSoloCount = 0;
let parentCount = 0;
let siblingChildCount = 0;
let targetCount = 0;
let soloTargetCount = 0;
let invalidOrResetCount = 0;

// Apply logic group-by-group
for (const rows of groups.values()) {
  if (!Array.isArray(rows) || rows.length === 0) continue;

  siblingSetCount += 1;

  const forcedRows = rows.filter(r => r.isForcedSolo);
  const normalRows = rows.filter(r => !r.isForcedSolo);

  // 1) Forced-solo rows inside the family
  for (const row of forcedRows) {
    const existing = updatesById.get(row.id) || { id: row.id, fields: {} };
    existing.fields[FIELD_IS_PARENT] = false;
    existing.fields[FIELD_IS_SIBLING] = false;
    existing.fields[FIELD_PARENT_CLASS_NUMBER] = Number.isFinite(row.classNumber) ? row.classNumber : null;
    existing.fields[FIELD_IS_TARGET] = true;
    existing.fields[FIELD_ENTRY_SEQUENCE] = row.forcedSoloSequence;
    existing.fields[FIELD_TARGETS_LAST_AT] = NOW_ISO;
    updatesById.set(row.id, existing);

    forcedSoloCount += 1;
    targetCount += 1;
  }

  // 2) Remaining non-forced rows in the same family
  if (normalRows.length === 0) {
    continue;
  }

  const distinctClassNumbers = [...new Set(normalRows.map(r => r.classNumber))].sort((a, b) => a - b);

  // Solo set among remaining non-forced rows
  if (distinctClassNumbers.length === 1) {
    soloSetCount += 1;

    const onlyClassNumber = distinctClassNumbers[0];

    for (const row of normalRows) {
      const existing = updatesById.get(row.id) || { id: row.id, fields: {} };
      existing.fields[FIELD_IS_PARENT] = false;
      existing.fields[FIELD_IS_SIBLING] = false;
      existing.fields[FIELD_PARENT_CLASS_NUMBER] = onlyClassNumber;
      existing.fields[FIELD_IS_TARGET] = true;
      existing.fields[FIELD_ENTRY_SEQUENCE] = 1;
      existing.fields[FIELD_TARGETS_LAST_AT] = NOW_ISO;
      updatesById.set(row.id, existing);

      targetCount += 1;
      soloTargetCount += 1;
    }

    continue;
  }

  // Multi-class sibling set among remaining non-forced rows
  multiClassSetCount += 1;

  const lowestClassNumber = distinctClassNumbers[0];

  for (const row of normalRows) {
    const isParent = row.classNumber === lowestClassNumber;

    const existing = updatesById.get(row.id) || { id: row.id, fields: {} };
    existing.fields[FIELD_IS_SIBLING] = true;
    existing.fields[FIELD_PARENT_CLASS_NUMBER] = lowestClassNumber;
    existing.fields[FIELD_IS_PARENT] = isParent;
    existing.fields[FIELD_IS_TARGET] = isParent;
    existing.fields[FIELD_ENTRY_SEQUENCE] = isParent ? 1 : 2;

    if (isParent) {
      existing.fields[FIELD_TARGETS_LAST_AT] = NOW_ISO;
      parentCount += 1;
      targetCount += 1;
    } else {
      siblingChildCount += 1;
    }

    updatesById.set(row.id, existing);
  }
}

// Apply forced-solo rows that were NOT part of any valid grouped set
for (const rec of records) {
  const forcedSoloSequence = getForcedSoloSequence(rec);
  const isForcedSolo = forcedSoloSequence !== null;

  if (!isForcedSolo) continue;
  if (groupedRecordIds.has(rec.id)) continue;

  const classNumber = normalizeClassNumber(rec.getCellValue(FIELD_CLASS_NUMBER));
  const existing = updatesById.get(rec.id) || { id: rec.id, fields: {} };

  existing.fields[FIELD_IS_PARENT] = false;
  existing.fields[FIELD_IS_SIBLING] = false;
  existing.fields[FIELD_PARENT_CLASS_NUMBER] = Number.isFinite(classNumber) ? classNumber : null;
  existing.fields[FIELD_IS_TARGET] = true;
  existing.fields[FIELD_ENTRY_SEQUENCE] = forcedSoloSequence;
  existing.fields[FIELD_TARGETS_LAST_AT] = NOW_ISO;

  updatesById.set(rec.id, existing);

  forcedSoloCount += 1;
  targetCount += 1;
}

// Count records that remained reset / ungrouped
for (const update of updatesById.values()) {
  const f = update.fields || {};
  const isParent = !!f[FIELD_IS_PARENT];
  const isSibling = !!f[FIELD_IS_SIBLING];
  const isTarget = !!f[FIELD_IS_TARGET];
  const entrySequence = f[FIELD_ENTRY_SEQUENCE];

  if (!isParent && !isSibling && !isTarget && (entrySequence === null || entrySequence === undefined)) {
    invalidOrResetCount += 1;
  }
}

// Write updates
const updates = [...updatesById.values()];

for (const batch of chunk50(updates)) {
  await table.updateRecordsAsync(batch);
}

output.set("table", TABLE_NAME);
output.set("view", VIEW_NAME);
output.set("evaluated_at", NOW_ISO);
output.set("records_in_view", records.length);
output.set("updated_records", updates.length);

output.set("forced_solo_rows_marked", forcedSoloCount);
output.set("sibling_sets_found", siblingSetCount);
output.set("solo_sets_found", soloSetCount);
output.set("multi_class_sets_found", multiClassSetCount);

output.set("parent_rows_marked", parentCount);
output.set("solo_target_rows_marked", soloTargetCount);
output.set("target_rows_marked_total", targetCount);
output.set("sibling_child_rows_marked", siblingChildCount);
output.set("reset_or_ungrouped_rows", invalidOrResetCount);
