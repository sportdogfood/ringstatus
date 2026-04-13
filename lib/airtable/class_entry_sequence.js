/**
 * Airtable Automation Script
 * Evaluate target rows in watch_schedule / TODAY
 *
 * Matches stripped watch_trips model:
 * - no is_parent logic
 * - no is_sibling logic
 * - no parent_class_number logic
 * - no class_sequence usage
 * - uses class_group_sequence as the secondary discriminator
 *
 * Writes only:
 * - is_target
 * - entry_sequence
 * - targets_last_at
 *
 * Special types
 * TARGETABLE:
 * - usf       => entry_sequence 5  => is_target true
 *
 * NON-TARGETABLE / SCRUBBED:
 * - classic         => entry_sequence 3  => is_target false
 * - handy           => entry_sequence 4  => is_target false
 * - warmup          => entry_sequence 96 => is_target false
 * - schooling_pony  => entry_sequence 97 => is_target false
 * - mulligan        => entry_sequence 98 => is_target false
 * - add_back        => entry_sequence 99 => is_target false
 *
 * Rules:
 * - Special-classified rows are stamped directly.
 * - Non-special rows are grouped by class_id.
 * - Solo class_id family => target row => entry_sequence 1
 * - Multi-row class_id family:
 *     - first choose rows by lowest class_group_sequence when available
 *     - tie-break on lowest class_number
 *     - final tie-break on record id
 *     - winning normal row => is_target true, entry_sequence 1
 *     - remaining normal rows => is_target false, entry_sequence 2
 *     - special rows remain classified but non-target unless usf
 *
 * - Rows without class_id or without numeric class_number are reset unless
 *   they are special-classified, in which case special logic still applies.
 */

const TABLE_NAME = "watch_schedule";
const VIEW_NAME = "TODAY";

const FIELD_CLASS_ID = "class_id";
const FIELD_CLASS_GROUP_SEQUENCE = "class_group_sequence";

const FIELD_CLASS_TYPE = "class_type";
const FIELD_CLASS_NUMBER = "class_number";
const FIELD_CLASS_NAME = "class_name";
const FIELD_SCHEDULE_SEQUENCE_TYPE = "schedule_sequencetype";

const FIELD_IS_TARGET = "is_target";
const FIELD_TARGETS_LAST_AT = "targets_last_at";
const FIELD_ENTRY_SEQUENCE = "entry_sequence";

const FIELD_IS_USF = "is_usf";
const FIELD_IS_CLASSIC = "is_classic";
const FIELD_IS_HANDY = "is_handy";
const FIELD_IS_WARMUP = "is_warmup";
const FIELD_IS_SCHOOLING_PONY = "is_schooling_pony";
const FIELD_IS_MULLIGAN = "is_mulligan";
const FIELD_IS_ADD_BACK = "is_add_back";

const NOW_ISO = new Date().toISOString();

const SPECIAL_TYPE_CONFIG = {
  warmup:         { sequence: 96, isTarget: false, isScrubbed: true  },
  schooling_pony: { sequence: 97, isTarget: false, isScrubbed: true  },
  mulligan:       { sequence: 98, isTarget: false, isScrubbed: true  },
  add_back:       { sequence: 99, isTarget: false, isScrubbed: true  },
  classic:        { sequence: 3,  isTarget: false, isScrubbed: false },
  handy:          { sequence: 4,  isTarget: false, isScrubbed: false },
  usf:            { sequence: 5,  isTarget: true,  isScrubbed: false },
};

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

function normalizeSequenceNumber(v) {
  if (v === undefined || v === null) return undefined;
  if (typeof v === "number" && Number.isFinite(v)) return v;

  const s = String(v).trim();
  if (!s) return undefined;
  if (/^-?\d+$/.test(s)) return Number(s);

  return undefined;
}

function isChecked(v) {
  return v === true || v === 1 || v === "1" || String(v || "").toLowerCase() === "checked";
}

function hasWordBoundary(text, phrase) {
  const escaped = String(phrase).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`\\b${escaped}\\b`, "i").test(text);
}

function hasUsfText(text) {
  if (!text) return false;
  return /\bu\/s\b/i.test(text) || /\bu\s*\/\s*s\b/i.test(text);
}

function isUnderSaddleFlat(v) {
  return normalizeLower(v) === "under saddle/flat";
}

function isHuntersClassType(v) {
  return normalizeClassType(v) === "hunters";
}

function detectSpecialType(rec) {
  if (isChecked(rec.getCellValue(FIELD_IS_WARMUP))) return "warmup";
  if (isChecked(rec.getCellValue(FIELD_IS_SCHOOLING_PONY))) return "schooling_pony";
  if (isChecked(rec.getCellValue(FIELD_IS_MULLIGAN))) return "mulligan";
  if (isChecked(rec.getCellValue(FIELD_IS_ADD_BACK))) return "add_back";

  if (isChecked(rec.getCellValue(FIELD_IS_CLASSIC))) return "classic";
  if (isChecked(rec.getCellValue(FIELD_IS_HANDY))) return "handy";
  if (isChecked(rec.getCellValue(FIELD_IS_USF))) return "usf";

  const className = normalizeLower(rec.getCellValue(FIELD_CLASS_NAME));
  const classType = rec.getCellValue(FIELD_CLASS_TYPE);
  const scheduleSequenceType = rec.getCellValue(FIELD_SCHEDULE_SEQUENCE_TYPE);

  if (className) {
    if (hasWordBoundary(className, "warmup") || hasWordBoundary(className, "warm up")) return "warmup";
    if (hasWordBoundary(className, "schooling pony")) return "schooling_pony";
    if (hasWordBoundary(className, "mulligan")) return "mulligan";
    if (hasWordBoundary(className, "add_back")) return "add_back";
    if (hasWordBoundary(className, "classic") && isHuntersClassType(classType)) return "classic";
    if (hasWordBoundary(className, "handy")) return "handy";
    if (hasUsfText(className)) return "usf";
  }

  if (isUnderSaddleFlat(scheduleSequenceType)) return "usf";

  return null;
}

function getSpecialMeta(rec) {
  const type = detectSpecialType(rec);
  if (!type) return null;

  const cfg = SPECIAL_TYPE_CONFIG[type];
  if (!cfg) return null;

  return {
    type,
    sequence: cfg.sequence,
    isTarget: cfg.isTarget,
    isScrubbed: cfg.isScrubbed,
  };
}

function getClassIdKey(rec) {
  return normalizeKeyStr(rec.getCellValue(FIELD_CLASS_ID));
}

function getClassGroupSequenceKey(rec) {
  return normalizeKeyStr(rec.getCellValue(FIELD_CLASS_GROUP_SEQUENCE));
}

function sortRowsForTargetChoice(a, b) {
  const aGroupSeq = Number.isFinite(a.classGroupSequence) ? a.classGroupSequence : Number.POSITIVE_INFINITY;
  const bGroupSeq = Number.isFinite(b.classGroupSequence) ? b.classGroupSequence : Number.POSITIVE_INFINITY;
  if (aGroupSeq !== bGroupSeq) return aGroupSeq - bGroupSeq;

  const aClassNum = Number.isFinite(a.classNumber) ? a.classNumber : Number.POSITIVE_INFINITY;
  const bClassNum = Number.isFinite(b.classNumber) ? b.classNumber : Number.POSITIVE_INFINITY;
  if (aClassNum !== bClassNum) return aClassNum - bClassNum;

  return String(a.id).localeCompare(String(b.id));
}

const table = base.getTable(TABLE_NAME);
const view = table.getView(VIEW_NAME);

const requiredFields = [
  FIELD_CLASS_ID,
  FIELD_CLASS_GROUP_SEQUENCE,
  FIELD_CLASS_TYPE,
  FIELD_CLASS_NUMBER,
  FIELD_CLASS_NAME,
  FIELD_SCHEDULE_SEQUENCE_TYPE,
  FIELD_IS_TARGET,
  FIELD_TARGETS_LAST_AT,
  FIELD_ENTRY_SEQUENCE,
  FIELD_IS_USF,
  FIELD_IS_CLASSIC,
  FIELD_IS_HANDY,
  FIELD_IS_WARMUP,
  FIELD_IS_SCHOOLING_PONY,
  FIELD_IS_MULLIGAN,
  FIELD_IS_ADD_BACK,
];

for (const fieldName of requiredFields) {
  if (!table.fields.some(f => f.name === fieldName)) {
    throw new Error(`Missing required field in ${TABLE_NAME}: ${fieldName}`);
  }
}

const query = await view.selectRecordsAsync({ fields: requiredFields });
const records = query.records;

const updatesById = new Map();
for (const rec of records) {
  updatesById.set(rec.id, {
    id: rec.id,
    fields: {
      [FIELD_IS_TARGET]: false,
      [FIELD_ENTRY_SEQUENCE]: null,
    },
  });
}

const rows = records.map(rec => {
  const classId = getClassIdKey(rec);
  const classGroupSequenceKey = getClassGroupSequenceKey(rec);
  const classGroupSequence = normalizeSequenceNumber(rec.getCellValue(FIELD_CLASS_GROUP_SEQUENCE));
  const classNumber = normalizeClassNumber(rec.getCellValue(FIELD_CLASS_NUMBER));
  const specialMeta = getSpecialMeta(rec);

  return {
    id: rec.id,
    rec,
    classId,
    classGroupSequenceKey,
    classGroupSequence,
    classNumber,
    specialMeta,
    isSpecial: specialMeta !== null,
  };
});

let specialRowsMarked = 0;
let specialTargetableCount = 0;
let specialBlockedCount = 0;

let scrubbedWarmupCount = 0;
let scrubbedSchoolingPonyCount = 0;
let scrubbedMulliganCount = 0;
let scrubbedAddBackCount = 0;

let specialUsfCount = 0;
let specialClassicCount = 0;
let specialHandyCount = 0;

let eligibleDuplicatePoolCount = 0;
let classIdFamiliesCount = 0;
let classIdSoloCount = 0;
let classIdDuplicateFamilyCount = 0;
let classIdDuplicateRowCount = 0;

let classIdGroupSequenceDuplicateFamilyCount = 0;
let classIdGroupSequenceDuplicateRowCount = 0;
let unresolvedAtClassIdGroupSequenceCount = 0;

let targetCount = 0;
let soloTargetCount = 0;
let nonTargetRowsInMultiSets = 0;
let invalidOrResetCount = 0;

for (const row of rows) {
  if (!row.isSpecial) continue;

  const existing = updatesById.get(row.id) || { id: row.id, fields: {} };

  existing.fields[FIELD_IS_TARGET] = row.specialMeta.isTarget;
  existing.fields[FIELD_ENTRY_SEQUENCE] = row.specialMeta.sequence;

  if (row.specialMeta.isTarget) {
    existing.fields[FIELD_TARGETS_LAST_AT] = NOW_ISO;
    targetCount += 1;
    specialTargetableCount += 1;
  } else {
    specialBlockedCount += 1;
  }

  updatesById.set(row.id, existing);
  specialRowsMarked += 1;

  if (row.specialMeta.type === "warmup") scrubbedWarmupCount += 1;
  if (row.specialMeta.type === "schooling_pony") scrubbedSchoolingPonyCount += 1;
  if (row.specialMeta.type === "mulligan") scrubbedMulliganCount += 1;
  if (row.specialMeta.type === "add_back") scrubbedAddBackCount += 1;
  if (row.specialMeta.type === "usf") specialUsfCount += 1;
  if (row.specialMeta.type === "classic") specialClassicCount += 1;
  if (row.specialMeta.type === "handy") specialHandyCount += 1;
}

const eligibleRows = rows.filter(row => !row.isSpecial && row.classId && Number.isFinite(row.classNumber));
eligibleDuplicatePoolCount = eligibleRows.length;

const byClassId = new Map();
for (const row of eligibleRows) {
  if (!byClassId.has(row.classId)) byClassId.set(row.classId, []);
  byClassId.get(row.classId).push(row);
}

classIdFamiliesCount = byClassId.size;

for (const familyRows of byClassId.values()) {
  if (!Array.isArray(familyRows) || familyRows.length === 0) continue;

  if (familyRows.length === 1) {
    classIdSoloCount += 1;

    const row = familyRows[0];
    const existing = updatesById.get(row.id) || { id: row.id, fields: {} };

    existing.fields[FIELD_IS_TARGET] = true;
    existing.fields[FIELD_ENTRY_SEQUENCE] = 1;
    existing.fields[FIELD_TARGETS_LAST_AT] = NOW_ISO;

    updatesById.set(row.id, existing);

    targetCount += 1;
    soloTargetCount += 1;
    continue;
  }

  classIdDuplicateFamilyCount += 1;
  classIdDuplicateRowCount += familyRows.length;

  const byClassIdGroupSequence = new Map();
  for (const row of familyRows) {
    const seqKey = row.classGroupSequenceKey || "__blank__";
    const key = `${row.classId}|${seqKey}`;
    if (!byClassIdGroupSequence.has(key)) byClassIdGroupSequence.set(key, []);
    byClassIdGroupSequence.get(key).push(row);
  }

  let foundGroupSequenceDupes = false;

  for (const seqRows of byClassIdGroupSequence.values()) {
    if (seqRows.length > 1) {
      foundGroupSequenceDupes = true;
      classIdGroupSequenceDuplicateRowCount += seqRows.length;

      const hasAtLeastOneNumericSequence = seqRows.some(r => Number.isFinite(r.classGroupSequence));
      if (!hasAtLeastOneNumericSequence || seqRows.every(r => r.classGroupSequenceKey === seqRows[0].classGroupSequenceKey)) {
        unresolvedAtClassIdGroupSequenceCount += seqRows.length;
      }
    }
  }

  if (foundGroupSequenceDupes) {
    classIdGroupSequenceDuplicateFamilyCount += 1;
  }

  const sortedFamily = [...familyRows].sort(sortRowsForTargetChoice);
  const targetRow = sortedFamily[0];

  for (const row of sortedFamily) {
    const isTarget = row.id === targetRow.id;
    const existing = updatesById.get(row.id) || { id: row.id, fields: {} };

    existing.fields[FIELD_IS_TARGET] = isTarget;
    existing.fields[FIELD_ENTRY_SEQUENCE] = isTarget ? 1 : 2;

    if (isTarget) {
      existing.fields[FIELD_TARGETS_LAST_AT] = NOW_ISO;
      targetCount += 1;
    } else {
      nonTargetRowsInMultiSets += 1;
    }

    updatesById.set(row.id, existing);
  }
}

for (const update of updatesById.values()) {
  const f = update.fields || {};
  const isTarget = !!f[FIELD_IS_TARGET];
  const entrySequence = f[FIELD_ENTRY_SEQUENCE];

  if (!isTarget && (entrySequence === null || entrySequence === undefined)) {
    invalidOrResetCount += 1;
  }
}

const updates = [...updatesById.values()];
for (const batch of chunk50(updates)) {
  await table.updateRecordsAsync(batch);
}

output.set("table", TABLE_NAME);
output.set("view", VIEW_NAME);
output.set("evaluated_at", NOW_ISO);
output.set("records_in_view", records.length);
output.set("updated_records", updates.length);

output.set("special_rows_marked_total", specialRowsMarked);
output.set("special_targetable_rows_marked", specialTargetableCount);
output.set("special_blocked_rows_marked", specialBlockedCount);

output.set("scrubbed_warmup_rows_marked", scrubbedWarmupCount);
output.set("scrubbed_schooling_pony_rows_marked", scrubbedSchoolingPonyCount);
output.set("scrubbed_mulligan_rows_marked", scrubbedMulliganCount);
output.set("scrubbed_add_back_rows_marked", scrubbedAddBackCount);

output.set("special_usf_rows_marked", specialUsfCount);
output.set("special_classic_rows_marked", specialClassicCount);
output.set("special_handy_rows_marked", specialHandyCount);

output.set("eligible_rows_in_duplicate_pool", eligibleDuplicatePoolCount);
output.set("class_id_families_found", classIdFamiliesCount);
output.set("class_id_solo_families_found", classIdSoloCount);
output.set("class_id_duplicate_families_found", classIdDuplicateFamilyCount);
output.set("class_id_duplicate_rows_found", classIdDuplicateRowCount);

output.set("class_id_group_sequence_duplicate_families_found", classIdGroupSequenceDuplicateFamilyCount);
output.set("class_id_group_sequence_duplicate_rows_found", classIdGroupSequenceDuplicateRowCount);
output.set("unresolved_duplicates_at_class_id_group_sequence", unresolvedAtClassIdGroupSequenceCount);

output.set("solo_target_rows_marked", soloTargetCount);
output.set("target_rows_marked_total", targetCount);
output.set("non_target_rows_marked_in_multi_sets", nonTargetRowsInMultiSets);
output.set("reset_or_ungrouped_rows", invalidOrResetCount);
