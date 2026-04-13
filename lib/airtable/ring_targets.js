const TABLE_NAME = "watch_schedule";
// Set to a view name if you want to limit the run, else leave null.
const VIEW_NAME = null;

const FIELD_RING = "ring_number";
const FIELD_CLASS_ID = "class_id";
const FIELD_CLASS_NUMBER = "class_number";
const FIELD_SECONDS_TILL = "secondsTill";
const FIELD_TRIP_TARGET = "tripTarget";

const FIELD_FOCUS = "focusTargetClassId";
const FIELD_NEXT = "nextTargetClassId";

const MIN_PRIOR_ROWS_FOR_TARGET = 1;

function toNum(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toBool(v) {
  if (v === true || v === false) return v;
  if (v === null || v === undefined) return false;
  const s = String(v).trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "checked";
}

function sameNum(a, b) {
  const na = toNum(a);
  const nb = toNum(b);
  return na === nb;
}

async function batchUpdate(table, updates) {
  const BATCH = 50;
  for (let i = 0; i < updates.length; i += BATCH) {
    await table.updateRecordsAsync(updates.slice(i, i + BATCH));
  }
}

const table = base.getTable(TABLE_NAME);
const query = VIEW_NAME
  ? await table.getView(VIEW_NAME).selectRecordsAsync({
      fields: [
        FIELD_RING,
        FIELD_CLASS_ID,
        FIELD_CLASS_NUMBER,
        FIELD_SECONDS_TILL,
        FIELD_TRIP_TARGET,
        FIELD_FOCUS,
        FIELD_NEXT
      ]
    })
  : await table.selectRecordsAsync({
      fields: [
        FIELD_RING,
        FIELD_CLASS_ID,
        FIELD_CLASS_NUMBER,
        FIELD_SECONDS_TILL,
        FIELD_TRIP_TARGET,
        FIELD_FOCUS,
        FIELD_NEXT
      ]
    });

const rows = query.records.map((record) => ({
  recordId: record.id,
  ringNumber: record.getCellValue(FIELD_RING),
  classId: toNum(record.getCellValue(FIELD_CLASS_ID)),
  classNumber: toNum(record.getCellValue(FIELD_CLASS_NUMBER)),
  secondsTill: toNum(record.getCellValue(FIELD_SECONDS_TILL)),
  tripTarget: toBool(record.getCellValue(FIELD_TRIP_TARGET)),
  currentFocus: toNum(record.getCellValue(FIELD_FOCUS)),
  currentNext: toNum(record.getCellValue(FIELD_NEXT))
}));

const byRing = new Map();

for (const row of rows) {
  const ringKey = String(row.ringNumber ?? "");
  if (!byRing.has(ringKey)) byRing.set(ringKey, []);
  byRing.get(ringKey).push(row);
}

const updates = [];
let qualifyingTargetCount = 0;

for (const [ringKey, ringRows] of byRing.entries()) {
  ringRows.sort((a, b) => {
    const aSec = a.secondsTill ?? Number.POSITIVE_INFINITY;
    const bSec = b.secondsTill ?? Number.POSITIVE_INFINITY;
    if (aSec !== bSec) return aSec - bSec;

    const aClass = a.classNumber ?? Number.POSITIVE_INFINITY;
    const bClass = b.classNumber ?? Number.POSITIVE_INFINITY;
    if (aClass !== bClass) return aClass - bClass;

    const aId = a.classId ?? Number.POSITIVE_INFINITY;
    const bId = b.classId ?? Number.POSITIVE_INFINITY;
    return aId - bId;
  });

  const qualifyingTargets = [];
  for (let i = 0; i < ringRows.length; i++) {
    const row = ringRows[i];
    if (row.tripTarget && i >= MIN_PRIOR_ROWS_FOR_TARGET && row.classId !== null) {
      qualifyingTargets.push({
        index: i,
        classId: row.classId
      });
    }
  }

  qualifyingTargetCount += qualifyingTargets.length;

  for (let i = 0; i < ringRows.length; i++) {
    const row = ringRows[i];

    const ahead = qualifyingTargets.filter(t => t.index > i);
    const focusId = ahead[0]?.classId ?? null;
    const nextId = ahead[1]?.classId ?? null;

    const changed =
      !sameNum(row.currentFocus, focusId) ||
      !sameNum(row.currentNext, nextId);

    if (changed) {
      updates.push({
        id: row.recordId,
        fields: {
          [FIELD_FOCUS]: focusId,
          [FIELD_NEXT]: nextId
        }
      });
    }
  }
}

if (updates.length) {
  await batchUpdate(table, updates);
}

output.set("ok", true);
output.set("ringsProcessed", byRing.size);
output.set("qualifyingTargets", qualifyingTargetCount);
output.set("recordsUpdated", updates.length);
