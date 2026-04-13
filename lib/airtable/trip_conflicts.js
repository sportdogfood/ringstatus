/**
 * Airtable Automation Script (FULL DROP / ROBUST + RESOLUTION)
 *
 * SOURCE:
 *   table "watch_trips"
 *   view  "watch_conflicts"
 *
 * TARGET:
 *   table "watch_conflicts"
 *   creates NEW records only when a conflict is identified
 *
 * CONFLICT RULE (per entry_id, adjacent pair after sort):
 *   - different ring
 *   - different class_group_id
 *   - start-time gap is 0..25 minutes inclusive
 *
 * RESOLUTION:
 *   - computes rough minutes-per-trip (MPT)
 *   - falls back to 2.5 minutes if needed
 *   - estimates class spans from total_trips
 *   - generates two viable plays
 *   - chooses recommended play based on longer-span class
 *
 * IMPORTANT TARGET REQUIREMENT:
 *   - "conflict_id" must exist in watch_conflicts
 *   - it must be a writable single-line-text field (primary field / first column)
 *
 * TYPE SAFETY:
 *   - if target field is Number, values are coerced to Number
 *   - if target field is text, values are written as text
 *   - non-writable / risky field types are skipped
 */

const CFG = {
  SOURCE_TABLE: "watch_trips",
  SOURCE_VIEW: "watch_conflicts",
  TARGET_TABLE: "watch_conflicts",
  TARGET_PRIMARY_FIELD: "conflict_id",

  GAP_MIN_MIN: 0,
  GAP_MAX_MIN: 25,

  DEFAULT_MPT: 2.5,
  MIN_VALID_MPT: 0.5,
  MAX_VALID_MPT: 10,

  EARLY_MAX_SLOT: 5,
  LATE_MIN_SLOTS_A: 8,
  LATE_MIN_SLOTS_B: 6,
  LATE_PERCENT: 0.25,
};

// ---------------- helpers ----------------
const isBlank = (v) =>
  v === null ||
  v === undefined ||
  (typeof v === "string" && v.trim() === "");

function hasField(table, name) {
  return table.fields.some((f) => f.name === name);
}

function fieldSet(table) {
  return new Set(table.fields.map((f) => f.name));
}

function fieldMap(table) {
  const map = new Map();
  for (const f of table.fields) map.set(f.name, f);
  return map;
}

/**
 * Normalize Airtable values:
 * - linked/select objects -> .name if present
 * - arrays of linked/select -> join names
 * - Date -> keep Date
 * - others -> trimmed string
 */
function norm(val) {
  if (val === null || val === undefined) return null;

  if (val instanceof Date && !isNaN(val.getTime())) return val;

  if (Array.isArray(val)) {
    if (val.length === 0) return null;
    const parts = val
      .map((x) => {
        if (x && typeof x === "object") return x.name ?? x.id ?? JSON.stringify(x);
        return String(x);
      })
      .filter((x) => !isBlank(x))
      .map((x) => String(x).trim());
    return parts.length ? parts.join(",") : null;
  }

  if (typeof val === "object") {
    return val.name ?? val.id ?? val.value ?? JSON.stringify(val);
  }

  return String(val).trim();
}

function toNumber(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === "number" && isFinite(val)) return val;
  const s = norm(val);
  if (isBlank(s)) return null;
  const n = Number(String(s).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : null;
}

function round1(n) {
  return Math.round(n * 10) / 10;
}

function clampMpt(n) {
  if (!Number.isFinite(n)) return null;
  if (n < CFG.MIN_VALID_MPT || n > CFG.MAX_VALID_MPT) return null;
  return n;
}

function fmtNumber(n) {
  return Number.isFinite(n) ? String(round1(n)) : "";
}

function toMinutes(val) {
  if (val === null || val === undefined) return null;

  if (val instanceof Date && !isNaN(val.getTime())) {
    return val.getHours() * 60 + val.getMinutes() + val.getSeconds() / 60;
  }

  const s = String(val).trim();
  if (!s) return null;

  // ISO-like
  const iso = s.match(/T(\d{2}):(\d{2})(?::(\d{2}))?/);
  if (iso) {
    const hh = parseInt(iso[1], 10);
    const mm = parseInt(iso[2], 10);
    const ss = iso[3] ? parseInt(iso[3], 10) : 0;
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59) return hh * 60 + mm + ss / 60;
  }

  // 24-hour
  const h24 = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (h24) {
    const hh = parseInt(h24[1], 10);
    const mm = parseInt(h24[2], 10);
    const ss = h24[3] ? parseInt(h24[3], 10) : 0;
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59) return hh * 60 + mm + ss / 60;
  }

  // 12-hour
  const h12 = s.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)\b/i);
  if (h12) {
    let hh = parseInt(h12[1], 10);
    const mm = parseInt(h12[2], 10);
    const ss = h12[3] ? parseInt(h12[3], 10) : 0;
    const ap = h12[4].toUpperCase();
    if (hh < 1 || hh > 12 || mm < 0 || mm > 59) return null;
    if (hh === 12) hh = 0;
    if (ap === "PM") hh += 12;
    return hh * 60 + mm + ss / 60;
  }

  // loose match
  const loose = s.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?/i);
  if (loose) {
    let hh = parseInt(loose[1], 10);
    const mm = parseInt(loose[2], 10);
    const ss = loose[3] ? parseInt(loose[3], 10) : 0;
    const ap = loose[4] ? loose[4].toUpperCase() : null;

    if (ap) {
      if (hh === 12) hh = 0;
      if (ap === "PM") hh += 12;
    }
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59) return hh * 60 + mm + ss / 60;
  }

  return null;
}

function fmtTime(val) {
  if (val instanceof Date && !isNaN(val.getTime())) {
    const hh = String(val.getHours()).padStart(2, "0");
    const mm = String(val.getMinutes()).padStart(2, "0");
    const ss = String(val.getSeconds()).padStart(2, "0");
    return `${hh}:${mm}:${ss}`;
  }
  return isBlank(val) ? "" : String(val);
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function firstExisting(candidates, sourceFieldNames) {
  for (const n of candidates) {
    if (sourceFieldNames.has(n)) return n;
  }
  return null;
}

function pickFirstValue(rec, candidates) {
  for (const f of candidates) {
    const v = rec.getCellValue(f);
    if (!isBlank(v)) return { field: f, value: v };
  }
  return { field: null, value: null };
}

function earlyRange(totalTrips, maxSlots = CFG.EARLY_MAX_SLOT) {
  const t = toNumber(totalTrips);
  if (!Number.isFinite(t) || t <= 0) return "1-5";
  const end = Math.max(1, Math.min(maxSlots, Math.floor(t)));
  return `1-${end}`;
}

function lateRange(totalTrips, minSlots) {
  const t = toNumber(totalTrips);
  if (!Number.isFinite(t) || t <= 0) return "late";
  const lateCount = Math.max(minSlots, Math.ceil(t * CFG.LATE_PERCENT));
  const start = Math.max(1, Math.floor(t) - lateCount + 1);
  const end = Math.max(1, Math.floor(t));
  return `${start}-${end}`;
}

function safeWritableFieldType(type) {
  return [
    "singleLineText",
    "multilineText",
    "richText",
    "email",
    "url",
    "phoneNumber",
    "number",
    "currency",
    "percent",
    "duration",
    "checkbox",
    "date",
    "dateTime",
  ].includes(type);
}

function coerceForField(field, value) {
  if (!field) return undefined;
  if (!safeWritableFieldType(field.type)) return undefined;
  if (value === undefined || value === null) return undefined;

  switch (field.type) {
    case "number":
    case "currency":
    case "percent":
    case "duration": {
      const n = toNumber(value);
      return Number.isFinite(n) ? n : undefined;
    }

    case "checkbox":
      return !!value;

    case "date":
    case "dateTime": {
      if (value instanceof Date && !isNaN(value.getTime())) return value;
      const d = new Date(value);
      return isNaN(d.getTime()) ? undefined : d;
    }

    default:
      return String(value);
  }
}

function setSmart(fields, targetFieldMap, name, value) {
  const field = targetFieldMap.get(name);
  if (!field) return;
  const coerced = coerceForField(field, value);
  if (coerced === undefined) return;
  fields[name] = coerced;
}

// ---------------- tables ----------------
const srcTable = base.getTable(CFG.SOURCE_TABLE);
const srcView = srcTable.getView(CFG.SOURCE_VIEW);
const tgtTable = base.getTable(CFG.TARGET_TABLE);

const srcFieldNames = fieldSet(srcTable);
const tgtFieldMap = fieldMap(tgtTable);

// target requirement
if (!hasField(tgtTable, CFG.TARGET_PRIMARY_FIELD)) {
  throw new Error(`Target table "${CFG.TARGET_TABLE}" must have field "${CFG.TARGET_PRIMARY_FIELD}".`);
}

// dedupe
const targetHasConflictKey = hasField(tgtTable, "conflict_key");
const DEDUPE_FIELD = targetHasConflictKey ? "conflict_key" : CFG.TARGET_PRIMARY_FIELD;

// ---------------- source field candidates ----------------
const F = {
  entry_id: ["entry_id"],
  entry_number: ["entry_number"],
  horse: ["horseName", "horse"],

  ring: ["ring_number", "ring"],
  class_group_id: ["class_group_id"],
  class_number: ["class_number"],
  total_trips: ["total_trips"],

  start_candidates: [
    "last_estimated_start_time",
    "latest_estimated_start_time",
    "last_estimated_time",
    "estimated_start_time",
    "sanity_start_time",
    "sanity_start_time (from watch_schedule)",
    "startKey",
    "latestStart",
  ],

  go_first_candidates: [
    "estimated_go_time",
  ],

  go_last_candidates: [
    "last_estimated_go_time",
    "lastGO",
    "latestGO",
  ],

  oog_first_candidates: [
    "order_of_go",
  ],

  oog_last_candidates: [
    "last_order_of_go",
    "lastOOG",
  ],
};

const FN = {
  entry_id: firstExisting(F.entry_id, srcFieldNames),
  entry_number: firstExisting(F.entry_number, srcFieldNames),
  horse: firstExisting(F.horse, srcFieldNames),
  ring: firstExisting(F.ring, srcFieldNames),
  class_group_id: firstExisting(F.class_group_id, srcFieldNames),
  class_number: firstExisting(F.class_number, srcFieldNames),
  total_trips: firstExisting(F.total_trips, srcFieldNames),
  start_candidates: F.start_candidates.filter((n) => srcFieldNames.has(n)),
  go_first_candidates: F.go_first_candidates.filter((n) => srcFieldNames.has(n)),
  go_last_candidates: F.go_last_candidates.filter((n) => srcFieldNames.has(n)),
  oog_first_candidates: F.oog_first_candidates.filter((n) => srcFieldNames.has(n)),
  oog_last_candidates: F.oog_last_candidates.filter((n) => srcFieldNames.has(n)),
};

if (!FN.entry_id || !FN.ring || !FN.class_group_id) {
  throw new Error(
    `Missing required source fields. Need entry_id/ring_number/class_group_id. Found: entry_id=${FN.entry_id}, ring=${FN.ring}, class_group_id=${FN.class_group_id}`
  );
}

if (FN.start_candidates.length === 0) {
  throw new Error(`No usable start-time fields found on watch_trips.`);
}

// ---------------- reads ----------------
const sourceFieldList = [
  FN.entry_id,
  FN.entry_number,
  FN.horse,
  FN.ring,
  FN.class_group_id,
  FN.class_number,
  FN.total_trips,
  ...FN.start_candidates,
  ...FN.go_first_candidates,
  ...FN.go_last_candidates,
  ...FN.oog_first_candidates,
  ...FN.oog_last_candidates,
].filter((x, i, a) => !!x && a.indexOf(x) === i);

const srcQuery = await srcView.selectRecordsAsync({ fields: sourceFieldList });

const tgtFields = [CFG.TARGET_PRIMARY_FIELD];
if (targetHasConflictKey) tgtFields.push("conflict_key");
const tgtQuery = await tgtTable.selectRecordsAsync({ fields: tgtFields });

const existingKeys = new Set();
for (const r of tgtQuery.records) {
  const k = r.getCellValue(DEDUPE_FIELD);
  if (!isBlank(k)) existingKeys.add(String(k).trim());
}

// ---------------- build source rows ----------------
const byEntry = new Map();

let skippedMissingStart = 0;
let skippedUnparseableStart = 0;

for (const rec of srcQuery.records) {
  const entryId = norm(rec.getCellValue(FN.entry_id));
  if (isBlank(entryId)) continue;

  const ring = norm(rec.getCellValue(FN.ring));
  const classGroupId = norm(rec.getCellValue(FN.class_group_id));

  const pickedStart = pickFirstValue(rec, FN.start_candidates);
  const startRaw = pickedStart.value ? norm(pickedStart.value) : null;
  const startMin = toMinutes(startRaw);

  if (isBlank(startRaw)) {
    skippedMissingStart++;
    continue;
  }
  if (startMin === null) {
    skippedUnparseableStart++;
    continue;
  }

  const pickedGoFirst = pickFirstValue(rec, FN.go_first_candidates);
  const pickedGoLast = pickFirstValue(rec, FN.go_last_candidates);
  const pickedOogFirst = pickFirstValue(rec, FN.oog_first_candidates);
  const pickedOogLast = pickFirstValue(rec, FN.oog_last_candidates);

  const goFirstRaw = pickedGoFirst.value ? norm(pickedGoFirst.value) : null;
  const goLastRaw = pickedGoLast.value ? norm(pickedGoLast.value) : null;
  const goFirstMin = toMinutes(goFirstRaw);
  const goLastMin = toMinutes(goLastRaw);

  const oogFirst = toNumber(norm(pickedOogFirst.value));
  const oogLast = toNumber(norm(pickedOogLast.value));

  const currentGoRaw = !isBlank(goLastRaw) ? goLastRaw : goFirstRaw;
  const currentGoMin = !isBlank(goLastRaw) && goLastMin !== null ? goLastMin : goFirstMin;
  const currentOog = Number.isFinite(oogLast) ? oogLast : oogFirst;

  let mpt = null;
  let mptSource = "fallback";

  // midday / first->latest delta
  if (
    goFirstMin !== null &&
    goLastMin !== null &&
    Number.isFinite(oogFirst) &&
    Number.isFinite(oogLast)
  ) {
    const oogDelta = oogLast - oogFirst;
    const timeDelta = goLastMin - goFirstMin;
    const calc = clampMpt(timeDelta > 0 && oogDelta > 0 ? timeDelta / oogDelta : null);
    if (calc !== null) {
      mpt = calc;
      mptSource = "delta_go";
    }
  }

  // pre-show / single snapshot
  if (
    mpt === null &&
    currentGoMin !== null &&
    startMin !== null &&
    Number.isFinite(currentOog) &&
    currentOog >= 2
  ) {
    const timeDelta = currentGoMin - startMin;
    const oogDelta = currentOog - 1;
    const calc = clampMpt(timeDelta > 0 && oogDelta > 0 ? timeDelta / oogDelta : null);
    if (calc !== null) {
      mpt = calc;
      mptSource = "snapshot_go_start";
    }
  }

  if (mpt === null) {
    mpt = CFG.DEFAULT_MPT;
    mptSource = "fallback";
  }

  const item = {
    recId: rec.id,
    entryId: String(entryId),
    entryNumber: FN.entry_number ? norm(rec.getCellValue(FN.entry_number)) : null,
    horseName: FN.horse ? norm(rec.getCellValue(FN.horse)) : null,
    ring: isBlank(ring) ? null : String(ring),
    classGroupId: isBlank(classGroupId) ? null : String(classGroupId),
    classNumber: FN.class_number ? norm(rec.getCellValue(FN.class_number)) : null,
    totalTrips: FN.total_trips ? toNumber(rec.getCellValue(FN.total_trips)) : null,
    startRaw,
    startMin,
    goFirstRaw,
    goLastRaw,
    currentGoRaw,
    oogFirst,
    oogLast,
    currentOog,
    mpt,
    mptSource,
  };

  if (!byEntry.has(item.entryId)) byEntry.set(item.entryId, []);
  byEntry.get(item.entryId).push(item);
}

// ---------------- detect conflicts + build resolution ----------------
const conflicts = [];

for (const [entryId, items] of byEntry.entries()) {
  if (items.length < 2) continue;

  // fast eliminate
  const rings = new Set(items.map((x) => x.ring).filter((x) => !isBlank(x)));
  const groups = new Set(items.map((x) => x.classGroupId).filter((x) => !isBlank(x)));
  if (rings.size < 2) continue;
  if (groups.size < 2) continue;

  items.sort((a, b) => a.startMin - b.startMin);

  for (let i = 0; i < items.length - 1; i++) {
    const A = items[i];
    const B = items[i + 1];

    if (isBlank(A.ring) || isBlank(B.ring)) continue;
    if (isBlank(A.classGroupId) || isBlank(B.classGroupId)) continue;
    if (A.ring === B.ring) continue;
    if (A.classGroupId === B.classGroupId) continue;

    const gap = B.startMin - A.startMin;
    if (gap < CFG.GAP_MIN_MIN || gap > CFG.GAP_MAX_MIN) continue;

    const gapMin = Math.round(gap);

    const conflictKey = `${entryId}|${A.classGroupId}|${B.classGroupId}|R${A.ring}->R${B.ring}|gap${gapMin}`;
    if (existingKeys.has(conflictKey)) continue;

    const mptA = Number.isFinite(A.mpt) ? A.mpt : CFG.DEFAULT_MPT;
    const mptB = Number.isFinite(B.mpt) ? B.mpt : CFG.DEFAULT_MPT;
    const mptUsed = round1((mptA + mptB) / 2);

    const totalTripsA = Number.isFinite(A.totalTrips) ? A.totalTrips : null;
    const totalTripsB = Number.isFinite(B.totalTrips) ? B.totalTrips : null;

    const spanA = Number.isFinite(totalTripsA) ? round1(Math.max(0, (totalTripsA - 1) * mptA)) : null;
    const spanB = Number.isFinite(totalTripsB) ? round1(Math.max(0, (totalTripsB - 1) * mptB)) : null;

    const play1EarlyA = earlyRange(totalTripsA);
    const play1LateB = lateRange(totalTripsB, CFG.LATE_MIN_SLOTS_B);
    const play2LateA = lateRange(totalTripsA, CFG.LATE_MIN_SLOTS_A);

    const play1 =
      `Go early in Ring ${A.ring} (${play1EarlyA}), then ask Ring ${B.ring} for a late slot (${play1LateB}).`;

    const play2 =
      `Ride Ring ${B.ring} first, then ask Ring ${A.ring} for a late slot (${play2LateA}).`;

    let recommendedPlayId = "play1";
    if (Number.isFinite(spanA) && Number.isFinite(spanB)) {
      recommendedPlayId = spanA >= spanB ? "play2" : "play1";
    } else if (Number.isFinite(totalTripsA) && Number.isFinite(totalTripsB)) {
      recommendedPlayId = totalTripsA >= totalTripsB ? "play2" : "play1";
    }

    const recommendedPlay = recommendedPlayId === "play2" ? play2 : play1;

    const resolutionText =
      recommendedPlayId === "play2"
        ? `Conflict found. Earlier class is Ring ${A.ring} at ${fmtTime(A.startRaw)} and later class is Ring ${B.ring} at ${fmtTime(B.startRaw)} with a ${gapMin}-minute gap. Preferred play: use the longer runway in Ring ${A.ring} and ask for a late slot (${play2LateA}) after riding Ring ${B.ring} first. Alternate play: go early in Ring ${A.ring} (${play1EarlyA}) and ask Ring ${B.ring} for a late slot (${play1LateB}).`
        : `Conflict found. Earlier class is Ring ${A.ring} at ${fmtTime(A.startRaw)} and later class is Ring ${B.ring} at ${fmtTime(B.startRaw)} with a ${gapMin}-minute gap. Preferred play: go early in Ring ${A.ring} (${play1EarlyA}) and ask Ring ${B.ring} for a late slot (${play1LateB}). Alternate play: ride Ring ${B.ring} first and ask Ring ${A.ring} for a late slot (${play2LateA}).`;

    const summary =
      `entry ${entryId} gap ${gapMin}m: R${A.ring} ${fmtTime(A.startRaw)} -> R${B.ring} ${fmtTime(B.startRaw)} | rec=${recommendedPlayId}`;

    conflicts.push({
      conflictKey,
      entryId,
      A,
      B,
      gapMin,
      mptA: round1(mptA),
      mptB: round1(mptB),
      mptUsed,
      spanA,
      spanB,
      play1,
      play2,
      recommendedPlayId,
      recommendedPlay,
      resolutionText,
      summary,
    });
  }
}

// ---------------- create target records ----------------
const creates = [];

for (const c of conflicts) {
  const fields = {};

  // required safe text fields
  setSmart(fields, tgtFieldMap, CFG.TARGET_PRIMARY_FIELD, c.conflictKey);
  if (targetHasConflictKey) setSmart(fields, tgtFieldMap, "conflict_key", c.conflictKey);

  // ids / numbers: write safely according to target field type
  setSmart(fields, tgtFieldMap, "entry_id", c.entryId);
  setSmart(fields, tgtFieldMap, "entry_number", c.A.entryNumber);

  setSmart(fields, tgtFieldMap, "ringA", c.A.ring);
  setSmart(fields, tgtFieldMap, "ringB", c.B.ring);

  setSmart(fields, tgtFieldMap, "class_group_idA", c.A.classGroupId);
  setSmart(fields, tgtFieldMap, "class_group_idB", c.B.classGroupId);

  setSmart(fields, tgtFieldMap, "class_numberA", c.A.classNumber);
  setSmart(fields, tgtFieldMap, "class_numberB", c.B.classNumber);

  setSmart(fields, tgtFieldMap, "gap_min", c.gapMin);
  setSmart(fields, tgtFieldMap, "total_tripsA", c.A.totalTrips);
  setSmart(fields, tgtFieldMap, "total_tripsB", c.B.totalTrips);

  // safe text fields
  setSmart(fields, tgtFieldMap, "startA", fmtTime(c.A.startRaw));
  setSmart(fields, tgtFieldMap, "startB", fmtTime(c.B.startRaw));
  setSmart(fields, tgtFieldMap, "record_idA", c.A.recId);
  setSmart(fields, tgtFieldMap, "record_idB", c.B.recId);
  setSmart(fields, tgtFieldMap, "horseName", c.A.horseName);
  setSmart(fields, tgtFieldMap, "summary", c.summary);

  // optional resolution fields (only written if they exist and are writable)
  setSmart(fields, tgtFieldMap, "has_conflict", true);

  setSmart(fields, tgtFieldMap, "mptA", c.mptA);
  setSmart(fields, tgtFieldMap, "mptB", c.mptB);
  setSmart(fields, tgtFieldMap, "mpt_used", c.mptUsed);

  setSmart(fields, tgtFieldMap, "spanA_min", c.spanA);
  setSmart(fields, tgtFieldMap, "spanB_min", c.spanB);

  setSmart(fields, tgtFieldMap, "play1", c.play1);
  setSmart(fields, tgtFieldMap, "play2", c.play2);
  setSmart(fields, tgtFieldMap, "recommended_play_id", c.recommendedPlayId);
  setSmart(fields, tgtFieldMap, "recommended_play", c.recommendedPlay);
  setSmart(fields, tgtFieldMap, "resolution_text", c.resolutionText);

  creates.push({ fields });
}

// batch create
let created = 0;
for (const batch of chunk(creates, 50)) {
  if (batch.length === 0) continue;
  await tgtTable.createRecordsAsync(batch);
  created += batch.length;
}

// outputs
output.set("source_rows", srcQuery.records.length);
output.set("grouped_entry_ids", byEntry.size);
output.set("conflicts_found", conflicts.length);
output.set("conflicts_created", created);
output.set("skipped_missing_start", skippedMissingStart);
output.set("skipped_unparseable_start", skippedUnparseableStart);
output.set("dedupe_field", DEDUPE_FIELD);
output.set("start_fields_checked", FN.start_candidates.join(", "));
