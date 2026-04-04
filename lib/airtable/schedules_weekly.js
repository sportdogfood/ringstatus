/**
 * Airtable Automation Script — WEEKLY ROSTER V3
 *
 * Corrected source-of-truth rules in this drop:
 * - /ring?customer_id=15 remains source of truth for app_sid only
 * - app_sql_date is sourced from Airtable table: shows
 * - app_sql_date is treated as raw text only
 * - no normalization, no date parsing, no timezone conversion for app_sql_date
 * - the matched shows record id is written into the shows link field
 *   on every record created or updated
 * - PID discovery from ww_trainers requires:
 *     active = true
 *     pull_pid_allowed = true
 *     pid populated
 * - empty trips payload is logged to automation_errs as a caution and skipped
 * - people show_id conflict is logged to automation_errs and skipped
 * - fetch failures are logged to automation_errs and the script attempts to continue
 * - upserts:
 *     ww_riders      by rider_id
 *     ww_horses      by horse_id
 *     active_classes by key = app_sid|pid|class_id
 *     active_entries by key = app_sid|pid|entry_id
 *
 * Inputs (Run script → Input variables):
 * - pid (Number, optional)
 * - pids (Text, optional)      // list: "8778,1234|5678"
 * - base_url (Text, optional)  default: https://broad-tooth-b8ed.gombcg.workers.dev
 * - pull_pip (Number, optional) default: 4
 * - customer_id (Number, optional) default: 15
 * - automation_name (Text, optional) default: weekly_roster_v3
 */

//////////////////////
// INPUTS / HELPERS
//////////////////////
const cfg = input.config();

function toInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}
function s(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}
function chunk50(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 50) out.push(arr.slice(i, i + 50));
  return out;
}
function getTable(name) {
  const t = base.getTable(name);
  if (!t) throw new Error(`Missing table: ${name}`);
  return t;
}
function getWritableFieldSet(table) {
  return new Set(table.fields.filter(f => !f.isComputed).map(f => f.name));
}
function pickWritable(writableSet, obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (!writableSet.has(k)) continue;
    if (v === undefined || v === null) continue;
    out[k] = v;
  }
  return out;
}
function firstFieldName(table, candidates) {
  for (const n of candidates) {
    if (table.fields.some(f => f.name === n)) return n;
  }
  return null;
}
async function runPool(items, concurrency, handler) {
  const queue = [...items];
  const workers = new Array(Math.max(1, concurrency)).fill(0).map(async () => {
    while (queue.length) {
      const item = queue.shift();
      await handler(item);
    }
  });
  await Promise.all(workers);
}
async function fetchJsonSafe(url) {
  try {
    const res = await fetch(url);

    let text = "";
    let data = null;
    try {
      text = await res.text();
      data = text ? JSON.parse(text) : null;
    } catch (_) {
      data = null;
    }

    if (!res.ok) {
      return {
        ok: false,
        status: res.status,
        statusText: res.statusText,
        url,
        text: text || "",
        data,
      };
    }

    return {
      ok: true,
      status: res.status,
      statusText: res.statusText,
      url,
      text: text || "",
      data,
    };
  } catch (e) {
    return {
      ok: false,
      status: null,
      statusText: "FETCH_EXCEPTION",
      url,
      text: "",
      data: null,
      error: String(e?.message || e),
    };
  }
}

const base_url = s(cfg.base_url || "https://broad-tooth-b8ed.gombcg.workers.dev").replace(/\/+$/, "");
const pull_pip = Math.max(1, toInt(cfg.pull_pip) || 4);
const customer_id = Math.max(1, toInt(cfg.customer_id) || 15);
const AUTOMATION_NAME = s(cfg.automation_name || "weekly_roster_v3");

//////////////////////
// RUN FIELDS
//////////////////////
const now = new Date();
const RUN_ID = Number(
  new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now).replaceAll("-", "")
);
const LAST_RUN = now.toISOString();

//////////////////////
// TABLES
//////////////////////
const showsTable          = getTable("shows");
const wwTrainersTable     = getTable("ww_trainers");
const wwRidersTable       = getTable("ww_riders");
const wwHorsesTable       = getTable("ww_horses");
const activeClassesTable  = getTable("active_classes");
const activeEntriesTable  = getTable("active_entries");
const automationErrsTable = getTable("automation_errs");

const W_WW_RIDERS        = getWritableFieldSet(wwRidersTable);
const W_WW_HORSES        = getWritableFieldSet(wwHorsesTable);
const W_ACTIVE_CLASSES   = getWritableFieldSet(activeClassesTable);
const W_ACTIVE_ENTRIES   = getWritableFieldSet(activeEntriesTable);
const W_AUTOMATION_ERRS  = getWritableFieldSet(automationErrsTable);

//////////////////////
// FIELD MAPS
//////////////////////

// shows
const F_SHOWS_SHOW_ID = firstFieldName(showsTable, ["show_id"]) || "show_id";
const F_SHOWS_APP_SQL_DATE = firstFieldName(showsTable, ["app_sql_date"]) || "app_sql_date";

// ww_trainers
const F_WW_TRAINER_PID = firstFieldName(wwTrainersTable, ["pid"]) || "pid";
const F_WW_TRAINER_ACTIVE = firstFieldName(wwTrainersTable, ["active"]) || "active";
const F_WW_TRAINER_PULL_ALLOWED = firstFieldName(wwTrainersTable, ["pull_pid_allowed"]) || "pull_pid_allowed";

// ww_riders
const F_RIDER_ID = firstFieldName(wwRidersTable, ["rider_id"]) || "rider_id";
const F_RIDER_NAME = firstFieldName(wwRidersTable, ["rider_name"]) || "rider_name";
const F_RIDER_PID = firstFieldName(wwRidersTable, ["pid"]) || "pid";
const F_RIDER_APP_SID = firstFieldName(wwRidersTable, ["app_sid"]) || "app_sid";
const F_RIDER_APP_SQL_DATE = firstFieldName(wwRidersTable, ["app_sql_date"]) || "app_sql_date";
const F_RIDER_SHOWS = firstFieldName(wwRidersTable, ["shows"]) || "shows";
const F_RIDER_INACTIVE = firstFieldName(wwRidersTable, ["inactive"]) || "inactive";
const F_RIDER_RUN_ID = firstFieldName(wwRidersTable, ["run_id"]) || "run_id";
const F_RIDER_LAST_RUN = firstFieldName(wwRidersTable, ["last_run"]) || "last_run";
const F_RIDER_NEW = firstFieldName(wwRidersTable, ["new_rider"]) || "new_rider";
const F_RIDER_PID_MISMATCH = firstFieldName(wwRidersTable, ["pid_mismatch"]) || "pid_mismatch";

// ww_horses
const F_HORSE_ID = firstFieldName(wwHorsesTable, ["horse_id"]) || "horse_id";
const F_HORSE_NAME = firstFieldName(wwHorsesTable, ["horse"]) || "horse";
const F_HORSE_PID = firstFieldName(wwHorsesTable, ["pid"]) || "pid";
const F_HORSE_APP_SID = firstFieldName(wwHorsesTable, ["app_sid"]) || "app_sid";
const F_HORSE_APP_SQL_DATE = firstFieldName(wwHorsesTable, ["app_sql_date"]) || "app_sql_date";
const F_HORSE_SHOWS = firstFieldName(wwHorsesTable, ["shows"]) || "shows";
const F_HORSE_INACTIVE = firstFieldName(wwHorsesTable, ["inactive"]) || "inactive";
const F_HORSE_RUN_ID = firstFieldName(wwHorsesTable, ["run_id"]) || "run_id";
const F_HORSE_LAST_RUN = firstFieldName(wwHorsesTable, ["last_run"]) || "last_run";
const F_HORSE_NEW = firstFieldName(wwHorsesTable, ["new_horse"]) || "new_horse";
const F_HORSE_PID_MISMATCH = firstFieldName(wwHorsesTable, ["pid_mismatch"]) || "pid_mismatch";

// active_classes
const F_CLASS_KEY = firstFieldName(activeClassesTable, ["key"]) || "key";
const F_CLASS_APP_SID = firstFieldName(activeClassesTable, ["app_sid"]) || "app_sid";
const F_CLASS_APP_SQL_DATE = firstFieldName(activeClassesTable, ["app_sql_date"]) || "app_sql_date";
const F_CLASS_SHOWS = firstFieldName(activeClassesTable, ["shows"]) || "shows";
const F_CLASS_PID = firstFieldName(activeClassesTable, ["pid"]) || "pid";
const F_CLASS_ID = firstFieldName(activeClassesTable, ["class_id", "classes_id"]) || "class_id";
const F_CLASS_NUMBER = firstFieldName(activeClassesTable, ["class_number"]) || "class_number";
const F_CLASS_NAME = firstFieldName(activeClassesTable, ["class_name"]) || "class_name";
const F_CLASS_INACTIVE = firstFieldName(activeClassesTable, ["inactive"]) || "inactive";
const F_CLASS_RUN_ID = firstFieldName(activeClassesTable, ["run_id"]) || "run_id";
const F_CLASS_LAST_RUN = firstFieldName(activeClassesTable, ["last_run"]) || "last_run";
const F_CLASS_NEW = firstFieldName(activeClassesTable, ["new_class_id"]) || "new_class_id";
const F_CLASS_PID_MISMATCH = firstFieldName(activeClassesTable, ["pid_mismatch"]) || "pid_mismatch";

// active_entries
const F_ENTRY_KEY = firstFieldName(activeEntriesTable, ["key"]) || "key";
const F_ENTRY_APP_SID = firstFieldName(activeEntriesTable, ["app_sid"]) || "app_sid";
const F_ENTRY_APP_SQL_DATE = firstFieldName(activeEntriesTable, ["app_sql_date"]) || "app_sql_date";
const F_ENTRY_SHOWS = firstFieldName(activeEntriesTable, ["shows"]) || "shows";
const F_ENTRY_PID = firstFieldName(activeEntriesTable, ["pid"]) || "pid";
const F_ENTRY_ID = firstFieldName(activeEntriesTable, ["entry_id"]) || "entry_id";
const F_ENTRY_NUMBER = firstFieldName(activeEntriesTable, ["entry_number"]) || "entry_number";
const F_ENTRY_HORSE_ID = firstFieldName(activeEntriesTable, ["horse_id"]) || "horse_id";
const F_ENTRY_HORSE = firstFieldName(activeEntriesTable, ["horse"]) || "horse";
const F_ENTRY_RIDER_ID = firstFieldName(activeEntriesTable, ["rider_id"]) || "rider_id";
const F_ENTRY_RIDER_NAME = firstFieldName(activeEntriesTable, ["rider_name"]) || "rider_name";
const F_ENTRY_INACTIVE = firstFieldName(activeEntriesTable, ["inactive"]) || "inactive";
const F_ENTRY_RUN_ID = firstFieldName(activeEntriesTable, ["run_id"]) || "run_id";
const F_ENTRY_LAST_RUN = firstFieldName(activeEntriesTable, ["last_run"]) || "last_run";
const F_ENTRY_NEW = firstFieldName(activeEntriesTable, ["new_entry"]) || "new_entry";
const F_ENTRY_PID_MISMATCH = firstFieldName(activeEntriesTable, ["pid_mismatch"]) || "pid_mismatch";

// automation_errs
const F_ERR_KEY = firstFieldName(automationErrsTable, ["automation_key"]) || "automation_key";
const F_ERR_NAME = firstFieldName(automationErrsTable, ["automation_name"]) || "automation_name";
const F_ERR_PID = firstFieldName(automationErrsTable, ["pid"]) || "pid";
const F_ERR_APP_SHOW_ID = firstFieldName(automationErrsTable, ["app_show_id"]) || "app_show_id";
const F_ERR_PEOPLE_SHOW_ID = firstFieldName(automationErrsTable, ["people_show_id"]) || "people_show_id";
const F_ERR_APP_SQL_DATE = firstFieldName(automationErrsTable, ["app_sql_date"]) || "app_sql_date";
const F_ERR_RUN_ID = firstFieldName(automationErrsTable, ["run_id"]) || "run_id";
const F_ERR_LAST_RUN = firstFieldName(automationErrsTable, ["last_run"]) || "last_run";
const F_ERR_TYPE = firstFieldName(automationErrsTable, ["error_type"]) || "error_type";
const F_ERR_MESSAGE = firstFieldName(automationErrsTable, ["message"]) || "message";
const F_ERR_RESOLVED = firstFieldName(automationErrsTable, ["resolved"]) || "resolved";

//////////////////////
// ERR LOG
//////////////////////
async function logAutomationErr({
  pid,
  appShowId,
  peopleShowId,
  appSqlDate,
  errorType,
  message,
  resolved = false,
}) {
  const automationKey = `${AUTOMATION_NAME}|${pid || 0}|${RUN_ID}|${errorType || "notice"}`;

  const fields = pickWritable(W_AUTOMATION_ERRS, {
    [F_ERR_KEY]: automationKey,
    [F_ERR_NAME]: AUTOMATION_NAME,
    [F_ERR_PID]: pid != null ? Number(pid) : undefined,
    [F_ERR_APP_SHOW_ID]: appShowId != null ? Number(appShowId) : undefined,
    [F_ERR_PEOPLE_SHOW_ID]: peopleShowId != null ? Number(peopleShowId) : undefined,
    [F_ERR_APP_SQL_DATE]: s(appSqlDate),
    [F_ERR_RUN_ID]: RUN_ID,
    [F_ERR_LAST_RUN]: LAST_RUN,
    [F_ERR_TYPE]: errorType || "notice",
    [F_ERR_MESSAGE]: message || "",
    [F_ERR_RESOLVED]: !!resolved,
  });

  await automationErrsTable.createRecordAsync(fields);
}

//////////////////////
// PID SOURCE BUILD
//////////////////////
async function getAllowedTrainerPids() {
  const out = new Set();

  const q = await wwTrainersTable.selectRecordsAsync({
    fields: [F_WW_TRAINER_PID, F_WW_TRAINER_ACTIVE, F_WW_TRAINER_PULL_ALLOWED]
  });

  for (const rec of q.records) {
    const isActive = rec.getCellValue(F_WW_TRAINER_ACTIVE) === true;
    const isAllowed = rec.getCellValue(F_WW_TRAINER_PULL_ALLOWED) === true;
    if (!isActive || !isAllowed) continue;

    const pidVal = rec.getCellValue(F_WW_TRAINER_PID);
    const pidNum = toInt(pidVal && typeof pidVal === "object" && "name" in pidVal ? pidVal.name : pidVal);
    if (pidNum && pidNum > 0) out.add(pidNum);
  }

  return out;
}

const pidSet = new Set();

const pid_single = toInt(cfg.pid);
if (pid_single && pid_single > 0) pidSet.add(pid_single);

const pids_csv = s(cfg.pids);
if (pids_csv) {
  for (const part of pids_csv.split(/[,\s|]+/g)) {
    const n = toInt(part);
    if (n && n > 0) pidSet.add(n);
  }
}

const trainerPidSet = await getAllowedTrainerPids();
for (const n of trainerPidSet) pidSet.add(n);

const pidList = [...pidSet].sort((a, b) => a - b);
if (!pidList.length) {
  throw new Error(`No valid pid values found from cfg.pid, cfg.pids, or ww_trainers(active=true,pull_pid_allowed=true).`);
}

//////////////////////
// RING SOURCE OF TRUTH
//////////////////////
const ringUrl = `${base_url}/ring?customer_id=${encodeURIComponent(customer_id)}`;
const ringRes = await fetchJsonSafe(ringUrl);

if (!ringRes.ok || !ringRes.data) {
  throw new Error(`Ring source failed ${ringRes.status || ""} ${ringRes.statusText || ""} ${ringUrl}`);
}

const ringPayload = ringRes.data;
const APP_SID = toInt(ringPayload?.show_id);

if (!APP_SID) {
  throw new Error(`Ring source of truth missing show_id.`);
}

//////////////////////
// SHOWS SOURCE OF TRUTH
//////////////////////
const showsQuery = await showsTable.selectRecordsAsync({
  fields: [F_SHOWS_SHOW_ID, F_SHOWS_APP_SQL_DATE]
});

let SHOWS_RECORD = null;
for (const rec of showsQuery.records) {
  const recShowId = toInt(rec.getCellValue(F_SHOWS_SHOW_ID));
  if (recShowId === APP_SID) {
    SHOWS_RECORD = rec;
    break;
  }
}

if (!SHOWS_RECORD) {
  await logAutomationErr({
    pid: 0,
    appShowId: APP_SID,
    peopleShowId: null,
    appSqlDate: "",
    errorType: "shows_lookup_missing",
    message: `No record found in shows where show_id = ${APP_SID}.`,
  });
  throw new Error(`No record found in shows where show_id = ${APP_SID}.`);
}

const SHOWS_RECORD_ID = SHOWS_RECORD.id;
const APP_SQL_DATE = s(SHOWS_RECORD.getCellValue(F_SHOWS_APP_SQL_DATE));

if (!APP_SQL_DATE) {
  await logAutomationErr({
    pid: 0,
    appShowId: APP_SID,
    peopleShowId: null,
    appSqlDate: "",
    errorType: "shows_app_sql_date_missing",
    message: `Matched shows record ${SHOWS_RECORD_ID} for show_id ${APP_SID} but app_sql_date is blank.`,
  });
  throw new Error(`Matched shows record ${SHOWS_RECORD_ID} for show_id ${APP_SID} but app_sql_date is blank.`);
}

//////////////////////
// BUILDERS
//////////////////////
function buildRiderRows(payload, pidNum) {
  const trips = Array.isArray(payload?.trips) ? payload.trips : [];
  const byId = new Map();

  for (const t of trips) {
    const rider_id = toInt(t?.rider_id);
    const rider_name = s(t?.rider_name);
    if (!rider_id || rider_id <= 0) continue;

    if (!byId.has(rider_id)) {
      byId.set(rider_id, { rider_id, rider_name });
    } else if (rider_name && !byId.get(rider_id).rider_name) {
      byId.get(rider_id).rider_name = rider_name;
    }
  }

  return [...byId.values()].map(r => ({
    [F_RIDER_ID]: Number(r.rider_id),
    [F_RIDER_NAME]: r.rider_name || "",
    [F_RIDER_PID]: Number(pidNum),
    [F_RIDER_APP_SID]: Number(APP_SID),
    [F_RIDER_APP_SQL_DATE]: APP_SQL_DATE,
    [F_RIDER_SHOWS]: [{ id: SHOWS_RECORD_ID }],
    [F_RIDER_INACTIVE]: false,
    [F_RIDER_RUN_ID]: RUN_ID,
    [F_RIDER_LAST_RUN]: LAST_RUN,
  }));
}

function buildHorseRows(payload, pidNum) {
  const trips = Array.isArray(payload?.trips) ? payload.trips : [];
  const byId = new Map();

  for (const t of trips) {
    const horse_id = toInt(t?.horse_id);
    const horse = s(t?.horse);
    if (!horse_id || horse_id <= 0) continue;

    if (!byId.has(horse_id)) {
      byId.set(horse_id, { horse_id, horse });
    } else if (horse && !byId.get(horse_id).horse) {
      byId.get(horse_id).horse = horse;
    }
  }

  return [...byId.values()].map(h => ({
    [F_HORSE_ID]: Number(h.horse_id),
    [F_HORSE_NAME]: h.horse || "",
    [F_HORSE_PID]: Number(pidNum),
    [F_HORSE_APP_SID]: Number(APP_SID),
    [F_HORSE_APP_SQL_DATE]: APP_SQL_DATE,
    [F_HORSE_SHOWS]: [{ id: SHOWS_RECORD_ID }],
    [F_HORSE_INACTIVE]: false,
    [F_HORSE_RUN_ID]: RUN_ID,
    [F_HORSE_LAST_RUN]: LAST_RUN,
  }));
}

function buildClassRows(payload, pidNum) {
  const trips = Array.isArray(payload?.trips) ? payload.trips : [];
  const byId = new Map();

  for (const t of trips) {
    const class_id = toInt(t?.class_id);
    const class_number = toInt(t?.class_number);
    const class_name = s(t?.class_name);
    if (!class_id || class_id <= 0) continue;

    if (!byId.has(class_id)) {
      byId.set(class_id, { class_id, class_number, class_name });
    } else {
      const cur = byId.get(class_id);
      if (cur.class_number == null && class_number != null) cur.class_number = class_number;
      if (!cur.class_name && class_name) cur.class_name = class_name;
    }
  }

  return [...byId.values()].map(c => ({
    [F_CLASS_KEY]: `${APP_SID}|${pidNum}|${c.class_id}`,
    [F_CLASS_APP_SID]: Number(APP_SID),
    [F_CLASS_APP_SQL_DATE]: APP_SQL_DATE,
    [F_CLASS_SHOWS]: [{ id: SHOWS_RECORD_ID }],
    [F_CLASS_PID]: Number(pidNum),
    [F_CLASS_ID]: Number(c.class_id),
    [F_CLASS_NUMBER]: c.class_number != null ? Number(c.class_number) : undefined,
    [F_CLASS_NAME]: c.class_name || "",
    [F_CLASS_INACTIVE]: false,
    [F_CLASS_RUN_ID]: RUN_ID,
    [F_CLASS_LAST_RUN]: LAST_RUN,
  }));
}

function buildEntryRows(payload, pidNum) {
  const trips = Array.isArray(payload?.trips) ? payload.trips : [];
  const byId = new Map();

  for (const t of trips) {
    const entry_id = toInt(t?.entry_id);
    if (!entry_id || entry_id <= 0) continue;

    if (!byId.has(entry_id)) {
      byId.set(entry_id, {
        entry_id,
        entry_number: toInt(t?.entry_number),
        horse_id: toInt(t?.horse_id),
        horse: s(t?.horse),
        rider_id: toInt(t?.rider_id),
        rider_name: s(t?.rider_name),
      });
    }
  }

  return [...byId.values()].map(e => ({
    [F_ENTRY_KEY]: `${APP_SID}|${pidNum}|${e.entry_id}`,
    [F_ENTRY_APP_SID]: Number(APP_SID),
    [F_ENTRY_APP_SQL_DATE]: APP_SQL_DATE,
    [F_ENTRY_SHOWS]: [{ id: SHOWS_RECORD_ID }],
    [F_ENTRY_PID]: Number(pidNum),
    [F_ENTRY_ID]: Number(e.entry_id),
    [F_ENTRY_NUMBER]: e.entry_number != null ? Number(e.entry_number) : undefined,
    [F_ENTRY_HORSE_ID]: e.horse_id != null ? Number(e.horse_id) : undefined,
    [F_ENTRY_HORSE]: e.horse || "",
    [F_ENTRY_RIDER_ID]: e.rider_id != null ? Number(e.rider_id) : undefined,
    [F_ENTRY_RIDER_NAME]: e.rider_name || "",
    [F_ENTRY_INACTIVE]: false,
    [F_ENTRY_RUN_ID]: RUN_ID,
    [F_ENTRY_LAST_RUN]: LAST_RUN,
  }));
}

//////////////////////
// UPSERT HELPERS
//////////////////////
async function upsertByNumberId({
  table,
  writableSet,
  fieldId,
  fieldPid,
  fieldInactive,
  fieldRunId,
  fieldLastRun,
  fieldNewFlag,
  fieldPidMismatch,
  rows,
}) {
  if (!rows.length) return { table: table.name, created: 0, updated: 0, skipped: true };

  const q = await table.selectRecordsAsync({
    fields: [fieldId, fieldPid]
  });

  const existingById = new Map();
  for (const rec of q.records) {
    const idNum = toInt(rec.getCellValue(fieldId));
    if (!idNum || idNum <= 0) continue;
    if (!existingById.has(idNum)) existingById.set(idNum, rec);
  }

  const creates = [];
  const updates = [];

  for (const row of rows) {
    const idNum = toInt(row[fieldId]);
    if (!idNum || idNum <= 0) continue;

    const existing = existingById.get(idNum);
    if (existing) {
      const oldPid = toInt(existing.getCellValue(fieldPid));
      const currentPid = toInt(row[fieldPid]);
      const pidMismatch = oldPid != null && currentPid != null && oldPid !== currentPid;

      updates.push({
        id: existing.id,
        fields: pickWritable(writableSet, {
          ...row,
          [fieldInactive]: false,
          [fieldRunId]: RUN_ID,
          [fieldLastRun]: LAST_RUN,
          [fieldNewFlag]: false,
          [fieldPidMismatch]: pidMismatch,
        }),
      });
    } else {
      creates.push({
        fields: pickWritable(writableSet, {
          ...row,
          [fieldInactive]: false,
          [fieldRunId]: RUN_ID,
          [fieldLastRun]: LAST_RUN,
          [fieldNewFlag]: true,
          [fieldPidMismatch]: false,
        }),
      });
    }
  }

  let created = 0;
  for (const batch of chunk50(creates)) {
    if (batch.length) {
      await table.createRecordsAsync(batch);
      created += batch.length;
    }
  }

  let updated = 0;
  for (const batch of chunk50(updates)) {
    if (batch.length) {
      await table.updateRecordsAsync(batch);
      updated += batch.length;
    }
  }

  return { table: table.name, created, updated, skipped: false };
}

async function upsertActiveByKey({
  table,
  writableSet,
  fieldKey,
  fieldPid,
  fieldInactive,
  fieldRunId,
  fieldLastRun,
  fieldNewFlag,
  fieldPidMismatch,
  rows,
}) {
  if (!rows.length) return { table: table.name, created: 0, updated: 0, inactivated: 0, skipped: true };

  const q = await table.selectRecordsAsync({
    fields: [fieldKey, fieldPid]
  });

  const existingByKey = new Map();
  const pidScope = new Map();

  for (const rec of q.records) {
    const k = s(rec.getCellValue(fieldKey));
    if (!k) continue;
    if (!existingByKey.has(k)) existingByKey.set(k, rec);

    const recPid = toInt(rec.getCellValue(fieldPid));
    if (recPid != null) {
      if (!pidScope.has(recPid)) pidScope.set(recPid, []);
      pidScope.get(recPid).push(rec);
    }
  }

  const keepByPid = new Map();
  for (const row of rows) {
    const pidNum = toInt(row[fieldPid]);
    const k = s(row[fieldKey]);
    if (pidNum == null || !k) continue;
    if (!keepByPid.has(pidNum)) keepByPid.set(pidNum, new Set());
    keepByPid.get(pidNum).add(k);
  }

  const creates = [];
  const updates = [];

  for (const row of rows) {
    const k = s(row[fieldKey]);
    const currentPid = toInt(row[fieldPid]);
    if (!k) continue;

    const existing = existingByKey.get(k);
    if (existing) {
      const oldPid = toInt(existing.getCellValue(fieldPid));
      const pidMismatch = oldPid != null && currentPid != null && oldPid !== currentPid;

      updates.push({
        id: existing.id,
        fields: pickWritable(writableSet, {
          ...row,
          [fieldInactive]: false,
          [fieldRunId]: RUN_ID,
          [fieldLastRun]: LAST_RUN,
          [fieldNewFlag]: false,
          [fieldPidMismatch]: pidMismatch,
        }),
      });
    } else {
      creates.push({
        fields: pickWritable(writableSet, {
          ...row,
          [fieldInactive]: false,
          [fieldRunId]: RUN_ID,
          [fieldLastRun]: LAST_RUN,
          [fieldNewFlag]: true,
          [fieldPidMismatch]: false,
        }),
      });
    }
  }

  let created = 0;
  for (const batch of chunk50(creates)) {
    if (batch.length) {
      await table.createRecordsAsync(batch);
      created += batch.length;
    }
  }

  let updated = 0;
  for (const batch of chunk50(updates)) {
    if (batch.length) {
      await table.updateRecordsAsync(batch);
      updated += batch.length;
    }
  }

  const toInactivate = [];
  for (const [pidNum, recs] of pidScope.entries()) {
    const keepSet = keepByPid.get(pidNum);
    if (!keepSet) continue;

    for (const rec of recs) {
      const k = s(rec.getCellValue(fieldKey));
      if (!k || keepSet.has(k)) continue;

      toInactivate.push({
        id: rec.id,
        fields: pickWritable(writableSet, {
          [fieldInactive]: true,
          [fieldRunId]: RUN_ID,
          [fieldLastRun]: LAST_RUN,
        }),
      });
    }
  }

  let inactivated = 0;
  for (const batch of chunk50(toInactivate)) {
    if (batch.length) {
      await table.updateRecordsAsync(batch);
      inactivated += batch.length;
    }
  }

  return { table: table.name, created, updated, inactivated, skipped: false };
}

//////////////////////
// MAIN
//////////////////////
const results = [];
const errors = [];

async function processPid(pidNum) {
  try {
    const url = `${base_url}/people/${encodeURIComponent(pidNum)}?pid=${encodeURIComponent(pidNum)}&customer_id=${encodeURIComponent(customer_id)}`;
    const peopleRes = await fetchJsonSafe(url);

    if (!peopleRes.ok || !peopleRes.data) {
      const msg = peopleRes.error
        ? `Fetch exception for pid ${pidNum}: ${peopleRes.error}`
        : `Fetch failed ${peopleRes.status || ""} ${peopleRes.statusText || ""} for ${url}${peopleRes.text ? `\n${peopleRes.text.slice(0, 400)}` : ""}`;

      await logAutomationErr({
        pid: pidNum,
        appShowId: APP_SID,
        peopleShowId: null,
        appSqlDate: APP_SQL_DATE,
        errorType: "people_fetch_failed",
        message: msg,
      });

      results.push({ pid: pidNum, skipped: true, reason: "people_fetch_failed" });
      return;
    }

    const payload = peopleRes.data;
    const peopleShowId = toInt(payload?.show_id) || toInt(payload?.people?.show_id);

    if (!peopleShowId || peopleShowId !== APP_SID) {
      await logAutomationErr({
        pid: pidNum,
        appShowId: APP_SID,
        peopleShowId,
        appSqlDate: APP_SQL_DATE,
        errorType: "show_id_conflict",
        message: `Skipped pid ${pidNum}: people show_id ${peopleShowId || "blank"} does not match app_show_id ${APP_SID}.`,
      });

      results.push({
        pid: pidNum,
        skipped: true,
        reason: "show_id_conflict",
        app_show_id: APP_SID,
        people_show_id: peopleShowId || null,
      });
      return;
    }

    const trips = Array.isArray(payload?.trips) ? payload.trips : [];
    if (!trips.length) {
      await logAutomationErr({
        pid: pidNum,
        appShowId: APP_SID,
        peopleShowId,
        appSqlDate: APP_SQL_DATE,
        errorType: "empty_trips_payload",
        message: `Skipped pid ${pidNum}: empty trips payload.`,
      });

      results.push({ pid: pidNum, skipped: true, reason: "empty_trips_payload" });
      return;
    }

    const riderRows = buildRiderRows(payload, pidNum);
    const horseRows = buildHorseRows(payload, pidNum);
    const classRows = buildClassRows(payload, pidNum);
    const entryRows = buildEntryRows(payload, pidNum);

    results.push(await upsertByNumberId({
      table: wwRidersTable,
      writableSet: W_WW_RIDERS,
      fieldId: F_RIDER_ID,
      fieldPid: F_RIDER_PID,
      fieldInactive: F_RIDER_INACTIVE,
      fieldRunId: F_RIDER_RUN_ID,
      fieldLastRun: F_RIDER_LAST_RUN,
      fieldNewFlag: F_RIDER_NEW,
      fieldPidMismatch: F_RIDER_PID_MISMATCH,
      rows: riderRows,
    }));

    results.push(await upsertByNumberId({
      table: wwHorsesTable,
      writableSet: W_WW_HORSES,
      fieldId: F_HORSE_ID,
      fieldPid: F_HORSE_PID,
      fieldInactive: F_HORSE_INACTIVE,
      fieldRunId: F_HORSE_RUN_ID,
      fieldLastRun: F_HORSE_LAST_RUN,
      fieldNewFlag: F_HORSE_NEW,
      fieldPidMismatch: F_HORSE_PID_MISMATCH,
      rows: horseRows,
    }));

    results.push(await upsertActiveByKey({
      table: activeClassesTable,
      writableSet: W_ACTIVE_CLASSES,
      fieldKey: F_CLASS_KEY,
      fieldPid: F_CLASS_PID,
      fieldInactive: F_CLASS_INACTIVE,
      fieldRunId: F_CLASS_RUN_ID,
      fieldLastRun: F_CLASS_LAST_RUN,
      fieldNewFlag: F_CLASS_NEW,
      fieldPidMismatch: F_CLASS_PID_MISMATCH,
      rows: classRows,
    }));

    results.push(await upsertActiveByKey({
      table: activeEntriesTable,
      writableSet: W_ACTIVE_ENTRIES,
      fieldKey: F_ENTRY_KEY,
      fieldPid: F_ENTRY_PID,
      fieldInactive: F_ENTRY_INACTIVE,
      fieldRunId: F_ENTRY_RUN_ID,
      fieldLastRun: F_ENTRY_LAST_RUN,
      fieldNewFlag: F_ENTRY_NEW,
      fieldPidMismatch: F_ENTRY_PID_MISMATCH,
      rows: entryRows,
    }));

    results.push({ pid: pidNum, ok: true });
  } catch (e) {
    const msg = String(e?.message || e);

    errors.push({ pid: pidNum, error: msg });

    try {
      await logAutomationErr({
        pid: pidNum,
        appShowId: APP_SID,
        peopleShowId: null,
        appSqlDate: APP_SQL_DATE,
        errorType: "runtime_error",
        message: msg,
      });
    } catch (_) {}

    results.push({ pid: pidNum, skipped: true, reason: "runtime_error" });
  }
}

await runPool(pidList, pull_pip, processPid);

output.set("ok", errors.length === 0);
output.set("app_sid", APP_SID);
output.set("app_sql_date", APP_SQL_DATE);
output.set("shows_record_id", SHOWS_RECORD_ID);
output.set("pids", JSON.stringify(pidList));
output.set("run_id", RUN_ID);
output.set("last_run", LAST_RUN);
output.set("results", JSON.stringify(results.slice(0, 200)));
output.set("errors", JSON.stringify(errors.slice(0, 50)));
