const fs = require("fs");
const path = require("path");

const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

const TABLES = {
  focusShow: "focus_show",
  classHide: "class_hide",
  rings: "rings",
  horses: "horses",
  riders: "riders",
  trainers: "trainers",
  entries: "entries",
  wecLogs: "wec-logs",
  wecAlerts: "wec-alerts"
};

const repoRoot = path.resolve(__dirname, "..", "..");
const helperRoot = path.join(__dirname, "helpers");
const logRoot = path.join(__dirname, "logs");
const summaryStatePath = path.join(logRoot, "wec-airtable-summary-state.json");
const backfillStatePath = path.join(logRoot, "wec-airtable-helper-backfill-state.json");

function requireToken() {
  if (!AIRTABLE_TOKEN) {
    throw new Error("AIRTABLE_TOKEN is required");
  }
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function writeCsv(filePath, headers, rows) {
  const csv = [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => csvEscape(row[header])).join(","))
  ].join("\n");
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${csv}\n`);
}

async function airtableGetAll(tableId) {
  requireToken();
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`
      }
    });
    if (!response.ok) {
      throw new Error(`Airtable ${tableId} failed: ${response.status} ${await response.text()}`);
    }
    const payload = await response.json();
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset);
  return records;
}

async function airtableCreate(tableId, fields, options = {}) {
  requireToken();
  const response = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ fields, ...options })
  });
  if (!response.ok) {
    throw new Error(`Airtable create ${tableId} failed: ${response.status} ${await response.text()}`);
  }
  return response.json();
}

async function airtableBatchCreate(tableId, records) {
  requireToken();
  const created = [];
  for (let index = 0; index < records.length; index += 10) {
    const batch = records.slice(index, index + 10);
    const response = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ records: batch.map((fields) => ({ fields })) })
    });
    if (!response.ok) {
      throw new Error(`Airtable batch create ${tableId} failed: ${response.status} ${await response.text()}`);
    }
    const payload = await response.json();
    created.push(...(payload.records || []));
  }
  return created;
}

function safeJson(value) {
  return JSON.stringify(value, null, 2).slice(0, 90000);
}

function appendLocalJsonl(fileName, payload) {
  fs.mkdirSync(logRoot, { recursive: true });
  fs.appendFileSync(path.join(logRoot, fileName), `${JSON.stringify(payload)}\n`, "utf8");
}

function resolveWorkflowLane(logType, checkName) {
  if (logType === "airtable_check") return "Helpers";
  if (checkName === "airtable_helpers_summary" || checkName === "airtable_helper_backfill") return "Audits";
  return "";
}

async function writeWecLog({ logType = "airtable_check", checkName, showNo = "", focusDay = "", status = "ok", recordsSeen = 0, recordsChanged = 0, summary = "", payload = {} }) {
  const createdAt = new Date().toISOString();
  const workflowLane = resolveWorkflowLane(logType, checkName);
  const fields = {
    log_key_run: `${createdAt}|${logType}|${checkName}`,
    created_at: createdAt,
    log_type: logType,
    check_name: checkName,
    show_no: showNo,
    focus_day: focusDay || undefined,
    status,
    records_seen: recordsSeen,
    records_changed: recordsChanged,
    summary,
    payload_json: safeJson(payload)
  };
  if (workflowLane) {
    fields.workflow_lanes = workflowLane;
  }
  appendLocalJsonl("wec-logs.jsonl", fields);
  return airtableCreate(TABLES.wecLogs, fields, { typecast: true });
}

async function writeWecAlert({ severity = "error", alertType, showNo = "", focusDay = "", message, payload = {} }) {
  const createdAt = new Date().toISOString();
  const fields = {
    alert_key_run: `${createdAt}|${severity}|${alertType}`,
    created_at: createdAt,
    severity,
    status: "open",
    alert_type: alertType,
    show_no: showNo,
    focus_day: focusDay || undefined,
    message,
    payload_json: safeJson(payload)
  };
  appendLocalJsonl("wec-alerts.jsonl", fields);
  return airtableCreate(TABLES.wecAlerts, fields);
}

async function resolveOpenAlertsByType(alertType, message, payload = {}) {
  requireToken();
  const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${TABLES.wecAlerts}`);
  url.searchParams.set("pageSize", "100");
  url.searchParams.set("filterByFormula", `AND({status}='open', {alert_type}='${alertType}')`);
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`
    }
  });
  if (!response.ok) {
    throw new Error(`Airtable resolve query ${TABLES.wecAlerts} failed: ${response.status} ${await response.text()}`);
  }
  const result = await response.json();
  const records = result.records || [];
  for (let index = 0; index < records.length; index += 10) {
    const batch = records.slice(index, index + 10);
    const patch = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${TABLES.wecAlerts}`, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        records: batch.map((record) => ({
          id: record.id,
          fields: {
            status: "resolved",
            message,
            payload_json: safeJson({
              ...payload,
              previous_alert_key_run: record.fields.alert_key_run,
              resolved_reason: "successful_current_controls_check"
            })
          }
        }))
      })
    });
    if (!patch.ok) {
      throw new Error(`Airtable resolve patch ${TABLES.wecAlerts} failed: ${patch.status} ${await patch.text()}`);
    }
  }
  return records.length;
}

function readSummaryState() {
  try {
    return JSON.parse(fs.readFileSync(summaryStatePath, "utf8"));
  } catch {
    return {};
  }
}

function writeSummaryState(state) {
  fs.mkdirSync(path.dirname(summaryStatePath), { recursive: true });
  fs.writeFileSync(summaryStatePath, `${JSON.stringify(state, null, 2)}\n`);
}

function readBackfillState() {
  try {
    return JSON.parse(fs.readFileSync(backfillStatePath, "utf8"));
  } catch {
    return {};
  }
}

function writeBackfillState(state) {
  fs.mkdirSync(path.dirname(backfillStatePath), { recursive: true });
  fs.writeFileSync(backfillStatePath, `${JSON.stringify(state, null, 2)}\n`);
}

function backfillDue(showNo, focusDay, minutes = 60) {
  if (process.argv.includes("--force-backfill")) return true;
  const state = readBackfillState();
  const key = `${showNo}|${focusDay}`;
  if (state.last_focus_key !== key) return true;
  const last = state.last_backfill_at ? new Date(state.last_backfill_at) : null;
  if (!last || Number.isNaN(last.getTime())) return true;
  return Date.now() - last.getTime() >= minutes * 60 * 1000;
}

function markBackfillRun(showNo, focusDay, summary) {
  const now = new Date().toISOString();
  writeBackfillState({
    last_focus_key: `${showNo}|${focusDay}`,
    last_show_no: showNo,
    last_focus_day: focusDay,
    last_backfill_at: now,
    last_summary: summary
  });
}

function summaryDue(minutes = 30) {
  const state = readSummaryState();
  const last = state.last_summary_at ? new Date(state.last_summary_at) : null;
  if (!last || Number.isNaN(last.getTime())) return true;
  return Date.now() - last.getTime() >= minutes * 60 * 1000;
}

async function writeThirtyMinuteSummary(summary) {
  if (!summaryDue()) return false;
  fs.mkdirSync(logRoot, { recursive: true });
  const createdAt = new Date().toISOString();
  const filePath = path.join(logRoot, "wec-logs-30m-summary.jsonl");
  fs.appendFileSync(filePath, `${JSON.stringify({ created_at: createdAt, ...summary })}\n`, "utf8");
  await writeWecLog({
    logType: "summary_30m",
    checkName: "airtable_helpers_summary",
    showNo: summary.focus_show?.[0]?.show_no || "",
    focusDay: summary.focus_show?.[0]?.focus_day || "",
    status: "ok",
    recordsSeen: summary.total_records_seen,
    summary: `focus_show=${summary.counts.focus_show}; class_hide=${summary.counts.class_hide}; rings=${summary.counts.rings}; horses=${summary.counts.horses}; riders=${summary.counts.riders}; trainers=${summary.counts.trainers}; entries=${summary.counts.entries}`,
    payload: summary
  });
  writeSummaryState({ last_summary_at: createdAt });
  return true;
}

function normalizeDate(value) {
  if (!value) return "";
  return String(value).slice(0, 10);
}

function normalizeFocusShow(record) {
  const fields = record.fields || {};
  const showNo = fields.show_no == null ? "" : String(fields.show_no);
  const focusDay = normalizeDate(fields.focus_day);
  return {
    record_id: record.id,
    focus_show_key: fields.focus_show_key || `${showNo}|${focusDay}`,
    mirror_focus_show_key: fields.mirror_focus_show_key || fields.focus_show_key || `${showNo}|${focusDay}`,
    show_no: showNo,
    show_name: fields.show_name || fields.name || `Show ${showNo}`,
    subtitle: focusDay,
    show_start: normalizeDate(fields.show_start),
    show_end: normalizeDate(fields.show_end),
    focus_day: focusDay,
    source: fields.source || "airtable.focus_show"
  };
}

function normalizeClassHide(record) {
  const fields = record.fields || {};
  const showNo = fields.show_no == null ? "" : String(fields.show_no);
  const classNo = fields.class_no == null ? "" : String(fields.class_no).replace(/\.0$/, "");
  const hideText = fields.hide_text || fields.hide_lib || fields.name || "";
  const keyRule = classNo ? `class_no:${classNo}` : `text:${hideText.trim().toLowerCase()}`;
  return {
    record_id: record.id,
    class_hide_key: fields.class_hide_key || `${showNo}|${keyRule}`,
    mirror_class_hide_key: fields.mirror_class_hide_key || fields.class_hide_key || `${showNo}|${keyRule}`,
    show_no: showNo,
    class_no: classNo,
    hide_text: hideText,
    active: fields.active ? "1" : "0"
  };
}

function normalizeRing(record, showNo) {
  const fields = record.fields || {};
  return {
    record_id: record.id,
    show_no: showNo,
    ring_no: fields.ring_no == null ? "" : String(fields.ring_no),
    ring_name: fields.ring_name || "",
    ring_display: fields.ring_display || fields.ring_name || "",
    priority: fields.priority == null ? "" : String(fields.priority),
    active: "1"
  };
}

function normalizeHorse(record, showNo) {
  const fields = record.fields || {};
  return {
    record_id: record.id,
    show_no: showNo,
    horse: fields.horse || "",
    horse_display: fields.horse_display || fields.horse || "",
    barn_name: fields.barn_name || "",
    tag: fields.tag || "",
    active: "1"
  };
}

function normalizeEntry(record, showNo) {
  const fields = record.fields || {};
  return {
    record_id: record.id,
    show_no: showNo,
    entry_no: fields.entry_no == null ? "" : String(fields.entry_no),
    entry_display: fields.entry_no == null ? "" : String(fields.entry_no),
    horse: fields.horse || "",
    horse_display: Array.isArray(fields["horse_display (from horses)"])
      ? fields["horse_display (from horses)"].join(", ")
      : fields["horse_display (from horses)"] || fields.horse || "",
    rider: fields.rider || "",
    rider_display: Array.isArray(fields["rider_display (from riders)"])
      ? fields["rider_display (from riders)"].join(", ")
      : fields["rider_display (from riders)"] || fields.rider || "",
    trainer: fields.trainer || "",
    trainer_display: Array.isArray(fields["trainer_display (from trainers)"])
      ? fields["trainer_display (from trainers)"].join(", ")
      : fields["trainer_display (from trainers)"] || fields.trainer || "",
    active: "1"
  };
}

function normalizeRider(record, showNo) {
  const fields = record.fields || {};
  return {
    record_id: record.id,
    show_no: showNo,
    rider: fields.rider || "",
    rider_display: fields.rider_display || fields.rider || "",
    tag: fields.tag || "",
    active: "1"
  };
}

function normalizeTrainer(record, showNo) {
  const fields = record.fields || {};
  return {
    record_id: record.id,
    show_no: showNo,
    trainer: fields.trainer || "",
    trainer_display: fields.trainer_display || fields.trainer || "",
    tag: fields.tag || "",
    active: fields.active ? "1" : "0"
  };
}

async function pushFocusShowToCatalyst(row) {
  const params = new URLSearchParams({
    action: "set-show-config",
    show_no: row.show_no,
    show_title: row.show_name,
    show_start_date: row.show_start,
    show_end_date: row.show_end,
    focus_day: row.focus_day
  });
  return catalystGet(params, "set-show-config");
}

async function catalystGet(params, label) {
  const response = await fetch(`${CATALYST_ENDPOINT}?${params.toString()}`, { method: "GET" });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Catalyst ${label} failed: ${response.status} ${text}`);
  }
  return JSON.parse(text);
}

async function catalystPost(payload, label) {
  const params = new URLSearchParams();
  for (const key of ["action", "show_no", "focus_day"]) {
    if (payload[key] !== undefined && payload[key] !== null && payload[key] !== "") {
      params.set(key, String(payload[key]));
    }
  }
  const response = await fetch(`${CATALYST_ENDPOINT}?${params.toString()}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Catalyst ${label} failed: ${response.status} ${text}`);
  }
  return JSON.parse(text);
}

async function catalystSnapshot(showNo, focusDay) {
  const params = new URLSearchParams({
    action: "focus-day-snapshot",
    show_no: showNo,
    focus_day: focusDay
  });
  return catalystGet(params, "focus-day-snapshot");
}

function keySet(records, fieldName) {
  return new Set(records
    .map((record) => record.fields?.[fieldName])
    .filter((value) => value !== null && value !== undefined && value !== "")
    .map((value) => String(value).trim().toLowerCase())
    .filter(Boolean));
}

function addUnique(map, key, fields) {
  const cleanKey = String(key ?? "").trim();
  if (!cleanKey) return;
  const normalized = cleanKey.toLowerCase();
  if (!map.has(normalized)) map.set(normalized, fields);
}

function firstText(...values) {
  for (const value of values) {
    const clean = String(value ?? "").trim();
    if (clean) return clean;
  }
  return "";
}

function numberOrUndefined(value) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

async function backfillAirtableHelpersFromCatalyst(row, existingRecords) {
  if (!backfillDue(row.show_no, row.focus_day, 60)) {
    return {
      skipped: true,
      reason: "not_due",
      created: { rings: 0, horses: 0, riders: 0, trainers: 0, entries: 0 }
    };
  }

  const snapshot = await catalystSnapshot(row.show_no, row.focus_day);
  if (!snapshot.ok) throw new Error(`Catalyst focus-day-snapshot failed for ${row.show_no} ${row.focus_day}`);

  const classOog = Array.isArray(snapshot.class_oog) ? snapshot.class_oog : [];
  const updateSchedule = Array.isArray(snapshot.update_schedule) ? snapshot.update_schedule : [];
  const helperHorses = Array.isArray(snapshot.helpers?.horses) ? snapshot.helpers.horses : [];
  const helperRiders = Array.isArray(snapshot.helpers?.riders) ? snapshot.helpers.riders : [];
  const helperTrainers = Array.isArray(snapshot.helpers?.trainers) ? snapshot.helpers.trainers : [];

  const existing = {
    rings: keySet(existingRecords.rings, "ring_no"),
    horses: keySet(existingRecords.horses, "horse"),
    riders: keySet(existingRecords.riders, "rider"),
    trainers: keySet(existingRecords.trainers, "trainer"),
    entries: keySet(existingRecords.entries, "entry_no")
  };

  const candidates = {
    rings: new Map(),
    horses: new Map(),
    riders: new Map(),
    trainers: new Map(),
    entries: new Map()
  };

  for (const item of [...updateSchedule, ...classOog]) {
    const ringNo = firstText(item.ring_no);
    if (ringNo && !existing.rings.has(ringNo.toLowerCase())) {
      addUnique(candidates.rings, ringNo, {
        ring_no: numberOrUndefined(ringNo),
        ring_name: firstText(item.ring_name, item.ring),
        ring_display: firstText(item.ring_name, item.ring),
        source: "get_ring_days.php"
      });
    }
  }

  for (const item of [...classOog, ...helperHorses]) {
    const horse = firstText(item.horse);
    if (horse && !existing.horses.has(horse.toLowerCase())) {
      addUnique(candidates.horses, horse, {
        horse,
        horse_display: horse,
        rider: firstText(item.rider),
        trainer: firstText(item.trainer),
        source: "class_oog.php"
      });
    }
  }

  for (const item of [...classOog, ...helperRiders]) {
    const rider = firstText(item.rider);
    if (rider && !existing.riders.has(rider.toLowerCase())) {
      addUnique(candidates.riders, rider, {
        rider,
        horse: firstText(item.horse),
        trainer: firstText(item.trainer),
        source: "class_oog.php"
      });
    }
  }

  for (const item of [...classOog, ...helperTrainers]) {
    const trainer = firstText(item.trainer);
    if (trainer && !existing.trainers.has(trainer.toLowerCase())) {
      addUnique(candidates.trainers, trainer, {
        trainer,
        trainer_display: trainer,
        source: "class_oog.php"
      });
    }
  }

  for (const item of classOog) {
    const entryNo = firstText(item.entry_no);
    if (entryNo && !existing.entries.has(entryNo.toLowerCase())) {
      addUnique(candidates.entries, entryNo, {
        entry_no: numberOrUndefined(entryNo),
        horse: firstText(item.horse),
        rider: firstText(item.rider),
        trainer: firstText(item.trainer),
        source: "class_oog.php"
      });
    }
  }

  const created = {};
  for (const tableName of ["rings", "horses", "riders", "trainers", "entries"]) {
    const rows = [...candidates[tableName].values()].filter((fields) => Object.keys(fields).length);
    created[tableName] = rows.length ? (await airtableBatchCreate(TABLES[tableName], rows)).length : 0;
  }

  const summary = {
    show_no: row.show_no,
    focus_day: row.focus_day,
    source_counts: {
      update_schedule: updateSchedule.length,
      class_oog: classOog.length,
      helper_horses: helperHorses.length,
      helper_riders: helperRiders.length,
      helper_trainers: helperTrainers.length
    },
    created
  };
  markBackfillRun(row.show_no, row.focus_day, summary);
  return { skipped: false, ...summary };
}

async function pushActiveTrainersToCatalyst(row, trainerRows) {
  const activeTrainerRows = trainerRows
    .filter((trainer) => trainer.active === "1" && trainer.trainer);
  const activeTrainers = activeTrainerRows.map((trainer) => trainer.trainer);
  const trainerDisplays = {};
  for (const trainer of activeTrainerRows) {
    trainerDisplays[trainer.trainer] = trainer.trainer_display || trainer.trainer;
  }
  const params = new URLSearchParams({
    action: "set-active-trainers",
    show_no: row.show_no,
    focus_day: row.focus_day,
    active_trainers: activeTrainers.join("|"),
    trainer_displays: JSON.stringify(trainerDisplays)
  });
  return catalystGet(params, "set-active-trainers");
}

function activeTrainerControl(trainerRows) {
  const activeTrainerRows = trainerRows.filter((trainer) => trainer.active === "1" && trainer.trainer);
  const trainerDisplays = {};
  for (const trainer of activeTrainerRows) {
    trainerDisplays[trainer.trainer] = trainer.trainer_display || trainer.trainer;
  }
  return {
    active_trainers: activeTrainerRows.map((trainer) => trainer.trainer),
    trainer_displays: trainerDisplays
  };
}

async function pushHideClassesToCatalyst(row, hideRows, trainerRows = []) {
  const hideClasses = [];
  for (const hide of hideRows.filter((item) => item.show_no === row.show_no && item.active === "1")) {
    if (hide.class_no) hideClasses.push(`class_no:${hide.class_no}`);
    if (hide.hide_text) hideClasses.push(`text:${hide.hide_text}`);
  }
  const trainerControl = activeTrainerControl(trainerRows);
  const params = new URLSearchParams({
    action: "set-hide-classes",
    show_no: row.show_no,
    focus_day: row.focus_day,
    hide_classes: hideClasses.join("|"),
    active_trainers: trainerControl.active_trainers.join("|"),
    trainer_displays: JSON.stringify(trainerControl.trainer_displays)
  });
  return catalystGet(params, "set-hide-classes");
}

async function pushHorseDisplaysToCatalyst(row, horseRows, entryRows, trainerRows) {
  const showNo = String(row.show_no || "");
  const trainerControl = activeTrainerControl(trainerRows);
  const activeTrainers = new Set(
    trainerControl.active_trainers
      .map((trainer) => trainer.toLowerCase())
  );
  const scopedHorseNames = new Set(
    entryRows
      .filter((entry) => String(entry.show_no || "") === showNo && entry.horse && activeTrainers.has(String(entry.trainer || "").toLowerCase()))
      .map((entry) => entry.horse.toLowerCase())
  );
  const displays = {};
  const meta = {};
  for (const horse of horseRows.filter((item) => (
    String(item.show_no || "") === showNo &&
    item.horse &&
    scopedHorseNames.has(item.horse.toLowerCase())
  ))) {
    const showName = horse.horse;
    const barnName = horse.barn_name || "";
    const display = barnName || horse.horse_display || showName;
    displays[showName] = display;
    meta[showName] = {
      barn_name: barnName,
      barn_name_missing: !barnName
    };
  }
  return catalystPost({
    action: "set-horse-displays",
    show_no: row.show_no,
    focus_day: row.focus_day,
    horse_displays: displays,
    horse_display_meta: meta,
    active_trainers: trainerControl.active_trainers.join("|"),
    trainer_displays: trainerControl.trainer_displays
  }, "set-horse-displays");
}

async function main() {
  const syncCatalyst = !process.argv.includes("--no-catalyst");
  const checks = [
    ["focus_show", TABLES.focusShow],
    ["class_hide", TABLES.classHide],
    ["rings", TABLES.rings],
    ["horses", TABLES.horses],
    ["riders", TABLES.riders],
    ["trainers", TABLES.trainers],
    ["entries", TABLES.entries]
  ];
  const recordsByCheck = {};
  for (const [checkName, tableId] of checks) {
    const records = await airtableGetAll(tableId);
    recordsByCheck[checkName] = records;
    await writeWecLog({
      checkName,
      status: "ok",
      recordsSeen: records.length,
      summary: `${checkName} checked`,
      payload: { table_id: tableId, records_seen: records.length }
    });
  }

  const focusRecords = recordsByCheck.focus_show;
  const hideRecords = recordsByCheck.class_hide;
  let ringRecords = recordsByCheck.rings;
  let horseRecords = recordsByCheck.horses;
  let riderRecords = recordsByCheck.riders;
  let trainerRecords = recordsByCheck.trainers;
  let entryRecords = recordsByCheck.entries;

  const focusRows = focusRecords
    .map(normalizeFocusShow)
    .filter((row) => row.show_no && row.focus_day);
  const hideRows = hideRecords
    .map(normalizeClassHide)
    .filter((row) => row.show_no && (row.class_no || row.hide_text));

  const backfillResults = [];
  for (const row of focusRows) {
    const result = await backfillAirtableHelpersFromCatalyst(row, {
      rings: ringRecords,
      horses: horseRecords,
      riders: riderRecords,
      trainers: trainerRecords,
      entries: entryRecords
    });
    backfillResults.push(result);
    const totalCreated = Object.values(result.created || {}).reduce((sum, count) => sum + Number(count || 0), 0);
    await writeWecLog({
      logType: "heartbeat",
      checkName: "airtable_helper_backfill",
      showNo: row.show_no,
      focusDay: row.focus_day,
      status: result.skipped ? "skipped" : "ok",
      recordsSeen: result.source_counts ? Object.values(result.source_counts).reduce((sum, count) => sum + Number(count || 0), 0) : 0,
      recordsChanged: totalCreated,
      summary: result.skipped
        ? `helper backfill skipped: ${result.reason}`
        : `helper backfill created rings=${result.created.rings}; horses=${result.created.horses}; riders=${result.created.riders}; trainers=${result.created.trainers}; entries=${result.created.entries}`,
      payload: result
    });
  }

  if (backfillResults.some((result) => Object.values(result.created || {}).some((count) => Number(count || 0) > 0))) {
    ringRecords = await airtableGetAll(TABLES.rings);
    horseRecords = await airtableGetAll(TABLES.horses);
    riderRecords = await airtableGetAll(TABLES.riders);
    trainerRecords = await airtableGetAll(TABLES.trainers);
    entryRecords = await airtableGetAll(TABLES.entries);
  }

  const showNos = new Set([...focusRows.map((row) => row.show_no), ...hideRows.map((row) => row.show_no)]);
  const catalystResults = [];
  for (const showNo of showNos) {
    const showDir = path.join(helperRoot, showNo);
    const ringRows = ringRecords
      .map((record) => normalizeRing(record, showNo))
      .filter((row) => row.ring_no);
    const horseRows = horseRecords
      .map((record) => normalizeHorse(record, showNo))
      .filter((row) => row.horse);
    const riderRows = riderRecords
      .map((record) => normalizeRider(record, showNo))
      .filter((row) => row.rider);
    const trainerRows = trainerRecords
      .map((record) => normalizeTrainer(record, showNo))
      .filter((row) => row.trainer);
    const entryRows = entryRecords
      .map((record) => normalizeEntry(record, showNo))
      .filter((row) => row.entry_no);

    writeCsv(path.join(showDir, "focus_show.csv"), [
      "record_id",
      "focus_show_key",
      "mirror_focus_show_key",
      "show_no",
      "show_name",
      "subtitle",
      "show_start",
      "show_end",
      "focus_day",
      "source"
    ], focusRows.filter((row) => row.show_no === showNo));
    writeCsv(path.join(showDir, "class_hide.csv"), [
      "record_id",
      "class_hide_key",
      "mirror_class_hide_key",
      "show_no",
      "class_no",
      "hide_text",
      "active"
    ], hideRows.filter((row) => row.show_no === showNo));
    writeCsv(path.join(showDir, "rings.csv"), [
      "record_id",
      "show_no",
      "ring_no",
      "ring_name",
      "ring_display",
      "priority",
      "active"
    ], ringRows);
    writeCsv(path.join(showDir, "horses.csv"), [
      "record_id",
      "show_no",
      "horse",
      "horse_display",
      "barn_name",
      "tag",
      "active"
    ], horseRows);
    writeCsv(path.join(showDir, "entries.csv"), [
      "record_id",
      "show_no",
      "entry_no",
      "entry_display",
      "horse",
      "horse_display",
      "rider",
      "rider_display",
      "trainer",
      "trainer_display",
      "active"
    ], entryRows);
    writeCsv(path.join(showDir, "riders.csv"), [
      "record_id",
      "show_no",
      "rider",
      "rider_display",
      "tag",
      "active"
    ], riderRows);
    writeCsv(path.join(showDir, "trainers.csv"), [
      "record_id",
      "show_no",
      "trainer",
      "trainer_display",
      "tag",
      "active"
    ], trainerRows);

    if (syncCatalyst) {
      for (const row of focusRows.filter((item) => item.show_no === showNo)) {
        catalystResults.push(await pushFocusShowToCatalyst(row));
        catalystResults.push(await pushHideClassesToCatalyst(row, hideRows, trainerRows));
        catalystResults.push(await pushHorseDisplaysToCatalyst(row, horseRows, entryRows, trainerRows));
        catalystResults.push(await pushActiveTrainersToCatalyst(row, trainerRows));
      }
    }
  }

  const resolvedControlFailures = await resolveOpenAlertsByType(
    "airtable_controls_check_failed",
    "Resolved: current Airtable controls sync completed successfully.",
    {
      focus_show_rows: focusRows.length,
      class_hide_rows: hideRows.length,
      rings_rows: ringRecords.length,
      horses_rows: horseRecords.length,
      riders_rows: riderRecords.length,
      trainers_rows: trainerRecords.length,
      entries_rows: entryRecords.length,
      catalyst_synced: catalystResults.length
    }
  );
  const resolvedReliabilityWarnings = await resolveOpenAlertsByType(
    "airtable_connection_reliability",
    "Resolved: current Airtable controls read/write path completed successfully.",
    {
      focus_show_rows: focusRows.length,
      class_hide_rows: hideRows.length,
      rings_rows: ringRecords.length,
      horses_rows: horseRecords.length,
      riders_rows: riderRecords.length,
      trainers_rows: trainerRecords.length,
      entries_rows: entryRecords.length,
      catalyst_synced: catalystResults.length
    }
  );

  const output = {
    base_id: BASE_ID,
    focus_show_rows: focusRows.length,
    class_hide_rows: hideRows.length,
    rings_rows: ringRecords.length,
    horses_rows: horseRecords.length,
    riders_rows: riderRecords.length,
    trainers_rows: trainerRecords.length,
    entries_rows: entryRecords.length,
    helper_root: helperRoot,
    catalyst_synced: catalystResults.length,
    resolved_control_failures: resolvedControlFailures,
    resolved_reliability_warnings: resolvedReliabilityWarnings,
    backfill: backfillResults
  };
  output.summary_written = await writeThirtyMinuteSummary({
    base_id: BASE_ID,
    total_records_seen: focusRecords.length + hideRecords.length + ringRecords.length + horseRecords.length + riderRecords.length + trainerRecords.length + entryRecords.length,
    counts: {
      focus_show: focusRows.length,
      class_hide: hideRows.length,
      rings: ringRecords.length,
      horses: horseRecords.length,
      riders: riderRecords.length,
      trainers: trainerRecords.length,
      entries: entryRecords.length
    },
    focus_show: focusRows,
    catalyst_synced: catalystResults.length
  });

  console.log(JSON.stringify(output, null, 2));
}

main().catch(async (error) => {
  try {
    await writeWecAlert({
      alertType: "airtable_controls_check_failed",
      message: error.message,
      payload: { stack: error.stack }
    });
  } catch {
    // Preserve the original failure for the runner.
  }
  console.error(error.message);
  process.exit(1);
});
