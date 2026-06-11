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

async function airtableCreate(tableId, fields) {
  requireToken();
  const response = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ fields })
  });
  if (!response.ok) {
    throw new Error(`Airtable create ${tableId} failed: ${response.status} ${await response.text()}`);
  }
  return response.json();
}

function safeJson(value) {
  return JSON.stringify(value, null, 2).slice(0, 90000);
}

function appendLocalJsonl(fileName, payload) {
  fs.mkdirSync(logRoot, { recursive: true });
  fs.appendFileSync(path.join(logRoot, fileName), `${JSON.stringify(payload)}\n`, "utf8");
}

async function writeWecLog({ logType = "airtable_check", checkName, showNo = "", focusDay = "", status = "ok", recordsSeen = 0, recordsChanged = 0, summary = "", payload = {} }) {
  const createdAt = new Date().toISOString();
  const fields = {
    log_key: `${createdAt}|${logType}|${checkName}`,
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
  appendLocalJsonl("wec-logs.jsonl", fields);
  return airtableCreate(TABLES.wecLogs, fields);
}

async function writeWecAlert({ severity = "error", alertType, showNo = "", focusDay = "", message, payload = {} }) {
  const createdAt = new Date().toISOString();
  const fields = {
    alert_key: `${createdAt}|${severity}|${alertType}`,
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
    show_no: showNo,
    show_title: fields.name || fields.show_title || `Show ${showNo}`,
    show_start: normalizeDate(fields.show_start),
    show_end: normalizeDate(fields.show_end),
    focus_day: focusDay,
    source: fields.source || "airtable.focus_show"
  };
}

function normalizeClassHide(record) {
  const fields = record.fields || {};
  return {
    record_id: record.id,
    show_no: fields.show_no == null ? "" : String(fields.show_no),
    hide_text: fields.hide_text || "",
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
    show_title: row.show_title,
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

async function catalystPost(params, body, label) {
  const response = await fetch(`${CATALYST_ENDPOINT}?${params.toString()}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Catalyst ${label} failed: ${response.status} ${text}`);
  }
  return JSON.parse(text);
}

async function pushActiveTrainersToCatalyst(row, trainerRows) {
  const activeTrainers = trainerRows
    .filter((trainer) => trainer.show_no === row.show_no && trainer.active === "1")
    .map((trainer) => trainer.trainer)
    .filter(Boolean);
  const params = new URLSearchParams({
    action: "set-active-trainers",
    show_no: row.show_no,
    focus_day: row.focus_day,
    active_trainers: activeTrainers.join("|")
  });
  return catalystGet(params, "set-active-trainers");
}

async function pushHideClassesToCatalyst(row, hideRows) {
  const hideClasses = hideRows
    .filter((hide) => hide.show_no === row.show_no && hide.active === "1")
    .map((hide) => hide.hide_text)
    .filter(Boolean);
  const params = new URLSearchParams({
    action: "set-hide-classes",
    show_no: row.show_no,
    focus_day: row.focus_day,
    hide_classes: hideClasses.join("|")
  });
  return catalystGet(params, "set-hide-classes");
}

async function pushHorseDisplaysToCatalyst(row, horseRows) {
  const horseDisplays = {};
  const displayByKey = new Map();
  for (const horse of horseRows.filter((item) => item.show_no === row.show_no)) {
    const display = horse.barn_name || horse.horse_display || horse.horse;
    if (!horse.horse || !display) continue;

    const key = horse.horse.trim().toLowerCase();
    const existing = displayByKey.get(key);
    const score = (horse.barn_name ? 3 : 0) + (display && display !== horse.horse ? 2 : 0);
    if (!existing || score >= existing.score) {
      displayByKey.set(key, { display, score });
    }
  }
  for (const horse of horseRows.filter((item) => item.show_no === row.show_no)) {
    if (!horse.horse) continue;
    const preferred = displayByKey.get(horse.horse.trim().toLowerCase());
    if (preferred?.display && preferred.display !== horse.horse) horseDisplays[horse.horse] = preferred.display;
  }
  const params = new URLSearchParams({
    action: "set-horse-displays",
    show_no: row.show_no,
    focus_day: row.focus_day
  });
  return catalystPost(params, { horse_displays: JSON.stringify(horseDisplays) }, "set-horse-displays");
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
  const ringRecords = recordsByCheck.rings;
  const horseRecords = recordsByCheck.horses;
  const riderRecords = recordsByCheck.riders;
  const trainerRecords = recordsByCheck.trainers;
  const entryRecords = recordsByCheck.entries;

  const focusRows = focusRecords
    .map(normalizeFocusShow)
    .filter((row) => row.show_no && row.focus_day);
  const hideRows = hideRecords
    .map(normalizeClassHide)
    .filter((row) => row.show_no && row.hide_text);

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
      "show_no",
      "show_title",
      "show_start",
      "show_end",
      "focus_day",
      "source"
    ], focusRows.filter((row) => row.show_no === showNo));
    writeCsv(path.join(showDir, "class_hide.csv"), [
      "record_id",
      "show_no",
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
        catalystResults.push(await pushActiveTrainersToCatalyst(row, trainerRows));
        catalystResults.push(await pushHideClassesToCatalyst(row, hideRows));
        catalystResults.push(await pushHorseDisplaysToCatalyst(row, horseRows));
      }
    }
  }

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
    catalyst_synced: catalystResults.length
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
