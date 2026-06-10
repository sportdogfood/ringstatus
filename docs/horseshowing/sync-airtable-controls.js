const fs = require("fs");
const path = require("path");

const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || process.env.AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

const TABLES = {
  focusShow: "tblQldkP8wwIRxd4z",
  classHide: "tblOTmCatxsjV1f08",
  rings: "tbl5WKTbwL6IVrjyI",
  horses: "tblgWogH7B6Cvusvm",
  riders: "tbl75W08G7nB4MYAl",
  trainers: "tblB72MubQbWfEqdf",
  entries: "tblrRnqH6utOdyhSk"
};

const repoRoot = path.resolve(__dirname, "..", "..");
const helperRoot = path.join(__dirname, "helpers");

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
  const response = await fetch(`${CATALYST_ENDPOINT}?${params.toString()}`, { method: "GET" });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Catalyst set-show-config failed: ${response.status} ${text}`);
  }
  return JSON.parse(text);
}

async function main() {
  const syncCatalyst = !process.argv.includes("--no-catalyst");
  const [focusRecords, hideRecords, ringRecords, horseRecords, riderRecords, trainerRecords, entryRecords] = await Promise.all([
    airtableGetAll(TABLES.focusShow),
    airtableGetAll(TABLES.classHide),
    airtableGetAll(TABLES.rings),
    airtableGetAll(TABLES.horses),
    airtableGetAll(TABLES.riders),
    airtableGetAll(TABLES.trainers),
    airtableGetAll(TABLES.entries)
  ]);

  const focusRows = focusRecords
    .map(normalizeFocusShow)
    .filter((row) => row.show_no && row.focus_day);
  const hideRows = hideRecords
    .map(normalizeClassHide)
    .filter((row) => row.show_no && row.hide_text);

  const showNos = new Set([...focusRows.map((row) => row.show_no), ...hideRows.map((row) => row.show_no)]);
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
  }

  const catalystResults = [];
  if (syncCatalyst) {
    for (const row of focusRows) {
      catalystResults.push(await pushFocusShowToCatalyst(row));
    }
  }

  console.log(JSON.stringify({
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
  }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
