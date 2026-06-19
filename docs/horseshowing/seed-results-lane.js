const fs = require("node:fs");

const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function clean(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function bool(value) {
  return value === true || value === "true" || value === "1" || value === 1;
}

function intValue(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function key(...parts) {
  return parts.map(clean).filter(Boolean).join("|");
}

function rawJson(row) {
  return JSON.stringify(row || {}).slice(0, 90000);
}

function chunk(rows, size) {
  const chunks = [];
  for (let index = 0; index < rows.length; index += size) chunks.push(rows.slice(index, index + size));
  return chunks;
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${url} failed ${response.status}: ${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : {};
}

async function airtableFetch(table, options = {}) {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
  const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(table)}`);
  const response = await fetch(url, {
    method: options.method || "GET",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable ${table} failed ${response.status}: ${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : {};
}

async function upsertAirtable(table, mergeFields, rows) {
  let changed = 0;
  for (const batch of chunk(rows.filter((row) => mergeFields.every((field) => clean(row[field]))), 10)) {
    await airtableFetch(table, {
      method: "PATCH",
      body: {
        performUpsert: { fieldsToMergeOn: mergeFields },
        records: batch.map((fields) => ({ fields })),
        typecast: true
      }
    });
    changed += batch.length;
  }
  return changed;
}

function resultClassRow(showNo, focusDay, row) {
  const classNo = clean(row.class_no);
  const classNumber = clean(row.class_number);
  return {
    result_class_key: key(showNo, classNo || classNumber, row.sect_no),
    show_no: intValue(showNo),
    focus_day: focusDay,
    class_no: intValue(classNo),
    sect_no: intValue(row.sect_no),
    class_number: intValue(classNumber),
    class_name: clean(row.class_name),
    result_entry_count: intValue(row.result_entry_count || row.entry_count),
    has_score: bool(row.has_score),
    has_prize: bool(row.has_prize),
    completed_at: row.completed_at || new Date().toISOString(),
    source: row.source || "horseshowing.results",
    raw_json: rawJson(row)
  };
}

function resultQueueRow(showNo, focusDay, row) {
  const classNo = clean(row.class_no);
  const classNumber = clean(row.class_number);
  const now = new Date().toISOString();
  return {
    result_queue_key: key(showNo, focusDay, classNo || classNumber),
    show_no: intValue(showNo),
    focus_day: focusDay,
    class_no: intValue(classNo),
    sect_no: intValue(row.sect_no),
    class_number: intValue(classNumber),
    class_name: clean(row.class_name),
    status: "completed",
    queued_at: now,
    last_checked_at: now,
    attempts: 1,
    result_rows: intValue(row.result_entry_count || row.entry_count),
    completed_at: now,
    source: row.source || "horseshowing.results",
    raw_json: rawJson(row)
  };
}

function classResultRow(showNo, focusDay, row) {
  const classNo = clean(row.class_no);
  const classNumber = clean(row.class_number);
  const identity = key(row.entry_no, row.place, row.horse, row.rider);
  return {
    class_result_key: key(showNo, classNo || classNumber, identity),
    show_no: intValue(showNo),
    focus_day: focusDay,
    class_no: intValue(classNo),
    sect_no: intValue(row.sect_no),
    class_number: intValue(classNumber),
    class_name: clean(row.class_name),
    place: clean(row.place),
    entry_no: intValue(row.entry_no),
    horse: clean(row.horse),
    rider: clean(row.rider),
    owner: clean(row.owner),
    score: clean(row.score),
    prize: clean(row.prize),
    completed_at: row.completed_at || new Date().toISOString(),
    source: row.source || "horseshowing.results",
    raw_json: rawJson(row)
  };
}

async function seedCatalyst(showNo, focusDay, payload) {
  const url = `${CATALYST_ENDPOINT}?action=import-results&show_no=${encodeURIComponent(showNo)}`;
  const totals = {
    result_queue: { rows: 0, inserted: 0, updated: 0, skipped: 0 },
    result_classes: { rows: 0, inserted: 0, updated: 0, skipped: 0 },
    class_results: { rows: 0, inserted: 0, updated: 0, skipped: 0 }
  };
  const addTotals = (response) => {
    for (const table of Object.keys(totals)) {
      for (const field of Object.keys(totals[table])) {
        totals[table][field] += Number(response.imported?.[table]?.[field] || 0);
      }
    }
  };
  for (const classes of chunk(payload.classes || [], 50)) {
    addTotals(await postJson(url, { action: "import-results", show_no: showNo, focus_day: focusDay, classes, results: [] }));
  }
  for (const results of chunk(payload.results || [], 75)) {
    addTotals(await postJson(url, { action: "import-results", show_no: showNo, focus_day: focusDay, classes: [], results }));
  }
  return totals;
}

async function seedAirtable(showNo, focusDay, payload) {
  const classRows = (payload.classes || []).map((row) => resultClassRow(showNo, focusDay, row)).filter((row) => row.result_class_key);
  const queueRows = (payload.classes || []).map((row) => resultQueueRow(showNo, focusDay, row)).filter((row) => row.result_queue_key);
  const resultRows = (payload.results || []).map((row) => classResultRow(showNo, focusDay, row)).filter((row) => row.class_result_key);
  return {
    result_queue: await upsertAirtable("result_queue", ["result_queue_key"], queueRows),
    result_classes: await upsertAirtable("result_classes", ["result_class_key"], classRows),
    class_results: await upsertAirtable("class_results", ["class_result_key"], resultRows)
  };
}

async function main() {
  const file = argValue("--file");
  const focusDay = argValue("--focus-day");
  if (!file || !focusDay) throw new Error("Usage: node seed-results-lane.js --file <payload.json> --focus-day YYYY-MM-DD");
  const payload = JSON.parse(fs.readFileSync(file, "utf8"));
  const showNo = clean(argValue("--show-no", payload.show_no));
  if (!showNo) throw new Error("Missing show_no");
  const catalyst = await seedCatalyst(showNo, focusDay, payload);
  const airtable = await seedAirtable(showNo, focusDay, payload);
  console.log(JSON.stringify({ ok: true, show_no: showNo, focus_day: focusDay, catalyst, airtable }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
