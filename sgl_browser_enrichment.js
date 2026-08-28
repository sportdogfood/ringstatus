const { chromium } = require("playwright");

const AIRTABLE_API = "https://api.airtable.com/v0";
const TOKEN = process.env.AIRTABLE_TOKEN;
const SCHEDULE_BASE = process.env.SGL_SCHEDULE_BASE_ID || process.env.AIRTABLE_BASE_ID;
const OOG_BASE = process.env.SGL_OOG_BASE_ID || "apptdhhNzduxm5gjn";
const SCHEDULE_TABLE = process.env.SGL_SCHEDULE_TABLE || "watch_schedule";
const SCHEDULE_VIEW = process.env.SGL_SCHEDULE_VIEW || "heartbeat";
const OOG_TABLE = process.env.SGL_OOG_TABLE || "watch_trips";
const OOG_VIEW = process.env.SGL_OOG_VIEW || "scrape-oog";
const ERR_TABLE = process.env.TABLE_AUTOMATION_ERRS || "automation_errs";
const SHOW_ID = process.env.SGL_SHOW_ID || "";
const FOCUS_DAY = process.env.SGL_FOCUS_DAY || "";
const DRY_RUN = process.env.DRY_RUN === "1";
const MAX_RECORDS = Number(process.env.SGL_MAX_RECORDS || "0");
const CHROME_PATH = process.env.CHROME_PATH || "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const TIME_RE = /\b(?:0?[1-9]|1[0-2])(?::[0-5]\d)?\s*(?:AM|PM)\b/i;

function text(value) { return String(value ?? "").replace(/\s+/g, " ").trim(); }
function number(value) { const n = Number(String(value ?? "").replace(/[^0-9.-]/g, "")); return Number.isFinite(n) ? n : null; }
function field(fields, ...names) { for (const name of names) if (fields[name] != null && text(fields[name]) !== "") return fields[name]; return null; }
function normalizeTime(value) {
  const match = text(value).match(TIME_RE); if (!match) return null;
  const parts = match[0].replace(/\s+/g, " ").toUpperCase().split(/[: ]/);
  let hour = Number(parts[0]); const minute = /^\d+$/.test(parts[1] || "") ? Number(parts[1]) : 0;
  if (parts.at(-1) === "PM" && hour !== 12) hour += 12; if (parts.at(-1) === "AM" && hour === 12) hour = 0;
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00`;
}
function contains(value, needle) { return needle && text(value).toLowerCase().includes(text(needle).toLowerCase()); }

async function airtableList(base, table, params = {}) {
  const records = []; let offset = "";
  do {
    const query = new URLSearchParams({ pageSize: "100", ...params }); if (offset) query.set("offset", offset);
    const response = await fetch(`${AIRTABLE_API}/${base}/${encodeURIComponent(table)}?${query}`, { headers: { Authorization: `Bearer ${TOKEN}` } });
    const body = await response.text(); if (!response.ok) throw new Error(`Airtable ${response.status}: ${body.slice(0, 500)}`);
    const payload = body ? JSON.parse(body) : {}; records.push(...(payload.records || [])); offset = payload.offset || "";
  } while (offset); return records;
}
async function airtableWrite(base, table, records) {
  for (let i = 0; i < records.length; i += 10) {
    const response = await fetch(`${AIRTABLE_API}/${base}/${encodeURIComponent(table)}`, { method: "PATCH", headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }, body: JSON.stringify({ records: records.slice(i, i + 10), typecast: true }) });
    const body = await response.text(); if (!response.ok) throw new Error(`Airtable PATCH ${response.status}: ${body.slice(0, 500)}`);
  }
  return records.length;
}
async function logError(message, errorType, showId) {
  if (DRY_RUN) return;
  try { await airtableWrite(OOG_BASE, ERR_TABLE, [{ fields: { automation_name: "sgl_browser_enrichment", error_type: errorType, message: text(message).slice(0, 1000), app_show_id: number(showId), last_run: new Date().toISOString(), resolved: false } }]); } catch (error) { console.error(`automation_errs write failed: ${error.message}`); }
}
async function loadRenderedPage(browser, url) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 }); await page.waitForTimeout(Number(process.env.SGL_BROWSER_WAIT_MS || 5000));
  const root = page.locator("#sgl-root"); const rendered = text(await (await root.count() ? root : page.locator("body")).innerText());
  if (!rendered) throw new Error("rendered page was empty");
  const rows = (await page.locator("#sgl-root tr, #sgl-root li").allInnerTexts().catch(() => [])).map(text).filter(Boolean);
  return { page, rendered, rows };
}
function scheduleMatch(record, rows, rendered) {
  const f = record.fields || {}; const cls = field(f, "class_number", "class_no"); const name = field(f, "class_name", "class_label", "group_name");
  const candidates = rows.filter(row => (cls && contains(row, cls)) || (name && contains(row, name)));
  for (const row of candidates) { const estimated = normalizeTime(row); if (estimated) return { estimated, evidence: row }; }
  const broad = cls && rendered.match(new RegExp(`.{0,180}${String(cls).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}.{0,180}`, "i"));
  return broad && normalizeTime(broad[0]) ? { estimated: normalizeTime(broad[0]), evidence: broad[0] } : null;
}
function oogMatch(record, rows) {
  const f = record.fields || {}; const entry = text(field(f, "entry_number", "entry_no")); const cls = field(f, "class_number", "class_no"); const rider = field(f, "rider_name", "rider"); const horse = field(f, "horse", "horse_name");
  const entryRows = rows.filter(row => entry && new RegExp(`\\b${entry.replace(/\D/g, "")}\\b`).test(row));
  const exactClassRows = cls ? entryRows.filter(row => new RegExp(`\\b${String(cls).replace(/\D/g, "")}\\b`).test(row)) : [];
  const identityRows = entryRows.filter(row => (rider && contains(row, rider)) || (horse && contains(row, horse)));
  const candidates = exactClassRows.length ? exactClassRows : identityRows;
  if (candidates.length !== 1) return candidates.length ? { ambiguous: candidates.slice(0, 3) } : null;
  const values = [...candidates[0].matchAll(/\b\d+\b/g)].map(m => Number(m[0])); const order = values.find(v => v !== Number(entry)) ?? values[0];
  return Number.isFinite(order) ? { order, evidence: candidates[0] } : null;
}
async function runSchedule(browser) {
  if (!SCHEDULE_BASE) throw new Error("SGL_SCHEDULE_BASE_ID or AIRTABLE_BASE_ID is required");
  const records = await airtableList(SCHEDULE_BASE, SCHEDULE_TABLE, { view: SCHEDULE_VIEW }); const scopedRecords = MAX_RECORDS > 0 ? records.slice(0, MAX_RECORDS) : records; const updates = [], unmatched = [];
  for (const record of scopedRecords) { const f = record.fields || {}, sid = field(f, "sid", "show_id", "app_show_id"), date = field(f, "dt", "date", "schedule_date", "show_date", "schedule_show_datev2", "app_sql_date", "app_sql_datev2") || FOCUS_DAY; if (SHOW_ID && String(sid) !== String(SHOW_ID) || FOCUS_DAY && String(date) !== String(FOCUS_DAY)) continue;
    const url = `https://www.wellingtoninternational.com/showgrounds/show-schedule/?date=${encodeURIComponent(date)}&sid=${encodeURIComponent(sid)}`;
    try { const loaded = await loadRenderedPage(browser, url); const match = scheduleMatch(record, loaded.rows, loaded.rendered); if (match) updates.push({ id: record.id, fields: { estimated_start_time: match.estimated } }); else unmatched.push({ id: record.id, reason: "schedule_time_not_found", url }); await loaded.page.close(); }
    catch (error) { unmatched.push({ id: record.id, reason: error.message, url }); await logError(`${url}: ${error.message}`, "schedule_scrape", sid); }
  }
  return { lane: "schedule", records: scopedRecords.length, matched: updates.length, written: DRY_RUN ? 0 : await airtableWrite(SCHEDULE_BASE, SCHEDULE_TABLE, updates), unmatched: unmatched.length, unmatched_samples: unmatched.slice(0, 5) };
}
async function runOog(browser) {
  const records = await airtableList(OOG_BASE, OOG_TABLE, { view: OOG_VIEW }); const scopedRecords = MAX_RECORDS > 0 ? records.slice(0, MAX_RECORDS) : records; const updates = [], unmatched = [];
  for (const record of scopedRecords) { const f = record.fields || {}, url = field(f, "classsignup_url_viewsetorder_scrape"); if (!url) { unmatched.push({ id: record.id, reason: "missing_url" }); continue; }
    try { const loaded = await loadRenderedPage(browser, url); const match = oogMatch(record, loaded.rows); if (match?.order !== undefined) updates.push({ id: record.id, fields: { order_of_go: match.order } }); else unmatched.push({ id: record.id, reason: match?.ambiguous ? "ambiguous_match" : "order_not_found", url }); await loaded.page.close(); }
    catch (error) { unmatched.push({ id: record.id, reason: error.message, url }); await logError(`${url}: ${error.message}`, "oog_scrape", field(f, "sid", "show_id")); }
  }
  return { lane: "oog", records: scopedRecords.length, matched: updates.length, written: DRY_RUN ? 0 : await airtableWrite(OOG_BASE, OOG_TABLE, updates), unmatched: unmatched.length, unmatched_samples: unmatched.slice(0, 5) };
}
(async () => { if (!TOKEN) throw new Error("AIRTABLE_TOKEN is required"); const args = new Set(process.argv.slice(2)); const browser = await chromium.launch({ headless: true, executablePath: CHROME_PATH }); try { const results = []; if (!args.has("--oog-only")) results.push(await runSchedule(browser)); if (!args.has("--schedule-only")) results.push(await runOog(browser)); console.log(JSON.stringify({ ok: true, dry_run: DRY_RUN, results, observed_at: new Date().toISOString() }, null, 2)); } finally { await browser.close(); } })().catch(error => { console.error(JSON.stringify({ ok: false, error: error.message, observed_at: new Date().toISOString() }, null, 2)); process.exitCode = 1; });
module.exports = { normalizeTime, oogMatch };
