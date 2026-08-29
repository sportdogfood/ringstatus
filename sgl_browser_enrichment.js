const { chromium } = require("playwright");
const fs = require("fs");

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
const FETCH_TIMEOUT_MS = Number(process.env.SGL_FETCH_TIMEOUT_MS || "30000");
const ENRICHMENT_LOG = process.env.SGL_ENRICHMENT_LOG || "C:\\actions-runner\\ringstatus\\sgl-browser-enrichment.log";
function progress(event, details = {}) { const line = JSON.stringify({ ts: new Date().toISOString(), event, ...details }); try { fs.appendFileSync(ENRICHMENT_LOG, `${line}\n`); } catch {} console.log(line); }
async function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) { const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), timeoutMs); try { return await fetch(url, { ...options, signal: controller.signal }); } catch (error) { throw new Error(`${error.name === "AbortError" ? `request_timeout_${timeoutMs}ms` : error.message}: ${url}`); } finally { clearTimeout(timer); } }


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
    const response = await fetchWithTimeout(`${AIRTABLE_API}/${base}/${encodeURIComponent(table)}?${query}`, { headers: { Authorization: `Bearer ${TOKEN}` } });
    const body = await response.text(); if (!response.ok) throw new Error(`Airtable ${response.status}: ${body.slice(0, 500)}`);
    const payload = body ? JSON.parse(body) : {}; records.push(...(payload.records || [])); offset = payload.offset || "";
  } while (offset); return records;
}
async function airtableWrite(base, table, records) {
  for (let i = 0; i < records.length; i += 10) {
    const response = await fetchWithTimeout(`${AIRTABLE_API}/${base}/${encodeURIComponent(table)}`, { method: "PATCH", headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }, body: JSON.stringify({ records: records.slice(i, i + 10), typecast: true }) });
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
  page.setDefaultTimeout(Number(process.env.SGL_PAGE_TIMEOUT_MS || "15000"));
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: Number(process.env.SGL_NAVIGATION_TIMEOUT_MS || "45000") }); await page.waitForTimeout(Number(process.env.SGL_BROWSER_WAIT_MS || 5000));
  const root = page.locator("#sgl-root"); const rendered = text(await (await root.count() ? root : page.locator("body")).innerText());
  if (!rendered) throw new Error("rendered page was empty");
  const rows = (await page.locator("#sgl-root tr, #sgl-root li").allInnerTexts().catch(() => [])).map(text).filter(Boolean);
  const oogRows = await page.locator("#sgl-root tr").evaluateAll(elements => { const rows = elements.map(row => Array.from(row.querySelectorAll("th,td")).map(cell => cell.innerText.trim()).filter(Boolean)).filter(row => row.length); const header = rows.find(row => row.some(cell => /^order$/i.test(cell))); const orderIndex = header ? header.findIndex(cell => /^order$/i.test(cell)) : 0; return rows.filter(row => row !== header).map(row => orderIndex > 0 && row.length > orderIndex ? [row[orderIndex], ...row.slice(0, orderIndex), ...row.slice(orderIndex + 1)] : row); }).catch(() => []);
  return { page, rendered, rows, oogRows };
}
function scheduleMatch(record, rows, rendered) {
  const f = record.fields || {}; const cls = field(f, "class_number", "class_no"); const name = field(f, "class_name", "class_label", "group_name");
  const candidates = rows.filter(row => (cls && contains(row, cls)) || (name && contains(row, name)));
  for (const row of candidates) { const estimated = normalizeTime(row); if (estimated) return { estimated, evidence: row }; }
  const broad = cls && rendered.match(new RegExp(`.{0,180}${String(cls).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}.{0,180}`, "i"));
  return broad && normalizeTime(broad[0]) ? { estimated: normalizeTime(broad[0]), evidence: broad[0] } : null;
}
function oogMatch(record, rows, structuredRows = []) {
  const f = record.fields || {}; const entry = text(field(f, "entry_number", "entry_no")); const cls = field(f, "class_number", "class_no"); const rider = field(f, "rider_name", "rider"); const horse = field(f, "horse", "horse_name");
  const candidates = (structuredRows.length ? structuredRows.map(cells => ({ text: cells.join(" "), order: number(cells[0]) })) : []).filter(row => entry && new RegExp(`\\b${entry.replace(/\\D/g, "")}\\b`).test(row.text));
  const exactClassRows = cls ? candidates.filter(row => new RegExp(`\\b${String(cls).replace(/\\D/g, "")}\\b`).test(row.text)) : [];
  const identityRows = candidates.filter(row => (rider && contains(row.text, rider)) || (horse && contains(row.text, horse)));
  const matches = exactClassRows.length ? exactClassRows : identityRows;
  if (matches.length !== 1 || !Number.isFinite(matches[0].order)) return matches.length ? { ambiguous: matches.slice(0, 3) } : null;
  return { order: matches[0].order, evidence: matches[0].text };
}
async function runSchedule(browser) {
  if (!SCHEDULE_BASE) throw new Error("SGL_SCHEDULE_BASE_ID or AIRTABLE_BASE_ID is required");
  const scheduleParams = { view: SCHEDULE_VIEW };
  if (SHOW_ID && FOCUS_DAY) {
    scheduleParams.filterByFormula = `AND({app_show_idv2}=${Number(SHOW_ID)}, {app_sql_datev2}="${FOCUS_DAY}")`;
  }
  progress("schedule_list_started", { table: SCHEDULE_TABLE, show_id: SHOW_ID, focus_day: FOCUS_DAY });
  const records = await airtableList(SCHEDULE_BASE, SCHEDULE_TABLE, scheduleParams);
  progress("schedule_list_completed", { records: records.length });
  const scopedRecords = records.filter(record => {
    const f = record.fields || {};
    const sid = field(f, "sid", "show_id", "app_show_id");
    const date = field(f, "dt", "date", "schedule_date", "show_date", "schedule_show_datev2", "app_sql_date", "app_sql_datev2") || FOCUS_DAY;
    return (!SHOW_ID || String(sid) === String(SHOW_ID)) && (!FOCUS_DAY || String(date) === String(FOCUS_DAY));
  }).slice(0, MAX_RECORDS > 0 ? MAX_RECORDS : undefined);
  const updates = [], unmatched = [];
  const groups = new Map();
  for (const record of scopedRecords) {
    const f = record.fields || {};
    const sid = field(f, "sid", "show_id", "app_show_id");
    const date = field(f, "dt", "date", "schedule_date", "show_date", "schedule_show_datev2", "app_sql_date", "app_sql_datev2") || FOCUS_DAY;
    const key = `${sid}|${date}`;
    if (!groups.has(key)) groups.set(key, { sid, date, records: [] });
    groups.get(key).records.push(record);
  }
  for (const group of groups.values()) {
    const url = `https://www.wellingtoninternational.com/showgrounds/show-schedule/?date=${encodeURIComponent(group.date)}&sid=${encodeURIComponent(group.sid)}`;
    progress("schedule_page_started", { url, records: group.records.length });
    try {
      const loaded = await loadRenderedPage(browser, url);
      progress("schedule_page_loaded", { url, rows: loaded.rows.length });
      for (const record of group.records) {
        const f = record.fields || {};
        if (f.manual_time_override === true || f.manual_time_overide === true) continue;
        const match = scheduleMatch(record, loaded.rows, loaded.rendered);
        if (match) updates.push({ id: record.id, fields: { estimated_start_time: match.estimated } });
        else unmatched.push({ id: record.id, reason: "schedule_time_not_found", url });
      }
      await loaded.page.close();
    } catch (error) {
      for (const record of group.records) unmatched.push({ id: record.id, reason: error.message, url });
      await logError(`${url}: ${error.message}`, "schedule_scrape", group.sid);
    }
  }
  return { lane: "schedule", records: scopedRecords.length, show_date_groups: groups.size, matched: updates.length, written: DRY_RUN ? 0 : await airtableWrite(SCHEDULE_BASE, SCHEDULE_TABLE, updates), unmatched: unmatched.length, unmatched_samples: unmatched.slice(0, 5) };
}
async function runOog(browser) {
  progress("oog_list_started", { table: OOG_TABLE, view: OOG_VIEW });
  const records = await airtableList(OOG_BASE, OOG_TABLE, { view: OOG_VIEW });
  progress("oog_list_completed", { records: records.length }); const scopedRecords = MAX_RECORDS > 0 ? records.slice(0, MAX_RECORDS) : records; const updates = [], unmatched = [];
  for (const record of scopedRecords) { const f = record.fields || {}, url = field(f, "classsignup_url_viewsetorder_scrape"); if (!url) { unmatched.push({ id: record.id, reason: "missing_url" }); continue; }
    try { const loaded = await loadRenderedPage(browser, url); const match = oogMatch(record, loaded.rows, loaded.oogRows); if (match?.order !== undefined) updates.push({ id: record.id, fields: { order_of_go: match.order } }); else unmatched.push({ id: record.id, reason: match?.ambiguous ? "ambiguous_match" : "order_not_found", url }); await loaded.page.close(); }
    catch (error) { unmatched.push({ id: record.id, reason: error.message, url }); await logError(`${url}: ${error.message}`, "oog_scrape", field(f, "sid", "show_id")); }
  }
  return { lane: "oog", records: scopedRecords.length, matched: updates.length, written: DRY_RUN ? 0 : await airtableWrite(OOG_BASE, OOG_TABLE, updates), unmatched: unmatched.length, unmatched_samples: unmatched.slice(0, 5) };
}
(async () => { if (!TOKEN) throw new Error("AIRTABLE_TOKEN is required"); const args = new Set(process.argv.slice(2)); const scheduleOnly = args.has("--schedule-only") || process.env.SGL_SCHEDULE_ONLY === "1"; const oogOnly = args.has("--oog-only") || process.env.SGL_OOG_ONLY === "1"; progress("enrichment_started", { schedule_only: scheduleOnly, oog_only: oogOnly }); const browser = await chromium.launch({ headless: true, executablePath: CHROME_PATH, timeout: Number(process.env.SGL_BROWSER_LAUNCH_TIMEOUT_MS || "30000") }); try { const results = []; if (!oogOnly) results.push(await runSchedule(browser)); if (!scheduleOnly) results.push(await runOog(browser)); console.log(JSON.stringify({ ok: true, dry_run: DRY_RUN, results, observed_at: new Date().toISOString() }, null, 2)); } finally { await browser.close(); progress("enrichment_finished"); } })().catch(error => { progress("enrichment_failed", { error: error.message }); console.error(JSON.stringify({ ok: false, error: error.message, observed_at: new Date().toISOString() }, null, 2)); process.exitCode = 1; });
module.exports = { normalizeTime, oogMatch };
