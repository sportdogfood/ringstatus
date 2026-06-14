globalThis.process ??= {};
globalThis.process.env ??= {};
import { env } from "cloudflare:workers";
const __vite_import_meta_env__ = { "ASSETS_PREFIX": "https://110f06dd-c1ea-4839-98af-d829cbe77941.wf-app-prod.cosmic.webflow.services/test", "BASE_URL": "/test", "DEV": false, "MODE": "production", "PROD": true, "SITE": void 0, "SSR": true };
const config = {
  runtime: "edge"
};
const DEFAULT_BASE_ID = "app6XS1RvsPNRT6os";
const TABLES = {
  focusShow: "focus_show",
  classStartTimes: "class_start_times",
  entryGoTimes: "entry_go_times",
  classHide: "class_hide",
  rings: "rings"
};
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const url = new URL(request.url);
    const showNo = clean(url.searchParams.get("show_no")) || "14906";
    const focus = await getFocusShow(airtable, showNo);
    const focusDay = isoDate(url.searchParams.get("focus_day")) || focus.focus_day;
    if (!focusDay) return json({ ok: false, error: "missing_focus_day" }, 400);
    const [classRows, entryRows, hideRows, ringRows] = await Promise.all([
      listAirtableRecords(airtable, airtable.classStartTimesTable),
      listAirtableRecords(airtable, airtable.entryGoTimesTable),
      listAirtableRecords(airtable, airtable.classHideTable),
      listAirtableRecords(airtable, airtable.ringsTable)
    ]);
    const hide = buildHideRules(hideRows);
    const ringDisplayByNo = buildRingDisplayByNo(ringRows);
    const entriesByClass = groupEntries(entryRows, showNo, focusDay);
    const rows = classRows.map((record) => record.fields || {}).filter((fields) => clean(fields.show_no) === showNo).filter((fields) => isoDate(fields.focus_day) === focusDay).filter((fields) => normalizeStatus(fields.status) !== "inactive").filter((fields) => !isHiddenClass(fields, hide)).map((fields) => classRowToScheduleRow({
      fields,
      focus,
      focusDay,
      ringDisplayByNo,
      entryRollups: entriesByClass.get(clean(fields.class_no)) || []
    })).sort(compareScheduleRows);
    const rowClassNos = new Set(rows.map((row) => clean(row.class_no)).filter(Boolean));
    const entryOnlyClassRows = entryRows.map((record) => record.fields || {}).filter((fields) => clean(fields.show_no) === showNo).filter((fields) => isoDate(fields.focus_day) === focusDay).filter((fields) => normalizeStatus(fields.status) !== "inactive").filter((fields) => clean(fields.class_no) && !rowClassNos.has(clean(fields.class_no))).filter((fields, index, all) => all.findIndex((item) => clean(item.class_no) === clean(fields.class_no)) === index).filter((fields) => !isHiddenClass(fields, hide)).map((fields) => classRowToScheduleRow({
      fields,
      focus,
      focusDay,
      ringDisplayByNo,
      entryRollups: entriesByClass.get(clean(fields.class_no)) || []
    }));
    return json([...rows, ...entryOnlyClassRows].sort(compareScheduleRows));
  } catch (error) {
    console.error("[wec-schedule] state failed", error);
    return json({
      ok: false,
      error: "wec_schedule_state_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
function getAirtableConfig() {
  const runtime = { ...globalThis.process?.env || {}, ...Object.assign(__vite_import_meta_env__, { AIRTABLE_TOKEN: "patDeqY9NAQsuYx6q.7fd75026f0820373f62a72ca063f99b2203b9d873cb77aa3962637ab7bb0ec37", AIRTABLE_BASE_ID: "apptdhhNzduxm5gjn" }) || {}, ...env || {} };
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.WEC_AIRTABLE_BASE_ID || runtime.AIRTABLE_WEC_SCHEDULES_BASE_ID || DEFAULT_BASE_ID;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  return {
    ok: true,
    token,
    baseId,
    focusShowTable: runtime.AIRTABLE_WEC_FOCUS_SHOW_TABLE || TABLES.focusShow,
    classStartTimesTable: runtime.AIRTABLE_WEC_CLASS_START_TIMES_TABLE || TABLES.classStartTimes,
    entryGoTimesTable: runtime.AIRTABLE_WEC_ENTRY_GO_TIMES_TABLE || TABLES.entryGoTimes,
    classHideTable: runtime.AIRTABLE_WEC_CLASS_HIDE_TABLE || TABLES.classHide,
    ringsTable: runtime.AIRTABLE_WEC_RINGS_TABLE || TABLES.rings
  };
}
async function getFocusShow(airtable, showNo) {
  const records = await listAirtableRecords(airtable, airtable.focusShowTable);
  const record = records.find((item) => item.fields?.active && clean(item.fields?.show_no) === showNo) || records.find((item) => clean(item.fields?.show_no) === showNo) || records.find((item) => item.fields?.active) || records[0];
  if (!record) throw new Error("focus_show_not_found");
  const fields = record.fields || {};
  return {
    show_no: clean(fields.show_no) || showNo,
    show_name: clean(fields.show_name || fields.name) || `Show ${showNo}`,
    focus_day: isoDate(fields.focus_day),
    show_end_date: isoDate(fields.show_end),
    record_id: record.id
  };
}
async function listAirtableRecords(airtable, table) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(airtable.baseId)}/${encodeURIComponent(table)}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, { headers: { Authorization: `Bearer ${airtable.token}` } });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`list ${table} ${response.status}: ${JSON.stringify(result)}`);
    records.push(...result.records || []);
    offset = result.offset || "";
  } while (offset);
  return records;
}
function buildHideRules(records) {
  const text = /* @__PURE__ */ new Set();
  const classNos = /* @__PURE__ */ new Set();
  for (const record of records || []) {
    const fields = record.fields || {};
    const active = fields.active !== false && fields.hide !== false;
    if (!active) continue;
    for (const key of ["hide_text", "class_hide", "class_name", "name"]) {
      const value = clean(fields[key]);
      if (value) text.add(value.toLowerCase());
    }
    const classNo = clean(fields.class_no);
    if (classNo) classNos.add(classNo);
  }
  return { text, classNos };
}
function buildRingDisplayByNo(records) {
  const out = /* @__PURE__ */ new Map();
  for (const record of records || []) {
    const fields = record.fields || {};
    const ringNo = clean(fields.ring_no);
    if (!ringNo) continue;
    out.set(ringNo, clean(fields.ring_display || fields.ring || fields.ring_name || fields.name));
  }
  return out;
}
function groupEntries(records, showNo, focusDay) {
  const byClassTrainer = /* @__PURE__ */ new Map();
  for (const record of records || []) {
    const fields = record.fields || {};
    if (clean(fields.show_no) !== showNo) continue;
    if (isoDate(fields.focus_day) !== focusDay) continue;
    if (normalizeStatus(fields.status) === "inactive") continue;
    const classNo = clean(fields.class_no);
    if (!classNo) continue;
    const trainer = clean(fields.trainer_display || fields.trainer);
    if (!trainer) continue;
    const classMap = byClassTrainer.get(classNo) || /* @__PURE__ */ new Map();
    const bucket = classMap.get(trainer) || [];
    const label = `${clean(fields.horse_display || fields.horse)} (${clean(fields.entry_order)})`;
    bucket.push({
      label,
      horse: clean(fields.horse),
      display: clean(fields.horse_display || fields.horse),
      entry_no: clean(fields.entry_no),
      entry_order: numberOrZero(fields.entry_order)
    });
    bucket.sort((a, b) => a.entry_order - b.entry_order);
    classMap.set(trainer, bucket);
    byClassTrainer.set(classNo, classMap);
  }
  const out = /* @__PURE__ */ new Map();
  for (const [classNo, trainerMap] of byClassTrainer.entries()) {
    out.set(classNo, Array.from(trainerMap.entries()).map(([trainer, horses]) => ({
      trainer,
      trainer_display: trainer,
      horses
    })));
  }
  return out;
}
function classRowToScheduleRow({ fields, focus, focusDay, ringDisplayByNo, entryRollups }) {
  const classNo = clean(fields.class_no);
  const ringNo = clean(fields.ring_no);
  const classNumber = clean(fields.class_number);
  const className = clean(fields.class_name);
  const classStart = clean(fields.class_start_time);
  const displayTime = formatDisplayTime(fields.display_time || classStart);
  const ringName = ringDisplayByNo.get(ringNo) || clean(fields.ring_display || fields.ring_name) || `Ring ${ringNo}`;
  const groupDisplay = rollupGroupDisplay(entryRollups);
  return {
    show_id: focus.show_no,
    show_no: focus.show_no,
    show_days_report_title: focus.show_name,
    show_days_display_date: focusDay,
    show_day_key: focusDay,
    show_end_date: focus.show_end_date,
    ring_number: ringNo,
    ring_no: ringNo,
    ring_name: ringName,
    class_group_id: classNo,
    class_group_sequence: classStart,
    group_group_name: className,
    class_no: classNo,
    class_number: classNumber,
    class_name: className,
    start_display: displayTime,
    class_start_time: classStart,
    entry_count: fields.entry_count,
    group_display: groupDisplay,
    sched_display: groupDisplay,
    "8778_sched_display": groupDisplay,
    trainer_rollups: entryRollups,
    diff_class: clean(fields.diff_class)
  };
}
function rollupGroupDisplay(rollups) {
  const out = [];
  const seen = /* @__PURE__ */ new Set();
  for (const rollup of rollups || []) {
    for (const horse of rollup.horses || []) {
      const label = clean(horse.label || horse.display || horse.horse);
      const key = label.toLowerCase();
      if (!label || seen.has(key)) continue;
      seen.add(key);
      out.push(label);
    }
  }
  return out.join(", ");
}
function isHiddenClass(fields, hide) {
  const classNo = clean(fields.class_no);
  if (classNo && hide.classNos.has(classNo)) return true;
  const haystack = [fields.class_name, fields.event_name, fields.group_group_name].map(clean).join(" ").toLowerCase();
  for (const token of hide.text) {
    if (token && haystack.includes(token)) return true;
  }
  return false;
}
function compareScheduleRows(a, b) {
  return clean(a.ring_name).localeCompare(clean(b.ring_name)) || clean(a.class_start_time).localeCompare(clean(b.class_start_time)) || numberOrZero(a.class_number) - numberOrZero(b.class_number);
}
function formatDisplayTime(value) {
  const raw = clean(value);
  if (!raw) return "check time";
  if (/^\d{2}:\d{2}:\d{2}$/.test(raw)) {
    const [hh, mm] = raw.split(":").map((part) => Number(part));
    const suffix = hh >= 12 ? "PM" : "AM";
    const h12 = (hh + 11) % 12 + 1;
    return `${h12}:${String(mm).padStart(2, "0")} ${suffix}`;
  }
  return raw.replace(/\s+/g, " ");
}
function normalizeStatus(value) {
  return clean(value).toLowerCase();
}
function clean(value) {
  if (value == null) return "";
  if (Array.isArray(value)) return clean(value[0]);
  if (typeof value === "object" && value.name) return clean(value.name);
  return String(value).trim();
}
function isoDate(value) {
  const raw = clean(value);
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  return match ? `${match[1]}-${match[2]}-${match[3]}` : "";
}
function numberOrZero(value) {
  const n = Number(clean(value));
  return Number.isFinite(n) ? n : 0;
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
