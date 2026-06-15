export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

const DEFAULT_BASE_ID = "app6XS1RvsPNRT6os";

const TABLES = {
  focusShow: "focus_show",
  classStartTimes: "class_start_times",
  classOog: "class_oog",
  classHide: "class_hide",
  rings: "rings",
  trainers: "trainers"
};

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);

  try {
    const url = new URL(request.url);
    const showNo = clean(url.searchParams.get("show_no")) || "14906";
    const focus = await getFocusShow(airtable, showNo);
    const focusDay = isoDate(url.searchParams.get("focus_day")) || focus.focus_day;
    if (!focusDay) return json({ ok: false, error: "missing_focus_day" }, 400);

    const [classRows, oogRows, hideRows, ringRows, trainerRows] = await Promise.all([
      listAirtableRecords(airtable, airtable.classStartTimesTable),
      listAirtableRecords(airtable, airtable.classOogTable),
      listAirtableRecords(airtable, airtable.classHideTable),
      listAirtableRecords(airtable, airtable.ringsTable),
      listAirtableRecords(airtable, airtable.trainersTable)
    ]);

    const hide = buildHideRules(hideRows);
    const rings = buildRingLookup(ringRows);
    const activeTrainers = buildActiveTrainers(trainerRows);
    const entriesByClass = groupOogEntries(oogRows, showNo, focusDay, activeTrainers);

    const classItems = classRows
      .map((record) => record.fields || {})
      .filter((fields) => clean(fields.show_no).replace(/\.0$/, "") === showNo)
      .filter((fields) => isoDate(fields.focus_day) === focusDay)
      .filter((fields) => normalizeStatus(fields.status) !== "inactive")
      .filter((fields) => !isHiddenClass(fields, hide))
      .map((fields) => classRowToCommentClass(fields, focus, focusDay, rings, entriesByClass))
      .sort(compareClasses);

    const ringsOut = groupClassesByRing(classItems, rings);

    return json({
      ok: true,
      source: "comment-state",
      show_no: focus.show_no,
      show_name: focus.show_name,
      focus_day: focusDay,
      show_end_date: focus.show_end_date,
      ring_count: ringsOut.length,
      class_count: classItems.length,
      entry_count: classItems.reduce((sum, item) => sum + item.entries.length, 0),
      cwf_entry_count: classItems.reduce((sum, item) => sum + item.entries.filter((entry) => entry.is_cwf).length, 0),
      rings: ringsOut
    });
  } catch (error) {
    console.error("[wec-schedule] comment-state failed", error);
    return json({
      ok: false,
      error: "wec_comment_state_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

function getAirtableConfig() {
  const runtime = { ...(globalThis.process?.env || {}), ...(import.meta.env || {}), ...(env || {}) };
  const token = runtime.AIRTABLE_WEC_TOKEN || runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_WEC_BASE_ID || runtime.WEC_AIRTABLE_BASE_ID || runtime.AIRTABLE_WEC_SCHEDULES_BASE_ID || DEFAULT_BASE_ID;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  return {
    ok: true,
    token,
    baseId,
    focusShowTable: runtime.AIRTABLE_WEC_FOCUS_SHOW_TABLE || TABLES.focusShow,
    classStartTimesTable: runtime.AIRTABLE_WEC_CLASS_START_TIMES_TABLE || TABLES.classStartTimes,
    classOogTable: runtime.AIRTABLE_WEC_CLASS_OOG_TABLE || TABLES.classOog,
    classHideTable: runtime.AIRTABLE_WEC_CLASS_HIDE_TABLE || TABLES.classHide,
    ringsTable: runtime.AIRTABLE_WEC_RINGS_TABLE || TABLES.rings,
    trainersTable: runtime.AIRTABLE_WEC_TRAINERS_HELPER_TABLE || runtime.AIRTABLE_WEC_TRAINERS_TABLE || TABLES.trainers
  };
}

async function getFocusShow(airtable, showNo) {
  const records = await listAirtableRecords(airtable, airtable.focusShowTable);
  const record = records.find((item) => item.fields?.active && clean(item.fields?.show_no).replace(/\.0$/, "") === showNo)
    || records.find((item) => clean(item.fields?.show_no).replace(/\.0$/, "") === showNo)
    || records.find((item) => item.fields?.active)
    || records[0];
  if (!record) throw new Error("focus_show_not_found");
  const fields = record.fields || {};
  return {
    show_no: clean(fields.show_no).replace(/\.0$/, "") || showNo,
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
    records.push(...(result.records || []));
    offset = result.offset || "";
  } while (offset);
  return records;
}

function buildHideRules(records) {
  const text = new Set();
  const classNos = new Set();
  for (const record of records || []) {
    const fields = record.fields || {};
    const active = fields.active !== false && fields.hide !== false;
    if (!active) continue;
    for (const key of ["hide_text", "class_hide", "class_name", "name"]) {
      const value = clean(fields[key]);
      if (value) text.add(value.toLowerCase());
    }
    const classNo = clean(fields.class_no).replace(/\.0$/, "");
    if (classNo) classNos.add(classNo);
  }
  return { text, classNos };
}

function buildRingLookup(records) {
  const out = new Map();
  for (const record of records || []) {
    const fields = record.fields || {};
    const ringNo = clean(fields.ring_no).replace(/\.0$/, "");
    if (!ringNo) continue;
    out.set(ringNo, {
      ring_no: ringNo,
      ring_name: clean(fields.ring_display || fields.ring || fields.ring_name || fields.name) || `Ring ${ringNo}`,
      priority: numberOrZero(fields.priority)
    });
  }
  return out;
}

function buildActiveTrainers(records) {
  const out = new Map();
  for (const record of records || []) {
    const fields = record.fields || {};
    if (fields.active !== true) continue;
    const trainer = clean(fields.trainer).toLowerCase();
    if (!trainer) continue;
    out.set(trainer, clean(fields.trainer_display || fields.trainer) || "CWF");
  }
  return out;
}

function groupOogEntries(records, showNo, focusDay, activeTrainers) {
  const byClass = new Map();
  const seen = new Set();
  for (const record of records || []) {
    const fields = record.fields || {};
    const recordShowNo = clean(fields.show_no).replace(/\.0$/, "");
    if (recordShowNo && recordShowNo !== showNo) continue;
    if (isoDate(fields.focus_day) !== focusDay) continue;
    if (fields.ignore === true) continue;

    const classNo = clean(fields.class_no).replace(/\.0$/, "");
    const entryNo = clean(fields.entry_no).replace(/\.0$/, "");
    if (!classNo || !entryNo) continue;
    const key = `${classNo}|${entryNo}|${clean(fields.entry_order).replace(/\.0$/, "")}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const trainer = clean(fields.trainer);
    const activeDisplay = activeTrainers.get(trainer.toLowerCase()) || "";
    const horseDisplay = clean(fields["horse_display (from horses)"]) || clean(fields.horse_display) || clean(fields.horse);
    const riderDisplay = clean(fields["rider_display (from riders)"]) || clean(fields.rider_display) || clean(fields.rider);
    const trainerDisplay = clean(fields["trainer_display (from trainers)"]) || clean(fields.trainer_display) || trainer;
    const entry = {
      entry_no: entryNo,
      entry_order: numberOrZero(fields.entry_order),
      horse: clean(fields.horse),
      horse_display: horseDisplay,
      rider: clean(fields.rider),
      rider_display: riderDisplay,
      trainer,
      trainer_display: activeDisplay || trainerDisplay,
      source: clean(fields.source),
      is_cwf: Boolean(activeDisplay),
      entry_class: activeDisplay ? "cwf-entry" : ""
    };
    const entries = byClass.get(classNo) || [];
    entries.push(entry);
    byClass.set(classNo, entries);
  }

  for (const entries of byClass.values()) {
    entries.sort((a, b) => a.entry_order - b.entry_order || numberOrZero(a.entry_no) - numberOrZero(b.entry_no));
  }
  return byClass;
}

function classRowToCommentClass(fields, focus, focusDay, rings, entriesByClass) {
  const classNo = clean(fields.class_no).replace(/\.0$/, "");
  const ringNo = clean(fields.ring_no).replace(/\.0$/, "");
  const ringInfo = rings.get(ringNo) || { ring_no: ringNo, ring_name: clean(fields.ring_display || fields.ring_name) || `Ring ${ringNo}`, priority: 0 };
  const classStart = clean(fields.class_start_time);
  return {
    show_no: focus.show_no,
    focus_day: focusDay,
    ring_no: ringNo,
    ring_name: ringInfo.ring_name,
    ring_priority: ringInfo.priority,
    ring_day_no: clean(fields.ring_day_no),
    class_no: classNo,
    class_number: clean(fields.class_number).replace(/\.0$/, ""),
    class_name: clean(fields.class_name),
    class_start_time: classStart,
    start_display: formatDisplayTime(fields.display_time || classStart),
    entry_count: numberOrZero(fields.entry_count),
    n_gone: numberOrEmpty(fields.n_gone),
    n_to_go: numberOrEmpty(fields.n_to_go),
    elapsed_seconds: numberOrEmpty(fields.elapsed_seconds),
    source: clean(fields.source),
    entries: entriesByClass.get(classNo) || []
  };
}

function groupClassesByRing(classItems, rings) {
  const byRing = new Map();
  for (const item of classItems) {
    if (!item.ring_no) continue;
    const ring = byRing.get(item.ring_no) || {
      ring_no: item.ring_no,
      ring_name: item.ring_name,
      ring_priority: rings.get(item.ring_no)?.priority || item.ring_priority || 0,
      classes: []
    };
    ring.classes.push(item);
    byRing.set(item.ring_no, ring);
  }
  return Array.from(byRing.values())
    .map((ring) => ({
      ...ring,
      class_count: ring.classes.length,
      entry_count: ring.classes.reduce((sum, item) => sum + item.entries.length, 0),
      cwf_entry_count: ring.classes.reduce((sum, item) => sum + item.entries.filter((entry) => entry.is_cwf).length, 0)
    }))
    .sort((a, b) => a.ring_priority - b.ring_priority || clean(a.ring_name).localeCompare(clean(b.ring_name)));
}

function isHiddenClass(fields, hide) {
  const classNo = clean(fields.class_no).replace(/\.0$/, "");
  if (classNo && hide.classNos.has(classNo)) return true;
  const haystack = [fields.class_name, fields.event_name, fields.group_group_name].map(clean).join(" ").toLowerCase();
  for (const token of hide.text) {
    if (token && haystack.includes(token)) return true;
  }
  return false;
}

function compareClasses(a, b) {
  return a.ring_priority - b.ring_priority
    || clean(a.ring_name).localeCompare(clean(b.ring_name))
    || clean(a.class_start_time).localeCompare(clean(b.class_start_time))
    || numberOrZero(a.class_number) - numberOrZero(b.class_number);
}

function formatDisplayTime(value) {
  const raw = clean(value);
  if (!raw) return "check time";
  if (/^\d{2}:\d{2}:\d{2}$/.test(raw)) {
    const [hh, mm] = raw.split(":").map((part) => Number(part));
    const suffix = hh >= 12 ? "PM" : "AM";
    const h12 = ((hh + 11) % 12) + 1;
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

function numberOrEmpty(value) {
  const raw = clean(value);
  if (!raw) return "";
  const n = Number(raw);
  return Number.isFinite(n) ? n : "";
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
