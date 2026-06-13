globalThis.process ??= {};
globalThis.process.env ??= {};
import { env } from "cloudflare:workers";
const __vite_import_meta_env__ = { "ASSETS_PREFIX": "https://110f06dd-c1ea-4839-98af-d829cbe77941.wf-app-prod.cosmic.webflow.services/test", "BASE_URL": "/test", "DEV": false, "MODE": "production", "PROD": true, "SITE": void 0, "SSR": true };
const config = {
  runtime: "edge"
};
const DEFAULT_BASE_ID = "app6XS1RvsPNRT6os";
const DEFAULT_FOCUS_SHOW_TABLE = "focus_show";
const DEFAULT_HORSES_TABLE = "horses";
const DEFAULT_LOGS_TABLE = "wec-logs";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async () => json({
  ok: true,
  service: "wec-schedule-edit",
  actions: ["set-focus-day", "set-barn-name"]
});
const POST = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const payload = await request.json().catch(() => ({}));
    const action = clean(payload.action);
    const schema = await getBaseSchema(airtable);
    if (action === "set-focus-day") {
      const result = await setFocusDay(airtable, schema, payload);
      return json(result);
    }
    if (action === "set-barn-name") {
      const result = await setBarnName(airtable, schema, payload);
      return json(result);
    }
    return json({ ok: false, error: "unknown_action", actions: ["set-focus-day", "set-barn-name"] }, 400);
  } catch (error) {
    console.error("[wec-schedule] edit failed", error);
    return json({
      ok: false,
      error: "wec_schedule_edit_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
async function setFocusDay(airtable, schema, payload) {
  const showNo = clean(payload.show_no || payload.showNo);
  const focusDay = isoDate(payload.focus_day || payload.focusDay);
  if (!showNo) return jsonError("missing_show_no");
  if (!focusDay) return jsonError("invalid_focus_day");
  const record = await findFocusShowRecord(airtable, payload.record_id || payload.recordId, showNo);
  if (!record) return jsonError("focus_show_not_found", { show_no: showNo }, 404);
  const showStart = isoDate(record.fields?.show_start);
  const showEnd = isoDate(record.fields?.show_end);
  if (showStart && focusDay < showStart) return jsonError("focus_day_before_show_start", { focus_day: focusDay, show_start: showStart });
  if (showEnd && focusDay > showEnd) return jsonError("focus_day_after_show_end", { focus_day: focusDay, show_end: showEnd });
  const updated = await patchAirtableRecord(airtable, schema, airtable.focusShowTable, record.id, { focus_day: focusDay });
  const logged = await createWecLog(airtable, schema, {
    log_type: "webflow_edit",
    check_name: "focus_show",
    workflow_lanes: "Helpers",
    show_no: showNo,
    focus_day: focusDay,
    status: "ok",
    records_seen: 1,
    records_changed: 1,
    summary: `focus_show.focus_day updated to ${focusDay}`,
    payload_json: JSON.stringify({
      action: "set-focus-day",
      source: clean(payload.source) || "wec-mobile",
      record_id: record.id,
      old_focus_day: isoDate(record.fields?.focus_day),
      focus_day: focusDay
    }, null, 2)
  });
  return {
    ok: true,
    action: "set-focus-day",
    updated: {
      table: airtable.focusShowTable,
      record_id: updated.id,
      show_no: showNo,
      focus_day: focusDay
    },
    log: logged
  };
}
async function setBarnName(airtable, schema, payload) {
  const showNo = clean(payload.show_no || payload.showNo);
  const horseRecordId = clean(payload.horse_record_id || payload.horseRecordId || payload.record_id || payload.recordId);
  const horseName = clean(payload.horse || payload.show_name || payload.showName);
  const barnName = clean(payload.barn_name || payload.barnName);
  if (!horseRecordId && !horseName) return jsonError("missing_horse_identifier");
  if (!barnName) return jsonError("missing_barn_name");
  const record = await findHorseRecord(airtable, horseRecordId, horseName, showNo);
  if (!record) return jsonError("horse_not_found", { horse: horseName, show_no: showNo }, 404);
  const updated = await patchAirtableRecord(airtable, schema, airtable.horsesTable, record.id, { barn_name: barnName });
  const currentHorse = clean(record.fields?.horse);
  const logged = await createWecLog(airtable, schema, {
    log_type: "webflow_edit",
    check_name: "horses_barn_name",
    workflow_lanes: "Helpers",
    show_no: showNo || clean(record.fields?.show_no),
    focus_day: clean(payload.focus_day || payload.focusDay),
    status: "ok",
    records_seen: 1,
    records_changed: 1,
    summary: `horses.barn_name updated for ${currentHorse || horseName}`,
    payload_json: JSON.stringify({
      action: "set-barn-name",
      source: clean(payload.source) || "wec-mobile",
      record_id: record.id,
      horse: currentHorse || horseName,
      old_barn_name: clean(record.fields?.barn_name),
      barn_name: barnName
    }, null, 2)
  });
  return {
    ok: true,
    action: "set-barn-name",
    updated: {
      table: airtable.horsesTable,
      record_id: updated.id,
      horse: currentHorse || horseName,
      barn_name: barnName
    },
    log: logged
  };
}
function getAirtableConfig() {
  const runtime = { ...globalThis.process?.env || {}, ...Object.assign(__vite_import_meta_env__, { AIRTABLE_BASE_ID: "apptdhhNzduxm5gjn", AIRTABLE_TOKEN: "patDeqY9NAQsuYx6q.7fd75026f0820373f62a72ca063f99b2203b9d873cb77aa3962637ab7bb0ec37", OS: "Windows_NT" }) || {}, ...env || {} };
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.WEC_AIRTABLE_BASE_ID || runtime.AIRTABLE_WEC_SCHEDULES_BASE_ID || runtime.AIRTABLE_BASE_ID || runtime.AIRTABLE_BASE || DEFAULT_BASE_ID;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  return {
    ok: true,
    token,
    baseId,
    focusShowTable: runtime.AIRTABLE_WEC_FOCUS_SHOW_TABLE || DEFAULT_FOCUS_SHOW_TABLE,
    horsesTable: runtime.AIRTABLE_WEC_HORSES_HELPER_TABLE || runtime.AIRTABLE_WEC_HORSES_TABLE || DEFAULT_HORSES_TABLE,
    logsTable: runtime.AIRTABLE_WEC_LOGS_TABLE || DEFAULT_LOGS_TABLE
  };
}
async function findFocusShowRecord(airtable, recordId, showNo) {
  if (recordId) return getAirtableRecord(airtable, airtable.focusShowTable, recordId);
  const records = await listAirtableRecords(airtable, airtable.focusShowTable);
  return records.find((record) => clean(record.fields?.show_no) === showNo) || null;
}
async function findHorseRecord(airtable, recordId, horseName, showNo) {
  if (recordId) return getAirtableRecord(airtable, airtable.horsesTable, recordId);
  const records = await listAirtableRecords(airtable, airtable.horsesTable);
  const matches = records.filter((record) => {
    const fields = record.fields || {};
    if (showNo && clean(fields.show_no) && clean(fields.show_no) !== showNo) return false;
    return clean(fields.horse).toLowerCase() === horseName.toLowerCase();
  });
  if (matches.length > 1) {
    throw new Error(`ambiguous_horse_match: ${horseName}`);
  }
  return matches[0] || null;
}
async function listAirtableRecords(airtable, table) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(airtableUrl(airtable.baseId, table));
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, { headers: airtableHeaders(airtable.token) });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`list ${table} ${response.status}: ${JSON.stringify(result)}`);
    records.push(...result.records || []);
    offset = result.offset || "";
  } while (offset);
  return records;
}
async function getAirtableRecord(airtable, table, recordId) {
  const response = await fetch(`${airtableUrl(airtable.baseId, table)}/${encodeURIComponent(recordId)}`, {
    headers: airtableHeaders(airtable.token)
  });
  const result = await response.json().catch(() => ({}));
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`get ${table}/${recordId} ${response.status}: ${JSON.stringify(result)}`);
  return result;
}
async function patchAirtableRecord(airtable, schema, table, recordId, fields) {
  const response = await fetch(`${airtableUrl(airtable.baseId, table)}/${encodeURIComponent(recordId)}`, {
    method: "PATCH",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      fields: filterAirtableFields(schema, table, fields),
      typecast: true
    })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`patch ${table}/${recordId} ${response.status}: ${JSON.stringify(result)}`);
  return result;
}
async function createWecLog(airtable, schema, fields) {
  const createdAt = (/* @__PURE__ */ new Date()).toISOString();
  const logFields = filterAirtableFields(schema, airtable.logsTable, {
    log_key_run: `${createdAt}|${fields.log_type}|${fields.check_name}`,
    created_at: createdAt,
    ...fields
  });
  const response = await fetch(airtableUrl(airtable.baseId, airtable.logsTable), {
    method: "POST",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: [{ fields: logFields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`log ${response.status}: ${JSON.stringify(result)}`);
  return {
    table: airtable.logsTable,
    record_id: result.records?.[0]?.id || "",
    log_key_run: logFields.log_key_run
  };
}
async function getBaseSchema(airtable) {
  try {
    const response = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
      headers: airtableHeaders(airtable.token)
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) return null;
    const tables = {};
    for (const table of result.tables || []) {
      const fields = new Set((table.fields || []).map((field) => field.name));
      tables[table.name] = fields;
      tables[table.id] = fields;
    }
    return { tables };
  } catch {
    return null;
  }
}
function filterAirtableFields(schema, table, fields) {
  const allowed = schema?.tables?.[table];
  if (!allowed) return compactFields(fields);
  const out = {};
  for (const [key, value] of Object.entries(compactFields(fields))) {
    if (allowed.has(key)) out[key] = value;
  }
  if (!Object.keys(out).length) throw new Error(`no_matching_fields_in_${table}`);
  return out;
}
function airtableUrl(baseId, table) {
  return `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`;
}
function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}` };
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
function jsonError(error, detail = {}, status = 400) {
  return { ok: false, error, ...detail, status };
}
function clean(value) {
  if (value == null) return "";
  if (Array.isArray(value)) return clean(value[0]);
  if (typeof value === "object" && value.name) return clean(value.name);
  return String(value).trim();
}
function isoDate(value) {
  const raw = clean(value);
  if (!raw) return "";
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  return match ? `${match[1]}-${match[2]}-${match[3]}` : "";
}
function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== void 0 && value !== null && value !== ""));
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
