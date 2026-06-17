/*
Trigger: focus_show record created or updated
Input variable: recordId = Airtable record ID from focus_show

Purpose:
1. Push the current focus_show values to Catalyst.
2. Refresh ring days immediately after a focus_day change.

Cadence still belongs to run-wec-catalyst-workflow.ps1. This automation only
removes the wait for the first support-table refresh after a manual focus_day
change.
*/

const { recordId } = input.config();

const CATALYST_ENDPOINT = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

function clean(value) {
  return value == null ? "" : String(value).trim();
}

function safeString(record, fieldName) {
  try {
    return clean(record.getCellValueAsString(fieldName));
  } catch {
    return "";
  }
}

function safeBool(record, fieldName) {
  try {
    return record.getCellValue(fieldName) === true;
  } catch {
    return false;
  }
}

function addIfPresent(params, key, value) {
  const cleanValue = clean(value);
  if (cleanValue) params.set(key, cleanValue);
}

async function catalystGet(params) {
  const response = await fetch(`${CATALYST_ENDPOINT}?${params.toString()}`);
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Catalyst ${params.get("action")} failed ${response.status}: ${body}`);
  }
  let payload;
  try {
    payload = JSON.parse(body);
  } catch {
    throw new Error(`Catalyst ${params.get("action")} returned non-JSON: ${body.slice(0, 300)}`);
  }
  if (payload.ok === false) {
    throw new Error(`Catalyst ${params.get("action")} failed: ${payload.error || body}`);
  }
  return payload;
}

async function main() {
  const focusShowTable = base.getTable("focus_show");
  const record = await focusShowTable.selectRecordAsync(recordId);
  if (!record) throw new Error(`focus_show record not found: ${recordId}`);

  if (safeString(record, "active") && !safeBool(record, "active")) {
    console.log(JSON.stringify({ ok: true, skipped: true, reason: "focus_show_not_active", record_id: recordId }));
    return;
  }

  const showNo = clean(safeString(record, "show_no")).replace(/\.0$/, "");
  const focusDay = clean(safeString(record, "focus_day")).slice(0, 10);
  if (!showNo) throw new Error(`focus_show.show_no is required: ${recordId}`);
  if (!focusDay) throw new Error(`focus_show.focus_day is required: ${recordId}`);

  const configParams = new URLSearchParams({
    action: "set-show-config",
    show_no: showNo,
    focus_day: focusDay
  });

  addIfPresent(configParams, "show_start_date", safeString(record, "show_start"));
  addIfPresent(configParams, "show_end_date", safeString(record, "show_end"));
  addIfPresent(configParams, "focus_status_cadence", safeString(record, "focus_status_cadence"));
  addIfPresent(configParams, "focus_day_cadence", safeString(record, "focus_day_cadence"));
  addIfPresent(configParams, "future_days_cadence", safeString(record, "future_days_cadence"));
  addIfPresent(configParams, "zoom_cadence", safeString(record, "zoom_cadence"));

  const config = await catalystGet(configParams);
  const ringDays = await catalystGet(new URLSearchParams({
    action: "sync-ring-days",
    show_no: showNo,
    focus_day: focusDay,
    refresh_existing: "1"
  }));

  console.log(JSON.stringify({
    ok: true,
    show_no: showNo,
    focus_day: focusDay,
    config_ok: config.ok !== false,
    ring_days_rows: ringDays.parsed_rows,
    upstream_requests: ringDays.upstream_requests
  }));
}

main().catch((error) => {
  throw error;
});
