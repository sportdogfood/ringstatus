export const config = {
  runtime: "edge"
};

import { env } from "cloudflare:workers";

const DEFAULT_BASE_ID = "app6XS1RvsPNRT6os";
const DEFAULT_FOCUS_SHOW_TABLE = "focus_show";
const DEFAULT_HORSES_TABLE = "horses";
const DEFAULT_CLASS_HIDE_TABLE = "class_hide";
const DEFAULT_LOGS_TABLE = "wec-logs";
const DEFAULT_SESSIONS_TABLE = "wec_sessions";
const DEFAULT_COMMENTS_TABLE = "wec_comments";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async () => json({
  ok: true,
  service: "wec-schedule-edit",
  actions: ["set-focus-day", "set-barn-name", "hide-classes", "start-session", "session-heartbeat", "add-comment"]
});

export const POST = async ({ request }) => {
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

    if (action === "hide-classes") {
      const result = await hideClasses(airtable, schema, payload);
      return json(result);
    }

    if (action === "start-session") {
      const result = await startSession(airtable, schema, payload);
      return json(result);
    }

    if (action === "session-heartbeat") {
      const result = await sessionHeartbeat(airtable, schema, payload);
      return json(result);
    }

    if (action === "add-comment") {
      const result = await addComment(airtable, schema, payload);
      return json(result);
    }

    return json({ ok: false, error: "unknown_action", actions: ["set-focus-day", "set-barn-name", "hide-classes", "start-session", "session-heartbeat", "add-comment"] }, 400);
  } catch (error) {
    console.error("[wec-schedule] edit failed", error);
    return json({
      ok: false,
      error: "wec_schedule_edit_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};

async function startSession(airtable, schema, payload) {
  const sessionId = clean(payload.session_id || payload.sessionId);
  const deviceId = clean(payload.device_id || payload.deviceId);
  const showNo = clean(payload.show_no || payload.showNo);
  const focusDay = isoDate(payload.focus_day || payload.focusDay);
  const page = clean(payload.page) || "wec-mobile";
  const source = clean(payload.source) || "wec-mobile";
  const now = new Date().toISOString();

  if (!sessionId) return jsonError("missing_session_id");
  if (!deviceId) return jsonError("missing_device_id");

  const existing = await findSessionRecord(airtable, sessionId);
  const fields = {
    session_id: sessionId,
    device_id: deviceId,
    show_no: showNo ? Number(showNo) : undefined,
    focus_day: focusDay,
    started_at: existing ? undefined : now,
    last_seen_at: now,
    status: "active",
    page,
    source
  };

  const record = existing
    ? await patchAirtableRecord(airtable, schema, airtable.sessionsTable, existing.id, fields)
    : await createAirtableRecord(airtable, schema, airtable.sessionsTable, fields);

  return {
    ok: true,
    action: "start-session",
    table: airtable.sessionsTable,
    record_id: record.id,
    session_id: sessionId,
    status: "active"
  };
}

async function sessionHeartbeat(airtable, schema, payload) {
  const sessionId = clean(payload.session_id || payload.sessionId);
  if (!sessionId) return jsonError("missing_session_id");

  const existing = await findSessionRecord(airtable, sessionId);
  if (!existing) return jsonError("session_not_found", { session_id: sessionId }, 404);

  const now = new Date().toISOString();
  const record = await patchAirtableRecord(airtable, schema, airtable.sessionsTable, existing.id, {
    last_seen_at: now,
    status: "active"
  });

  return {
    ok: true,
    action: "session-heartbeat",
    table: airtable.sessionsTable,
    record_id: record.id,
    session_id: sessionId,
    status: "active",
    last_seen_at: now
  };
}

async function addComment(airtable, schema, payload) {
  const commentId = clean(payload.comment_id || payload.commentId) || `comment_${Date.now()}`;
  const sessionId = clean(payload.session_id || payload.sessionId);
  const deviceId = clean(payload.device_id || payload.deviceId);
  const showNo = clean(payload.show_no || payload.showNo);
  const focusDay = isoDate(payload.focus_day || payload.focusDay);
  const commentScope = clean(payload.comment_scope || payload.commentScope);
  const ringNo = clean(payload.ring_no || payload.ringNo);
  const classNo = clean(payload.class_no || payload.classNo);
  const entryNo = clean(payload.entry_no || payload.entryNo);
  const commentText = clean(payload.comment_text || payload.commentText);
  const source = clean(payload.source) || "wec-mobile";

  if (!sessionId) return jsonError("missing_session_id");
  if (!deviceId) return jsonError("missing_device_id");
  if (!["ring", "class", "entry"].includes(commentScope)) return jsonError("invalid_comment_scope");
  if (!commentText) return jsonError("missing_comment_text");

  const record = await createAirtableRecord(airtable, schema, airtable.commentsTable, {
    comment_id: commentId,
    session_id: sessionId,
    device_id: deviceId,
    show_no: showNo ? Number(showNo) : undefined,
    focus_day: focusDay,
    comment_scope: commentScope,
    ring_no: ringNo ? Number(ringNo) : undefined,
    class_no: classNo ? Number(classNo) : undefined,
    entry_no: entryNo ? Number(entryNo) : undefined,
    comment_text: commentText,
    created_at: new Date().toISOString(),
    status: "open",
    source
  });

  return {
    ok: true,
    action: "add-comment",
    table: airtable.commentsTable,
    record_id: record.id,
    comment_id: commentId
  };
}

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

async function hideClasses(airtable, schema, payload) {
  const showNo = clean(payload.show_no || payload.showNo);
  const focusDay = isoDate(payload.focus_day || payload.focusDay);
  const classRows = Array.isArray(payload.classes) ? payload.classes : [];
  const classes = classRows
    .map((item) => ({
      class_no: clean(item.class_no || item.classNo).replace(/\.0$/, ""),
      hide_text: clean(item.hide_text || item.hideText || item.class_label || item.classLabel || item.label)
    }))
    .filter((item, index, all) => item.class_no && all.findIndex((other) => other.class_no === item.class_no) === index);

  if (!showNo) return jsonError("missing_show_no");
  if (!classes.length) return jsonError("missing_classes");

  const existingRecords = await listAirtableRecords(airtable, airtable.classHideTable);
  let changed = 0;
  const updated = [];

  for (const item of classes) {
    const classHideKey = `${showNo}|class_no:${item.class_no}`;
    const existing = existingRecords.find((record) => {
      const fields = record.fields || {};
      return clean(fields.show_no).replace(/\.0$/, "") === showNo
        && clean(fields.class_no).replace(/\.0$/, "") === item.class_no;
    });
    const fields = {
      class_hide_key: classHideKey,
      mirror_class_hide_key: classHideKey,
      show_no: Number(showNo),
      class_no: Number(item.class_no),
      hide_text: item.hide_text,
      active: true
    };

    if (existing) {
      const patched = await patchAirtableRecord(airtable, schema, airtable.classHideTable, existing.id, fields);
      changed += 1;
      updated.push({ record_id: patched.id, class_no: item.class_no });
      continue;
    }

    const created = await createAirtableRecord(airtable, schema, airtable.classHideTable, fields);
    changed += 1;
    updated.push({ record_id: created.id, class_no: item.class_no });
  }

  const logged = await createWecLog(airtable, schema, {
    log_type: "webflow_edit",
    check_name: "class_hide",
    workflow_lanes: "Helpers",
    show_no: showNo,
    focus_day: focusDay,
    status: "ok",
    records_seen: classes.length,
    records_changed: changed,
    summary: `class_hide updated ${changed} classes for show ${showNo}`,
    payload_json: JSON.stringify({
      action: "hide-classes",
      source: clean(payload.source) || "wec-mobile",
      show_no: showNo,
      focus_day: focusDay,
      classes
    }, null, 2)
  });

  return {
    ok: true,
    action: "hide-classes",
    updated,
    log: logged
  };
}

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
    focusShowTable: runtime.AIRTABLE_WEC_FOCUS_SHOW_TABLE || DEFAULT_FOCUS_SHOW_TABLE,
    horsesTable: runtime.AIRTABLE_WEC_HORSES_HELPER_TABLE || runtime.AIRTABLE_WEC_HORSES_TABLE || DEFAULT_HORSES_TABLE,
    classHideTable: runtime.AIRTABLE_WEC_CLASS_HIDE_TABLE || DEFAULT_CLASS_HIDE_TABLE,
    logsTable: runtime.AIRTABLE_WEC_LOGS_TABLE || DEFAULT_LOGS_TABLE,
    sessionsTable: runtime.AIRTABLE_WEC_SESSIONS_TABLE || DEFAULT_SESSIONS_TABLE,
    commentsTable: runtime.AIRTABLE_WEC_COMMENTS_TABLE || DEFAULT_COMMENTS_TABLE
  };
}

async function findSessionRecord(airtable, sessionId) {
  const records = await listAirtableRecords(airtable, airtable.sessionsTable);
  return records.find((record) => clean(record.fields?.session_id) === sessionId) || null;
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
    records.push(...(result.records || []));
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

async function createAirtableRecord(airtable, schema, table, fields) {
  const response = await fetch(airtableUrl(airtable.baseId, table), {
    method: "POST",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      records: [{ fields: filterAirtableFields(schema, table, fields) }],
      typecast: true
    })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`create ${table} ${response.status}: ${JSON.stringify(result)}`);
  return result.records?.[0] || {};
}

async function createWecLog(airtable, schema, fields) {
  const createdAt = new Date().toISOString();
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
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined && value !== null && value !== ""));
}
