function firstValue(value) {
  if (Array.isArray(value)) return value.length ? firstValue(value[0]) : undefined;
  if (value && typeof value === "object" && "name" in value) return value.name;
  return value;
}

function isBlank(value) {
  const v = firstValue(value);
  if (v === null || v === undefined) return true;
  const text = String(v).trim();
  return !text || text.toLowerCase() === "null" || text.toLowerCase() === "nan";
}

function strOrNull(value) {
  return isBlank(value) ? null : String(firstValue(value)).trim();
}

function numOrNull(value) {
  const text = strOrNull(value);
  if (text === null) return null;
  const num = Number(text);
  return Number.isFinite(num) ? num : null;
}

function toIsoDateOnly(value) {
  const text = strOrNull(value);
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function heartbeatLinkIds(value) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => (typeof item === "string" ? item : item?.id)).filter(Boolean);
}

function watchScheduleRowScope(fields = {}) {
  return {
    app_show_id: numOrNull(fields.app_show_idv2 ?? fields.show_id),
    app_sql_date: toIsoDateOnly(
      fields.app_sql_datev2 ??
      fields.schedule_show_datev2 ??
      fields.scheduled_date ??
      fields.show_date
    ),
    app_dow_raw: strOrNull(fields.app_dow_rawv2),
  };
}

function rowMatchesAppScope(fields = {}, appCtx = {}) {
  const row = watchScheduleRowScope(fields);
  const appShowId = numOrNull(appCtx.appShowId ?? appCtx.app_show_id ?? appCtx.app_show_idv2);
  const appSqlDate = toIsoDateOnly(appCtx.appSqlDate ?? appCtx.app_sql_date ?? appCtx.app_sql_datev2);
  const appDowRaw = strOrNull(appCtx.appDowRaw ?? appCtx.app_dow_raw ?? appCtx.app_dow_rawv2);

  if (row.app_show_id === null || appShowId === null || row.app_show_id !== appShowId) return false;
  if (!row.app_sql_date || !appSqlDate || row.app_sql_date !== appSqlDate) return false;
  if (row.app_dow_raw && appDowRaw && row.app_dow_raw !== appDowRaw) return false;
  return true;
}

function linkedHeartbeatDate(fields = {}) {
  return toIsoDateOnly(fields["app_sql_date (from heartbeat)"]);
}

function classifyWatchScheduleHeartbeatRelink(fields = {}, appCtx = {}, heartbeatId) {
  const current = heartbeatLinkIds(fields.heartbeat);
  const matchesScope = rowMatchesAppScope(fields, appCtx);
  const alreadyCorrect = current.length === 1 && current[0] === heartbeatId;

  if (matchesScope) {
    return {
      action: alreadyCorrect ? "keep" : "link",
      current,
      matches_scope: true,
      row_scope: watchScheduleRowScope(fields),
      linked_heartbeat_date: linkedHeartbeatDate(fields),
    };
  }

  const rowDate = watchScheduleRowScope(fields).app_sql_date;
  const hbDate = linkedHeartbeatDate(fields);
  const mismatchedLookup = !!(rowDate && hbDate && rowDate !== hbDate);

  return {
    action: current.length && mismatchedLookup ? "clear" : "skip",
    current,
    matches_scope: false,
    row_scope: watchScheduleRowScope(fields),
    linked_heartbeat_date: hbDate,
  };
}

module.exports = {
  classifyWatchScheduleHeartbeatRelink,
  heartbeatLinkIds,
  rowMatchesAppScope,
  watchScheduleRowScope,
};
