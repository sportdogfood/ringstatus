const {
  resolveRecordScopeDate,
  resolveRecordScopeShowId,
} = require("./watch_trips_scope");

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

function boolValue(value) {
  const v = firstValue(value);
  if (v === true || v === false) return v;
  if (v === 1 || v === "1") return true;
  if (v === 0 || v === "0") return false;
  const text = strOrNull(v);
  if (!text) return false;
  return ["true", "yes", "y", "checked"].includes(text.toLowerCase());
}

function numOrNull(value) {
  const v = firstValue(value);
  if (isBlank(v)) return null;
  const num = Number(v);
  return Number.isFinite(num) ? num : null;
}

function toIsoDateOnly(value) {
  const v = firstValue(value);
  if (isBlank(v)) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(String(v).trim())) return String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}T/.test(String(v).trim())) return String(v).trim().slice(0, 10);
  const parsed = Date.parse(String(v).trim());
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function heartbeatLinkIds(value) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => (typeof item === "string" ? item : item?.id)).filter(Boolean);
}

function watchTripsRowScope(fields = {}) {
  return {
    app_show_id: resolveRecordScopeShowId(fields),
    app_sql_date: resolveRecordScopeDate(fields),
  };
}

function rowMatchesAppScope(fields = {}, appCtx = {}) {
  const row = watchTripsRowScope(fields);
  const appShowId = numOrNull(appCtx.appShowId ?? appCtx.app_show_id ?? appCtx.app_show_idv2);
  const appSqlDate = toIsoDateOnly(appCtx.appSqlDate ?? appCtx.app_sql_date ?? appCtx.app_sql_datev2);

  if (row.app_show_id === null || appShowId === null || row.app_show_id !== appShowId) return false;
  if (!row.app_sql_date || !appSqlDate || row.app_sql_date !== appSqlDate) return false;
  return true;
}

function linkedHeartbeatDate(fields = {}) {
  return toIsoDateOnly(fields["app_sql_date (from heartbeat)"]);
}

function classifyWatchTripsHeartbeatRelink(fields = {}, appCtx = {}, heartbeatId) {
  const current = heartbeatLinkIds(fields.heartbeat);
  const inactive = boolValue(fields.archive) || boolValue(fields.inactive) || !!strOrNull(fields.dropped_at) ||
    String(strOrNull(fields.scope_status) || "").toLowerCase() === "dropped";
  if (inactive) {
    return {
      action: current.length ? "clear" : "skip",
      current,
      matches_scope: false,
      scope_inactive: true,
      row_scope: watchTripsRowScope(fields),
      linked_heartbeat_date: linkedHeartbeatDate(fields),
    };
  }
  const matchesScope = rowMatchesAppScope(fields, appCtx);
  const alreadyCorrect = current.length === 1 && current[0] === heartbeatId;

  if (matchesScope) {
    return {
      action: alreadyCorrect ? "keep" : "link",
      current,
      matches_scope: true,
      row_scope: watchTripsRowScope(fields),
      linked_heartbeat_date: linkedHeartbeatDate(fields),
    };
  }

  const rowDate = watchTripsRowScope(fields).app_sql_date;
  const hbDate = linkedHeartbeatDate(fields);
  const mismatchedLookup = !!(rowDate && hbDate && rowDate !== hbDate);

  return {
    action: current.length && mismatchedLookup ? "clear" : "skip",
    current,
    matches_scope: false,
    row_scope: watchTripsRowScope(fields),
    linked_heartbeat_date: hbDate,
  };
}

module.exports = {
  classifyWatchTripsHeartbeatRelink,
  heartbeatLinkIds,
  rowMatchesAppScope,
  watchTripsRowScope,
};
