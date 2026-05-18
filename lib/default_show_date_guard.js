const { normalizeHeartbeatMode } = require("./heartbeat_mode");

const DEFAULT_MAX_LOOKAHEAD_DAYS = Math.max(0, Number(process.env.DEFAULT_SHOW_DATE_MAX_LOOKAHEAD_DAYS || "21") || 21);
const DEFAULT_MIN_WINDOW_DAYS = Math.max(0, Number(process.env.DEFAULT_SHOW_MIN_WINDOW_DAYS || "2") || 2);

function firstValue(value) {
  if (Array.isArray(value)) return value.length ? firstValue(value[0]) : undefined;
  if (value && typeof value === "object" && "name" in value) return value.name;
  return value;
}

function strOrNull(value) {
  const v = firstValue(value);
  if (v === null || v === undefined) return null;
  const text = String(v).trim();
  if (!text || text.toLowerCase() === "null" || text.toLowerCase() === "nan") return null;
  return text;
}

function boolValue(value) {
  const v = firstValue(value);
  if (typeof v === "boolean") return v;
  if (v === 1 || v === "1") return true;
  if (v === 0 || v === "0") return false;
  const text = String(v ?? "").trim().toLowerCase();
  return text === "true" || text === "yes" || text === "y" || text === "checked";
}

function toIsoDateOnly(value) {
  const text = strOrNull(value);
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function daysBetweenSqlDates(left, right) {
  const a = toIsoDateOnly(left);
  const b = toIsoDateOnly(right);
  if (!a || !b) return null;
  const leftMs = Date.parse(`${a}T00:00:00Z`);
  const rightMs = Date.parse(`${b}T00:00:00Z`);
  if (!Number.isFinite(leftMs) || !Number.isFinite(rightMs)) return null;
  return Math.round((rightMs - leftMs) / 86400000);
}

function sqlDateInRange(value, start, end) {
  const date = toIsoDateOnly(value);
  const startDate = toIsoDateOnly(start);
  const endDate = toIsoDateOnly(end);
  if (!date || !startDate || !endDate) return false;
  return date >= startDate && date <= endDate;
}

function computeDefaultShowDateGuard({
  rawSqlDate,
  appSqlDate,
  defaultAppSqlDateIs,
  showAppSqlStartDate,
  showAppSqlEndDate,
  setToDefaultAppSqlDate,
  maxLookaheadDays = DEFAULT_MAX_LOOKAHEAD_DAYS,
  minWindowDays = DEFAULT_MIN_WINDOW_DAYS,
} = {}) {
  const reasons = [];
  const rawDate = toIsoDateOnly(rawSqlDate);
  const appDate = toIsoDateOnly(appSqlDate);
  const defaultDate = toIsoDateOnly(defaultAppSqlDateIs);
  const startDate = toIsoDateOnly(showAppSqlStartDate);
  const endDate = toIsoDateOnly(showAppSqlEndDate);
  const showWindowDays = startDate && endDate ? daysBetweenSqlDates(startDate, endDate) : null;
  const defaultDaysFromRaw = rawDate && defaultDate ? daysBetweenSqlDates(rawDate, defaultDate) : null;
  const appDaysFromRaw = rawDate && appDate ? daysBetweenSqlDates(rawDate, appDate) : null;

  if (!startDate || !endDate) {
    reasons.push("missing_show_window");
  } else if (showWindowDays !== null && showWindowDays < minWindowDays) {
    reasons.push("short_show_window");
  }

  if (defaultDate && startDate && endDate && !sqlDateInRange(defaultDate, startDate, endDate)) {
    reasons.push("default_outside_show_window");
  }
  if (appDate && startDate && endDate && !sqlDateInRange(appDate, startDate, endDate)) {
    reasons.push("app_outside_show_window");
  }
  if (defaultDaysFromRaw !== null && Math.abs(defaultDaysFromRaw) > maxLookaheadDays) {
    reasons.push("default_too_far_from_raw");
  }
  if (appDaysFromRaw !== null && Math.abs(appDaysFromRaw) > maxLookaheadDays) {
    reasons.push("app_too_far_from_raw");
  }

  const checkShowDate = boolValue(setToDefaultAppSqlDate) && reasons.length > 0;
  return {
    check_show_date: checkShowDate,
    default_show_date_status: checkShowDate ? "needs_manual_confirmation" : "ok_no_review_needed",
    default_show_date_reason: reasons.join("|") || "ok",
    default_show_date_metrics: {
      raw_sql_date: rawDate,
      app_sql_date: appDate,
      default_app_sql_date_is: defaultDate,
      show_app_sql_start_date: startDate,
      show_app_sql_end_date: endDate,
      show_window_days: showWindowDays,
      default_days_from_raw: defaultDaysFromRaw,
      app_days_from_raw: appDaysFromRaw,
      max_lookahead_days: maxLookaheadDays,
      min_window_days: minWindowDays,
    },
  };
}

function normalizeControlMode(value) {
  const text = strOrNull(value);
  if (!text) return null;
  const upper = text.toUpperCase();
  if (upper === "AUTO") return "AUTO";
  return normalizeHeartbeatMode(upper, null);
}

function isOperationalMode(mode) {
  return mode === "DAY" || mode === "NIGHT" || mode === "OVERNIGHT";
}

function modeForDateContext(controlMode, clockMode) {
  const normalized = normalizeControlMode(controlMode);
  return isOperationalMode(normalized) ? normalized : normalizeHeartbeatMode(clockMode, "DAY");
}

function decideEffectiveMode({
  clockMode,
  forcedMode,
  defaultShowDateGuard,
  showControl = {},
} = {}) {
  const normalizedClockMode = normalizeHeartbeatMode(clockMode, "DAY");
  const normalizedForceMode = normalizeControlMode(forcedMode);
  const normalizedControlMode = normalizeControlMode(showControl.mode_control);
  const guard = defaultShowDateGuard || computeDefaultShowDateGuard({});
  const manualOverride = boolValue(showControl.is_default_show_manual_override);
  const guardedStatus = guard.check_show_date
    ? (manualOverride ? "confirmed_default_show_date" : "needs_manual_confirmation")
    : "ok_no_review_needed";

  if (normalizedForceMode && normalizedForceMode !== "AUTO") {
    return {
      mode: normalizedForceMode,
      mode_source: "force_mode",
      mode_reason: "FORCE_MODE",
      default_show_date_status: guardedStatus,
    };
  }

  if (normalizedControlMode && normalizedControlMode !== "AUTO") {
    return {
      mode: normalizedControlMode,
      mode_source: "show_manual",
      mode_reason: `show.mode_control:${normalizedControlMode}`,
      default_show_date_status: guardedStatus,
    };
  }

  if (guard.check_show_date && !manualOverride) {
    return {
      mode: "OFF",
      mode_source: "default_show_date_guard",
      mode_reason: guard.default_show_date_reason || "needs_manual_confirmation",
      default_show_date_status: "needs_manual_confirmation",
    };
  }

  return {
    mode: normalizedClockMode,
    mode_source: "clock",
    mode_reason: "clock_mode",
    default_show_date_status: guardedStatus,
  };
}

module.exports = {
  boolValue,
  computeDefaultShowDateGuard,
  daysBetweenSqlDates,
  decideEffectiveMode,
  modeForDateContext,
  normalizeControlMode,
  sqlDateInRange,
  strOrNull,
  toIsoDateOnly,
};
