function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function firstValue(value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      if (!isBlank(item)) return item;
    }
    return undefined;
  }
  return value;
}

function pickFirst(...values) {
  for (const value of values) {
    const picked = firstValue(value);
    if (!isBlank(picked)) return picked;
  }
  return undefined;
}

function numOrNull(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  const num = Number(picked);
  return Number.isFinite(num) ? num : null;
}

function toDateOnly(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  if (picked instanceof Date) return picked.toISOString().slice(0, 10);

  const text = String(picked).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);

  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function boolValue(value) {
  const picked = firstValue(value);
  if (typeof picked === "boolean") return picked;
  if (typeof picked === "number") return picked !== 0;
  if (typeof picked === "string") {
    const text = picked.trim().toLowerCase();
    if (["true", "yes", "1", "checked"].includes(text)) return true;
    if (["false", "no", "0", "unchecked"].includes(text)) return false;
  }
  return null;
}

function resolveRecordScopeShowId(fields = {}) {
  return numOrNull(pickFirst(
    fields.show_id,
    fields.app_show_id,
    fields.app_show_idv2,
    fields.app_sid
  ));
}

function resolveRecordScopeDate(fields = {}) {
  return toDateOnly(pickFirst(
    fields.schedule_show_datev2,
    fields.scheduled_date,
    fields[" scheduled_date"],
    fields["schedule_show_datev2 (from watch_schedule)"],
    fields.show_date,
    fields.app_sql_date,
    fields.app_sql_datev2,
    fields.app_dt,
    fields.date
  ));
}

function recordIsExplicitlyNonCurrent(fields = {}) {
  const scopeStatus = String(firstValue(fields.scope_status) || "").trim().toLowerCase();
  if (scopeStatus === "dropped") return true;

  const isCurrentScope = boolValue(fields.is_current_scope);
  if (isCurrentScope === false) return true;

  const inactive = boolValue(fields.inactive);
  if (inactive === true) return true;

  return false;
}

function recordMatchesAppScope(fields = {}, appCtx = {}) {
  if (recordIsExplicitlyNonCurrent(fields)) return false;

  const expectedShowId = numOrNull(appCtx.app_show_id);
  const expectedDate = toDateOnly(appCtx.app_sql_date);
  const recordShowId = resolveRecordScopeShowId(fields);
  const recordDate = resolveRecordScopeDate(fields);

  if (expectedShowId === null || !expectedDate) return false;
  return recordShowId === expectedShowId && recordDate === expectedDate;
}

module.exports = {
  recordIsExplicitlyNonCurrent,
  recordMatchesAppScope,
  resolveRecordScopeDate,
  resolveRecordScopeShowId,
};
