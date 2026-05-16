function isBlank(value) {
  return value === null ||
    value === undefined ||
    String(value).trim() === "" ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function firstValue(value) {
  if (Array.isArray(value)) return value.length ? firstValue(value[0]) : undefined;
  if (value && typeof value === "object" && "name" in value) return value.name;
  return value;
}

function keyPart(value) {
  const picked = firstValue(value);
  return isBlank(picked) ? "" : String(picked).trim();
}

function toIsoDateOnly(value) {
  const text = keyPart(value);
  if (!text) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : "";
}

function buildShowScopeKey(input = {}) {
  const customerId = keyPart(input.customerId ?? input.customer_id);
  const showId = keyPart(input.showId ?? input.show_id ?? input.appShowId ?? input.app_show_id);
  const focusDay = toIsoDateOnly(input.focusDay ?? input.focus_day ?? input.appSqlDate ?? input.app_sql_date);
  if (!customerId || !showId || !focusDay) return "";
  return [customerId, showId, focusDay].join("|");
}

module.exports = {
  buildShowScopeKey,
};
