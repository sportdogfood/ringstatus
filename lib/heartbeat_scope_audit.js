function isBlank(value) {
  return value === null || value === undefined || String(value).trim() === "";
}

function firstValue(value) {
  if (Array.isArray(value)) return value.length ? firstValue(value[0]) : undefined;
  if (value && typeof value === "object" && "name" in value) return value.name;
  return value;
}

function cleanText(value) {
  const picked = firstValue(value);
  return isBlank(picked) ? null : String(picked).trim();
}

function toIsoDateOnly(value) {
  const text = cleanText(value);
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function numberOrNull(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  const num = Number(picked);
  return Number.isFinite(num) ? num : null;
}

function boolOrNull(value) {
  const picked = firstValue(value);
  if (picked === null || picked === undefined) return null;
  if (typeof picked === "boolean") return picked;
  const text = String(picked).trim().toLowerCase();
  if (!text) return null;
  if (["true", "1", "yes", "checked"].includes(text)) return true;
  if (["false", "0", "no", "unchecked"].includes(text)) return false;
  return null;
}

function pickWithSource(fields, testName, liveName, normalize) {
  const testValue = normalize(fields?.[testName]);
  if (testValue !== null && testValue !== undefined) {
    return { value: testValue, source: testName };
  }
  return { value: normalize(fields?.[liveName]), source: liveName };
}

function resolveShowHeartbeatAuditScope(record) {
  const fields = record?.fields || {};
  const showId = numberOrNull(fields.show_id);
  const customerId = numberOrNull(fields.customer_id);
  const focus = pickWithSource(fields, "focus_day_test", "focus_day", toIsoDateOnly);
  const mode = pickWithSource(fields, "mode_control_test", "mode_control", cleanText);
  const shifted = pickWithSource(fields, "shifted_to_next_day_test", "shifted_to_next_day", boolOrNull);
  const errors = [];

  if (showId === null) errors.push("missing_show_id");
  if (customerId === null) errors.push("missing_customer_id");
  if (!focus.value) errors.push("missing_focus_day");

  const showScopeKey = showId !== null && customerId !== null && focus.value
    ? `${customerId}|${showId}|${focus.value}`
    : "";

  const notes = [];
  if (shifted.value) notes.push("shifted_to_next_day ignored for date");
  if (mode.value) notes.push("mode_control cadence only");
  if (!notes.length) notes.push("focus_day selected directly");

  return {
    show_record_id: record?.id || null,
    show_id: showId,
    customer_id: customerId,
    focus_day: focus.value,
    mode_control: mode.value,
    shifted_to_next_day: shifted.value === true,
    show_scope_key: showScopeKey,
    sources: {
      focus_day: focus.source,
      mode_control: mode.source,
      shifted_to_next_day: shifted.source,
    },
    show_meta: {
      show_name: cleanText(fields.show_name),
      start_date: toIsoDateOnly(fields.start_date),
      end_date: toIsoDateOnly(fields.end_date),
    },
    notes,
    errors,
  };
}

module.exports = {
  resolveShowHeartbeatAuditScope,
};
