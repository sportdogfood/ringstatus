const READ_ONLY_FIELD_TYPES = new Set([
  "formula",
  "rollup",
  "lookup",
  "count",
  "autoNumber",
  "createdTime",
  "lastModifiedTime",
  "createdBy",
  "lastModifiedBy",
  "button",
  "externalSyncSource",
]);

function isBlank(value) {
  return value === null || value === undefined || String(value).trim() === "";
}

function isWritableAirtableField(field) {
  if (!field || typeof field !== "object") return false;
  if (field.isComputed === true) return false;
  return !READ_ONLY_FIELD_TYPES.has(String(field.type || ""));
}

function buildAirtableFieldMeta(fields = []) {
  const names = new Set();
  const writableNames = new Set();
  const actualByTrim = new Map();
  const writableByTrim = new Map();

  for (const field of Array.isArray(fields) ? fields : []) {
    const actualName = String(field?.name || "");
    const trimmedName = actualName.trim();
    if (!trimmedName) continue;
    names.add(trimmedName);
    if (!actualByTrim.has(trimmedName)) actualByTrim.set(trimmedName, actualName);
    if (isWritableAirtableField(field)) {
      writableNames.add(trimmedName);
      if (!writableByTrim.has(trimmedName)) writableByTrim.set(trimmedName, actualName);
    }
  }

  return { names, writableNames, actualByTrim, writableByTrim };
}

function writableFieldName(fieldMetaOrSet, logicalName) {
  if (!fieldMetaOrSet || !logicalName) return null;
  if (fieldMetaOrSet instanceof Set) {
    return fieldMetaOrSet.has(logicalName) ? logicalName : null;
  }
  if (fieldMetaOrSet.writableByTrim?.has(logicalName)) return fieldMetaOrSet.writableByTrim.get(logicalName);
  if (fieldMetaOrSet.writableNames?.has(logicalName)) return logicalName;
  return null;
}

function setWritableField(fields, fieldMetaOrSet, logicalName, value) {
  const actualName = writableFieldName(fieldMetaOrSet, logicalName);
  if (!actualName || value === undefined || value === null) return;
  if (typeof value === "string" && value.trim() === "") return;
  fields[actualName] = value;
}

function buildScopeFieldPatch(fieldMetaOrSet, scope = {}) {
  const fields = {};
  const customerId = scope.customerId ?? scope.customer_id;
  const focusDay = scope.focusDay ?? scope.focus_day;
  const ringCollection = scope.ringCollection ?? scope.ring_collection;
  const showScopeKey = scope.showScopeKey ?? scope.show_scope_key;
  const showRecordId = scope.showRecordId ?? scope.show_record_id;

  setWritableField(fields, fieldMetaOrSet, "customer_id", customerId);
  setWritableField(fields, fieldMetaOrSet, "focus_day", focusDay);
  setWritableField(fields, fieldMetaOrSet, "ring_collection", ringCollection);
  setWritableField(fields, fieldMetaOrSet, "show_scope_key", showScopeKey);
  if (!isBlank(showRecordId)) {
    setWritableField(fields, fieldMetaOrSet, "show", [String(showRecordId)]);
  }

  return fields;
}

module.exports = {
  buildAirtableFieldMeta,
  buildScopeFieldPatch,
  isWritableAirtableField,
  setWritableField,
  writableFieldName,
};
