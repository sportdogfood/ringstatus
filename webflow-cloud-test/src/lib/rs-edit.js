export const DEFAULT_RS_EDIT_BASE_ID = "appDN3R51ZPmwgMib";
export const DEFAULT_RS_EDIT_FIELDS_TABLE = "tblJZJm8ODCYyJQQv";
export const DEFAULT_RS_EDIT_EDITS_TABLE = "tblY0MpTNPe9h3F6P";
export const DEFAULT_RS_EDIT_TARGET_PAGE_ID = "6a5e20479253fee5b5fbbe13";
export const DEFAULT_RS_EDIT_IMAGE_FIELD = "fldpZhRxlhGjgDz3m";
export const MAX_RS_EDIT_CHANGES = 100;
export const MAX_RS_EDIT_IMAGE_BYTES = 5 * 1024 * 1024;

const allowedImageTypes = new Set([
  "image/gif",
  "image/jpeg",
  "image/png",
  "image/webp"
]);

export function normalizePageKey(value) {
  const key = String(value || "home").trim().toLowerCase();
  return /^[a-z0-9][a-z0-9_-]{0,63}$/.test(key) ? key : "";
}

export function normalizeFieldType(value) {
  const type = String(value || "").trim().toLowerCase();
  return type === "text" || type === "image" || type === "color" ? type : "";
}

export function normalizeHex(value) {
  const color = String(value || "").trim();
  return /^#[0-9a-f]{6}$/i.test(color) ? color.toUpperCase() : "";
}

export function parseBase64Image(image) {
  if (!image || typeof image !== "object") return { ok: false, error: "missing_image" };
  const filename = String(image.filename || "").trim().slice(0, 180);
  const contentType = String(image.contentType || "").trim().toLowerCase();
  const file = String(image.base64 || "").replace(/^data:[^;]+;base64,/, "").trim();
  if (!filename) return { ok: false, error: "missing_image_filename" };
  if (!allowedImageTypes.has(contentType)) return { ok: false, error: "unsupported_image_type" };
  if (!file || !/^[a-z0-9+/=]+$/i.test(file)) return { ok: false, error: "invalid_image_base64" };
  const bytes = Math.floor(file.length * 3 / 4) - (file.endsWith("==") ? 2 : file.endsWith("=") ? 1 : 0);
  if (bytes <= 0 || bytes > MAX_RS_EDIT_IMAGE_BYTES) return { ok: false, error: "image_too_large" };
  return { ok: true, filename, contentType, file, bytes };
}

export function validateChanges(payload, allowlist) {
  if (!payload || typeof payload !== "object") return { ok: false, error: "invalid_payload" };
  const pageKey = normalizePageKey(payload.pageKey);
  if (!pageKey) return { ok: false, error: "invalid_page_key" };
  if (!Array.isArray(payload.changes) || !payload.changes.length) return { ok: false, error: "missing_changes" };
  if (payload.changes.length > MAX_RS_EDIT_CHANGES) return { ok: false, error: "too_many_changes" };

  const seen = new Set();
  const changes = [];
  for (const input of payload.changes) {
    const fieldKey = String(input?.fieldKey || "").trim();
    const field = allowlist.get(fieldKey);
    if (!field || !field.editable || field.pageKey !== pageKey) {
      return { ok: false, error: "field_not_allowed", fieldKey };
    }
    if (seen.has(fieldKey)) return { ok: false, error: "duplicate_field", fieldKey };
    seen.add(fieldKey);

    const fieldType = normalizeFieldType(input.fieldType);
    if (!fieldType || fieldType !== field.fieldType) {
      return { ok: false, error: "field_type_mismatch", fieldKey };
    }

    if (fieldType === "text") {
      const textContent = String(input.textContent ?? "");
      if (!textContent.trim()) return { ok: false, error: "empty_text", fieldKey };
      if (textContent.length > 10000) return { ok: false, error: "text_too_long", fieldKey };
      changes.push({ field, fieldKey, fieldType, textContent });
      continue;
    }

    if (fieldType === "color") {
      const colorHex = normalizeHex(input.colorHex);
      if (!colorHex) return { ok: false, error: "invalid_color", fieldKey };
      changes.push({ field, fieldKey, fieldType, colorHex });
      continue;
    }

    const image = parseBase64Image(input.image);
    if (!image.ok) return { ok: false, error: image.error, fieldKey };
    changes.push({ field, fieldKey, fieldType, image });
  }

  return { ok: true, pageKey, changes };
}

export function safeEditField(record) {
  const fields = record?.fields || {};
  return {
    recordId: record.id,
    fieldKey: String(fields.field_key || ""),
    pageKey: String(fields.source_table || ""),
    page: firstLink(fields.page),
    mcp: firstLink(fields.mcp),
    contentAction: firstLink(fields.content_action),
    fieldType: choiceName(fields.field_type),
    elementId: String(fields.element_id || ""),
    textContent: String(fields.text_content || ""),
    colorHex: normalizeHex(fields.color_hex),
    image: Array.isArray(fields.image) ? fields.image : [],
    mcpSort: numberOrBlank(fields.mcp_sort),
    mainSort: numberOrBlank(fields.main_sort),
    cardIter: numberOrBlank(fields.card_iter),
    tagIter: numberOrBlank(fields.tag_iter),
    sourceRecordId: String(fields.source_record_id || ""),
    editable: fields.editable === true,
    editorClass: String(fields.editor_class || "")
  };
}

export function homeBackgroundFields(bindings) {
  const parents = new Map();
  for (const binding of bindings) {
    if (!binding?.parentElementId || parents.has(binding.parentElementId)) continue;
    parents.set(binding.parentElementId, {
      recordId: "",
      fieldKey: `home:bg:${binding.parentElementId}`,
      pageKey: "home",
      page: "",
      mcp: "",
      contentAction: "",
      fieldType: "color",
      elementId: binding.parentElementId,
      textContent: "",
      colorHex: "#FFFFFF",
      image: [],
      mcpSort: "",
      mainSort: "",
      cardIter: "",
      tagIter: "",
      sourceRecordId: "",
      editable: true,
      editorClass: "rs-edit-background"
    });
  }
  return [...parents.values()];
}

function firstLink(value) {
  const item = Array.isArray(value) ? value[0] : "";
  return typeof item === "string" ? item : String(item?.id || "");
}

function choiceName(value) {
  return typeof value === "string" ? value : String(value?.name || "");
}

function numberOrBlank(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : "";
}
