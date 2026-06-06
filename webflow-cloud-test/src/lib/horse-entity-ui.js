export const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const HORSE_ENTITY_ALLOWED_WRITE_FIELDS = [
  "horse",
  "barn_name",
  "show_name",
  "active",
  "inactive",
  "wec_wave_1",
  "wec_wave_2",
  "wec_not_going",
  "horse_gender",
  "horse_genders",
  "horse_disciplines",
  "horse_color",
  "horse_colors",
  "notes"
];

export const HORSE_ENTITY_ALLOWED_CREATE_FIELDS = HORSE_ENTITY_ALLOWED_WRITE_FIELDS;

const DEFAULT_META_TABLE = "tbllJywsOstkqT5yZ";
const MODULE_KEY = "horse_entity_ui";
const ROSTER_TABLE = "pak_horses_roster";
const PROFILE_TABLE = "pak_horses_profiles";
const GENDER_TABLE = "horse_genders";
const DISCIPLINE_TABLE = "horse_disciplines";
const COLOR_TABLE = "horse_colors";
const LOG_TABLE = "horses_change_log";
const COMMENTS_TABLE = "wec_commenting";

const TABLE_NAMES = [
  ROSTER_TABLE,
  PROFILE_TABLE,
  GENDER_TABLE,
  DISCIPLINE_TABLE,
  COLOR_TABLE,
  LOG_TABLE,
  COMMENTS_TABLE
];

const READ_FIELDS = {
  pak_horses_roster: [
    "pak_horse_id",
    "horse",
    "show_name",
    "barn_name",
    "display_horse_barn_name",
    "active",
    "inactive",
    "wec_wave_1",
    "wec_wave_2",
    "wec_not_going",
    "wec_show",
    "pak_grooms",
    "wec_horses",
    "ww_horses",
    "wec_list_plans",
    "wec_pack_lists",
    "pak_kits",
    "pak_kit_items",
    "pack_items",
    "pack_waves",
    "comments",
    "horse_roster_logs",
    "rec_id",
    "table_name",
    "table_api",
    "notes",
    "sort_order",
    "profile_url",
    "entry_uri",
    "search_uri",
    "url",
    "link"
  ],
  pak_horses_profiles: [
    "horse",
    "pak_horses_roster",
    "barn_name",
    "show_name",
    "profile_url",
    "notes",
    "active"
  ],
  horse_genders: ["gender", "display_label", "horse_attribute", "status", "sort_order", "notes"],
  horse_disciplines: ["discipline", "display_label", "horse_attribute", "status", "sort_order", "notes"],
  horse_colors: ["color", "display_label", "horse_attribute", "status", "sort_order", "notes"],
  horses_change_log: [
    "horse",
    "horse_record_id",
    "horse_id",
    "horse_ids",
    "horse_key",
    "horse_name",
    "barn_name",
    "show_name",
    "field_name",
    "old_value",
    "new_value",
    "changed_at",
    "updated_at",
    "created_by",
    "source",
    "status",
    "payload_json",
    "raw_payload",
    "notes",
    "app_sid"
  ],
  wec_commenting: [
    "event",
    "horse",
    "scope_type",
    "scope_id",
    "scope_label",
    "comment_status",
    "comment",
    "created_at",
    "created_by",
    "updated_at",
    "updated_by",
    "notes"
  ]
};

const STACK = [
  "header",
  "primary_tabs",
  "summary_aggs",
  "search",
  "main_table",
  "drawer_detail",
  "list_memberships",
  "comments",
  "change_log"
];

export function runtimeEnv(extra = {}) {
  const localEnv = globalThis.process?.env || {};
  const astroEnv = import.meta.env || {};
  return { ...localEnv, ...astroEnv, ...extra };
}

export function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}

export function airtableConfig(runtime = runtimeEnv()) {
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_BASE_ID || runtime.AIRTABLE_BASE;
  const metaTable = runtime.AIRTABLE_WEC_META_TABLE || DEFAULT_META_TABLE;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  return { ok: true, token, baseId, metaTable, runtime };
}

export async function horseEntityReport(airtable, requestUrl, adapter = createAirtableAdapter(airtable)) {
  const url = new URL(requestUrl);
  const query = clean(url.searchParams.get("q") || url.searchParams.get("search"));
  const schema = await adapter.schema();
  const source = sourceContract(schema);
  assertRequiredSource(source);

  const [
    horseRecords,
    genderRecords,
    disciplineRecords,
    colorRecords,
    changeRecords,
    commentRecords
  ] = await Promise.all([
    adapter.listRecords(ROSTER_TABLE, { fields: allowedReadFields(schema, ROSTER_TABLE) }),
    adapter.listRecords(GENDER_TABLE, { fields: allowedReadFields(schema, GENDER_TABLE) }),
    adapter.listRecords(DISCIPLINE_TABLE, { fields: allowedReadFields(schema, DISCIPLINE_TABLE) }),
    adapter.listRecords(COLOR_TABLE, { fields: allowedReadFields(schema, COLOR_TABLE) }),
    adapter.listRecords(LOG_TABLE, { fields: allowedReadFields(schema, LOG_TABLE) }),
    adapter.listRecords(COMMENTS_TABLE, { fields: allowedReadFields(schema, COMMENTS_TABLE) })
  ]);

  const horses = horseRecords.map(normalizeHorse).filter((horse) => matchesSearch(horse, query)).sort(compareHorses);
  const visibleHorseIds = new Set(horses.map((horse) => horse.id));
  const comments = commentRecords.map(normalizeComment).filter((row) => row.active && (!row.horseIds.length || row.horseIds.some((id) => visibleHorseIds.has(id))));
  const changeLog = changeRecords.map(normalizeChangeLog).filter((row) => !row.horseId || visibleHorseIds.has(row.horseId)).sort(compareRecentRows);

  return {
    ok: true,
    v: 1,
    moduleKey: MODULE_KEY,
    source,
    allowedFields: {
      read: READ_FIELDS,
      create: allowedCreateFields(schema),
      write: allowedWriteFields(schema)
    },
    stack: STACK,
    counts: {
      horses: horses.length,
      active: horses.filter((horse) => horse.active).length,
      inactive: horses.filter((horse) => !horse.active).length,
      comments: comments.length,
      changes: changeLog.length
    },
    attributes: {
      gender: genderRecords.map((record) => normalizeAttribute(record, "gender")).filter((row) => row.active).sort(compareAttributes),
      disciplines: disciplineRecords.map((record) => normalizeAttribute(record, "discipline")).filter((row) => row.active).sort(compareAttributes),
      colors: colorRecords.map((record) => normalizeAttribute(record, "color")).filter((row) => row.active).sort(compareAttributes)
    },
    horses,
    comments,
    changeLog
  };
}

export async function horseEntityActionReport(airtable, requestUrl, payload, adapter = createAirtableAdapter(airtable)) {
  const action = clean(payload?.action);
  const schema = await adapter.schema();
  const source = sourceContract(schema);
  assertRequiredSource(source);

  let result;
  if (action === "add_horse") {
    result = await addHorse(adapter, schema, payload);
  } else if (action === "edit_horse") {
    result = await editHorse(adapter, schema, payload);
  } else if (action === "apply_horse_attribute") {
    result = await applyHorseAttribute(adapter, schema, payload);
  } else {
    return { ok: false, error: "unknown_horse_entity_action", action };
  }

  return {
    ok: true,
    v: 1,
    moduleKey: MODULE_KEY,
    action,
    result,
    state: await horseEntityReport(airtable, requestUrl, adapter)
  };
}

async function addHorse(adapter, schema, payload) {
  const fields = fieldsAllowedBySchema(pickAllowed(payload?.fields || {}, HORSE_ENTITY_ALLOWED_CREATE_FIELDS), schema, ROSTER_TABLE);
  if (!Object.keys(fields).length) throw new Error("no_allowed_horse_create_fields");
  if (payload?.dryRun === true) {
    return { dryRun: true, action: "add_horse", fields };
  }
  const created = await adapter.createRecord(ROSTER_TABLE, fields);
  const logs = [];
  for (const [fieldName, newValue] of Object.entries(fields)) {
    logs.push(await createAuditLog(adapter, schema, {
      changeType: "horse_added",
      horseId: created.id,
      horseName: horseNameFromFields({ ...fields, ...(created.fields || {}) }),
      fieldName,
      oldValue: "",
      newValue,
      payload
    }));
  }
  return { created, logs };
}

async function editHorse(adapter, schema, payload) {
  const horseId = clean(payload?.horseId || payload?.recordId);
  if (!horseId) throw new Error("missing_horse_id");
  const records = await adapter.listRecords(ROSTER_TABLE, { fields: allowedReadFields(schema, ROSTER_TABLE) });
  const before = records.find((record) => record.id === horseId);
  if (!before) throw new Error("horse_not_found");
  const fields = fieldsAllowedBySchema(pickAllowed(payload?.fields || {}, HORSE_ENTITY_ALLOWED_WRITE_FIELDS), schema, ROSTER_TABLE);
  if (!Object.keys(fields).length) throw new Error("no_allowed_horse_write_fields");
  if (payload?.dryRun === true) {
    return { dryRun: true, action: "edit_horse", horseId, fields };
  }
  const updated = await adapter.patchRecord(ROSTER_TABLE, horseId, fields);
  const logs = [];
  for (const [fieldName, newValue] of Object.entries(fields)) {
    logs.push(await createAuditLog(adapter, schema, {
      changeType: "horse_edited",
      horseId,
      horseName: horseNameFromFields({ ...(before.fields || {}), ...fields }),
      fieldName,
      oldValue: before.fields?.[fieldName],
      newValue,
      payload
    }));
  }
  return { updated, logs };
}

async function applyHorseAttribute(adapter, schema, payload) {
  const group = slugify(payload?.attributeGroup || payload?.group);
  const fieldName = attributeFieldForGroup(group);
  if (!fieldName) throw new Error("invalid_attribute_group");
  return editHorse(adapter, schema, {
    ...payload,
    action: "edit_horse",
    fields: {
      [fieldName]: clean(payload?.attributeValue || payload?.value || payload?.attributeLabel)
    }
  });
}

function attributeFieldForGroup(group) {
  if (group === "horse_gender" || group === "horse_genders" || group === "gender") return "horse_gender";
  if (group === "horse_disciplines" || group === "disciplines") return "horse_disciplines";
  if (group === "horse_colors" || group === "colors") return "horse_color";
  return "";
}

function allowedWriteFields(schema) {
  return fieldListAllowedBySchema(HORSE_ENTITY_ALLOWED_WRITE_FIELDS, schema, ROSTER_TABLE);
}

function allowedCreateFields(schema) {
  return fieldListAllowedBySchema(HORSE_ENTITY_ALLOWED_CREATE_FIELDS, schema, ROSTER_TABLE);
}

function fieldListAllowedBySchema(fields, schema, tableName) {
  const schemaNames = new Set((findSchemaTable(schema, tableName)?.fields || []).map((field) => field.name));
  return fields.filter((field) => !schemaNames.size || schemaNames.has(field));
}

async function createAuditLog(adapter, schema, { changeType, horseId, horseName, fieldName, oldValue, newValue, payload }) {
  const fields = fieldsAllowedBySchema({
    horse_record_id: horseId,
    horse_id: horseId,
    horse_name: horseName,
    field_name: fieldName,
    old_value: stringifyValue(oldValue),
    new_value: stringifyValue(newValue),
    changed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    created_by: clean(payload?.user || payload?.createdBy || payload?.updatedBy || "webflow"),
    source: MODULE_KEY,
    status: "logged",
    app_sid: clean(payload?.sessionId || payload?.session_id),
    notes: clean(payload?.notes),
    payload_json: JSON.stringify({
      moduleKey: MODULE_KEY,
      changeType,
      actionKey: clean(payload?.actionKey),
      optimisticKey: clean(payload?.optimisticKey)
    })
  }, schema, LOG_TABLE);
  return adapter.createRecord(LOG_TABLE, fields);
}

function sourceContract(schema) {
  const tableMap = new Map((schema?.tables || []).map((table) => [table.name, table]));
  return {
    horseSource: ROSTER_TABLE,
    profileSource: tableMap.has(PROFILE_TABLE) ? PROFILE_TABLE : "",
    attributeSources: {
      gender: tableMap.has("horse_gender") ? "horse_gender" : GENDER_TABLE,
      disciplines: DISCIPLINE_TABLE,
      colors: COLOR_TABLE
    },
    commentsSource: tableMap.has(COMMENTS_TABLE) ? COMMENTS_TABLE : "",
    changeLogSource: LOG_TABLE,
    tables: Object.fromEntries(TABLE_NAMES.map((name) => [name, tableMap.get(name)?.id || ""]))
  };
}

function assertRequiredSource(source) {
  for (const name of [ROSTER_TABLE, GENDER_TABLE, DISCIPLINE_TABLE, COLOR_TABLE, LOG_TABLE]) {
    if (!source.tables[name]) throw new Error(`horse_entity_missing_table:${name}`);
  }
}

function allowedReadFields(schema, tableName) {
  return fieldsAllowedBySchema(Object.fromEntries((READ_FIELDS[tableName] || []).map((field) => [field, true])), schema, tableName, true);
}

function fieldsAllowedBySchema(fields, schema, tableName, returnKeys = false) {
  const schemaNames = new Set((findSchemaTable(schema, tableName)?.fields || []).map((field) => field.name));
  const result = {};
  for (const [key, value] of Object.entries(fields || {})) {
    if (!schemaNames.size || schemaNames.has(key)) result[key] = value;
  }
  return returnKeys ? Object.keys(result) : result;
}

function findSchemaTable(schema, name) {
  return (schema?.tables || []).find((table) => table.name === name) || null;
}

function pickAllowed(fields, allowed) {
  const allowedSet = new Set(allowed);
  const result = {};
  for (const [key, value] of Object.entries(fields || {})) {
    if (allowedSet.has(key)) result[key] = value;
  }
  return result;
}

function normalizeHorse(record) {
  const fields = record.fields || {};
  const inactive = !!fields.inactive || !!fields.wec_not_going;
  return {
    id: record.id,
    name: horseNameFromFields(fields),
    barnName: stringField(fields.barn_name || fields.display_horse_barn_name || fields.horse),
    showName: stringField(fields.show_name || fields.horse),
    active: fields.active === true || !inactive,
    inactive,
    waveOne: !!fields.wec_wave_1,
    waveTwo: !!fields.wec_wave_2,
    notGoing: !!fields.wec_not_going,
    notes: stringField(fields.notes),
    profileUrl: firstUrl(fields),
    memberships: {
      waveKeys: membershipWaveKeys(fields),
      planIds: linkedIds(fields.wec_list_plans),
      packListIds: linkedIds(fields.wec_pack_lists),
      packWaveIds: linkedIds(fields.pack_waves),
      kitIds: linkedIds(fields.pak_kits),
      kitItemIds: linkedIds(fields.pak_kit_items),
      sourceHorseIds: linkedIds(fields.ww_horses || fields.wec_horses),
      commentIds: linkedIds(fields.comments)
    }
  };
}

function normalizeAttribute(record, fieldName) {
  const fields = record.fields || {};
  const label = stringField(fields.display_label || fields[fieldName] || fields.horse_attribute);
  return {
    id: record.id,
    label,
    value: label,
    active: slugify(fields.status || "active") !== "inactive",
    sortOrder: numberField(fields.sort_order)
  };
}

function normalizeComment(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    comment: stringField(fields.comment || fields.notes),
    scopeType: slugify(fields.scope_type || "horse"),
    scopeId: stringField(fields.scope_id),
    scopeLabel: stringField(fields.scope_label),
    horseIds: linkedIds(fields.horse),
    active: slugify(fields.comment_status || "active") !== "inactive",
    createdAt: stringField(fields.created_at || fields.Created),
    createdBy: stringField(fields.created_by)
  };
}

function normalizeChangeLog(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    horseId: stringField(fields.horse_record_id || fields.horse_id || firstLinkedId(fields.horse)),
    horseName: stringField(fields.horse_name || fields.barn_name || fields.show_name || fields.horse),
    fieldName: stringField(fields.field_name),
    oldValue: stringField(fields.old_value),
    newValue: stringField(fields.new_value),
    changedAt: stringField(fields.changed_at || fields.updated_at),
    changedBy: stringField(fields.created_by),
    source: stringField(fields.source),
    status: stringField(fields.status)
  };
}

export function createAirtableAdapter(airtable) {
  let schemaCache = null;
  return {
    async schema() {
      if (!schemaCache) schemaCache = await getBaseSchema(airtable);
      return schemaCache;
    },
    async listRecords(tableName, options = {}) {
      const schema = await this.schema();
      const table = findSchemaTable(schema, tableName);
      if (!table?.id) return [];
      return listAirtableRecords(airtable, table.id, "", options);
    },
    async createRecord(tableName, fields) {
      const schema = await this.schema();
      const table = findSchemaTable(schema, tableName);
      if (!table?.id) throw new Error(`missing_table:${tableName}`);
      return createAirtableRecord(airtable, table.id, fields);
    },
    async patchRecord(tableName, recordId, fields) {
      const schema = await this.schema();
      const table = findSchemaTable(schema, tableName);
      if (!table?.id) throw new Error(`missing_table:${tableName}`);
      return patchAirtableRecord(airtable, table.id, recordId, fields);
    }
  };
}

async function getBaseSchema(airtable) {
  const response = await airtableFetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
    headers: { Authorization: `Bearer ${airtable.token}` }
  }, "airtable_schema_failed");
  return response.json();
}

async function listAirtableRecords(airtable, tableId, view = "", options = {}) {
  const out = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(airtable.baseId)}/${encodeURIComponent(tableId)}`);
    if (view) url.searchParams.set("view", view);
    for (const field of options.fields || []) url.searchParams.append("fields[]", field);
    if (offset) url.searchParams.set("offset", offset);
    const response = await airtableFetch(url, { headers: { Authorization: `Bearer ${airtable.token}` } }, `airtable_list_failed:${tableId}`);
    const data = await response.json();
    out.push(...(data.records || []));
    offset = data.offset || "";
  } while (offset);
  return out;
}

async function createAirtableRecord(airtable, tableId, fields) {
  const response = await airtableFetch(`https://api.airtable.com/v0/${encodeURIComponent(airtable.baseId)}/${encodeURIComponent(tableId)}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${airtable.token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ fields })
  }, `airtable_create_failed:${tableId}`);
  return response.json();
}

async function patchAirtableRecord(airtable, tableId, recordId, fields) {
  const response = await airtableFetch(`https://api.airtable.com/v0/${encodeURIComponent(airtable.baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(recordId)}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${airtable.token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ fields })
  }, `airtable_patch_failed:${tableId}`);
  return response.json();
}

async function airtableFetch(url, options, errorPrefix) {
  let lastStatus = 0;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const response = await fetch(url, options);
    if (response.ok) return response;
    lastStatus = response.status;
    if (response.status !== 429 && response.status < 500) break;
    const retryAfter = Number(response.headers.get("retry-after"));
    const delay = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 350 * (attempt + 1);
    await sleep(delay);
  }
  throw new Error(`${errorPrefix}:${lastStatus}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function matchesSearch(horse, query) {
  if (!query) return true;
  const haystack = [horse.name, horse.barnName, horse.showName, horse.notes].join(" ").toLowerCase();
  return haystack.includes(query.toLowerCase());
}

function membershipWaveKeys(fields) {
  const keys = [];
  if (fields.wec_wave_1) keys.push("wave_one");
  if (fields.wec_wave_2) keys.push("wave_two");
  if (fields.wec_not_going) keys.push("not_going");
  return keys;
}

function horseNameFromFields(fields) {
  return stringField(fields.display_horse_barn_name || fields.barn_name || fields.horse || fields.show_name);
}

function firstUrl(fields) {
  return stringField(fields.profile_url || fields.entry_uri || fields.search_uri || fields.url || fields.link);
}

function linkedIds(value) {
  if (!value) return [];
  const values = Array.isArray(value) ? value : [value];
  return values.map((item) => {
    if (typeof item === "string") return item;
    return item?.id || item?.recordId || "";
  }).map(clean).filter(Boolean);
}

function firstLinkedId(value) {
  return linkedIds(value)[0] || "";
}

function compareHorses(a, b) {
  return compareText(a.name, b.name) || compareText(a.id, b.id);
}

function compareAttributes(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label);
}

function compareRecentRows(a, b) {
  return compareText(b.changedAt, a.changedAt) || compareText(b.id, a.id);
}

function compareText(a, b) {
  return String(a || "").localeCompare(String(b || ""), undefined, { numeric: true, sensitivity: "base" });
}

function compareNumber(a, b) {
  return (Number(a) || 0) - (Number(b) || 0);
}

function numberField(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function stringField(value) {
  if (Array.isArray(value)) return value.map(stringField).filter(Boolean).join(", ");
  if (value && typeof value === "object") return stringField(value.name || value.label || value.id);
  return String(value ?? "").trim();
}

function stringifyValue(value) {
  if (value == null) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

function clean(value) {
  return stringField(value);
}

function slugify(value) {
  return clean(value).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}
