globalThis.process ??= {};
globalThis.process.env ??= {};
import { env } from "cloudflare:workers";
const config = {
  runtime: "edge"
};
const DEFAULT_HORSES_TABLE = "ww_horses";
const DEFAULT_VIEW_PREFIX = "hps_";
const DEFAULT_LOG_TABLE = "hp_cls";
const DEFAULT_FEED_PLAN_TABLE = "hp_feed_plan";
const DEFAULT_WEC_HORSES_TABLE = "wec_horses";
const DEFAULT_ACTIVE_TENANTS_TABLE = "active_tenants";
const DEFAULT_ACTIVE_TENANTS_VIEW = "active_tenants";
const TENANT_FIELD_CANDIDATES = ["tenant_id", "tenantId", "Tenant ID", "pid", "PID", "path_tenant"];
const FEED_HORSE_FIELD_CANDIDATES = ["horse_record_id", "horseRecordId", "horse_airtable_id", "airtable_id", "horse_id", "horse", "horses", "ww_horses", "horse_link", "horse_links"];
const PROFILE_READ_FIELDS = ["horse", "horse_id", "show_name", "pid", "last_modified_time", "tenant_id", "airtable_id"];
const PROFILE_EDITABLE_FIELDS = [
  "barn_name",
  "horse_colors",
  "horse_genders",
  "emergency_phone",
  "emergency_contacts",
  "horse_disciplines",
  "horse_age",
  "hands",
  "app_active",
  "app_inactive",
  "wec_wave_1",
  "wec_wave_2",
  "wec_not_going",
  "print_batch",
  "horse_note"
];
const PROFILE_ACTION_FIELDS = ["stall_card_input_print"];
const PROFILE_MEMBERSHIP_FIELDS = ["wec_horses_link", "lists"];
const PROFILE_DEFERRED_FIELDS = ["active_subscribers", "ww_riders", "horse_profile_tabs_link"];
const PROFILE_LINKED_FIELD_MAP = {
  tenant_id: "ww_tenants",
  trainer_id: "ww_trainers",
  horse_disciplines: "horse_disciplines_link",
  horse_colors: "horse_colors_link",
  horse_genders: "horse_genders_link"
};
const WEC_SUMMER_FIELDS = ["wec_wave_1", "wec_wave_2", "wec_not_going"];
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  const tenantId = getTenantIdFromUrl(request.url);
  const validation = validateTenantId(tenantId);
  if (!validation.ok) return json({ ok: false, error: validation.error }, 400);
  try {
    const activeTenant = await requireActiveTenant(airtable, tenantId);
    const view = tenantView(airtable, tenantId);
    const horseRecordId = getHorseRecordIdFromUrl(request.url);
    if (horseRecordId) {
      const record = await requireHorseInTenantView(airtable, tenantId, horseRecordId);
      return json({
        ok: true,
        tenantId,
        activeTenant,
        source: {
          table: airtable.horsesTable,
          view
        },
        profileContract: profileContract(),
        count: 1,
        records: [{
          ...record,
          feedPlan: []
        }]
      });
    }
    const records = await listAirtableRecords(airtable, airtable.horsesTable, view);
    const feedPlan = await loadFeedPlanByHorse(airtable, tenantId, records);
    return json({
      ok: true,
      tenantId,
      activeTenant,
      source: {
        table: airtable.horsesTable,
        view
      },
      profileContract: profileContract(),
      count: records.length,
      records: records.map((record) => ({
        ...record,
        feedPlan: feedPlan.get(record.id) || []
      }))
    });
  } catch (error) {
    console.error("[hps] load failed", error);
    return json({
      ok: false,
      error: "airtable_load_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
const POST = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  const payload = await readJson(request);
  const tenantId = normalizeTenantId(payload.tenantId || payload.tenant_id);
  const tenantValidation = validateTenantId(tenantId);
  if (!tenantValidation.ok) return json({ ok: false, error: tenantValidation.error }, 400);
  const changeValidation = validateChange(payload);
  if (!changeValidation.ok) return json({ ok: false, error: changeValidation.error }, 400);
  try {
    const activeTenant = await requireActiveTenant(airtable, tenantId);
    const horseRecord = await requireHorseInTenantView(airtable, tenantId, payload.horseRecordId);
    const schema = await getBaseSchema(airtable);
    const updated = await updateHorseRecord(airtable, schema, payload, horseRecord);
    const logged = await createChangeLogRecord(airtable, schema, { ...payload, tenantId }, updated);
    return json({
      ok: true,
      tenantId,
      activeTenant,
      action: "updated_logged",
      updated,
      log: logged
    });
  } catch (error) {
    console.error("[hps] save failed", error);
    return json({
      ok: false,
      error: "airtable_save_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
function getAirtableConfig() {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE;
  const horsesTable = env.AIRTABLE_HPS_HORSES_TABLE || env.AIRTABLE_WW_HORSES_TABLE || env.AIRTABLE_HORSES_TABLE || DEFAULT_HORSES_TABLE;
  const viewPrefix = env.AIRTABLE_HPS_VIEW_PREFIX || DEFAULT_VIEW_PREFIX;
  const logTable = env.AIRTABLE_HPS_CHANGE_LOG_TABLE || DEFAULT_LOG_TABLE;
  const feedPlanTable = env.AIRTABLE_HPS_FEED_PLAN_TABLE || DEFAULT_FEED_PLAN_TABLE;
  const feedPlanView = env.AIRTABLE_HPS_FEED_PLAN_VIEW || "";
  const wecHorsesTable = env.AIRTABLE_HPS_WEC_HORSES_TABLE || env.AIRTABLE_WEC_HORSES_TABLE || DEFAULT_WEC_HORSES_TABLE;
  const activeTenantsTable = env.AIRTABLE_HPS_ACTIVE_TENANTS_TABLE || DEFAULT_ACTIVE_TENANTS_TABLE;
  const activeTenantsView = env.AIRTABLE_HPS_ACTIVE_TENANTS_VIEW || DEFAULT_ACTIVE_TENANTS_VIEW;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  return {
    ok: true,
    token,
    baseId,
    horsesTable,
    viewPrefix,
    logTable,
    feedPlanTable,
    feedPlanView,
    wecHorsesTable,
    activeTenantsTable,
    activeTenantsView
  };
}
async function loadFeedPlanByHorse(airtable, tenantId, horseRecords) {
  const grouped = new Map(horseRecords.map((record) => [record.id, []]));
  if (!airtable.feedPlanTable) return grouped;
  let records = [];
  try {
    records = await listAirtableRecords(airtable, airtable.feedPlanTable, airtable.feedPlanView);
  } catch (error) {
    console.warn("[hps] feed plan load skipped", error);
    return grouped;
  }
  const horseIndex = buildHorseIndex(horseRecords);
  for (const record of records) {
    const fields = record.fields || {};
    if (!recordMatchesTenant(fields, tenantId)) continue;
    const horseId = findFeedHorseId(fields, horseIndex);
    if (!horseId || !grouped.has(horseId)) continue;
    grouped.get(horseId).push({
      id: record.id,
      fields
    });
  }
  for (const rows of grouped.values()) {
    rows.sort((a, b) => feedSortLabel(a.fields).localeCompare(feedSortLabel(b.fields), void 0, { numeric: true }));
  }
  return grouped;
}
function buildHorseIndex(records) {
  const index = /* @__PURE__ */ new Map();
  for (const record of records) {
    const fields = record.fields || {};
    const keys = [
      record.id,
      firstValue(fields, ["airtable_id", "horse_id", "record_key", "horse_key", "source_id", "horse", "show_name", "barn_name"])
    ];
    for (const key of keys) {
      const normalized = normalizeLookupKey(key);
      if (normalized) index.set(normalized, record.id);
    }
  }
  return index;
}
function recordMatchesTenant(fields, tenantId) {
  const tenantValue = firstValue(fields, TENANT_FIELD_CANDIDATES);
  if (!tenantValue) return true;
  const values = Array.isArray(tenantValue) ? tenantValue : [tenantValue];
  return values.map(normalizeTenantId).includes(tenantId);
}
function findFeedHorseId(fields, horseIndex) {
  for (const fieldName of FEED_HORSE_FIELD_CANDIDATES) {
    const value = fields[fieldName];
    const values = Array.isArray(value) ? value : [value];
    for (const item of values) {
      const match = horseIndex.get(normalizeLookupKey(item));
      if (match) return match;
    }
  }
  return "";
}
function feedSortLabel(fields) {
  const type = firstValue(fields, ["feedType", "feed_type", "type"]);
  const typeOrder = { grain: "1", supplement: "2", hay: "3" };
  return [
    typeOrder[String(type || "").trim().toLowerCase()] || "9",
    type,
    firstValue(fields, ["feedName", "feed_name", "feed", "ration"]),
    firstValue(fields, ["uid", "assignmentId"])
  ].map((value) => stringifyValue(value)).join(" ");
}
async function requireActiveTenant(airtable, tenantId) {
  const tenants = await listAirtableRecords(airtable, airtable.activeTenantsTable, airtable.activeTenantsView);
  const match = tenants.map((record) => ({
    id: record.id,
    tenantId: normalizeTenantId(firstValue(record.fields || {}, TENANT_FIELD_CANDIDATES))
  })).find((tenant) => tenant.tenantId === tenantId);
  if (!match) {
    throw new Error(`tenant_not_active: ${tenantId}`);
  }
  return match;
}
async function requireHorseInTenantView(airtable, tenantId, horseRecordId) {
  const view = tenantView(airtable, tenantId);
  const records = await listAirtableRecords(airtable, airtable.horsesTable, view);
  const match = records.find((record) => record.id === horseRecordId);
  if (!match) {
    throw new Error(`horse_not_in_tenant_view: ${horseRecordId}`);
  }
  return match;
}
async function listAirtableRecords(airtable, table, view) {
  const records = [];
  let offset = "";
  do {
    const url = airtableUrl(airtable.baseId, table);
    url.searchParams.set("pageSize", "100");
    if (view) url.searchParams.set("view", view);
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, { headers: airtableHeaders(airtable.token) });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(`list ${table}/${view || "all"} ${response.status}: ${JSON.stringify(result)}`);
    }
    records.push(...(result.records || []).map((record) => ({
      id: record.id,
      createdTime: record.createdTime,
      fields: record.fields || {}
    })));
    offset = result.offset || "";
  } while (offset);
  return records;
}
async function updateHorseRecord(airtable, schema, payload, horseRecord) {
  const fieldName = String(payload.fieldName || "").trim();
  if (schema?.tables?.[airtable.horsesTable] && !schema.tables[airtable.horsesTable].has(fieldName)) {
    throw new Error(`field_not_found_in_${airtable.horsesTable}: ${fieldName}`);
  }
  const value = airtableFieldValue(fieldName, payload.newValue);
  const response = await fetch(`${airtableUrl(airtable.baseId, airtable.horsesTable)}/${encodeURIComponent(payload.horseRecordId)}`, {
    method: "PATCH",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      fields: { [fieldName]: value },
      typecast: true
    })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`update ${response.status}: ${JSON.stringify(result)}`);
  }
  const linkedUpdates = await updateLinkedWecHorseRecords(airtable, schema, payload, horseRecord, value);
  return {
    id: result.id || payload.horseRecordId,
    fieldName,
    value: result.fields?.[fieldName] ?? value,
    action: "updated",
    linkedUpdates
  };
}
async function updateLinkedWecHorseRecords(airtable, schema, payload, horseRecord, value) {
  const fieldName = String(payload.fieldName || "").trim();
  if (!WEC_SUMMER_FIELDS.includes(fieldName)) return [];
  const linkedRecordIds = linkedWecHorseRecordIds(horseRecord);
  if (!linkedRecordIds.length) return [];
  const allowedFields = schema?.tables?.[airtable.wecHorsesTable];
  if (allowedFields && !allowedFields.has(fieldName)) {
    throw new Error(`field_not_found_in_${airtable.wecHorsesTable}: ${fieldName}`);
  }
  const updates = [];
  for (const recordId of linkedRecordIds) {
    const response = await fetch(`${airtableUrl(airtable.baseId, airtable.wecHorsesTable)}/${encodeURIComponent(recordId)}`, {
      method: "PATCH",
      headers: {
        ...airtableHeaders(airtable.token),
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        fields: { [fieldName]: value },
        typecast: true
      })
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(`update ${airtable.wecHorsesTable}/${recordId} ${response.status}: ${JSON.stringify(result)}`);
    }
    updates.push({
      table: airtable.wecHorsesTable,
      id: result.id || recordId,
      fieldName,
      value: result.fields?.[fieldName] ?? value,
      action: "updated"
    });
  }
  return updates;
}
function linkedWecHorseRecordIds(horseRecord) {
  const fields = horseRecord?.fields || {};
  const value = fields.wec_horses_link;
  const values = Array.isArray(value) ? value : [value];
  return values.map((item) => String(item || "").trim()).filter((item) => /^rec[A-Za-z0-9]+$/.test(item));
}
async function createChangeLogRecord(airtable, schema, payload, updated) {
  const changedAt = (/* @__PURE__ */ new Date()).toISOString();
  const changeKey = `hps:${payload.tenantId}:${payload.horseRecordId || payload.horseKey}:${payload.fieldName}:${Date.now()}`;
  const allowedLogFields = schema?.tables?.[airtable.logTable];
  if (allowedLogFields && !allowedLogFields.has("tenant_id")) {
    throw new Error(`field_not_found_in_${airtable.logTable}: tenant_id`);
  }
  const fields = filterAirtableFields(schema, airtable.logTable, compactFields({
    change_label: `${payload.horseName || payload.horseKey || payload.horseRecordId || "horse"} - ${payload.fieldName}`,
    tenant_id: payload.tenantId,
    change_key: changeKey,
    horse_record_id: payload.horseRecordId,
    horse_key: payload.horseKey,
    horse_name: payload.horseName,
    field_name: payload.fieldName,
    old_value: stringifyValue(payload.oldValue),
    new_value: stringifyValue(payload.newValue),
    changed_at: changedAt,
    source: payload.source || "hps",
    raw_payload: JSON.stringify({
      ...payload,
      tenant_id: payload.tenantId,
      update_action: updated?.action || "",
      update_record_id: updated?.id || ""
    })
  }));
  if (!Object.keys(fields).length) {
    throw new Error(`no_matching_log_fields_in_${airtable.logTable}`);
  }
  const response = await fetch(airtableUrl(airtable.baseId, airtable.logTable), {
    method: "POST",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`log ${response.status}: ${JSON.stringify(result)}`);
  }
  return {
    id: result.records?.[0]?.id || "",
    changeKey,
    fieldCount: Object.keys(fields).length,
    changedAt
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
      tables[table.name] = new Set((table.fields || []).map((field) => field.name));
      tables[table.id] = tables[table.name];
    }
    return { tables };
  } catch {
    return null;
  }
}
function filterAirtableFields(schema, table, fields) {
  const allowed = schema?.tables?.[table];
  if (!allowed) return fields;
  return Object.fromEntries(Object.entries(fields).filter(([field]) => allowed.has(field)));
}
function airtableFieldValue(fieldName, value) {
  if (fieldName === "disciplines" || fieldName === "horse_disciplines") {
    return String(value || "").split(",").map((item) => item.trim()).filter(Boolean);
  }
  if (fieldName === "horse_age" || fieldName === "age" || fieldName === "Age" || fieldName === "hands" || fieldName === "Hands") {
    const number = Number(value);
    return Number.isFinite(number) && String(value).trim() !== "" ? number : value;
  }
  if (fieldName === "app_active" || fieldName === "app_inactive" || fieldName === "print_batch" || WEC_SUMMER_FIELDS.includes(fieldName)) {
    const normalized = String(value || "").trim().toLowerCase();
    return value === true || ["true", "1", "yes", "y"].includes(normalized);
  }
  if (fieldName === "ignore") {
    const normalized = String(value || "").trim().toLowerCase();
    return ["ignore", "ignored", "true", "1", "yes", "y"].includes(normalized);
  }
  return value;
}
function validateChange(payload) {
  if (!payload || typeof payload !== "object") return { ok: false, error: "invalid_payload" };
  if (!payload.horseRecordId) return { ok: false, error: "missing_horse_record_id" };
  if (!payload.fieldName) return { ok: false, error: "missing_field_name" };
  if (!PROFILE_EDITABLE_FIELDS.includes(String(payload.fieldName).trim())) {
    return { ok: false, error: "field_not_allowed" };
  }
  return { ok: true };
}
function profileContract() {
  return {
    readFields: PROFILE_READ_FIELDS,
    editableFields: PROFILE_EDITABLE_FIELDS,
    actionFields: PROFILE_ACTION_FIELDS,
    membershipFields: PROFILE_MEMBERSHIP_FIELDS,
    deferredFields: PROFILE_DEFERRED_FIELDS,
    feedPlan: {
      table: DEFAULT_FEED_PLAN_TABLE,
      mode: "read_only",
      horseMatchFields: FEED_HORSE_FIELD_CANDIDATES
    },
    linkedFieldMap: PROFILE_LINKED_FIELD_MAP
  };
}
function getTenantIdFromUrl(url) {
  const parsed = new URL(url);
  return normalizeTenantId(parsed.searchParams.get("tenantId") || parsed.searchParams.get("tenant_id"));
}
function getHorseRecordIdFromUrl(url) {
  const parsed = new URL(url);
  return String(parsed.searchParams.get("horseRecordId") || parsed.searchParams.get("recordId") || "").trim();
}
function validateTenantId(tenantId) {
  if (!tenantId) return { ok: false, error: "missing_tenant_id" };
  if (!/^[A-Za-z0-9_-]+$/.test(tenantId)) return { ok: false, error: "invalid_tenant_id" };
  return { ok: true };
}
function tenantView(airtable, tenantId) {
  return `${airtable.viewPrefix}${tenantId}`;
}
async function readJson(request) {
  const text = await request.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}
function firstValue(fields, names) {
  for (const name of names) {
    if (fields[name] !== void 0 && fields[name] !== null && fields[name] !== "") return fields[name];
  }
  return "";
}
function normalizeTenantId(value) {
  return String(value || "").trim();
}
function normalizeLookupKey(value) {
  return String(value || "").trim().toLowerCase();
}
function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}
function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}
function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== void 0 && value !== null && value !== ""));
}
function stringifyValue(value) {
  if (value === void 0 || value === null) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
