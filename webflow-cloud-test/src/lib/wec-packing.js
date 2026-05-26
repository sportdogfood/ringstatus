const WEC_META_TABLE = "wec_meta";
const WEC_META_TABLE_ID = "tbllJywsOstkqT5yZ";
const PACK_ITEMS_MASTER_VIEW = "master";
const LIST_PLAN_VALUES = new Set(["quantity", "per_horse", "horse_specific", "per_groom"]);
const DECISION_VALUES = ["max", "kill", "note", "purchase_onsite", "unresolved"];

export function getWecAirtableConfig(env) {
  const token = env.AIRTABLE_TOKEN;
  const baseId = env.AIRTABLE_BASE_ID || env.AIRTABLE_BASE;
  const metaTable = env.AIRTABLE_WEC_META_TABLE || WEC_META_TABLE_ID || WEC_META_TABLE;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  if (!baseId) return { ok: false, error: "missing_airtable_base_id" };
  return { ok: true, token, baseId, metaTable };
}

export async function loadWecPackingState(airtable, options = {}) {
  const schema = await getBaseSchema(airtable);
  const metaRecords = await listAirtableRecords(airtable, airtable.metaTable || WEC_META_TABLE);
  const registry = buildRegistry(metaRecords, schema);
  const required = requiredRegistry(registry);

  const lists = await listIfPhysical(airtable, required.wec_pack_lists);
  const sourceItems = await listIfPhysical(airtable, required.wec_pack_items, PACK_ITEMS_MASTER_VIEW);
  const horses = await listIfPhysical(airtable, required.wec_horses);
  const grooms = await listIfPhysical(airtable, required.wec_grooms);
  const packingItems = await listIfPhysical(airtable, required.wec_packing_items);
  const packingItemHorses = await listIfPhysical(airtable, required.wec_packing_item_horses);

  const normalizedHorses = normalizeHorses(horses);
  const activeHorseCount = normalizedHorses.filter((horse) => horse.record_state === "active").length;
  const groomCount = groomCountFromOptions(options, grooms, activeHorseCount);
  const listIndex = buildListIndex(lists);
  const currentItemIndex = indexByField(packingItems, "item_id");
  const currentHorseRows = normalizePackingItemHorses(packingItemHorses);
  const items = normalizeSourceItems(sourceItems, {
    listIndex,
    horses: normalizedHorses,
    activeHorseCount,
    groomCount,
    currentItemIndex,
    currentHorseRows
  });

  return {
    ok: true,
    service: "wec-packing-state",
    source: {
      metaTable: airtable.metaTable || WEC_META_TABLE,
      packItemsView: PACK_ITEMS_MASTER_VIEW,
      showId: options.showId || "",
      packWaveId: options.packWaveId || "",
      groomCount
    },
    gates: {
      registry: registry.map((row) => ({
        meta: row.meta,
        table_name: row.table_name,
        table_api: row.table_api,
        status: row.status,
        const_env: row.const_env,
        AIRTABLE__TABLE: row.AIRTABLE__TABLE,
        AIRTABLE__VIEW: row.AIRTABLE__VIEW,
        fields_allowed: row.fields_allowed
      })),
      plannedTables: registry.filter((row) => row.status === "planned" && row.table_name.startsWith("wec_")).map((row) => row.table_name),
      ignoredTables: registry.filter((row) => row.ignore).map((row) => row.table_name),
      decisionValues: DECISION_VALUES
    },
    counts: {
      lists: lists.length,
      sourceItems: sourceItems.length,
      worksheetItems: items.length,
      horses: normalizedHorses.length,
      activeHorses: activeHorseCount,
      grooms: grooms.length,
      currentPackingItems: packingItems.length,
      currentHorseItems: packingItemHorses.length
    },
    lists: normalizeLists(lists),
    horses: normalizedHorses,
    items
  };
}

function requiredRegistry(registry) {
  return Object.fromEntries(registry.map((row) => [row.table_name, row]));
}

function buildRegistry(records, schema) {
  return records
    .map((record) => {
      const fields = record.fields || {};
      const tableName = text(fields.table_name || fields.meta);
      const tableApi = text(fields.table_api);
      const hasPhysicalTable = Boolean(tableApi && schema.tables[tableApi]);
      return {
        id: record.id,
        meta: text(fields.meta),
        priority: numberOrNull(fields.priority),
        meta_type: text(fields.meta_type),
        meta_purpose: text(fields.meta_purpose),
        table_name: tableName,
        table_api: tableApi,
        const_env: Boolean(fields.const_env),
        ignore: Boolean(fields.ignore),
        support: Boolean(fields.support),
        AIRTABLE__TABLE: text(fields.AIRTABLE__TABLE),
        AIRTABLE__VIEW: text(fields.AIRTABLE__VIEW),
        fields_allowed: parseFieldsAllowed(fields.fields_allowed),
        status: fields.ignore ? "ignored" : hasPhysicalTable ? "physical" : "planned"
      };
    })
    .filter((row) => row.table_name)
    .sort((a, b) => (a.priority || 999) - (b.priority || 999));
}

async function listIfPhysical(airtable, registryRow, view = "") {
  if (!registryRow || registryRow.ignore || registryRow.status !== "physical") return [];
  return listAirtableRecords(airtable, registryRow.table_api || registryRow.table_name, view);
}

function normalizeLists(records) {
  return records
    .filter((record) => !record.fields?.ignore)
    .map((record) => ({
      id: record.id,
      list: text(record.fields?.list || record.fields?.name),
      short_description: text(record.fields?.short_description),
      long_description: text(record.fields?.long_description),
      item_ids: arrayValue(record.fields?.wec_pack_items)
    }));
}

function buildListIndex(records) {
  const byItem = new Map();
  for (const list of normalizeLists(records)) {
    for (const itemId of list.item_ids) {
      if (!byItem.has(itemId)) byItem.set(itemId, list);
    }
  }
  return byItem;
}

function normalizeHorses(records) {
  return records
    .filter((record) => !record.fields?.ignore)
    .map((record) => {
      const fields = record.fields || {};
      const active = fields.record_state === "active" || fields.active === true;
      const inactive = fields.record_state === "inactive" || fields.inactive === true;
      return {
        id: record.id,
        horse: text(fields.horse || fields.name),
        barn_name: firstText(fields.barn_name, fields["barn_name (from ww_horses)"]),
        show_name: firstText(fields.show_name, fields["show_name (from ww_horses)"]),
        record_state: active ? "active" : inactive ? "inactive" : "inactive",
        week_ids: arrayValue(fields.wec_weeks),
        source_item_ids: arrayValue(fields.wec_pack_items),
        sort_order: numberOrNull(fields.sort_order)
      };
    })
    .sort((a, b) => (a.sort_order ?? 9999) - (b.sort_order ?? 9999) || a.horse.localeCompare(b.horse));
}

function normalizeSourceItems(records, context) {
  return records
    .filter((record) => !record.fields?.ignore)
    .map((record) => normalizeSourceItem(record, context))
    .filter(Boolean)
    .sort((a, b) => a.sort_order - b.sort_order || a.item_name.localeCompare(b.item_name));
}

function normalizeSourceItem(record, context) {
  const fields = record.fields || {};
  const listPlan = text(fields.list_plan);
  if (!LIST_PLAN_VALUES.has(listPlan)) return null;

  const list = context.listIndex.get(record.id);
  const current = context.currentItemIndex.get(record.id)?.fields || {};
  const linkedHorseIds = arrayValue(fields.wec_horses);
  const activeLinkedHorses = context.horses.filter((horse) => (
    horse.record_state === "active" && linkedHorseIds.includes(horse.id)
  ));
  const perHorseAmount = numberOrDefault(fields.per_horse, listPlan === "horse_specific" ? 1 : 0);
  const baseQuantity = quantityForPlan(listPlan, fields, {
    activeHorseCount: context.activeHorseCount,
    activeLinkedHorseCount: activeLinkedHorses.length,
    groomCount: context.groomCount,
    perHorseAmount
  });
  const packedQuantity = numberOrDefault(current.quantity_packed, 0);
  const leftQuantity = Math.max(0, baseQuantity - packedQuantity);
  const packState = text(current.pack_state) || (leftQuantity <= 0 && baseQuantity > 0 ? "packed" : "not_packed");
  const itemId = text(current.item_id) || record.id;

  return {
    id: itemId,
    source_item_id: record.id,
    section: list?.list || "unassigned",
    section_label: list?.long_description || list?.short_description || list?.list || "Unassigned",
    category: firstText(fields.___original_aisle, list?.short_description, list?.long_description),
    item_name: text(fields.app_name || fields.name),
    description: text(fields.long_description),
    location: firstText(fields.___original_aisle, list?.short_description, "Packing staging"),
    list_plan: listPlan,
    quantity_base: baseQuantity,
    quantity_needed: baseQuantity,
    quantity_packed: packedQuantity,
    quantity_left: leftQuantity,
    unit: text(fields.uom || ""),
    pack_state: packState,
    resolution_state: text(current.resolution_state),
    record_state: text(current.record_state) || "active",
    notes: text(current.notes || fields.note),
    sort_order: numberOrDefault(fields.sorted, 9999),
    horse_specific: listPlan === "horse_specific",
    horse_ids: activeLinkedHorses.map((horse) => horse.id),
    horse_members: activeLinkedHorses.map((horse) => ({
      id: horse.id,
      horse: horse.horse,
      barn_name: horse.barn_name,
      show_name: horse.show_name,
      pack_state: context.currentHorseRows.get(`${record.id}:${horse.id}`)?.horse_pack_state || "not_packed"
    }))
  };
}

function quantityForPlan(plan, fields, counts) {
  if (plan === "quantity") return numberOrDefault(fields.quantity, 0);
  if (plan === "per_horse") return roundQuantity(numberOrDefault(fields.per_horse, 0) * counts.activeHorseCount);
  if (plan === "horse_specific") return roundQuantity(counts.perHorseAmount * counts.activeLinkedHorseCount);
  if (plan === "per_groom") return roundQuantity(numberOrDefault(fields.per_groom, 0) * counts.groomCount);
  return 0;
}

function normalizePackingItemHorses(records) {
  const byKey = new Map();
  for (const record of records) {
    const fields = record.fields || {};
    const packingItem = arrayValue(fields.packing_item)[0] || "";
    const horse = arrayValue(fields.horse)[0] || "";
    const key = text(fields.item_horse_key) || `${packingItem}:${horse}`;
    byKey.set(key, {
      id: record.id,
      horse_pack_state: text(fields.horse_pack_state) || "not_packed",
      notes: text(fields.notes)
    });
  }
  return byKey;
}

function indexByField(records, fieldName) {
  const index = new Map();
  for (const record of records) {
    const value = text(record.fields?.[fieldName]);
    if (value) index.set(value, record);
  }
  return index;
}

function groomCountFromOptions(options, grooms, activeHorseCount) {
  const manual = numberOrNull(options.groomCount);
  if (manual !== null) return manual;
  const activeGrooms = grooms.filter((record) => {
    const state = text(record.fields?.record_state);
    return state ? state === "active" : true;
  }).length;
  if (activeGrooms > 0) return activeGrooms;
  return Math.ceil(activeHorseCount / 6);
}

export async function getBaseSchema(airtable) {
  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
    headers: airtableHeaders(airtable.token)
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`schema ${response.status}: ${JSON.stringify(result)}`);
  }
  const tables = {};
  for (const table of result.tables || []) {
    const fields = new Set((table.fields || []).map((field) => field.name));
    tables[table.name] = fields;
    tables[table.id] = fields;
  }
  return { tables, raw: result.tables || [] };
}

export async function listAirtableRecords(airtable, table, view = "") {
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

export function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...headers,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}

export const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

function parseFieldsAllowed(value) {
  return text(value)
    .split(/[\n,]/)
    .map((field) => field.trim())
    .filter(Boolean);
}

function arrayValue(value) {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null || value === "") return [];
  return [value];
}

function firstText(...values) {
  for (const value of values) {
    const normalized = text(value);
    if (normalized) return normalized;
  }
  return "";
}

function text(value) {
  if (Array.isArray(value)) return value.map(text).filter(Boolean).join(", ");
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

function numberOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function numberOrDefault(value, fallback) {
  const number = numberOrNull(value);
  return number === null ? fallback : number;
}

function roundQuantity(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.ceil(value);
}
