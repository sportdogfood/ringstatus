import { env } from "cloudflare:workers";

export const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export const REQUIRED_TABLES = [
  "wec_meta",
  "wec_shows",
  "wec_weeks",
  "wec_horses",
  "wec_pack_lists",
  "wec_pack_items",
  "wec_pack_waves",
  "wec_packing_items",
  "wec_packing_item_horses",
  "wec_packing_events"
];

export const ENV_TABLES = {
  wec_pack_waves: {
    table: "AIRTABLE_WEC_PACK_WAVES_TABLE",
    view: "AIRTABLE_WEC_PACK_WAVES_VIEW"
  },
  wec_packing_items: {
    table: "AIRTABLE_WEC_PACKING_ITEMS_TABLE",
    view: "AIRTABLE_WEC_PACKING_ITEMS_VIEW"
  },
  wec_packing_item_horses: {
    table: "AIRTABLE_WEC_PACKING_ITEM_HORSES_TABLE",
    view: "AIRTABLE_WEC_PACKING_ITEM_HORSES_VIEW"
  },
  wec_packing_events: {
    table: "AIRTABLE_WEC_PACKING_EVENTS_TABLE",
    view: "AIRTABLE_WEC_PACKING_EVENTS_VIEW"
  }
};

const DEFAULT_META_TABLE = "tbllJywsOstkqT5yZ";
const DEFAULT_SOURCE_VIEWS = {
  wec_pack_items: "master"
};

export function runtimeEnv() {
  const localEnv = globalThis.process?.env || {};
  return { ...localEnv, ...(env || {}) };
}

export function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
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

export async function loadWecContext(airtable) {
  const [schema, metaRecords] = await Promise.all([
    getBaseSchema(airtable),
    listAirtableRecords(airtable, airtable.metaTable)
  ]);
  const registry = buildRegistry(metaRecords);
  const tables = buildTableConfig(airtable, registry, schema);
  return { schema, registry, tables };
}

export async function healthReport(airtable) {
  const context = await loadWecContext(airtable);
  const required = REQUIRED_TABLES.map((name) => {
    const registryRow = context.registry.byName[name] || null;
    const tableId = context.tables[name]?.id || registryRow?.tableApi || "";
    const schemaTable = findSchemaTable(context.schema, name, tableId);
    const allowedFields = registryRow?.fieldsAllowed || [];
    const schemaFields = new Set((schemaTable?.fields || []).map((field) => field.name));
    const missingFields = allowedFields.filter((field) => !schemaFields.has(field));
    return {
      name,
      tableId,
      registry: !!registryRow,
      physical: !!schemaTable,
      fieldsAllowed: allowedFields.length,
      missingFields
    };
  });
  const envKeys = Object.entries(ENV_TABLES).map(([name, keys]) => ({
    name,
    tableKey: keys.table,
    tableValue: airtable.runtime[keys.table] || "",
    hasTableValue: !!airtable.runtime[keys.table],
    viewKey: keys.view,
    viewValue: airtable.runtime[keys.view] || "",
    hasViewValue: !!airtable.runtime[keys.view]
  }));
  return {
    ok: required.every((item) => item.registry && item.physical && item.missingFields.length === 0),
    service: "wec-packing",
    source: {
      metaTable: airtable.metaTable,
      requiredTables: REQUIRED_TABLES.length
    },
    env: {
      hasAirtableToken: !!airtable.token,
      hasAirtableBaseId: !!airtable.baseId,
      keys: envKeys
    },
    required
  };
}

export async function stateReport(airtable, requestUrl) {
  const url = new URL(requestUrl);
  const showId = clean(url.searchParams.get("showId"));
  const packWaveId = clean(url.searchParams.get("packWaveId"));
  const context = await loadWecContext(airtable);
  const health = await healthReportFromContext(airtable, context);
  if (!health.ok) {
    return {
      ok: false,
      error: "wec_setup_incomplete",
      health
    };
  }

  const tables = context.tables;
  const [waves, worksheetItems, worksheetHorses, horses] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_pack_waves.id, tables.wec_pack_waves.view),
    listAirtableRecords(airtable, tables.wec_packing_items.id, tables.wec_packing_items.view),
    listAirtableRecords(airtable, tables.wec_packing_item_horses.id, tables.wec_packing_item_horses.view),
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view)
  ]);

  const selectedWave = selectWave(waves, packWaveId);
  const selectedShowId = showId || firstLinkedId(selectedWave?.fields?.show);
  const normalizedHorses = horses
    .filter((record) => !selectedShowId || includesLinkedId(record.fields.wec_show, selectedShowId))
    .map(normalizeRosterHorse)
    .sort(compareHorseRosterRows);
  const filteredItems = selectedWave ? worksheetItems.filter((record) => (
    isActiveWorksheetRow(record) &&
    includesLinkedId(record.fields.pack_wave, selectedWave.id) &&
    (!selectedShowId || includesLinkedId(record.fields.show, selectedShowId))
  )) : [];
  const itemIds = new Set(filteredItems.map((record) => record.id));
  const filteredHorses = worksheetHorses.filter((record) => (
    itemIds.has(firstLinkedId(record.fields.packing_item)) ||
    (selectedWave && includesLinkedId(record.fields.pack_wave, selectedWave.id))
  ));
  const horsesByItem = groupByLinkedId(filteredHorses, "packing_item");
  const items = filteredItems
    .map((record) => normalizePackingItem(record, horsesByItem.get(record.id) || []))
    .sort(compareWorksheetRows);

  return {
    ok: true,
    source: {
      showId: selectedShowId || "",
      packWaveId: selectedWave?.id || "",
      tables: {
        packWaves: tables.wec_pack_waves.id,
        packingItems: tables.wec_packing_items.id,
        packingItemHorses: tables.wec_packing_item_horses.id,
        packingEvents: tables.wec_packing_events.id
      }
    },
    wave: selectedWave ? normalizeWave(selectedWave) : null,
    availableWaves: waves.map(normalizeWave).sort((a, b) => compareNumber(a.sortOrder, b.sortOrder)),
    horses: normalizedHorses,
    sections: buildSections(items),
    counts: {
      waves: waves.length,
      worksheetItems: items.length,
      horseMembers: filteredHorses.length,
      horses: normalizedHorses.length
    },
    needsGeneration: items.length === 0,
    items
  };
}

async function healthReportFromContext(airtable, context) {
  const required = REQUIRED_TABLES.map((name) => {
    const registryRow = context.registry.byName[name] || null;
    const tableId = context.tables[name]?.id || registryRow?.tableApi || "";
    const schemaTable = findSchemaTable(context.schema, name, tableId);
    const allowedFields = registryRow?.fieldsAllowed || [];
    const schemaFields = new Set((schemaTable?.fields || []).map((field) => field.name));
    const missingFields = allowedFields.filter((field) => !schemaFields.has(field));
    return {
      name,
      tableId,
      registry: !!registryRow,
      physical: !!schemaTable,
      fieldsAllowed: allowedFields.length,
      missingFields
    };
  });
  return {
    ok: required.every((item) => item.registry && item.physical && item.missingFields.length === 0),
    required
  };
}

function buildRegistry(records) {
  const rows = records.map((record) => {
    const fields = record.fields || {};
    const name = clean(fields.table_name || fields.meta);
    return {
      id: record.id,
      name,
      meta: clean(fields.meta),
      tableApi: clean(fields.table_api),
      tableName: clean(fields.table_name || fields.meta),
      ignore: !!fields.ignore,
      constEnv: !!fields.const_env,
      tableEnv: clean(fields.AIRTABLE__TABLE),
      viewEnv: clean(fields.AIRTABLE__VIEW),
      fieldsAllowed: splitLines(fields.fields_allowed)
    };
  }).filter((row) => row.name && !row.ignore);
  return {
    rows,
    byName: Object.fromEntries(rows.map((row) => [row.name, row]))
  };
}

function buildTableConfig(airtable, registry, schema) {
  const tables = {};
  for (const row of registry.rows) {
    const envKeys = ENV_TABLES[row.name];
    const envTableId = envKeys ? clean(airtable.runtime[envKeys.table]) : "";
    const envView = envKeys ? clean(airtable.runtime[envKeys.view]) : "";
    const schemaTable = findSchemaTable(schema, row.name, envTableId || row.tableApi || row.tableName);
    tables[row.name] = {
      id: envTableId || row.tableApi || schemaTable?.id || row.tableName,
      name: row.name,
      view: envView || DEFAULT_SOURCE_VIEWS[row.name] || ""
    };
  }
  return tables;
}

async function getBaseSchema(airtable) {
  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
    headers: airtableHeaders(airtable.token)
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`schema ${response.status}: ${JSON.stringify(result)}`);
  }
  return result;
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
      throw new Error(`list ${table} ${response.status}: ${JSON.stringify(result)}`);
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

function normalizeWave(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    wave: stringField(fields.wave),
    waveType: stringField(fields.wave_type),
    active: !!fields.active,
    horseCount: numberField(fields.horse_count),
    groomCountFinal: numberField(fields.groom_count_final),
    sortOrder: numberField(fields.sort_order),
    showIds: linkedIds(fields.show),
    includedWeekIds: linkedIds(fields.included_weeks)
  };
}

function normalizePackingItem(record, horseRecords) {
  const fields = record.fields || {};
  const needed = numberField(fields.quantity_needed ?? fields.quantity_base);
  const packed = numberField(fields.quantity_packed);
  const left = numberField(fields.quantity_left ?? Math.max(0, needed - packed));
  return {
    id: record.id,
    name: stringField(fields.item_name),
    itemId: stringField(fields.item_id),
    section: stringField(fields.section),
    category: stringField(fields.category),
    location: stringField(fields.location),
    listPlan: stringField(fields.list_plan || fields.quantity_mode),
    quantityBase: numberField(fields.quantity_base),
    needed,
    packed,
    left,
    unit: stringField(fields.unit),
    packState: stringField(fields.pack_state || "not_packed"),
    resolutionState: stringField(fields.resolution_state),
    recordState: stringField(fields.record_state || "active"),
    ignored: !!fields.ignore,
    notes: stringField(fields.notes),
    sortOrder: numberField(fields.sort_order),
    sourcePackItemIds: linkedIds(fields.source_pack_item),
    packListIds: linkedIds(fields.pack_list),
    horseMembers: horseRecords.map(normalizeHorseMember).sort(compareHorseRows)
  };
}

function normalizeHorseMember(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    horseIds: linkedIds(fields.horse),
    packingItemIds: linkedIds(fields.packing_item),
    sourcePackItemIds: linkedIds(fields.source_pack_item),
    needed: numberField(fields.quantity_needed || 1),
    packed: numberField(fields.quantity_packed),
    horsePackState: stringField(fields.horse_pack_state || "not_packed"),
    notes: stringField(fields.notes),
    sortOrder: numberField(fields.sort_order)
  };
}

function normalizeRosterHorse(record) {
  const fields = record.fields || {};
  const recordState = stringField(fields.record_state || (fields.active ? "active" : "inactive")) || "inactive";
  return {
    id: record.id,
    name: stringField(fields.barn_name || fields.horse || fields.show_name),
    showName: stringField(fields.show_name || fields.horse),
    recordState,
    active: recordState === "active",
    sortOrder: numberField(fields.sort_order),
    weekIds: linkedIds(fields.wec_weeks),
    sourcePackItemIds: linkedIds(fields.wec_pack_items),
    notes: stringField(fields.notes)
  };
}

function buildSections(items) {
  const sections = new Map();
  for (const item of items) {
    const key = item.section || "unsectioned";
    const section = sections.get(key) || {
      section: key,
      rows: 0,
      done: 0,
      open: 0
    };
    section.rows += 1;
    if (isSatisfied(item)) section.done += 1;
    sections.set(key, section);
  }
  return [...sections.values()].map((section) => ({
    ...section,
    open: section.rows - section.done
  }));
}

function isSatisfied(item) {
  return item.packState === "packed" || !!item.resolutionState;
}

function isActiveWorksheetRow(record) {
  const fields = record.fields || {};
  if (fields.ignore) return false;
  const recordState = stringField(fields.record_state || "active");
  return recordState === "active";
}

function selectWave(waves, packWaveId) {
  if (packWaveId) return waves.find((record) => record.id === packWaveId) || null;
  return waves.find((record) => !!record.fields?.active) || waves[0] || null;
}

function groupByLinkedId(records, fieldName) {
  const grouped = new Map();
  for (const record of records) {
    for (const id of linkedIds(record.fields?.[fieldName])) {
      const list = grouped.get(id) || [];
      list.push(record);
      grouped.set(id, list);
    }
  }
  return grouped;
}

function findSchemaTable(schema, name, idOrName) {
  const target = clean(idOrName);
  return (schema.tables || []).find((table) => (
    table.name === name ||
    table.id === target ||
    table.name === target
  ));
}

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

function linkedIds(value) {
  return Array.isArray(value) ? value.map(clean).filter(Boolean) : [];
}

function includesLinkedId(value, id) {
  return linkedIds(value).includes(id);
}

function firstLinkedId(value) {
  return linkedIds(value)[0] || "";
}

function splitLines(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function stringField(value) {
  if (Array.isArray(value)) return value.map(stringField).filter(Boolean).join(", ");
  return clean(value);
}

function numberField(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function clean(value) {
  return String(value ?? "").trim();
}

function compareWorksheetRows(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || a.name.localeCompare(b.name);
}

function compareHorseRows(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || a.id.localeCompare(b.id);
}

function compareHorseRosterRows(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || a.name.localeCompare(b.name);
}

function compareNumber(a, b) {
  return (Number(a) || 0) - (Number(b) || 0);
}
