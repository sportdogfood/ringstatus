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
  wec_pack_lists: "Grid view",
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
  const envKeys = envReportRows(airtable, context.registry);
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
  const [waves, packLists, sourcePackItems, worksheetItems, worksheetHorses, horses] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_pack_waves.id, tables.wec_pack_waves.view),
    listAirtableRecords(airtable, tables.wec_pack_lists.id, tables.wec_pack_lists.view),
    listAirtableRecords(airtable, tables.wec_pack_items.id, tables.wec_pack_items.view),
    listAirtableRecords(airtable, tables.wec_packing_items.id, tables.wec_packing_items.view),
    listAirtableRecords(airtable, tables.wec_packing_item_horses.id, tables.wec_packing_item_horses.view),
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view)
  ]);

  const selectedWave = selectWave(waves, packWaveId);
  const normalizedWave = selectedWave ? normalizeWave(selectedWave) : null;
  const selectedShowId = showId || firstLinkedId(selectedWave?.fields?.show);
  const normalizedPackLists = packLists
    .filter((record) => !record.fields?.ignore)
    .map(normalizePackList)
    .sort(comparePackLists);
  const packListLookup = new Map(normalizedPackLists.map((list) => [list.id, list]));
  const sourcePackItemLookup = new Map(sourcePackItems.map((record) => {
    const item = normalizeSourcePackItem(record);
    return [item.id, item];
  }));
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
    .map((record) => decoratePackingItem(
      normalizePackingItem(record, horsesByItem.get(record.id) || []),
      packListLookup,
      sourcePackItemLookup,
      normalizedWave
    ))
    .sort(compareWorksheetRows);
  const lists = buildListSummaries(items, normalizedPackLists);

  return {
    ok: true,
    source: {
      showId: selectedShowId || "",
      packWaveId: selectedWave?.id || "",
      tables: {
        packWaves: tables.wec_pack_waves.id,
        packLists: tables.wec_pack_lists.id,
        packItems: tables.wec_pack_items.id,
        packingItems: tables.wec_packing_items.id,
        packingItemHorses: tables.wec_packing_item_horses.id,
        packingEvents: tables.wec_packing_events.id
      }
    },
    wave: normalizedWave,
    availableWaves: waves.map(normalizeWave).sort((a, b) => compareNumber(a.sortOrder, b.sortOrder)),
    horses: normalizedHorses,
    lists,
    sections: lists.map((list) => ({
      section: list.id,
      label: list.label,
      rows: list.rows,
      done: list.done,
      open: list.open
    })),
    counts: {
      waves: waves.length,
      packLists: normalizedPackLists.length,
      sourcePackItems: sourcePackItems.length,
      worksheetItems: items.length,
      horseMembers: filteredHorses.length,
      horses: normalizedHorses.length
    },
    needsGeneration: items.length === 0,
    items
  };
}

export async function reconcileReport(airtable, requestUrl) {
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
  const [waves, packLists, sourcePackItems, worksheetItems, worksheetHorses, horses, events] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_pack_waves.id, tables.wec_pack_waves.view),
    listAirtableRecords(airtable, tables.wec_pack_lists.id, tables.wec_pack_lists.view),
    listAirtableRecords(airtable, tables.wec_pack_items.id, tables.wec_pack_items.view),
    listAirtableRecords(airtable, tables.wec_packing_items.id, tables.wec_packing_items.view),
    listAirtableRecords(airtable, tables.wec_packing_item_horses.id, tables.wec_packing_item_horses.view),
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view),
    listAirtableRecords(airtable, tables.wec_packing_events.id, tables.wec_packing_events.view)
  ]);

  const selectedWave = selectWave(waves, packWaveId);
  const wave = selectedWave ? normalizeWave(selectedWave) : null;
  const selectedShowId = showId || firstLinkedId(selectedWave?.fields?.show);
  const normalizedPackLists = packLists
    .filter((record) => !record.fields?.ignore)
    .map(normalizePackList)
    .sort(comparePackLists);
  const packListLookup = new Map(normalizedPackLists.map((list) => [list.id, list]));
  const sourcePackItemLookup = new Map(sourcePackItems.map((record) => {
    const item = normalizeSourcePackItem(record);
    return [item.id, item];
  }));
  const normalizedHorses = horses
    .filter((record) => !selectedShowId || includesLinkedId(record.fields.wec_show, selectedShowId))
    .map(normalizeRosterHorse)
    .sort(compareHorseRosterRows);
  const waveHorses = normalizedHorses.filter((horse) => isHorseInWave(horse, wave));
  const waveHorseIds = new Set(waveHorses.map((horse) => horse.id));
  const filteredItems = selectedWave ? worksheetItems.filter((record) => (
    isActiveWorksheetRow(record) &&
    includesLinkedId(record.fields.pack_wave, selectedWave.id) &&
    (!selectedShowId || includesLinkedId(record.fields.show, selectedShowId))
  )) : [];
  const itemIds = new Set(filteredItems.map((record) => record.id));
  const filteredHorseMembers = worksheetHorses.filter((record) => (
    itemIds.has(firstLinkedId(record.fields.packing_item)) ||
    (selectedWave && includesLinkedId(record.fields.pack_wave, selectedWave.id))
  ));
  const horsesByItem = groupByLinkedId(filteredHorseMembers, "packing_item");
  const items = filteredItems
    .map((record) => decoratePackingItem(
      normalizePackingItem(record, horsesByItem.get(record.id) || []),
      packListLookup,
      sourcePackItemLookup,
      wave
    ))
    .sort(compareWorksheetRows);
  const packingItemBySourceId = groupFirstByLinkedId(filteredItems, "source_pack_item");
  const packingItemsById = new Map(filteredItems.map((record) => [record.id, record]));
  const eventsByHorseMember = groupByLinkedId(events, "packing_item_horse");
  const orphanHorseMembers = [];
  const staleHorseMembers = [];
  const blockedHorseMembers = [];

  for (const record of filteredHorseMembers) {
    const member = normalizeHorseMember(record);
    const parentItem = packingItemsById.get(firstLinkedId(record.fields.packing_item));
    const sourcePackItemIds = member.sourcePackItemIds.length
      ? member.sourcePackItemIds
      : linkedIds(parentItem?.fields?.source_pack_item);
    const sourcePackItemId = sourcePackItemIds[0] || "";
    const sourceItem = sourcePackItemLookup.get(sourcePackItemId);
    const eventCount = (eventsByHorseMember.get(record.id) || []).length + member.eventIds.length;
    const safeToRemove = isSafeToRemoveHorseMember(member, eventCount);
    const row = horseMemberAuditRow(member, sourceItem, eventCount, safeToRemove);
    const hasHorse = member.horseIds.length > 0 && !!member.barnName;

    if (!hasHorse) {
      orphanHorseMembers.push({
        ...row,
        reason: "missing_horse_link_or_barn_name"
      });
      if (!safeToRemove) blockedHorseMembers.push({ ...row, reason: "orphan_has_progress_or_history" });
      continue;
    }

    if (sourcePackItemId && !expectedSourceHorseIds(sourceItem, waveHorseIds).has(member.horseIds[0])) {
      staleHorseMembers.push({
        ...row,
        reason: "horse_no_longer_expected_for_wave_or_source_item"
      });
      if (!safeToRemove) blockedHorseMembers.push({ ...row, reason: "stale_has_progress_or_history" });
    }
  }

  const existingMemberKeys = new Set(filteredHorseMembers.map((record) => {
    const member = normalizeHorseMember(record);
    const sourcePackItemId = member.sourcePackItemIds[0] || linkedIds(packingItemsById.get(firstLinkedId(record.fields.packing_item))?.fields?.source_pack_item)[0] || "";
    return `${sourcePackItemId}:${member.horseIds[0] || ""}`;
  }));
  const missingHorseMembers = [];
  for (const sourceItem of sourcePackItemLookup.values()) {
    if (!isHorseSpecificSourceItem(sourceItem)) continue;
    const packingItem = packingItemBySourceId.get(sourceItem.id);
    if (!packingItem) continue;
    for (const horseId of expectedSourceHorseIds(sourceItem, waveHorseIds)) {
      const key = `${sourceItem.id}:${horseId}`;
      if (!existingMemberKeys.has(key)) {
        missingHorseMembers.push({
          sourcePackItemId: sourceItem.id,
          sourceItem: sourceItem.appName,
          packingItemId: packingItem.id,
          horseId,
          reason: "expected_horse_member_missing"
        });
      }
    }
  }

  const quantityMismatches = items
    .filter((item) => item.quantityCalculation && !item.quantityCalculation.matchesFrozen)
    .map((item) => ({
      id: item.id,
      itemName: item.name,
      itemId: item.itemId,
      listPlan: item.quantityCalculation.plan,
      calculatedNeeded: item.quantityCalculation.calculatedNeeded,
      frozenNeeded: item.quantityCalculation.frozenNeeded,
      formula: item.quantityCalculation.formula
    }));
  const safeToRemoveHorseMembers = [...orphanHorseMembers, ...staleHorseMembers].filter((row) => row.safeToRemove);

  return {
    ok: true,
    dryRun: true,
    source: {
      showId: selectedShowId || "",
      packWaveId: selectedWave?.id || "",
      tables: {
        packWaves: tables.wec_pack_waves.id,
        packItems: tables.wec_pack_items.id,
        packingItems: tables.wec_packing_items.id,
        packingItemHorses: tables.wec_packing_item_horses.id,
        packingEvents: tables.wec_packing_events.id
      }
    },
    wave,
    waveCounts: {
      frozenHorseCount: numberField(wave?.horseCount),
      currentWaveHorseCount: waveHorses.length,
      horseCountMismatch: !!wave && numberField(wave.horseCount) !== waveHorses.length,
      groomCountFinal: numberField(wave?.groomCountFinal)
    },
    summary: {
      worksheetItems: items.length,
      worksheetHorseMembers: filteredHorseMembers.length,
      currentWaveHorses: waveHorses.length,
      orphanHorseMembers: orphanHorseMembers.length,
      staleHorseMembers: staleHorseMembers.length,
      missingHorseMembers: missingHorseMembers.length,
      safeToRemoveHorseMembers: safeToRemoveHorseMembers.length,
      blockedHorseMembers: blockedHorseMembers.length,
      quantityMismatches: quantityMismatches.length
    },
    orphanHorseMembers,
    staleHorseMembers,
    missingHorseMembers,
    safeToRemoveHorseMembers,
    blockedHorseMembers,
    quantityMismatches
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
    const fallbackEnvKeys = ENV_TABLES[row.name] || {};
    const tableEnvKey = row.tableEnv || fallbackEnvKeys.table || "";
    const viewEnvKey = row.viewEnv || fallbackEnvKeys.view || "";
    const envTableId = clean(airtable.runtime[tableEnvKey]);
    const envView = clean(airtable.runtime[viewEnvKey]);
    const schemaTable = findSchemaTable(schema, row.name, envTableId || row.tableApi || row.tableName);
    tables[row.name] = {
      id: envTableId || row.tableApi || schemaTable?.id || row.tableName,
      name: row.name,
      view: envView || DEFAULT_SOURCE_VIEWS[row.name] || ""
    };
  }
  return tables;
}

function envReportRows(airtable, registry) {
  return registry.rows
    .map((row) => {
      const fallbackEnvKeys = ENV_TABLES[row.name] || {};
      const tableKey = row.tableEnv || fallbackEnvKeys.table || "";
      const viewKey = row.viewEnv || fallbackEnvKeys.view || "";
      return {
        name: row.name,
        tableKey,
        tableValue: tableKey ? airtable.runtime[tableKey] || "" : "",
        hasTableValue: tableKey ? !!airtable.runtime[tableKey] : false,
        viewKey,
        viewValue: viewKey ? airtable.runtime[viewKey] || "" : "",
        hasViewValue: viewKey ? !!airtable.runtime[viewKey] : false
      };
    })
    .filter((row) => row.tableKey || row.viewKey);
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

function normalizePackList(record) {
  const fields = record.fields || {};
  const label = stringField(fields.list) || record.id;
  return {
    id: record.id,
    key: slugify(label),
    label,
    shortDescription: stringField(fields.short_description),
    longDescription: stringField(fields.long_description),
    itemCount: numberField(fields.list_items_count)
  };
}

function normalizeSourcePackItem(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    appName: stringField(fields.app_name),
    listPlan: stringField(fields.list_plan),
    quantity: numberField(fields.quantity),
    perHorse: numberField(fields.per_horse),
    perGroom: numberField(fields.per_groom),
    horseSpecific: !!fields["horse-specific"],
    uom: stringField(fields.uom),
    packListIds: linkedIds(fields.wec_pack_lists),
    horseIds: linkedIds(fields.wec_horses),
    ignored: !!fields.ignore,
    active: !!fields.active && !fields.inactive && !fields.remove
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
    location: stringField(fields.location),
    listPlan: stringField(fields.list_plan),
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
    packListLabels: [],
    horseMembers: horseRecords.map(normalizeHorseMember).sort(compareHorseRows)
  };
}

function decoratePackingItem(item, packListLookup, sourcePackItemLookup, wave) {
  const sourceItems = item.sourcePackItemIds
    .map((id) => sourcePackItemLookup.get(id))
    .filter(Boolean);
  return {
    ...item,
    packListLabels: item.packListIds
      .map((id) => packListLookup.get(id)?.label || "")
      .filter(Boolean),
    sourceItems,
    quantityCalculation: buildQuantityCalculation(item, sourceItems[0], wave)
  };
}

function normalizeHorseMember(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    itemHorseId: stringField(fields.item_horse_id),
    itemHorseKey: stringField(fields.item_horse_key),
    barnName: stringField(fields["barn_name (from horse)"]),
    horseIds: linkedIds(fields.horse),
    packingItemIds: linkedIds(fields.packing_item),
    packWaveIds: linkedIds(fields.pack_wave),
    sourcePackItemIds: linkedIds(fields.source_pack_item),
    eventIds: linkedIds(fields.wec_packing_events),
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

function buildListSummaries(items, packLists) {
  const summaries = new Map();
  for (const list of packLists) {
    summaries.set(list.id, {
      ...list,
      rows: 0,
      done: 0,
      open: 0
    });
  }

  for (const item of items) {
    const listIds = item.packListIds.length ? item.packListIds : ["unlisted"];
    for (const id of listIds) {
      const summary = summaries.get(id) || {
        id,
        key: id,
        label: id === "unlisted" ? "Unlisted" : id,
        shortDescription: "",
        longDescription: "",
        itemCount: 0,
        rows: 0,
        done: 0,
        open: 0
      };
      summary.rows += 1;
      if (isSatisfied(item)) summary.done += 1;
      summaries.set(id, summary);
    }
  }

  return [...summaries.values()]
    .filter((summary) => summary.rows > 0 || summary.itemCount > 0)
    .map((summary) => ({
      ...summary,
      open: summary.rows - summary.done
    }));
}

function buildSections(items) {
  const sections = new Map();
  for (const item of items) {
    const listIds = item.packListIds.length ? item.packListIds : ["unlisted"];
    for (const key of listIds) {
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
  }
  return [...sections.values()].map((section) => ({
    ...section,
    open: section.rows - section.done
  }));
}

function buildQuantityCalculation(item, sourceItem, wave) {
  const plan = item.listPlan || sourceItem?.listPlan || "";
  const frozenNeeded = numberField(item.needed);
  const unit = item.unit || sourceItem?.uom || "";

  if (plan === "per_groom") {
    const perGroom = numberField(sourceItem?.perGroom || item.quantityBase);
    const groomCount = numberField(wave?.groomCountFinal);
    const calculatedNeeded = perGroom * groomCount;
    return calculationRow({
      plan,
      formula: "per_groom * groom_count_final",
      sourceField: "wec_pack_items.per_groom",
      multiplierField: "wec_pack_waves.groom_count_final",
      base: perGroom,
      multiplier: groomCount,
      calculatedNeeded,
      frozenNeeded,
      unit
    });
  }

  if (plan === "per_horse") {
    const perHorse = numberField(sourceItem?.perHorse || item.quantityBase);
    const horseCount = numberField(wave?.horseCount);
    const calculatedNeeded = perHorse * horseCount;
    return calculationRow({
      plan,
      formula: "per_horse * horse_count",
      sourceField: "wec_pack_items.per_horse",
      multiplierField: "wec_pack_waves.horse_count",
      base: perHorse,
      multiplier: horseCount,
      calculatedNeeded,
      frozenNeeded,
      unit
    });
  }

  if (plan === "horse_specific" || plan === "horse-specific") {
    const calculatedNeeded = item.horseMembers.reduce((sum, horse) => sum + numberField(horse.needed || 1), 0);
    return calculationRow({
      plan,
      formula: "sum(wec_packing_item_horses.quantity_needed)",
      sourceField: "wec_packing_item_horses.quantity_needed",
      multiplierField: "active horse item members",
      base: calculatedNeeded,
      multiplier: item.horseMembers.length,
      calculatedNeeded,
      frozenNeeded,
      unit
    });
  }

  if (plan === "quantity") {
    const calculatedNeeded = numberField(sourceItem?.quantity || item.quantityBase || frozenNeeded);
    return calculationRow({
      plan,
      formula: "quantity",
      sourceField: "wec_pack_items.quantity",
      multiplierField: "",
      base: calculatedNeeded,
      multiplier: 1,
      calculatedNeeded,
      frozenNeeded,
      unit
    });
  }

  return calculationRow({
    plan: plan || "unresolved",
    formula: "quantity_needed",
    sourceField: "wec_packing_items.quantity_needed",
    multiplierField: "",
    base: frozenNeeded,
    multiplier: 1,
    calculatedNeeded: frozenNeeded,
    frozenNeeded,
    unit
  });
}

function calculationRow({ plan, formula, sourceField, multiplierField, base, multiplier, calculatedNeeded, frozenNeeded, unit }) {
  return {
    plan,
    formula,
    sourceField,
    multiplierField,
    base,
    multiplier,
    calculatedNeeded,
    frozenNeeded,
    unit,
    matchesFrozen: Math.abs(numberField(calculatedNeeded) - numberField(frozenNeeded)) < 0.0001
  };
}

function isHorseInWave(horse, wave) {
  if (!horse.active) return false;
  if (!wave || !wave.includedWeekIds.length) return true;
  return horse.weekIds.some((weekId) => wave.includedWeekIds.includes(weekId));
}

function isHorseSpecificSourceItem(sourceItem) {
  if (!sourceItem || sourceItem.ignored || !sourceItem.active) return false;
  return sourceItem.horseSpecific || sourceItem.listPlan === "horse_specific" || sourceItem.listPlan === "horse-specific";
}

function expectedSourceHorseIds(sourceItem, waveHorseIds) {
  if (!sourceItem) return new Set();
  return new Set(sourceItem.horseIds.filter((horseId) => waveHorseIds.has(horseId)));
}

function isSafeToRemoveHorseMember(member, eventCount) {
  return numberField(member.packed) === 0 &&
    eventCount === 0 &&
    member.horsePackState !== "packed";
}

function horseMemberAuditRow(member, sourceItem, eventCount, safeToRemove) {
  return {
    id: member.id,
    itemHorseId: member.itemHorseId,
    itemHorseKey: member.itemHorseKey,
    sourcePackItemId: sourceItem?.id || member.sourcePackItemIds[0] || "",
    sourceItem: sourceItem?.appName || "",
    horseIds: member.horseIds,
    barnName: member.barnName,
    quantityNeeded: member.needed,
    quantityPacked: member.packed,
    horsePackState: member.horsePackState,
    eventCount,
    safeToRemove,
    suggestedAction: safeToRemove ? "remove_from_current_wave" : "review_manually"
  };
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

function groupFirstByLinkedId(records, fieldName) {
  const grouped = new Map();
  for (const record of records) {
    for (const id of linkedIds(record.fields?.[fieldName])) {
      if (!grouped.has(id)) grouped.set(id, record);
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

function comparePackLists(a, b) {
  return a.label.localeCompare(b.label);
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

function slugify(value) {
  return clean(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}
