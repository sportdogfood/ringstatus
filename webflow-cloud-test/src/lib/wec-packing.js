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

const OPTIONAL_TABLES = [
  "wec_list_plans",
  "wec_places",
  "wec_places_tags",
  "wec_commenting",
  "pak_groups"
];

export const ENV_TABLES = {
  wec_list_plans: {
    table: "AIRTABLE_WEC_LIST_PLANS_TABLE",
    view: "AIRTABLE_WEC_LIST_PLANS_VIEW"
  },
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
  },
  wec_commenting: {
    table: "AIRTABLE_WEC_COMMENTING_TABLE",
    view: "AIRTABLE_WEC_COMMENTING_VIEW"
  },
  pak_groups: {
    table: "AIRTABLE_PAK_GROUPS_TABLE",
    view: "AIRTABLE_PAK_GROUPS_VIEW"
  }
};

const DEFAULT_META_TABLE = "tbllJywsOstkqT5yZ";
const DEFAULT_SOURCE_VIEWS = {
  wec_pack_lists: "Grid view",
  wec_list_plans: "Grid view"
};
const OPTIONAL_LEGACY_FIELDS_BY_TABLE = {
  wec_pack_waves: new Set(["groom_count_final", "horse_sanity"])
};

export function runtimeEnv() {
  const localEnv = globalThis.process?.env || {};
  const astroEnv = import.meta.env || {};
  return { ...localEnv, ...astroEnv, ...(env || {}) };
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
    const missingFields = requiredMissingFields(name, allowedFields, schemaFields);
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
  const packWaveKey = clean(url.searchParams.get("packWaveKey") || url.searchParams.get("packWave") || url.searchParams.get("wave"));
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
  const [waves, packLists, activePackLists, homePackLists, sourcePackItems, purchaseOnsiteItems, worksheetItems, worksheetHorses, horses, listPlans, packingEvents, packingComments, places, placeTags] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_pack_waves.id, tables.wec_pack_waves.view),
    listAirtableRecords(airtable, tables.wec_pack_lists.id, tables.wec_pack_lists.view),
    listOptionalViewRecords(airtable, tables.wec_pack_lists.id, "active"),
    listOptionalViewRecords(airtable, tables.wec_pack_lists.id, "wec_home"),
    listAirtableRecords(airtable, tables.wec_pack_items.id, tables.wec_pack_items.view),
    listOptionalViewRecords(airtable, tables.wec_pack_items.id, "wec_purchase_onsite"),
    listAirtableRecords(airtable, tables.wec_packing_items.id, tables.wec_packing_items.view),
    listAirtableRecords(airtable, tables.wec_packing_item_horses.id, tables.wec_packing_item_horses.view),
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view),
    listOptionalRecords(airtable, tables.wec_list_plans),
    listAirtableRecords(airtable, tables.wec_packing_events.id, tables.wec_packing_events.view),
    listOptionalRecords(airtable, tables.wec_commenting),
    listOptionalRecords(airtable, tables.wec_places),
    listOptionalRecords(airtable, tables.wec_places_tags)
  ]);

  const listPlanLookup = new Map(listPlans.map((record) => {
    const plan = normalizeListPlan(record);
    return [plan.id, plan];
  }));
  const placeTagLookup = new Map(placeTags.map((record) => {
    const tag = normalizePlaceTag(record);
    return [tag.id, tag];
  }));
  const placeLookup = buildPlaceLookup(places, placeTagLookup);
  const selectedWave = selectWave(waves, packWaveId, packWaveKey);
  const waveBase = selectedWave ? normalizeWave(selectedWave) : null;
  const selectedShowId = showId || firstLinkedId(selectedWave?.fields?.show);
  const normalizedPackLists = packLists
    .filter((record) => !record.fields?.ignore)
    .map(normalizePackList)
    .filter(isPackingListLane)
    .sort(comparePackLists);
  const normalizedHomeLists = homePackLists
    .filter((record) => !record.fields?.ignore)
    .map(normalizePackList)
    .sort(comparePackLists);
  const normalizedActiveLists = activePackLists
    .filter((record) => !record.fields?.ignore)
    .map(normalizePackList)
    .sort(comparePackLists);
  const activeTaskLists = normalizedActiveLists.filter((list) => list.lane === "task_lists");
  const packListLookup = new Map([...normalizedPackLists, ...normalizedHomeLists, ...normalizedActiveLists].map((list) => [list.id, list]));
  const sourcePackItemLookup = new Map(sourcePackItems.map((record) => {
    const item = decorateSourcePackItem(normalizeSourcePackItem(record, listPlanLookup), placeLookup);
    return [item.id, item];
  }));
  const normalizedHorses = horses
    .filter((record) => !selectedShowId || includesLinkedId(record.fields.wec_show, selectedShowId))
    .map(normalizeRosterHorse)
    .sort(compareHorseRosterRows);
  const waveHorses = normalizedHorses.filter((horse) => isHorseInWave(horse, waveBase));
  const waveHorseIds = new Set(waveHorses.map((horse) => horse.id));
  const normalizedWave = withEffectiveWaveCounts(waveBase, waveHorses);
  const filteredItems = selectedWave ? worksheetItems.filter((record) => (
    isActiveWorksheetRow(record) &&
    includesLinkedId(record.fields.pack_wave, selectedWave.id) &&
    (!selectedShowId || includesLinkedId(record.fields.show, selectedShowId))
  )) : [];
  const packingLedger = buildPackingLedgerState(filteredItems);
  const sourceWorksheetRecords = buildSourceWorksheetRecords({
    sourceItems: [...sourcePackItemLookup.values()],
    ledgerState: packingLedger,
    selectedWave,
    selectedShowId,
    packListIds: new Set(normalizedPackLists.map((list) => list.id))
  });
  const itemIds = new Set([
    ...filteredItems.map((record) => record.id),
    ...sourceWorksheetRecords.map((record) => record.id)
  ]);
  const filteredHorses = worksheetHorses.filter((record) => (
    itemIds.has(firstLinkedId(record.fields.packing_item)) ||
    itemIds.has(firstLinkedId(record.fields.source_pack_item)) ||
    (selectedWave && includesLinkedId(record.fields.pack_wave, selectedWave.id))
  ));
  const horseKitStateLookup = horseKitStatesFromEvents(packingEvents, {
    packWaveId: selectedWave?.id || "",
    showId: selectedShowId
  });
  const horsesByItem = groupHorseMembersByWorksheetItem(filteredHorses);
  const items = sourceWorksheetRecords
    .map((record) => decoratePackingItem(
      normalizePackingItem(record, horsesByItem.get(record.id) || [], listPlanLookup),
      packListLookup,
      sourcePackItemLookup,
      normalizedWave,
      waveHorses,
      horseKitStateLookup
    ))
    .sort(compareWorksheetRows);
  const lists = buildListSummaries(items, normalizedPackLists);
  const tabGroups = buildTabSummaries(lists);
  const homeModules = buildHomeModules({
    homeLists: normalizedHomeLists.length ? normalizedHomeLists : activeTaskLists,
    purchaseOnsiteItems,
    items,
    sourcePackItemLookup,
    listPlanLookup,
    placeLookup,
    packListLookup,
    packingEvents,
    selectedShowId,
    selectedWaveId: selectedWave?.id || ""
  });
  const activeListLanes = buildActiveListLanes(normalizedActiveLists, {
    homeModules,
    lists,
    items
  });
  const activeListDetails = await buildActiveListDetails(airtable, tables, normalizedActiveLists, {
    homeModules,
    normalizedPackLists,
    sourcePackItemLookup,
    listPlanLookup,
    placeTagLookup,
    items
  });
  const commentFilter = {
    selectedWaveId: selectedWave?.id || "",
    itemIds
  };
  const comments = mergeComments(
    commentsFromCommentingTable(packingComments, commentFilter),
    commentsFromEvents(packingEvents, commentFilter)
  );

  return {
    ok: true,
    source: {
      showId: selectedShowId || "",
      packWaveId: selectedWave?.id || "",
      packWaveKey: normalizeWave(selectedWave)?.key || "",
      tables: {
        packWaves: tables.wec_pack_waves.id,
        packLists: tables.wec_pack_lists.id,
        packItems: tables.wec_pack_items.id,
        listPlans: tables.wec_list_plans?.id || "",
        packingItems: tables.wec_packing_items.id,
        packingItemHorses: tables.wec_packing_item_horses.id,
        packingEvents: tables.wec_packing_events.id,
        commenting: tables.wec_commenting?.id || ""
      }
    },
    wave: normalizedWave,
    availableWaves: waves.map(normalizeWave).sort((a, b) => compareNumber(a.sortOrder, b.sortOrder)),
    horses: normalizedHorses,
    lists,
    tabGroups,
    activeListLanes,
    activeListDetails,
    homeModules,
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
      activePackLists: normalizedActiveLists.length,
      homePackLists: normalizedHomeLists.length,
      purchaseOnsiteItems: purchaseOnsiteItems.length,
      sourcePackItems: sourcePackItems.length,
      worksheetItems: items.length,
      horseMembers: filteredHorses.length,
      horses: normalizedHorses.length,
      listPlans: listPlans.length
    },
    needsGeneration: items.length === 0,
    comments,
    items
  };
}

function buildHomeModules({ homeLists, purchaseOnsiteItems, items, sourcePackItemLookup, listPlanLookup, placeLookup, packListLookup, packingEvents, selectedShowId, selectedWaveId }) {
  const purchaseList = homeLists.find((list) => list.key === "purchase_onsite" || slugify(list.label) === "purchase_onsite");
  if (!purchaseList) return [];
  const taskStates = onsiteTaskStatesFromEvents(packingEvents, {
    showId: selectedShowId,
    packWaveId: selectedWaveId
  });
  const staticRows = purchaseOnsiteItems
    .map((record) => normalizePurchaseOnsiteTask(record, listPlanLookup, placeLookup, packListLookup, purchaseList.id, taskStates));
  const decisionRows = (items || [])
    .filter(isPurchaseOnsiteItem)
    .map((item) => normalizePurchaseOnsiteDecisionTask(item, sourcePackItemLookup, packListLookup, purchaseList.id, taskStates));
  const rows = mergeTaskRows(staticRows, decisionRows).sort(compareOnsiteTasks);
  const lists = buildOnsiteListSummaries(rows, packListLookup, purchaseList);
  return [{
    id: "purchase_onsite",
    listId: purchaseList.id,
    label: purchaseList.label || "purchase_onsite",
    type: "task_list",
    rows: rows.length,
    done: rows.filter((row) => row.taskState === "done").length,
    open: rows.filter((row) => row.taskState !== "done").length,
    lists,
    tasks: rows
  }];
}

function buildActiveListLanes(activeLists, { homeModules, lists, items }) {
  const laneGroups = new Map();
  const homeModuleLookup = new Map((homeModules || []).map((module) => [slugify(module.id || module.label), module]));
  const listLookup = new Map((lists || []).map((list) => [list.id, list]));
  const unresolvedItems = (items || []).filter(isUnresolvedItem);

  for (const list of activeLists || []) {
    const lane = list.lane || "unassigned";
    const group = laneGroups.get(lane) || {
      id: lane,
      lane,
      label: activeLaneLabel(lane),
      lists: []
    };
    const key = slugify(list.key || list.label);
    const module = homeModuleLookup.get(key);
    const worksheetList = listLookup.get(list.id);
    const isUnresolved = key === "unresolved" || key === "open";
    const rows = module?.rows ?? worksheetList?.rows ?? (isUnresolved ? unresolvedItems.length : list.itemCount);
    const done = module?.done ?? worksheetList?.done ?? 0;
    const open = module?.open ?? worksheetList?.open ?? (isUnresolved ? unresolvedItems.length : Math.max(0, rows - done));
    group.lists.push({
      id: list.id,
      key: list.key,
      lane,
      label: activeListDisplayLabel(list),
      list: list.label,
      listLabel: list.listLabel,
      displayLabel: list.displayLabel,
      sourceTable: list.sourceTable,
      sourceView: list.sourceView,
      localLists: list.localLists,
      allowed: list.allowed,
      tabs: list.tabs,
      rows,
      done,
      open,
      homeModuleId: module?.id || "",
      printTarget: module ? `home:${module.id}` : worksheetList ? list.id : ""
    });
    laneGroups.set(lane, group);
  }

  return [...laneGroups.values()].sort((a, b) => {
    const order = activeLaneOrder(a.lane) - activeLaneOrder(b.lane);
    if (order) return order;
    return a.label.localeCompare(b.label, undefined, { sensitivity: "base" });
  }).map((group) => ({
    ...group,
    lists: group.lists.sort(comparePackLists)
  }));
}

async function buildActiveListDetails(airtable, tables, activeLists, { homeModules, normalizedPackLists, sourcePackItemLookup, listPlanLookup, placeTagLookup, items }) {
  const homeModuleLookup = new Map((homeModules || []).map((module) => [slugify(module.id || module.label), module]));
  const details = await Promise.all((activeLists || []).map(async (list) => {
    let rows = [];
    const key = slugify(list.key || list.label);
    const module = homeModuleLookup.get(key);
    if (module?.tasks) {
      rows = module.tasks.map((task) => ({
        ...task,
        type: "task",
        label: task.name || task.id || "",
        meta: task.packListLabels?.join(", ") || task.listPlanLabel || ""
      }));
    } else if (list.sourceTable === "wec_places" && tables.wec_places?.id) {
      const view = activeListSourceView(list);
      const records = view
        ? await listOptionalViewRecords(airtable, tables.wec_places.id, view)
        : await listOptionalRecords(airtable, tables.wec_places);
      rows = records.map((record) => placeDetailRow(normalizePlace(record, placeTagLookup)));
    } else if (list.key === "list_labels") {
      rows = (normalizedPackLists || []).map(packListDetailRow);
    } else if (list.key === "item_labels") {
      rows = [...(sourcePackItemLookup?.values?.() || [])].map(sourceItemDetailRow);
    } else if (key === "unresolved" || key === "open") {
      rows = (items || []).filter(isUnresolvedItem).map(itemDetailRow);
    }
    return {
      id: list.id,
      key: list.key,
      lane: list.lane,
      label: activeListDisplayLabel(list),
      sourceTable: list.sourceTable,
      sourceView: activeListSourceView(list),
      rows: rows.sort(compareDetailRows)
    };
  }));
  return details;
}

function activeListSourceView(list) {
  if (list.sourceView) return list.sourceView;
  if (list.sourceTable === "wec_places" && list.key?.startsWith("places_")) return list.key.replace(/^places_/, "");
  return "";
}

function activeListDisplayLabel(list) {
  const key = slugify(list?.key || list?.label || "");
  if (key === "unresolved" || key === "open") return "Needs Attention";
  return list?.displayLabel || list?.listLabel || list?.label || "";
}

function placeDetailRow(place) {
  return {
    id: place.id,
    type: "place",
    label: place.label,
    meta: place.localTags.join(", ") || place.placeType,
    phone: place.phone,
    website: place.website,
    mapsUrl: place.mapsUrl,
    attributes: place.attributes
  };
}

function packListDetailRow(list) {
  return {
    id: list.id,
    type: "list",
    label: list.displayLabel || list.listLabel || list.label,
    meta: [list.tabs?.join(", "), list.longDescription || list.shortDescription].filter(Boolean).join(" | ")
  };
}

function sourceItemDetailRow(item) {
  return {
    id: item.id,
    type: "source_item",
    label: displayLabel(item.appName || item.name || item.id),
    meta: [item.listPlanLabel, item.longDescription].filter(Boolean).join(" | ")
  };
}

function itemDetailRow(item) {
  return {
    id: item.id,
    type: "item",
    label: displayLabel(item.name || item.itemId || item.id),
    meta: [item.location, item.listPlanLabel].filter(Boolean).join(" | ")
  };
}

function compareDetailRows(a, b) {
  return displayLabel(a.label || "").localeCompare(displayLabel(b.label || ""), undefined, { sensitivity: "base" })
    || String(a.id || "").localeCompare(String(b.id || ""), undefined, { sensitivity: "base" });
}

function activeLaneLabel(lane) {
  if (lane === "task_lists") return "Item Tasks";
  if (lane === "custom") return "Custom";
  if (lane === "locale") return "Locale";
  if (lane === "pack_lists") return "Pack Lists";
  if (lane === "search") return "Search";
  return displayLabel(lane);
}

function activeLaneOrder(lane) {
  const order = {
    pack_lists: 10,
    task_lists: 20,
    custom: 30,
    locale: 40,
    search: 50
  };
  return order[lane] || 100;
}

function commentsFromEvents(events, { selectedWaveId, itemIds } = {}) {
  const selectedItems = itemIds instanceof Set ? itemIds : new Set();
  return (events || [])
    .map(normalizePackingComment)
    .filter(Boolean)
    .filter((comment) => commentBelongsToWave(comment, selectedWaveId, selectedItems));
}

function commentsFromCommentingTable(records, { selectedWaveId, itemIds } = {}) {
  const selectedItems = itemIds instanceof Set ? itemIds : new Set();
  return (records || [])
    .map(normalizeCommentingRecord)
    .filter(Boolean)
    .filter((comment) => commentBelongsToWave(comment, selectedWaveId, selectedItems));
}

function commentBelongsToWave(comment, selectedWaveId, selectedItems) {
  if (!selectedWaveId) return true;
  if (comment.packWaveIds.includes(selectedWaveId)) return true;
  return comment.scopeType === "item" && selectedItems.has(comment.scopeId);
}

function mergeComments(...groups) {
  const seen = new Set();
  const comments = [];
  for (const comment of groups.flat()) {
    const key = [
      comment.sourceTable || "",
      comment.id || "",
      comment.scopeType || "",
      comment.scopeId || "",
      comment.createdTime || "",
      comment.comment || ""
    ].join("|");
    if (seen.has(key)) continue;
    seen.add(key);
    comments.push(comment);
  }
  return comments;
}

function normalizeCommentingRecord(record) {
  const fields = record.fields || {};
  const parsed = parseCommentNotes(fields.notes);
  const linkedItemId = firstLinkedId(fields.packing_item);
  const linkedWaveId = firstLinkedId(fields.pack_wave);
  const linkedHorseId = firstLinkedId(fields.horse);
  const linkedItemHorseId = firstLinkedId(fields.packing_item_horse);
  const status = slugify(fields.comment_status || fields.status || "active");
  if (status === "deleted") return null;
  const scopeType = slugify(fields.scope_type || fields.comment_scope || parsed.scopeType)
    || (linkedItemId ? "item" : linkedItemHorseId ? "item_horse" : linkedHorseId ? "horse" : linkedWaveId ? "wave" : "");
  const scopeId = clean(fields.scope_id || fields.comment_scope_id || parsed.scopeId)
    || linkedItemId
    || linkedItemHorseId
    || linkedHorseId
    || linkedWaveId
    || "";
  const comment = stringField(fields.comment || parsed.comment || fields.notes);
  if (!scopeType || !scopeId || !comment) return null;
  return {
    id: record.id,
    sourceTable: "wec_commenting",
    createdTime: record.createdTime || stringField(fields.Created || fields.created_at),
    updatedTime: stringField(fields.updated_at),
    scopeType,
    scopeId,
    scopeLabel: stringField(fields.scope_label || fields.comment_scope_label || parsed.scopeLabel),
    comment,
    commentStatus: status || "active",
    createdBy: stringField(fields.created_by || "webflow"),
    updatedBy: stringField(fields.updated_by),
    packWaveIds: linkedIds(fields.pack_wave),
    itemIds: linkedIds(fields.packing_item),
    itemHorseIds: linkedIds(fields.packing_item_horse),
    horseIds: linkedIds(fields.horse)
  };
}

function normalizePackingComment(record) {
  const fields = record.fields || {};
  const eventType = stringField(fields.event_type);
  if (eventType !== "comment_add" && !eventType.startsWith("comment_")) return null;
  const parsed = parseCommentNotes(fields.notes);
  const linkedItemId = firstLinkedId(fields.packing_item);
  const linkedWaveId = firstLinkedId(fields.pack_wave);
  const scopeType = parsed.scopeType || (eventType === "comment_add" ? "" : eventType.replace(/^comment_/, "")) || (linkedItemId ? "item" : "wave");
  const scopeId = parsed.scopeId || linkedItemId || linkedWaveId || "";
  const comment = parsed.comment || stringField(fields.notes);
  if (!scopeType || !scopeId || !comment) return null;
  return {
    id: record.id,
    sourceTable: "wec_packing_events",
    createdTime: record.createdTime || stringField(fields.created_at),
    scopeType,
    scopeId,
    scopeLabel: parsed.scopeLabel,
    comment,
    createdBy: stringField(fields.created_by || "webflow"),
    packWaveIds: linkedIds(fields.pack_wave),
    itemIds: linkedIds(fields.packing_item)
  };
}

function parseCommentNotes(value) {
  const lines = String(value || "").split(/\r?\n/);
  const meta = {};
  const body = [];
  let inBody = false;
  for (const line of lines) {
    if (!inBody) {
      if (!line.trim()) {
        inBody = true;
        continue;
      }
      const match = line.match(/^comment_(scope|scope_id|scope_label):\s*(.*)$/i);
      if (match) {
        meta[match[1].toLowerCase()] = clean(match[2]);
        continue;
      }
      inBody = true;
    }
    body.push(line);
  }
  return {
    scopeType: clean(meta.scope),
    scopeId: clean(meta.scope_id),
    scopeLabel: clean(meta.scope_label),
    comment: body.join("\n").trim()
  };
}

function normalizePurchaseOnsiteTask(record, listPlanLookup, placeLookup, packListLookup, purchaseListId, taskStates) {
  const source = decorateSourcePackItem(normalizeSourcePackItem(record, listPlanLookup), placeLookup);
  const originalListIds = source.packListIds.filter((id) => id !== purchaseListId && packListLookup.has(id));
  const taskListIds = originalListIds.length ? originalListIds : [purchaseListId];
  const taskState = taskStates.get(source.id) || "task";
  return {
    id: source.id,
    name: source.appName,
    listPlan: source.listPlan || "purchase_onsite",
    listPlanLabel: source.listPlanLabel || "purchase_onsite",
    taskState,
    done: taskState === "done",
    packListIds: taskListIds,
    packListLabels: taskListIds.map((id) => packListLookup.get(id)?.label || "").filter(Boolean),
    purchaseListId,
    vendorIds: source.vendorIds,
    placeIds: source.placeIds,
    placeLabels: source.placeLabels,
    localTags: source.localTags,
    note: source.note,
    longDescription: source.longDescription
  };
}

function normalizePurchaseOnsiteDecisionTask(item, sourcePackItemLookup, packListLookup, purchaseListId, taskStates) {
  const source = item.sourcePackItemIds
    .map((id) => sourcePackItemLookup.get(id))
    .filter(Boolean)[0];
  const sourceListIds = (source?.packListIds || []).filter((id) => id !== purchaseListId && packListLookup.has(id));
  const itemListIds = (item.packListIds || []).filter((id) => id !== purchaseListId && packListLookup.has(id));
  const taskListIds = itemListIds.length ? itemListIds : sourceListIds.length ? sourceListIds : [purchaseListId];
  const taskId = source?.id || item.sourcePackItemIds[0] || item.id;
  const taskState = taskStates.get(taskId) || taskStates.get(item.id) || "task";
  return {
    id: taskId,
    packingItemId: item.id,
    sourceItemId: source?.id || "",
    name: source?.appName || item.name,
    listPlan: "purchase_onsite",
    listPlanLabel: "purchase_onsite",
    taskState,
    done: taskState === "done",
    packListIds: taskListIds,
    packListLabels: taskListIds.map((id) => packListLookup.get(id)?.label || "").filter(Boolean),
    purchaseListId,
    vendorIds: source?.vendorIds || [],
    placeIds: source?.placeIds || [],
    placeLabels: source?.placeLabels || [],
    localTags: source?.localTags || [],
    note: item.notes || source?.note || "",
    longDescription: source?.longDescription || item.notes || ""
  };
}

function mergeTaskRows(...groups) {
  const rowsById = new Map();
  for (const row of groups.flat()) {
    if (!row?.id) continue;
    rowsById.set(row.id, {
      ...(rowsById.get(row.id) || {}),
      ...row
    });
  }
  return [...rowsById.values()];
}

function buildOnsiteListSummaries(tasks, packListLookup, purchaseList) {
  const listIds = new Set(tasks.flatMap((task) => task.packListIds || []));
  if (!listIds.size) listIds.add(purchaseList.id);
  return [...listIds].map((id) => {
    const rows = tasks.filter((task) => (task.packListIds || []).includes(id));
    const list = packListLookup.get(id) || (id === purchaseList.id ? purchaseList : { id, label: id });
    return {
      id,
      key: slugify(list.label || id),
      label: list.label || id,
      rows: rows.length,
      done: rows.filter((row) => row.taskState === "done").length,
      open: rows.filter((row) => row.taskState !== "done").length
    };
  }).filter((list) => list.rows > 0).sort(comparePackLists);
}

function onsiteTaskStatesFromEvents(records, context = {}) {
  const states = new Map();
  const relevant = [...(records || [])]
    .filter((record) => ["onsite_task_done", "onsite_task_reopen"].includes(stringField(record.fields?.event_type)))
    .filter((record) => eventMatchesContext(record, context))
    .sort((a, b) => compareText(a.createdTime, b.createdTime) || compareText(stringField(a.fields?.event), stringField(b.fields?.event)));
  for (const record of relevant) {
    const sourceItemId = onsiteTaskEventSourceId(record);
    if (!sourceItemId) continue;
    states.set(sourceItemId, stringField(record.fields?.event_type) === "onsite_task_done" ? "done" : "task");
  }
  return states;
}

function horseKitStatesFromEvents(records, context = {}) {
  const states = new Map();
  const relevant = [...(records || [])]
    .filter((record) => ["horse_kit_packed", "horse_kit_not_needed", "horse_kit_reopened"].includes(stringField(record.fields?.event_type)))
    .filter((record) => eventMatchesContext(record, context))
    .sort((a, b) => compareText(a.createdTime, b.createdTime) || compareText(stringField(a.fields?.event), stringField(b.fields?.event)));
  for (const record of relevant) {
    const ids = horseKitEventIds(record);
    if (!ids.sourcePackItemId || !ids.horseId) continue;
    const eventType = stringField(record.fields?.event_type);
    const horsePackState = eventType === "horse_kit_packed"
      ? "packed"
      : eventType === "horse_kit_not_needed"
        ? "not_needed"
        : "not_packed";
    states.set(`${ids.sourcePackItemId}:${ids.horseId}`, {
      horsePackState,
      packed: horsePackState === "packed" ? 1 : 0,
      eventId: record.id,
      updatedAt: record.createdTime || stringField(record.fields?.created_at)
    });
  }
  return states;
}

function horseKitEventIds(record) {
  const fields = record.fields || {};
  const notes = stringField(fields.notes);
  return {
    sourcePackItemId: noteRecordId(notes, "source_pack_item"),
    horseId: firstLinkedId(fields.horse) || noteRecordId(notes, "horse_id"),
    packingItemId: noteRecordId(notes, "packing_item")
  };
}

function noteRecordId(notes, label) {
  const pattern = new RegExp(`${label}:\\s*(rec[a-zA-Z0-9]+)`);
  return notes.match(pattern)?.[1] || "";
}

function eventMatchesContext(record, context = {}) {
  const fields = record.fields || {};
  const packWaveIds = linkedIds(fields.pack_wave);
  const showIds = linkedIds(fields.show);
  if (context.packWaveId && packWaveIds.length && !packWaveIds.includes(context.packWaveId)) return false;
  if (context.showId && showIds.length && !showIds.includes(context.showId)) return false;
  return true;
}

function onsiteTaskEventSourceId(record) {
  const event = stringField(record.fields?.event);
  const eventMatch = event.match(/^onsite_task_(?:done|reopen):(rec[a-zA-Z0-9]+):/);
  if (eventMatch) return eventMatch[1];
  const notes = stringField(record.fields?.notes);
  const noteMatch = notes.match(/source_pack_item:\s*(rec[a-zA-Z0-9]+)/);
  return noteMatch?.[1] || "";
}

function compareOnsiteTasks(a, b) {
  return compareText(a.name, b.name) || compareText(a.id, b.id);
}

export function printReportHtml(report, requestUrl) {
  const url = new URL(requestUrl);
  const target = clean(url.searchParams.get("target") || "overview");
  const horseId = clean(url.searchParams.get("horseId"));
  const title = horseId
    ? `${displayLabel(printHorseName(report.horses.find((horse) => horse.id === horseId)))} Packing List`
    : target === "overview"
      ? "WEC Packing Report"
      : `${printTargetTitle(report, target)} Packing List`;
  const body = horseId
    ? printHorsePackingPageHtml(report, horseId)
    : printBodyHtml(report, target);
  return printDocumentHtml(title, body);
}

function printBodyHtml(report, target) {
  if (target === "overview") {
    const pages = (report.tabGroups || []).map((group) => printPackingPageHtml(report, group.label, printListSections(report, group.id))).join("");
    return `${pages}${printHorsesPageHtml(report)}`;
  }
  if (target === "horses") return printHorsesPageHtml(report);
  if (String(target || "").startsWith("home:")) return printHomeModulePageHtml(report, target);
  return printPackingPageHtml(report, printTargetTitle(report, target), printListSections(report, target));
}

function printTargetTitle(report, target) {
  if (target === "horses") return "Horses";
  if (String(target || "").startsWith("tab:")) {
    return displayLabel((report.tabGroups || []).find((group) => group.id === target)?.label || target.replace(/^tab:/, ""));
  }
  if (String(target || "").startsWith("home:")) {
    return displayLabel((report.homeModules || []).find((module) => `home:${module.id}` === target)?.label || target.replace(/^home:/, ""));
  }
  return displayLabel((report.lists || []).find((list) => list.id === target)?.label || target);
}

function printListSections(report, target) {
  if (String(target || "").startsWith("tab:")) {
    const group = (report.tabGroups || []).find((row) => row.id === target);
    return (group?.listIds || []).map((listId) => printListSection(report, listId)).filter(Boolean);
  }
  return [printListSection(report, target)].filter(Boolean);
}

function printListSection(report, listId) {
  const list = (report.lists || []).find((row) => row.id === listId) || { id: listId, label: listId };
  const rows = (report.items || []).filter((item) => printItemBelongsToList(item, list.id));
  return {
    title: displayLabel(list.label || list.id),
    rows
  };
}

function printItemBelongsToList(item, listId) {
  const ids = Array.isArray(item.packListIds) ? item.packListIds : [];
  return ids.includes(listId) || item.section === listId || (!ids.length && !item.section && listId === "unlisted");
}

const PRINT_RECORDS_PER_PAGE = 11;

function printPackingPageHtml(report, title, sections) {
  const rows = sections.flatMap((section) => section.rows);
  const percent = progressPercent(rows.filter(isSatisfied).length, rows.length);
  const pages = sections.flatMap((section) => printQuantitySectionPageContents(section, printItemTableRowData));
  return printPageContents(report, title, percent, pages.length ? pages : [printEmptyPrintSectionHtml("No rows")]);
}

function printEmptyPageHtml(report, title, percent, label) {
  return printPageContents(report, title, percent, [printEmptyPrintSectionHtml(label)]);
}

function printPageContents(report, title, percent, pages) {
  return pages.map((content, index) => `
    <section class="packing-print-page">
      ${printGlobalHeaderHtml(report, title, percent)}
      ${content}
      ${printFooterHtml(index + 1)}
    </section>
  `).join("");
}

function printQuantitySectionPageContents(section, rowMapper) {
  const rows = Array.isArray(section.rows) ? section.rows : [];
  const chunks = chunkRows(rows, PRINT_RECORDS_PER_PAGE);
  if (!chunks.length) {
    return [printQuantityTableHtml(section.title, [], { continued: false, rowMapper })];
  }
  return chunks.map((chunk, index) => printQuantityTableHtml(section.title, chunk, {
    continued: index > 0,
    pageNumber: index + 1,
    pageCount: chunks.length,
    rowMapper
  }));
}

function printQuantityTableHtml(title, rows, options = {}) {
  const continued = Boolean(options.continued);
  const rowMapper = options.rowMapper || ((row) => row);
  return `
    <section class="packing-print-list">
      <div class="packing-print-list-head">
        <h2>${escapeHtml(printUpperLabel(title))}</h2>
        ${continued ? `<p>${escapeHtml(`CONTINUED ${options.pageNumber || ""} OF ${options.pageCount || ""}`.trim())}</p>` : ""}
      </div>
      <table class="packing-print-table">
        ${printQuantityTableColgroupHtml()}
        <thead>
          <tr>
            <th scope="col">NAME</th>
            <th scope="col" colspan="2">NEEDED</th>
            <th scope="col" colspan="2">PACKED</th>
            <th scope="col" colspan="2">LEFT</th>
            <th scope="col">INITIAL</th>
          </tr>
        </thead>
        <tbody>
          ${rows.length ? rows.map((row, index) => printQuantityTableRows(rowMapper(row), index)).join("") : printEmptyTableRowHtml()}
        </tbody>
      </table>
    </section>
  `;
}

function printQuantityTableColgroupHtml() {
  return `
    <colgroup>
      <col style="width:36%">
      <col style="width:9%">
      <col style="width:8%">
      <col style="width:9%">
      <col style="width:8%">
      <col style="width:9%">
      <col style="width:8%">
      <col style="width:13%">
    </colgroup>
  `;
}

function printQuantityTableRows(row, index = 0) {
  const done = Boolean(row.done);
  const zebraClass = index % 2 ? " is-zebra" : "";
  const name = row.name || "Unnamed item";
  return `
    <tr class="packing-print-data-row${done ? " is-packed" : ""}${zebraClass}">
      <td class="packing-print-name-cell">${escapeHtml(printUpperLabel(name))}</td>
      <td class="packing-print-number">${escapeHtml(printTableValue(row.needed))}<span class="packing-print-cell-label">need</span></td>
      <td class="packing-print-mark-cell"><span class="packing-print-cell-label">ok</span></td>
      <td class="packing-print-number">${escapeHtml(printTableValue(row.packed))}<span class="packing-print-cell-label">packed</span></td>
      <td class="packing-print-mark-cell"><span class="packing-print-cell-label">ok</span></td>
      <td class="packing-print-number">${escapeHtml(printTableValue(row.left))}<span class="packing-print-cell-label">left</span></td>
      <td class="packing-print-mark-cell"><span class="packing-print-cell-label">ok</span></td>
      <td class="packing-print-initial-cell"><span class="packing-print-cell-label">initial</span></td>
    </tr>
    <tr class="packing-print-notes-row${zebraClass}">
      <td colspan="8">${escapeHtml(row.notes || "")}<span class="packing-print-notes-meta">{${escapeHtml(clean(name).toLowerCase())}} + notes + date</span></td>
    </tr>
  `;
}

function printItemTableRowData(item) {
  const needed = numberField(item?.needed);
  const packed = numberField(item?.packed);
  const left = Math.max(0, numberField(item?.left || needed - packed));
  return {
    name: item?.name || "Unnamed item",
    needed,
    packed,
    left,
    done: isSatisfied(item)
  };
}

function printHorseItemTableRowData(row) {
  const needed = numberField(row.member?.needed);
  const packed = numberField(row.member?.packed);
  const left = Math.max(0, needed - packed);
  return {
    name: row.item?.name || "Unnamed item",
    needed,
    packed,
    left,
    done: isHorseMemberPacked(row.member)
  };
}

function printEmptyTableRowHtml() {
  return `
    <tr class="packing-print-data-row">
      <td class="packing-print-name-cell" colspan="8">NO ROWS</td>
    </tr>
    <tr class="packing-print-notes-row">
      <td colspan="8"></td>
    </tr>
  `;
}

function printHorsesPageHtml(report) {
  const rows = activePrintHorses(report);
  const members = horseMemberRows(report);
  const percent = progressPercent(members.filter(isHorseMemberPacked).length, members.length);
  return printPageContents(report, "Horses", percent, [printHorseTableHtml("Horses", rows)]);
}

function printHorsePackingPageHtml(report, horseId) {
  const horse = (report.horses || []).find((row) => row.id === horseId);
  if (!horse) return printEmptyPrintSectionHtml("No horse");
  const rows = horseItemRows(report, horse);
  const percent = progressPercent(rows.filter((row) => isHorseMemberPacked(row.member)).length, rows.length);
  const section = {
    title: `${printHorseName(horse)} Items`,
    rows
  };
  return printPageContents(report, printHorseName(horse), percent, printQuantitySectionPageContents(section, printHorseItemTableRowData));
}

function printHomeModulePageHtml(report, target) {
  const moduleId = String(target || "").replace(/^home:/, "");
  const module = (report.homeModules || []).find((row) => row.id === moduleId);
  if (!module) return printPackingPageHtml(report, printTargetTitle(report, target), []);
  const sections = (module.lists || []).map((list) => ({
    title: displayLabel(list.label || list.id),
    rows: (module.tasks || []).filter((task) => (task.packListIds || []).includes(list.id))
  })).filter((section) => section.rows.length);
  const percent = progressPercent(numberField(module.done), numberField(module.rows));
  const pages = sections.flatMap(printTaskSectionPageContents);
  return pages.length ? printPageContents(report, module.label, percent, pages) : printEmptyPageHtml(report, module.label, percent, "No rows");
}

function printTaskSectionPageContents(section) {
  const rows = Array.isArray(section.rows) ? section.rows : [];
  const chunks = chunkRows(rows, PRINT_RECORDS_PER_PAGE);
  if (!chunks.length) return [printEmptyPrintSectionHtml("No rows")];
  return chunks.map((chunk, index) => printQuantityTableHtml(section.title, chunk, {
    continued: index > 0,
    pageNumber: index + 1,
    pageCount: chunks.length,
    rowMapper: printTaskRowData
  }));
}

function printTaskRowData(task) {
  const done = task.taskState === "done";
  return {
    name: task.name || "Unnamed task",
    needed: done ? "" : "TASK",
    packed: done ? "DONE" : "",
    left: done ? "" : "OPEN",
    done
  };
}

function printHorseTableHtml(title, rows) {
  return `
    <section class="packing-print-list">
      <div class="packing-print-list-head">
        <h2>${escapeHtml(printUpperLabel(title))}</h2>
      </div>
      <table class="packing-print-horse-table">
        <tbody>
          ${rows.length ? rows.map((horse) => `
            <tr>
              <td>${escapeHtml(printUpperLabel(printHorseName(horse)))}</td>
            </tr>
          `).join("") : `<tr><td>NO HORSES</td></tr>`}
        </tbody>
      </table>
    </section>
  `;
}

function activePrintHorses(report) {
  const members = horseMemberRows(report);
  if (!members.length) return (report.horses || []).filter((horse) => horse.active || String(horse.recordState || "").toLowerCase() === "active");
  const horseIds = new Set();
  const horseKeys = new Set();
  for (const member of members) {
    for (const horseId of member.horseIds || []) horseIds.add(horseId);
    if (member.barnName) horseKeys.add(slugify(member.barnName));
  }
  return (report.horses || [])
    .filter((horse) => horseIds.has(horse.id) || horseKeys.has(slugify(printHorseName(horse))))
    .sort((a, b) => compareNumber(a.sortOrder, b.sortOrder) || printHorseName(a).localeCompare(printHorseName(b)));
}

function horseMemberRows(report) {
  return (report.items || []).flatMap((item) => Array.isArray(item.horseMembers) ? item.horseMembers : []);
}

function horseItemRows(report, horse) {
  return (report.items || []).flatMap((item) => {
    const members = Array.isArray(item.horseMembers) ? item.horseMembers : [];
    return members
      .filter((member) => horseMemberBelongsToHorse(member, horse))
      .map((member) => ({ item, member }));
  });
}

function horseMemberBelongsToHorse(member, horse) {
  if (!member || !horse) return false;
  if (Array.isArray(member.horseIds) && member.horseIds.includes(horse.id)) return true;
  return slugify(member.barnName) === slugify(printHorseName(horse));
}

function isHorseMemberPacked(member) {
  return member.horsePackState === "packed" || numberField(member.packed) >= numberField(member.needed);
}

function printHorseName(horse) {
  return horse?.name || horse?.barnName || horse?.showName || "Unnamed horse";
}

function chunkRows(rows, size) {
  const chunks = [];
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size));
  }
  return chunks;
}

function printUpperLabel(value) {
  return displayLabel(value).toUpperCase();
}

function printTableValue(value) {
  const text = clean(value);
  if (!text) return "";
  const number = Number(text);
  return Number.isFinite(number) ? quantityDisplay(number) : printUpperLabel(text);
}

function printGlobalHeaderHtml(report, title, percent) {
  return `
    <header class="packing-print-head">
      <h1>${escapeHtml(printPageTitle(title))}</h1>
      <p>${escapeHtml(printStatusLine(report, percent))}</p>
    </header>
  `;
}

function printFooterHtml(pageNumber) {
  return `<footer class="packing-print-footer">printed: page ${escapeHtml(pageNumber)} + ${escapeHtml(printDateDisplay())}</footer>`;
}

function printPageTitle(title) {
  const label = displayLabel(title).replace(/\s+Packing\s+List$/i, "").trim();
  return /\sList$/i.test(label) ? label : `${label} List`;
}

function printEmptyPrintSectionHtml(label) {
  return `<div class="packing-print-empty">${escapeHtml(label)}</div>`;
}

function printStatusLine(report, percent) {
  return `${printWaveKey(report)} | printed ${printDateDisplay()}`;
}

function printWaveKey(report) {
  return clean(report.wave?.wave || "wave_one")
    .toLowerCase()
    .replace(/[_\s]+/g, "-");
}

function printDateDisplay() {
  return new Date().toLocaleDateString("en-US", { timeZone: "America/New_York" });
}

function printDocumentHtml(title, body) {
  return `<!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>${escapeHtml(title)}</title>
        <style>${printStyles()}</style>
      </head>
      <body>${body}</body>
    </html>`;
}

function printStyles() {
  return `
    @import url("https://fonts.googleapis.com/css2?family=Outfit:wght@400;600&display=swap");
    @page { size: Letter; margin: 0; }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: #ffffff;
      color: #000000;
      font-family: "Outfit", ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 10px;
      line-height: 1.12;
    }
    .packing-print-page {
      position: relative;
      width: 8.5in;
      min-height: 11in;
      padding: 0.25in;
      break-after: page;
    }
    .packing-print-page:last-child { break-after: auto; }
    .packing-print-head {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 0.2in;
      padding-bottom: 0.12in;
      border-bottom: 2px solid #000000;
      margin-bottom: 0;
    }
    .packing-print-head h1 {
      margin: 0;
      font-size: 24px;
      font-weight: 600;
      line-height: 0.95;
      letter-spacing: -0.06em;
    }
    .packing-print-head p {
      max-width: 4.8in;
      margin: 0;
      font-size: 11px;
      font-weight: 600;
      line-height: 1.1;
      text-align: right;
      text-transform: uppercase;
    }
    .packing-print-list {
      border: 0;
      border-radius: 0;
      overflow: hidden;
      background: #ffffff;
    }
    .packing-print-list-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.12in;
      min-height: 0.34in;
      padding: 0 0.04in;
      background: #f0f0f0;
      border: 0;
    }
    .packing-print-list-head h2 {
      margin: 0;
      font-size: 15px;
      font-weight: 600;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-list-head p {
      margin: 0;
      color: #333333;
      font-size: 8px;
      font-weight: 600;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-table,
    .packing-print-horse-table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    .packing-print-table th {
      height: 0.23in;
      padding: 0 0.025in;
      border: 1px solid #000000;
      background: #000000;
      color: #ffffff;
      font-size: 10px;
      font-weight: 600;
      line-height: 1;
      text-align: center;
      text-transform: uppercase;
      vertical-align: middle;
      white-space: nowrap;
    }
    .packing-print-table th:first-child {
      padding-left: 0.04in;
      text-align: left;
    }
    .packing-print-data-row td {
      height: 0.34in;
      padding: 0 0.025in 0.03in;
      border-top: 1px solid #bdbdbd;
      border-right: 1px solid #dddddd;
      border-bottom: 1px solid #dddddd;
      border-left: 1px solid #dddddd;
      font-size: 13px;
      font-weight: 600;
      line-height: 1;
      text-transform: uppercase;
      vertical-align: middle;
    }
    .packing-print-data-row td:first-child {
      border-left-color: #bdbdbd;
    }
    .packing-print-data-row td:last-child {
      border-right-color: #bdbdbd;
    }
    .packing-print-data-row.is-zebra td,
    .packing-print-notes-row.is-zebra td {
      background: #f6f6f6;
    }
    .packing-print-name-cell {
      padding-left: 0.04in;
      text-align: left;
    }
    .packing-print-data-row.is-packed .packing-print-name-cell {
      opacity: 0.6;
      text-decoration: line-through;
      text-decoration-thickness: 1px;
    }
    .packing-print-number,
    .packing-print-initial-cell,
    .packing-print-mark-cell {
      position: relative;
      text-align: center;
    }
    .packing-print-cell-label {
      position: absolute;
      right: 0.025in;
      bottom: 0.025in;
      color: #777777;
      font-size: 5px;
      font-weight: 500;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-notes-row td {
      position: relative;
      height: 0.46in;
      padding: 0 0.04in 0.03in;
      border-top: 0;
      border-right: 1px solid #bdbdbd;
      border-bottom: 1px solid #bdbdbd;
      border-left: 1px solid #bdbdbd;
      color: #333333;
      font-size: 10px;
      font-weight: 400;
      line-height: 1;
      vertical-align: middle;
    }
    .packing-print-notes-meta {
      position: absolute;
      right: 0.05in;
      bottom: 0.04in;
      color: #555555;
      font-size: 6px;
      font-weight: 500;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-footer {
      position: absolute;
      right: 0.25in;
      bottom: 0.14in;
      padding-top: 0.08in;
      color: #555555;
      font-size: 8px;
      font-weight: 600;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-horse-table td {
      height: 0.3in;
      padding: 0 0.1in;
      border-bottom: 1px solid #eeeeee;
      font-size: 10px;
      font-weight: 600;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-horse-table tr:last-child td {
      border-bottom: 0;
    }
    .packing-print-empty {
      padding: 10px;
      color: #333333;
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
    }
  `;
}

export async function reconcileReport(airtable, requestUrl) {
  const url = new URL(requestUrl);
  const showId = clean(url.searchParams.get("showId"));
  const packWaveId = clean(url.searchParams.get("packWaveId"));
  const packWaveKey = clean(url.searchParams.get("packWaveKey") || url.searchParams.get("packWave") || url.searchParams.get("wave"));
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
  const [waves, packLists, sourcePackItems, worksheetItems, worksheetHorses, horses, events, listPlans] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_pack_waves.id, tables.wec_pack_waves.view),
    listAirtableRecords(airtable, tables.wec_pack_lists.id, tables.wec_pack_lists.view),
    listAirtableRecords(airtable, tables.wec_pack_items.id, tables.wec_pack_items.view),
    listAirtableRecords(airtable, tables.wec_packing_items.id, tables.wec_packing_items.view),
    listAirtableRecords(airtable, tables.wec_packing_item_horses.id, tables.wec_packing_item_horses.view),
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view),
    listAirtableRecords(airtable, tables.wec_packing_events.id, tables.wec_packing_events.view),
    listOptionalRecords(airtable, tables.wec_list_plans)
  ]);

  const listPlanLookup = new Map(listPlans.map((record) => {
    const plan = normalizeListPlan(record);
    return [plan.id, plan];
  }));
  const selectedWave = selectWave(waves, packWaveId, packWaveKey);
  const waveBase = selectedWave ? normalizeWave(selectedWave) : null;
  const selectedShowId = showId || firstLinkedId(selectedWave?.fields?.show);
  const normalizedPackLists = packLists
    .filter((record) => !record.fields?.ignore)
    .map(normalizePackList)
    .filter(isPackingListLane)
    .sort(comparePackLists);
  const packListLookup = new Map(normalizedPackLists.map((list) => [list.id, list]));
  const sourcePackItemLookup = new Map(sourcePackItems.map((record) => {
    const item = normalizeSourcePackItem(record, listPlanLookup);
    return [item.id, item];
  }));
  const normalizedHorses = horses
    .filter((record) => !selectedShowId || includesLinkedId(record.fields.wec_show, selectedShowId))
    .map(normalizeRosterHorse)
    .sort(compareHorseRosterRows);
  const horseLookup = new Map(normalizedHorses.map((horse) => [horse.id, horse]));
  const waveHorses = normalizedHorses.filter((horse) => isHorseInWave(horse, waveBase));
  const wave = withEffectiveWaveCounts(waveBase, waveHorses);
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
  const horseKitStateLookup = horseKitStatesFromEvents(events, {
    packWaveId: selectedWave?.id || "",
    showId: selectedShowId
  });
  const horsesByItem = groupByLinkedId(filteredHorseMembers, "packing_item");
  const items = filteredItems
    .map((record) => decoratePackingItem(
      normalizePackingItem(record, horsesByItem.get(record.id) || [], listPlanLookup),
      packListLookup,
      sourcePackItemLookup,
      wave,
      waveHorses,
      horseKitStateLookup
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
    const memberHorse = horseLookup.get(member.horseIds[0]);
    const eventCount = (eventsByHorseMember.get(record.id) || []).length + member.eventIds.length;
    const safeToRemove = isSafeToRemoveHorseMember(member, eventCount, memberHorse);
    const row = horseMemberAuditRow(member, sourceItem, eventCount, safeToRemove, memberHorse);
    const hasHorse = member.horseIds.length > 0 && !!member.barnName;

    if (!hasHorse) {
      orphanHorseMembers.push({
        ...row,
        reason: "missing_horse_link_or_barn_name"
      });
      if (!safeToRemove) blockedHorseMembers.push({ ...row, reason: "orphan_has_progress_or_history" });
      continue;
    }

    if (sourcePackItemId && (!expectedSourceHorseIds(sourceItem, waveHorseIds).has(member.horseIds[0]) || !memberHorse?.manualLock)) {
      staleHorseMembers.push({
        ...row,
        reason: memberHorse && !memberHorse.manualLock
          ? "horse_member_exists_without_horse_lock"
          : "horse_no_longer_expected_for_wave_or_source_item"
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
      const horse = horseLookup.get(horseId);
      if (!horse?.manualLock) continue;
      const key = `${sourceItem.id}:${horseId}`;
      if (!existingMemberKeys.has(key)) {
        missingHorseMembers.push({
          sourcePackItemId: sourceItem.id,
          sourceItem: sourceItem.appName,
          packingItemId: packingItem.id,
          horseId,
          reason: "locked_horse_member_missing"
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
      packWaveKey: normalizeWave(selectedWave)?.key || "",
      tables: {
        packWaves: tables.wec_pack_waves.id,
        packItems: tables.wec_pack_items.id,
        listPlans: tables.wec_list_plans?.id || "",
        packingItems: tables.wec_packing_items.id,
        packingItemHorses: tables.wec_packing_item_horses.id,
        packingEvents: tables.wec_packing_events.id
      }
    },
    wave,
    waveCounts: {
      frozenHorseCount: numberField(wave?.horseCount),
      countHorsesWaveOne: numberField(wave?.countHorsesWaveOne),
      effectiveHorseCount: numberField(wave?.effectiveHorseCount),
      currentWaveHorseCount: waveHorses.length,
      horseCountMismatch: !!wave && numberField(wave.countHorsesWaveOne) !== numberField(wave.effectiveHorseCount),
      groomCountFinal: numberField(wave?.groomCountFinal),
      groomSanity: numberField(wave?.groomSanity),
      effectiveGroomCountFinal: numberField(wave?.effectiveGroomCountFinal),
      manualLock: !!wave?.manualLock,
      countSource: wave?.countSource || "",
      groomCountSource: wave?.groomCountSource || ""
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

export async function actionReport(airtable, requestUrl, payload) {
  const action = clean(payload?.action);
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
  let result;
  if (action === "add_quantity") {
    result = await applyAddQuantity(airtable, context, payload);
  } else if (action === "set_pack_state") {
    result = await applyPackState(airtable, context, payload);
  } else if (action === "set_resolution") {
    result = await applyResolutionState(airtable, context, payload);
  } else if (action === "update_item_fields") {
    result = await applyItemFieldUpdate(airtable, context, payload);
  } else if (action === "set_horse_pack_state") {
    result = await applyHorsePackState(airtable, tables, payload);
  } else if (action === "set_horse_kit_state") {
    result = await applyHorseKitState(airtable, context, payload);
  } else if (action === "set_horse_record_state") {
    result = await applyHorseRecordState(airtable, tables, payload);
  } else if (action === "set_source_flag") {
    result = await applySourceFlag(airtable, tables, payload);
  } else if (action === "set_onsite_task_state") {
    result = await applyOnsiteTaskState(airtable, tables, payload);
  } else if (action === "add_comment") {
    result = await applyCommentEvent(airtable, context, payload);
  } else if (action === "update_comment") {
    result = await applyCommentUpdate(airtable, context, payload);
  } else if (action !== "session_start") {
    return { ok: false, error: "unknown_action", action };
  }

  const state = await stateReport(airtable, requestUrl);
  if (action === "session_start") {
    result = await applySessionStart(airtable, tables, payload, state);
  }
  return {
    ok: true,
    action,
    result,
    state
  };
}

export async function horseKitLaneReport(airtable, requestUrl) {
  const url = new URL(requestUrl);
  const packWaveId = clean(url.searchParams.get("packWaveId"));
  const packWaveKey = clean(url.searchParams.get("packWaveKey") || url.searchParams.get("wave") || "wave_one");
  const pakGroupsView = "horse_specific";
  const context = await loadWecContext(airtable);
  const baseTables = horseKitLaneTables(context);
  const groupRecords = await listOptionalViewRecords(airtable, baseTables.pak_groups.id, pakGroupsView);
  const groupStack = normalizePakGroupStack(groupRecords, { groupPrefix: "gp" });
  const tables = horseKitLaneTables(context, groupStack);
  const [
    waveRecords,
    legacyHorseRecords,
    pakHorseRosterRecords,
    legacyKitRecords,
    legacyKitItemRecords,
    pakKitRecords,
    pakKitItemRecords,
    packingKitRecords,
    exceptionRecords,
    changeRecords,
    commentRecords,
    commentShortRecords,
    commentLogRecords,
    horseGenderRecords,
    horseDisciplineRecords,
    horseColorRecords,
    horseRosterLogRecords
  ] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_pack_waves.id, tables.wec_pack_waves.view),
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view),
    listOptionalRecords(airtable, tables.pak_horses_roster),
    listAirtableRecords(airtable, tables.horse_kits.id),
    listAirtableRecords(airtable, tables.horse_kit_items.id),
    listOptionalRecords(airtable, tables.pak_kits),
    listOptionalRecords(airtable, tables.pak_kit_items),
    listAirtableRecords(airtable, tables.horse_packing_kits.id),
    listOptionalRecords(airtable, tables.horse_kit_exceptions),
    listOptionalRecords(airtable, tables.horse_kit_changes),
    listOptionalRecords(airtable, tables.wec_commenting),
    listOptionalRecords(airtable, tables.comment_shorts),
    listOptionalRecords(airtable, tables.comment_logs),
    listOptionalRecords(airtable, tables.horse_genders),
    listOptionalRecords(airtable, tables.horse_disciplines),
    listOptionalRecords(airtable, tables.horse_colors),
    listOptionalRecords(airtable, tables.horse_roster_logs)
  ]);
  const usePakKitSource = pakKitRecords.length > 0 || pakKitItemRecords.length > 0;
  const kitRecords = usePakKitSource ? pakKitRecords : legacyKitRecords;
  const kitItemRecords = usePakKitSource ? pakKitItemRecords : legacyKitItemRecords;

  const selectedWaveRecord = selectWave(waveRecords, packWaveId, packWaveKey);
  const selectedWave = selectedWaveRecord ? normalizeWave(selectedWaveRecord) : null;
  const usePakHorseRoster = pakHorseRosterRecords.length > 0;
  const horses = (usePakHorseRoster ? pakHorseRosterRecords : legacyHorseRecords)
    .map(usePakHorseRoster ? normalizePakHorseRoster : normalizeRosterHorse)
    .sort(compareHorseRosterRows);
  const kits = kitRecords.map(normalizeHorseKitTemplate).sort(compareKitTemplates);
  const kitItems = kitItemRecords.map(normalizeHorseKitTemplateItem).sort(compareKitItems);
  const kitItemById = new Map(kitItems.map((item) => [item.id, item]));
  const kitById = new Map(kits.map((kit) => [kit.id, kit]));
  const horseById = new Map(horses.flatMap((horse) => [
    [horse.id, horse],
    ...(horse.writeHorseId && horse.writeHorseId !== horse.id ? [[horse.writeHorseId, horse]] : [])
  ]));
  const waveById = new Map(waveRecords.map((record) => {
    const wave = normalizeWave(record);
    return [wave.id, wave];
  }));
  const packingRows = packingKitRecords
    .map((record) => normalizeHorsePackingKit(record, { horseById, kitById, kitItemById, waveById }))
    .filter((row) => !selectedWave?.id || row.packWaveIds.length === 0 || row.packWaveIds.includes(selectedWave.id))
    .sort(compareHorsePackingRows);
  const rowsByHorse = groupByFirstId(packingRows, "horseIds");
  const horsesWithCounts = horses.map((horse) => ({
    ...horse,
    waveState: horseWaveState(horse),
    kitSummary: horseKitSummary(rowsByHorse.get(horse.id) || [])
  }));
  const visibleHorses = selectedWave
    ? horsesWithCounts.filter((horse) => isHorseInWave(horse, selectedWave))
    : horsesWithCounts;
  const exceptions = exceptionRecords.map(normalizeHorseKitException).sort(compareChangeLikeRows);
  const changes = changeRecords.map(normalizeHorseKitChange).sort(compareChangeLikeRows).slice(0, 50);
  const comments = commentRecords.map(normalizeHorseKitComment).sort(compareChangeLikeRows);
  const commentShorts = commentShortRecords.map(normalizeCommentShort).sort(compareCommentShorts);
  const commentLogs = commentLogRecords.map(normalizeCommentLog).sort(compareChangeLikeRows).slice(0, 50);
  const horseAttributes = [
    ...horseGenderRecords.map((record) => normalizeHorseAttributeOption(record, "horse_gender", "horse_genders", "horse_genders")),
    ...horseDisciplineRecords.map((record) => normalizeHorseAttributeOption(record, "horse_disciplines", "horse_disciplines", "horse_disciplines")),
    ...horseColorRecords.map((record) => normalizeHorseAttributeOption(record, "horse_colors", "horse_colors", "horse_colors"))
  ].sort(compareHorseAttributes);
  const horseRosterLogs = horseRosterLogRecords.map(normalizeHorseRosterLog).sort(compareChangeLikeRows).slice(0, 50);

  return {
    ok: true,
    source: {
      packWaveId: selectedWave?.id || "",
      packWaveKey: selectedWave?.key || packWaveKey,
      pakGroupsView,
      kitSource: usePakKitSource ? "pak" : "legacy",
      horseSource: usePakHorseRoster ? "pak_horses_roster" : "wec_horses",
      tables: Object.fromEntries(Object.entries(tables).map(([key, table]) => [key, table?.id || ""])),
      tableAliases: PAK_GROUP_TABLE_ALIASES
    },
    wave: selectedWave,
    counts: {
      horses: horses.length,
      visibleHorses: visibleHorses.length,
      kits: kits.length,
      kitItems: kitItems.length,
      packingRows: packingRows.length,
      packedRows: packingRows.filter((row) => row.packState === "packed").length,
      notNeededRows: packingRows.filter((row) => row.neededState === "not_needed" || row.packState === "not_needed").length
    },
    horses: visibleHorses,
    allHorses: horsesWithCounts,
    kits,
    kitItems,
    packingRows,
    exceptions,
    changes,
    comments,
    commentShorts,
    commentLogs,
    horseAttributes,
    horseRosterLogs,
    groupStack
  };
}

export async function horseKitLaneActionReport(airtable, requestUrl, payload) {
  const action = clean(payload?.action);
  const context = await loadWecContext(airtable);
  const pakGroupsView = "horse_specific";
  const baseTables = horseKitLaneTables(context);
  const groupRecords = await listOptionalViewRecords(airtable, baseTables.pak_groups.id, pakGroupsView);
  const groupStack = normalizePakGroupStack(groupRecords, { groupPrefix: "gp" });
  const tables = horseKitLaneTables(context, groupStack);
  let result;

  if (action === "set_horse_wave") {
    result = await applyHorseKitLaneWaveState(airtable, tables, payload);
  } else if (action === "create_kit") {
    result = await applyHorseKitCreate(airtable, tables, payload);
  } else if (action === "set_packing_kit_state") {
    result = await applyHorsePackingKitState(airtable, tables, payload);
  } else if (action === "set_packing_kit_needed") {
    result = await applyHorsePackingKitNeededState(airtable, tables, payload);
  } else if (action === "update_packing_kit_quantity") {
    result = await applyHorsePackingKitQuantity(airtable, tables, payload);
  } else if (action === "set_static_kit_item_state") {
    result = await applyStaticHorseKitItemState(airtable, tables, payload);
  } else if (action === "add_missing_packing_rows") {
    result = await applyHorsePackingKitMissingRows(airtable, tables, payload);
  } else if (action === "create_kit_item") {
    result = await applyHorseKitItemCreate(airtable, tables, payload);
  } else if (action === "move_kit_item") {
    result = await applyHorseKitItemMove(airtable, tables, payload);
  } else if (action === "save_comment") {
    result = await applyHorseKitCommentSave(airtable, tables, payload);
  } else if (action === "apply_horse_attribute") {
    result = await applyHorseRosterAttribute(airtable, context, tables, payload);
  } else {
    return { ok: false, error: "unknown_horse_kit_action", action };
  }

  return {
    ok: true,
    action,
    result,
    state: await horseKitLaneReport(airtable, requestUrl)
  };
}

function horseKitLaneTables(context, groupStack = null) {
  const groupTable = (renderKey, fallbackName) => pakGroupPhysicalTableName(groupStack, renderKey, fallbackName);
  return {
    wec_horses: physicalTableConfig(context, "wec_horses"),
    wec_pack_waves: physicalTableConfig(context, "wec_pack_waves"),
    wec_packing_events: physicalTableConfig(context, "wec_packing_events"),
    horse_kits: physicalTableConfig(context, "horse_kits"),
    horse_kit_items: physicalTableConfig(context, "horse_kit_items"),
    pak_kits: physicalTableConfig(context, groupTable("kit_source", "pak_kits"), true),
    pak_kit_items: physicalTableConfig(context, groupTable("drawer_items", "pak_kit_items"), true),
    pak_horses_roster: physicalTableConfig(context, groupTable("main_table", "pak_horses_roster"), true),
    horse_packing_kits: physicalTableConfig(context, groupTable("state_links", "horse_packing_kits")),
    horse_kit_exceptions: physicalTableConfig(context, "horse_kit_exceptions", true),
    horse_kit_changes: physicalTableConfig(context, groupTable("change_log", "horse_kit_changes"), true),
    wec_commenting: physicalTableConfig(context, groupTable("comments", "wec_commenting"), true),
    comment_shorts: physicalTableConfig(context, "comment_shorts", true),
    comment_logs: physicalTableConfig(context, "comment_logs", true),
    ww_horses: physicalTableConfig(context, "ww_horses", true),
    horse_genders: physicalTableConfig(context, "horse_genders", true),
    horse_disciplines: physicalTableConfig(context, "horse_disciplines", true),
    horse_colors: physicalTableConfig(context, "horse_colors", true),
    horse_attributes: physicalTableConfig(context, "horse_attributes", true),
    horse_roster_logs: physicalTableConfig(context, "horses_change_log", true),
    pak_groups: physicalTableConfig(context, "pak_groups", true)
  };
}

const PAK_GROUP_TABLE_ALIASES = {
  pak_horses: "pak_horses_roster",
  pak_horse_kits_list: "pak_kits",
  pak_horse_kit_items: "pak_kit_items",
  pak_horse_kit_links: "horse_packing_kits",
  pak_horse_kit_logs: "horse_kit_changes",
  pak_comments: "wec_commenting"
};

function normalizePakGroupStack(records = [], options = {}) {
  const groupPrefix = clean(options.groupPrefix || "");
  const rows = records
    .map((record, index) => normalizePakGroupRow(record, index))
    .filter((row) => row.groupPrefix === groupPrefix)
    .sort((a, b) => a.sortOrder - b.sortOrder || a.stack - b.stack || a.sourceIndex - b.sourceIndex);
  return {
    groupPrefix,
    rows,
    activeRows: rows.filter((row) => row.active && !row.hidden),
    hiddenRows: rows.filter((row) => row.hidden)
  };
}

function normalizePakGroupRow(record, index = 0) {
  const fields = record.fields || {};
  const tableName = clean(fields.table_name);
  const physicalTableName = clean(fields.physical_table) || PAK_GROUP_TABLE_ALIASES[tableName] || tableName;
  const sortOrder = numberField(fields.sort_order);
  return {
    id: record.id,
    sourceIndex: index,
    groupKey: clean(fields.group_key),
    groupPrefix: clean(fields.gp_pre),
    stack: numberField(fields.stack),
    sortOrder,
    role: slugify(fields.role),
    renderKey: slugify(fields.render_key),
    displayLabel: clean(fields.display_label),
    componentKey: clean(fields.component_key),
    tableName,
    physicalTableName,
    active: !!fields.active,
    hidden: !!fields.is_hidden,
    drillDown: !!fields.is_drill_down,
    addFilter: !!fields.add_filter,
    filterBy: stringListField(fields.filter_by),
    addSearch: !!fields.add_search,
    searchBy: stringListField(fields.search_by),
    addAggregates: !!fields.add_aggregates,
    pakAggIds: linkedIds(fields.pak_aggs),
    aggregates: stringListField(fields.all_aggregates),
    needsUi: stringListField(fields.needs_ui),
    allowAddNew: !!fields.allow_add_new,
    allowInlineEdit: !!fields.allow_inline_edit
  };
}

function pakGroupPhysicalTableName(groupStack, renderKey, fallbackName) {
  const normalizedKey = slugify(renderKey);
  const row = (groupStack?.activeRows || []).find((candidate) => candidate.renderKey === normalizedKey);
  return row?.physicalTableName || fallbackName;
}

function physicalTableConfig(context, name, optional = false) {
  const existing = context.tables?.[name];
  const schemaTable = findSchemaTable(context.schema, name, existing?.id || name);
  const tableId = existing?.id || schemaTable?.id || "";
  if (!tableId && !optional) throw new Error(`missing_table:${name}`);
  if (!tableId) return null;
  return {
    id: tableId,
    name,
    view: existing?.view || ""
  };
}

async function applyHorseKitLaneWaveState(airtable, tables, payload) {
  const horseId = clean(payload?.horseId);
  const waveState = slugify(payload?.waveState || payload?.state);
  if (!horseId) throw new Error("missing_horse_id");
  if (!["wave_one", "wave_1", "wave_two", "wave_2", "not_going"].includes(waveState)) {
    throw new Error("invalid_horse_wave_state");
  }
  const normalizedState = waveState === "wave_1" ? "wave_one" : waveState === "wave_2" ? "wave_two" : waveState;
  const { record } = await findRecordInConfiguredView(airtable, tables.wec_horses, horseId);
  const before = {
    wec_wave_1: !!record.fields?.wec_wave_1,
    wec_wave_2: !!record.fields?.wec_wave_2,
    wec_not_going: !!record.fields?.wec_not_going
  };
  const fields = {
    wec_wave_1: normalizedState === "wave_one",
    wec_wave_2: normalizedState === "wave_two",
    wec_not_going: normalizedState === "not_going"
  };
  const updated = await patchAirtableRecord(airtable, tables.wec_horses.id, horseId, fields);
  const event = await createPackingEvent(airtable, tables, {
    eventType: "wave_count_change",
    eventSubjectId: horseId,
    horseIds: [horseId],
    quantityDelta: 0,
    quantityBefore: 0,
    quantityAfter: 0,
    notes: [
      `horse_id: ${horseId}`,
      `before: ${JSON.stringify(before)}`,
      `after: ${JSON.stringify(fields)}`
    ].join("\n")
  });
  return { updated, event };
}

async function applyHorsePackingKitState(airtable, tables, payload) {
  const rowId = clean(payload?.packingKitId || payload?.rowId);
  const nextState = slugify(payload?.packState || payload?.state);
  if (!rowId) throw new Error("missing_horse_packing_kit_id");
  if (!["packed", "not_packed", "not_needed"].includes(nextState)) throw new Error("invalid_pack_state");
  const { record } = await findRecordInConfiguredView(airtable, tables.horse_packing_kits, rowId);
  const beforeState = stringField(record.fields?.pack_state || "not_packed");
  const beforeQuantity = wholeQuantityField(record.fields?.quantity_packed);
  const needed = nextState === "not_needed" ? 0 : wholeQuantityField(record.fields?.quantity_needed || 1);
  const afterQuantity = nextState === "packed" ? needed : 0;
  const updateFields = {
    pack_state: nextState,
    quantity_packed: afterQuantity
  };
  if (nextState === "not_needed") updateFields.needed_state = "not_needed";
  const updated = await patchAirtableRecord(airtable, tables.horse_packing_kits.id, rowId, updateFields);
  const eventType = nextState === "packed"
    ? "horse_kit_packed"
    : "horse_kit_reopened";
  const event = await createPackingEvent(airtable, tables, {
    eventType,
    eventSubjectId: rowId,
    packWaveIds: linkedIds(record.fields?.pack_wave),
    horseIds: linkedIds(record.fields?.horse),
    quantityDelta: afterQuantity - beforeQuantity,
    quantityBefore: beforeQuantity,
    quantityAfter: afterQuantity,
    packStateBefore: eventPackStateValue(beforeState),
    packStateAfter: eventPackStateValue(nextState),
    notes: horsePackingKitEventNotes(record, payload)
  });
  await createHorseKitChange(airtable, tables, {
    changeType: nextState === "not_needed" ? "exception_applied" : "quantity_changed",
    packingKitRecord: record,
    oldValue: { pack_state: beforeState, quantity_packed: beforeQuantity },
    newValue: updateFields,
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyHorsePackingKitNeededState(airtable, tables, payload) {
  const rowId = clean(payload?.packingKitId || payload?.rowId);
  const neededState = slugify(payload?.neededState || payload?.state);
  if (!rowId) throw new Error("missing_horse_packing_kit_id");
  if (!["needed", "not_needed"].includes(neededState)) throw new Error("invalid_needed_state");
  const { record } = await findRecordInConfiguredView(airtable, tables.horse_packing_kits, rowId);
  const beforeState = stringField(record.fields?.needed_state || "needed");
  const fields = neededState === "not_needed"
    ? { needed_state: "not_needed", pack_state: "not_needed", quantity_packed: 0 }
    : { needed_state: "needed", pack_state: "not_packed", quantity_packed: 0 };
  const updated = await patchAirtableRecord(airtable, tables.horse_packing_kits.id, rowId, fields);
  const event = await createPackingEvent(airtable, tables, {
    eventType: "horse_kit_reopened",
    eventSubjectId: rowId,
    packWaveIds: linkedIds(record.fields?.pack_wave),
    horseIds: linkedIds(record.fields?.horse),
    quantityDelta: 0,
    quantityBefore: wholeQuantityField(record.fields?.quantity_packed),
    quantityAfter: 0,
    packStateBefore: eventPackStateValue(stringField(record.fields?.pack_state || "not_packed")),
    packStateAfter: eventPackStateValue(fields.pack_state),
    notes: horsePackingKitEventNotes(record, payload)
  });
  await createHorseKitChange(airtable, tables, {
    changeType: neededState === "not_needed" ? "exception_applied" : "exception_resolved",
    packingKitRecord: record,
    oldValue: { needed_state: beforeState },
    newValue: fields,
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyHorsePackingKitQuantity(airtable, tables, payload) {
  const rowId = clean(payload?.packingKitId || payload?.rowId);
  const quantityNeeded = wholeQuantityField(payload?.quantityNeeded ?? payload?.quantity);
  if (!rowId) throw new Error("missing_horse_packing_kit_id");
  if (quantityNeeded < 0) throw new Error("invalid_quantity_needed");
  const { record } = await findRecordInConfiguredView(airtable, tables.horse_packing_kits, rowId);
  const beforeQuantity = wholeQuantityField(record.fields?.quantity_needed);
  const beforePacked = wholeQuantityField(record.fields?.quantity_packed);
  const packState = stringField(record.fields?.pack_state || "not_packed");
  const fields = {
    quantity_needed: quantityNeeded,
    quantity_packed: packState === "packed" ? quantityNeeded : Math.min(beforePacked, quantityNeeded)
  };
  const updated = await patchAirtableRecord(airtable, tables.horse_packing_kits.id, rowId, fields);
  const event = await createPackingEvent(airtable, tables, {
    eventType: "horse_kit_reopened",
    eventSubjectId: rowId,
    packWaveIds: linkedIds(record.fields?.pack_wave),
    horseIds: linkedIds(record.fields?.horse),
    quantityDelta: quantityNeeded - beforeQuantity,
    quantityBefore: beforeQuantity,
    quantityAfter: quantityNeeded,
    packStateBefore: eventPackStateValue(packState),
    packStateAfter: eventPackStateValue(packState),
    notes: horsePackingKitEventNotes(record, payload)
  });
  await createHorseKitChange(airtable, tables, {
    changeType: "quantity_changed",
    packingKitRecord: record,
    oldValue: { quantity_needed: beforeQuantity, quantity_packed: beforePacked },
    newValue: fields,
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyStaticHorseKitItemState(airtable, tables, payload) {
  const horseId = clean(payload?.horseId);
  const kitId = clean(payload?.kitId);
  const kitItemId = clean(payload?.kitItemId);
  const packWaveId = clean(payload?.packWaveId);
  const nextState = slugify(payload?.packState || payload?.state);
  if (!horseId) throw new Error("missing_horse_id");
  if (!kitItemId) throw new Error("missing_kit_item_id");
  if (!packWaveId) throw new Error("missing_pack_wave_id");
  if (!["packed", "not_packed", "not_needed"].includes(nextState)) throw new Error("invalid_static_kit_item_state");

  const [horses, legacyKitItems, pakKitItems, existingRows] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view),
    listAirtableRecords(airtable, tables.horse_kit_items.id),
    listOptionalRecords(airtable, tables.pak_kit_items),
    listAirtableRecords(airtable, tables.horse_packing_kits.id)
  ]);
  const horse = horses.find((record) => record.id === horseId);
  const kitItems = [
    ...legacyKitItems.map((record) => ({ ...record, kitSource: "legacy" })),
    ...pakKitItems.map((record) => ({ ...record, kitSource: "pak" }))
  ];
  const itemRecord = kitItems.find((record) => record.id === kitItemId);
  if (!horse) throw new Error("horse_not_found");
  if (!itemRecord) throw new Error("kit_item_not_found");

  const item = normalizeHorseKitTemplateItem(itemRecord);
  const usesPakKitSource = itemRecord.kitSource === "pak";

  const existing = existingRows.find((record) =>
    includesLinkedId(record.fields?.horse, horseId) &&
    includesLinkedId(record.fields?.pack_wave, packWaveId) &&
    includesLinkedId(usesPakKitSource ? record.fields?.pak_kit_items : record.fields?.horse_kit_item, kitItemId) &&
    (!kitId || includesLinkedId(usesPakKitSource ? record.fields?.pak_kits : record.fields?.horse_kits, kitId))
  );

  const beforeState = stringField(existing?.fields?.pack_state || "not_packed");
  const beforeNeededState = stringField(existing?.fields?.needed_state || "needed");
  const beforeQuantity = wholeQuantityField(existing?.fields?.quantity_packed);
  const quantityNeeded = wholeQuantityField(existing?.fields?.quantity_needed || item.manualQuantity || 1) || 1;
  const updateFields = nextState === "not_needed"
    ? { needed_state: "not_needed", pack_state: "not_needed", quantity_needed: quantityNeeded, quantity_packed: 0 }
    : {
        needed_state: "needed",
        pack_state: nextState,
        quantity_needed: quantityNeeded,
        quantity_packed: nextState === "packed" ? quantityNeeded : 0
      };

  let packingKitRecord = existing;
  let updated;
  if (existing) {
    updated = await patchAirtableRecord(airtable, tables.horse_packing_kits.id, existing.id, updateFields);
    packingKitRecord = { id: existing.id, fields: { ...existing.fields, ...updateFields } };
  } else {
    const createFields = compactFields({
      horse_packing_kit: `${stringField(horse.fields?.barn_name || horse.fields?.horse || horse.fields?.show_name)} - ${item.displayName || item.name}`,
      pack_wave: [packWaveId],
      horse: [horseId],
      horse_kits: !usesPakKitSource && kitId ? [kitId] : undefined,
      horse_kit_item: !usesPakKitSource ? [kitItemId] : undefined,
      pak_kits: usesPakKitSource && kitId ? [kitId] : undefined,
      pak_kit_items: usesPakKitSource ? [kitItemId] : undefined,
      sort_order: item.sortOrder,
      ...updateFields
    });
    updated = await createAirtableRecord(airtable, tables.horse_packing_kits.id, createFields);
    packingKitRecord = updated;
  }

  const change = await createHorseKitChange(airtable, tables, {
    changeType: nextState === "not_needed" ? "exception_applied" : "quantity_changed",
    packingKitRecord,
    kitIds: !usesPakKitSource && kitId ? [kitId] : undefined,
    itemIds: !usesPakKitSource ? [kitItemId] : undefined,
    pakKitIds: usesPakKitSource && kitId ? [kitId] : undefined,
    pakItemIds: usesPakKitSource ? [kitItemId] : undefined,
    oldValue: { pack_state: beforeState, needed_state: beforeNeededState, quantity_packed: beforeQuantity },
    newValue: updateFields,
    notes: `${stringField(horse.fields?.barn_name || horse.fields?.horse || horse.fields?.show_name)} ${item.displayName || item.name} ${nextState}`
  });
  return { updated, change, created: !existing };
}

async function applyHorsePackingKitMissingRows(airtable, tables, payload) {
  const horseId = clean(payload?.horseId);
  const kitId = clean(payload?.kitId);
  const packWaveId = clean(payload?.packWaveId);
  if (!horseId) throw new Error("missing_horse_id");
  if (!kitId) throw new Error("missing_kit_id");
  if (!packWaveId) throw new Error("missing_pack_wave_id");
  const [horses, kits, kitItems, existingRows] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view),
    listAirtableRecords(airtable, tables.horse_kits.id),
    listAirtableRecords(airtable, tables.horse_kit_items.id),
    listAirtableRecords(airtable, tables.horse_packing_kits.id)
  ]);
  const horse = horses.find((record) => record.id === horseId);
  const kit = kits.find((record) => record.id === kitId);
  if (!horse) throw new Error("horse_not_found");
  if (!kit) throw new Error("kit_not_found");
  const present = new Set(existingRows
    .filter((record) => includesLinkedId(record.fields?.horse, horseId) && includesLinkedId(record.fields?.pack_wave, packWaveId))
    .map((record) => firstLinkedId(record.fields?.horse_kit_item))
    .filter(Boolean));
  const itemsToCreate = kitItems
    .map(normalizeHorseKitTemplateItem)
    .filter((item) => item.kitIds.includes(kitId) && item.status !== "inactive" && !present.has(item.id));
  const created = [];
  for (const item of itemsToCreate) {
    const fields = {
      horse_packing_kit: `${stringField(horse.fields?.barn_name || horse.fields?.horse || horse.fields?.show_name)} - ${item.displayName || item.name}`,
      pack_wave: [packWaveId],
      horse: [horseId],
      horse_kits: [kitId],
      horse_kit_item: [item.id],
      needed_state: "needed",
      pack_state: "not_packed",
      quantity_needed: item.manualQuantity || 1,
      quantity_packed: 0,
      sort_order: item.sortOrder
    };
    created.push(await createAirtableRecord(airtable, tables.horse_packing_kits.id, fields));
  }
  await createHorseKitChange(airtable, tables, {
    changeType: "kit_item_added",
    kitIds: [kitId],
    oldValue: { created: 0 },
    newValue: { created: created.length, horseId, packWaveId },
    notes: `Added missing packing rows for ${stringField(horse.fields?.barn_name || horse.fields?.horse || horse.fields?.show_name)}`
  });
  return { created, count: created.length };
}

async function applyHorseKitCreate(airtable, tables, payload) {
  const label = clean(payload?.kit || payload?.label || payload?.name);
  if (!label) throw new Error("missing_kit_label");
  const fields = {
    kit: label,
    status: slugify(payload?.status || "active") === "inactive" ? "inactive" : "active",
    sort_order: wholeQuantityField(payload?.sortOrder),
    notes: clean(payload?.notes)
  };
  const created = await createAirtableRecord(airtable, tables.horse_kits.id, fields);
  await createHorseKitChange(airtable, tables, {
    changeType: "kit_created",
    kitIds: [created.id],
    oldValue: {},
    newValue: fields,
    notes: clean(payload?.notes)
  });
  return { created };
}

async function applyHorseKitItemCreate(airtable, tables, payload) {
  const kitId = clean(payload?.kitId);
  const label = clean(payload?.label || payload?.itemName || payload?.kitItem);
  const quantity = wholeQuantityField(payload?.quantity || payload?.manualQuantity || 1) || 1;
  if (!kitId) throw new Error("missing_kit_id");
  if (!label) throw new Error("missing_kit_item_label");
  const [legacyKits, pakKits] = await Promise.all([
    listAirtableRecords(airtable, tables.horse_kits.id),
    listOptionalRecords(airtable, tables.pak_kits)
  ]);
  const usesPakKitSource = pakKits.some((record) => record.id === kitId) || !legacyKits.some((record) => record.id === kitId);
  const fields = {
    kit_item: label,
    horse_kits: usesPakKitSource ? undefined : [kitId],
    pak_kits: usesPakKitSource ? [kitId] : undefined,
    display_name: usesPakKitSource ? undefined : clean(payload?.displayName) || label,
    display_label: usesPakKitSource ? clean(payload?.displayName) || label : undefined,
    manual_quantity: quantity,
    uom: clean(payload?.uom),
    item_status: "active",
    inline_edit: usesPakKitSource ? undefined : "allowed",
    sort_order: wholeQuantityField(payload?.sortOrder)
  };
  const created = await createAirtableRecord(
    airtable,
    usesPakKitSource ? tables.pak_kit_items.id : tables.horse_kit_items.id,
    compactFields(fields)
  );
  await createHorseKitChange(airtable, tables, {
    changeType: "kit_item_added",
    kitIds: usesPakKitSource ? undefined : [kitId],
    itemIds: usesPakKitSource ? undefined : [created.id],
    pakKitIds: usesPakKitSource ? [kitId] : undefined,
    pakItemIds: usesPakKitSource ? [created.id] : undefined,
    oldValue: {},
    newValue: fields,
    notes: clean(payload?.notes)
  });
  return { created };
}

async function applyHorseKitItemMove(airtable, tables, payload) {
  const itemId = clean(payload?.kitItemId || payload?.itemId);
  const kitId = clean(payload?.kitId || payload?.targetKitId);
  if (!itemId) throw new Error("missing_kit_item_id");
  if (!kitId) throw new Error("missing_target_kit_id");
  const { record } = await findRecordInConfiguredView(airtable, tables.horse_kit_items, itemId);
  const beforeKitIds = linkedIds(record.fields?.horse_kits);
  const updated = await patchAirtableRecord(airtable, tables.horse_kit_items.id, itemId, {
    horse_kits: [kitId]
  });
  await createHorseKitChange(airtable, tables, {
    changeType: "kit_item_added",
    kitIds: [kitId],
    itemIds: [itemId],
    oldValue: { horse_kits: beforeKitIds },
    newValue: { horse_kits: [kitId] },
    notes: clean(payload?.notes) || "Moved kit item to another kit."
  });
  return { updated };
}

async function applyHorseKitCommentSave(airtable, tables, payload) {
  if (!tables.wec_commenting?.id) throw new Error("commenting_table_not_configured");
  const commentId = clean(payload?.commentId);
  const horseId = clean(payload?.horseId || payload?.scopeId);
  const scopeLabel = clean(payload?.scopeLabel);
  const packWaveId = clean(payload?.packWaveId);
  const commentShortId = clean(payload?.commentShortId);
  const commentShort = commentShortId && tables.comment_shorts?.id
    ? await findOptionalRecordInConfiguredView(airtable, tables.comment_shorts, commentShortId)
    : null;
  const shortText = clean(commentShort?.fields?.display_label || commentShort?.fields?.comment_short);
  const comment = clean(payload?.comment || shortText);
  if (!horseId) throw new Error("missing_comment_horse_id");
  if (!comment) throw new Error("comment_required");

  const before = commentId
    ? await findOptionalRecordInConfiguredView(airtable, tables.wec_commenting, commentId)
    : null;
  const fields = compactFields({
    event: before ? undefined : `comment:horse:${horseId}:${Date.now()}`,
    pack_wave: packWaveId ? [packWaveId] : undefined,
    horse: [horseId],
    event_type: before ? "comment_edit" : "comment_add",
    scope_type: "horse",
    scope_id: horseId,
    scope_label: scopeLabel || horseId,
    comment_status: before ? "edited" : "active",
    comment,
    notes: formatCommentNotes({
      scopeType: "horse",
      scopeId: horseId,
      scopeLabel: scopeLabel || horseId,
      comment
    }),
    created_at: before ? undefined : new Date().toISOString().slice(0, 10),
    created_by: "webflow"
  });
  const saved = before
    ? await patchAirtableRecord(airtable, tables.wec_commenting.id, before.id, fields)
    : await createAirtableRecord(airtable, tables.wec_commenting.id, fields);
  const log = await createHorseKitCommentLog(airtable, tables, {
    commentRecord: saved,
    commentShortId,
    action: before ? "edited" : "created",
    scopeType: "horse",
    scopeId: horseId,
    scopeLabel: scopeLabel || horseId,
    oldValue: before ? { comment: stringField(before.fields?.comment) } : {},
    newValue: { comment },
    notes: clean(payload?.notes)
  });
  return { comment: saved, log };
}

async function createHorseKitCommentLog(airtable, tables, payload) {
  if (!tables.comment_logs?.id) return null;
  return createAirtableRecord(airtable, tables.comment_logs.id, compactFields({
    comment_log: `comment:${payload.action}:${payload.scopeId}:${Date.now()}`,
    wec_commenting: payload.commentRecord?.id ? [payload.commentRecord.id] : undefined,
    comment_shorts: payload.commentShortId ? [payload.commentShortId] : undefined,
    action: payload.action,
    scope_type: payload.scopeType,
    scope_id: payload.scopeId,
    scope_label: payload.scopeLabel,
    old_value: stringifyChangeValue(payload.oldValue),
    new_value: stringifyChangeValue(payload.newValue),
    created_by: "webflow",
    notes: payload.notes
  }));
}

async function applyHorseRosterAttribute(airtable, context, tables, payload) {
  if (!tables.pak_horses_roster?.id) throw new Error("horse_roster_table_not_configured");
  if (!tables.ww_horses?.id) throw new Error("ww_horses_table_not_configured");
  const horseId = clean(payload?.horseId);
  const rosterId = clean(payload?.rosterId);
  const attributeId = clean(payload?.attributeId);
  const attributeGroup = slugify(payload?.attributeGroup || payload?.group);
  if (!horseId && !rosterId) throw new Error("missing_roster_horse_id");
  if (!attributeId) throw new Error("missing_horse_attribute_id");

  const horseRecord = await findPakHorseRosterRecord(airtable, tables, { horseId, rosterId });
  if (!horseRecord) throw new Error("roster_horse_not_found");
  const selection = await findHorseAttributeSelection(airtable, tables, attributeId, attributeGroup);
  if (!selection?.record) throw new Error("horse_attribute_not_found");

  const wwHorseId = linkedIds(horseRecord.fields?.ww_horses)[0];
  if (!wwHorseId) throw new Error("roster_missing_ww_horse_link");
  const wwHorse = await findOptionalRecordInConfiguredView(airtable, tables.ww_horses, wwHorseId);
  if (!wwHorse) throw new Error("ww_horse_not_found");

  const profileFields = tableFieldNames(context, tables.ww_horses);
  if (!profileFields.has(selection.fieldKey)) throw new Error(`horse_attribute_field_not_found:${selection.fieldKey}`);
  const beforeValue = linkedIds(wwHorse.fields?.[selection.fieldKey]);
  const nextValue = [attributeId];
  const updated = await patchAirtableRecord(airtable, tables.ww_horses.id, wwHorse.id, {
    [selection.fieldKey]: nextValue
  });
  const log = await createHorseRosterLog(airtable, tables, {
    horseRecord,
    wwHorse,
    attributeRecord: selection.record,
    action: "apply_attribute",
    fieldKey: selection.fieldKey,
    oldValue: beforeValue,
    newValue: nextValue,
    notes: clean(payload?.notes)
  });
  return { updated, log };
}

async function createHorseRosterLog(airtable, tables, payload) {
  if (!tables.horse_roster_logs?.id) return null;
  const horseFields = payload.horseRecord?.fields || {};
  const scopeLabel = stringField(
    horseFields.display_horse_barn_name ||
    horseFields.barn_name ||
    horseFields.show_name ||
    horseFields.horse ||
    payload.horseRecord?.id
  );
  return createAirtableRecord(airtable, tables.horse_roster_logs.id, compactFields({
    change_key: `horse_roster:${payload.action}:${payload.horseRecord?.id || "horse"}:${Date.now()}`,
    horse_record_id: payload.wwHorse?.id || payload.horseRecord?.id,
    horse_key: stringField(horseFields.pak_horse_id || payload.horseRecord?.id),
    horse_name: scopeLabel,
    source: "webflow_horse_kits",
    record_key: payload.horseRecord?.id,
    payload_json: stringifyChangeValue({
      rosterId: payload.horseRecord?.id,
      wwHorseId: payload.wwHorse?.id,
      attributeId: payload.attributeRecord?.id,
      fieldKey: payload.fieldKey
    }),
    status: "applied",
    field_name: payload.fieldKey,
    old_value: stringifyChangeValue(payload.oldValue),
    new_value: stringifyChangeValue(payload.newValue),
    changed_at: new Date().toISOString(),
    notes: payload.notes
  }));
}

async function findPakHorseRosterRecord(airtable, tables, { horseId, rosterId }) {
  if (rosterId) {
    const direct = await findOptionalRecordInConfiguredView(airtable, tables.pak_horses_roster, rosterId);
    if (direct) return direct;
  }
  if (horseId) {
    const direct = await findOptionalRecordInConfiguredView(airtable, tables.pak_horses_roster, horseId);
    if (direct) return direct;
    const records = await listOptionalRecords(airtable, tables.pak_horses_roster);
    return records.find((record) => includesLinkedId(record.fields?.wec_horses, horseId) || includesLinkedId(record.fields?.ww_horses, horseId)) || null;
  }
  return null;
}

async function findHorseAttributeSelection(airtable, tables, attributeId, attributeGroup) {
  const candidates = [
    { group: "horse_gender", fieldKey: "horse_genders", table: tables.horse_genders },
    { group: "horse_genders", fieldKey: "horse_genders", table: tables.horse_genders },
    { group: "horse_disciplines", fieldKey: "horse_disciplines", table: tables.horse_disciplines },
    { group: "horse_colors", fieldKey: "horse_colors", table: tables.horse_colors }
  ].filter((candidate) => candidate.table?.id);
  const ordered = attributeGroup
    ? [...candidates.filter((candidate) => candidate.group === attributeGroup), ...candidates.filter((candidate) => candidate.group !== attributeGroup)]
    : candidates;
  for (const candidate of ordered) {
    const record = await findOptionalRecordInConfiguredView(airtable, candidate.table, attributeId);
    if (record) return { ...candidate, record };
  }
  return null;
}

async function applyCommentEvent(airtable, context, payload) {
  const tables = context.tables;
  const scopeType = slugify(payload?.scopeType);
  const scopeId = clean(payload?.scopeId);
  const scopeLabel = clean(payload?.scopeLabel);
  const comment = clean(payload?.comment || payload?.notes);
  const allowedScopes = ["item", "section", "tab", "wave"];
  if (!allowedScopes.includes(scopeType)) throw new Error("invalid_comment_scope");
  if (!scopeId) throw new Error("missing_comment_scope_id");
  if (!comment) throw new Error("comment_required");

  const itemId = clean(payload?.itemId || (scopeType === "item" ? scopeId : ""));
  const itemRecord = itemId
    ? await findOptionalRecordInConfiguredView(airtable, tables.wec_packing_items, itemId)
    : null;
  const packWaveId = clean(payload?.packWaveId || (scopeType === "wave" ? scopeId : ""));
  const showId = clean(payload?.showId);
  if (tables.wec_commenting?.id) {
    const commentRecord = await createWecComment(airtable, context, {
      scopeType,
      scopeId,
      scopeLabel,
      comment,
      itemRecord,
      showIds: showId ? [showId] : undefined,
      packWaveIds: packWaveId ? [packWaveId] : undefined
    });
    return { comment: commentRecord, table: "wec_commenting" };
  }
  const event = await createPackingEvent(airtable, tables, {
    eventType: "comment_add",
    eventSubjectId: `${scopeType}:${scopeId}`,
    itemRecord,
    showIds: showId ? [showId] : undefined,
    packWaveIds: packWaveId ? [packWaveId] : undefined,
    quantityDelta: 0,
    quantityBefore: 0,
    quantityAfter: 0,
    notes: formatCommentNotes({
      scopeType,
      scopeId,
      scopeLabel,
      comment
    })
  });
  return { event, table: "wec_packing_events" };
}

async function applyCommentUpdate(airtable, context, payload) {
  const tables = context.tables;
  const commentId = clean(payload?.commentId);
  const scopeType = slugify(payload?.scopeType);
  const scopeId = clean(payload?.scopeId);
  const scopeLabel = clean(payload?.scopeLabel);
  const comment = clean(payload?.comment || payload?.notes);
  if (!tables.wec_commenting?.id) throw new Error("commenting_table_not_configured");
  if (!commentId) throw new Error("missing_comment_id");
  if (!comment) throw new Error("comment_required");
  const fieldNames = tableFieldNames(context, tables.wec_commenting);
  const fields = fieldsAllowedBySchema(compactFields({
    comment,
    notes: formatCommentNotes({
      scopeType,
      scopeId,
      scopeLabel,
      comment
    }),
    comment_status: "edited"
  }), fieldNames);
  const updated = await patchAirtableRecord(airtable, tables.wec_commenting.id, commentId, fields);
  return { comment: updated, table: "wec_commenting" };
}

function formatCommentNotes({ scopeType, scopeId, scopeLabel, comment }) {
  return [
    `comment_scope: ${scopeType}`,
    `comment_scope_id: ${scopeId}`,
    `comment_scope_label: ${scopeLabel || scopeId}`,
    "",
    comment
  ].join("\n");
}

async function createWecComment(airtable, context, payload) {
  const tables = context.tables;
  const fields = compactFields({
    event: `comment:${payload.scopeType}:${payload.scopeId}:${Date.now()}`,
    show: payload.showIds,
    pack_wave: payload.packWaveIds,
    packing_item: payload.itemRecord?.id ? [payload.itemRecord.id] : [],
    event_type: "comment_add",
    scope_type: payload.scopeType,
    scope_id: payload.scopeId,
    scope_label: payload.scopeLabel,
    comment_status: "active",
    comment: payload.comment,
    notes: formatCommentNotes(payload),
    created_at: new Date().toISOString().slice(0, 10),
    created_by: "webflow"
  });
  const fieldNames = tableFieldNames(context, tables.wec_commenting);
  const commentFields = fieldsAllowedBySchema(fields, fieldNames);
  const response = await fetch(airtableUrl(airtable.baseId, tables.wec_commenting.id), {
    method: "POST",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: [{ fields: commentFields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`comment ${response.status}: ${JSON.stringify(result)}`);
  }
  return {
    id: result.records?.[0]?.id || "",
    fields: result.records?.[0]?.fields || commentFields
  };
}

function tableFieldNames(context, tableConfig) {
  const schemaTable = findSchemaTable(context.schema, tableConfig?.name, tableConfig?.id);
  return new Set((schemaTable?.fields || []).map((field) => field.name));
}

function fieldsAllowedBySchema(fields, fieldNames) {
  if (!fieldNames?.size) return fields;
  return Object.fromEntries(Object.entries(fields).filter(([name]) => fieldNames.has(name)));
}

async function applySessionStart(airtable, tables, payload, state) {
  const sessionId = clean(payload?.sessionId || "unknown_session").slice(0, 120);
  const wave = state?.wave || {};
  const showId = clean(state?.source?.showId);
  const packWaveId = clean(state?.source?.packWaveId);
  const baseNotes = [
    `session: ${sessionId}`,
    `wave: ${wave.key || wave.wave || ""}`,
    `client_url: ${clean(payload?.clientUrl).slice(0, 300)}`,
    waveCountNoteLine(wave)
  ].filter(Boolean).join("\n");

  const sessionEvent = await createPackingEvent(airtable, tables, {
    eventType: "session_start",
    showIds: showId ? [showId] : [],
    packWaveIds: packWaveId ? [packWaveId] : [],
    quantityDelta: 0,
    quantityBefore: 0,
    quantityAfter: 0,
    notes: baseNotes
  });

  const countNotes = waveCountChangeNotes(wave);
  let countEvent = null;
  if (countNotes) {
    countEvent = await createPackingEvent(airtable, tables, {
      eventType: "wave_count_change",
      showIds: showId ? [showId] : [],
      packWaveIds: packWaveId ? [packWaveId] : [],
      quantityDelta: 0,
      quantityBefore: 0,
      quantityAfter: 0,
      notes: [
        `session: ${sessionId}`,
        `wave: ${wave.key || wave.wave || ""}`,
        countNotes
      ].filter(Boolean).join("\n")
    });
  }

  return {
    sessionEvent,
    countEvent,
    countChanged: !!countEvent
  };
}

function waveCountNoteLine(wave) {
  if (!wave) return "";
  return [
    `count_horses_wave_one=${quantityDisplay(wave.countHorsesWaveOne)}`,
    `effective_horse_count=${quantityDisplay(wave.effectiveHorseCount)}`,
    `groom_sanity=${quantityDisplay(wave.groomSanity)}`,
    `effective_groom_count_final=${quantityDisplay(wave.effectiveGroomCountFinal)}`,
    `count_source=${wave.countSource || ""}`,
    `groom_count_source=${wave.groomCountSource || ""}`
  ].join("; ");
}

function waveCountChangeNotes(wave) {
  if (!wave || wave.countsLocked) return "";
  const changes = [];
  if (numberField(wave.countHorsesWaveOne) !== numberField(wave.effectiveHorseCount)) {
    changes.push(`count_horses_wave_one changed: stored=${quantityDisplay(wave.countHorsesWaveOne)} current=${quantityDisplay(wave.effectiveHorseCount)} source=${wave.countSource || ""}`);
  }
  if (numberField(wave.groomSanity) !== numberField(wave.effectiveGroomCountFinal)) {
    changes.push(`groom_sanity changed: stored=${quantityDisplay(wave.groomSanity)} current=${quantityDisplay(wave.effectiveGroomCountFinal)} source=${wave.groomCountSource || ""}`);
  }
  return changes.join("\n");
}

async function healthReportFromContext(airtable, context) {
  const required = REQUIRED_TABLES.map((name) => {
    const registryRow = context.registry.byName[name] || null;
    const tableId = context.tables[name]?.id || registryRow?.tableApi || "";
    const schemaTable = findSchemaTable(context.schema, name, tableId);
    const allowedFields = registryRow?.fieldsAllowed || [];
    const schemaFields = new Set((schemaTable?.fields || []).map((field) => field.name));
    const missingFields = requiredMissingFields(name, allowedFields, schemaFields);
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

function requiredMissingFields(tableName, allowedFields, schemaFields) {
  const optionalLegacy = OPTIONAL_LEGACY_FIELDS_BY_TABLE[tableName] || new Set();
  return allowedFields.filter((field) => !schemaFields.has(field) && !optionalLegacy.has(field));
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
  for (const name of OPTIONAL_TABLES) {
    if (tables[name]) continue;
    const fallbackEnvKeys = ENV_TABLES[name] || {};
    const tableEnvKey = fallbackEnvKeys.table || "";
    const viewEnvKey = fallbackEnvKeys.view || "";
    const envTableId = clean(airtable.runtime[tableEnvKey]);
    const envView = clean(airtable.runtime[viewEnvKey]);
    const schemaTable = findSchemaTable(schema, name, envTableId || name);
    if (!envTableId && !schemaTable) continue;
    tables[name] = {
      id: envTableId || schemaTable?.id || name,
      name,
      view: envView || DEFAULT_SOURCE_VIEWS[name] || ""
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
    const result = await fetchAirtableListPage(airtable, table, url);
    records.push(...(result.records || []).map((record) => ({
      id: record.id,
      createdTime: record.createdTime,
      fields: record.fields || {}
    })));
    offset = result.offset || "";
  } while (offset);
  return records;
}

async function fetchAirtableListPage(airtable, table, url) {
  const retryDelays = [450, 900, 1800, 3200];
  let lastError = null;
  for (let attempt = 0; attempt <= retryDelays.length; attempt += 1) {
    const response = await fetch(url, { headers: airtableHeaders(airtable.token) });
    const result = await response.json().catch(() => ({}));
    if (response.ok) return result;
    lastError = new Error(`list ${table} ${response.status}: ${JSON.stringify(result)}`);
    if (response.status !== 429 && response.status < 500) break;
    if (attempt >= retryDelays.length) break;
    const retryAfter = Number(response.headers.get("retry-after"));
    const delay = Number.isFinite(retryAfter) && retryAfter > 0
      ? retryAfter * 1000
      : retryDelays[attempt] + Math.floor(Math.random() * 250);
    await sleep(delay);
  }
  throw lastError;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function listOptionalRecords(airtable, tableConfig) {
  if (!tableConfig?.id) return [];
  try {
    return await listAirtableRecords(airtable, tableConfig.id, tableConfig.view);
  } catch (error) {
    console.warn(`[wec-packing] optional table skipped: ${tableConfig.name || tableConfig.id}`, error);
    return [];
  }
}

async function listOptionalViewRecords(airtable, tableId, view) {
  if (!tableId || !view) return [];
  try {
    return await listAirtableRecords(airtable, tableId, view);
  } catch (error) {
    console.warn(`[wec-packing] optional view skipped: ${tableId}/${view}`, error);
    return [];
  }
}

async function resolveSourceActionItem(airtable, context, payload) {
  const tables = context.tables;
  const sourceItemId = clean(payload?.sourcePackItemId || payload?.sourceItemId || payload?.itemId || payload?.packingItemId);
  if (!sourceItemId) throw new Error("missing_source_item_id");
  let sourceRecord;
  let legacyPackingRecord = null;
  try {
    sourceRecord = (await findRecordInConfiguredView(airtable, tables.wec_pack_items, sourceItemId)).record;
  } catch (sourceError) {
    legacyPackingRecord = await findOptionalRecordInConfiguredView(airtable, tables.wec_packing_items, sourceItemId);
    const legacySourceId = firstLinkedId(legacyPackingRecord?.fields?.source_pack_item);
    if (!legacySourceId) throw sourceError;
    sourceRecord = (await findRecordInConfiguredView(airtable, tables.wec_pack_items, legacySourceId)).record;
  }
  const sourceItem = normalizeSourcePackItem(sourceRecord);
  const packWaveId = clean(payload?.packWaveId);
  const showId = clean(payload?.showId);
  const ledgerState = await currentSourcePackingState(airtable, tables, sourceRecord.id, {
    packWaveId,
    showId
  });
  const currentPacked = ledgerState.hasEntries
    ? ledgerState.packed
    : wholeQuantityField(payload?.currentPacked ?? legacyPackingRecord?.fields?.quantity_packed);
  const currentPackState = ledgerState.packState || stringField(legacyPackingRecord?.fields?.pack_state || "not_packed");
  const currentResolutionState = ledgerState.resolutionState || stringField(legacyPackingRecord?.fields?.resolution_state);
  const itemName = clean(payload?.itemName)
    || sourceItemDisplayName(sourceItem)
    || stringField(legacyPackingRecord?.fields?.item_name)
    || sourceRecord.id;
  const payloadPackListIds = Array.isArray(payload?.packListIds) ? payload.packListIds.map(clean).filter(Boolean) : [];
  const packListIds = payloadPackListIds.length
    ? payloadPackListIds
    : linkedIds(legacyPackingRecord?.fields?.pack_list).length
    ? linkedIds(legacyPackingRecord.fields.pack_list)
    : sourceItem.packListIds;
  const fields = {
    item_name: itemName,
    source_pack_item: [sourceRecord.id],
    pack_wave: packWaveId ? [packWaveId] : linkedIds(legacyPackingRecord?.fields?.pack_wave),
    show: showId ? [showId] : linkedIds(legacyPackingRecord?.fields?.show),
    pack_list: packListIds,
    quantity_base: sourceQuantityBase(sourceItem),
    quantity_packed: currentPacked,
    quantity_needed: ledgerState.quantityNeededFrozen ?? legacyPackingRecord?.fields?.quantity_needed,
    quantity_needed_dynamic: payload?.effectiveNeeded,
    pack_state: currentPackState,
    resolution_state: currentResolutionState,
    unit: sourceItem.uom,
    record_state: "active"
  };
  return {
    sourceRecord,
    sourceItem,
    legacyPackingRecord,
    sourceItemId: sourceRecord.id,
    itemName,
    packWaveId,
    showId,
    packListIds,
    fields,
    currentPacked,
    currentPackState,
    currentResolutionState
  };
}

async function currentSourcePackingState(airtable, tables, sourceItemId, { packWaveId = "", showId = "" } = {}) {
  const records = await listAirtableRecords(airtable, tables.wec_packing_items.id, tables.wec_packing_items.view);
  const filtered = records.filter((record) => {
    const fields = record.fields || {};
    if (!isActiveWorksheetRow(record)) return false;
    if (!includesLinkedId(fields.source_pack_item, sourceItemId)) return false;
    if (packWaveId && linkedIds(fields.pack_wave).length && !includesLinkedId(fields.pack_wave, packWaveId)) return false;
    if (showId && linkedIds(fields.show).length && !includesLinkedId(fields.show, showId)) return false;
    return true;
  });
  const states = buildPackingLedgerState(filtered);
  return states.get(sourceItemId) || {
    hasEntries: false,
    packed: 0,
    packState: "not_packed",
    resolutionState: "",
    quantityNeededFrozen: null
  };
}

async function createPackingLedgerEntry(airtable, context, actionItem, update) {
  const tables = context.tables;
  const fieldNames = tableFieldNames(context, tables.wec_packing_items);
  const quantityDelta = numberField(update.quantityDelta);
  const fields = fieldsAllowedBySchema(compactFields({
    item_name: clean(update.itemName) || actionItem.itemName,
    show: actionItem.showId ? [actionItem.showId] : linkedIds(actionItem.fields.show),
    pack_wave: actionItem.packWaveId ? [actionItem.packWaveId] : linkedIds(actionItem.fields.pack_wave),
    pack_list: actionItem.packListIds,
    source_pack_item: [actionItem.sourceItemId],
    quantity_base: sourceQuantityBase(actionItem.sourceItem),
    quantity_packed: quantityDelta,
    quantity_needed: update.quantityNeeded,
    pack_state: clean(update.packStateAfter || "not_packed"),
    resolution_state: clean(update.resolutionStateAfter),
    unit: actionItem.sourceItem.uom,
    record_state: "active",
    notes: clean(update.notes)
  }), fieldNames);
  const record = await createAirtableRecord(airtable, tables.wec_packing_items.id, fields);
  return {
    ...record,
    fields: {
      ...record.fields,
      show: fields.show || [],
      pack_wave: fields.pack_wave || [],
      pack_list: fields.pack_list || [],
      source_pack_item: fields.source_pack_item || [],
      item_name: fields.item_name || actionItem.itemName,
      quantity_packed: numberField(update.quantityAfter),
      pack_state: update.packStateAfter,
      resolution_state: update.resolutionStateAfter
    }
  };
}

async function applyAddQuantity(airtable, context, payload) {
  const tables = context.tables;
  const itemId = clean(payload?.itemId || payload?.packingItemId);
  const delta = wholeQuantityField(payload?.quantityDelta || payload?.delta || 0);
  if (!itemId) throw new Error("missing_item_id");
  if (!Number.isFinite(delta) || delta <= 0) throw new Error("quantity_delta_must_be_positive");

  const actionItem = await resolveSourceActionItem(airtable, context, payload);
  const fields = actionItem.fields;
  const before = actionItem.currentPacked;
  const needed = actionNeeded(fields, payload);
  const after = Math.min(needed || before + delta, before + delta);
  const nextPackState = needed > 0 && after >= needed ? "packed" : "not_packed";
  const quantityDelta = after - before;

  const updated = await createPackingLedgerEntry(airtable, context, actionItem, {
    quantityDelta,
    quantityAfter: after,
    packStateAfter: nextPackState,
    resolutionStateAfter: actionItem.currentResolutionState,
    notes: clean(payload?.notes)
  });
  const event = await createPackingEvent(airtable, tables, {
    eventType: "quantity_add",
    itemRecord: updated,
    quantityDelta,
    quantityBefore: before,
    quantityAfter: after,
    packStateBefore: actionItem.currentPackState,
    packStateAfter: nextPackState,
    decisionBefore: actionItem.currentResolutionState,
    decisionAfter: actionItem.currentResolutionState,
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyPackState(airtable, context, payload) {
  const tables = context.tables;
  const itemId = clean(payload?.itemId || payload?.packingItemId);
  const nextPackState = clean(payload?.packState || payload?.state);
  if (!itemId) throw new Error("missing_item_id");
  if (!["packed", "not_packed"].includes(nextPackState)) throw new Error("invalid_pack_state");
  if (nextPackState === "packed" && !payload?.confirmed) throw new Error("confirmation_required");

  const actionItem = await resolveSourceActionItem(airtable, context, payload);
  const fields = actionItem.fields;
  const beforeQuantity = actionItem.currentPacked;
  const needed = actionNeeded(fields, payload);
  const afterQuantity = nextPackState === "packed" ? needed : 0;
  const quantityDelta = afterQuantity - beforeQuantity;

  const updated = await createPackingLedgerEntry(airtable, context, actionItem, {
    quantityDelta,
    quantityAfter: afterQuantity,
    packStateAfter: nextPackState,
    resolutionStateAfter: actionItem.currentResolutionState,
    notes: clean(payload?.notes)
  });
  const event = await createPackingEvent(airtable, tables, {
    eventType: nextPackState === "packed" ? "mark_packed" : "mark_not_packed",
    itemRecord: updated,
    quantityDelta,
    quantityBefore: beforeQuantity,
    quantityAfter: afterQuantity,
    packStateBefore: actionItem.currentPackState,
    packStateAfter: nextPackState,
    decisionBefore: actionItem.currentResolutionState,
    decisionAfter: actionItem.currentResolutionState,
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyResolutionState(airtable, context, payload) {
  const tables = context.tables;
  const itemId = clean(payload?.itemId || payload?.packingItemId);
  const nextResolution = clean(payload?.resolutionState || payload?.resolution || payload?.decision);
  const allowed = ["max", "kill", "note", "purchase_onsite", "unresolved", "clear"];
  if (!itemId) throw new Error("missing_item_id");
  if (!allowed.includes(nextResolution)) throw new Error("invalid_resolution_state");
  if (!payload?.confirmed) throw new Error("confirmation_required");

  const actionItem = await resolveSourceActionItem(airtable, context, payload);
  const fields = actionItem.fields;
  const beforeDecision = actionItem.currentResolutionState;
  const packed = actionItem.currentPacked;
  const needed = actionNeeded(fields, payload);
  let afterPacked = packed;
  let afterPackState = actionItem.currentPackState;
  let afterResolution = nextResolution;
  if (nextResolution === "clear") {
    afterPacked = 0;
    afterPackState = "not_packed";
    afterResolution = "";
  } else if (nextResolution === "max") {
    afterPacked = needed;
    afterPackState = needed > 0 ? "packed" : "not_packed";
  } else if (nextResolution === "purchase_onsite") {
    afterPacked = 0;
    afterPackState = "not_packed";
  } else {
    afterPackState = packed >= needed && needed > 0 ? "packed" : "not_packed";
  }
  const quantityDelta = afterPacked - packed;

  const updated = await createPackingLedgerEntry(airtable, context, actionItem, {
    quantityDelta,
    quantityAfter: afterPacked,
    packStateAfter: afterPackState,
    resolutionStateAfter: afterResolution,
    notes: clean(payload?.notes)
  });
  const event = await createPackingEvent(airtable, tables, {
    eventType: nextResolution === "clear" ? "decision_clear" : `decision_${nextResolution}`,
    itemRecord: updated,
    quantityDelta,
    quantityBefore: packed,
    quantityAfter: afterPacked,
    packStateBefore: actionItem.currentPackState,
    packStateAfter: afterPackState,
    decisionBefore: beforeDecision,
    decisionAfter: afterResolution,
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyItemFieldUpdate(airtable, context, payload) {
  const tables = context.tables;
  const itemId = clean(payload?.itemId || payload?.packingItemId);
  if (!itemId) throw new Error("missing_item_id");

  const incoming = payload?.fields || {};
  const updateFields = {};
  if (Object.prototype.hasOwnProperty.call(incoming, "item_name")) {
    const name = clean(incoming.item_name);
    if (!name) throw new Error("item_name_required");
    updateFields.item_name = name;
  }
  if (Object.prototype.hasOwnProperty.call(incoming, "quantity_packed")) {
    const packed = wholeQuantityField(incoming.quantity_packed);
    if (!Number.isFinite(packed) || packed < 0) throw new Error("quantity_packed_invalid");
    updateFields.quantity_packed = packed;
  }
  if (Object.prototype.hasOwnProperty.call(incoming, "quantity_needed")) {
    const needed = wholeQuantityField(incoming.quantity_needed);
    if (!Number.isFinite(needed) || needed < 0) throw new Error("quantity_needed_invalid");
    updateFields.quantity_needed = needed;
  }
  if (!Object.keys(updateFields).length) throw new Error("no_allowed_fields");

  const actionItem = await resolveSourceActionItem(airtable, context, payload);
  if (Object.prototype.hasOwnProperty.call(updateFields, "quantity_needed")) {
    await applySourceQuantityUpdateForQuantityPlan(airtable, context, actionItem, updateFields.quantity_needed);
  }
  const fields = { ...actionItem.fields, ...updateFields };
  let afterPacked = actionItem.currentPacked;
  let quantityDelta = 0;
  if (Object.prototype.hasOwnProperty.call(updateFields, "quantity_packed") || Object.prototype.hasOwnProperty.call(updateFields, "quantity_needed")) {
    const packed = wholeQuantityField(fields.quantity_packed);
    const needed = Object.prototype.hasOwnProperty.call(updateFields, "quantity_needed")
      ? worksheetNeeded(fields)
      : actionNeeded(fields, payload);
    afterPacked = packed;
    quantityDelta = afterPacked - actionItem.currentPacked;
    updateFields.pack_state = needed > 0 && packed >= needed ? "packed" : "not_packed";
  }

  const updated = await createPackingLedgerEntry(airtable, context, actionItem, {
    itemName: updateFields.item_name,
    quantityDelta,
    quantityAfter: afterPacked,
    quantityNeeded: Object.prototype.hasOwnProperty.call(updateFields, "quantity_needed") ? updateFields.quantity_needed : undefined,
    packStateAfter: updateFields.pack_state || actionItem.currentPackState,
    resolutionStateAfter: actionItem.currentResolutionState,
    notes: "inline edit"
  });
  const event = await createPackingEvent(airtable, tables, {
    eventType: "item_field_update",
    itemRecord: updated,
    quantityDelta,
    quantityBefore: actionItem.currentPacked,
    quantityAfter: afterPacked,
    packStateBefore: actionItem.currentPackState,
    packStateAfter: updateFields.pack_state || actionItem.currentPackState,
    decisionBefore: actionItem.currentResolutionState,
    decisionAfter: actionItem.currentResolutionState,
    notes: Object.keys(updateFields).join(", ")
  });
  return { updated, event };
}

async function applySourceQuantityUpdateForQuantityPlan(airtable, context, actionItem, needed) {
  const plan = slugify(actionItem?.sourceItem?.listPlan || actionItem?.fields?.list_plan || "");
  if (plan !== "quantity") throw new Error("quantity_needed_not_editable_for_dynamic_plan");

  const tables = context.tables;
  const fieldNames = tableFieldNames(context, tables.wec_pack_items);
  const fields = fieldsAllowedBySchema({ quantity: needed }, fieldNames);
  if (!Object.keys(fields).length) throw new Error("wec_pack_items_quantity_field_missing");

  const updated = await patchAirtableRecord(airtable, tables.wec_pack_items.id, actionItem.sourceItemId, fields);
  actionItem.sourceItem.quantity = needed;
  actionItem.fields.quantity_base = needed;
  actionItem.fields.quantity_needed_dynamic = needed;
  return updated;
}

async function applyHorsePackState(airtable, tables, payload) {
  const memberId = clean(payload?.itemHorseId || payload?.packingItemHorseId);
  const nextState = clean(payload?.horsePackState || payload?.state);
  if (!memberId) throw new Error("missing_item_horse_id");
  if (!["packed", "not_packed", "not_needed"].includes(nextState)) throw new Error("invalid_horse_pack_state");

  const { record: memberRecord, records: allMembers } = await findRecordInConfiguredView(airtable, tables.wec_packing_item_horses, memberId);
  const member = normalizeHorseMember(memberRecord);
  const packingItemId = member.packingItemIds[0];
  if (!packingItemId) throw new Error("missing_parent_packing_item");
  const { record: itemRecord } = await findRecordInConfiguredView(airtable, tables.wec_packing_items, packingItemId);

  const beforeState = stringField(memberRecord.fields?.horse_pack_state || "not_packed");
  const before = beforeState === "not_needed" ? 0 : wholeQuantityField(memberRecord.fields?.quantity_packed);
  const memberNeeded = wholeQuantityField(memberRecord.fields?.quantity_needed || 1);
  const after = nextState === "packed" ? memberNeeded : 0;
  const updatedMember = await patchAirtableRecord(airtable, tables.wec_packing_item_horses.id, memberId, {
    quantity_packed: after,
    horse_pack_state: nextState
  });

  const rolledMembers = allMembers
    .filter((record) => includesLinkedId(record.fields?.packing_item, packingItemId))
    .map((record) => record.id === memberId
      ? { ...record, fields: { ...record.fields, quantity_packed: after, horse_pack_state: nextState } }
      : record);
  const packedTotal = rolledMembers.reduce((sum, record) => sum + wholeQuantityField(record.fields?.quantity_packed), 0);
  const neededTotal = rolledMembers.reduce((sum, record) => sum + effectiveHorseMemberNeeded(record.fields), 0);
  const parentPackState = neededTotal > 0 && packedTotal >= neededTotal ? "packed" : "not_packed";
  const updatedParent = await patchAirtableRecord(airtable, tables.wec_packing_items.id, packingItemId, {
    quantity_packed: packedTotal,
    pack_state: parentPackState
  });

  const event = await createPackingEvent(airtable, tables, {
    eventType: horseMemberEventType(nextState, "mark"),
    itemRecord,
    memberRecord,
    quantityDelta: after - before,
    quantityBefore: before,
    quantityAfter: after,
    packStateBefore: beforeState,
    packStateAfter: nextState,
    decisionBefore: "",
    decisionAfter: "",
    notes: clean(payload?.notes)
  });
  return { updatedMember, updatedParent, event };
}

async function applyHorseKitState(airtable, context, payload) {
  const tables = context.tables;
  const packingItemId = clean(payload?.packingItemId || payload?.itemId);
  const horseId = clean(payload?.horseId);
  const sourcePackItemId = clean(payload?.sourcePackItemId);
  const packWaveId = clean(payload?.packWaveId);
  const showId = clean(payload?.showId);
  const nextState = clean(payload?.horsePackState || payload?.state);
  if (!packingItemId) throw new Error("missing_packing_item_id");
  if (!horseId) throw new Error("missing_horse_id");
  if (!sourcePackItemId) throw new Error("missing_source_pack_item_id");
  if (!["packed", "not_packed", "not_needed"].includes(nextState)) throw new Error("invalid_horse_pack_state");

  const { record: horseRecord } = await findRecordInConfiguredView(airtable, tables.wec_horses, horseId);
  const sourceRecord = tables.wec_pack_items?.id
    ? (await findRecordInConfiguredView(airtable, tables.wec_pack_items, sourcePackItemId)).record
    : null;
  const events = await listAirtableRecords(airtable, tables.wec_packing_events.id, tables.wec_packing_events.view);
  const previous = horseKitStatesFromEvents(events, {
    packWaveId,
    showId
  }).get(`${sourcePackItemId}:${horseId}`);
  const beforeState = previous?.horsePackState || "not_packed";
  const before = beforeState === "packed" ? 1 : 0;
  const after = nextState === "packed" ? 1 : 0;
  const event = await createPackingEvent(airtable, tables, {
    eventType: horseMemberEventType(nextState, "horse_kit"),
    eventSubjectId: `${sourcePackItemId}:${horseId}`,
    showIds: showId ? [showId] : [],
    packWaveIds: packWaveId ? [packWaveId] : [],
    horseIds: [horseId],
    quantityDelta: after - before,
    quantityBefore: before,
    quantityAfter: after,
    packStateBefore: beforeState,
    packStateAfter: nextState,
    decisionBefore: "",
    decisionAfter: "",
    notes: [
      `source_pack_item: ${sourcePackItemId}`,
      `horse_id: ${horseId}`,
      `packing_item: ${packingItemId}`,
      `source_item: ${stringField(sourceRecord?.fields?.app_name)}`,
      `horse: ${stringField(horseRecord.fields?.barn_name || horseRecord.fields?.horse || horseRecord.fields?.show_name)}`,
      clean(payload?.notes)
    ].filter(Boolean).join("\n")
  });
  return { event };
}

async function applyHorseRecordState(airtable, tables, payload) {
  const horseId = clean(payload?.horseId);
  const nextState = clean(payload?.recordState || payload?.state);
  if (!horseId) throw new Error("missing_horse_id");
  if (!["active", "inactive"].includes(nextState)) throw new Error("invalid_horse_record_state");
  const { record } = await findRecordInConfiguredView(airtable, tables.wec_horses, horseId);
  const updated = await patchAirtableRecord(airtable, tables.wec_horses.id, horseId, {
    record_state: nextState
  });
  return {
    updated,
    previousState: stringField(record.fields?.record_state || "inactive")
  };
}

async function applySourceFlag(airtable, tables, payload) {
  const sourceItemId = clean(payload?.sourceItemId);
  const flagName = clean(payload?.flagName);
  const nextValue = !!payload?.value;
  const allowed = {
    ignore: "ignore",
    rename: "rename",
    change_lane: "change_lane"
  };
  if (!sourceItemId) throw new Error("missing_source_item_id");
  if (!allowed[flagName]) throw new Error("invalid_source_flag");
  await findRecordInConfiguredView(airtable, tables.wec_pack_items, sourceItemId);
  const updated = await patchAirtableRecord(airtable, tables.wec_pack_items.id, sourceItemId, {
    [allowed[flagName]]: nextValue
  });
  return { updated };
}

async function applyOnsiteTaskState(airtable, tables, payload) {
  const sourceItemId = clean(payload?.sourceItemId);
  const taskState = clean(payload?.taskState || payload?.state);
  if (!sourceItemId) throw new Error("missing_source_item_id");
  if (!["task", "done"].includes(taskState)) throw new Error("invalid_onsite_task_state");

  const sourceRecords = await listAirtableRecords(airtable, tables.wec_pack_items.id, "wec_purchase_onsite");
  const sourceRecord = sourceRecords.find((record) => record.id === sourceItemId);
  if (!sourceRecord) throw new Error(`wec_purchase_onsite_record_not_in_view: ${sourceItemId}`);

  const eventType = taskState === "done" ? "onsite_task_done" : "onsite_task_reopen";
  const event = await createPackingEvent(airtable, tables, {
    eventType,
    eventSubjectId: sourceItemId,
    showIds: clean(payload?.showId) ? [clean(payload.showId)] : [],
    packWaveIds: clean(payload?.packWaveId) ? [clean(payload.packWaveId)] : [],
    quantityDelta: 0,
    quantityBefore: 0,
    quantityAfter: 0,
    notes: [
      `source_pack_item: ${sourceItemId}`,
      `item: ${stringField(sourceRecord.fields?.app_name)}`,
      `task_state: ${taskState}`
    ].join("\n")
  });
  return { event, taskState };
}

async function findRecordInConfiguredView(airtable, tableConfig, recordId) {
  if (!tableConfig?.id) throw new Error("missing_table_config");
  const records = await listAirtableRecords(airtable, tableConfig.id, tableConfig.view);
  const record = records.find((item) => item.id === recordId);
  if (!record) throw new Error(`${tableConfig.name || tableConfig.id}_record_not_in_configured_view: ${recordId}`);
  return { record, records };
}

async function findOptionalRecordInConfiguredView(airtable, tableConfig, recordId) {
  if (!recordId || !tableConfig?.id) return null;
  try {
    return (await findRecordInConfiguredView(airtable, tableConfig, recordId)).record;
  } catch {
    return null;
  }
}

async function patchAirtableRecord(airtable, table, recordId, fields) {
  const response = await fetch(`${airtableUrl(airtable.baseId, table)}/${encodeURIComponent(recordId)}`, {
    method: "PATCH",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ fields, typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`patch ${table}/${recordId} ${response.status}: ${JSON.stringify(result)}`);
  }
  return {
    id: result.id || recordId,
    fields: result.fields || fields
  };
}

async function createAirtableRecord(airtable, table, fields) {
  const response = await fetch(airtableUrl(airtable.baseId, table), {
    method: "POST",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`create ${table} ${response.status}: ${JSON.stringify(result)}`);
  }
  return {
    id: result.records?.[0]?.id || "",
    fields: result.records?.[0]?.fields || fields
  };
}

async function createPackingEvent(airtable, tables, payload) {
  const itemFields = payload.itemRecord?.fields || {};
  const memberFields = payload.memberRecord?.fields || {};
  const subjectId = payload.eventSubjectId || payload.itemRecord?.id || "";
  const eventFields = compactFields({
    event: `${payload.eventType}:${subjectId}:${Date.now()}`,
    show: payload.showIds || linkedIds(itemFields.show),
    pack_wave: payload.packWaveIds || (linkedIds(itemFields.pack_wave).length ? linkedIds(itemFields.pack_wave) : linkedIds(memberFields.pack_wave)),
    packing_item: payload.itemRecord?.id ? [payload.itemRecord.id] : [],
    packing_item_horse: payload.memberRecord?.id ? [payload.memberRecord.id] : [],
    horse: payload.horseIds || linkedIds(memberFields.horse),
    event_type: payload.eventType,
    quantity_delta: payload.quantityDelta,
    quantity_before: payload.quantityBefore,
    quantity_after: payload.quantityAfter,
    pack_state_before: payload.packStateBefore,
    pack_state_after: payload.packStateAfter,
    decision_before: payload.decisionBefore,
    decision_after: payload.decisionAfter,
    notes: payload.notes,
    created_at: new Date().toISOString().slice(0, 10),
    created_by: "webflow"
  });
  const response = await fetch(airtableUrl(airtable.baseId, tables.wec_packing_events.id), {
    method: "POST",
    headers: {
      ...airtableHeaders(airtable.token),
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ records: [{ fields: eventFields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`event ${response.status}: ${JSON.stringify(result)}`);
  }
  return {
    id: result.records?.[0]?.id || "",
    fields: result.records?.[0]?.fields || eventFields
  };
}

async function createHorseKitChange(airtable, tables, payload) {
  if (!tables.horse_kit_changes?.id) return null;
  const timestamp = Date.now();
  const subject = [
    payload.changeType,
    payload.packingKitRecord?.id || payload.itemIds?.[0] || payload.kitIds?.[0] || "change",
    timestamp
  ].filter(Boolean).join(":");
  const fields = compactFields({
    change: subject,
    change_type: payload.changeType,
    horse_kits: payload.kitIds || linkedIds(payload.packingKitRecord?.fields?.horse_kits),
    horse_kit_item: payload.itemIds || linkedIds(payload.packingKitRecord?.fields?.horse_kit_item),
    pak_kits: payload.pakKitIds || linkedIds(payload.packingKitRecord?.fields?.pak_kits),
    pak_kit_items: payload.pakItemIds || linkedIds(payload.packingKitRecord?.fields?.pak_kit_items),
    horse_packing_kit: payload.packingKitRecord?.id ? [payload.packingKitRecord.id] : payload.packingKitIds,
    horse_kit_exception: payload.exceptionIds,
    old_value: stringifyChangeValue(payload.oldValue),
    new_value: stringifyChangeValue(payload.newValue),
    created_by: "webflow",
    notes: clean(payload.notes)
  });
  return createAirtableRecord(airtable, tables.horse_kit_changes.id, fields);
}

function stringifyChangeValue(value) {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

function eventPackStateValue(value) {
  const state = slugify(value);
  if (state === "packed") return "packed";
  if (state === "not_packed") return "not_packed";
  if (state === "not_needed") return "not_needed";
  return undefined;
}

function horsePackingKitEventNotes(record, payload) {
  const fields = record?.fields || {};
  return [
    `horse_packing_kit: ${record?.id || ""}`,
    `horse: ${linkedIds(fields.horse).join(",")}`,
    `kit: ${[...linkedIds(fields.horse_kits), ...linkedIds(fields.pak_kits)].join(",")}`,
    `kit_item: ${[...linkedIds(fields.horse_kit_item), ...linkedIds(fields.pak_kit_items)].join(",")}`,
    clean(payload?.notes)
  ].filter(Boolean).join("\n");
}

function worksheetNeeded(fields) {
  return wholeQuantityField(fields?.quantity_needed ?? fields?.quantity_needed_dynamic ?? fields?.quantity_base);
}

function actionNeeded(fields, payload) {
  const effectiveNeeded = wholeQuantityField(payload?.effectiveNeeded ?? payload?.needed);
  return effectiveNeeded > 0 ? effectiveNeeded : worksheetNeeded(fields);
}

function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => {
    if (value === null) return true;
    if (Array.isArray(value)) return value.length > 0;
    return value !== undefined && value !== "";
  }));
}

function normalizeWave(record) {
  if (!record) return null;
  const fields = record.fields || {};
  const wave = stringField(fields.wave || fields.wave_key || fields.key || fields.Name || record.id);
  const countHorsesWaveOneValue = fields.count_horses_wave_one;
  return {
    id: record.id,
    key: slugify(fields.wave_key || fields.key || wave),
    wave,
    waveType: stringField(fields.wave_type),
    active: !!fields.active,
    manualLock: !!fields.manual_lock,
    deadlineDate: stringField(fields.deadline_date),
    daysTill: numberField(fields.days_till),
    horseCount: numberField(fields.horse_count),
    countHorsesWaveOne: numberField(countHorsesWaveOneValue),
    countHorsesWaveOneAvailable: hasNumberField(countHorsesWaveOneValue),
    groomCountFinal: numberField(fields.groom_sanity),
    groomSanity: numberField(fields.groom_sanity),
    sortOrder: numberField(fields.sort_order),
    showIds: linkedIds(fields.show),
    includedWeekIds: linkedIds(fields.included_weeks)
  };
}

function withEffectiveWaveCounts(wave, waveHorses) {
  if (!wave) return null;
  const linkedHorseCount = waveHorses.length;
  const waveKey = slugify(wave.key || wave.wave || "");
  const usesWaveOneCount = (waveKey === "wave_one" || waveKey === "wave_1" || waveKey === "one") && wave.countHorsesWaveOneAvailable;
  const currentHorseCount = usesWaveOneCount
    ? wave.countHorsesWaveOne
    : linkedHorseCount;
  const dynamicGroomCountFinal = wave.groomSanity;
  const effectiveHorseCount = wave.manualLock ? wave.horseCount : currentHorseCount;
  const effectiveGroomCountFinal = dynamicGroomCountFinal;
  return {
    ...wave,
    linkedHorseCount,
    currentHorseCount,
    currentGroomCountFinal: dynamicGroomCountFinal,
    effectiveHorseCount,
    effectiveGroomCountFinal,
    countsLocked: wave.manualLock,
    countSource: wave.manualLock ? "manual_lock" : (usesWaveOneCount ? "count_horses_wave_one" : "current_wave_scope"),
    groomCountSource: "groom_sanity"
  };
}

function normalizePackList(record) {
  const fields = record.fields || {};
  const label = stringField(fields.list) || record.id;
  const tabs = stringListField(fields.tabs);
  return {
    id: record.id,
    key: slugify(label),
    lane: slugify(fields.lane),
    label,
    listLabel: stringField(fields.list_label),
    displayLabel: stringField(fields.display_label),
    tabs,
    tabKey: slugify(tabs[0] || label),
    tabLabel: tabs[0] || label,
    shortDescription: stringField(fields.short_description),
    longDescription: stringField(fields.long_description),
    itemCount: numberField(fields.count_wec_pack_items ?? fields.list_items_count),
    sourceTable: stringField(fields.source_table),
    sourceView: stringField(fields.source_view),
    localLists: !!fields.local_lists,
    allowed: !!fields.wec_allowed,
    includeOnHome: !!fields.include_on_home,
    display: fields.display !== false
  };
}

function isPackingListLane(list) {
  return !list.lane || list.lane === "pack_lists";
}

function normalizeListPlan(record) {
  const fields = record.fields || {};
  const plan = slugify(stringField(fields.plan) || record.id);
  return {
    id: record.id,
    plan,
    label: stringField(fields.plan) || plan,
    logic: stringField(fields.logic)
  };
}

function resolveListPlan(fields, listPlanLookup) {
  const planId = linkedIds(fields.wec_list_plans)[0] || "";
  const linkedPlan = planId ? listPlanLookup.get(planId) : null;
  const fallbackPlan = slugify(stringField(fields.list_plan));
  return {
    id: planId,
    plan: linkedPlan?.plan || fallbackPlan,
    label: linkedPlan?.label || stringField(fields.list_plan),
    logic: linkedPlan?.logic || ""
  };
}

function normalizePlaceTag(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    label: stringField(fields.tag || fields.name)
  };
}

function normalizePlace(record, placeTagLookup = new Map()) {
  const fields = record.fields || {};
  const localTags = uniqueStrings([
    ...stringListField(fields.wec_local_tags_rollups || fields.local_tags || fields.tags),
    ...linkedIds(fields.wec_local_tags).map((id) => placeTagLookup.get(id)?.label || "")
  ]);
  return {
    id: record.id,
    label: stringField(fields.place || fields.name),
    itemIds: linkedIds(fields.tack_grocery_items || fields.wec_pack_items),
    localTags,
    placeType: stringField(fields.wec_place_type),
    mapsUrl: stringField(fields.maps_url),
    phone: stringField(fields.phone),
    website: stringField(fields.website),
    attributes: placeAttributeRows(fields)
  };
}

const PLACE_ATTRIBUTE_EXCLUDED_FIELDS = new Set([
  "place",
  "name",
  "wec_local_tags_rollups",
  "local_tags",
  "tags",
  "wec_local_tags",
  "tack_grocery_items",
  "wec_pack_items",
  "wec_place_type",
  "maps_url",
  "phone",
  "website",
  "location",
  "count_items",
  "crt_rec_id",
  "place_id",
  "table_api",
  "table_name",
  "created",
  "created_at",
  "created_by",
  "record_id"
]);

function placeAttributeRows(fields) {
  return Object.entries(fields || {})
    .map(([field, value]) => {
      const key = field.toLowerCase();
      if (PLACE_ATTRIBUTE_EXCLUDED_FIELDS.has(key)) return null;
      const values = displayableAttributeValues(value).filter((item) => !isRecordIdValue(item));
      if (!values.length) return null;
      return {
        label: placeAttributeLabel(field),
        value: values.join(", ")
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base" }));
}

function placeAttributeLabel(field) {
  if (String(field || "").toLowerCase() === "acronym_attributes") return "Attributes";
  return displayLabel(field);
}

function displayableAttributeValues(value) {
  if (Array.isArray(value)) return value.flatMap(displayableAttributeValues);
  if (value && typeof value === "object") return [];
  if (typeof value === "boolean") return [value ? "Yes" : "No"];
  return stringListField(value);
}

function isRecordIdValue(value) {
  return /^rec[a-z0-9]{10,}$/i.test(String(value || "").trim());
}

function buildPlaceLookup(records, placeTagLookup = new Map()) {
  const lookup = new Map();
  for (const record of records || []) {
    const place = normalizePlace(record, placeTagLookup);
    lookup.set(place.id, place);
    for (const itemId of place.itemIds || []) {
      const existing = lookup.get(`item:${itemId}`) || [];
      existing.push(place);
      lookup.set(`item:${itemId}`, existing);
    }
  }
  return lookup;
}

function decorateSourcePackItem(item, placeLookup = new Map()) {
  const linkedPlaceIds = item.placeIds.length ? item.placeIds : item.vendorIds;
  const placesFromLinks = linkedPlaceIds.map((id) => placeLookup.get(id)).filter(Boolean);
  const placesFromReverseLinks = placeLookup.get(`item:${item.id}`) || [];
  const places = uniqueById([...placesFromLinks, ...placesFromReverseLinks]);
  return {
    ...item,
    placeIds: uniqueStrings([...item.placeIds, ...places.map((place) => place.id)]),
    placeLabels: uniqueStrings([
      ...item.placeLabels,
      ...places.map((place) => place.label)
    ]),
    localTags: uniqueStrings([
      ...item.localTags,
      ...places.flatMap((place) => place.localTags || [])
    ]),
    places
  };
}

function buildPackingLedgerState(records) {
  const states = new Map();
  for (const record of records || []) {
    const fields = record.fields || {};
    const sourceItemId = firstLinkedId(fields.source_pack_item);
    if (!sourceItemId) continue;
    const state = states.get(sourceItemId) || {
      hasEntries: false,
      packed: 0,
      name: "",
      packState: "not_packed",
      resolutionState: "",
      quantityNeededFrozen: null,
      notes: "",
      sortOrder: 0,
      updatedAt: ""
    };
    state.hasEntries = true;
    state.packed += numberField(fields.quantity_packed);
    if (hasNumberField(fields.quantity_needed)) state.quantityNeededFrozen = wholeQuantityField(fields.quantity_needed);
    const updatedAt = stringField(record.createdTime || fields.Created || fields.created_at || "");
    const isLatest = !state.updatedAt || updatedAt >= state.updatedAt;
    if (isLatest) {
      state.updatedAt = updatedAt;
      state.name = stringField(fields.item_name) || state.name;
      state.packState = stringField(fields.pack_state || state.packState);
      state.resolutionState = stringField(fields.resolution_state);
      state.notes = stringField(fields.notes);
      state.sortOrder = numberField(fields.sort_order);
    }
    states.set(sourceItemId, state);
  }
  for (const state of states.values()) {
    state.packed = wholeQuantityField(Math.max(0, state.packed));
  }
  return states;
}

function buildSourceWorksheetRecords({ sourceItems, ledgerState, selectedWave, selectedShowId, packListIds }) {
  if (!selectedWave) return [];
  return (sourceItems || [])
    .filter((sourceItem) => isActiveSourceWorksheetItem(sourceItem, packListIds))
    .map((sourceItem) => sourceItemToWorksheetRecord(sourceItem, ledgerState.get(sourceItem.id), selectedWave, selectedShowId));
}

function isActiveSourceWorksheetItem(sourceItem, packListIds = new Set()) {
  if (!sourceItem?.active || sourceItem.ignored) return false;
  if (!sourceItem.packListIds?.length) return false;
  if (!packListIds?.size) return true;
  return sourceItem.packListIds.some((id) => packListIds.has(id));
}

function sourceItemToWorksheetRecord(sourceItem, state, selectedWave, selectedShowId) {
  const name = state?.name || sourceItemDisplayName(sourceItem);
  const quantityBase = sourceQuantityBase(sourceItem);
  const packListIds = isTaskOnlySourceItem(sourceItem) ? [] : sourceItem.packListIds;
  return {
    id: sourceItem.id,
    createdTime: state?.updatedAt || "",
    fields: compactFields({
      item_name: name,
      item_id: sourceItem.appName || sourceItem.id,
      show: selectedShowId ? [selectedShowId] : linkedIds(selectedWave?.fields?.show),
      pack_wave: selectedWave?.id ? [selectedWave.id] : [],
      pack_list: packListIds,
      source_pack_item: [sourceItem.id],
      quantity_base: quantityBase,
      quantity_packed: state?.packed || 0,
      quantity_needed: state?.quantityNeededFrozen ?? undefined,
      pack_state: state?.packState || "not_packed",
      resolution_state: state?.resolutionState || "",
      unit: sourceItem.uom,
      record_state: "active",
      sort_order: sourceItem.sortOrder,
      notes: state?.notes || sourceItem.note
    })
  };
}

function isTaskOnlySourceItem(sourceItem) {
  const plan = slugify(sourceItem?.listPlan);
  return plan === "purchase_onsite" ||
    plan === "unresolved" ||
    plan === "needs_attention" ||
    !!sourceItem?.sourceFlags?.purchaseOnsite ||
    !!sourceItem?.sourceFlags?.unresolved ||
    !!sourceItem?.sourceFlags?.needsAttention;
}

function sourceItemDisplayName(sourceItem) {
  if (!sourceItem) return "";
  if (sourceItem.horseSpecific && sourceItem.displayNamePerHorse) return sourceItem.displayNamePerHorse;
  return sourceItem.displayName || sourceItem.appName || sourceItem.id;
}

function sourceQuantityBase(sourceItem) {
  if (!sourceItem) return 0;
  if (sourceItem.perGroom) return sourceItem.perGroom;
  if (sourceItem.perHorse) return sourceItem.perHorse;
  if (sourceItem.quantity) return sourceItem.quantity;
  return 1;
}

function normalizeSourcePackItem(record, listPlanLookup = new Map()) {
  const fields = record.fields || {};
  const listPlan = resolveListPlan(fields, listPlanLookup);
  return {
    id: record.id,
    appName: stringField(fields.app_name),
    displayName: stringField(fields.item_display_name || fields.display_name || fields.label || fields.app_name),
    displayNamePerHorse: stringField(fields.item_display_name_per_horse),
    listPlan: listPlan.plan,
    listPlanId: listPlan.id,
    listPlanLabel: listPlan.label,
    listPlanLogic: listPlan.logic,
    quantity: numberField(fields.quantity),
    perHorse: numberField(fields.per_horse),
    perGroom: numberField(fields.per_groom),
    horseSpecific: !!fields["horse-specific"],
    uom: stringField(fields.uom),
    packListIds: linkedIds(fields.wec_pack_lists),
    horseIds: linkedIds(fields.wec_horses),
    vendorIds: linkedIds(fields.wec_vendors),
    placeIds: linkedIds(fields.wec_places),
    placeLabels: stringListField(fields.wec_places_rollup || fields.place_names || fields["place_names (from wec_places)"]),
    localTags: stringListField(fields.wec_local_tags_rollups || fields.local_tags || fields["local_tags (from wec_places)"]),
    ignored: !!fields.ignore,
    active: (fields.active === undefined ? true : !!fields.active) && !fields.inactive && !fields.remove,
    sortOrder: numberField(fields.sort_order || fields.sorted),
    sourceFlags: {
      ignore: !!fields.ignore,
      rename: !!fields.rename,
      changeLane: !!fields.change_lane,
      inactive: !!fields.inactive,
      remove: !!fields.remove,
      needsAttention: !!fields.needs_attention,
      unresolved: !!fields.unresolved,
      purchaseOnsite: !!fields.purchase_onsite,
      maxxed: !!fields.maxxed
    },
    note: stringField(fields.note),
    longDescription: stringField(fields.long_description)
  };
}

function normalizePackingItem(record, horseRecords, listPlanLookup = new Map()) {
  const fields = record.fields || {};
  const listPlan = resolveListPlan(fields, listPlanLookup);
  const quantityNeededDynamicRaw = nullableNumberField(fields.quantity_needed_dynamic);
  const quantityNeededFrozenRaw = nullableNumberField(fields.quantity_needed);
  const quantityNeededDynamic = quantityNeededDynamicRaw === null ? null : wholeQuantityField(quantityNeededDynamicRaw);
  const quantityNeededFrozen = quantityNeededFrozenRaw === null ? null : wholeQuantityField(quantityNeededFrozenRaw);
  const needed = quantityNeededDynamic ?? quantityNeededFrozen ?? wholeQuantityField(fields.quantity_base);
  const packed = wholeQuantityField(fields.quantity_packed);
  const left = wholeQuantityField(fields.quantity_left ?? Math.max(0, needed - packed));
  return {
    id: record.id,
    name: stringField(fields.item_name),
    itemId: stringField(fields.item_id),
    location: stringField(fields.location),
    listPlan: listPlan.plan,
    listPlanId: listPlan.id,
    listPlanLabel: listPlan.label,
    listPlanLogic: listPlan.logic,
    quantityBase: wholeQuantityField(fields.quantity_base),
    quantityNeededDynamic,
    quantityNeededFrozen,
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

function decoratePackingItem(item, packListLookup, sourcePackItemLookup, wave, waveHorses = [], horseKitStateLookup = new Map()) {
  const sourceItems = item.sourcePackItemIds
    .map((id) => sourcePackItemLookup.get(id))
    .filter(Boolean);
  const sourceItem = sourceItems[0];
  const horseSpecific = isHorseSpecificPlan(item, sourceItem);
  const horseMembers = horseSpecific
    ? expectedHorseKitMembers(item, sourceItem, wave, waveHorses, horseKitStateLookup)
    : item.horseMembers;
  const itemForCalculation = { ...item, horseMembers };
  const quantityCalculation = buildQuantityCalculation(itemForCalculation, sourceItem, wave, waveHorses);
  const packed = horseSpecific
    ? horseMembers.filter(isPackedHorseMember).length
    : item.packed;
  const effectiveNeeded = horseSpecific
    ? horseMembers.reduce((sum, member) => sum + effectiveHorseMemberNeeded(member), 0)
    : wave?.countsLocked
      ? item.quantityNeededFrozen ?? item.needed
      : calculatedNeededForUnlockedItem(item, quantityCalculation);
  const effectiveItem = {
    ...item,
    horseMembers,
    packed: wholeQuantityField(packed),
    needed: wholeQuantityField(effectiveNeeded),
    left: wholeQuantityField(Math.max(0, effectiveNeeded - packed)),
    packState: effectiveNeeded > 0 && packed >= effectiveNeeded ? "packed" : "not_packed"
  };
  return {
    ...effectiveItem,
    packListLabels: effectiveItem.packListIds
      .map((id) => packListLookup.get(id)?.label || "")
      .filter(Boolean),
    sourceItems,
    quantityCalculation: {
      ...quantityCalculation,
      appliedNeeded: effectiveItem.needed,
      matchesApplied: Math.abs(numberField(quantityCalculation.calculatedNeeded) - numberField(effectiveItem.needed)) < 0.0001
    }
  };
}

function isHorseSpecificPlan(item, sourceItem) {
  const plan = slugify(item?.listPlan || sourceItem?.listPlan || "");
  return plan === "horse_specific" || plan === "horse-specific" || !!sourceItem?.horseSpecific;
}

function expectedHorseKitMembers(item, sourceItem, wave, waveHorses = [], horseKitStateLookup = new Map()) {
  const sourcePackItemId = sourceItem?.id || item.sourcePackItemIds[0] || "";
  const expectedHorses = expectedHorsesForSourceItem(sourceItem, waveHorses);
  const existingByHorseId = new Map();
  for (const member of item.horseMembers || []) {
    if (sourcePackItemId && member.sourcePackItemIds.length && !member.sourcePackItemIds.includes(sourcePackItemId)) continue;
    for (const horseId of member.horseIds || []) {
      if (!existingByHorseId.has(horseId)) existingByHorseId.set(horseId, member);
    }
  }
  return expectedHorses.map((horse) => {
    const existing = existingByHorseId.get(horse.id);
    const eventState = sourcePackItemId && horse.id
      ? horseKitStateLookup.get(`${sourcePackItemId}:${horse.id}`)
      : null;
    if (existing) {
      const horsePackState = eventState?.horsePackState || stringField(existing.horsePackState) || "not_packed";
      return {
        ...existing,
        needed: 1,
        packed: horsePackState === "packed" ? 1 : 0,
        horsePackState,
        barnName: existing.barnName || horse.name,
        packWaveIds: existing.packWaveIds.length ? existing.packWaveIds : (wave?.id ? [wave.id] : []),
        sourcePackItemIds: existing.sourcePackItemIds.length ? existing.sourcePackItemIds : (sourcePackItemId ? [sourcePackItemId] : [])
      };
    }
    const horsePackState = eventState?.horsePackState || "not_packed";
    return {
      id: `virtual:${item.id}:${horse.id}:${sourcePackItemId}`,
      virtual: true,
      itemHorseId: "",
      itemHorseKey: [wave?.id || "", horse.id, sourcePackItemId].filter(Boolean).join(":"),
      barnName: horse.name,
      horseIds: [horse.id],
      packingItemIds: [item.id],
      packWaveIds: wave?.id ? [wave.id] : [],
      sourcePackItemIds: sourcePackItemId ? [sourcePackItemId] : [],
      needed: 1,
      packed: horsePackState === "packed" ? 1 : 0,
      horsePackState,
      notes: "",
      sortOrder: horse.sortOrder
    };
  }).sort(compareHorseRows);
}

function expectedHorsesForSourceItem(sourceItem, waveHorses = []) {
  if (!sourceItem) return [];
  const linkedFromHorse = waveHorses.filter((horse) => (horse.sourcePackItemIds || []).includes(sourceItem.id));
  if (linkedFromHorse.length) return linkedFromHorse;
  const sourceHorseIds = new Set(sourceItem.horseIds || []);
  return waveHorses.filter((horse) => sourceHorseIds.has(horse.id));
}

function isPackedHorseMember(member) {
  if (stringField(member?.horsePackState) === "not_needed") return false;
  return stringField(member?.horsePackState) === "packed" || wholeQuantityField(member?.packed) >= (wholeQuantityField(member?.needed) || 1);
}

function effectiveHorseMemberNeeded(fields = {}) {
  return stringField(fields.horse_pack_state) === "not_needed" ? 0 : wholeQuantityField(fields.quantity_needed || 1);
}

function horseMemberEventType(nextState, prefix) {
  if (prefix === "horse_kit") {
    if (nextState === "packed") return "horse_kit_packed";
    if (nextState === "not_needed") return "horse_kit_not_needed";
    return "horse_kit_reopened";
  }
  if (nextState === "packed") return "mark_packed";
  if (nextState === "not_needed") return "mark_not_needed";
  return "mark_not_packed";
}

function calculatedNeededForUnlockedItem(item, calculation) {
  const plan = slugify(calculation?.plan);
  if (["per_groom", "per_horse", "horse_specific", "horse-specific", "quantity"].includes(plan)) {
    return wholeQuantityField(calculation.calculatedNeeded);
  }
  return item.quantityNeededDynamic ?? item.needed;
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
    needed: wholeQuantityField(fields.quantity_needed || 1),
    packed: wholeQuantityField(fields.quantity_packed),
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
    rosterId: record.id,
    writeHorseId: record.id,
    name: stringField(fields.barn_name || fields.horse || fields.show_name),
    barnName: stringField(fields.barn_name || fields.horse),
    showName: stringField(fields.show_name || fields.horse),
    recordState,
    active: recordState === "active",
    waveOne: !!fields.wec_wave_1,
    waveTwo: !!fields.wec_wave_2,
    notGoing: !!fields.wec_not_going,
    manualLock: !!fields.manual_lock,
    sortOrder: numberField(fields.sort_order),
    weekIds: linkedIds(fields.wec_weeks),
    sourcePackItemIds: linkedIds(fields.wec_pack_items),
    notes: stringField(fields.notes)
  };
}

function normalizePakHorseRoster(record) {
  const fields = record.fields || {};
  const linkedHorseId = linkedIds(fields.wec_horses)[0] || record.id;
  const inactive = !!fields.inactive || !!fields.wec_not_going;
  const active = fields.active === true || !inactive;
  return {
    id: linkedHorseId,
    rosterId: record.id,
    writeHorseId: linkedHorseId,
    pakHorseId: stringField(fields.pak_horse_id),
    name: stringField(fields.display_horse_barn_name || fields.barn_name || fields.horse || fields.show_name),
    barnName: stringField(fields.display_horse_barn_name || fields.barn_name || fields.horse),
    showName: stringField(fields.show_name || fields.horse),
    recordState: active ? "active" : "inactive",
    active,
    waveOne: !!fields.wec_wave_1,
    waveTwo: !!fields.wec_wave_2,
    notGoing: !!fields.wec_not_going,
    manualLock: false,
    sortOrder: numberField(fields.sort_order),
    weekIds: linkedIds(fields.wec_weeks),
    sourcePackItemIds: linkedIds(fields.pack_items),
    pakKitItemIds: linkedIds(fields.pak_kit_items),
    packWaveIds: linkedIds(fields.pack_waves),
    wwHorseIds: linkedIds(fields.ww_horses),
    wecHorseIds: linkedIds(fields.wec_horses),
    countPakKitItems: wholeQuantityField(fields.count_pak_kit_items),
    notes: stringField(fields.notes)
  };
}

function normalizeHorseKitTemplate(record) {
  const fields = record.fields || {};
  const label = stringField(fields.kit || fields.name || record.id);
  const displayLabelValue = stringField(fields.display_label || fields.display_name || label);
  return {
    id: record.id,
    label: displayLabelValue || label,
    name: label,
    displayLabel: displayLabelValue,
    key: slugify(label || record.id),
    status: slugify(fields.status || "active") || "active",
    active: slugify(fields.status || "active") !== "inactive",
    sortOrder: numberField(fields.sort_order),
    notes: stringField(fields.notes),
    kitItemIds: [...linkedIds(fields.horse_kit_items), ...linkedIds(fields.pak_kit_items)],
    packingKitIds: linkedIds(fields.horse_packing_kits),
    changeIds: linkedIds(fields.horse_kit_changes)
  };
}

function normalizeHorseKitTemplateItem(record) {
  const fields = record.fields || {};
  const name = stringField(fields.kit_item || fields.name || record.id);
  const displayLabel = stringField(fields.display_label || fields.display_name || name);
  const status = slugify(fields.item_status || "active") || "active";
  return {
    id: record.id,
    name,
    displayName: displayLabel,
    displayLabel,
    label: displayLabel || name,
    kitIds: [...linkedIds(fields.horse_kits), ...linkedIds(fields.pak_kits)],
    manualQuantity: wholeQuantityField(fields.manual_quantity || 1) || 1,
    uom: stringField(fields.uom),
    status,
    active: status !== "inactive",
    inlineEdit: slugify(fields.inline_edit || "locked") || "locked",
    inlineAllowed: slugify(fields.inline_edit || "locked") === "allowed",
    sortOrder: numberField(fields.sort_order),
    notes: stringField(fields.notes),
    packingKitIds: linkedIds(fields.horse_packing_kits),
    exceptionIds: linkedIds(fields.horse_kit_exceptions),
    changeIds: linkedIds(fields.horse_kit_changes)
  };
}

function normalizeHorsePackingKit(record, lookups = {}) {
  const fields = record.fields || {};
  const horseIds = linkedIds(fields.horse);
  const kitIds = linkedIds(fields.pak_kits).length ? linkedIds(fields.pak_kits) : linkedIds(fields.horse_kits);
  const kitItemIds = linkedIds(fields.pak_kit_items).length ? linkedIds(fields.pak_kit_items) : linkedIds(fields.horse_kit_item);
  const packWaveIds = linkedIds(fields.pack_wave);
  const horse = lookups.horseById?.get(horseIds[0]);
  const kit = lookups.kitById?.get(kitIds[0]);
  const kitItem = lookups.kitItemById?.get(kitItemIds[0]);
  const wave = lookups.waveById?.get(packWaveIds[0]);
  const neededState = slugify(fields.needed_state || "needed") || "needed";
  const packState = slugify(fields.pack_state || "not_packed") || "not_packed";
  const quantityNeededRaw = wholeQuantityField(fields.quantity_needed || kitItem?.manualQuantity || 1);
  const needed = neededState === "not_needed" || packState === "not_needed" ? 0 : quantityNeededRaw;
  const quantityPackedRaw = wholeQuantityField(fields.quantity_packed);
  const packed = packState === "packed" ? (quantityPackedRaw || needed) : 0;
  return {
    id: record.id,
    label: stringField(fields.horse_packing_kit)
      || [horse?.name, kitItem?.label].filter(Boolean).join(" - ")
      || record.id,
    horseIds,
    horseName: horse?.name || horseIds[0] || "",
    kitIds,
    kitLabel: kit?.label || kitIds[0] || "",
    kitItemIds,
    itemLabel: kitItem?.label || kitItemIds[0] || "",
    packWaveIds,
    waveKey: wave?.key || "",
    neededState,
    packState,
    quantityNeeded: quantityNeededRaw,
    quantityPacked: quantityPackedRaw,
    needed,
    packed,
    left: Math.max(0, needed - packed),
    percentPacked: needed > 0 ? progressPercent(packed, needed) : 0,
    inlineAllowed: !!kitItem?.inlineAllowed,
    sortOrder: numberField(fields.sort_order || kitItem?.sortOrder),
    notes: stringField(fields.notes),
    eventIds: linkedIds(fields.wec_packing_events),
    exceptionIds: linkedIds(fields.horse_kit_exceptions),
    changeIds: linkedIds(fields.horse_kit_changes)
  };
}

function normalizeHorseKitException(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    label: stringField(fields.exception || record.id),
    packWaveIds: linkedIds(fields.pack_wave),
    horseIds: linkedIds(fields.horse),
    kitItemIds: linkedIds(fields.horse_kit_item),
    packingKitIds: linkedIds(fields.horse_packing_kit),
    type: slugify(fields.exception_type),
    state: slugify(fields.exception_state),
    requestedBy: stringField(fields.requested_by),
    resolvedBy: stringField(fields.resolved_by),
    notes: stringField(fields.notes),
    changeIds: linkedIds(fields.horse_kit_changes),
    createdTime: record.createdTime || ""
  };
}

function normalizeHorseKitChange(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    label: stringField(fields.change || record.id),
    changeType: slugify(fields.change_type),
    kitIds: linkedIds(fields.horse_kits),
    kitItemIds: linkedIds(fields.horse_kit_item),
    packingKitIds: linkedIds(fields.horse_packing_kit),
    exceptionIds: linkedIds(fields.horse_kit_exception),
    oldValue: stringField(fields.old_value),
    newValue: stringField(fields.new_value),
    createdBy: stringField(fields.created_by),
    notes: stringField(fields.notes),
    createdTime: record.createdTime || ""
  };
}

function normalizeHorseKitComment(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    label: stringField(fields.scope_label || fields.comment || record.id),
    comment: stringField(fields.comment),
    notes: stringField(fields.notes),
    scopeType: slugify(fields.scope_type),
    scopeId: stringField(fields.scope_id),
    scopeLabel: stringField(fields.scope_label),
    status: slugify(fields.comment_status || "active") || "active",
    horseIds: linkedIds(fields.horse),
    packWaveIds: linkedIds(fields.pack_wave),
    createdBy: stringField(fields.created_by),
    createdAt: stringField(fields.created_at || record.createdTime),
    createdTime: record.createdTime || ""
  };
}

function normalizeCommentShort(record) {
  const fields = record.fields || {};
  const label = stringField(fields.display_label || fields.comment_short || record.id);
  const status = slugify(fields.status || "active") || "active";
  return {
    id: record.id,
    label,
    comment: stringField(fields.comment_short || label),
    scopeType: slugify(fields.scope_type || "horse"),
    status,
    active: status !== "inactive",
    sortOrder: numberField(fields.sort_order),
    notes: stringField(fields.notes)
  };
}

function normalizeCommentLog(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    label: stringField(fields.comment_log || record.id),
    action: slugify(fields.action),
    scopeType: slugify(fields.scope_type),
    scopeId: stringField(fields.scope_id),
    scopeLabel: stringField(fields.scope_label),
    commentIds: linkedIds(fields.wec_commenting),
    commentShortIds: linkedIds(fields.comment_shorts),
    oldValue: stringField(fields.old_value),
    newValue: stringField(fields.new_value),
    createdBy: stringField(fields.created_by),
    notes: stringField(fields.notes),
    createdTime: record.createdTime || ""
  };
}

function normalizeHorseAttributeOption(record, group, fieldKey, sourceTable) {
  const fields = record.fields || {};
  const label = stringField(
    fields.display_label ||
    fields.gender ||
    fields.discipline ||
    fields.color ||
    fields.horse_attribute ||
    record.id
  );
  return {
    id: record.id,
    label,
    attributeGroup: group,
    attributeKey: fieldKey,
    sourceTable,
    status: slugify(fields.status || "active") || "active",
    active: slugify(fields.status || "active") !== "inactive",
    sortOrder: numberField(fields.sort_order),
    notes: stringField(fields.notes),
    createdTime: record.createdTime || ""
  };
}

function normalizeHorseRosterLog(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    label: stringField(fields.change_key || fields.record_key || record.id),
    action: slugify(fields.status || fields.action),
    horseRecordId: stringField(fields.horse_record_id),
    horseKey: stringField(fields.horse_key),
    horseName: stringField(fields.horse_name || fields.barn_name || fields.show_name),
    source: stringField(fields.source),
    fieldKey: stringField(fields.field_name),
    oldValue: stringField(fields.old_value),
    newValue: stringField(fields.new_value),
    notes: stringField(fields.notes),
    createdTime: stringField(fields.changed_at || fields.updated_at || record.createdTime)
  };
}

function compareHorseAttributes(a, b) {
  return Number(a.sortOrder || 0) - Number(b.sortOrder || 0) ||
    String(a.attributeGroup || "").localeCompare(String(b.attributeGroup || "")) ||
    String(a.label || "").localeCompare(String(b.label || ""));
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

function buildTabSummaries(lists) {
  const summaries = new Map();
  for (const list of lists) {
    const tabLabels = list.tabs?.length ? list.tabs : [list.label];
    for (const tabLabel of tabLabels) {
      const key = slugify(tabLabel || list.label) || list.key || list.id;
      const id = `tab:${key}`;
      const summary = summaries.get(id) || {
        id,
        key,
        label: tabLabel || list.label,
        listIds: [],
        rows: 0,
        done: 0,
        open: 0
      };
      summary.listIds.push(list.id);
      summary.rows += list.rows;
      summary.done += list.done;
      summary.open += list.open;
      summaries.set(id, summary);
    }
  }
  return [...summaries.values()];
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

function buildQuantityCalculation(item, sourceItem, wave, waveHorses = []) {
  const plan = item.listPlan || sourceItem?.listPlan || "";
  const frozenNeeded = numberField(item.needed);
  const unit = item.unit || sourceItem?.uom || "";

  if (plan === "per_groom") {
    const perGroom = numberField(sourceItem?.perGroom || item.quantityBase);
    const groomCount = numberField(wave?.effectiveGroomCountFinal ?? wave?.groomCountFinal);
    const calculatedNeeded = wholeQuantityField(perGroom * groomCount);
    return calculationRow({
      plan,
      formula: "per_groom * effective_groom_count_final",
      sourceField: "wec_pack_items.per_groom",
      multiplierField: "wec_pack_waves.groom_sanity",
      base: perGroom,
      multiplier: groomCount,
      calculatedNeeded,
      frozenNeeded,
      unit,
      countSource: wave?.countSource || "",
      countsLocked: !!wave?.countsLocked
    });
  }

  if (plan === "per_horse") {
    const perHorse = numberField(sourceItem?.perHorse || item.quantityBase);
    const horseCount = numberField(wave?.effectiveHorseCount ?? wave?.horseCount);
    const calculatedNeeded = wholeQuantityField(perHorse * horseCount);
    return calculationRow({
      plan,
      formula: "per_horse * effective_horse_count",
      sourceField: "wec_pack_items.per_horse",
      multiplierField: wave?.countsLocked
        ? "wec_pack_waves.horse_count"
        : wave?.countSource === "count_horses_wave_one"
          ? "wec_pack_waves.count_horses_wave_one"
          : "current wave horse count",
      base: perHorse,
      multiplier: horseCount,
      calculatedNeeded,
      frozenNeeded,
      unit,
      countSource: wave?.countSource || "",
      countsLocked: !!wave?.countsLocked
    });
  }

  if (plan === "horse_specific" || plan === "horse-specific") {
    const expectedHorseCount = sourceItem ? expectedHorsesForSourceItem(sourceItem, waveHorses).length : item.horseMembers.length;
    const calculatedNeeded = wholeQuantityField(expectedHorseCount);
    return calculationRow({
      plan,
      formula: "current expected horse kits",
      sourceField: "wec_horses.wec_pack_items",
      multiplierField: "current eligible horse count",
      base: 1,
      multiplier: expectedHorseCount,
      calculatedNeeded,
      frozenNeeded,
      unit,
      countSource: "current_wave_scope",
      countsLocked: false
    });
  }

  if (plan === "quantity") {
    const calculatedNeeded = wholeQuantityField(sourceItem?.quantity || item.quantityBase || frozenNeeded);
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
    base: wholeQuantityField(frozenNeeded),
    multiplier: 1,
    calculatedNeeded: wholeQuantityField(frozenNeeded),
    frozenNeeded,
    unit
  });
}

function calculationRow({ plan, formula, sourceField, multiplierField, base, multiplier, calculatedNeeded, frozenNeeded, unit, countSource = "", countsLocked = false }) {
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
    countSource,
    countsLocked,
    matchesFrozen: Math.abs(numberField(calculatedNeeded) - numberField(frozenNeeded)) < 0.0001
  };
}

function isHorseInWave(horse, wave) {
  const waveKey = slugify(wave?.key || wave?.wave || "");
  if (waveKey === "wave_one" || waveKey === "wave_1" || waveKey === "one") return !!horse?.waveOne && !horse?.notGoing;
  if (waveKey === "wave_two" || waveKey === "wave_2" || waveKey === "two") return !!horse?.waveTwo && !horse?.notGoing;
  if (!wave || !wave.includedWeekIds.length) return true;
  return horse.weekIds.some((weekId) => wave.includedWeekIds.includes(weekId));
}

function horseWaveState(horse) {
  if (horse?.notGoing) return "not_going";
  if (horse?.waveOne) return "wave_one";
  if (horse?.waveTwo) return "wave_two";
  return "unassigned";
}

function horseKitSummary(rows = []) {
  const needed = rows.reduce((sum, row) => sum + wholeQuantityField(row.needed), 0);
  const packed = rows.reduce((sum, row) => sum + wholeQuantityField(row.packed), 0);
  const notNeeded = rows.filter((row) => row.neededState === "not_needed" || row.packState === "not_needed").length;
  const open = rows.filter((row) => row.packState !== "packed" && row.packState !== "not_needed").length;
  return {
    rows: rows.length,
    needed,
    packed,
    left: Math.max(0, needed - packed),
    open,
    notNeeded,
    percentPacked: needed > 0 ? progressPercent(packed, needed) : 0
  };
}

function isHorseSpecificSourceItem(sourceItem) {
  if (!sourceItem || sourceItem.ignored || !sourceItem.active) return false;
  return sourceItem.horseSpecific || sourceItem.listPlan === "horse_specific" || sourceItem.listPlan === "horse-specific";
}

function expectedSourceHorseIds(sourceItem, waveHorseIds) {
  if (!sourceItem) return new Set();
  return new Set(sourceItem.horseIds.filter((horseId) => waveHorseIds.has(horseId)));
}

function isSafeToRemoveHorseMember(member, eventCount, horse) {
  return numberField(member.packed) === 0 &&
    eventCount === 0 &&
    member.horsePackState !== "packed" &&
    !horse?.manualLock;
}

function horseMemberAuditRow(member, sourceItem, eventCount, safeToRemove, horse) {
  return {
    id: member.id,
    itemHorseId: member.itemHorseId,
    itemHorseKey: member.itemHorseKey,
    sourcePackItemId: sourceItem?.id || member.sourcePackItemIds[0] || "",
    sourceItem: sourceItem?.appName || "",
    horseIds: member.horseIds,
    barnName: member.barnName,
    horseManualLock: !!horse?.manualLock,
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

function isPurchaseOnsiteItem(item) {
  return item?.resolutionState === "purchase_onsite";
}

function isUnresolvedItem(item) {
  return item?.resolutionState === "note" || item?.resolutionState === "unresolved";
}

function progressPercent(done, rows) {
  const total = numberField(rows);
  if (total <= 0) return 0;
  return Math.max(0, Math.min(100, Math.round((numberField(done) / total) * 100)));
}

function isActiveWorksheetRow(record) {
  const fields = record.fields || {};
  if (fields.ignore) return false;
  const recordState = stringField(fields.record_state || "active");
  return recordState === "active";
}

function selectWave(waves, packWaveId, packWaveKey = "") {
  if (packWaveId) return waves.find((record) => record.id === packWaveId) || null;
  if (packWaveKey) {
    const targets = waveKeyAliases(packWaveKey);
    return waves.find((record) => waveRecordKeys(record).some((key) => targets.includes(key))) || null;
  }
  return waves.find((record) => !!record.fields?.active) || waves[0] || null;
}

function waveRecordKeys(record) {
  const fields = record.fields || {};
  const keys = [
    record.id,
    fields.wave_key,
    fields.key,
    fields.wave,
    fields.Name
  ].flatMap(waveKeyAliases).filter(Boolean);
  return Array.from(new Set(keys));
}

function waveKeyAliases(value) {
  const key = slugify(value);
  if (!key) return [];
  const aliases = new Set([key]);
  const wordToNumber = {
    one: "1",
    two: "2",
    three: "3",
    four: "4",
    five: "5"
  };
  const numberToWord = Object.fromEntries(Object.entries(wordToNumber).map(([word, numberValue]) => [numberValue, word]));
  for (const [word, numberValue] of Object.entries(wordToNumber)) {
    aliases.add(key.replace(new RegExp(`(^|_)${word}($|_)`, "g"), `$1${numberValue}$2`));
  }
  for (const [numberValue, word] of Object.entries(numberToWord)) {
    aliases.add(key.replace(new RegExp(`(^|_)${numberValue}($|_)`, "g"), `$1${word}$2`));
  }
  return Array.from(aliases);
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

function groupByFirstId(rows, fieldName) {
  const grouped = new Map();
  for (const row of rows || []) {
    const ids = Array.isArray(row?.[fieldName]) ? row[fieldName] : [];
    const id = ids[0] || "";
    if (!id) continue;
    const list = grouped.get(id) || [];
    list.push(row);
    grouped.set(id, list);
  }
  return grouped;
}

function groupHorseMembersByWorksheetItem(records) {
  const grouped = new Map();
  for (const record of records) {
    const ids = new Set([
      ...linkedIds(record.fields?.packing_item),
      ...linkedIds(record.fields?.source_pack_item)
    ]);
    for (const id of ids) {
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

function stringListField(value) {
  if (Array.isArray(value)) return value.map(stringField).filter(Boolean);
  const single = stringField(value);
  return single ? [single] : [];
}

function numberField(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function hasNumberField(value) {
  if (Array.isArray(value)) return value.some(hasNumberField);
  if (value === undefined || value === null || value === "") return false;
  return Number.isFinite(Number(value));
}

function wholeQuantityField(value) {
  const number = numberField(value);
  if (number <= 0) return 0;
  return Math.abs(number - Math.round(number)) < 0.000001
    ? Math.round(number)
    : Math.ceil(number - 0.000001);
}

function quantityDisplay(value) {
  return String(wholeQuantityField(value));
}

function nullableNumberField(value) {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function clean(value) {
  return String(value ?? "").trim();
}

function uniqueStrings(values) {
  const seen = new Set();
  const result = [];
  for (const value of values || []) {
    const text = stringField(value);
    const key = text.toLowerCase();
    if (!text || seen.has(key)) continue;
    seen.add(key);
    result.push(text);
  }
  return result;
}

function uniqueById(values) {
  const seen = new Set();
  const result = [];
  for (const value of values || []) {
    if (!value?.id || seen.has(value.id)) continue;
    seen.add(value.id);
    result.push(value);
  }
  return result;
}

function compareWorksheetRows(a, b) {
  return compareText(a.name, b.name) || compareNumber(a.sortOrder, b.sortOrder) || compareText(a.id, b.id);
}

function comparePackLists(a, b) {
  return compareText(a.label, b.label) || compareText(a.id, b.id);
}

function compareHorseRows(a, b) {
  return compareText(a.barnName, b.barnName) || compareNumber(a.sortOrder, b.sortOrder) || compareText(a.id, b.id);
}

function compareHorseRosterRows(a, b) {
  return compareText(a.name, b.name) || compareNumber(a.sortOrder, b.sortOrder) || compareText(a.id, b.id);
}

function compareKitTemplates(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label) || compareText(a.id, b.id);
}

function compareKitItems(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label) || compareText(a.id, b.id);
}

function compareHorsePackingRows(a, b) {
  return compareText(a.horseName, b.horseName)
    || compareText(a.kitLabel, b.kitLabel)
    || compareNumber(a.sortOrder, b.sortOrder)
    || compareText(a.itemLabel, b.itemLabel)
    || compareText(a.id, b.id);
}

function compareChangeLikeRows(a, b) {
  return compareText(b.createdTime, a.createdTime) || compareText(a.label, b.label) || compareText(a.id, b.id);
}

function compareCommentShorts(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label) || compareText(a.id, b.id);
}

function compareNumber(a, b) {
  return (Number(a) || 0) - (Number(b) || 0);
}

function compareText(a, b) {
  return clean(a).localeCompare(clean(b), undefined, { sensitivity: "base" });
}

function displayLabel(value) {
  const text = clean(value).replace(/[_-]+/g, " ").trim();
  return text.replace(/\b[a-z]/g, (letter) => letter.toUpperCase());
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function slugify(value) {
  return clean(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}
