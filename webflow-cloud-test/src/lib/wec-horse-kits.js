import { env } from "cloudflare:workers";

export const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

const DEFAULT_META_TABLE = "tbllJywsOstkqT5yZ";

const ENV_TABLES = {
  wec_pack_waves: {
    table: "AIRTABLE_WEC_PACK_WAVES_TABLE",
    view: "AIRTABLE_WEC_PACK_WAVES_VIEW"
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

export async function horseKitReport(airtable, requestUrl) {
  const url = new URL(requestUrl);
  const packWaveId = clean(url.searchParams.get("packWaveId"));
  const packWaveKey = slugify(url.searchParams.get("packWaveKey") || url.searchParams.get("wave") || "wave_one");
  const horseView = packWaveKey === "all" ? "" : packWaveKey || "wave_one";
  const pakGroupsView = "horse_specific";
  const context = await loadHorseKitContext(airtable);
  const baseTables = horseKitTables(context);
  const groupRecords = await listOptionalViewRecords(airtable, baseTables.pak_groups, pakGroupsView, {
    fields: horseKitReadFields(baseTables.pak_groups)
  });
  const groupStack = normalizePakGroupStack(groupRecords, { groupPrefix: "gp" });
  const tables = horseKitTables(context, groupStack);
  assertHorseKitTables(tables);
  const rosterLinkFields = horseKitRosterLinkFields(tables);

  const [
    waveRecords,
    horseRecords,
    kitRecords,
    kitItemRecords,
    packingKitRecords,
    commentRecords,
    commentShortRecords,
    tabRecords,
    laneRecords,
    viewRecords
  ] = await Promise.all([
    listHorseKitRecords(airtable, tables.wec_pack_waves),
    listHorseKitRecords(airtable, tables.pak_horses_roster, { view: horseView }),
    listHorseKitRecords(airtable, tables.pak_kits),
    listHorseKitRecords(airtable, tables.pak_kit_items),
    listHorseKitRecords(airtable, tables.horse_packing_kits, { extraFields: [rosterLinkFields.packingKitHorseFieldId] }),
    listOptionalHorseKitRecords(airtable, tables.wec_commenting, { extraFields: [rosterLinkFields.commentHorseFieldId] }),
    listOptionalHorseKitRecords(airtable, tables.comment_shorts),
    listOptionalHorseKitRecords(airtable, tables.pak_tabs),
    listOptionalViewRecords(airtable, tables.wec_lanes, "horse_specific", {
      fields: horseKitReadFields(tables.wec_lanes)
    }),
    listOptionalHorseKitRecords(airtable, tables.pak_views)
  ]);

  const selectedWaveRecord = selectWave(waveRecords, packWaveId, packWaveKey);
  const selectedWave = selectedWaveRecord ? normalizeWave(selectedWaveRecord) : null;
  const selectedWavePakTabIds = new Set(selectedWave?.pakTabIds || []);
  const primaryTabs = tabRecords
    .map(normalizePakTab)
    .filter((tab) => tab.active && (!selectedWavePakTabIds.size || selectedWavePakTabIds.has(tab.id)))
    .sort(comparePakTabs)
    .map((tab) => ({ id: tab.id, key: tab.key, label: tab.label, active: tab.active, sortOrder: tab.priority }));
  const laneControls = laneRecords
    .map(normalizeWecLane)
    .filter((lane) => lane.active)
    .sort(compareWecLanes)
    .map((lane) => ({ id: lane.id, key: lane.key, label: lane.label, active: lane.active, sortOrder: lane.priority }));
  const secondaryControlPakViewIds = new Set((groupStack.activeRows.find((row) => row.renderKey === "secondary_controls")?.pakViewIds || []));
  const secondaryControls = viewRecords
    .map(normalizePakView)
    .filter((view) => secondaryControlPakViewIds.has(view.id))
    .sort(comparePakViews)
    .map((view) => ({ id: view.id, key: view.key, label: view.label, active: true, sortOrder: view.sortOrder }));

  const fullKitItems = kitItemRecords.map(normalizeKitItem).sort(compareKitItems);
  const kitItemById = new Map(fullKitItems.map((item) => [item.id, item]));
  const kitItemsByKitId = groupItemsByLinkedKit(fullKitItems);
  const kitItems = fullKitItems.map((item) => ({
    id: item.id,
    name: item.name,
    displayName: item.displayName,
    displayLabel: item.displayLabel,
    label: item.label,
    kitIds: item.kitIds,
    status: item.status,
    active: item.active,
    sortOrder: item.sortOrder
  }));
  const kits = kitRecords.map(normalizeKit).sort(compareKitTemplates).map((kit) => {
    const linkedItems = kit.kitItemIds.map((id) => kitItemById.get(id)).filter(Boolean);
    const reverseLinkedItems = kitItemsByKitId.get(kit.id) || [];
    const items = [...new Map([...linkedItems, ...reverseLinkedItems].map((item) => [item.id, item])).values()]
      .filter((item) => item.status !== "inactive" && item.active !== false)
      .sort(compareKitItems);
    return {
      id: kit.id,
      label: kit.label,
      name: kit.name,
      displayLabel: kit.displayLabel,
      key: kit.key,
      status: kit.status,
      active: kit.active,
      sortOrder: kit.sortOrder,
      kitItemIds: items.map((item) => item.id),
      kitItemCount: items.length
    };
  });
  const kitById = new Map(kits.map((kit) => [kit.id, kit]));
  const horses = horseRecords.map((record) => {
    const horse = normalizePakHorseRoster(record);
    return {
      id: horse.id,
      rosterId: horse.rosterId,
      writeHorseId: horse.writeHorseId,
      pakHorseId: horse.pakHorseId,
      name: horse.name,
      barnName: horse.barnName,
      showName: horse.showName,
      active: horse.active,
      waveOne: horse.waveOne,
      waveTwo: horse.waveTwo,
      notGoing: horse.notGoing,
      sortOrder: horse.sortOrder,
      pakKitItemIds: horse.pakKitItemIds,
      countPakKitItems: horse.countPakKitItems,
      profileUrl: horse.profileUrl,
      waveState: horseWaveState(horse)
    };
  }).sort(compareHorseRosterRows);
  const horseById = new Map(horses.map((horse) => [horse.id, horse]));
  const waveById = new Map(waveRecords.map((record) => {
    const wave = normalizeWave(record);
    return [wave.id, wave];
  }));
  const packingRows = packingKitRecords
    .map((record) => normalizePackingKit(record, {
      horseById,
      kitById,
      kitItemById,
      waveById,
      horseLinkField: rosterLinkFields.packingKitHorse
    }))
    .filter((row) => row.horseIds.length && row.kitItemIds.length)
    .filter((row) => !selectedWave?.id || row.packWaveIds.length === 0 || row.packWaveIds.includes(selectedWave.id))
    .map((row) => ({
      id: row.id,
      label: row.label,
      horseIds: row.horseIds,
      kitIds: row.kitIds,
      kitItemIds: row.kitItemIds,
      packWaveIds: row.packWaveIds,
      neededState: row.neededState,
      packState: row.packState
    }))
    .sort(compareHorsePackingRows);
  const comments = commentRecords
    .map((record) => normalizeComment(record, { horseLinkField: rosterLinkFields.commentHorse }))
    .map((comment) => ({
      id: comment.id,
      comment: comment.comment,
      scopeLabel: comment.scopeLabel,
      horseIds: comment.horseIds,
      createdTime: comment.createdTime
    }))
    .sort(compareChangeLikeRows);
  const commentShorts = commentShortRecords.map(normalizeCommentShort).sort(compareCommentShorts);

  return {
    ok: true,
    v: 2,
    source: {
      packWaveId: selectedWave?.id || "",
      packWaveKey: selectedWave?.key || packWaveKey,
      pakGroupsView,
      horseView: horseView || "all_records",
      kitSource: "pak",
      horseSource: "pak_horses_roster",
      horseLinkFields: rosterLinkFields,
      tables: {
        wec_pack_waves: tables.wec_pack_waves?.id || "",
        wec_lanes: tables.wec_lanes?.id || "",
        pak_groups: tables.pak_groups?.id || "",
        pak_tabs: tables.pak_tabs?.id || "",
        pak_views: tables.pak_views?.id || "",
        pak_horses_roster: tables.pak_horses_roster?.id || "",
        pak_kits: tables.pak_kits?.id || "",
        pak_kit_items: tables.pak_kit_items?.id || "",
        horse_packing_kits: tables.horse_packing_kits?.id || "",
        horse_kit_changes: tables.horse_kit_changes?.id || "",
        wec_commenting: tables.wec_commenting?.id || "",
        comment_shorts: tables.comment_shorts?.id || ""
      }
    },
    wave: selectedWave ? {
      id: selectedWave.id,
      key: selectedWave.key,
      label: selectedWave.wave,
      reportTitle: selectedWave.wecReportTitle,
      reportSubtitle: selectedWave.wecReportSubtitle,
      pakTabIds: selectedWave.pakTabIds
    } : null,
    counts: {
      horses: horses.length,
      visibleHorses: horses.length,
      kits: kits.length,
      kitItems: kitItems.length,
      packingRows: packingRows.length
    },
    horses,
    kits,
    kitItems,
    packingRows,
    comments,
    commentShorts,
    primaryTabs,
    laneControls,
    secondaryControls,
    groupStack: {
      activeRows: (groupStack.activeRows || []).map((row) => ({
        id: row.id,
        renderKey: row.renderKey,
        displayLabel: row.displayLabel,
        componentKey: row.componentKey,
        role: row.role,
        sortOrder: row.sortOrder,
        pakViewIds: row.pakViewIds || []
      }))
    }
  };
}

export async function horseKitActionReport(airtable, requestUrl, payload) {
  const action = clean(payload?.action);
  const context = await loadHorseKitContext(airtable);
  const pakGroupsView = "horse_specific";
  const baseTables = horseKitTables(context);
  const groupRecords = await listOptionalViewRecords(airtable, baseTables.pak_groups, pakGroupsView, {
    fields: horseKitReadFields(baseTables.pak_groups)
  });
  const groupStack = normalizePakGroupStack(groupRecords, { groupPrefix: "gp" });
  const tables = horseKitTables(context, groupStack);
  assertHorseKitTables(tables);
  let result;
  if (action === "set_packing_kit_state") {
    result = await applyPackingKitState(airtable, tables, payload);
  } else if (action === "save_comment") {
    result = await saveHorseKitComment(airtable, tables, payload);
  } else {
    return { ok: false, error: "unknown_horse_kit_action", action };
  }
  return {
    ok: true,
    v: 2,
    action,
    result,
    state: await horseKitReport(airtable, requestUrl)
  };
}

async function loadHorseKitContext(airtable) {
  const [schema, metaRecords] = await Promise.all([
    getBaseSchema(airtable),
    listAirtableRecords(airtable, airtable.metaTable)
  ]);
  const registry = buildRegistry(metaRecords);
  const tables = buildTableConfig(airtable, registry, schema);
  return { schema, registry, tables };
}

function horseKitTables(context, groupStack = null) {
  const groupTable = (renderKey, defaultName) => pakGroupPhysicalTableName(groupStack, renderKey, defaultName);
  return {
    wec_lanes: physicalTableConfig(context, "wec_lanes", true),
    wec_pack_waves: physicalTableConfig(context, "wec_pack_waves"),
    pak_kits: physicalTableConfig(context, groupTable("kit_source", "pak_kits"), true),
    pak_kit_items: physicalTableConfig(context, groupTable("drawer_items", "pak_kit_items"), true),
    pak_horses_roster: physicalTableConfig(context, groupTable("main_table", "pak_horses_roster"), true),
    horse_packing_kits: physicalTableConfig(context, groupTable("state_links", "horse_packing_kits")),
    horse_kit_changes: physicalTableConfig(context, groupTable("change_log", "horse_kit_changes"), true),
    wec_commenting: physicalTableConfig(context, groupTable("comments", "wec_commenting"), true),
    comment_shorts: physicalTableConfig(context, "comment_shorts", true),
    pak_tabs: physicalTableConfig(context, "pak_tabs", true),
    pak_views: physicalTableConfig(context, "pak_views", true),
    pak_groups: physicalTableConfig(context, "pak_groups", true)
  };
}

function assertHorseKitTables(tables) {
  for (const key of ["pak_horses_roster", "pak_kits", "pak_kit_items", "horse_packing_kits", "horse_kit_changes"]) {
    if (!tables[key]?.id) throw new Error(`horse_kits_blueprint_missing:${key}`);
  }
}

function horseKitRosterLinkFields(tables) {
  const packingKitHorse = linkedRecordField(tables.horse_packing_kits, tables.pak_horses_roster, ["pak_horses_roster"]);
  const commentHorse = linkedRecordField(tables.wec_commenting, tables.pak_horses_roster, ["pak_horses_roster"]);
  return {
    packingKitHorse: packingKitHorse.name,
    packingKitHorseFieldId: packingKitHorse.id,
    commentHorse: commentHorse.name,
    commentHorseFieldId: commentHorse.id
  };
}

function linkedRecordField(tableConfig, targetTableConfig, defaultNames = []) {
  const targetTableId = targetTableConfig?.id || "";
  const schemaFields = tableConfig?.schemaFields || [];
  const linked = schemaFields.find((field) =>
    field.type === "multipleRecordLinks" &&
    field.options?.linkedTableId === targetTableId
  );
  if (linked?.name) return { name: linked.name, id: linked.id || "" };
  const schemaNames = new Set(schemaFields.map((field) => field.name));
  const name = defaultNames.find((candidate) => schemaNames.has(candidate)) || "";
  return { name, id: "" };
}

function horseKitReadFields(tableConfig, extraFields = []) {
  if (!tableConfig) return [];
  const defaults = HORSE_KIT_READ_FIELDS[tableConfig.name] || [];
  const configured = tableConfig.fields || [];
  const fields = uniqueStrings([...configured, ...defaults, ...extraFields]);
  const schemaKeys = new Set((tableConfig.schemaFields || []).flatMap((field) => [field.name, field.id].filter(Boolean)));
  return schemaKeys.size ? fields.filter((field) => schemaKeys.has(field)) : fields;
}

async function listHorseKitRecords(airtable, tableConfig, options = {}) {
  if (!tableConfig?.id) throw new Error(`missing_horse_kit_table:${options.name || tableConfig?.name || "unknown"}`);
  return listAirtableRecords(airtable, tableConfig.id, options.view ?? tableConfig.view, {
    fields: horseKitReadFields(tableConfig, options.extraFields || [])
  });
}

async function listOptionalHorseKitRecords(airtable, tableConfig, options = {}) {
  if (!tableConfig?.id) return [];
  try {
    return await listHorseKitRecords(airtable, tableConfig, options);
  } catch (error) {
    console.warn(`[wec-horse-kits] optional table skipped: ${tableConfig.name || tableConfig.id}`, error);
    return [];
  }
}

const HORSE_KIT_READ_FIELDS = {
  wec_pack_waves: [
    "wave",
    "wave_key",
    "key",
    "Name",
    "wave_type",
    "active",
    "manual_lock",
    "wec_report_title",
    "wec_report_subtitle",
    "deadline_date",
    "days_till",
    "horse_count",
    "count_horses_wave_one",
    "groom_sanity",
    "sort_order",
    "show",
    "included_weeks",
    "pak_tabs"
  ],
  pak_horses_roster: [
    "pak_horse_id",
    "display_horse_barn_name",
    "barn_name",
    "horse",
    "show_name",
    "active",
    "inactive",
    "wec_not_going",
    "wec_wave_1",
    "wec_wave_2",
    "sort_order",
    "wec_weeks",
    "pack_items",
    "pak_kit_items",
    "pack_waves",
    "count_pak_kit_items",
    "profile_url",
    "entry_uri",
    "search_uri",
    "url",
    "link",
    "notes"
  ],
  pak_kits: [
    "kit",
    "name",
    "display_label",
    "display_name",
    "status",
    "sort_order",
    "notes",
    "pak_kit_items",
    "horse_packing_kits",
    "horse_kit_changes"
  ],
  pak_kit_items: [
    "kit_item",
    "name",
    "display_label",
    "display_name",
    "item_status",
    "pak_kits",
    "uom",
    "inline_edit",
    "sort_order",
    "notes",
    "horse_packing_kits",
    "horse_kit_changes"
  ],
  horse_packing_kits: [
    "horse_packing_kit",
    "pack_wave",
    "pak_kits",
    "pak_kit_items",
    "needed_state",
    "pack_state",
    "sort_order",
    "notes",
    "horse_kit_changes"
  ],
  horse_kit_changes: [
    "change",
    "change_type",
    "pak_kits",
    "pak_kit_items",
    "horse_packing_kit",
    "old_value",
    "new_value",
    "created_by",
    "notes"
  ],
  wec_commenting: [
    "event",
    "pack_wave",
    "event_type",
    "scope_type",
    "scope_id",
    "scope_label",
    "comment_status",
    "comment",
    "notes",
    "created_at",
    "created_by"
  ],
  comment_shorts: ["display_label", "comment_short", "scope_type", "status", "sort_order", "notes"],
  pak_tabs: ["tab", "tab_label", "tab_priority", "active", "core", "pack_groups", "pak_views", "wec_pack_waves"],
  wec_lanes: ["lane", "lane_label", "lane_priority", "active", "entity", "purpose"],
  pak_views: ["view", "view_label", "pak_tabs", "pak_aggs", "pak_groups"],
  pak_groups: [
    "group_key",
    "gp_pre",
    "stack",
    "sort_order",
    "role",
    "render_key",
    "display_label",
    "component_key",
    "table_name",
    "physical_table",
    "active",
    "is_hidden",
    "include_on_drawer",
    "is_drill_down",
    "add_filter",
    "filter_by",
    "add_search",
    "search_by",
    "add_aggregates",
    "pak_aggs",
    "pak_views",
    "all_aggregates",
    "needs_ui",
    "allow_add_new",
    "allow_inline_edit"
  ]
};

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
  return {
    id: record.id,
    sourceIndex: index,
    groupKey: clean(fields.group_key),
    groupPrefix: clean(fields.gp_pre),
    stack: numberField(fields.stack),
    sortOrder: numberField(fields.sort_order),
    role: slugify(fields.role),
    renderKey: slugify(fields.render_key),
    displayLabel: clean(fields.display_label),
    componentKey: clean(fields.component_key),
    tableName,
    physicalTableName,
    active: !!fields.active,
    hidden: !!fields.is_hidden,
    includeOnDrawer: !!fields.include_on_drawer,
    pakViewIds: linkedIds(fields.pak_views)
  };
}

function pakGroupPhysicalTableName(groupStack, renderKey, defaultName) {
  const normalizedKey = slugify(renderKey);
  const row = (groupStack?.activeRows || []).find((candidate) => candidate.renderKey === normalizedKey);
  return row?.physicalTableName || defaultName;
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
    view: existing?.view || "",
    fields: existing?.fields || [],
    schemaFields: schemaTable?.fields || []
  };
}

async function applyPackingKitState(airtable, tables, payload) {
  const rowId = clean(payload?.packingKitId || payload?.rowId);
  const horseId = clean(payload?.horseId);
  const kitId = clean(payload?.kitId);
  const kitItemId = clean(payload?.kitItemId);
  const packWaveId = clean(payload?.packWaveId);
  const nextState = slugify(payload?.packState || payload?.state);
  if (!["packed", "not_packed", "not_needed"].includes(nextState)) throw new Error("invalid_pack_state");
  const rosterLinkFields = horseKitRosterLinkFields(tables);
  const horseLinkField = rosterLinkFields.packingKitHorse;
  if (!horseLinkField) throw new Error("missing_horse_packing_kits_roster_link");

  const [horses, pakKitItems, existingRows] = await Promise.all([
    rowId ? Promise.resolve([]) : listHorseKitRecords(airtable, tables.pak_horses_roster),
    rowId ? Promise.resolve([]) : listHorseKitRecords(airtable, tables.pak_kit_items),
    rowId ? Promise.resolve([]) : listHorseKitRecords(airtable, tables.horse_packing_kits, { extraFields: [rosterLinkFields.packingKitHorseFieldId] })
  ]);
  const existing = rowId
    ? (await findRecordInConfiguredView(airtable, tables.horse_packing_kits, rowId)).record
    : existingRows.find((candidate) =>
        includesLinkedId(candidate.fields?.[horseLinkField], horseId) &&
        includesLinkedId(candidate.fields?.pack_wave, packWaveId) &&
        includesLinkedId(candidate.fields?.pak_kit_items, kitItemId) &&
        (!kitId || includesLinkedId(candidate.fields?.pak_kits, kitId))
      );
  const horse = rowId ? null : horses.find((record) => record.id === horseId);
  const itemRecord = rowId ? null : pakKitItems.find((record) => record.id === kitItemId);

  if (!existing) {
    if (!horseId) throw new Error("missing_horse_id");
    if (!kitId) throw new Error("missing_kit_id");
    if (!kitItemId) throw new Error("missing_kit_item_id");
    if (!packWaveId) throw new Error("missing_pack_wave_id");
    if (!horse) throw new Error("horse_not_found");
    if (!itemRecord) throw new Error("kit_item_not_found");
    const normalizedHorse = normalizePakHorseRoster(horse);
    const item = normalizeKitItem(itemRecord);
    if (normalizedHorse.notGoing) throw new Error("horse_not_packable_for_horse_specific");
    if (!normalizedHorse.pakKitItemIds.includes(kitItemId)) throw new Error("kit_item_not_assigned_to_horse");
    if (!item.kitIds.includes(kitId)) throw new Error("kit_item_not_in_kit");
  }

  const record = existing || null;
  const beforeState = stringField(record?.fields?.pack_state || "not_packed");
  const beforeNeededState = stringField(record?.fields?.needed_state || "needed");
  const updateFields = {
    needed_state: nextState === "not_needed" ? "not_needed" : "needed",
    pack_state: nextState
  };
  const updated = existing
    ? await patchAirtableRecord(airtable, tables.horse_packing_kits.id, record.id, updateFields)
    : await createAirtableRecord(airtable, tables.horse_packing_kits.id, compactFields({
        horse_packing_kit: `${stringField(horse.fields?.display_horse_barn_name || horse.fields?.barn_name || horse.fields?.horse || horse.fields?.show_name)} - ${stringField(itemRecord.fields?.display_label || itemRecord.fields?.display_name || itemRecord.fields?.kit_item || itemRecord.id)}`,
        pack_wave: [packWaveId],
        [horseLinkField]: [horseId],
        pak_kits: [kitId],
        pak_kit_items: [kitItemId],
        sort_order: wholeQuantityField(itemRecord.fields?.sort_order),
        ...updateFields
      }));
  const change = await createHorseKitChange(airtable, tables, {
    changeType: "kit_state_changed",
    packingKitRecord: {
      id: updated.id || record?.id,
      fields: {
        ...(record?.fields || {}),
        ...updateFields,
        pak_kits: kitId ? [kitId] : linkedIds(record?.fields?.pak_kits),
        pak_kit_items: kitItemId ? [kitItemId] : linkedIds(record?.fields?.pak_kit_items)
      }
    },
    oldValue: { needed_state: beforeNeededState, pack_state: beforeState },
    newValue: updateFields,
    notes: clean(payload?.notes)
  });
  return { updated, change };
}

async function saveHorseKitComment(airtable, tables, payload) {
  if (!tables.wec_commenting?.id) throw new Error("commenting_table_not_configured");
  const commentId = clean(payload?.commentId);
  const scopeType = slugify(payload?.scopeType || (payload?.horseId ? "horse" : "page")) || "page";
  const horseId = scopeType === "horse" ? clean(payload?.horseId || payload?.scopeId) : "";
  const scopeId = clean(payload?.scopeId || horseId || "horse_kits");
  const rosterLinkFields = horseKitRosterLinkFields(tables);
  const horseLinkField = rosterLinkFields.commentHorse;
  const horseWriteField = rosterLinkFields.commentHorseFieldId || horseLinkField;
  const scopeLabel = clean(payload?.scopeLabel);
  const packWaveId = clean(payload?.packWaveId);
  const commentShortId = clean(payload?.commentShortId);
  const commentShort = commentShortId && tables.comment_shorts?.id
    ? await findOptionalRecordInConfiguredView(airtable, tables.comment_shorts, commentShortId)
    : null;
  const shortText = clean(commentShort?.fields?.display_label || commentShort?.fields?.comment_short);
  const comment = clean(payload?.comment || shortText);
  if (scopeType === "horse" && !horseId) throw new Error("missing_comment_horse_id");
  if (scopeType === "horse" && !horseLinkField) throw new Error("missing_comment_roster_link");
  if (!comment) throw new Error("comment_required");

  const before = commentId
    ? await findOptionalRecordInConfiguredView(airtable, tables.wec_commenting, commentId)
    : null;
  const fields = compactFields({
    event: before ? undefined : `horse_kit_comment:${scopeType}:${scopeId}:${Date.now()}`,
    event_type: before ? undefined : "horse_kit_comment",
    scope_type: scopeType,
    scope_id: scopeId,
    scope_label: scopeLabel || scopeId,
    comment_status: "active",
    comment,
    pack_wave: packWaveId ? [packWaveId] : [],
    created_at: before ? undefined : new Date().toISOString().slice(0, 10),
    created_by: "webflow",
    notes: clean(payload?.notes)
  });
  if (scopeType === "horse" && horseWriteField) fields[horseWriteField] = [horseId];
  const fieldNames = tableFieldNames(tables.wec_commenting);
  const allowedFields = fieldsAllowedBySchema(fields, fieldNames);
  const saved = commentId
    ? await patchAirtableRecord(airtable, tables.wec_commenting.id, commentId, allowedFields)
    : await createAirtableRecord(airtable, tables.wec_commenting.id, allowedFields);
  return { saved };
}

async function createHorseKitChange(airtable, tables, payload) {
  if (!tables.horse_kit_changes?.id) return null;
  const timestamp = Date.now();
  const subject = [
    payload.changeType,
    payload.packingKitRecord?.id || "change",
    timestamp
  ].filter(Boolean).join(":");
  const fields = compactFields({
    change: subject,
    change_type: payload.changeType,
    pak_kits: payload.pakKitIds || linkedIds(payload.packingKitRecord?.fields?.pak_kits),
    pak_kit_items: payload.pakItemIds || linkedIds(payload.packingKitRecord?.fields?.pak_kit_items),
    horse_packing_kit: payload.packingKitRecord?.id ? [payload.packingKitRecord.id] : payload.packingKitIds,
    old_value: stringifyChangeValue(payload.oldValue),
    new_value: stringifyChangeValue(payload.newValue),
    created_by: "webflow",
    notes: clean(payload.notes)
  });
  const fieldNames = tableFieldNames(tables.horse_kit_changes);
  return createAirtableRecord(airtable, tables.horse_kit_changes.id, fieldsAllowedBySchema(fields, fieldNames));
}

function normalizePakHorseRoster(record) {
  const fields = record.fields || {};
  const inactive = !!fields.inactive || !!fields.wec_not_going;
  const active = fields.active === true || !inactive;
  return {
    id: record.id,
    rosterId: record.id,
    writeHorseId: record.id,
    pakHorseId: stringField(fields.pak_horse_id),
    name: stringField(fields.display_horse_barn_name || fields.barn_name || fields.horse || fields.show_name),
    barnName: stringField(fields.display_horse_barn_name || fields.barn_name || fields.horse),
    showName: stringField(fields.show_name || fields.horse),
    active,
    waveOne: !!fields.wec_wave_1,
    waveTwo: !!fields.wec_wave_2,
    notGoing: !!fields.wec_not_going,
    sortOrder: numberField(fields.sort_order),
    weekIds: linkedIds(fields.wec_weeks),
    pakKitItemIds: linkedIds(fields.pak_kit_items),
    packWaveIds: linkedIds(fields.pack_waves),
    profileUrl: stringField(fields.profile_url || fields.entry_uri || fields.search_uri || fields.url || fields.link),
    countPakKitItems: wholeQuantityField(fields.count_pak_kit_items),
    notes: stringField(fields.notes)
  };
}

function normalizeKit(record) {
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
    kitItemIds: linkedIds(fields.pak_kit_items)
  };
}

function normalizeKitItem(record) {
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
    kitIds: linkedIds(fields.pak_kits),
    uom: stringField(fields.uom),
    status,
    active: status !== "inactive",
    sortOrder: numberField(fields.sort_order),
    notes: stringField(fields.notes)
  };
}

function normalizePackingKit(record, lookups = {}) {
  const fields = record.fields || {};
  const horseLinkField = lookups.horseLinkField || "";
  const horseIds = horseLinkField ? linkedIds(fields[horseLinkField]) : [];
  const kitIds = linkedIds(fields.pak_kits);
  const kitItemIds = linkedIds(fields.pak_kit_items);
  const packWaveIds = linkedIds(fields.pack_wave);
  const horse = lookups.horseById?.get(horseIds[0]);
  const kit = lookups.kitById?.get(kitIds[0]);
  const kitItem = lookups.kitItemById?.get(kitItemIds[0]);
  const neededState = slugify(fields.needed_state || "needed") || "needed";
  const packState = slugify(fields.pack_state || "not_packed") || "not_packed";
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
    neededState,
    packState,
    sortOrder: numberField(fields.sort_order || kitItem?.sortOrder),
    notes: stringField(fields.notes)
  };
}

function normalizeComment(record, lookups = {}) {
  const fields = record.fields || {};
  const horseLinkField = lookups.horseLinkField || "";
  return {
    id: record.id,
    label: stringField(fields.scope_label || fields.comment || record.id),
    comment: stringField(fields.comment),
    scopeLabel: stringField(fields.scope_label),
    horseIds: horseLinkField ? linkedIds(fields[horseLinkField]) : [],
    packWaveIds: linkedIds(fields.pack_wave),
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

function normalizeWave(record) {
  if (!record) return null;
  const fields = record.fields || {};
  const wave = stringField(fields.wave || fields.wave_key || fields.key || fields.Name || record.id);
  return {
    id: record.id,
    key: slugify(fields.wave_key || fields.key || wave),
    wave,
    active: !!fields.active,
    wecReportTitle: stringField(fields.wec_report_title),
    wecReportSubtitle: stringField(fields.wec_report_subtitle),
    sortOrder: numberField(fields.sort_order),
    includedWeekIds: linkedIds(fields.included_weeks),
    pakTabIds: linkedIds(fields.pak_tabs)
  };
}

function normalizePakTab(record) {
  const fields = record.fields || {};
  const tab = stringField(fields.tab || fields.tab_label || record.id);
  return {
    id: record.id,
    key: slugify(tab),
    label: stringField(fields.tab_label || tab),
    priority: numberField(fields.tab_priority),
    active: !!fields.active
  };
}

function normalizeWecLane(record) {
  const fields = record.fields || {};
  const lane = stringField(fields.lane || fields.lane_label || record.id);
  return {
    id: record.id,
    key: slugify(lane),
    label: stringField(fields.lane_label || lane),
    priority: numberField(fields.lane_priority),
    active: !!fields.active
  };
}

function normalizePakView(record) {
  const fields = record.fields || {};
  const view = stringField(fields.view || fields.view_label || record.id);
  return {
    id: record.id,
    key: slugify(view),
    label: stringField(fields.view_label || view),
    sortOrder: numberField(fields.sort_order),
    pakGroupIds: linkedIds(fields.pak_groups)
  };
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
  const wordToNumber = { one: "1", two: "2", three: "3", four: "4", five: "5" };
  const numberToWord = Object.fromEntries(Object.entries(wordToNumber).map(([word, numberValue]) => [numberValue, word]));
  for (const [word, numberValue] of Object.entries(wordToNumber)) {
    aliases.add(key.replace(new RegExp(`(^|_)${word}($|_)`, "g"), `$1${numberValue}$2`));
  }
  for (const [numberValue, word] of Object.entries(numberToWord)) {
    aliases.add(key.replace(new RegExp(`(^|_)${numberValue}($|_)`, "g"), `$1${word}$2`));
  }
  return Array.from(aliases);
}

function horseWaveState(horse) {
  if (horse?.notGoing) return "not_going";
  if (horse?.waveOne) return "wave_one";
  if (horse?.waveTwo) return "wave_two";
  return "unassigned";
}

function groupItemsByLinkedKit(items) {
  const grouped = new Map();
  for (const item of items || []) {
    for (const kitId of item.kitIds || []) {
      const list = grouped.get(kitId) || [];
      list.push(item);
      grouped.set(kitId, list);
    }
  }
  return grouped;
}

async function getBaseSchema(airtable) {
  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
    headers: airtableHeaders(airtable.token)
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`schema ${response.status}: ${JSON.stringify(result)}`);
  return result;
}

async function listAirtableRecords(airtable, table, view = "", options = {}) {
  const records = [];
  let offset = "";
  const fields = uniqueStrings(options.fields || []);
  do {
    const url = airtableUrl(airtable.baseId, table);
    url.searchParams.set("pageSize", "100");
    if (view) url.searchParams.set("view", view);
    for (const field of fields) url.searchParams.append("fields[]", field);
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

async function listOptionalViewRecords(airtable, tableOrConfig, view, options = {}) {
  const tableConfig = typeof tableOrConfig === "object" ? tableOrConfig : null;
  const tableId = tableConfig?.id || tableOrConfig;
  if (!tableId || !view) return [];
  try {
    return await listAirtableRecords(airtable, tableId, view, {
      fields: options.fields || tableConfig?.fields || []
    });
  } catch (error) {
    console.warn(`[wec-horse-kits] optional view skipped: ${tableId}/${view}`, error);
    return [];
  }
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
  if (!response.ok) throw new Error(`patch ${table}/${recordId} ${response.status}: ${JSON.stringify(result)}`);
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
  if (!response.ok) throw new Error(`create ${table} ${response.status}: ${JSON.stringify(result)}`);
  return {
    id: result.records?.[0]?.id || "",
    fields: result.records?.[0]?.fields || fields
  };
}

function buildRegistry(records) {
  const rows = records.map((record) => {
    const fields = record.fields || {};
    const name = clean(fields.table_name || fields.meta);
    return {
      id: record.id,
      name,
      tableApi: clean(fields.table_api),
      tableName: clean(fields.table_name || fields.meta),
      ignore: !!fields.ignore,
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
    const envKeys = ENV_TABLES[row.name] || {};
    const tableEnvKey = row.tableEnv || envKeys.table || "";
    const viewEnvKey = row.viewEnv || envKeys.view || "";
    const envTableId = clean(airtable.runtime[tableEnvKey]);
    const envView = clean(airtable.runtime[viewEnvKey]);
    const schemaTable = findSchemaTable(schema, row.name, envTableId || row.tableApi || row.tableName);
    tables[row.name] = {
      id: envTableId || row.tableApi || schemaTable?.id || row.tableName,
      name: row.name,
      view: envView || "",
      fields: row.fieldsAllowed || []
    };
  }
  for (const name of Object.keys(ENV_TABLES)) {
    if (tables[name]) continue;
    const tableEnvKey = ENV_TABLES[name].table || "";
    const viewEnvKey = ENV_TABLES[name].view || "";
    const envTableId = clean(airtable.runtime[tableEnvKey]);
    const schemaTable = findSchemaTable(schema, name, envTableId || name);
    if (!envTableId && !schemaTable?.id) continue;
    tables[name] = {
      id: envTableId || schemaTable?.id || "",
      name,
      view: clean(airtable.runtime[viewEnvKey]),
      fields: []
    };
  }
  return tables;
}

function findSchemaTable(schema, name, idOrName) {
  const target = clean(idOrName);
  return (schema.tables || []).find((table) => (
    table.name === name ||
    table.id === target ||
    table.name === target
  ));
}

function tableFieldNames(tableConfig) {
  return new Set((tableConfig?.schemaFields || []).map((field) => field.name));
}

function fieldsAllowedBySchema(fields, fieldNames) {
  if (!fieldNames?.size) return fields;
  return Object.fromEntries(Object.entries(fields).filter(([name]) => fieldNames.has(name)));
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

function wholeQuantityField(value) {
  const number = numberField(value);
  if (number <= 0) return 0;
  return Math.abs(number - Math.round(number)) < 0.000001
    ? Math.round(number)
    : Math.ceil(number - 0.000001);
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

function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => {
    if (value === null) return true;
    if (Array.isArray(value)) return value.length > 0;
    return value !== undefined && value !== "";
  }));
}

function stringifyChangeValue(value) {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

function comparePakTabs(a, b) {
  return compareNumber(a.priority, b.priority) || compareText(a.label, b.label) || compareText(a.id, b.id);
}

function compareWecLanes(a, b) {
  return compareNumber(a.priority, b.priority) || compareText(a.label, b.label) || compareText(a.id, b.id);
}

function comparePakViews(a, b) {
  const order = new Map([["all", 0], ["wave_one", 1], ["wave_two", 2], ["not_going", 3]]);
  return compareNumber(order.has(a.key) ? order.get(a.key) : 99, order.has(b.key) ? order.get(b.key) : 99) ||
    compareText(a.label || a.key, b.label || b.key);
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

function slugify(value) {
  return clean(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}
