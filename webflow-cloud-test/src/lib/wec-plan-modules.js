import { env } from "cloudflare:workers";

export const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

const PLAN_SPECS = {
  quantity: {
    planKey: "quantity",
    planLabel: "Quantity",
    planRecordId: "recBT7H5KeigIrAGK",
    sourceTable: "pak_byqtys",
    itemTable: "pak_byqty_items",
    linkTable: "pak_byqty_links",
    logTable: "pak_byqty_logs",
    laneTable: "pak_byqty_lanes",
    slotTable: "pak_byqty_slots",
    sourceKeyField: "byqty_key",
    itemKeyField: "byqty_item_key",
    linkKeyField: "byqty_link_key",
    logKeyField: "byqty_log_key",
    itemSourceLinkField: "pak_byqtys",
    linkItemField: "pak_byqty_items",
    logItemField: "pak_byqty_items",
    logLinkField: "pak_byqty_links",
    laneKeyField: "byqty_lane_key"
  },
  per_horse: {
    planKey: "per_horse",
    planLabel: "Per Horse",
    planRecordId: "recsrc6x7AdibwbMa",
    sourceTable: "pak_byhorses",
    itemTable: "pak_byhorse_items",
    linkTable: "pak_byhorse_links",
    logTable: "pak_byhorse_logs",
    laneTable: "pak_byhorse_lanes",
    slotTable: "pak_byhorse_slots",
    sourceKeyField: "byhorse_key",
    itemKeyField: "byhorse_item_key",
    linkKeyField: "byhorse_link_key",
    logKeyField: "byhorse_log_key",
    itemSourceLinkField: "pak_byhorses",
    linkItemField: "pak_byhorse_items",
    logItemField: "pak_byhorse_items",
    logLinkField: "pak_byhorse_links",
    laneKeyField: "byhorse_lane_key"
  },
  per_groom: {
    planKey: "per_groom",
    planLabel: "Per Groom",
    planRecordId: "recZLWe1SktapDZRZ",
    sourceTable: "pak_bygrooms",
    itemTable: "pak_bygroom_items",
    linkTable: "pak_bygroom_links",
    logTable: "pak_bygroom_logs",
    laneTable: "pak_bygroom_lanes",
    slotTable: "pak_bygroom_slots",
    sourceKeyField: "bygroom_key",
    itemKeyField: "bygroom_item_key",
    linkKeyField: "bygroom_link_key",
    logKeyField: "bygroom_log_key",
    itemSourceLinkField: "pak_bygrooms",
    linkItemField: "pak_bygroom_items",
    logItemField: "pak_bygroom_items",
    logLinkField: "pak_bygroom_links",
    laneKeyField: "bygroom_lane_key"
  }
};

const DEFAULT_META_TABLE = "tbllJywsOstkqT5yZ";

const READ_FIELDS = {
  wec_pack_waves: [
    "wave",
    "wave_key",
    "key",
    "active",
    "sort_order",
    "wec_report_title",
    "wec_report_subtitle",
    "horse_count",
    "count_horses_wave_one",
    "count_horses_wave_two",
    "groom_count_manual",
    "groom_ratio",
    "__groom_count_final",
    "groom_ratio_wave_one_dynamic",
    "pak_tabs"
  ],
  pak_tabs: ["tab", "tab_label", "tab_priority", "active", "pack_groups", "pak_views", "wec_pack_waves"],
  pak_views: ["view", "view_label", "pak_tabs", "pak_aggs", "pak_groups"],
  pak_aggs: ["aggregates", "tab_label", "tab_priority", "active_shade", "pak_views", "pak_groups", "active"],
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
    "add_filter",
    "add_search",
    "add_aggregates",
    "pak_aggs",
    "pak_views",
    "allow_add_new",
    "allow_inline_edit",
    "support_table"
  ],
  pak_horses_roster: [
    "display_horse_barn_name",
    "barn_name",
    "horse",
    "show_name",
    "active",
    "inactive",
    "wec_not_going",
    "wec_wave_1",
    "wec_wave_2",
    "pack_waves",
    "sort_order"
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
  pak_sessions: [
    "session_key",
    "session_state",
    "user_label",
    "device_id",
    "pack_wave",
    "current_lane",
    "current_list",
    "current_filter",
    "started_at",
    "last_seen_at",
    "ended_at",
    "notes"
  ]
};

const PLAN_FIELD_NAMES = [
  "display_label",
  "plan_key",
  "wec_list_plans",
  "wec_pack_waves",
  "pak_tabs",
  "active",
  "sort_order",
  "notes"
];

const ITEM_FIELD_NAMES = [
  "display_label",
  "source_item_key",
  "starting_quantity",
  "multiplier",
  "wildcard_key",
  "unit",
  "active",
  "sort_order",
  "notes"
];

const LINK_FIELD_NAMES = [
  "wec_pack_waves",
  "pak_sessions",
  "needed_current",
  "packed_current",
  "left_current",
  "item_state",
  "exception_state",
  "active",
  "notes"
];

const LOG_FIELD_NAMES = [
  "pak_sessions",
  "action_type",
  "debit_qty",
  "credit_qty",
  "quantity_before",
  "quantity_after",
  "needed_before",
  "needed_after",
  "exception_state",
  "reason",
  "notes",
  "created_by",
  "created_at"
];

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

export function normalizePlanKey(planKey) {
  const key = slugify(planKey).replace(/-/g, "_");
  if (key === "perhorse") return "per_horse";
  if (key === "pergroom") return "per_groom";
  if (key === "byqty" || key === "by_quantity") return "quantity";
  return key;
}

export function planSpec(planKey) {
  const normalized = normalizePlanKey(planKey);
  const spec = PLAN_SPECS[normalized];
  if (!spec) throw new Error(`unknown_plan:${planKey}`);
  return spec;
}

export async function planReport(airtable, requestUrl, planKey) {
  const spec = planSpec(planKey);
  const url = new URL(requestUrl);
  const packWaveId = clean(url.searchParams.get("packWaveId"));
  const packWaveKey = slugify(url.searchParams.get("packWaveKey") || url.searchParams.get("wave") || "wave_one");
  const selectedViewKey = slugify(url.searchParams.get("viewKey") || url.searchParams.get("view") || packWaveKey);
  const context = await loadContext(airtable);
  const tables = planTables(context, spec);
  assertPlanTables(tables, spec);

  const groupRecords = await listOptionalViewRecords(airtable, tables.pak_groups, spec.planKey, {
    fields: readFields(tables.pak_groups)
  });
  const groupStack = normalizePakGroupStack(groupRecords, { groupPrefix: "gp" });

  const [
    waveRecords,
    sourceRecords,
    itemRecords,
    linkRecords,
    logRecords,
    tabRecords,
    viewRecords,
    aggRecords,
    laneRecords,
    commentRecords,
    commentShortRecords,
    horseRosterRecords
  ] = await Promise.all([
    listRecords(airtable, tables.wec_pack_waves),
    listRecords(airtable, tables.source, tableViewOptionsForPlan(spec, selectedViewKey)),
    listRecords(airtable, tables.items, tableViewOptionsForPlan(spec, selectedViewKey)),
    listRecords(airtable, tables.links),
    listRecords(airtable, tables.logs),
    listOptionalRecords(airtable, tables.pak_tabs),
    listOptionalRecords(airtable, tables.pak_views),
    listOptionalRecords(airtable, tables.pak_aggs),
    listOptionalRecords(airtable, tables.lanes),
    listOptionalRecords(airtable, tables.wec_commenting),
    listOptionalRecords(airtable, tables.comment_shorts),
    spec.planKey === "per_horse"
      ? listRecords(airtable, tables.pak_horses_roster, { view: selectedViewKey || undefined })
      : Promise.resolve([])
  ]);

  const selectedWaveRecord = selectWave(waveRecords, packWaveId, packWaveKey);
  const selectedWave = normalizeWave(selectedWaveRecord);
  const horseRoster = horseRosterRecords.map(normalizeRosterHorse);
  const planContext = {
    selectedViewKey,
    horseCount: spec.planKey === "per_horse"
      ? horseCountForView(horseRoster, selectedViewKey, selectedWave)
      : spec.planKey === "per_groom"
        ? waveHorseCount(selectedWave)
        : 0
  };
  const sourceRows = sourceRecords
    .map((record) => normalizeSource(record, spec))
    .filter((row) => row.active && sourceMatchesSelectedView(row, selectedViewKey, selectedWave));
  const sourceById = new Map(sourceRows.map((row) => [row.id, row]));
  const activeSourceIds = new Set(sourceRows.map((row) => row.id));
  const links = linkRecords.map((record) => normalizeLink(record, spec));
  const logs = logRecords.map((record) => normalizeLog(record, spec)).sort(compareChangeLikeRows);
  const itemRows = itemRecords
    .map((record) => normalizeItem(record, spec, sourceById, selectedWave, links, planContext))
    .filter((row) => row.active && itemMatchesActiveSource(row, activeSourceIds))
    .sort(compareItems);

  const selectedWavePakTabIds = new Set(selectedWave?.pakTabIds || []);
  const primaryTabs = tabRecords
    .map(normalizePakTab)
    .filter((tab) => tab.active && (!selectedWavePakTabIds.size || selectedWavePakTabIds.has(tab.id)))
    .sort(comparePakTabs)
    .map((tab) => ({ id: tab.id, key: tab.key, label: tab.label, active: tab.active, sortOrder: tab.priority }));

  const secondaryControlPakViewIds = new Set((groupStack.activeRows.find((row) => row.renderKey === "secondary_controls")?.pakViewIds || []));
  const secondaryControls = viewRecords
    .map(normalizePakView)
    .filter((view) => secondaryControlPakViewIds.has(view.id))
    .sort(comparePakViews)
    .map((view) => ({ id: view.id, key: view.key, label: view.label, active: true, sortOrder: view.sortOrder }));

  const laneControls = laneRecords
    .map((record) => normalizeLane(record, spec))
    .filter((lane) => lane.active)
    .sort(compareLanes)
    .map((lane) => ({ id: lane.id, key: lane.key, label: lane.label, active: lane.active, sortOrder: lane.sortOrder }));

  const groupStackForClient = attachAggsToGroupStack(groupStack, aggRecords.map(normalizeAgg).filter((agg) => agg.active));
  const itemIds = new Set(itemRows.map((row) => row.id));
  const comments = commentRecords
    .map(normalizeComment)
    .filter((row) => row.active && commentMatchesPlan(row, spec, selectedWave, itemIds))
    .sort(compareChangeLikeRows);
  const commentShorts = commentShortRecords.map(normalizeCommentShort).filter((row) => row.active).sort(compareCommentShorts);

  return {
    ok: true,
    v: 1,
    plan: {
      key: spec.planKey,
      label: spec.planLabel,
      type: spec.planKey
    },
    source: {
      packWaveId: selectedWave?.id || "",
      packWaveKey: selectedWave?.key || packWaveKey,
      selectedViewKey,
      pakGroupsView: spec.planKey,
      horseSource: ["per_horse", "per_groom"].includes(spec.planKey) ? "pak_horses_roster" : "",
      horseCount: planContext.horseCount,
      tableFamily: {
        source: tables.source.name,
        items: tables.items.name,
        links: tables.links.name,
        logs: tables.logs.name,
        lanes: tables.lanes.name,
        slots: tables.slots.name
      },
      tables: {
        wec_pack_waves: tables.wec_pack_waves.id,
        pak_groups: tables.pak_groups.id,
        source: tables.source.id,
        items: tables.items.id,
        links: tables.links.id,
        logs: tables.logs.id,
        lanes: tables.lanes.id,
        slots: tables.slots.id,
        pak_tabs: tables.pak_tabs?.id || "",
        pak_views: tables.pak_views?.id || "",
        pak_aggs: tables.pak_aggs?.id || "",
        wec_commenting: tables.wec_commenting?.id || "",
        comment_shorts: tables.comment_shorts?.id || "",
        pak_horses_roster: tables.pak_horses_roster?.id || "",
        pak_sessions: tables.pak_sessions?.id || ""
      }
    },
    wave: selectedWave,
    counts: aggregateCounts(itemRows),
    horseRoster,
    sourceRows,
    items: itemRows,
    links,
    logs,
    comments,
    commentShorts,
    primaryTabs,
    secondaryControls,
    laneControls,
    groupStack: groupStackForClient
  };
}

export async function planActionReport(airtable, requestUrl, planKey, payload) {
  const spec = planSpec(planKey);
  const action = slugify(payload?.action);
  const context = await loadContext(airtable);
  const tables = planTables(context, spec);
  assertPlanTables(tables, spec);
  let result;
  if (action === "session_ping") {
    result = await ensureSession(airtable, tables, payload);
  } else if (action === "set_item_count") {
    result = await setItemCount(airtable, tables, spec, requestUrl, payload);
  } else if (action === "adjust_needed") {
    result = await adjustNeeded(airtable, tables, spec, requestUrl, payload);
  } else if (action === "save_comment") {
    result = await savePlanComment(airtable, tables, spec, requestUrl, payload);
  } else {
    return { ok: false, error: "unknown_plan_action", action };
  }
  return {
    ok: true,
    v: 1,
    plan: spec.planKey,
    action,
    result,
    state: await planReport(airtable, requestUrl, spec.planKey)
  };
}

export async function planPrintHtml(airtable, requestUrl, planKey) {
  const report = await planReport(airtable, requestUrl, planKey);
  const rows = report.items || [];
  const title = `${report.wave?.reportTitle || "WEC PACK"} - ${report.plan.label}`;
  const bodyRows = rows.map((row, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${escapeHtml(row.label)}</td>
      <td>${escapeHtml(row.sourceLabel || "")}</td>
      <td>${row.need}</td>
      <td>${row.packed}</td>
      <td>${row.left}</td>
    </tr>`).join("");
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>${escapeHtml(title)}</title>
  <style>
    body{font-family:Outfit,Arial,sans-serif;color:#111827;margin:24px}
    h1{font-size:24px;margin:0 0 4px}
    p{margin:0 0 18px;color:#68707a}
    table{width:100%;border-collapse:collapse;font-size:12px}
    th,td{border:1px solid #d9dde3;padding:8px;text-align:left}
    th:nth-child(n+4),td:nth-child(n+4){text-align:center}
    th{background:#f3f4f6;text-transform:uppercase}
  </style>
</head>
<body>
  <h1>${escapeHtml(report.plan.label)}</h1>
  <p>${escapeHtml(report.wave?.label || report.source.packWaveKey || "")}</p>
  <table>
    <thead><tr><th>#</th><th>Item</th><th>Source</th><th>Need</th><th>Packed</th><th>Left</th></tr></thead>
    <tbody>${bodyRows || `<tr><td colspan="6">No items.</td></tr>`}</tbody>
  </table>
  <script>window.addEventListener("load",function(){setTimeout(function(){window.print();},150);});</script>
</body>
</html>`;
}

async function setItemCount(airtable, tables, spec, requestUrl, payload) {
  const itemId = clean(payload?.itemId);
  const delta = numberField(payload?.delta);
  const packedValue = payload?.packed === undefined ? null : wholeQuantityField(payload.packed);
  const exceptionState = slugify(payload?.exceptionState);
  if (!itemId) throw new Error("missing_item_id");
  if (!delta && packedValue === null && !exceptionState) throw new Error("missing_item_count_action");
  const current = await itemActionContext(airtable, tables, spec, requestUrl, itemId);
  const session = await ensureSession(airtable, tables, payload);
  const nextPacked = exceptionState === "packed_max"
    ? current.need
    : clamp(packedValue === null ? current.packed + delta : packedValue, 0, current.need);
  const nextException = exceptionState || current.exceptionState || "";
  const nextLeft = Math.max(0, current.need - nextPacked);
  const itemState = nextPacked >= current.need && current.need > 0 ? "packed" : nextPacked > 0 ? "partial" : "open";
  const linkFields = linkFieldsFor(spec, current, {
    packed: nextPacked,
    need: current.need,
    left: nextLeft,
    itemState,
    exceptionState: nextException,
    sessionId: session?.id || "",
    notes: clean(payload?.notes)
  });
  const saved = current.link
    ? await patchAirtableRecord(airtable, tables.links.id, current.link.id, fieldsAllowedBySchema(linkFields, tableFieldNames(tables.links)))
    : await createAirtableRecord(airtable, tables.links.id, fieldsAllowedBySchema(linkFields, tableFieldNames(tables.links)));
  const quantityDelta = nextPacked - current.packed;
  const log = await createPlanLog(airtable, tables, spec, {
    actionType: exceptionState ? `exception_${exceptionState}` : "packed_changed",
    linkId: saved.id,
    itemId,
    sessionId: session?.id || "",
    debitQty: quantityDelta > 0 ? quantityDelta : 0,
    creditQty: quantityDelta < 0 ? Math.abs(quantityDelta) : 0,
    quantityBefore: current.packed,
    quantityAfter: nextPacked,
    neededBefore: current.need,
    neededAfter: current.need,
    exceptionState: nextException,
    reason: clean(payload?.reason),
    notes: clean(payload?.notes)
  });
  return { saved, log };
}

async function adjustNeeded(airtable, tables, spec, requestUrl, payload) {
  if (spec.planKey !== "quantity") throw new Error("adjust_needed_only_for_quantity");
  const itemId = clean(payload?.itemId);
  const nextNeed = wholeQuantityField(payload?.needed);
  if (!itemId) throw new Error("missing_item_id");
  const current = await itemActionContext(airtable, tables, spec, requestUrl, itemId);
  const session = await ensureSession(airtable, tables, payload);
  const nextPacked = Math.min(current.packed, nextNeed);
  const nextLeft = Math.max(0, nextNeed - nextPacked);
  const fields = linkFieldsFor(spec, current, {
    packed: nextPacked,
    need: nextNeed,
    left: nextLeft,
    itemState: nextPacked >= nextNeed && nextNeed > 0 ? "packed" : nextPacked > 0 ? "partial" : "open",
    exceptionState: current.exceptionState || "",
    sessionId: session?.id || "",
    notes: clean(payload?.notes)
  });
  const saved = current.link
    ? await patchAirtableRecord(airtable, tables.links.id, current.link.id, fieldsAllowedBySchema(fields, tableFieldNames(tables.links)))
    : await createAirtableRecord(airtable, tables.links.id, fieldsAllowedBySchema(fields, tableFieldNames(tables.links)));
  const log = await createPlanLog(airtable, tables, spec, {
    actionType: "needed_adjusted",
    linkId: saved.id,
    itemId,
    sessionId: session?.id || "",
    quantityBefore: current.packed,
    quantityAfter: nextPacked,
    neededBefore: current.need,
    neededAfter: nextNeed,
    reason: clean(payload?.reason),
    notes: clean(payload?.notes)
  });
  return { saved, log };
}

async function savePlanComment(airtable, tables, spec, requestUrl, payload) {
  if (!tables.wec_commenting?.id) throw new Error("commenting_table_not_configured");
  const url = new URL(requestUrl);
  const wave = await selectedWaveFromUrl(airtable, tables, url);
  const session = await ensureSession(airtable, tables, payload);
  const scopeType = slugify(payload?.scopeType || "plan") || "plan";
  const scopeId = clean(payload?.scopeId || spec.planKey);
  const scopeLabel = clean(payload?.scopeLabel || spec.planLabel);
  const comment = clean(payload?.comment);
  if (!comment) throw new Error("comment_required");
  const fields = compactFields({
    event: `${spec.planKey}_comment:${scopeType}:${scopeId}:${Date.now()}`,
    event_type: `${spec.planKey}_comment`,
    scope_type: scopeType,
    scope_id: scopeId,
    scope_label: scopeLabel,
    comment_status: "active",
    comment,
    pack_wave: wave?.id ? [wave.id] : [],
    created_at: new Date().toISOString(),
    created_by: "webflow",
    notes: session?.id ? `session:${session.id}` : ""
  });
  return {
    saved: await createAirtableRecord(airtable, tables.wec_commenting.id, fieldsAllowedBySchema(fields, tableFieldNames(tables.wec_commenting)))
  };
}

async function itemActionContext(airtable, tables, spec, requestUrl, itemId) {
  const url = new URL(requestUrl);
  const wave = await selectedWaveFromUrl(airtable, tables, url);
  const selectedViewKey = slugify(url.searchParams.get("viewKey") || url.searchParams.get("view") || url.searchParams.get("packWaveKey") || "wave_one");
  const [item, links, horseRosterRecords] = await Promise.all([
    findRecord(airtable, tables.items, itemId),
    listRecords(airtable, tables.links),
    spec.planKey === "per_horse"
      ? listRecords(airtable, tables.pak_horses_roster, { view: selectedViewKey || undefined })
      : Promise.resolve([])
  ]);
  const planContext = {
    selectedViewKey,
    horseCount: spec.planKey === "per_horse"
      ? horseCountForView(horseRosterRecords.map(normalizeRosterHorse), selectedViewKey, wave)
      : 0
  };
  const link = links.map((record) => normalizeLink(record, spec)).find((row) =>
    row.itemIds.includes(itemId) && (!wave?.id || !row.waveIds.length || row.waveIds.includes(wave.id))
  ) || null;
  const normalized = normalizeItem(item, spec, new Map(), wave, link ? [link] : [], planContext);
  return {
    item,
    link,
    wave,
    need: normalized.need,
    packed: normalized.packed,
    left: normalized.left,
    exceptionState: normalized.exceptionState,
    horseCount: planContext.horseCount,
    linkKey: `${spec.planKey}:${wave?.id || "all"}:${itemId}`
  };
}

async function selectedWaveFromUrl(airtable, tables, url) {
  const packWaveId = clean(url.searchParams.get("packWaveId"));
  const packWaveKey = slugify(url.searchParams.get("packWaveKey") || url.searchParams.get("wave") || "wave_one");
  const waves = await listRecords(airtable, tables.wec_pack_waves);
  return normalizeWave(selectWave(waves, packWaveId, packWaveKey));
}

function linkFieldsFor(spec, current, next) {
  const fields = {
    [spec.linkKeyField]: current.linkKey,
    [spec.linkItemField]: [current.item.id],
    wec_pack_waves: current.wave?.id ? [current.wave.id] : [],
    pak_sessions: next.sessionId ? [next.sessionId] : [],
    needed_current: next.need,
    packed_current: next.packed,
    left_current: next.left,
    item_state: next.itemState,
    exception_state: next.exceptionState,
    active: true,
    notes: next.notes
  };
  return compactFields(fields);
}

async function createPlanLog(airtable, tables, spec, payload) {
  const fields = compactFields({
    [spec.logKeyField]: `${payload.actionType}:${payload.itemId || "item"}:${Date.now()}`,
    [spec.logLinkField]: payload.linkId ? [payload.linkId] : [],
    [spec.logItemField]: payload.itemId ? [payload.itemId] : [],
    pak_sessions: payload.sessionId ? [payload.sessionId] : [],
    action_type: payload.actionType,
    debit_qty: payload.debitQty || 0,
    credit_qty: payload.creditQty || 0,
    quantity_before: payload.quantityBefore,
    quantity_after: payload.quantityAfter,
    needed_before: payload.neededBefore,
    needed_after: payload.neededAfter,
    exception_state: payload.exceptionState,
    reason: payload.reason,
    notes: payload.notes,
    created_by: "webflow",
    created_at: new Date().toISOString()
  });
  return createAirtableRecord(airtable, tables.logs.id, fieldsAllowedBySchema(fields, tableFieldNames(tables.logs)));
}

async function ensureSession(airtable, tables, payload) {
  if (!tables.pak_sessions?.id) return null;
  const sessionKey = clean(payload?.sessionKey);
  if (!sessionKey) return null;
  const records = await listRecords(airtable, tables.pak_sessions);
  const existing = records.find((record) => clean(record.fields?.session_key) === sessionKey);
  const now = new Date().toISOString();
  const fields = compactFields({
    session_key: sessionKey,
    session_state: "active",
    user_label: clean(payload?.userLabel),
    device_id: clean(payload?.deviceId),
    pack_wave: clean(payload?.packWaveId) ? [clean(payload.packWaveId)] : [],
    current_lane: clean(payload?.currentLane),
    current_list: clean(payload?.currentList),
    current_filter: clean(payload?.currentFilter),
    started_at: existing ? undefined : now,
    last_seen_at: now,
    notes: clean(payload?.notes)
  });
  const allowed = fieldsAllowedBySchema(fields, tableFieldNames(tables.pak_sessions));
  return existing
    ? patchAirtableRecord(airtable, tables.pak_sessions.id, existing.id, allowed)
    : createAirtableRecord(airtable, tables.pak_sessions.id, allowed);
}

async function loadContext(airtable) {
  const schema = await getBaseSchema(airtable);
  const tables = Object.fromEntries((schema.tables || []).map((table) => [table.name, {
    id: table.id,
    name: table.name,
    schemaFields: table.fields || []
  }]));
  return { schema, tables };
}

function planTables(context, spec) {
  return {
    wec_pack_waves: physicalTableConfig(context, "wec_pack_waves"),
    pak_groups: physicalTableConfig(context, "pak_groups"),
    pak_tabs: physicalTableConfig(context, "pak_tabs", true),
    pak_views: physicalTableConfig(context, "pak_views", true),
    pak_aggs: physicalTableConfig(context, "pak_aggs", true),
    pak_sessions: physicalTableConfig(context, "pak_sessions", true),
    wec_commenting: physicalTableConfig(context, "wec_commenting", true),
    comment_shorts: physicalTableConfig(context, "comment_shorts", true),
    pak_horses_roster: physicalTableConfig(context, "pak_horses_roster", spec.planKey !== "per_horse"),
    source: physicalTableConfig(context, spec.sourceTable),
    items: physicalTableConfig(context, spec.itemTable),
    links: physicalTableConfig(context, spec.linkTable),
    logs: physicalTableConfig(context, spec.logTable),
    lanes: physicalTableConfig(context, spec.laneTable),
    slots: physicalTableConfig(context, spec.slotTable, true)
  };
}

function assertPlanTables(tables, spec) {
  for (const key of ["wec_pack_waves", "pak_groups", "source", "items", "links", "logs", "lanes"]) {
    if (!tables[key]?.id) throw new Error(`${spec.planKey}_missing_table:${key}`);
  }
  if (spec.planKey === "per_horse" && !tables.pak_horses_roster?.id) {
    throw new Error(`${spec.planKey}_missing_table:pak_horses_roster`);
  }
}

function tableViewOptionsForPlan(spec, selectedViewKey) {
  if (spec.planKey === "quantity") return {};
  if (spec.planKey === "per_horse") return {};
  if (spec.planKey === "per_groom") return {};
  return { view: selectedViewKey || undefined };
}

function physicalTableConfig(context, name, optional = false) {
  const schemaTable = context.tables?.[name];
  if (!schemaTable?.id && !optional) throw new Error(`missing_table:${name}`);
  if (!schemaTable?.id) return null;
  return schemaTable;
}

function readFields(tableConfig, extraFields = []) {
  if (!tableConfig) return [];
  const planSpecForTable = planSpecByTableName(tableConfig.name);
  const specFields = [];
  if (planSpecForTable?.sourceTable === tableConfig.name) specFields.push(planSpecForTable.sourceKeyField, ...PLAN_FIELD_NAMES);
  if (planSpecForTable?.itemTable === tableConfig.name) specFields.push(planSpecForTable.itemKeyField, planSpecForTable.itemSourceLinkField, ...ITEM_FIELD_NAMES);
  if (planSpecForTable?.linkTable === tableConfig.name) specFields.push(planSpecForTable.linkKeyField, planSpecForTable.linkItemField, ...LINK_FIELD_NAMES);
  if (planSpecForTable?.logTable === tableConfig.name) specFields.push(planSpecForTable.logKeyField, planSpecForTable.logLinkField, planSpecForTable.logItemField, ...LOG_FIELD_NAMES);
  if (planSpecForTable?.laneTable === tableConfig.name) specFields.push(planSpecForTable.laneKeyField, "display_label", "plan_key", "active", "sort_order", "target_bg", "agg_bg", "active_bg", "active_text", "notes");
  const defaults = READ_FIELDS[tableConfig.name] || [];
  const fields = uniqueStrings([...defaults, ...specFields, ...extraFields]);
  const schemaKeys = new Set((tableConfig.schemaFields || []).flatMap((field) => [field.name, field.id].filter(Boolean)));
  return schemaKeys.size ? fields.filter((field) => schemaKeys.has(field)) : fields;
}

function planSpecByTableName(name) {
  return Object.values(PLAN_SPECS).find((spec) => [
    spec.sourceTable,
    spec.itemTable,
    spec.linkTable,
    spec.logTable,
    spec.laneTable,
    spec.slotTable
  ].includes(name));
}

async function listRecords(airtable, tableConfig, options = {}) {
  if (!tableConfig?.id) throw new Error(`missing_table_config:${options.name || "unknown"}`);
  return listAirtableRecords(airtable, tableConfig.id, options.view ?? tableConfig.view, {
    fields: readFields(tableConfig, options.extraFields || [])
  });
}

async function listOptionalRecords(airtable, tableConfig, options = {}) {
  if (!tableConfig?.id) return [];
  try {
    return await listRecords(airtable, tableConfig, options);
  } catch (error) {
    console.warn(`[wec-plan-modules] optional table skipped: ${tableConfig.name || tableConfig.id}`, error);
    return [];
  }
}

async function listOptionalViewRecords(airtable, tableConfig, view, options = {}) {
  if (!tableConfig?.id || !view) return [];
  try {
    return await listAirtableRecords(airtable, tableConfig.id, view, {
      fields: options.fields || readFields(tableConfig)
    });
  } catch (error) {
    console.warn(`[wec-plan-modules] optional view skipped: ${tableConfig.id}/${view}`, error);
    return [];
  }
}

async function findRecord(airtable, tableConfig, recordId) {
  const records = await listRecords(airtable, tableConfig);
  const record = records.find((row) => row.id === recordId);
  if (!record) throw new Error(`${tableConfig.name}_record_not_found:${recordId}`);
  return record;
}

function normalizeSource(record, spec) {
  const fields = record.fields || {};
  const label = stringField(fields.display_label || fields[spec.sourceKeyField] || record.id);
  return {
    id: record.id,
    key: stringField(fields[spec.sourceKeyField] || record.id),
    label,
    planKey: stringField(fields.plan_key),
    waveIds: linkedIds(fields.wec_pack_waves),
    tabIds: linkedIds(fields.pak_tabs),
    active: fields.active !== false,
    sortOrder: numberField(fields.sort_order),
    notes: stringField(fields.notes)
  };
}

function normalizeItem(record, spec, sourceById, wave, links = [], planContext = {}) {
  const fields = record.fields || {};
  const label = stringField(fields.display_label || fields[spec.itemKeyField] || record.id);
  const sourceIds = linkedIds(fields[spec.itemSourceLinkField]);
  const link = findItemLink(links, record.id, wave?.id);
  const computedNeed = computedNeeded(spec, fields, wave, planContext);
  const need = spec.planKey === "quantity"
    ? wholeQuantityField(link?.neededCurrent ?? computedNeed)
    : wholeQuantityField(computedNeed);
  const packed = wholeQuantityField(link?.packedCurrent);
  const left = Math.max(0, need - packed);
  const sourceLabels = sourceIds.map((id) => sourceById.get(id)?.label).filter(Boolean);
  return {
    id: record.id,
    key: stringField(fields[spec.itemKeyField] || record.id),
    label,
    sourceIds,
    sourceLabel: sourceLabels.join(", "),
    sourceItemKey: stringField(fields.source_item_key),
    startingQuantity: wholeQuantityField(fields.starting_quantity),
    multiplier: numberField(fields.multiplier),
    wildcardKey: stringField(fields.wildcard_key),
    unit: stringField(fields.unit),
    active: fields.active !== false,
    sortOrder: numberField(fields.sort_order),
    notes: stringField(fields.notes),
    linkId: link?.id || "",
    need,
    packed,
    left,
    itemState: stringField(link?.itemState || (packed >= need && need > 0 ? "packed" : packed > 0 ? "partial" : "open")),
    exceptionState: stringField(link?.exceptionState),
    waveId: wave?.id || ""
  };
}

function sourceMatchesSelectedView(row, selectedViewKey, wave) {
  const viewKey = slugify(selectedViewKey || "");
  if (!row.waveIds.length) return true;
  if (viewKey === "all" || viewKey === "not_going") return true;
  if (!wave?.id) return true;
  return row.waveIds.includes(wave.id);
}

function itemMatchesActiveSource(row, activeSourceIds) {
  if (!row.sourceIds.length) return true;
  return row.sourceIds.some((id) => activeSourceIds.has(id));
}

function commentMatchesPlan(row, spec, wave, itemIds) {
  if (row.packWaveIds.length && wave?.id && !row.packWaveIds.includes(wave.id)) return false;
  if (row.eventType && !row.eventType.startsWith(`${spec.planKey}_comment`)) return false;
  if (row.scopeType === "item") return itemIds.has(row.scopeId);
  if (row.scopeType === "plan") return row.scopeId === spec.planKey;
  return false;
}

function computedNeeded(spec, fields, wave, planContext = {}) {
  if (spec.planKey === "quantity") return wholeQuantityField(fields.starting_quantity);
  const multiplier = numberField(fields.multiplier);
  if (!multiplier) return 0;
  if (spec.planKey === "per_horse") return Math.max(0, Math.round(multiplier * waveHorseCount(wave, planContext)));
  if (spec.planKey === "per_groom") return Math.max(0, Math.round(multiplier * waveGroomCount(wave)));
  return 0;
}

function waveHorseCount(wave, planContext = {}) {
  if (Number.isFinite(planContext.horseCount)) return wholeQuantityField(planContext.horseCount);
  if (!wave) return 0;
  if (wave.key === "wave_one") return wholeQuantityField(wave.countHorsesWaveOne ?? wave.horseCount);
  if (wave.key === "wave_two") return wholeQuantityField(wave.countHorsesWaveTwo ?? wave.horseCount);
  return wholeQuantityField(wave.horseCount);
}

function waveGroomCount(wave) {
  if (!wave) return 0;
  const finalCount = numberField(wave.groomCountFinal);
  if (finalCount) return finalCount;
  const manual = numberField(wave.groomCountManual);
  if (manual) return manual;
  const horseCount = waveHorseCount(wave);
  const ratio = numberField(wave.groomRatio);
  return ratio > 0 ? Math.ceil(horseCount / ratio) : 0;
}

function normalizeLink(record, spec) {
  const fields = record.fields || {};
  return {
    id: record.id,
    key: stringField(fields[spec.linkKeyField] || record.id),
    itemIds: linkedIds(fields[spec.linkItemField]),
    waveIds: linkedIds(fields.wec_pack_waves),
    sessionIds: linkedIds(fields.pak_sessions),
    neededCurrent: fields.needed_current,
    packedCurrent: fields.packed_current,
    leftCurrent: fields.left_current,
    itemState: stringField(fields.item_state),
    exceptionState: stringField(fields.exception_state),
    active: fields.active !== false,
    notes: stringField(fields.notes)
  };
}

function normalizeLog(record, spec) {
  const fields = record.fields || {};
  return {
    id: record.id,
    key: stringField(fields[spec.logKeyField] || record.id),
    linkIds: linkedIds(fields[spec.logLinkField]),
    itemIds: linkedIds(fields[spec.logItemField]),
    actionType: stringField(fields.action_type),
    debitQty: numberField(fields.debit_qty),
    creditQty: numberField(fields.credit_qty),
    quantityBefore: numberField(fields.quantity_before),
    quantityAfter: numberField(fields.quantity_after),
    neededBefore: numberField(fields.needed_before),
    neededAfter: numberField(fields.needed_after),
    exceptionState: stringField(fields.exception_state),
    reason: stringField(fields.reason),
    notes: stringField(fields.notes),
    createdBy: stringField(fields.created_by),
    createdAt: stringField(fields.created_at || record.createdTime)
  };
}

function normalizeRosterHorse(record) {
  const fields = record.fields || {};
  const inactive = !!fields.inactive || !!fields.wec_not_going;
  return {
    id: record.id,
    label: stringField(fields.display_horse_barn_name || fields.barn_name || fields.horse || fields.show_name || record.id),
    active: fields.active === true || !inactive,
    inactive,
    notGoing: !!fields.wec_not_going,
    waveOne: !!fields.wec_wave_1,
    waveTwo: !!fields.wec_wave_2,
    packWaveIds: linkedIds(fields.pack_waves),
    sortOrder: numberField(fields.sort_order)
  };
}

function horseCountForView(horses, selectedViewKey, wave) {
  const key = slugify(selectedViewKey || wave?.key || "");
  if (key === "not_going") return 0;
  return (horses || []).filter((horse) => horseCountsForPacking(horse, key, wave)).length;
}

function horseCountsForPacking(horse, selectedViewKey, wave) {
  if (!horse?.active || horse.notGoing) return false;
  if (selectedViewKey === "wave_one" && horse.waveOne === false && horse.waveTwo === true) return false;
  if (selectedViewKey === "wave_two" && horse.waveTwo === false && horse.waveOne === true) return false;
  if (wave?.id && horse.packWaveIds.length && !horse.packWaveIds.includes(wave.id)) return false;
  return true;
}

function findItemLink(links, itemId, waveId) {
  return (links || []).find((link) =>
    link.active &&
    link.itemIds.includes(itemId) &&
    (!waveId || !link.waveIds.length || link.waveIds.includes(waveId))
  ) || null;
}

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
  const physicalTableName = clean(fields.physical_table) || tableName;
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
    addSearch: !!fields.add_search,
    addFilter: !!fields.add_filter,
    addAggregates: !!fields.add_aggregates,
    supportTable: !!fields.support_table,
    pakViewIds: linkedIds(fields.pak_views),
    pakAggIds: linkedIds(fields.pak_aggs),
    aggs: []
  };
}

function attachAggsToGroupStack(groupStack, aggs) {
  const byGroup = new Map();
  for (const agg of aggs) {
    for (const groupId of agg.pakGroupIds || []) {
      const list = byGroup.get(groupId) || [];
      list.push(agg);
      byGroup.set(groupId, list);
    }
  }
  const attach = (row) => ({
    id: row.id,
    renderKey: row.renderKey,
    displayLabel: row.displayLabel,
    componentKey: row.componentKey,
    role: row.role,
    sortOrder: row.sortOrder,
    pakViewIds: row.pakViewIds,
    aggs: (byGroup.get(row.id) || []).sort(compareAggs)
  });
  return {
    activeRows: (groupStack.activeRows || []).map(attach),
    hiddenRows: (groupStack.hiddenRows || []).map(attach)
  };
}

function normalizeAgg(record) {
  const fields = record.fields || {};
  const key = slugify(fields.aggregates || fields.tab_label || record.id);
  return {
    id: record.id,
    key,
    label: stringField(fields.tab_label || fields.aggregates || key),
    sortOrder: numberField(fields.tab_priority),
    shade: slugify(fields.active_shade || ""),
    active: fields.active !== false,
    pakViewIds: linkedIds(fields.pak_views),
    pakGroupIds: linkedIds(fields.pak_groups)
  };
}

function normalizeWave(record) {
  if (!record) return null;
  const fields = record.fields || {};
  const label = stringField(fields.wave || fields.wave_key || fields.key || record.id);
  return {
    id: record.id,
    key: slugify(fields.wave_key || fields.key || label),
    label,
    active: !!fields.active,
    reportTitle: stringField(fields.wec_report_title),
    reportSubtitle: stringField(fields.wec_report_subtitle),
    sortOrder: numberField(fields.sort_order),
    horseCount: wholeQuantityField(fields.horse_count),
    countHorsesWaveOne: wholeQuantityField(fields.count_horses_wave_one),
    countHorsesWaveTwo: wholeQuantityField(fields.count_horses_wave_two),
    groomCountManual: numberField(fields.groom_count_manual),
    groomRatio: numberField(fields.groom_ratio),
    groomCountFinal: numberField(fields.__groom_count_final),
    groomRatioWaveOneDynamic: numberField(fields.groom_ratio_wave_one_dynamic),
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

function normalizeLane(record, spec) {
  const fields = record.fields || {};
  const key = stringField(fields[spec.laneKeyField] || fields.display_label || record.id);
  return {
    id: record.id,
    key: slugify(key.replace(`${spec.planKey}_`, "")),
    label: stringField(fields.display_label || key),
    planKey: stringField(fields.plan_key),
    active: !!fields.active,
    sortOrder: numberField(fields.sort_order),
    targetBg: stringField(fields.target_bg),
    aggBg: stringField(fields.agg_bg),
    activeBg: stringField(fields.active_bg),
    activeText: stringField(fields.active_text)
  };
}

function normalizeComment(record) {
  const fields = record.fields || {};
  const status = slugify(fields.comment_status || "active") || "active";
  return {
    id: record.id,
    label: stringField(fields.scope_label || fields.comment || record.id),
    eventType: slugify(fields.event_type),
    comment: stringField(fields.comment),
    scopeType: slugify(fields.scope_type || "plan"),
    scopeId: stringField(fields.scope_id),
    scopeLabel: stringField(fields.scope_label),
    packWaveIds: linkedIds(fields.pack_wave),
    active: status !== "inactive",
    createdAt: stringField(fields.created_at || record.createdTime)
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
    scopeType: slugify(fields.scope_type || "plan"),
    active: status !== "inactive",
    sortOrder: numberField(fields.sort_order)
  };
}

function aggregateCounts(items) {
  return items.reduce((sum, item) => {
    sum.items += 1;
    sum.need += item.need;
    sum.packed += item.packed;
    sum.left += item.left;
    if (item.packed > 0 || item.exceptionState) sum.touched += 1;
    return sum;
  }, { items: 0, need: 0, packed: 0, left: 0, touched: 0 });
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
  return [record.id, fields.wave_key, fields.key, fields.wave, fields.Name].flatMap(waveKeyAliases).filter(Boolean);
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
    await sleep(retryDelays[attempt] + Math.floor(Math.random() * 250));
  }
  throw lastError;
}

async function patchAirtableRecord(airtable, table, recordId, fields) {
  const response = await fetch(`${airtableUrl(airtable.baseId, table)}/${encodeURIComponent(recordId)}`, {
    method: "PATCH",
    headers: { ...airtableHeaders(airtable.token), "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`patch ${table}/${recordId} ${response.status}: ${JSON.stringify(result)}`);
  return { id: result.id || recordId, fields: result.fields || fields };
}

async function createAirtableRecord(airtable, table, fields) {
  const response = await fetch(airtableUrl(airtable.baseId, table), {
    method: "POST",
    headers: { ...airtableHeaders(airtable.token), "Content-Type": "application/json" },
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`create ${table} ${response.status}: ${JSON.stringify(result)}`);
  return { id: result.records?.[0]?.id || "", fields: result.records?.[0]?.fields || fields };
}

function airtableUrl(baseId, table) {
  return new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`);
}

function airtableHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

function tableFieldNames(tableConfig) {
  return new Set((tableConfig?.schemaFields || []).map((field) => field.name));
}

function fieldsAllowedBySchema(fields, fieldNames) {
  if (!fieldNames?.size) return fields;
  return Object.fromEntries(Object.entries(fields || {}).filter(([key, value]) => fieldNames.has(key) && value !== undefined));
}

function compactFields(fields) {
  return Object.fromEntries(Object.entries(fields || {}).filter(([, value]) => {
    if (value === undefined || value === null) return false;
    if (Array.isArray(value)) return value.length > 0;
    return true;
  }));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function compareItems(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label);
}

function comparePakTabs(a, b) {
  return compareNumber(a.priority, b.priority) || compareText(a.label, b.label);
}

function comparePakViews(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label);
}

function compareLanes(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label);
}

function compareAggs(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label);
}

function compareCommentShorts(a, b) {
  return compareNumber(a.sortOrder, b.sortOrder) || compareText(a.label, b.label);
}

function compareChangeLikeRows(a, b) {
  return compareText(b.createdAt || b.createdTime || "", a.createdAt || a.createdTime || "");
}

function compareText(a, b) {
  return String(a || "").localeCompare(String(b || ""), undefined, { numeric: true, sensitivity: "base" });
}

function compareNumber(a, b) {
  return (Number(a) || 0) - (Number(b) || 0);
}

function clean(value) {
  return String(value ?? "").trim();
}

function stringField(value) {
  if (Array.isArray(value)) return stringField(value[0]);
  if (value && typeof value === "object") return stringField(value.name || value.text || value.id);
  return clean(value);
}

function numberField(value) {
  if (Array.isArray(value)) return numberField(value[0]);
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function wholeQuantityField(value) {
  return Math.max(0, Math.round(numberField(value)));
}

function linkedIds(value) {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value.map((item) => typeof item === "string" ? item : item?.id).filter(Boolean);
  }
  if (typeof value === "string") return [value].filter(Boolean);
  return value.id ? [value.id] : [];
}

function uniqueStrings(values) {
  return [...new Set((values || []).map((value) => clean(value)).filter(Boolean))];
}

function slugify(value) {
  return clean(value).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, wholeQuantityField(value)));
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
}
