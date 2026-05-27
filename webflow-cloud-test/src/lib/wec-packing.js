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
  "wec_list_plans"
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
  }
};

const DEFAULT_META_TABLE = "tbllJywsOstkqT5yZ";
const DEFAULT_SOURCE_VIEWS = {
  wec_pack_lists: "Grid view",
  wec_pack_items: "master",
  wec_list_plans: "Grid view"
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
  const [waves, packLists, sourcePackItems, worksheetItems, worksheetHorses, horses, listPlans] = await Promise.all([
    listAirtableRecords(airtable, tables.wec_pack_waves.id, tables.wec_pack_waves.view),
    listAirtableRecords(airtable, tables.wec_pack_lists.id, tables.wec_pack_lists.view),
    listAirtableRecords(airtable, tables.wec_pack_items.id, tables.wec_pack_items.view),
    listAirtableRecords(airtable, tables.wec_packing_items.id, tables.wec_packing_items.view),
    listAirtableRecords(airtable, tables.wec_packing_item_horses.id, tables.wec_packing_item_horses.view),
    listAirtableRecords(airtable, tables.wec_horses.id, tables.wec_horses.view),
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
  const waveHorses = normalizedHorses.filter((horse) => isHorseInWave(horse, waveBase));
  const waveHorseIds = new Set(waveHorses.map((horse) => horse.id));
  const normalizedWave = withEffectiveWaveCounts(waveBase, waveHorses);
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
      normalizePackingItem(record, horsesByItem.get(record.id) || [], listPlanLookup),
      packListLookup,
      sourcePackItemLookup,
      normalizedWave,
      waveHorseIds
    ))
    .sort(compareWorksheetRows);
  const lists = buildListSummaries(items, normalizedPackLists);
  const tabGroups = buildTabSummaries(lists);

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
        packingEvents: tables.wec_packing_events.id
      }
    },
    wave: normalizedWave,
    availableWaves: waves.map(normalizeWave).sort((a, b) => compareNumber(a.sortOrder, b.sortOrder)),
    horses: normalizedHorses,
    lists,
    tabGroups,
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
      horses: normalizedHorses.length,
      listPlans: listPlans.length
    },
    needsGeneration: items.length === 0,
    items
  };
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
  return printPackingPageHtml(report, printTargetTitle(report, target), printListSections(report, target));
}

function printTargetTitle(report, target) {
  if (target === "horses") return "Horses";
  if (String(target || "").startsWith("tab:")) {
    return displayLabel((report.tabGroups || []).find((group) => group.id === target)?.label || target.replace(/^tab:/, ""));
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

function printPackingPageHtml(report, title, sections) {
  const rows = sections.flatMap((section) => section.rows);
  const percent = progressPercent(rows.filter(isSatisfied).length, rows.length);
  const chunks = printSectionChunks(sections);
  return chunks.map((chunk) => printPackingPageChunkHtml(report, title, chunk, percent)).join("");
}

function printPackingPageChunkHtml(report, title, sections, percent) {
  return `
    <section class="packing-print-page">
      <header class="packing-print-head">
        <h1>${escapeHtml(displayLabel(title))}</h1>
        <p>${escapeHtml(printStatusLine(report, percent))}</p>
      </header>
      <div class="packing-print-columns">
        ${sections.length ? sections.map(printListColumnHtml).join("") : printEmptyPrintSectionHtml("No rows")}
      </div>
    </section>
  `;
}

function printListColumnHtml(section) {
  return `
    <section class="packing-print-list ${printDensityClass(section.rows)}">
      <h2>${escapeHtml(section.title)}</h2>
      ${section.rows.length ? section.rows.map(printItemRowHtml).join("") : printEmptyPrintSectionHtml("No rows")}
    </section>
  `;
}

function printItemRowHtml(item) {
  const packed = isSatisfied(item);
  return `
    <div class="packing-print-item ${packed ? "is-packed" : ""}">
      <div class="packing-print-item-main">
        <strong class="packing-print-item-name">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</strong>
      </div>
      <span class="packing-print-metrics">${printQuantityMetricsHtml(item.needed, item.packed, item.left)}</span>
      <span class="packing-print-scratch" aria-hidden="true"></span>
    </div>
  `;
}

function printQuantityMetricsHtml(needed, packed, left) {
  return [
    `Need: ${quantityDisplay(needed)}`,
    `Packed: ${quantityDisplay(packed)}`,
    `Left: ${quantityDisplay(left)}`
  ].map(escapeHtml).join(" ");
}

function printHorsesPageHtml(report) {
  const rows = activePrintHorses(report);
  const columns = splitRows(rows);
  const members = horseMemberRows(report);
  const percent = progressPercent(members.filter(isHorseMemberPacked).length, members.length);
  return `
    <section class="packing-print-page">
      <header class="packing-print-head">
        <h1>Horses</h1>
        <p>${escapeHtml(printStatusLine(report, percent))}</p>
      </header>
      <div class="packing-print-columns">
        ${printHorseColumnHtml("Horses", columns[0])}
        ${printHorseColumnHtml("Horses", columns[1])}
      </div>
    </section>
  `;
}

function printHorsePackingPageHtml(report, horseId) {
  const horse = (report.horses || []).find((row) => row.id === horseId);
  if (!horse) return printEmptyPrintSectionHtml("No horse");
  const rows = horseItemRows(report, horse);
  const columns = splitRows(rows);
  const percent = progressPercent(rows.filter((row) => isHorseMemberPacked(row.member)).length, rows.length);
  return `
    <section class="packing-print-page">
      <header class="packing-print-head">
        <h1>${escapeHtml(printHorseName(horse))}</h1>
        <p>${escapeHtml(printStatusLine(report, percent))}</p>
      </header>
      <div class="packing-print-columns">
        ${printHorsePackingColumnHtml("Items", columns[0])}
        ${printHorsePackingColumnHtml("Items", columns[1])}
      </div>
    </section>
  `;
}

function printHorsePackingColumnHtml(title, rows) {
  return `
    <section class="packing-print-list ${printDensityClass(rows)}">
      <h2>${escapeHtml(title)}</h2>
      ${rows.length ? rows.map(printHorsePackingItemHtml).join("") : printEmptyPrintSectionHtml("No rows")}
    </section>
  `;
}

function printHorsePackingItemHtml(row) {
  const needed = numberField(row.member?.needed);
  const packed = numberField(row.member?.packed);
  const left = Math.max(0, needed - packed);
  const done = isHorseMemberPacked(row.member);
  return `
    <div class="packing-print-item ${done ? "is-packed" : ""}">
      <div class="packing-print-item-main">
        <strong class="packing-print-item-name">${escapeHtml(displayLabel(row.item?.name || "Unnamed item"))}</strong>
      </div>
      <span class="packing-print-metrics">${printQuantityMetricsHtml(needed, packed, left)}</span>
      <span class="packing-print-scratch" aria-hidden="true"></span>
    </div>
  `;
}

function printHorseColumnHtml(title, rows) {
  return `
    <section class="packing-print-list ${printDensityClass(rows)}">
      <h2>${escapeHtml(title)}</h2>
      ${rows.length ? rows.map((horse) => `
        <div class="packing-print-horse">
          <strong>${escapeHtml(printHorseName(horse))}</strong>
        </div>
      `).join("") : printEmptyPrintSectionHtml("No horses")}
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

function splitRows(rows) {
  const middle = Math.ceil(rows.length / 2);
  return [rows.slice(0, middle), rows.slice(middle)];
}

function printSectionChunks(sections) {
  const chunks = [];
  for (let index = 0; index < sections.length; index += 2) {
    chunks.push(sections.slice(index, index + 2));
  }
  return chunks.length ? chunks : [[]];
}

function printDensityClass(rows = []) {
  if (rows.length >= 22) return "is-ultra-dense";
  if (rows.length >= 14) return "is-dense";
  return "";
}

function printEmptyPrintSectionHtml(label) {
  return `<div class="packing-print-empty">${escapeHtml(label)}</div>`;
}

function printStatusLine(report, percent) {
  const wave = displayLabel(report.wave?.wave || "wave_one");
  const days = report.wave?.daysTill ? `${quantityDisplay(report.wave.daysTill)} days remaining` : "days remaining not set";
  return `${wave} | ${days} | ${percent}% packed | Printed: ${printDateDisplay()}`;
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
    @page { size: Letter; margin: 0.35in; }
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
      width: 100%;
      min-height: 10.3in;
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
      margin-bottom: 0.16in;
    }
    .packing-print-head h1 {
      margin: 0;
      font-size: 24px;
      font-weight: 600;
      line-height: 0.95;
    }
    .packing-print-head p {
      max-width: 4.8in;
      margin: 0;
      font-size: 9px;
      font-weight: 600;
      text-align: right;
      text-transform: uppercase;
    }
    .packing-print-columns {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 0.16in;
      align-items: start;
    }
    .packing-print-list {
      border: 1px solid #d9d9d9;
      border-radius: 8px;
      overflow: hidden;
      break-inside: avoid;
      page-break-inside: avoid;
      background: #ffffff;
    }
    .packing-print-list h2 {
      margin: 0;
      padding: 8px 10px;
      background: #f0f0f0;
      border-bottom: 1px solid #d9d9d9;
      font-size: 12px;
      font-weight: 600;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-item,
    .packing-print-horse {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto 0.42in;
      gap: 8px;
      align-items: center;
      padding: 7px 10px;
      border-bottom: 1px solid #eeeeee;
    }
    .packing-print-horse {
      grid-template-columns: minmax(0, 1fr);
    }
    .packing-print-item:last-child,
    .packing-print-horse:last-child { border-bottom: 0; }
    .packing-print-item-main {
      display: grid;
      gap: 3px;
      min-width: 0;
    }
    .packing-print-item strong,
    .packing-print-horse strong {
      font-size: 11px;
      font-weight: 600;
      line-height: 1;
      text-transform: uppercase;
    }
    .packing-print-item.is-packed .packing-print-item-name {
      opacity: 0.6;
      text-decoration: line-through;
      text-decoration-thickness: 1px;
    }
    .packing-print-metrics {
      color: #333333;
      font-size: 9px;
      font-weight: 600;
      line-height: 1.1;
      text-align: right;
      white-space: nowrap;
    }
    .packing-print-scratch {
      display: block;
      width: 0.42in;
      height: 0.2in;
      border: 1px solid #cfcfcf;
      border-radius: 3px;
      background: #ffffff;
    }
    .packing-print-list.is-dense h2 {
      padding: 6px 8px;
      font-size: 11px;
    }
    .packing-print-list.is-dense .packing-print-item,
    .packing-print-list.is-dense .packing-print-horse {
      padding: 5px 8px;
      gap: 6px;
    }
    .packing-print-list.is-ultra-dense h2 {
      padding: 5px 7px;
      font-size: 10px;
    }
    .packing-print-list.is-ultra-dense .packing-print-item,
    .packing-print-list.is-ultra-dense .packing-print-horse {
      padding: 3px 7px;
      gap: 5px;
    }
    .packing-print-list.is-ultra-dense .packing-print-item strong,
    .packing-print-list.is-ultra-dense .packing-print-horse strong {
      font-size: 9px;
    }
    .packing-print-list.is-ultra-dense .packing-print-metrics {
      font-size: 7px;
    }
    .packing-print-list.is-ultra-dense .packing-print-scratch {
      height: 0.15in;
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
  const horsesByItem = groupByLinkedId(filteredHorseMembers, "packing_item");
  const items = filteredItems
    .map((record) => decoratePackingItem(
      normalizePackingItem(record, horsesByItem.get(record.id) || [], listPlanLookup),
      packListLookup,
      sourcePackItemLookup,
      wave,
      waveHorseIds
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
      horseSanity: numberField(wave?.horseSanity),
      effectiveHorseCount: numberField(wave?.effectiveHorseCount),
      currentWaveHorseCount: waveHorses.length,
      horseCountMismatch: !!wave && numberField(wave.horseCount) !== numberField(wave.effectiveHorseCount),
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
    result = await applyAddQuantity(airtable, tables, payload);
  } else if (action === "set_pack_state") {
    result = await applyPackState(airtable, tables, payload);
  } else if (action === "set_resolution") {
    result = await applyResolutionState(airtable, tables, payload);
  } else if (action === "update_item_fields") {
    result = await applyItemFieldUpdate(airtable, tables, payload);
  } else if (action === "set_horse_pack_state") {
    result = await applyHorsePackState(airtable, tables, payload);
  } else if (action === "set_horse_record_state") {
    result = await applyHorseRecordState(airtable, tables, payload);
  } else if (action === "set_source_flag") {
    result = await applySourceFlag(airtable, tables, payload);
  } else {
    return { ok: false, error: "unknown_action", action };
  }

  const state = await stateReport(airtable, requestUrl);
  return {
    ok: true,
    action,
    result,
    state
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

async function listOptionalRecords(airtable, tableConfig) {
  if (!tableConfig?.id) return [];
  try {
    return await listAirtableRecords(airtable, tableConfig.id, tableConfig.view);
  } catch (error) {
    console.warn(`[wec-packing] optional table skipped: ${tableConfig.name || tableConfig.id}`, error);
    return [];
  }
}

async function applyAddQuantity(airtable, tables, payload) {
  const itemId = clean(payload?.itemId || payload?.packingItemId);
  const delta = wholeQuantityField(payload?.quantityDelta || payload?.delta || 0);
  if (!itemId) throw new Error("missing_item_id");
  if (!Number.isFinite(delta) || delta <= 0) throw new Error("quantity_delta_must_be_positive");

  const { record } = await findRecordInConfiguredView(airtable, tables.wec_packing_items, itemId);
  const fields = record.fields || {};
  const before = wholeQuantityField(fields.quantity_packed);
  const needed = actionNeeded(fields, payload);
  const after = Math.min(needed || before + delta, before + delta);
  const nextPackState = needed > 0 && after >= needed ? "packed" : "not_packed";

  const updated = await patchAirtableRecord(airtable, tables.wec_packing_items.id, itemId, {
    quantity_packed: after,
    pack_state: nextPackState
  });
  const event = await createPackingEvent(airtable, tables, {
    eventType: "quantity_add",
    itemRecord: record,
    quantityDelta: after - before,
    quantityBefore: before,
    quantityAfter: after,
    packStateBefore: stringField(fields.pack_state || "not_packed"),
    packStateAfter: nextPackState,
    decisionBefore: stringField(fields.resolution_state),
    decisionAfter: stringField(fields.resolution_state),
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyPackState(airtable, tables, payload) {
  const itemId = clean(payload?.itemId || payload?.packingItemId);
  const nextPackState = clean(payload?.packState || payload?.state);
  if (!itemId) throw new Error("missing_item_id");
  if (!["packed", "not_packed"].includes(nextPackState)) throw new Error("invalid_pack_state");
  if (nextPackState === "packed" && !payload?.confirmed) throw new Error("confirmation_required");

  const { record } = await findRecordInConfiguredView(airtable, tables.wec_packing_items, itemId);
  const fields = record.fields || {};
  const beforeQuantity = wholeQuantityField(fields.quantity_packed);
  const needed = actionNeeded(fields, payload);
  const afterQuantity = nextPackState === "packed" ? needed : beforeQuantity;

  const updated = await patchAirtableRecord(airtable, tables.wec_packing_items.id, itemId, {
    quantity_packed: afterQuantity,
    pack_state: nextPackState
  });
  const event = await createPackingEvent(airtable, tables, {
    eventType: nextPackState === "packed" ? "mark_packed" : "mark_not_packed",
    itemRecord: record,
    quantityDelta: afterQuantity - beforeQuantity,
    quantityBefore: beforeQuantity,
    quantityAfter: afterQuantity,
    packStateBefore: stringField(fields.pack_state || "not_packed"),
    packStateAfter: nextPackState,
    decisionBefore: stringField(fields.resolution_state),
    decisionAfter: stringField(fields.resolution_state),
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyResolutionState(airtable, tables, payload) {
  const itemId = clean(payload?.itemId || payload?.packingItemId);
  const nextResolution = clean(payload?.resolutionState || payload?.resolution || payload?.decision);
  const allowed = ["max", "kill", "note", "purchase_onsite", "unresolved", "clear"];
  if (!itemId) throw new Error("missing_item_id");
  if (!allowed.includes(nextResolution)) throw new Error("invalid_resolution_state");
  if (!payload?.confirmed) throw new Error("confirmation_required");

  const { record } = await findRecordInConfiguredView(airtable, tables.wec_packing_items, itemId);
  const fields = record.fields || {};
  const beforeDecision = stringField(fields.resolution_state);
  const packed = wholeQuantityField(fields.quantity_packed);
  const needed = actionNeeded(fields, payload);
  const packStateAfter = packed >= needed && needed > 0 ? "packed" : "not_packed";
  const updateFields = nextResolution === "clear"
    ? { resolution_state: null, pack_state: packStateAfter }
    : { resolution_state: nextResolution, pack_state: "not_packed" };

  const updated = await patchAirtableRecord(airtable, tables.wec_packing_items.id, itemId, updateFields);
  const event = await createPackingEvent(airtable, tables, {
    eventType: nextResolution === "clear" ? "decision_clear" : `decision_${nextResolution}`,
    itemRecord: record,
    quantityDelta: 0,
    quantityBefore: packed,
    quantityAfter: packed,
    packStateBefore: stringField(fields.pack_state || "not_packed"),
    packStateAfter: updateFields.pack_state,
    decisionBefore: beforeDecision,
    decisionAfter: nextResolution === "clear" ? "" : nextResolution,
    notes: clean(payload?.notes)
  });
  return { updated, event };
}

async function applyItemFieldUpdate(airtable, tables, payload) {
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

  const { record } = await findRecordInConfiguredView(airtable, tables.wec_packing_items, itemId);
  const fields = { ...(record.fields || {}), ...updateFields };
  if (Object.prototype.hasOwnProperty.call(updateFields, "quantity_packed") || Object.prototype.hasOwnProperty.call(updateFields, "quantity_needed")) {
    const packed = wholeQuantityField(fields.quantity_packed);
    const needed = Object.prototype.hasOwnProperty.call(updateFields, "quantity_needed")
      ? worksheetNeeded(fields)
      : actionNeeded(fields, payload);
    updateFields.pack_state = needed > 0 && packed >= needed ? "packed" : "not_packed";
  }

  const updated = await patchAirtableRecord(airtable, tables.wec_packing_items.id, itemId, updateFields);
  return { updated };
}

async function applyHorsePackState(airtable, tables, payload) {
  const memberId = clean(payload?.itemHorseId || payload?.packingItemHorseId);
  const nextState = clean(payload?.horsePackState || payload?.state);
  if (!memberId) throw new Error("missing_item_horse_id");
  if (!["packed", "not_packed"].includes(nextState)) throw new Error("invalid_horse_pack_state");

  const { record: memberRecord, records: allMembers } = await findRecordInConfiguredView(airtable, tables.wec_packing_item_horses, memberId);
  const member = normalizeHorseMember(memberRecord);
  const packingItemId = member.packingItemIds[0];
  if (!packingItemId) throw new Error("missing_parent_packing_item");
  const { record: itemRecord } = await findRecordInConfiguredView(airtable, tables.wec_packing_items, packingItemId);

  const before = wholeQuantityField(memberRecord.fields?.quantity_packed);
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
  const neededTotal = rolledMembers.reduce((sum, record) => sum + wholeQuantityField(record.fields?.quantity_needed || 1), 0);
  const parentPackState = neededTotal > 0 && packedTotal >= neededTotal ? "packed" : "not_packed";
  const updatedParent = await patchAirtableRecord(airtable, tables.wec_packing_items.id, packingItemId, {
    quantity_packed: packedTotal,
    pack_state: parentPackState
  });

  const event = await createPackingEvent(airtable, tables, {
    eventType: nextState === "packed" ? "mark_packed" : "mark_not_packed",
    itemRecord,
    memberRecord,
    quantityDelta: after - before,
    quantityBefore: before,
    quantityAfter: after,
    packStateBefore: stringField(memberRecord.fields?.horse_pack_state || "not_packed"),
    packStateAfter: nextState,
    decisionBefore: "",
    decisionAfter: "",
    notes: clean(payload?.notes)
  });
  return { updatedMember, updatedParent, event };
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

async function findRecordInConfiguredView(airtable, tableConfig, recordId) {
  if (!tableConfig?.id) throw new Error("missing_table_config");
  const records = await listAirtableRecords(airtable, tableConfig.id, tableConfig.view);
  const record = records.find((item) => item.id === recordId);
  if (!record) throw new Error(`${tableConfig.name || tableConfig.id}_record_not_in_configured_view: ${recordId}`);
  return { record, records };
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

async function createPackingEvent(airtable, tables, payload) {
  const itemFields = payload.itemRecord?.fields || {};
  const memberFields = payload.memberRecord?.fields || {};
  const eventFields = compactFields({
    event: `${payload.eventType}:${payload.itemRecord?.id || ""}:${Date.now()}`,
    show: linkedIds(itemFields.show),
    pack_wave: linkedIds(itemFields.pack_wave).length ? linkedIds(itemFields.pack_wave) : linkedIds(memberFields.pack_wave),
    packing_item: payload.itemRecord?.id ? [payload.itemRecord.id] : [],
    packing_item_horse: payload.memberRecord?.id ? [payload.memberRecord.id] : [],
    horse: linkedIds(memberFields.horse),
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
    horseSanity: numberField(fields.horse_sanity),
    groomCountMode: stringField(fields.groom_count_mode),
    groomCountManual: numberField(fields.groom_count_manual),
    groomRatio: numberField(fields.groom_ratio),
    groomCountFinal: numberField(fields.groom_count_final),
    groomSanity: numberField(fields.groom_sanity),
    sortOrder: numberField(fields.sort_order),
    showIds: linkedIds(fields.show),
    includedWeekIds: linkedIds(fields.included_weeks)
  };
}

function withEffectiveWaveCounts(wave, waveHorses) {
  if (!wave) return null;
  const linkedHorseCount = waveHorses.length;
  const currentHorseCount = wave.horseSanity > 0 ? wave.horseSanity : linkedHorseCount;
  const manualGroomCount = wave.groomCountManual > 0
    ? wave.groomCountManual
    : slugify(wave.groomCountMode) === "manual" && wave.groomCountFinal > 0
      ? wave.groomCountFinal
      : 0;
  const ratioGroomCount = wave.groomRatio > 0
    ? Math.ceil(currentHorseCount / wave.groomRatio)
    : 0;
  const dynamicGroomCountFinal = manualGroomCount || wave.groomSanity || ratioGroomCount || wave.groomCountFinal;
  const effectiveHorseCount = wave.manualLock ? wave.horseCount : currentHorseCount;
  const effectiveGroomCountFinal = wave.manualLock ? wave.groomCountFinal : dynamicGroomCountFinal;
  return {
    ...wave,
    linkedHorseCount,
    currentHorseCount,
    currentGroomCountFinal: dynamicGroomCountFinal,
    effectiveHorseCount,
    effectiveGroomCountFinal,
    countsLocked: wave.manualLock,
    countSource: wave.manualLock ? "manual_lock" : (wave.horseSanity > 0 ? "horse_sanity" : "current_wave_scope"),
    groomCountSource: wave.manualLock
      ? "manual_lock"
      : manualGroomCount
        ? (wave.groomCountManual > 0 ? "groom_count_manual" : "groom_count_final_manual")
        : wave.groomSanity
          ? "groom_sanity"
          : wave.groomRatio
            ? "groom_ratio"
            : "groom_count_final"
  };
}

function normalizePackList(record) {
  const fields = record.fields || {};
  const label = stringField(fields.list) || record.id;
  const tabs = stringListField(fields.tabs);
  return {
    id: record.id,
    key: slugify(label),
    label,
    tabs,
    tabKey: slugify(tabs[0] || label),
    tabLabel: tabs[0] || label,
    shortDescription: stringField(fields.short_description),
    longDescription: stringField(fields.long_description),
    itemCount: numberField(fields.list_items_count)
  };
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

function normalizeSourcePackItem(record, listPlanLookup = new Map()) {
  const fields = record.fields || {};
  const listPlan = resolveListPlan(fields, listPlanLookup);
  return {
    id: record.id,
    appName: stringField(fields.app_name),
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
    ignored: !!fields.ignore,
    active: !!fields.active && !fields.inactive && !fields.remove,
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

function decoratePackingItem(item, packListLookup, sourcePackItemLookup, wave, waveHorseIds = new Set()) {
  const sourceItems = item.sourcePackItemIds
    .map((id) => sourcePackItemLookup.get(id))
    .filter(Boolean);
  const quantityCalculation = buildQuantityCalculation(item, sourceItems[0], wave, waveHorseIds);
  const effectiveNeeded = wave?.countsLocked
    ? item.quantityNeededFrozen ?? item.needed
    : calculatedNeededForUnlockedItem(item, quantityCalculation);
  const effectiveItem = {
    ...item,
    needed: wholeQuantityField(effectiveNeeded),
    left: wholeQuantityField(Math.max(0, effectiveNeeded - item.packed))
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
    name: stringField(fields.barn_name || fields.horse || fields.show_name),
    showName: stringField(fields.show_name || fields.horse),
    recordState,
    active: recordState === "active",
    manualLock: !!fields.manual_lock,
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

function buildQuantityCalculation(item, sourceItem, wave, waveHorseIds = new Set()) {
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
      multiplierField: wave?.countsLocked
        ? "wec_pack_waves.groom_count_final"
        : wave?.groomCountSource === "groom_count_final_manual"
          ? "wec_pack_waves.groom_count_final"
          : wave?.groomCountSource === "groom_sanity"
            ? "wec_pack_waves.groom_sanity"
            : "current wave groom count",
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
        : wave?.countSource === "horse_sanity"
          ? "wec_pack_waves.horse_sanity"
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
    const perHorse = numberField(sourceItem?.perHorse || item.quantityBase || 1);
    const expectedHorseCount = sourceItem ? expectedSourceHorseIds(sourceItem, waveHorseIds).size : item.horseMembers.length;
    const calculatedNeeded = wholeQuantityField(expectedHorseCount * perHorse);
    return calculationRow({
      plan,
      formula: "eligible_horses * per_horse",
      sourceField: "wec_pack_items.per_horse",
      multiplierField: "current eligible horse count",
      base: perHorse,
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
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function clean(value) {
  return String(value ?? "").trim();
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
