import {
  airtableConfig,
  corsHeaders,
  json
} from "./wec-plan-modules.js";

const BLUEPRINT_TABLES = [
  "pak_system_index",
  "pak_page_index",
  "pak_page_stack_index",
  "pak_page_stack",
  "pak_pages",
  "pak_page_types",
  "pak_wire_index",
  "pak_wire_assignments",
  "pak_pivots",
  "pak_system_styling",
  "pak_system_logic",
  "pak_html_lib",
  "pak_entities_index",
  "pak_items_index",
  "pak_list_family_index",
  "pak_list_members",
  "pak_fields",
  "wec_pack_waves",
  "pak_horses_roster",
  "pak_components",
  "pak_groups",
  "table_index"
];

const INDEX_TABLES = [
  "pak_system_index",
  "pak_page_index",
  "pak_page_stack_index",
  "pak_wire_index"
];

const HIERARCHY_TABLES = [
  "pak_system_index",
  "pak_page_index",
  "pak_page_stack_index",
  "pak_wire_index",
  "pak_page_stack",
  "pak_html_lib",
  "pak_entities_index",
  "pak_items_index",
  "pak_list_family_index",
  "pak_list_members",
  "pak_fields",
  "pak_pivots",
  "pak_system_styling",
  "pak_system_logic"
];

export { airtableConfig, corsHeaders, json };

export async function blueprintReport(airtable) {
  const schema = await airtableSchema(airtable);
  const tableByName = new Map(schema.tables.map((table) => [table.name, table]));
  const tables = {};

  for (const name of BLUEPRINT_TABLES) {
    const table = tableByName.get(name);
    tables[name] = table
      ? {
        exists: true,
        id: table.id,
        fields: table.fields.map((field) => ({
          name: field.name,
          id: field.id,
          type: field.type,
          linkedTableId: field.options?.linkedTableId || null
        })),
        views: table.views.map((view) => view.name),
        records: await listRecords(airtable, table.id)
      }
      : {
        exists: false,
        id: "",
        fields: [],
        views: [],
        records: []
      };
  }

  return {
    ok: true,
    v: 1,
    source: {
      mode: "read_only",
      tables: BLUEPRINT_TABLES
    },
    indexes: buildIndexes(tables, tableByName),
    tests: buildTests(tables, tableByName),
    tables,
    warnings: blueprintWarnings(tables, tableByName)
  };
}

function buildTests(tables, tableByName) {
  const horseRows = tables.pak_horses_roster?.records || [];
  const waveRows = tables.wec_pack_waves?.records || [];
  const headerWave = waveRows.find((record) => clean(record.fields?.wave) === "wave_one" || clean(record.fields?.pack_wave_key) === "wave_one") ||
    waveRows.find((record) => record.fields?.active) ||
    waveRows[0];
  const testFieldRows = (tables.pak_fields?.records || [])
    .map((record) => ({ id: record.id, fields: record.fields || {} }))
    .filter((record) => record.fields.field_source_table === "pak_horses_roster" && /^test_button_/.test(record.fields.field_key || record.fields.data_rs_value || ""));
  const testKeys = [...new Set(testFieldRows.map((record) => record.fields.field_key || record.fields.data_rs_value).filter(Boolean))].sort();
  return {
    blueprintValidation: buildBlueprintValidationTests(tables, tableByName),
    headerRender: normalizeHeaderRender(headerWave),
    headerChain: buildHeaderChain(tables),
    navModel: buildNavModel(tables),
    wireModel: buildWireModel(tables),
    fieldsModel: buildFieldsModel(tables),
    rosterButtonFields: {
      fieldRegistry: testFieldRows,
      sampleRows: horseRows.slice(0, 12).map((record) => {
        const fields = record.fields || {};
        return {
          id: record.id,
          horse: fields.display_horse_barn_name || fields.barn_name || fields.horse || "",
          values: Object.fromEntries(testKeys.map((key) => [key, fields[key] ?? ""]))
        };
      })
    }
  };
}

function buildFieldsModel(tables) {
  const rows = (tables.pak_fields?.records || []).map((record) => ({ id: record.id, fields: record.fields || {} }));
  const bySource = new Map();
  for (const record of rows) {
    const sourceTable = clean(record.fields.field_source_table || "unassigned");
    if (!bySource.has(sourceTable)) {
      bySource.set(sourceTable, {
        sourceTable,
        total: 0,
        active: 0,
        allowed: 0,
        suggestAllowed: 0,
        suggestRemove: 0,
        fields: []
      });
    }
    const group = bySource.get(sourceTable);
    const active = Boolean(record.fields.active);
    const allowed = Boolean(record.fields.is_alloed);
    const suggestAllowed = Boolean(record.fields.suggest_allowed);
    const suggestRemove = Boolean(record.fields.suggest_remove);
    group.total += 1;
    if (active) group.active += 1;
    if (allowed) group.allowed += 1;
    if (suggestAllowed) group.suggestAllowed += 1;
    if (suggestRemove) group.suggestRemove += 1;
    group.fields.push({
      id: record.id,
      fieldKey: clean(record.fields.field_key),
      fieldId: clean(record.fields.field_id),
      dataRsValue: clean(record.fields.data_rs_value),
      fieldLabel: clean(record.fields.field_label),
      active,
      allowed,
      suggestAllowed,
      suggestRemove
    });
  }
  return {
    groups: [...bySource.values()].sort((a, b) => a.sourceTable.localeCompare(b.sourceTable))
  };
}

function buildWireModel(tables) {
  const roleLike = new Set(["entity_1", "entity_2", "links", "logs", "lanes", "slots", "comments", "support_1", "support_2", "support_3"]);
  const wireRows = (tables.pak_wire_index?.records || []).map((record) => ({ id: record.id, fields: record.fields || {} }));
  const assignmentRows = (tables.pak_wire_assignments?.records || []).map((record) => ({ id: record.id, fields: record.fields || {} }));
  const roles = wireRows
    .filter((record) => roleLike.has(clean(record.fields.wire_role)))
    .sort((a, b) => clean(a.fields.wire_role).localeCompare(clean(b.fields.wire_role)))
    .map((record) => ({
      id: record.id,
      wireRole: clean(record.fields.wire_role),
      scope: clean(record.fields.scope),
      required: Boolean(record.fields.required),
      active: Boolean(record.fields.active)
    }));
  const draftAssignments = wireRows
    .filter((record) => {
      const role = clean(record.fields.wire_role);
      return role && !roleLike.has(role);
    })
    .sort((a, b) => clean(a.fields.scope).localeCompare(clean(b.fields.scope)) || clean(a.fields.wire_role).localeCompare(clean(b.fields.wire_role)))
    .map((record) => ({
      id: record.id,
      wireKey: clean(record.fields.wire_role),
      scope: clean(record.fields.scope),
      reason: "in pak_wire_index but not assigned in pak_wire_assignments"
    }));
  const assignments = assignmentRows
    .sort((a, b) =>
      clean(a.fields.wire_key).localeCompare(clean(b.fields.wire_key)) ||
      clean(a.fields.wire_role).localeCompare(clean(b.fields.wire_role))
    )
    .map((record) => ({
      id: record.id,
      wireKey: clean(record.fields.wire_key),
      wireRole: clean(record.fields.wire_role),
      pageKey: clean(record.fields.page_key),
      scope: clean(record.fields.scope),
      tableSource: clean(record.fields.table_source || record.fields.lookup_table),
      tableSourceView: clean(record.fields.table_source_view || record.fields.lookup_view),
      required: Boolean(record.fields.required),
      active: Boolean(record.fields.active)
    }));
  return { roles, assignments, draftAssignments };
}

function buildNavModel(tables) {
  const pageRows = tables.pak_page_index?.records || [];
  const navRows = pageRows
    .map((record) => ({ id: record.id, fields: record.fields || {} }))
    .filter((record) => record.fields.active && clean(record.fields.scope) === "navigation");
  const top = navRows
    .filter((record) => clean(record.fields.nav_scope) === "top")
    .sort(sortByOrder)
    .map(normalizeNavRow);
  const trays = {};
  for (const record of navRows.filter((row) => clean(row.fields.nav_scope) === "tray").sort(sortByOrder)) {
    const parent = clean(record.fields.parent_page_key);
    if (!trays[parent]) trays[parent] = [];
    trays[parent].push(normalizeNavRow(record));
  }
  return { top, trays };
}

function normalizeNavRow(record) {
  return {
    id: record.id,
    key: clean(record.fields.page_key),
    label: clean(record.fields.page_label || record.fields.Label || record.fields.page_key),
    parentKey: clean(record.fields.parent_page_key),
    sortOrder: Number(record.fields.sort_order || 0),
    opensTray: Boolean(record.fields.opens_tray)
  };
}

function sortByOrder(a, b) {
  return Number(a.fields?.sort_order || 0) - Number(b.fields?.sort_order || 0) ||
    clean(a.fields?.page_key).localeCompare(clean(b.fields?.page_key));
}

function buildHeaderChain(tables) {
  const stackRows = tables.pak_page_stack?.records || [];
  const htmlRows = tables.pak_html_lib?.records || [];
  const componentRows = tables.pak_components?.records || [];
  const waveRows = tables.wec_pack_waves?.records || [];
  const stack = stackRows.find((record) => clean(record.fields?.stack_key) === "pak_page_header");
  const html = htmlRows.find((record) => clean(record.fields?.html_key) === clean(stack?.fields?.html_key));
  const componentKey = clean(stack?.fields?.component_key);
  const component = componentRows.find((record) =>
    clean(record.fields?.component_key || record.fields?.component) === componentKey
  );
  const sourceKey = clean(stack?.fields?.source_record_key);
  const source = waveRows.find((record) =>
    clean(record.fields?.wave || record.fields?.wave_key || record.fields?.pack_wave_key) === sourceKey
  ) || waveRows.find((record) => record.fields?.active) || waveRows[0];
  const titleField = clean(stack?.fields?.title_field);
  const subtitleField = clean(stack?.fields?.subtitle_field);
  const title = clean(source?.fields?.[titleField]);
  const subtitle = clean(source?.fields?.[subtitleField]);
  const htmlPattern = clean(html?.fields?.html_pattern);

  return {
    ok: Boolean(stack && html && source && titleField && subtitleField),
    stack: stack ? {
      id: stack.id,
      stackKey: clean(stack.fields?.stack_key),
      componentKey,
      htmlKey: clean(stack.fields?.html_key),
      sourceTable: clean(stack.fields?.table_source),
      sourceRecordKey: sourceKey,
      titleField,
      subtitleField
    } : null,
    component: component ? {
      id: component.id,
      componentKey: clean(component.fields?.component_key || component.fields?.component)
    } : null,
    html: html ? {
      id: html.id,
      htmlKey: clean(html.fields?.html_key),
      patternLabel: clean(html.fields?.pattern_label),
      rootClass: clean(html.fields?.root_class),
      htmlPattern
    } : null,
    source: source ? {
      id: source.id,
      table: clean(stack?.fields?.table_source),
      key: clean(source.fields?.wave || source.fields?.wave_key || source.fields?.pack_wave_key)
    } : null,
    values: { title, subtitle },
    renderedHtml: renderSlots(htmlPattern, { title, subtitle })
  };
}

function renderSlots(html, values) {
  if (!html) return "";
  return html
    .replace(/<span data-rs-slot="title"><\/span>/g, `<span data-rs-slot="title">${escapeHtml(values.title)}</span>`)
    .replace(/<span data-rs-slot="subtitle"><\/span>/g, `<span data-rs-slot="subtitle">${escapeHtml(values.subtitle)}</span>`);
}

function normalizeHeaderRender(record) {
  const fields = record?.fields || {};
  return {
    sourceTable: "wec_pack_waves",
    sourceFields: ["wec_report_title", "wec_report_subtitle"],
    recordId: record?.id || "",
    wave: clean(fields.wave || fields.pack_wave_key || fields.wave_key || fields.label),
    title: clean(fields.wec_report_title || fields.reportTitle || "WEC PACK"),
    subtitle: clean(fields.wec_report_subtitle || fields.reportSubtitle || fields.label)
  };
}

function buildBlueprintValidationTests(tables, tableByName) {
  const entityRows = tables.pak_entities_index?.records || [];
  const itemRows = tables.pak_items_index?.records || [];
  const listRows = tables.pak_list_family_index?.records || [];
  const fieldRows = tables.pak_fields?.records || [];

  const horses = findBy(entityRows, "entity_key", "horses");
  const comments = findBy(entityRows, "entity_key", "comments");
  const itemFamilies = new Set(itemRows.map((record) => clean(record.fields?.item_family)));
  const listFamilies = new Set(listRows.map((record) => clean(record.fields?.list_key)));
  const activeHorseFields = fieldRows.filter((record) =>
    record.fields?.field_source_table === "pak_horses_roster" && record.fields?.active
  );

  return [
    {
      key: "hierarchy_tables_declared",
      label: "Approved blueprint tables exist",
      pass: HIERARCHY_TABLES.every((name) => tableByName.has(name)),
      detail: missingNames(HIERARCHY_TABLES, tableByName).join(", ")
    },
    {
      key: "horse_entity_registered",
      label: "Horse entity registered",
      pass: Boolean(horses && horses.fields?.source_table === "pak_horses_roster"),
      detail: horses ? `source_table=${clean(horses.fields?.source_table)}` : "missing horses entity"
    },
    {
      key: "horse_entity_has_allowed_fields",
      label: "Horse entity allowed fields exist",
      pass: activeHorseFields.length > 0,
      detail: `${activeHorseFields.length} active pak_horses_roster fields`
    },
    {
      key: "comments_entity_registered",
      label: "Comments entity registered",
      pass: Boolean(comments && comments.fields?.source_table === "comments"),
      detail: comments ? `source_table=${clean(comments.fields?.source_table)}` : "missing comments entity"
    },
    {
      key: "item_families_registered",
      label: "Item families registered",
      pass: ["kit", "byqty", "byhorse", "bygroom", "feed"].every((key) => itemFamilies.has(key)),
      detail: missingSetValues(["kit", "byqty", "byhorse", "bygroom", "feed"], itemFamilies).join(", ")
    },
    {
      key: "list_families_registered",
      label: "List families registered",
      pass: ["purchase_onsite", "needs_attention", "unresolved", "packed_max"].every((key) => listFamilies.has(key)),
      detail: missingSetValues(["purchase_onsite", "needs_attention", "unresolved", "packed_max"], listFamilies).join(", ")
    }
  ];
}

function findBy(records, fieldName, value) {
  return records.find((record) => clean(record.fields?.[fieldName]) === value);
}

function missingNames(names, tableByName) {
  return names.filter((name) => !tableByName.has(name));
}

function missingSetValues(values, set) {
  return values.filter((value) => !set.has(value));
}

function buildIndexes(tables, tableByName) {
  return Object.fromEntries(INDEX_TABLES.map((tableName) => {
    const table = tables[tableName];
    return [tableName, {
      exists: Boolean(table?.exists),
      rows: (table?.records || []).map((record) => normalizeIndexRow(tableName, record, tableByName))
    }];
  }));
}

function normalizeIndexRow(tableName, record, tableByName) {
  const fields = record.fields || {};
  const key = clean(
    fields.system_key ||
    fields.page_key ||
    fields.stack_key ||
    fields.wire_role ||
    fields.page_type_key ||
    fields.page_key ||
    record.id
  );
  const lookupTable = clean(fields.lookup_table || fields.table_source);
  const lookupView = clean(fields.lookup_view || fields.table_source_view);
  const target = lookupTable ? tableByName.get(lookupTable) : null;
  return {
    id: record.id,
    key,
    scope: clean(fields.scope),
    lookupTable,
    lookupView,
    componentKey: clean(fields.component_key),
    required: Boolean(fields.required),
    active: Boolean(fields.active),
    value: clean(fields.value),
    validLookupTable: lookupTable ? Boolean(target) : null,
    validLookupView: target && lookupView ? target.views.some((view) => view.name === lookupView) : null,
    raw: fields
  };
}

function blueprintWarnings(tables, tableByName) {
  const warnings = [];
  for (const tableName of INDEX_TABLES) {
    const table = tables[tableName];
    if (!table?.exists) {
      warnings.push(`${tableName}: missing table`);
      continue;
    }
    for (const record of table.records || []) {
      const row = normalizeIndexRow(tableName, record, tableByName);
      if (row.lookupTable && !row.validLookupTable) warnings.push(`${tableName}.${row.key}: lookup table not found: ${row.lookupTable}`);
      if (row.lookupTable && row.lookupView && row.validLookupView === false) warnings.push(`${tableName}.${row.key}: lookup view not found: ${row.lookupTable}.${row.lookupView}`);
    }
  }

  return warnings;
}

async function airtableSchema(airtable) {
  const response = await fetchWithRetry(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(airtable.baseId)}/tables`, {
    headers: { Authorization: `Bearer ${airtable.token}` }
  });
  if (!response.ok) throw new Error(`airtable_schema_failed:${response.status}:${await response.text()}`);
  return response.json();
}

async function listRecords(airtable, tableId) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(airtable.baseId)}/${encodeURIComponent(tableId)}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetchWithRetry(url, {
      headers: { Authorization: `Bearer ${airtable.token}` }
    });
    if (!response.ok) throw new Error(`airtable_records_failed:${tableId}:${response.status}:${await response.text()}`);
    const payload = await response.json();
    records.push(...(payload.records || []).map((record) => ({
      id: record.id,
      createdTime: record.createdTime,
      fields: record.fields || {}
    })));
    offset = payload.offset || "";
  } while (offset);
  return records;
}

async function fetchWithRetry(url, options = {}, attempt = 0) {
  const response = await fetch(url, options);
  if ((response.status === 429 || response.status >= 500) && attempt < 5) {
    const retryAfter = Number(response.headers.get("retry-after"));
    const delay = Number.isFinite(retryAfter) && retryAfter > 0
      ? retryAfter * 1000
      : 500 * Math.pow(2, attempt);
    await sleep(delay);
    return fetchWithRetry(url, options, attempt + 1);
  }
  return response;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clean(value) {
  return String(value ?? "").trim();
}

function escapeHtml(value) {
  return clean(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  })[char]);
}
