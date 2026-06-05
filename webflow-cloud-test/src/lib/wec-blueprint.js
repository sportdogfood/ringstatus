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
  "wec_list_plans",
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

const REQUIRED_BLUEPRINT_TABLES = [
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

const WIRE_ROLES = new Set([
  "entity_1",
  "entity_2",
  "links",
  "logs",
  "lanes",
  "slots",
  "comments",
  "support_1",
  "support_2",
  "support_3"
]);

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
  const waveRows = tables.wec_pack_waves?.records || [];
  const headerWave = waveRows.find((record) => clean(record.fields?.wave) === "wave_one" || clean(record.fields?.pack_wave_key) === "wave_one") ||
    waveRows.find((record) => record.fields?.active) ||
    waveRows[0];
  const horseRows = tables.pak_horses_roster?.records || [];
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
    groupModel: buildGroupModel(tables),
    reviewQueue: buildReviewQueue(tables),
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

function buildReviewQueue(tables) {
  const rows = [];
  const wireModel = buildWireModel(tables);
  const fieldsModel = buildFieldsModel(tables);
  const groupModel = buildGroupModel(tables);

  if (!wireModel.assignments.length && wireModel.draftAssignments.length) {
    rows.push({
      type: "wire_assignments",
      severity: "review",
      source: "pak_wire_assignments",
      detail: `${wireModel.draftAssignments.length} draft assignments are visible but no assignment rows exist`,
      action: "review suggested wire matrix before writing assignments"
    });
  }

  for (const row of wireModel.coverage || []) {
    if (row.missing?.length) {
      rows.push({
        type: "wire_coverage",
        severity: "review",
        source: row.plan,
        detail: `missing ${row.missing.join(", ")}`,
        action: "confirm whether each missing role is truly required for this plan"
      });
    }
  }

  const unallowedGroups = (fieldsModel.groups || []).filter((group) => group.active > 0 && group.allowed === 0);
  if (unallowedGroups.length) {
    const activeTotal = unallowedGroups.reduce((sum, group) => sum + Number(group.active || 0), 0);
    rows.push({
      type: "field_allowlist",
      severity: "review",
      source: "pak_fields",
      detail: `${unallowedGroups.length} source tables have ${activeTotal} active fields and 0 marked allowed`,
      action: "use Field Model detail to decide which tables should drive allowed payloads"
    });
  }

  for (const plan of groupModel.plans || []) {
    if (plan.total !== plan.active) {
      rows.push({
        type: "group_activity",
        severity: "info",
        source: plan.planKey || plan.planId,
        detail: `${plan.active} active of ${plan.total} pak_groups rows`,
        action: "review inactive/hidden rows only if render is incomplete"
      });
    }
  }

  return rows;
}

function buildGroupModel(tables) {
  const planRows = tables.wec_list_plans?.records || [];
  const planById = new Map(planRows.map((record) => [record.id, {
    id: record.id,
    key: clean(record.fields?.plan_key || record.fields?.list_plan_key || record.fields?.key || record.fields?.Name || record.fields?.name || record.fields?.plan || record.fields?.display_label),
    label: clean(record.fields?.display_label || record.fields?.plan_label || record.fields?.label || record.fields?.Name || record.fields?.name || record.fields?.plan_key)
  }]));
  const groups = new Map();
  for (const record of tables.pak_groups?.records || []) {
    const fields = record.fields || {};
    const planIds = Array.isArray(fields.wec_list_plans) && fields.wec_list_plans.length ? fields.wec_list_plans : ["unassigned"];
    for (const planId of planIds) {
      const plan = planById.get(planId) || { id: planId, key: planId, label: planId };
      if (!groups.has(planId)) {
        const label = plan.label || plan.key || planId;
        groups.set(planId, {
          planId,
          planKey: plan.key,
          planLabel: label,
          total: 0,
          active: 0,
          hidden: 0,
          renderKeys: new Set(),
          roles: new Set(),
          sourceTables: new Set(),
          components: new Set()
        });
      }
      const group = groups.get(planId);
      group.total += 1;
      if (fields.active) group.active += 1;
      if (fields.is_hidden) group.hidden += 1;
      if (fields.render_key) group.renderKeys.add(clean(fields.render_key));
      if (fields.role || fields.role_select) group.roles.add(clean(fields.role || fields.role_select));
      if (fields.physical_table || fields.table_name) group.sourceTables.add(clean(fields.physical_table || fields.table_name));
      if (fields.component_key) group.components.add(clean(fields.component_key));
    }
  }
  return {
    plans: [...groups.values()]
      .map((group) => ({
        ...group,
        renderKeys: [...group.renderKeys].sort(),
        roles: [...group.roles].sort(),
        sourceTables: [...group.sourceTables].sort(),
        components: [...group.components].sort()
      }))
      .sort((a, b) => a.planLabel.localeCompare(b.planLabel))
  };
}

function buildBlueprintValidationTests(tables, tableByName) {
  const entityRows = tables.pak_entities_index?.records || [];
  const itemRows = tables.pak_items_index?.records || [];
  const listRows = tables.pak_list_family_index?.records || [];
  const fieldRows = tables.pak_fields?.records || [];
  const groupRows = tables.pak_groups?.records || [];
  const componentRows = tables.pak_components?.records || [];
  const horses = findBy(entityRows, "entity_key", "horses");
  const comments = findBy(entityRows, "entity_key", "comments");
  const itemFamilies = new Set(itemRows.map((record) => clean(record.fields?.item_family)));
  const listFamilies = new Set(listRows.map((record) => clean(record.fields?.list_key)));
  const componentIds = new Set(componentRows.map((record) => record.id));
  const groupSourceRows = groupRows.filter((record) => {
    const fields = record.fields || {};
    return clean(fields.role || fields.role_select) !== "group" && clean(fields.render_key) !== "group_shell";
  });
  const groupSourceTables = [...new Set(groupSourceRows.map((record) => clean(record.fields?.physical_table || record.fields?.table_name)).filter(Boolean))].sort();
  const missingGroupSourceTables = groupSourceTables.filter((name) => !tableByName.has(name));
  const groupComponentRows = groupRows.filter((record) => clean(record.fields?.component_key));
  const missingGroupComponentLinks = groupComponentRows.filter((record) => {
    const links = Array.isArray(record.fields?.pak_components) ? record.fields.pak_components : [];
    return links.some((id) => !componentIds.has(id));
  });
  const groupModel = buildGroupModel(tables);
  const activeHorseFields = fieldRows.filter((record) =>
    record.fields?.field_source_table === "pak_horses_roster" && record.fields?.active
  );

  return [
    {
      key: "hierarchy_tables_declared",
      label: "Approved blueprint tables exist",
      pass: REQUIRED_BLUEPRINT_TABLES.every((name) => tableByName.has(name)),
      detail: missingNames(REQUIRED_BLUEPRINT_TABLES, tableByName).join(", ")
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
    },
    {
      key: "pak_group_plan_counts",
      label: "Each plan has at least 18 pak_groups rows",
      pass: groupModel.plans.length >= 4 && groupModel.plans.every((plan) => Number(plan.total || 0) >= 18),
      detail: groupModel.plans.map((plan) => `${plan.planKey || plan.planId}:${plan.total}`).join(", ")
    },
    {
      key: "pak_group_sources_exist",
      label: "pak_groups source tables exist",
      pass: missingGroupSourceTables.length === 0,
      detail: missingGroupSourceTables.length ? missingGroupSourceTables.join(", ") : `${groupSourceTables.length} source tables`
    },
    {
      key: "pak_group_components_registered",
      label: "pak_groups components registered",
      pass: missingGroupComponentLinks.length === 0,
      detail: missingGroupComponentLinks.length
        ? missingGroupComponentLinks.map((record) => `${record.id}:${clean(record.fields?.component_key)}`).join(", ")
        : `${groupComponentRows.length} linked component rows`
    }
  ];
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
  const key = clean(record.fields.page_key);
  return {
    id: record.id,
    key,
    label: clean(record.fields.page_label || record.fields.Label || record.fields.page_key),
    parentKey: clean(record.fields.parent_page_key),
    sortOrder: Number(record.fields.sort_order || 0),
    opensTray: Boolean(record.fields.opens_tray),
    targetUrl: navTargetUrl(key)
  };
}

function navTargetUrl(key) {
  const targets = {
    home: "./packing-home-preview.html",
    comments: "./horse-entity-ui-preview.html?mode=comments",
    horse_roster: "./horse-entity-ui-preview.html?mode=roster",
    horse_profiles: "./horse-entity-ui-preview.html?mode=profile",
    horse_attributes: "./horse-entity-ui-preview.html?mode=attributes",
    horse_kits: "./horse-kits-static-proof-preview.html",
    quantity: "./packing-plan-preview.html?plan=quantity",
    per_horse: "./packing-plan-preview.html?plan=per_horse",
    per_groom: "./packing-plan-preview.html?plan=per_groom"
  };
  return targets[key] || `./wec-blueprint-preview.html#${encodeURIComponent(key || "page")}`;
}

function buildWireModel(tables) {
  const wireRows = (tables.pak_wire_index?.records || []).map((record) => ({ id: record.id, fields: record.fields || {} }));
  const assignmentRows = (tables.pak_wire_assignments?.records || []).map((record) => ({ id: record.id, fields: record.fields || {} }));
  const roles = wireRows
    .filter((record) => WIRE_ROLES.has(clean(record.fields.wire_role)))
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
      return role && !WIRE_ROLES.has(role);
    })
    .sort((a, b) => clean(a.fields.scope).localeCompare(clean(b.fields.scope)) || clean(a.fields.wire_role).localeCompare(clean(b.fields.wire_role)))
    .map((record) => ({
      id: record.id,
      wireKey: clean(record.fields.wire_role),
      scope: clean(record.fields.scope),
      reason: "in pak_wire_index but not assigned in pak_wire_assignments"
    }));
  const assignments = assignmentRows
    .sort((a, b) => clean(a.fields.wire_key).localeCompare(clean(b.fields.wire_key)) || clean(a.fields.wire_role).localeCompare(clean(b.fields.wire_role)))
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
  const suggestedAssignments = buildSuggestedWireAssignments(tables);
  return {
    roles,
    assignments,
    draftAssignments,
    suggestedAssignments,
    coverage: buildWireCoverage(suggestedAssignments)
  };
}

function buildWireCoverage(suggestedAssignments) {
  const required = ["entity_1", "entity_2", "links", "logs", "lanes", "slots", "comments", "support_1", "support_2", "support_3"];
  const byPlan = new Map();
  for (const row of suggestedAssignments) {
    const key = row.planLabel || row.planId || "unassigned";
    if (!byPlan.has(key)) byPlan.set(key, new Set());
    byPlan.get(key).add(row.wireRole);
  }
  return [...byPlan.entries()]
    .map(([plan, roles]) => ({
      plan,
      present: [...roles].sort(),
      missing: required.filter((role) => !roles.has(role))
    }))
    .sort((a, b) => a.plan.localeCompare(b.plan));
}

function buildSuggestedWireAssignments(tables) {
  const groupModel = buildGroupModel(tables);
  const planLabelById = new Map(groupModel.plans.map((plan) => [plan.planId, plan.planLabel || plan.planKey || plan.planId]));
  const rows = [];
  for (const record of tables.pak_groups?.records || []) {
    const fields = record.fields || {};
    if (!fields.active) continue;
    const role = inferWireRole(fields);
    if (!role) continue;
    const planIds = Array.isArray(fields.wec_list_plans) && fields.wec_list_plans.length ? fields.wec_list_plans : ["unassigned"];
    for (const planId of planIds) {
      rows.push({
        groupId: record.id,
        planId,
        planLabel: planLabelById.get(planId) || planId,
        wireRole: role,
        renderKey: clean(fields.render_key),
        sourceTable: clean(fields.physical_table || fields.table_name),
        sourceView: clean(fields.table_source_view || ""),
        componentKey: clean(fields.component_key),
        displayLabel: clean(fields.display_label)
      });
    }
  }
  return rows.sort((a, b) =>
    a.planLabel.localeCompare(b.planLabel) ||
    a.wireRole.localeCompare(b.wireRole) ||
    a.renderKey.localeCompare(b.renderKey)
  );
}

function inferWireRole(fields) {
  const explicit = clean(fields.role_select || fields.role);
  if (WIRE_ROLES.has(explicit)) return explicit;
  const renderKey = clean(fields.render_key);
  const map = {
    main_table: "entity_1",
    drawer_items: "entity_2",
    state_links: "links",
    change_log: "logs",
    lane_source: "lanes",
    lane_controls: "lanes",
    slot_source: "slots",
    kit_source_slot: "slots",
    comments: "comments",
    comment_shorts: "comments",
    item_source: "support_1",
    kit_source: "support_1"
  };
  return map[renderKey] || "";
}

function buildFieldsModel(tables) {
  const bySource = new Map();
  for (const record of tables.pak_fields?.records || []) {
    const fields = record.fields || {};
    const sourceTable = clean(fields.field_source_table || "unassigned");
    if (!bySource.has(sourceTable)) {
      bySource.set(sourceTable, { sourceTable, total: 0, active: 0, allowed: 0, suggestAllowed: 0, suggestRemove: 0, fields: [] });
    }
    const group = bySource.get(sourceTable);
    const active = Boolean(fields.active);
    const allowed = Boolean(fields.is_alloed);
    const suggestAllowed = Boolean(fields.suggest_allowed);
    const suggestRemove = Boolean(fields.suggest_remove);
    group.total += 1;
    if (active) group.active += 1;
    if (allowed) group.allowed += 1;
    if (suggestAllowed) group.suggestAllowed += 1;
    if (suggestRemove) group.suggestRemove += 1;
    group.fields.push({
      id: record.id,
      fieldKey: clean(fields.field_key),
      fieldId: clean(fields.field_id),
      dataRsValue: clean(fields.data_rs_value),
      fieldLabel: clean(fields.field_label),
      active,
      allowed,
      suggestAllowed,
      suggestRemove
    });
  }
  return { groups: [...bySource.values()].sort((a, b) => a.sourceTable.localeCompare(b.sourceTable)) };
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
    component: component ? { id: component.id, componentKey: clean(component.fields?.component_key || component.fields?.component) } : null,
    html: html ? { id: html.id, htmlKey: clean(html.fields?.html_key), patternLabel: clean(html.fields?.pattern_label), rootClass: clean(html.fields?.root_class), htmlPattern } : null,
    source: source ? { id: source.id, table: clean(stack?.fields?.table_source), key: clean(source.fields?.wave || source.fields?.wave_key || source.fields?.pack_wave_key) } : null,
    values: { title, subtitle },
    renderedHtml: renderSlots(htmlPattern, { title, subtitle })
  };
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
  const key = clean(fields.system_key || fields.page_key || fields.stack_key || fields.wire_role || fields.page_type_key || record.id);
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

function renderSlots(html, values) {
  if (!html) return "";
  return html
    .replace(/<span data-rs-slot="title"><\/span>/g, `<span data-rs-slot="title">${escapeHtml(values.title)}</span>`)
    .replace(/<span data-rs-slot="subtitle"><\/span>/g, `<span data-rs-slot="subtitle">${escapeHtml(values.subtitle)}</span>`);
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

function sortByOrder(a, b) {
  return Number(a.fields?.sort_order || 0) - Number(b.fields?.sort_order || 0) ||
    clean(a.fields?.page_key).localeCompare(clean(b.fields?.page_key));
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

async function fetchWithRetry(url, options, attempts = 3) {
  let lastError;
  for (let index = 0; index < attempts; index += 1) {
    try {
      const response = await fetch(url, options);
      if (response.status !== 429 && response.status < 500) return response;
      lastError = new Error(`http_${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 250 * (index + 1)));
  }
  throw lastError;
}

function clean(value) {
  if (Array.isArray(value)) return value.map(clean).filter(Boolean).join(", ");
  if (value && typeof value === "object") return JSON.stringify(value);
  return String(value ?? "").trim();
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  })[char]);
}
