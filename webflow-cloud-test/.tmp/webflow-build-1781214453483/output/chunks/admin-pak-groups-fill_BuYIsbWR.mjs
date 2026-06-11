globalThis.process ??= {};
globalThis.process.env ??= {};
import { env } from "cloudflare:workers";
const __vite_import_meta_env__ = { "ASSETS_PREFIX": "https://110f06dd-c1ea-4839-98af-d829cbe77941.wf-app-prod.cosmic.webflow.services/test", "BASE_URL": "/test", "DEV": false, "MODE": "production", "PROD": true, "SITE": void 0, "SSR": true };
const config = {
  runtime: "edge"
};
const BASE_ID = "apptdhhNzduxm5gjn";
const TABLE_ID = "tblaVTjlE89QhSZYE";
const PLAN_IDS = {
  horse_specific: "recjCUPeConMA462v",
  quantity: "recBT7H5KeigIrAGK",
  per_horse: "recsrc6x7AdibwbMa",
  per_groom: "recZLWe1SktapDZRZ"
};
const PLAN_LABELS = {
  horse_specific: "Horse Specific",
  quantity: "Quantity",
  per_horse: "Per Horse",
  per_groom: "Per Groom"
};
const PLAN_TABLES = {
  horse_specific: {
    main_table: "pak_horses_roster",
    kit_source: "pak_kits",
    drawer_items: "pak_kit_items",
    state_links: "horse_packing_kits",
    change_log: "horse_kit_changes",
    lane_source: "wec_lanes"
  },
  quantity: {
    main_table: "pak_byqty_items",
    kit_source: "pak_byqtys",
    drawer_items: "pak_byqty_items",
    state_links: "pak_byqty_links",
    change_log: "pak_byqty_logs",
    lane_source: "pak_byqty_lanes"
  },
  per_horse: {
    main_table: "pak_byhorse_items",
    kit_source: "pak_byhorses",
    drawer_items: "pak_byhorse_items",
    state_links: "pak_byhorse_links",
    change_log: "pak_byhorse_logs",
    lane_source: "pak_byhorse_lanes"
  },
  per_groom: {
    main_table: "pak_bygroom_items",
    kit_source: "pak_bygrooms",
    drawer_items: "pak_bygroom_items",
    state_links: "pak_bygroom_links",
    change_log: "pak_bygroom_logs",
    lane_source: "pak_bygroom_lanes"
  }
};
const CLONE_FIELDS = [
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
  "allow_add_new",
  "allow_inline_edit",
  "support_table"
];
const GET = async () => json(await snapshot());
const POST = async () => {
  const before = await snapshot();
  const templateRows = before.records.horse_specific;
  const drawerTemplate = templateRows.find((row) => row.fields?.render_key === "drawer_items")?.fields || {};
  const shellTemplate = templateRows.find((row) => row.fields?.render_key === "group_shell")?.fields || {};
  const created = {};
  for (const plan of ["horse_specific", "quantity", "per_horse", "per_groom"]) {
    const existing = before.records[plan] || [];
    const missing = requiredRowsForPlan(plan, existing, { drawerTemplate, shellTemplate });
    created[plan] = [];
    for (const row of missing) {
      const fields = row.fields;
      const saved = await airtable("POST", "", { records: [{ fields }] });
      created[plan].push({ id: saved.records?.[0]?.id || "", render_key: fields.render_key, stack: fields.stack });
    }
  }
  const after = await snapshot();
  return json({ ok: true, before: before.summary, created, after: after.summary });
};
function requiredRowsForPlan(plan, existing, templates) {
  const keys = new Set(existing.map((row) => String(row.fields?.render_key || "").trim()));
  const rows = [];
  if (!keys.has("comment_shorts")) rows.push({ fields: commentShortFields(plan) });
  if (plan !== "horse_specific" && !keys.has("drawer_items")) {
    rows.push({ fields: planFieldsFromTemplate(templates.drawerTemplate, plan, {
      render_key: "drawer_items",
      display_label: "Items",
      stack: 2,
      sort_order: 2,
      include_on_drawer: true,
      is_hidden: true
    }) });
  }
  if (plan !== "horse_specific" && !keys.has("group_shell")) {
    rows.push({ fields: planFieldsFromTemplate(templates.shellTemplate, plan, {
      render_key: "group_shell",
      display_label: `${PLAN_LABELS[plan]} Group`,
      stack: 0,
      sort_order: 0,
      is_hidden: true
    }) });
  }
  return rows;
}
function planFieldsFromTemplate(templateFields, plan, overrides = {}) {
  const tableMap = PLAN_TABLES[plan];
  const fields = {};
  for (const key of CLONE_FIELDS) {
    if (templateFields[key] !== void 0) fields[key] = templateFields[key];
  }
  Object.assign(fields, overrides);
  fields.display_label = labelForPlan(fields.display_label, plan);
  fields.wec_list_plans = [PLAN_IDS[plan]];
  fields.physical_table = mapPhysicalTable(fields.physical_table || fields.table_name, tableMap);
  fields.table_name = mapPhysicalTable(fields.table_name, tableMap);
  return fields;
}
function commentShortFields(plan) {
  return {
    stack: 9,
    sort_order: 9,
    role: "support",
    render_key: "comment_shorts",
    display_label: "Comment Shorts",
    component_key: "comment_shorts",
    table_name: "comment_shorts",
    physical_table: "comment_shorts",
    active: true,
    is_hidden: true,
    include_on_drawer: true,
    support_table: true,
    wec_list_plans: [PLAN_IDS[plan]]
  };
}
function labelForPlan(label, plan) {
  if (!label) return label;
  if (/horse kits/i.test(label)) return PLAN_LABELS[plan];
  if (/kit items/i.test(label)) return plan === "horse_specific" ? label : "Items";
  return label;
}
function mapPhysicalTable(value, tableMap) {
  const table = String(value || "").trim();
  if (!table) return table;
  if (table === "pak_horses_roster") return tableMap.main_table;
  if (table === "pak_kits") return tableMap.kit_source;
  if (table === "pak_kit_items") return tableMap.drawer_items;
  if (table === "horse_packing_kits") return tableMap.state_links;
  if (table === "horse_kit_changes") return tableMap.change_log;
  if (table === "wec_lanes") return tableMap.lane_source;
  return table;
}
async function snapshot() {
  const records = {};
  const summary = {};
  for (const plan of Object.keys(PLAN_IDS)) {
    records[plan] = await listView(plan);
    summary[plan] = {
      count: records[plan].length,
      active: records[plan].filter((row) => row.fields?.active !== false).length,
      under18: records[plan].length < 18
    };
  }
  return { ok: true, summary, records };
}
async function listView(view) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ view, pageSize: "100" });
    if (offset) params.set("offset", offset);
    const page2 = await airtable("GET", `?${params.toString()}`);
    records.push(...page2.records || []);
    offset = page2.offset || "";
  } while (offset);
  return records;
}
async function airtable(method, query = "", body) {
  const runtime = { ...globalThis.process?.env || {}, ...Object.assign(__vite_import_meta_env__, { AIRTABLE_BASE_ID: "apptdhhNzduxm5gjn", AIRTABLE_TOKEN: "patDeqY9NAQsuYx6q.7fd75026f0820373f62a72ca063f99b2203b9d873cb77aa3962637ab7bb0ec37", OS: "Windows_NT" }) || {}, ...env || {} };
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_BASE_ID || runtime.AIRTABLE_BASE || BASE_ID;
  if (!token) throw new Error("missing_airtable_token");
  const response = await fetch(`https://api.airtable.com/v0/${baseId}/${TABLE_ID}${query}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: body ? JSON.stringify(body) : void 0
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) throw new Error(JSON.stringify(data));
  return data;
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
