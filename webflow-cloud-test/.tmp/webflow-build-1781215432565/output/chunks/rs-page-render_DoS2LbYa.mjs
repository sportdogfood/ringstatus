globalThis.process ??= {};
globalThis.process.env ??= {};
import { r as runtimeEnv, j as json, c as corsHeaders } from "./wec-plan-modules_hXO0hoAk.mjs";
const config = {
  runtime: "edge"
};
const RSCOM_BASE_ID = "appDN3R51ZPmwgMib";
const TABLES = {
  pages: "rs_pages_index",
  blocks: "rs_page_blocks",
  divs: "rs_page_divs",
  typography: "rs_typography",
  content: "rs_content",
  navigation: "rs_navigation_items",
  globals: "rs_global_params"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ url }) => {
  const runtime = runtimeEnv();
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_RSCOM_BASE_ID || runtime.RSCOM_AIRTABLE_BASE_ID || RSCOM_BASE_ID;
  if (!token) return json({ ok: false, error: "missing_airtable_token" }, 500);
  const pageKey = clean(url.searchParams.get("pageKey") || "rs_home");
  try {
    const result = await buildPage({ token, baseId, pageKey });
    return json(result);
  } catch (error) {
    console.error("[rs-page-render] failed", error);
    return json({
      ok: false,
      error: "rs_page_render_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
async function buildPage({ token, baseId, pageKey }) {
  const [
    pages,
    blocks,
    divs,
    typography,
    content,
    navigation,
    globals
  ] = await Promise.all([
    listRecords({ token, baseId, tableName: TABLES.pages }),
    listRecords({ token, baseId, tableName: TABLES.blocks }),
    listRecords({ token, baseId, tableName: TABLES.divs }),
    listRecords({ token, baseId, tableName: TABLES.typography }),
    listRecords({ token, baseId, tableName: TABLES.content }),
    listRecords({ token, baseId, tableName: TABLES.navigation }),
    listRecords({ token, baseId, tableName: TABLES.globals })
  ]);
  const page2 = pages.find((record) => clean(record.fields.page_key) === pageKey);
  if (!page2) throw new Error(`page_not_found:${pageKey}`);
  const activeGlobals = globals.filter((record) => selectName(record.fields.active) === "active").sort(sortByOrder).map((record) => pick(record, ["param_key", "param_type", "param_value", "sort_order"]));
  const pageBlocks = blocks.filter((record) => includes(record.fields.page, page2.id)).filter((record) => isHierarchyBlock(record, pageKey)).filter((record) => selectName(record.fields.active) !== "inactive").sort(sortByOrder);
  const typographyByDiv = groupByFirstLink(typography.sort(sortByOrder), "div");
  const contentByTypography = groupByFirstLink(content.sort(sortByOrder), "typography");
  const divsByBlock = groupByFirstLink(divs.sort(sortByOrder), "block");
  const treeBlocks = pageBlocks.map((block) => ({
    id: block.id,
    ...pick(block, ["block_key", "block_type", "sort_order", "component_key", "html_key"]),
    divs: (divsByBlock.get(block.id) || []).map((div) => ({
      id: div.id,
      ...pick(div, ["div_key", "div_role", "sort_order", "class_key", "html_key"]),
      typography: (typographyByDiv.get(div.id) || []).map((typeRow) => ({
        id: typeRow.id,
        ...pick(typeRow, ["typography_key", "typography_role", "sort_order", "font_class", "data_rs_value"]),
        content: (contentByTypography.get(typeRow.id) || []).map((contentRow) => ({
          id: contentRow.id,
          ...pick(contentRow, ["content_key", "content_type", "content_value", "data_rs_value", "sort_order"])
        }))
      }))
    }))
  }));
  const tree = {
    page: pick(page2, ["page_key", "page_label", "webflow_slug"]),
    globals: activeGlobals,
    navigation: navigation.filter((record) => selectName(record.fields.active) === "active").sort(sortByOrder).map((record) => pick(record, ["nav_item_key", "nav_group", "label", "page_key", "href", "sort_order"])),
    blocks: treeBlocks
  };
  return {
    ok: true,
    source: {
      baseId,
      pageKey,
      mode: "airtable_rscom_page_hierarchy"
    },
    tree,
    html: renderPage(tree)
  };
}
async function listRecords({ token, baseId, tableName }) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!response.ok) throw new Error(`airtable_records_failed:${tableName}:${response.status}:${await response.text()}`);
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
function renderPage(tree) {
  const pageKey = clean(tree.page.page_key);
  const body = tree.blocks.map((block) => renderBlock(block, tree)).join("\n");
  return [
    `<main class="rs-page" data-rs-page="${escapeAttr(pageKey)}">`,
    body,
    `</main>`
  ].join("\n");
}
function renderBlock(block, tree) {
  const type = selectName(block.block_type);
  if (type === "navigation") return renderNavigation(block, tree.navigation, "main_nav");
  if (type === "footer") return renderNavigation(block, tree.navigation, "footer_nav");
  return renderSection(block);
}
function isHierarchyBlock(record, pageKey) {
  const key = clean(record.fields.block_key);
  const allowedKeys = /* @__PURE__ */ new Set([
    `${pageKey}_navigation`,
    `${pageKey}_section_1`,
    `${pageKey}_section_2`,
    `${pageKey}_footer`
  ]);
  return allowedKeys.has(key);
}
function renderNavigation(block, items, group) {
  const links = items.filter((item) => selectName(item.nav_group) === group).sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0)).map((item) => `<a class="rs-nav-link" href="${escapeAttr(item.href || "#")}" data-rs-page-key="${escapeAttr(item.page_key)}">${escapeHtml(item.label)}</a>`).join("");
  const tag = group === "footer_nav" ? "footer" : "nav";
  return [
    `<${tag} class="rs-${group.replace("_nav", "")}" data-rs-block="${escapeAttr(block.block_key)}">`,
    `  <div class="rs-nav-inner">`,
    links,
    `  </div>`,
    `</${tag}>`
  ].join("\n");
}
function renderSection(block) {
  const divHtml = block.divs.map((div) => {
    const className = clean(div.class_key) || "rs-content";
    const typeHtml = div.typography.map((typeRow) => {
      const content = first(typeRow.content)?.content_value || "";
      const role = selectName(typeRow.typography_role);
      const typeClass = clean(typeRow.font_class) || `rs-type-${role || "text"}`;
      return `<div class="${escapeAttr(typeClass)}" data-rs-role="${escapeAttr(role)}" data-rs-value="${escapeAttr(typeRow.data_rs_value || typeRow.typography_key)}">${escapeHtml(content)}</div>`;
    }).join("");
    return `<div class="${escapeAttr(className)}" data-rs-div="${escapeAttr(div.div_key)}">${typeHtml}</div>`;
  }).join("");
  return [
    `<section class="rs-section" data-rs-block="${escapeAttr(block.block_key)}">`,
    `  <div class="rs-section-container">`,
    `    <div class="rs-section-padding">`,
    divHtml,
    `    </div>`,
    `  </div>`,
    `</section>`
  ].join("\n");
}
function groupByFirstLink(records, fieldName) {
  const map = /* @__PURE__ */ new Map();
  for (const record of records) {
    const id = first(record.fields[fieldName]);
    if (!id) continue;
    if (!map.has(id)) map.set(id, []);
    map.get(id).push(record);
  }
  return map;
}
function pick(record, names) {
  return Object.fromEntries(names.map((name) => [name, record.fields?.[name] ?? ""]));
}
function sortByOrder(a, b) {
  return Number(a.fields?.sort_order || 0) - Number(b.fields?.sort_order || 0) || clean(a.fields?.block_key || a.fields?.div_key || a.fields?.typography_key || a.fields?.content_key || a.fields?.nav_item_key || a.fields?.param_key).localeCompare(clean(b.fields?.block_key || b.fields?.div_key || b.fields?.typography_key || b.fields?.content_key || b.fields?.nav_item_key || b.fields?.param_key));
}
function first(value) {
  return Array.isArray(value) ? value[0] : value;
}
function includes(list, value) {
  return Array.isArray(list) && list.includes(value);
}
function selectName(value) {
  if (value && typeof value === "object" && "name" in value) return clean(value.name);
  return clean(value);
}
function clean(value) {
  if (Array.isArray(value)) return value.map(clean).filter(Boolean).join(", ");
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
function escapeAttr(value) {
  return escapeHtml(value).replace(/`/g, "&#96;");
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
