globalThis.process ??= {};
globalThis.process.env ??= {};
import { r as runtimeEnv, j as json, c as corsHeaders } from "./wec-plan-modules_BsQGnEh2.mjs";
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
  sections: "rs_section_inventory"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ url }) => {
  const runtime = runtimeEnv();
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_RSCOM_BASE_ID || runtime.RSCOM_AIRTABLE_BASE_ID || RSCOM_BASE_ID;
  if (!token) return json({ ok: false, error: "missing_airtable_token" }, 500);
  const pageKey = clean(url.searchParams.get("pageKey") || "home");
  const blockKey = clean(url.searchParams.get("blockKey") || "");
  try {
    const result = await buildSection({ token, baseId, pageKey, blockKey });
    return json(result);
  } catch (error) {
    console.error("[rs-dynamic-section] failed", error);
    return json({
      ok: false,
      error: "rs_dynamic_section_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
async function buildSection({ token, baseId, pageKey, blockKey }) {
  const [pages, blocks, divs, typography, content, sections] = await Promise.all([
    listRecords({ token, baseId, tableName: TABLES.pages }),
    listRecords({ token, baseId, tableName: TABLES.blocks }),
    listRecords({ token, baseId, tableName: TABLES.divs }),
    listRecords({ token, baseId, tableName: TABLES.typography }),
    listRecords({ token, baseId, tableName: TABLES.content }),
    listRecords({ token, baseId, tableName: TABLES.sections })
  ]);
  const page2 = pages.find((record) => clean(record.fields.page_key) === pageKey);
  if (!page2) throw new Error(`page_not_found:${pageKey}`);
  const pageBlocks = blocks.filter((record) => includes(record.fields.page, page2.id)).sort(sortByOrder);
  const block = blockKey ? pageBlocks.find((record) => clean(record.fields.block_key) === blockKey) : pageBlocks.find((record) => clean(record.fields.block_type) !== "navigation") || pageBlocks[0];
  if (!block) throw new Error(`block_not_found:${blockKey || "first_content_block"}`);
  const section = sections.find((record) => includes(block.fields.section_inventory, record.id)) || null;
  const blockDivs = divs.filter((record) => includes(record.fields.block, block.id)).sort(sortByOrder);
  const typographyByDiv = /* @__PURE__ */ new Map();
  for (const row of typography.sort(sortByOrder)) {
    const divId = first(row.fields.div);
    if (!divId) continue;
    if (!typographyByDiv.has(divId)) typographyByDiv.set(divId, []);
    typographyByDiv.get(divId).push(row);
  }
  const contentByTypography = /* @__PURE__ */ new Map();
  for (const row of content.sort(sortByOrder)) {
    const typographyId = first(row.fields.typography);
    if (!typographyId) continue;
    if (!contentByTypography.has(typographyId)) contentByTypography.set(typographyId, []);
    contentByTypography.get(typographyId).push(row);
  }
  const tree = {
    page: pick(page2, ["page_key", "page_label", "webflow_slug"]),
    block: pick(block, ["block_key", "block_type", "sort_order", "component_key", "html_key"]),
    section: section ? pick(section, ["section_key", "section_label", "section_family", "modifier_classes"]) : null,
    divs: blockDivs.map((div) => ({
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
  };
  return {
    ok: true,
    source: {
      baseId,
      pageKey,
      blockKey: clean(block.fields.block_key),
      mode: "airtable_rscom"
    },
    tree,
    html: renderSection(tree)
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
function renderSection(tree) {
  const sectionClasses = clean(tree.section?.modifier_classes) || "rs-section";
  const title = clean(tree.section?.section_label || tree.block.block_key || "Dynamic Section");
  const divHtml = tree.divs.map((div) => {
    const className = clean(div.class_key) || "rs-content";
    const typeHtml = div.typography.map((typeRow) => {
      const content = first(typeRow.content)?.content_value || "";
      const role = clean(typeRow.typography_role);
      const tag = role === "primary" ? "h2" : "p";
      const typeClass = clean(typeRow.font_class) || `rs-type-${role || "text"}`;
      return `<${tag} class="${escapeAttr(typeClass)}" data-rs-value="${escapeAttr(typeRow.data_rs_value || typeRow.typography_key)}">${escapeHtml(content)}</${tag}>`;
    }).join("");
    return `<div class="${escapeAttr(className)}" data-rs-div="${escapeAttr(div.div_key)}">${typeHtml}</div>`;
  }).join("");
  return [
    `<section class="${escapeAttr(sectionClasses)}" data-rs-dynamic-section="${escapeAttr(tree.block.block_key)}">`,
    `  <div class="rs-section-container">`,
    `    <div class="rs-section-padding">`,
    `      <div class="rs-content-container">`,
    `        <div class="rs-content-flex" aria-label="${escapeAttr(title)}">`,
    divHtml,
    `        </div>`,
    `      </div>`,
    `    </div>`,
    `  </div>`,
    `</section>`
  ].join("\n");
}
function pick(record, names) {
  return Object.fromEntries(names.map((name) => [name, record.fields?.[name] ?? ""]));
}
function sortByOrder(a, b) {
  return Number(a.fields?.sort_order || 0) - Number(b.fields?.sort_order || 0) || clean(a.fields?.block_key || a.fields?.div_key || a.fields?.typography_key || a.fields?.content_key).localeCompare(clean(b.fields?.block_key || b.fields?.div_key || b.fields?.typography_key || b.fields?.content_key));
}
function first(value) {
  return Array.isArray(value) ? value[0] : value;
}
function includes(list, value) {
  return Array.isArray(list) && list.includes(value);
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
