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
  navigation: "rs_navigation_items",
  globals: "rs_global_params"
};
const PAGE_CACHE_TTL_MS = 5 * 60 * 1e3;
const DATASET_CACHE_TTL_MS = 30 * 1e3;
const pageCache = /* @__PURE__ */ new Map();
let datasetCache = null;
let datasetInflight = null;
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ url }) => {
  const runtime = runtimeEnv();
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_RSCOM_BASE_ID || runtime.RSCOM_AIRTABLE_BASE_ID || RSCOM_BASE_ID;
  if (!token) return json({ ok: false, error: "missing_airtable_token" }, 500);
  const pageKey = clean(url.searchParams.get("pageKey") || "rs_home");
  const mode = clean(url.searchParams.get("mode"));
  const refresh = clean(url.searchParams.get("refresh")) === "1";
  try {
    if (mode === "site_toggle") {
      const result2 = await buildSiteToggle({ token, baseId, pageKey });
      return renderJson(result2);
    }
    const result = await getCachedPage({ token, baseId, pageKey, refresh });
    return renderJson(result);
  } catch (error) {
    console.error("[rs-page-render] failed", error);
    return json({
      ok: false,
      error: "rs_page_render_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
async function renderRsPagePayload({ token, baseId, pageKey, refresh = false }) {
  return getCachedPage({ token, baseId, pageKey, refresh });
}
async function buildSiteToggle({ token, baseId, pageKey }) {
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
  const pageKeys = navigation.filter((record) => selectName(record.fields.active) === "active").filter((record) => selectName(record.fields.nav_group) === "main_nav").sort(sortByOrder).map((record) => clean(record.fields.page_key)).filter(Boolean);
  const uniquePageKeys = [...new Set(pageKeys)];
  const requestedPageKey = uniquePageKeys.includes(pageKey) ? pageKey : uniquePageKeys[0];
  const pageRecords = [requestedPageKey].map((key) => pages.find((record) => clean(record.fields.page_key) === key)).filter(Boolean);
  if (!pageRecords.length) throw new Error("site_pages_not_found");
  const typographyByDiv = groupByFirstLink(typography.sort(sortByOrder), "div");
  const contentByTypography = groupByFirstLink(content.sort(sortByOrder), "typography");
  const divsByBlock = groupByFirstLink(divs.sort(sortByOrder), "block");
  const pageTrees = pageRecords.map((page) => {
    const key = clean(page.fields.page_key);
    const pageBlocks = blocks.filter((record) => includes(record.fields.page, page.id)).filter((record) => isHierarchyBlock(record, key)).filter((record) => selectName(record.fields.active) !== "inactive").filter((record) => selectName(record.fields.block_type) !== "navigation").filter((record) => selectName(record.fields.block_type) !== "footer").sort(sortByOrder);
    return {
      page: pick(page, ["page_key", "page_label", "webflow_slug"]),
      blocks: buildTreeBlocks({ pageBlocks, divsByBlock, typographyByDiv, contentByTypography })
    };
  });
  const tree = {
    page: { page_key: pageKey, page_label: "Site Toggle", webflow_slug: "" },
    globals: globals.filter((record) => selectName(record.fields.active) === "active").sort(sortByOrder).map((record) => pick(record, ["param_key", "param_type", "param_value", "sort_order"])),
    navigation: navigation.filter((record) => selectName(record.fields.active) === "active").sort(sortByOrder).map((record) => pick(record, ["nav_item_key", "nav_group", "label", "page_key", "href", "sort_order"])),
    pages: pageTrees
  };
  return {
    ok: true,
    source: {
      baseId,
      pageKey,
      mode: "airtable_rscom_site_toggle"
    },
    tree,
    html: renderSiteToggle(tree, requestedPageKey)
  };
}
function renderJson(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "public, max-age=300, stale-while-revalidate=3600"
    }
  });
}
async function getCachedPage({ token, baseId, pageKey, refresh = false }) {
  const cacheKey = `${baseId}:${pageKey}`;
  const cached = pageCache.get(cacheKey);
  if (!refresh && cached && cached.expiresAt > Date.now()) {
    return {
      ...cached.payload,
      cache: {
        status: "hit",
        cachedAt: cached.cachedAt,
        expiresAt: cached.expiresAt
      }
    };
  }
  const dataset = await getDataset({ token, baseId, refresh });
  const payload = buildPageFromDataset({ baseId, pageKey, dataset });
  const now = Date.now();
  pageCache.set(cacheKey, {
    payload,
    cachedAt: new Date(now).toISOString(),
    expiresAt: now + PAGE_CACHE_TTL_MS
  });
  return {
    ...payload,
    cache: {
      status: "miss",
      cachedAt: new Date(now).toISOString(),
      expiresAt: now + PAGE_CACHE_TTL_MS
    }
  };
}
async function getDataset({ token, baseId, refresh = false }) {
  const now = Date.now();
  if (!refresh && datasetCache && datasetCache.baseId === baseId && datasetCache.expiresAt > now) {
    return datasetCache.payload;
  }
  if (!refresh && datasetInflight) return datasetInflight;
  datasetInflight = fetchDataset({ token, baseId }).then((payload) => {
    datasetCache = {
      baseId,
      payload,
      expiresAt: Date.now() + DATASET_CACHE_TTL_MS
    };
    return payload;
  }).finally(() => {
    datasetInflight = null;
  });
  return datasetInflight;
}
async function fetchDataset({ token, baseId }) {
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
  return { pages, blocks, divs, typography, content, navigation, globals };
}
function buildPageFromDataset({ baseId, pageKey, dataset }) {
  const { pages, blocks, divs, typography, content, navigation, globals } = dataset;
  const page = pages.find((record) => clean(record.fields.page_key) === pageKey);
  if (!page) throw new Error(`page_not_found:${pageKey}`);
  const activeGlobals = globals.filter((record) => selectName(record.fields.active) === "active").sort(sortByOrder).map((record) => pick(record, ["param_key", "param_type", "param_value", "sort_order"]));
  const pageBlocks = blocks.filter((record) => includes(record.fields.page, page.id)).filter((record) => isHierarchyBlock(record, pageKey)).filter((record) => selectName(record.fields.active) !== "inactive").sort(sortByOrder);
  const typographyByDiv = groupByFirstLink(typography.sort(sortByOrder), "div");
  const contentByTypography = groupByFirstLink(content.sort(sortByOrder), "typography");
  const divsByBlock = groupByFirstLink(divs.sort(sortByOrder), "block");
  const treeBlocks = buildTreeBlocks({ pageBlocks, divsByBlock, typographyByDiv, contentByTypography });
  const tree = {
    page: pick(page, ["page_key", "page_label", "webflow_slug"]),
    globals: activeGlobals,
    navigation: navigation.filter((record) => selectName(record.fields.active) === "active").sort(sortByOrder).map((record) => pick(record, ["nav_item_key", "nav_group", "label", "page_key", "href", "sort_order"])),
    blocks: treeBlocks
  };
  const prefetchPageKeys = getMainNavPageKeys(tree.navigation).filter((key) => key !== pageKey);
  return {
    ok: true,
    source: {
      baseId,
      pageKey,
      mode: "airtable_rscom_page_hierarchy"
    },
    prefetch: prefetchPageKeys.map((key) => `/test/rs-page-render?pageKey=${encodeURIComponent(key)}`),
    tree,
    html: renderPage(tree, prefetchPageKeys)
  };
}
function buildTreeBlocks({ pageBlocks, divsByBlock, typographyByDiv, contentByTypography }) {
  return pageBlocks.map((block) => ({
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
          ...pick(contentRow, ["content_key", "content_type", "content_value", "data_rs_value", "sort_order", "eyebrow", "headline", "body", "visual_label"])
        }))
      }))
    }))
  }));
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
function renderPage(tree, prefetchPageKeys = []) {
  const pageKey = clean(tree.page.page_key);
  const body = tree.blocks.map((block) => renderBlock(block, tree)).join("\n");
  return [
    `<main class="rs-page" data-rs-page="${escapeAttr(pageKey)}" data-rs-endpoint="/test/rs-page-render" data-rs-prefetch="${escapeAttr(prefetchPageKeys.join(","))}">`,
    renderBaseStyle(),
    renderPrefetchLinks(prefetchPageKeys),
    body,
    `</main>`
  ].join("\n");
}
function renderPrefetchLinks(pageKeys) {
  return pageKeys.filter(Boolean).map((key) => `<link rel="prefetch" as="fetch" href="/test/rs-page-render?pageKey=${escapeAttr(encodeURIComponent(key))}" crossorigin="anonymous">`).join("\n");
}
function getMainNavPageKeys(items) {
  return [...new Set(items.filter((item) => selectName(item.nav_group) === "main_nav").sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0)).map((item) => clean(item.page_key)).filter(Boolean))];
}
function renderSiteToggle(tree, activePageKey) {
  const activeKey = clean(activePageKey) || clean(first(tree.pages)?.page.page_key);
  const navItems = tree.navigation.filter((item) => selectName(item.nav_group) === "main_nav").sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0));
  const buttons = navItems.map((item) => {
    const itemPageKey = clean(item.page_key);
    const isActive = itemPageKey === activeKey;
    return `<button class="rs-site-toggle-button${isActive ? " is-active" : ""}" type="button" data-rs-toggle-page="${escapeAttr(itemPageKey)}" aria-expanded="${isActive ? "true" : "false"}" onclick="${escapeAttr(toggleClickHandler())}">${escapeHtml(item.label)}</button>`;
  }).join("");
  const panels = tree.pages.map((pageTree) => {
    const itemPageKey = clean(pageTree.page.page_key);
    const isActive = itemPageKey === activeKey;
    const blocks = pageTree.blocks.map((block) => renderSection(block)).join("\n");
    return [
      `<section class="rs-site-toggle-panel${isActive ? " is-active" : ""}" data-rs-page-panel="${escapeAttr(itemPageKey)}"${isActive ? "" : " hidden"}>`,
      blocks,
      `</section>`
    ].join("\n");
  }).join("\n");
  return [
    `<main class="rs-page rs-site-toggle" data-rs-mode="site_toggle" data-rs-page="${escapeAttr(activeKey)}">`,
    renderSiteToggleStyle(),
    `  <div class="rs-site-toggle-nav-shell">`,
    `    <div class="rs-section-container">`,
    `      <div class="rs-site-toggle-nav-inner">`,
    `        <nav class="rs-site-toggle-nav" aria-label="Page sections">`,
    buttons,
    `        </nav>`,
    `      </div>`,
    `    </div>`,
    `  </div>`,
    `  <div class="rs-site-toggle-panels">`,
    panels,
    `  </div>`,
    `</main>`
  ].join("\n");
}
function renderSiteToggleStyle() {
  return [
    `<style>`,
    `.rs-site-toggle{font-family:Outfit,Arial,sans-serif;color:#17263b;}`,
    `.rs-site-toggle-nav-shell{border-bottom:1px solid #d8dee6;}`,
    `.rs-site-toggle-nav-inner{padding:12px 0;}`,
    `.rs-site-toggle-nav{display:flex;gap:10px;align-items:center;overflow-x:auto;padding:12px 0;margin:0;}`,
    `.rs-site-toggle-button{appearance:none;border:1px solid #d8dee6;background:#fff;color:#17263b;border-radius:18px;padding:10px 18px;font:600 13px/1 Outfit,Arial,sans-serif;letter-spacing:.04em;text-transform:uppercase;white-space:nowrap;cursor:pointer;box-shadow:0 1px 2px rgba(15,23,42,.08);transition:background .16s ease,color .16s ease,box-shadow .16s ease,transform .16s ease;}`,
    `.rs-site-toggle-button:hover{box-shadow:0 4px 12px rgba(15,23,42,.14);transform:translateY(-1px);}`,
    `.rs-site-toggle-button.is-active{background:#10243b;color:#fff;border-color:#10243b;}`,
    `.rs-site-toggle-panel{border-top:1px solid #e5e9ef;padding:18px 0;}`,
    `.rs-section{padding:0 0 14px;}`,
    `.rs-section-padding{display:grid;gap:8px;}`,
    `.rs-content,[data-rs-role]{font:400 18px/1.35 Outfit,Arial,sans-serif;}`,
    `</style>`
  ].join("");
}
function toggleClickHandler() {
  return [
    `var root=this.closest('[data-rs-mode="site_toggle"]');`,
    "if(!root)return;",
    "var key=this.getAttribute('data-rs-toggle-page');",
    "var mount=root.closest('#rs-page-root');",
    "if(mount&&mount.__rsPageCache&&mount.__rsPageCache[key]){mount.innerHTML=mount.__rsPageCache[key];return;}",
    "var endpoint=new URL('https://ringstatus.com/test/rs-page-render');",
    "endpoint.searchParams.set('mode','site_toggle');",
    "endpoint.searchParams.set('pageKey',key);",
    "endpoint.searchParams.set('_',Date.now());",
    "if(mount)mount.setAttribute('data-rs-status','loading');",
    "root.setAttribute('data-rs-page',key);",
    "root.querySelectorAll('[data-rs-toggle-page]').forEach(function(btn){var on=btn.getAttribute('data-rs-toggle-page')===key;btn.classList.toggle('is-active',on);btn.setAttribute('aria-expanded',on?'true':'false');});",
    "fetch(endpoint.toString(),{cache:'no-store'}).then(function(response){return response.json();}).then(function(data){if(!data||!data.ok)throw new Error((data&&(data.detail||data.error))||'Render failed');if(mount){mount.__rsPageCache=mount.__rsPageCache||{};mount.__rsPageCache[key]=data.html||'';mount.innerHTML=data.html||'';mount.setAttribute('data-rs-status','ready');}}).catch(function(error){if(mount){mount.setAttribute('data-rs-status','failed');mount.textContent='Render failed: '+(error&&error.message?error.message:error);}});"
  ].join("");
}
function renderBlock(block, tree) {
  const type = selectName(block.block_type);
  if (type === "navigation") return renderNavigation(block, tree.navigation, "main_nav", clean(tree.page.page_key));
  if (type === "footer") return renderNavigation(block, tree.navigation, "footer_nav", clean(tree.page.page_key));
  return renderSection(block);
}
function isHierarchyBlock(record, pageKey) {
  const key = clean(record.fields.block_key);
  const prefixes = [pageKey, `rs_${pageKey}`];
  return prefixes.some((prefix) => key === `${prefix}_navigation` || key === `${prefix}_section_1` || key === `${prefix}_section_2` || key === `${prefix}_intro` || key === `${prefix}_grid` || key === `${prefix}_footer`);
}
function renderNavigation(block, items, group, activePageKey = "") {
  const links = items.filter((item) => selectName(item.nav_group) === group).sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0)).map((item) => {
    const itemPageKey = clean(item.page_key);
    const active = itemPageKey && itemPageKey === activePageKey;
    return `<a class="rs-nav-link${active ? " is-active" : ""}" href="${escapeAttr(item.href || "#")}" data-rs-page-key="${escapeAttr(itemPageKey)}"${active ? ` aria-current="page"` : ""}>${escapeHtml(item.label)}</a>`;
  }).join("");
  const tag = group === "footer_nav" ? "footer" : "nav";
  const logo = group === "main_nav" ? `<a class="rs-nav-logo" href="/rs/home" aria-label="RingStatus home">RING<span>STATUS</span></a>` : "";
  return [
    `<${tag} class="rs-${group.replace("_nav", "")}" data-rs-block="${escapeAttr(block.block_key)}">`,
    `  <div class="rs-nav-inner">`,
    logo,
    `    <div class="rs-nav-links">`,
    links,
    `    </div>`,
    `  </div>`,
    `</${tag}>`
  ].join("\n");
}
function renderSection(block) {
  const divHtml = block.divs.map((div) => {
    const className = clean(div.class_key) || "rs-content";
    const typeHtml = div.typography.map((typeRow) => {
      const contentRow = first(typeRow.content);
      const contentType = selectName(contentRow?.content_type);
      if (contentType === "html") {
        return renderStructuredContent(contentRow);
      }
      const role = selectName(typeRow.typography_role);
      const typeClass = clean(typeRow.font_class) || `rs-type-${role || "text"}`;
      const content = contentRow?.content_value || "";
      return `<div class="${escapeAttr(typeClass)}" data-rs-role="${escapeAttr(role)}" data-rs-value="${escapeAttr(typeRow.data_rs_value || typeRow.typography_key)}">${renderContent(content, contentType)}</div>`;
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
function renderContent(content, contentType) {
  if (contentType === "html") return sanitizeTrustedHtml(content);
  return escapeHtml(content);
}
function renderStructuredContent(contentRow) {
  const eyebrow = clean(contentRow?.eyebrow);
  const headline = clean(contentRow?.headline);
  const body = clean(contentRow?.body);
  const visualLabel = clean(contentRow?.visual_label);
  if (eyebrow || headline || body || visualLabel) {
    return [
      `<div>`,
      eyebrow ? `<h5>${escapeHtml(eyebrow)}</h5>` : "",
      headline ? `<h1>${escapeHtml(headline)}</h1>` : "",
      body ? `<p>${escapeHtml(body)}</p>` : "",
      `</div>`,
      visualLabel ? `<div class="rs-visual">${escapeHtml(visualLabel)}</div>` : ""
    ].join("");
  }
  return renderContent(contentRow?.content_value || "", selectName(contentRow?.content_type));
}
function sanitizeTrustedHtml(value) {
  return String(value ?? "").replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "").replace(/\son[a-z]+\s*=\s*(".*?"|'.*?'|[^\s>]+)/gi, "");
}
function renderBaseStyle() {
  return [
    `<style>`,
    `.rs-main{background:#050505;padding:18px 40px;border-bottom:1px solid #d8dee6;}`,
    `.rs-main .rs-nav-inner{display:flex;align-items:center;justify-content:space-between;gap:24px;width:100%;}`,
    `.rs-nav-logo{color:#fff;text-decoration:none;font:800 28px/1 Outfit,Arial,sans-serif;letter-spacing:-.02em;white-space:nowrap;}`,
    `.rs-nav-logo span{color:#56372d;}`,
    `.rs-nav-links{display:flex;align-items:center;justify-content:flex-end;gap:14px;min-width:0;overflow-x:auto;}`,
    `.rs-main .rs-nav-link{display:inline-flex;align-items:center;justify-content:center;border:1px solid #d8dee6;border-radius:18px;padding:10px 18px;background:#fff;color:#10243b;text-decoration:none;font:600 13px/1 Outfit,Arial,sans-serif;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap;box-shadow:0 1px 2px rgba(15,23,42,.16);}`,
    `.rs-main .rs-nav-link:hover{background:#f6f8fb;border-color:#c7d0db;}`,
    `.rs-main .rs-nav-link.is-active{background:#10243b;color:#fff;border-color:#10243b;}`,
    `.rs-content-flex{display:flex;align-items:center;justify-content:space-between;gap:32px;width:100%;}`,
    `.rs-content-flex>div:first-child{min-width:0;flex:1 1 auto;}`,
    `.rs-visual{display:flex;align-items:center;justify-content:center;min-height:220px;flex:0 0 min(38%,420px);border:1px solid #d8dee6;background:#f6f8fa;color:#65707d;}`,
    `@media(max-width:700px){.rs-main{padding:14px 16px}.rs-main .rs-nav-inner{align-items:flex-start}.rs-nav-logo{font-size:24px}.rs-content-flex{display:grid;gap:20px}.rs-visual{flex:auto;min-height:180px}}`,
    `</style>`
  ].join("");
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
  if (Array.isArray(value)) return selectName(first(value));
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
  config,
  renderRsPagePayload
}, Symbol.toStringTag, { value: "Module" }));
export {
  _page as _,
  renderRsPagePayload as r
};
