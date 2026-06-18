export const config = {
  runtime: "edge"
};

import {
  corsHeaders,
  json,
  runtimeEnv
} from "../lib/wec-plan-modules.js";

const RSCOM_BASE_ID = "appDN3R51ZPmwgMib";

const TABLES = {
  pages: "rs_pages_index",
  blocks: "rs_page_blocks",
  divs: "rs_page_divs",
  typography: "rs_typography",
  content: "rs_content",
  repeatableItemTypes: "rs_repeatable_item_types",
  navigation: "rs_navigation_items",
  globals: "rs_global_params"
};

const PAGE_CACHE_TTL_MS = 5 * 60 * 1000;
const DATASET_CACHE_TTL_MS = 30 * 1000;
const pageCache = new Map();
let datasetCache = null;
let datasetInflight = null;

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ url }) => {
  const runtime = runtimeEnv();
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.AIRTABLE_RSCOM_BASE_ID || runtime.RSCOM_AIRTABLE_BASE_ID || RSCOM_BASE_ID;
  if (!token) return json({ ok: false, error: "missing_airtable_token" }, 500);

  const pageKey = clean(url.searchParams.get("pageKey") || "rs_home");
  const mode = clean(url.searchParams.get("mode"));
  const refresh = clean(url.searchParams.get("refresh")) === "1";

  try {
    if (mode === "site_toggle") {
      const result = await buildSiteToggle({ token, baseId, pageKey });
      return renderJson(result);
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

export async function renderRsPagePayload({ token, baseId, pageKey, refresh = false }) {
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

  const pageKeys = navigation
    .filter((record) => selectName(record.fields.active) === "active")
    .filter((record) => selectName(record.fields.nav_group) === "main_nav")
    .sort(sortByOrder)
    .map((record) => clean(record.fields.page_key))
    .filter(Boolean);

  const uniquePageKeys = [...new Set(pageKeys)];
  const requestedPageKey = uniquePageKeys.includes(pageKey) ? pageKey : uniquePageKeys[0];
  const pageRecords = [requestedPageKey]
    .map((key) => pages.find((record) => clean(record.fields.page_key) === key))
    .filter(Boolean);

  if (!pageRecords.length) throw new Error("site_pages_not_found");

  const typographyByDiv = groupByFirstLink(typography.sort(sortByOrder), "div");
  const contentByTypography = groupByFirstLink(content.sort(sortByOrder), "typography");
  const divsByBlock = groupByFirstLink(divs.sort(sortByOrder), "block");

  const pageTrees = pageRecords.map((page) => {
    const key = clean(page.fields.page_key);
    const pageBlocks = blocks
      .filter((record) => includes(record.fields.page, page.id))
      .filter((record) => isHierarchyBlock(record, key))
      .filter((record) => selectName(record.fields.active) === "active")
      .filter((record) => selectName(record.fields.block_type) !== "navigation")
      .filter((record) => selectName(record.fields.block_type) !== "footer")
      .sort(sortByOrder);

    return {
      page: pick(page, ["page_key", "page_label", "webflow_slug"]),
      blocks: buildTreeBlocks({ pageBlocks, divsByBlock, typographyByDiv, contentByTypography })
    };
  });

  const tree = {
    page: { page_key: pageKey, page_label: "Site Toggle", webflow_slug: "" },
    globals: globals
      .filter((record) => selectName(record.fields.active) === "active")
      .sort(sortByOrder)
      .map((record) => pick(record, ["param_key", "param_type", "param_value", "sort_order"])),
    navigation: navigation
      .filter((record) => selectName(record.fields.active) === "active")
      .sort(sortByOrder)
      .map((record) => pick(record, ["nav_item_key", "nav_group", "label", "page_key", "href", "sort_order"])),
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

  datasetInflight = fetchDataset({ token, baseId })
    .then((payload) => {
      datasetCache = {
        baseId,
        payload,
        expiresAt: Date.now() + DATASET_CACHE_TTL_MS
      };
      return payload;
    })
    .finally(() => {
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
    repeatableItemTypes,
    navigation,
    globals
  ] = await Promise.all([
    listRecords({ token, baseId, tableName: TABLES.pages }),
    listRecords({ token, baseId, tableName: TABLES.blocks }),
    listRecords({ token, baseId, tableName: TABLES.divs }),
    listRecords({ token, baseId, tableName: TABLES.typography }),
    listRecords({ token, baseId, tableName: TABLES.content }),
    listRecords({ token, baseId, tableName: TABLES.repeatableItemTypes }),
    listRecords({ token, baseId, tableName: TABLES.navigation }),
    listRecords({ token, baseId, tableName: TABLES.globals })
  ]);
  return { pages, blocks, divs, typography, content, repeatableItemTypes, navigation, globals };
}

async function buildPage({ token, baseId, pageKey }) {
  const dataset = await fetchDataset({ token, baseId });
  return buildPageFromDataset({ baseId, pageKey, dataset });
}

function buildPageFromDataset({ baseId, pageKey, dataset }) {
  const { pages, blocks, divs, typography, content, repeatableItemTypes = [], navigation, globals } = dataset;

  const page = pages.find((record) => clean(record.fields.page_key) === pageKey);
  if (!page) throw new Error(`page_not_found:${pageKey}`);

  const activeGlobals = globals
    .filter((record) => selectName(record.fields.active) === "active")
    .sort(sortByOrder)
    .map((record) => pick(record, ["param_key", "param_type", "param_value", "sort_order"]));

  const pageBlocks = blocks
    .filter((record) => includes(record.fields.page, page.id))
    .filter((record) => isHierarchyBlock(record, pageKey))
    .filter((record) => selectName(record.fields.active) === "active")
    .sort(sortByOrder);

  const typographyByDiv = groupByFirstLink(typography.sort(sortByOrder), "div");
  const contentByTypography = groupByFirstLink(content.sort(sortByOrder), "typography");
  const divsByBlock = groupByFirstLink(divs.sort(sortByOrder), "block");

  const treeBlocks = buildTreeBlocks({ pageBlocks, divsByBlock, typographyByDiv, contentByTypography });

  const tree = {
    page: pick(page, ["page_key", "page_label", "webflow_slug"]),
    globals: activeGlobals,
    navigation: navigation
      .filter((record) => selectName(record.fields.active) === "active")
      .sort(sortByOrder)
      .map((record) => pick(record, ["nav_item_key", "nav_group", "label", "page_key", "href", "sort_order"])),
    repeatableItemTypes: repeatableItemTypes
      .filter((record) => selectName(record.fields.active) === "active")
      .sort(sortByOrder)
      .map((record) => pick(record, ["item_type_key", "item_type_label", "parent_slot_role", "default_class"])),
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
          ...pick(contentRow, ["content_key", "content_type", "content_value", "data_rs_value", "sort_order", "eyebrow", "headline", "body", "visual_label", "visual_src", "visual_alt", "visual_type"])
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
    renderRuntimeOverrideStyle(),
    `</main>`
  ].join("\n");
}

function renderRuntimeOverrideStyle() {
  return [
    `<style>`,
    `.rs-page #data-drawer-menu.rs-drawer.is-open{transform:translateX(0);}`,
    `.rs-page #data-nav-scrim.rs-nav-scrim.is-open{display:block;}`,
    `</style>`
  ].join("");
}

function renderPrefetchLinks(pageKeys) {
  return pageKeys
    .filter(Boolean)
    .map((key) => `<link rel="prefetch" as="fetch" href="/test/rs-page-render?pageKey=${escapeAttr(encodeURIComponent(key))}" crossorigin="anonymous">`)
    .join("\n");
}

function getMainNavPageKeys(items) {
  return [...new Set(items
    .filter((item) => selectName(item.nav_group) === "main_nav" || selectName(item.nav_group) === "app_nav")
    .sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0))
    .map((item) => clean(item.page_key))
    .filter(Boolean))];
}

function renderSiteToggle(tree, activePageKey) {
  const activeKey = clean(activePageKey) || clean(first(tree.pages)?.page.page_key);
  const navItems = tree.navigation
    .filter((item) => selectName(item.nav_group) === "main_nav")
    .sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0));

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
    "var root=this.closest('[data-rs-mode=\"site_toggle\"]');",
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
  if (isRepeatableBlock(block, tree.repeatableItemTypes)) return renderRepeatableSection(block);
  return renderSection(block);
}

function isRepeatableBlock(block, repeatableItemTypes = []) {
  const htmlKey = clean(block.html_key);
  const componentKey = clean(block.component_key);
  return htmlKey === "sticky_scroll_cards" ||
    repeatableItemTypes.some((item) => clean(item.item_type_key) === componentKey);
}

function isHierarchyBlock(record, pageKey) {
  const key = clean(record.fields.block_key);
  const prefixes = [pageKey, `rs_${pageKey}`];
  return prefixes.some((prefix) => key === `${prefix}_navigation` ||
    key === `${prefix}_section_1` ||
    key === `${prefix}_section_2` ||
    key === `${prefix}_intro` ||
    key === `${prefix}_grid` ||
    key === `${prefix}_footer`);
}

function renderNavigation(block, items, group, activePageKey = "") {
  const appItems = items
    .filter((item) => selectName(item.nav_group) === "app_nav")
    .sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0));
  if (group === "main_nav") return renderMainNavigation(block, appItems, activePageKey);

  const links = items
    .filter((item) => selectName(item.nav_group) === group)
    .sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0))
    .map((item) => {
      const itemPageKey = clean(item.page_key);
      const active = itemPageKey && itemPageKey === activePageKey;
      if (group === "main_nav" && itemPageKey === "rs_apps" && appItems.length) {
        const appLinks = appItems.map((appItem) => {
          const appPageKey = clean(appItem.page_key);
          const appActive = appPageKey && appPageKey === activePageKey;
          return `<a class="rs-nav-menu-link${appActive ? " is-active" : ""}" href="${escapeAttr(appItem.href || "#")}" data-rs-page-key="${escapeAttr(appPageKey)}"${appActive ? ` aria-current="page"` : ""}>${escapeHtml(appItem.label)}</a>`;
        }).join("");
        return [
          `<div class="rs-nav-menu" data-rs-nav-menu="apps">`,
          `  <button class="rs-nav-link rs-nav-menu-trigger${active ? " is-active" : ""}" type="button" data-rs-menu-trigger="apps" aria-expanded="false">${escapeHtml(item.label)}</button>`,
          `  <div class="rs-nav-menu-panel" data-rs-menu-panel="apps">`,
          appLinks,
          `  </div>`,
          `</div>`
        ].join("");
      }
      return `<a class="rs-nav-link${active ? " is-active" : ""}" href="${escapeAttr(item.href || "#")}" data-rs-page-key="${escapeAttr(itemPageKey)}"${active ? ` aria-current="page"` : ""}>${escapeHtml(item.label)}</a>`;
    })
    .join("");
  const tag = group === "footer_nav" ? "footer" : "nav";
  const logo = group === "main_nav"
    ? `<a class="rs-nav-logo" href="/rs/home" aria-label="RingStatus home">RING<span>STATUS</span></a>`
    : "";
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

function renderMainNavigation(block, appItems, activePageKey = "") {
  const columns = [
    {
      kicker: "Core",
      items: appItems.filter((item) => ["rs_schedules", "rs_waze", "rs_alerts", "rs-alerts", "rs_twoway"].includes(clean(item.page_key)))
    },
    {
      kicker: "Boards",
      items: appItems.filter((item) => ["rs_onsite", "rs_boards", "rs-boards"].includes(clean(item.page_key)))
    },
    {
      kicker: "Support",
      items: appItems.filter((item) => ["rs-pak", "rs_pak", "rs-reminders", "rs_reminders"].includes(clean(item.page_key)))
    }
  ].map((column) => ({
    ...column,
    items: column.items.length ? column.items : []
  }));

  const usedKeys = new Set(columns.flatMap((column) => column.items.map((item) => clean(item.page_key))));
  const uncategorized = appItems.filter((item) => !usedKeys.has(clean(item.page_key)));
  if (uncategorized.length) {
    columns.push({ kicker: "More", items: uncategorized });
  }

  const appActive = appItems.some((item) => clean(item.page_key) === activePageKey);
  const appColumns = columns
    .filter((column) => column.items.length)
    .map((column) => [
      `<div class="rs-mega-col">`,
      `  <p class="rs-content-kicker">${escapeHtml(column.kicker)}</p>`,
      column.items.map((item) => {
        const itemPageKey = clean(item.page_key);
        const active = itemPageKey && itemPageKey === activePageKey;
        return `  <a class="rs-mega-link${active ? " is-active" : ""}" href="${escapeAttr(item.href || "#")}" data-rs-page-key="${escapeAttr(itemPageKey)}"${active ? ` aria-current="page"` : ""}>${escapeHtml(item.label)}</a>`;
      }).join("\n"),
      `</div>`
    ].join("\n"))
    .join("\n");

  return [
    `<nav class="rs-nav" id="data-nav-root" data-rs-block="${escapeAttr(block.block_key)}" aria-label="Main navigation">`,
    `  <div class="rs-nav-inner">`,
    `    <a class="rs-logo" href="/rs/home" data-rs-page-key="rs_home" aria-label="RingStatus home">RS</a>`,
    `    <div class="rs-nav-links">`,
    `      <button class="rs-nav-button${appActive ? " is-active" : ""}" id="data-mega-toggle" type="button" data-rs-mega-trigger aria-expanded="false" aria-controls="data-mega-menu">Apps</button>`,
    `      <button class="rs-nav-button" id="data-drawer-toggle" type="button" data-rs-drawer-trigger aria-expanded="false" aria-controls="data-drawer-menu">Tools</button>`,
    `      <a class="rs-nav-link" href="#">Pricing</a>`,
    `      <a class="rs-nav-link${activePageKey === "rs_contact" ? " is-active" : ""}" href="/rs/contact" data-rs-page-key="rs_contact"${activePageKey === "rs_contact" ? ` aria-current="page"` : ""}>Contact</a>`,
    `    </div>`,
    `  </div>`,
    `  <div class="rs-mega" id="data-mega-menu" aria-hidden="true">`,
    `    <div class="rs-mega-inner">`,
    `      <div class="rs-mega-intro">`,
    `        <p class="rs-content-kicker">Mega Dropdown</p>`,
    `        <h2 class="rs-mega-title">Full Width Apps Menu</h2>`,
    `        <p class="rs-mega-text">This dropdown fills the full viewport width while the inner content stays capped to the same page container.</p>`,
    `      </div>`,
    appColumns,
    `    </div>`,
    `  </div>`,
    `</nav>`,
    `<div class="rs-nav-scrim" id="data-nav-scrim" aria-hidden="true"></div>`,
    `<aside class="rs-drawer" id="data-drawer-menu" aria-hidden="true" aria-label="Tools drawer">`,
    `  <div class="rs-drawer-inner">`,
    `    <div class="rs-drawer-head">`,
    `      <div>`,
    `        <p class="rs-content-kicker">Right Drawer</p>`,
    `        <h2 class="rs-drawer-title">Tools Menu</h2>`,
    `        <p class="rs-drawer-text">This opens from the right and locks the viewport. There is no separate mobile version.</p>`,
    `      </div>`,
    `      <button class="rs-drawer-close" id="data-drawer-close" type="button" data-rs-drawer-close aria-label="Close tools drawer">×</button>`,
    `    </div>`,
    `    <div class="rs-drawer-list">`,
    `      <a href="/rs/waze" data-rs-page-key="rs_waze">RingWaze</a>`,
    `      <a href="/rs/boards" data-rs-page-key="rs-boards">BarnTools</a>`,
    `      <a href="/rs/pak" data-rs-page-key="rs-pak">Packing Lists</a>`,
    `      <a href="/rs/reminders" data-rs-page-key="rs-reminders">Reminders</a>`,
    `      <a href="/rs/home" data-rs-page-key="rs_home">Account Dashboard</a>`,
    `    </div>`,
    `  </div>`,
    `</aside>`
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

function renderRepeatableSection(block) {
  const cards = block.divs
    .flatMap((div) => div.typography)
    .flatMap((typeRow) => typeRow.content)
    .sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0))
    .filter((contentRow) => selectName(contentRow.active) !== "inactive");

  const [featured, ...scrollCards] = cards;
  if (!featured) return renderSection(block);

  return [
    `<section class="rs-section rs-sticky-cards-section" data-rs-block="${escapeAttr(block.block_key)}" data-rs-repeatable="${escapeAttr(block.component_key)}">`,
    `  <div class="rs-section-container">`,
    `    <div class="rs-section-padding">`,
    `      <div class="rs-sticky-cards">`,
    `        <article class="rs-sticky-card is-featured">${renderCardContent(featured)}</article>`,
    `        <div class="rs-scroll-cards">`,
    scrollCards.map((contentRow) => `          <article class="rs-scroll-card">${renderCardContent(contentRow)}</article>`).join("\n"),
    `        </div>`,
    `      </div>`,
    `    </div>`,
    `  </div>`,
    `</section>`
  ].join("\n");
}

function renderCardContent(contentRow) {
  const eyebrow = clean(contentRow?.eyebrow);
  const headline = clean(contentRow?.headline);
  const body = clean(contentRow?.body);
  const visualLabel = clean(contentRow?.visual_label);
  const visualSrc = clean(contentRow?.visual_src);
  const visualAlt = clean(contentRow?.visual_alt) || visualLabel || headline;
  const visualType = selectName(contentRow?.visual_type);
  return [
    `<div class="rs-card-copy">`,
    eyebrow ? `<h5>${escapeHtml(eyebrow)}</h5>` : "",
    headline ? `<h2>${escapeHtml(headline)}</h2>` : "",
    body ? `<p>${escapeHtml(body)}</p>` : "",
    `</div>`,
    renderVisual({ visualSrc, visualAlt, visualLabel, visualType })
  ].join("");
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
  const visualSrc = clean(contentRow?.visual_src);
  const visualAlt = clean(contentRow?.visual_alt) || visualLabel || headline;
  const visualType = selectName(contentRow?.visual_type);
  const visualHtml = renderVisual({ visualSrc, visualAlt, visualLabel, visualType });
  if (eyebrow || headline || body || visualLabel || visualSrc) {
    return [
      `<div>`,
      eyebrow ? `<h5>${escapeHtml(eyebrow)}</h5>` : "",
      headline ? `<h1>${escapeHtml(headline)}</h1>` : "",
      body ? `<p>${escapeHtml(body)}</p>` : "",
      `</div>`,
      visualHtml
    ].join("");
  }
  return renderContent(contentRow?.content_value || "", selectName(contentRow?.content_type));
}

function renderVisual({ visualSrc, visualAlt, visualLabel, visualType }) {
  if (visualSrc) {
    if (visualType === "video") {
      return `<div class="rs-visual"><iframe src="${escapeAttr(visualSrc)}" title="${escapeAttr(visualAlt || "Embedded video")}" loading="lazy" allowfullscreen></iframe></div>`;
    }
    return `<div class="rs-visual"><img src="${escapeAttr(visualSrc)}" alt="${escapeAttr(visualAlt)}" loading="lazy"></div>`;
  }
  return visualLabel ? `<div class="rs-visual">${escapeHtml(visualLabel)}</div>` : "";
}

function sanitizeTrustedHtml(value) {
  return String(value ?? "")
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "")
    .replace(/\son[a-z]+\s*=\s*(".*?"|'.*?'|[^\s>]+)/gi, "");
}

function renderBaseStyle() {
  return [
    `<style>`,
    `.rs-main{position:relative;z-index:10;background:#050505;padding:0;border-bottom:1px solid #d8dee6;}`,
    `.rs-nav{position:relative;z-index:30;background:#050505;color:#fff;}`,
    `.rs-nav-inner{display:flex;align-items:center;justify-content:space-between;gap:24px;width:min(100%,1520px);margin:0 auto;padding:22px 40px;}`,
    `.rs-logo{display:inline-flex;align-items:center;justify-content:center;min-width:54px;color:#fff;text-decoration:none;font:800 32px/1 Outfit,Arial,sans-serif;letter-spacing:-.04em;white-space:nowrap;}`,
    `.rs-nav-links{display:flex;align-items:center;justify-content:flex-end;gap:14px;min-width:0;overflow-x:auto;scrollbar-width:none;}`,
    `.rs-nav-links::-webkit-scrollbar{display:none;}`,
    `.rs-nav-button,.rs-nav-link{display:inline-flex;align-items:center;justify-content:center;min-height:44px;border:1px solid #d8dee6;border-radius:999px;padding:0 22px;background:#fff;color:#10243b;text-decoration:none;font:600 13px/1 Outfit,Arial,sans-serif;text-transform:uppercase;letter-spacing:.08em;white-space:nowrap;box-shadow:0 2px 10px rgba(15,23,42,.18);cursor:pointer;}`,
    `.rs-nav-button:hover,.rs-nav-link:hover{background:#f6f8fb;border-color:#c7d0db;transform:translateY(-1px);}`,
    `.rs-nav-button.is-active,.rs-nav-link.is-active{background:#10243b;color:#fff;border-color:#10243b;}`,
    `.rs-mega{position:absolute;left:0;right:0;top:100%;z-index:35;display:none;background:#fff;color:#10243b;border-top:1px solid #d8dee6;border-bottom:1px solid #d8dee6;box-shadow:0 24px 60px rgba(15,23,42,.18);}`,
    `.rs-nav.is-mega-open .rs-mega{display:block;}`,
    `.rs-mega-inner{display:grid;grid-template-columns:minmax(240px,1.1fr) repeat(3,minmax(160px,.8fr));gap:28px;width:min(100%,1520px);margin:0 auto;padding:32px 40px;}`,
    `.rs-content-kicker{margin:0 0 10px;color:#65707d;font:700 12px/1 Outfit,Arial,sans-serif;text-transform:uppercase;letter-spacing:.1em;}`,
    `.rs-mega-title,.rs-drawer-title{margin:0;color:#050505;font:700 34px/.94 Outfit,Arial,sans-serif;letter-spacing:-.03em;}`,
    `.rs-mega-text,.rs-drawer-text{margin:12px 0 0;color:#46515f;font:400 15px/1.35 Outfit,Arial,sans-serif;}`,
    `.rs-mega-col{display:grid;align-content:start;gap:8px;}`,
    `.rs-mega-link{display:flex;align-items:center;min-height:42px;border-radius:14px;padding:0 12px;color:#10243b;text-decoration:none;font:600 14px/1 Outfit,Arial,sans-serif;}`,
    `.rs-mega-link:hover,.rs-mega-link.is-active{background:#10243b;color:#fff;}`,
    `.rs-nav-scrim{position:fixed;inset:0;z-index:25;display:none;background:rgba(15,23,42,.32);}`,
    `.rs-nav-scrim.is-open{display:block;}`,
    `.rs-drawer{position:fixed;inset:0 0 0 auto;z-index:40;width:min(440px,100vw);background:#fff;color:#10243b;box-shadow:-24px 0 60px rgba(15,23,42,.25);transform:translateX(105%);transition:transform .22s ease;}`,
    `.rs-drawer.is-open{transform:translateX(0);}`,
    `.rs-drawer-inner{display:flex;flex-direction:column;gap:26px;height:100%;padding:28px;overflow:auto;}`,
    `.rs-drawer-head{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;}`,
    `.rs-drawer-close{display:inline-flex;align-items:center;justify-content:center;width:46px;height:46px;border:1px solid #d8dee6;border-radius:50%;background:#f6f8fb;color:#050505;font:700 32px/1 Outfit,Arial,sans-serif;cursor:pointer;}`,
    `.rs-drawer-list{display:grid;gap:10px;}`,
    `.rs-drawer-list a{display:flex;align-items:center;min-height:48px;border:1px solid #d8dee6;border-radius:16px;padding:0 14px;color:#10243b;text-decoration:none;font:600 14px/1 Outfit,Arial,sans-serif;text-transform:uppercase;letter-spacing:.06em;}`,
    `.rs-drawer-list a:hover{background:#10243b;color:#fff;border-color:#10243b;}`,
    `.rs-content-flex{display:flex;align-items:center;justify-content:space-between;gap:32px;width:100%;}`,
    `.rs-content-flex>div:first-child{min-width:0;flex:1 1 auto;}`,
    `.rs-visual{display:flex;align-items:center;justify-content:center;flex:0 0 min(38%,460px);width:min(100%,460px);aspect-ratio:4/5;overflow:hidden;border:1px solid #d8dee6;border-radius:22px;background:#f6f8fa;color:#65707d;}`,
    `.rs-visual img,.rs-visual iframe{display:block;width:100%;height:100%;min-height:0;border:0;object-fit:cover;}`,
    `.rs-sticky-cards{width:100%;display:grid;grid-template-columns:minmax(0,0.9fr) minmax(0,1.1fr);gap:32px;align-items:start;}`,
    `.rs-sticky-card,.rs-scroll-card{border:1px solid #d8dee6;border-radius:22px;background:#fff;padding:24px;box-shadow:0 14px 38px rgba(15,23,42,.08);}`,
    `.rs-sticky-card{position:sticky;top:104px;min-height:420px;display:flex;flex-direction:column;justify-content:space-between;}`,
    `.rs-scroll-cards{display:grid;gap:24px;}`,
    `.rs-scroll-card{min-height:260px;display:flex;flex-direction:column;justify-content:space-between;}`,
    `.rs-card-copy h5{margin:0 0 14px;color:#65707d;font:700 13px/1 Outfit,Arial,sans-serif;letter-spacing:.08em;text-transform:uppercase;}`,
    `.rs-card-copy h2{margin:0;color:#050505;font:700 clamp(30px,4vw,58px)/.94 Outfit,Arial,sans-serif;letter-spacing:-.04em;}`,
    `.rs-card-copy p{margin:18px 0 0;color:#2b2b2b;font:400 clamp(17px,1.35vw,22px)/1.24 Outfit,Arial,sans-serif;}`,
    `@media(max-width:700px){.rs-nav-inner{display:grid;grid-template-columns:auto minmax(0,1fr);padding:18px 16px}.rs-logo{justify-content:flex-start;font-size:28px}.rs-nav-links{justify-content:flex-start}.rs-nav-button,.rs-nav-link{flex:0 0 auto;min-height:42px;padding:0 18px}.rs-mega-inner{display:grid;grid-template-columns:1fr;gap:20px;padding:24px 16px}.rs-drawer{width:100vw}.rs-content-flex{display:flex;flex-direction:column;align-items:stretch;gap:20px}.rs-visual{width:100%;flex:auto;aspect-ratio:16/10;min-height:0}}`,
    `@media(max-width:700px){.rs-sticky-cards{display:flex;gap:18px;overflow-x:auto;scroll-snap-type:x mandatory}.rs-sticky-card,.rs-scroll-card{position:relative;top:auto;flex:0 0 82%;min-height:360px;scroll-snap-align:start}.rs-card-copy h2{font-size:36px}}`,
    `</style>`
  ].join("");
}

function groupByFirstLink(records, fieldName) {
  const map = new Map();
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
  return Number(a.fields?.sort_order || 0) - Number(b.fields?.sort_order || 0) ||
    clean(a.fields?.block_key || a.fields?.div_key || a.fields?.typography_key || a.fields?.content_key || a.fields?.nav_item_key || a.fields?.param_key)
      .localeCompare(clean(b.fields?.block_key || b.fields?.div_key || b.fields?.typography_key || b.fields?.content_key || b.fields?.nav_item_key || b.fields?.param_key));
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
