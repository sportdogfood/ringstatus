(function () {
  "use strict";
  const config = window.RS_AG_REPORT_CONFIG;
  const root = document.getElementById("packing-app");
  if (!config || !root) throw new Error("AG report configuration or #packing-app is missing.");
  root.classList.add("ag-report-root");

  const state = { gridApi: null, rows: [], filteredRows: [], visibleRows: [], context: {}, focusMode: false, ringAnchor: "", filterPanelOpen: false, activeHorseFilters: new Set(), horseText: "", report: {} };
  const escapeHtml = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
  const normalize = value => String(value || "").toLowerCase().replace(/[^a-z0-9']+/g, " ").trim();
  const helpers = { escapeHtml, normalize };
  let api;
  let dialogSave = null;

  function renderShell() {
    root.innerHTML = `<section class="ag-report-shell lp-section-block lp-theme-classes" data-state="loading">
      <header class="lp-section-title ag-report-head">
        <div class="ag-report-title-copy"><h3>${escapeHtml(config.title)}</h3><p class="ag-report-meta" id="reportMeta">Loading...</p></div>
        <span class="lp-section-actions" id="headerActions"></span>
      </header>
      <section class="ag-action-bar"><div class="ag-action-group" id="primaryActions"></div><div class="ag-action-group" id="secondaryActions"></div></section>
      <nav class="ag-anchor-bar" id="ringAnchors" aria-label="Ring anchors"></nav>
      <main class="ag-grid-frame"><div id="agBaseGrid" class="ag-theme-quartz"></div></main>
      <section class="ag-filter-bar" id="filterBar" hidden><div class="ag-filter-options" id="filterOptions"></div><button class="lp-filter-toggle" type="button" data-filter-clear>Clear</button></section>
      <footer class="ag-status-line"><span><span id="rowCount">0</span> row(s)</span><span id="statusText">loading</span></footer>
    </section>
    <section class="ag-print-sheet" id="reportPrintSheet"></section>
    <div class="lp-modal" id="reportDrawer" hidden aria-hidden="true"><div class="lp-modal-backdrop" data-close-drawer></div><section class="lp-modal-card" role="dialog" aria-modal="true" aria-labelledby="drawerTitle" tabindex="-1"><button class="lp-modal-close" type="button" data-close-drawer aria-label="Close detail">x</button><div data-modal-content id="drawerContent"></div></section></div>
    <dialog class="ag-native-dialog" id="reportDialog"><form method="dialog" id="reportDialogForm"></form></dialog>`;
  }
  renderShell();

  const els = { shell: root.querySelector(".ag-report-shell"), meta: document.getElementById("reportMeta"), headerActions: document.getElementById("headerActions"), primaryActions: document.getElementById("primaryActions"), secondaryActions: document.getElementById("secondaryActions"), anchors: document.getElementById("ringAnchors"), filterBar: document.getElementById("filterBar"), filterOptions: document.getElementById("filterOptions"), rowCount: document.getElementById("rowCount"), status: document.getElementById("statusText"), grid: document.getElementById("agBaseGrid"), printSheet: document.getElementById("reportPrintSheet"), drawer: document.getElementById("reportDrawer"), drawerContent: document.getElementById("drawerContent"), dialog: document.getElementById("reportDialog"), dialogForm: document.getElementById("reportDialogForm") };

  function setStatus(message, status = "ready") { els.status.textContent = message; els.shell.dataset.state = status; }
  function actionVisible(action) { return typeof action.visible === "function" ? action.visible(api) : action.visible !== false; }
  function renderActions() {
    const zones = { header: [], primary: [], secondary: [] };
    for (const action of config.actions || []) {
      if (!actionVisible(action)) continue;
      const active = typeof action.active === "function" && action.active(api);
      const disabled = typeof action.disabled === "function" && action.disabled(api);
      zones[action.zone || "primary"].push(`<button class="lp-filter-toggle ${active ? "is-active" : ""}" type="button" data-action="${escapeHtml(action.id)}" ${disabled ? "disabled" : ""}>${escapeHtml(action.label)}</button>`);
    }
    els.headerActions.innerHTML = zones.header.join(""); els.primaryActions.innerHTML = zones.primary.join(""); els.secondaryActions.innerHTML = zones.secondary.join("");
  }
  const ringValue = row => config.filters?.ring ? config.filters.ring(row, api) : "";
  const horseValues = row => { const value = config.filters?.horse?.values ? config.filters.horse.values(row, api) : []; return (Array.isArray(value) ? value : [value]).filter(Boolean); };
  function applyFilters(rows) {
    let result = rows.filter(row => {
      if (state.ringAnchor && normalize(ringValue(row)) !== state.ringAnchor) return false;
      const horses = horseValues(row);
      if (state.horseText && !horses.some(value => normalize(value).includes(state.horseText))) return false;
      if (state.activeHorseFilters.size && ![...state.activeHorseFilters].some(key => new Set(horses.map(normalize)).has(key))) return false;
      return config.filters?.include ? config.filters.include(row, api) : true;
    });
    if (state.focusMode && config.filters?.focus) result = config.filters.focus(result, api);
    return result;
  }
  function renderAnchors() {
    const values = [...new Map(state.rows.map(row => [normalize(ringValue(row)), String(ringValue(row) || "").toUpperCase()]).filter(([key]) => key)).entries()].sort((a,b) => a[1].localeCompare(b[1], undefined, {numeric:true}));
    els.anchors.innerHTML = `<button class="lp-filter-toggle ${state.ringAnchor ? "" : "is-active"}" type="button" data-ring="">All</button>` + values.map(([key,label]) => `<button class="lp-filter-toggle ${state.ringAnchor === key ? "is-active" : ""}" type="button" data-ring="${escapeHtml(key)}">${escapeHtml(label)}</button>`).join("");
  }
  function renderFilters() {
    const horse = config.filters?.horse; els.filterBar.hidden = !state.filterPanelOpen || !horse; if (!horse) return;
    if (horse.mode === "text") { els.filterOptions.innerHTML = `<input class="ag-filter-input" id="horseFilterText" type="search" value="${escapeHtml(state.horseText)}" placeholder="${escapeHtml(horse.placeholder || "Horse")}"><button class="lp-filter-toggle" type="button" data-filter-apply>Apply</button>`; return; }
    const options = [...new Map(state.rows.flatMap(horseValues).filter(Boolean).map(value => [normalize(value), value])).entries()].sort((a,b) => String(a[1]).localeCompare(String(b[1])));
    els.filterOptions.innerHTML = options.length ? options.map(([key,label]) => `<button class="lp-filter-toggle ${state.activeHorseFilters.has(key) ? "is-active" : ""}" type="button" data-horse-key="${escapeHtml(key)}">${escapeHtml(label)}</button>`).join("") : '<button class="lp-filter-toggle" disabled>No Horses</button>';
  }
  function updateRows(message) {
    state.filteredRows = applyFilters(state.rows);
    state.visibleRows = config.row?.prepare ? config.row.prepare(state.filteredRows, api) : state.filteredRows;
    if (state.gridApi) { state.gridApi.setGridOption("rowData", state.visibleRows); state.visibleRows.length ? state.gridApi.hideOverlay() : state.gridApi.showNoRowsOverlay(); }
    els.rowCount.textContent = String(state.filteredRows.length); renderAnchors(); renderFilters(); renderActions(); setStatus(message || (state.filteredRows.length ? "ready" : "no rows"), state.filteredRows.length ? "ready" : "empty");
  }

  function ringFullWidthRenderer(params) { return `<div class="ag-ring-group">${escapeHtml(params.data.ring_title || params.data.ring || "Unassigned")}</div>`; }
  function rollupFullWidthRenderer(params) {
    const row = params.data || {}; const items = row.horse_items || row.rollup_items || [];
    const itemHtml = items.length ? items.map(item => `<span class="lp-achievement">${escapeHtml(item.name || item.label || item)}</span>`).join("") : '<span class="lp-achievement">No rollups</span>';
    return `<button class="lp-row ag-rollup-row" type="button" data-open-row="${escapeHtml(config.row.getId(row, api))}"><span class="ag-rollup-main"><span class="ag-rollup-title">${escapeHtml(row.rollup_title || row.class_label || row.class_name || "Class detail")}</span><span class="ag-rollup-items">${itemHtml}</span></span><span class="ag-rollup-meta">${escapeHtml(row.entry_count ?? items.length)} entries</span></button>`;
  }
  function fullWidthRenderer(params) { return params.data?.row_type === "ring" ? ringFullWidthRenderer(params) : rollupFullWidthRenderer(params); }

  function renderDrawerDetail(detail) {
    const rows = (detail.details || []).map(item => `<div class="lp-row is-detail"><span class="lp-row-title">${escapeHtml(item.label)}</span><span class="lp-row-meta">${escapeHtml(item.value)}</span></div>`).join("");
    const entries = (detail.entries || []).length ? detail.entries.map(item => `<div class="ag-drawer-entry"><span>${escapeHtml(item.time || "--")}</span><span class="ag-drawer-entry-name">${escapeHtml(item.name || "Entry")}</span><span>${escapeHtml(item.meta || "")}</span></div>`).join("") : '<div class="ag-drawer-empty">No entry detail available.</div>';
    return `<div class="lp-profile-shell packing-detail-shell packing-theme-classes"><div class="lp-profile-head"><h2 class="lp-profile-title rsa-H1" id="drawerTitle">${escapeHtml(detail.title || "Class Detail")}</h2><p class="lp-profile-subtitle rsa-p">${escapeHtml(detail.subtitle || "")}</p></div><section class="lp-profile-panel packing-detail"><div class="ag-drawer-detail"><div class="lp-detail-list">${rows}</div><div class="ag-drawer-entry-list">${entries}</div></div></section></div>`;
  }
  function openDrawer(row) { if (!row || row.row_type === "ring") return; const detail = config.row.drawer ? config.row.drawer(row, api) : null; if (!detail) return; els.drawerContent.innerHTML = renderDrawerDetail(detail); els.drawer.hidden = false; els.drawer.setAttribute("aria-hidden", "false"); }
  function closeDrawer() { els.drawer.hidden = true; els.drawer.setAttribute("aria-hidden", "true"); els.drawerContent.innerHTML = ""; }
  function findVisibleRow(key) { return state.visibleRows.find(row => String(config.row.getId(row, api)) === String(key)); }

  function buildPrintSheet() {
    const columns = config.print.columns || []; const template = columns.map(column => column.width || "minmax(0,1fr)").join(" ");
    els.printSheet.innerHTML = `<div class="ag-print-title"><h1>${escapeHtml(config.print.title || config.title)}</h1><p>${escapeHtml(config.output)}<br>${escapeHtml(config.print.meta ? config.print.meta(api) : els.meta.textContent)}<br>${escapeHtml(new Date().toISOString())}</p></div><div class="ag-print-columns">${state.filteredRows.map(row => `<div class="ag-print-row" style="--print-columns-template:${escapeHtml(template)}">${config.print.rollup ? `<div class="ag-print-rollup">${escapeHtml(config.print.rollup(row, api) || "")}</div>` : ""}${columns.map(column => `<span>${escapeHtml(column.value(row, api))}</span>`).join("")}</div>`).join("")}</div>`;
  }
  function printReport() { buildPrintSheet(); setStatus("printing " + config.output); setTimeout(() => window.print(), 50); }
  function openDialog(options) { dialogSave = options.onSave || null; els.dialogForm.innerHTML = `<p>${escapeHtml(options.title || "")}</p>${options.body || ""}<menu><button class="lp-filter-toggle" type="button" data-dialog-close>Close</button>${dialogSave ? `<button class="lp-filter-toggle is-active" type="button" data-dialog-save>${escapeHtml(options.saveLabel || "Save")}</button>` : ""}</menu>`; els.dialog.showModal(); }
  function closeDialog() { dialogSave = null; if (els.dialog.open) els.dialog.close(); }
  async function load() { setStatus("loading", "loading"); if (state.gridApi) state.gridApi.showLoadingOverlay(); try { const result = await config.data.load(api); state.rows = result.rows || []; state.context = result.context || {}; els.meta.textContent = result.meta || ""; updateRows(result.message || "loaded"); } catch (error) { state.rows = []; state.visibleRows = []; if (state.gridApi) { state.gridApi.setGridOption("rowData", []); state.gridApi.showNoRowsOverlay(); } setStatus("load failed: " + (error.message || error), "error"); } }
  function runAction(id) { const action = (config.actions || []).find(item => item.id === id); if (!action) return; if (action.type === "focus") { state.focusMode = !state.focusMode; updateRows(state.focusMode ? "focus on" : "focus off"); } else if (action.type === "filter") { state.filterPanelOpen = !state.filterPanelOpen; updateRows(state.filterPanelOpen ? "filter open" : "filter closed"); } else if (action.type === "print") printReport(); else if (action.type === "clear") { state.ringAnchor = ""; state.horseText = ""; state.activeHorseFilters.clear(); updateRows("filters cleared"); } else if (action.run) action.run(api); }

  api = { config, state, helpers, elements: els, setStatus, updateRows, renderActions, openDrawer, closeDrawer, openDialog, closeDialog, printReport, reload: load, setRows(rows,message){state.rows=rows;updateRows(message);} };
  const columns = typeof config.columns === "function" ? config.columns(api) : config.columns;
  const gridOptions = { rowData: [], columnDefs: columns, context: api, defaultColDef: {sortable:false,filter:false,resizable:false,suppressHeaderMenuButton:true}, getRowId: params => config.row.getId(params.data, api), animateRows:false, ensureDomOrder:true, suppressMovableColumns:true, suppressCellFocus:true, suppressHeaderFocus:true, isFullWidthRow: params => ["ring","rollup"].includes(params.rowNode.data?.row_type), fullWidthCellRenderer: fullWidthRenderer, getRowHeight: params => params.data?.row_type === "ring" ? 34 : params.data?.row_type === "rollup" ? 64 : 42, overlayLoadingTemplate:'<span>Loading</span>', overlayNoRowsTemplate:'<span>No rows</span>', onGridReady:event=>{state.gridApi=event.api;load();}, onCellClicked:params=>{ if (config.row?.onCellClicked?.(params,api) === true) return; if (!params.data?.row_type || params.data.row_type === "data") openDrawer(params.data); } };

  root.addEventListener("click", event => { const action=event.target.closest("[data-action]"); if(action)return runAction(action.dataset.action); const ring=event.target.closest("[data-ring]"); if(ring){state.ringAnchor=ring.dataset.ring||"";return updateRows(state.ringAnchor?"ring selected":"all rings");} const horse=event.target.closest("[data-horse-key]"); if(horse){const key=horse.dataset.horseKey;state.activeHorseFilters.has(key)?state.activeHorseFilters.delete(key):state.activeHorseFilters.add(key);return updateRows("horse filter updated");} if(event.target.closest("[data-filter-apply]")){state.horseText=normalize(document.getElementById("horseFilterText")?.value);return updateRows(state.horseText?"horse filter applied":"horse filter cleared");} if(event.target.closest("[data-filter-clear]")){state.horseText="";state.activeHorseFilters.clear();return updateRows("horse filter cleared");} const open=event.target.closest("[data-open-row]"); if(open)return openDrawer(findVisibleRow(open.dataset.openRow)); if(event.target.closest("[data-close-drawer]"))closeDrawer(); });
  els.dialog.addEventListener("click", async event => { if(event.target.closest("[data-dialog-close]"))closeDialog(); if(event.target.closest("[data-dialog-save]")&&dialogSave)await dialogSave(api,els.dialogForm); });
  document.addEventListener("keydown", event => { if(event.key === "Escape") { closeDrawer(); closeDialog(); } });
  if (!window.agGrid || typeof window.agGrid.createGrid !== "function") { setStatus("AG Grid 36.0.0 failed to load", "error"); return; }
  window.RS_AG_REPORT = api; window.agGrid.createGrid(els.grid, gridOptions);
})();
