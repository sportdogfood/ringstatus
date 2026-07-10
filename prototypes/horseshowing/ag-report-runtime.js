(function () {
  "use strict";

  const config = window.RS_AG_REPORT_CONFIG;
  const root = document.getElementById("agReportRoot");
  if (!config || !root) throw new Error("AG report configuration or mount root is missing.");

  const state = {
    gridApi: null,
    rows: [],
    visibleRows: [],
    context: {},
    focusMode: false,
    ringAnchor: "",
    filterPanelOpen: false,
    activeHorseFilters: new Set(),
    horseText: "",
    report: {}
  };

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, char => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;"
    }[char]));
  }

  function normalize(value) {
    return String(value || "").toLowerCase().replace(/[^a-z0-9']+/g, " ").trim();
  }

  function renderShell() {
    root.innerHTML = `
      <main class="app-shell" data-ag-system="${escapeHtml(config.id)}" data-ag-output="${escapeHtml(config.output)}" data-state="loading">
        <header class="app-head">
          <div class="header-left">
            <h1 class="app-title">${escapeHtml(config.title)}</h1>
            <div class="app-meta" id="reportMeta">Loading...</div>
          </div>
          <div class="header-right"><div class="action-bar-mini" id="headerActions" aria-label="Header actions"></div></div>
        </header>
        <section class="action-bar" aria-label="Report actions">
          <div class="action-group" id="primaryActions"></div>
          <div class="action-group secondary" id="secondaryActions"></div>
        </section>
        <nav class="action-anchors" aria-label="Ring anchors" id="ringAnchors"></nav>
        <section class="grid-frame"><div id="agBaseGrid" class="ag-theme-quartz"></div></section>
        <section class="action-bar-bottom" aria-label="Horse filters" id="filterBar" hidden>
          <div class="action-group" id="filterOptions"></div>
          <div class="action-group secondary"><button class="rs-button tap" type="button" data-filter-clear>Clear</button></div>
        </section>
        <footer class="status-line">
          <span><span id="rowCount">0</span> row(s) loaded</span>
          <span id="statusText">loading</span>
        </footer>
      </main>
      <section class="print-sheet" id="reportPrintSheet" aria-label="Print sheet"></section>
      <dialog id="reportDialog" aria-label="Report dialog"><form method="dialog" id="reportDialogForm"></form></dialog>`;
  }

  renderShell();

  const els = {
    shell: root.querySelector(".app-shell"),
    meta: document.getElementById("reportMeta"),
    headerActions: document.getElementById("headerActions"),
    primaryActions: document.getElementById("primaryActions"),
    secondaryActions: document.getElementById("secondaryActions"),
    ringAnchors: document.getElementById("ringAnchors"),
    filterBar: document.getElementById("filterBar"),
    filterOptions: document.getElementById("filterOptions"),
    rowCount: document.getElementById("rowCount"),
    status: document.getElementById("statusText"),
    grid: document.getElementById("agBaseGrid"),
    printSheet: document.getElementById("reportPrintSheet"),
    dialog: document.getElementById("reportDialog"),
    dialogForm: document.getElementById("reportDialogForm")
  };

  const helpers = { escapeHtml, normalize };
  let api;
  let dialogSave = null;

  function setStatus(message, status = "ready") {
    els.status.textContent = message;
    els.shell.dataset.state = status;
  }

  function actionVisible(action) {
    return typeof action.visible === "function" ? action.visible(api) : action.visible !== false;
  }

  function renderActions() {
    const groups = { header: [], primary: [], secondary: [] };
    for (const action of config.actions || []) {
      if (!actionVisible(action)) continue;
      const active = typeof action.active === "function" && action.active(api);
      const disabled = typeof action.disabled === "function" && action.disabled(api);
      groups[action.zone || "primary"].push(
        `<button class="rs-button tap ${active ? "tap-active" : ""} ${action.emphasis ? "tap-active-2" : ""}" type="button" data-action="${escapeHtml(action.id)}" ${disabled ? "disabled" : ""}>${escapeHtml(action.label)}</button>`
      );
    }
    els.headerActions.innerHTML = groups.header.join("");
    els.primaryActions.innerHTML = groups.primary.join("");
    els.secondaryActions.innerHTML = groups.secondary.join("");
  }

  function ringValue(row) {
    return config.filters?.ring ? config.filters.ring(row, api) : "";
  }

  function horseValues(row) {
    const value = config.filters?.horse?.values ? config.filters.horse.values(row, api) : [];
    return (Array.isArray(value) ? value : [value]).filter(Boolean);
  }

  function applyFilters(rows) {
    let filtered = rows.filter(row => {
      if (state.ringAnchor && normalize(ringValue(row)) !== state.ringAnchor) return false;
      const values = horseValues(row);
      if (state.horseText && !values.some(value => normalize(value).includes(state.horseText))) return false;
      if (state.activeHorseFilters.size) {
        const keys = new Set(values.map(normalize));
        if (![...state.activeHorseFilters].some(key => keys.has(key))) return false;
      }
      return config.filters?.include ? config.filters.include(row, api) : true;
    });
    if (state.focusMode && config.filters?.focus) filtered = config.filters.focus(filtered, api);
    return filtered;
  }

  function decorateRows(rows) {
    return config.row?.decorate ? config.row.decorate(rows, api) : rows;
  }

  function renderAnchors() {
    const values = [...new Map(state.rows.map(row => {
      const label = ringValue(row);
      return [normalize(label), String(label || "").toUpperCase()];
    }).filter(([key]) => key)).entries()].sort((a, b) => a[1].localeCompare(b[1], undefined, { numeric: true }));
    els.ringAnchors.innerHTML = `<button class="rs-button tap ${state.ringAnchor ? "" : "tap-active"}" type="button" data-ring="">All</button>` +
      values.map(([key, label]) => `<button class="rs-button tap ${state.ringAnchor === key ? "tap-active" : ""}" type="button" data-ring="${escapeHtml(key)}">${escapeHtml(label)}</button>`).join("");
  }

  function renderFilters() {
    const horse = config.filters?.horse;
    els.filterBar.hidden = !state.filterPanelOpen || !horse;
    if (!horse) return;
    if (horse.mode === "text") {
      els.filterOptions.innerHTML = `<input class="filter-text" id="horseFilterText" type="search" value="${escapeHtml(state.horseText)}" placeholder="${escapeHtml(horse.placeholder || "Horse")}"><button class="rs-button tap" type="button" data-filter-apply>Apply</button>`;
      return;
    }
    const options = [...new Map(state.rows.flatMap(row => horseValues(row)).filter(Boolean).map(value => [normalize(value), value])).entries()]
      .sort((a, b) => String(a[1]).localeCompare(String(b[1])));
    els.filterOptions.innerHTML = options.length
      ? options.map(([key, label]) => `<button class="rs-button tap ${state.activeHorseFilters.has(key) ? "tap-active" : ""}" type="button" data-horse-key="${escapeHtml(key)}">${escapeHtml(label)}</button>`).join("")
      : '<button class="rs-button tap" type="button" disabled>No Horses</button>';
  }

  function updateRows(message) {
    const filtered = applyFilters(state.rows);
    state.visibleRows = decorateRows(filtered);
    if (state.gridApi) {
      state.gridApi.setGridOption("rowData", state.visibleRows);
      if (state.visibleRows.length) state.gridApi.hideOverlay();
      else state.gridApi.showNoRowsOverlay();
    }
    const normalCount = state.visibleRows.filter(row => !config.row?.isSpecial?.(row, api)).length;
    els.rowCount.textContent = String(normalCount);
    renderAnchors();
    renderFilters();
    renderActions();
    setStatus(message || (normalCount ? "ready" : "no rows"), normalCount ? "ready" : "empty");
  }

  function specialRowRenderer(params) {
    const row = params.node?.data || params.rowNode?.data || {};
    const summary = config.row?.specialSummary ? config.row.specialSummary(row, api) : {};
    return `<div class="ag-full-width-anchor" role="presentation"><div class="class-related-data has-rollup">
      <div class="rollup-line"><div class="class-related-rollup"><button type="button" class="rollup-item" disabled>No Detail</button></div></div>
      <div class="class-line"><span class="class-time">${escapeHtml(summary.time)}</span><span class="class-ring">${escapeHtml(summary.ring)}</span><span class="class-name">${escapeHtml([summary.number, summary.name].filter(Boolean).join(" - "))}</span><span><span class="class-token">${escapeHtml(summary.count ?? 0)}</span></span><span><span class="class-token">No Detail</span></span></div>
    </div></div>`;
  }

  function buildPrintSheet() {
    const rows = state.visibleRows.filter(row => !config.row?.isSpecial?.(row, api));
    const columns = config.print.columns || [];
    const template = columns.map(column => column.width || "minmax(0, 1fr)").join(" ");
    els.printSheet.innerHTML = `<div class="print-title"><h1>${escapeHtml(config.print.title || config.title)}</h1><p>${escapeHtml(config.output)}<br>${escapeHtml(config.print.meta ? config.print.meta(api) : els.meta.textContent)}<br>${escapeHtml(new Date().toISOString())}</p></div><div class="print-columns">${rows.map(row => `<div class="print-row" style="--print-columns-template:${escapeHtml(template)}">${columns.map(column => `<span>${escapeHtml(column.value(row, api))}</span>`).join("")}</div>`).join("")}</div>`;
  }

  function printReport() {
    buildPrintSheet();
    setStatus("printing " + config.output);
    setTimeout(() => window.print(), 50);
  }

  function openDialog(options) {
    dialogSave = options.onSave || null;
    els.dialog.setAttribute("aria-label", options.title || "Report dialog");
    els.dialogForm.innerHTML = `<p>${escapeHtml(options.title || "")}</p>${options.body || ""}<menu><button class="rs-button tap" type="button" data-dialog-close>Close</button>${dialogSave ? `<button class="rs-button tap tap-active-2" type="button" data-dialog-save>${escapeHtml(options.saveLabel || "Save")}</button>` : ""}</menu>`;
    els.dialog.showModal();
  }

  function closeDialog() {
    dialogSave = null;
    if (els.dialog.open) els.dialog.close();
  }

  async function load() {
    setStatus("loading", "loading");
    if (state.gridApi) state.gridApi.showLoadingOverlay();
    try {
      const result = await config.data.load(api);
      state.rows = result.rows || [];
      state.context = result.context || {};
      els.meta.textContent = result.meta || "";
      updateRows(result.message || "loaded");
      if (config.data.afterLoad) await config.data.afterLoad(api, result);
    } catch (error) {
      state.rows = [];
      state.visibleRows = [];
      if (state.gridApi) {
        state.gridApi.setGridOption("rowData", []);
        state.gridApi.showNoRowsOverlay();
      }
      els.rowCount.textContent = "0";
      setStatus("load failed: " + (error.message || error), "error");
    }
  }

  function runAction(id) {
    const action = (config.actions || []).find(item => item.id === id);
    if (!action) return;
    if (action.type === "focus") {
      state.focusMode = !state.focusMode;
      updateRows(state.focusMode ? "focus on" : "focus off");
    } else if (action.type === "filter") {
      state.filterPanelOpen = !state.filterPanelOpen;
      updateRows(state.filterPanelOpen ? "filter open" : "filter closed");
    } else if (action.type === "print") {
      printReport();
    } else if (action.type === "clear") {
      state.ringAnchor = "";
      state.horseText = "";
      state.activeHorseFilters.clear();
      updateRows("filters cleared");
    } else if (action.run) {
      action.run(api);
    }
  }

  api = {
    config,
    state,
    helpers,
    elements: els,
    setStatus,
    updateRows,
    renderActions,
    openDialog,
    closeDialog,
    printReport,
    reload: load,
    setRows(rows, message) { state.rows = rows; updateRows(message); }
  };

  const columnDefs = typeof config.columns === "function" ? config.columns(api) : config.columns;
  const gridOptions = {
    rowData: [],
    columnDefs,
    context: api,
    defaultColDef: { sortable: false, filter: false, resizable: false, suppressHeaderMenuButton: true },
    getRowId: params => config.row.getId(params.data, api),
    rowSelection: { mode: "singleRow", checkboxes: false, enableClickSelection: true },
    animateRows: false,
    ensureDomOrder: true,
    suppressMovableColumns: true,
    suppressCellFocus: true,
    suppressHeaderFocus: true,
    isFullWidthRow: params => !!config.row?.isSpecial?.(params.rowNode.data, api),
    fullWidthCellRenderer: specialRowRenderer,
    getRowHeight: params => config.row?.isSpecial?.(params.data, api) ? (config.row.specialHeight || 58) : 42,
    overlayLoadingTemplate: '<span class="ag-overlay-loading-center">Loading</span>',
    overlayNoRowsTemplate: '<span class="ag-overlay-loading-center">No rows</span>',
    onGridReady: event => { state.gridApi = event.api; load(); },
    onCellClicked: params => config.row?.onCellClicked?.(params, api)
  };

  root.addEventListener("click", event => {
    const action = event.target.closest("[data-action]");
    if (action) return runAction(action.dataset.action);
    const ring = event.target.closest("[data-ring]");
    if (ring) { state.ringAnchor = ring.dataset.ring || ""; return updateRows(state.ringAnchor ? "ring selected" : "all rings"); }
    const horse = event.target.closest("[data-horse-key]");
    if (horse) {
      const key = horse.dataset.horseKey;
      if (state.activeHorseFilters.has(key)) state.activeHorseFilters.delete(key); else state.activeHorseFilters.add(key);
      return updateRows("horse filter updated");
    }
    if (event.target.closest("[data-filter-apply]")) {
      state.horseText = normalize(document.getElementById("horseFilterText")?.value);
      return updateRows(state.horseText ? "horse filter applied" : "horse filter cleared");
    }
    if (event.target.closest("[data-filter-clear]")) {
      state.horseText = "";
      state.activeHorseFilters.clear();
      return updateRows("horse filter cleared");
    }
  });

  els.dialog.addEventListener("click", async event => {
    if (event.target.closest("[data-dialog-close]")) closeDialog();
    if (event.target.closest("[data-dialog-save]") && dialogSave) await dialogSave(api, els.dialogForm);
  });

  if (!window.agGrid || typeof window.agGrid.createGrid !== "function") {
    setStatus("AG Grid 36.0.0 failed to load", "error");
    return;
  }
  window.RS_AG_REPORT = api;
  window.agGrid.createGrid(els.grid, gridOptions);
})();
