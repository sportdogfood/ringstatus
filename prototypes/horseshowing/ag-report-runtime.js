(function () {
  "use strict";

  const config = window.RS_AG_REPORT_CONFIG;
  const root = document.getElementById("packing-app");
  if (!config || !root) throw new Error("AG report configuration or #packing-app is missing.");

  root.classList.add("ag-report-root", "rss-stacked-form");
  document.body.classList.add("rss-stacked-form-body");

  const state = {
    gridApi: null,
    rows: [],
    filteredRows: [],
    visibleRows: [],
    context: {},
    focusMode: false,
    ringAnchor: "",
    filterPanelOpen: false,
    activeHorseFilters: new Set(),
    horseText: "",
    report: {}
  };
  const escapeHtml = value => String(value ?? "").replace(/[&<>"']/g, character => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;"
  })[character]);
  const normalize = value => String(value || "").toLowerCase().replace(/[^a-z0-9']+/g, " ").trim();
  const helpers = { escapeHtml, normalize };
  let api;
  let dialogSave = null;

  function renderShell() {
    const anchorPlacement = config.placements?.anchors || "top";
    const actionPlacement = config.placements?.actions || "top";
    const anchorsTop = anchorPlacement === "top" || anchorPlacement === "both";
    const anchorsBottom = anchorPlacement === "bottom" || anchorPlacement === "both";
    const actionsTop = actionPlacement === "top" || actionPlacement === "both";
    const actionsBottom = actionPlacement === "bottom" || actionPlacement === "both";
    const anchorMarkup = position => position === "top"
      ? '<nav class="packing-list-switcher action-anchors-top" id="ringAnchors" aria-label="Ring anchors"></nav>'
      : '<nav class="packing-list-switcher action-anchors-bottom" id="ringAnchorsBottom" aria-label="Ring anchors"></nav>';
    const actionMarkup = position => position === "top"
      ? '<div class="packing-list-switcher action-bar" aria-label="Report actions"><div class="action-group" id="primaryActions"></div><div class="action-group" id="secondaryActions"></div><section class="packing-section-search filter-bar" id="filterBar" hidden><div class="filter-options" id="filterOptions"></div><button class="lp-filter-toggle" type="button" data-filter-clear>Clear</button></section></div>'
      : '<div class="packing-list-switcher action-bar-bottom" aria-label="Report actions"><div class="action-group" id="primaryActionsBottom"></div><div class="action-group" id="secondaryActionsBottom"></div><section class="packing-section-search filter-bar" id="filterBarBottom" hidden><div class="filter-options" id="filterOptionsBottom"></div><button class="lp-filter-toggle" type="button" data-filter-clear>Clear</button></section></div>';

    root.innerHTML = `<section class="lp-section-block packing-theme-show lp-theme-classes ag-report-shell" data-state="loading">
      <header class="lp-section-title packing-section-title app-head">
        <div class="header-left">
          <h3 class="app-title">${escapeHtml(config.title)}</h3>
          <p class="app-meta"><span id="reportMeta">Loading...</span><span><span id="rowCount">0</span> row(s)</span><span id="statusText">loading</span></p>
        </div>
        <div class="header-right"><div class="lp-section-actions packing-section-title-actions action-bar-mini" id="headerActions"></div></div>
      </header>
      ${anchorsTop ? anchorMarkup("top") : ""}
      ${actionsTop ? actionMarkup("top") : ""}
      ${config.lists?.length ? '<nav class="packing-list-switcher action-list-switcher" id="listSwitcher" aria-label="Report lists"></nav>' : ""}
      <main class="grid-frame"><div id="scheduleGrid" class="ag-theme-quartz"></div></main>
      ${actionsBottom ? actionMarkup("bottom") : ""}
      ${anchorsBottom ? anchorMarkup("bottom") : ""}
    </section>
    <section class="ag-print-sheet" id="reportPrintSheet"></section>
    <div class="lp-modal" id="reportDrawer" hidden aria-hidden="true"><div class="lp-modal-backdrop" data-close-drawer></div><section class="lp-modal-card" role="dialog" aria-modal="true" aria-labelledby="drawerTitle" tabindex="-1"><button class="lp-modal-close" type="button" data-close-drawer aria-label="Close detail">x</button><div data-modal-content id="drawerContent"></div></section></div>
    <dialog class="ag-native-dialog" id="reportDialog"><form method="dialog" id="reportDialogForm"></form></dialog>`;
  }
  renderShell();

  const firstById = (...ids) => ids.map(id => document.getElementById(id)).find(Boolean) || null;
  const els = {
    root,
    shell: root.querySelector(".ag-report-shell"),
    meta: document.getElementById("reportMeta"),
    headerActions: document.getElementById("headerActions"),
    primaryActions: firstById("primaryActions", "primaryActionsBottom"),
    secondaryActions: firstById("secondaryActions", "secondaryActionsBottom"),
    anchors: firstById("ringAnchors", "ringAnchorsBottom"),
    filterBar: firstById("filterBar", "filterBarBottom"),
    filterOptions: firstById("filterOptions", "filterOptionsBottom"),
    rowCount: document.getElementById("rowCount"),
    status: document.getElementById("statusText"),
    grid: document.getElementById("scheduleGrid"),
    printSheet: document.getElementById("reportPrintSheet"),
    drawer: document.getElementById("reportDrawer"),
    drawerCard: root.querySelector(".lp-modal-card"),
    drawerContent: document.getElementById("drawerContent"),
    dialog: document.getElementById("reportDialog"),
    dialogForm: document.getElementById("reportDialogForm")
  };

  function setStatus(message, status = "ready") {
    els.status.textContent = message;
    els.shell.dataset.state = status;
  }

  function actionVisible(action) {
    return typeof action.visible === "function" ? action.visible(api) : action.visible !== false;
  }

  function renderActions() {
    const zones = { header: [], primary: [], secondary: [] };
    for (const action of config.actions || []) {
      if (!actionVisible(action)) continue;
      const active = typeof action.active === "function" && action.active(api);
      const disabled = typeof action.disabled === "function" && action.disabled(api);
      zones[action.zone || "primary"].push(`<button class="lp-filter-toggle ${active ? "is-active" : ""}" type="button" data-action="${escapeHtml(action.id)}" ${disabled ? "disabled" : ""}>${escapeHtml(action.label)}</button>`);
    }
    els.headerActions.innerHTML = zones.header.join("");
    if (els.primaryActions) els.primaryActions.innerHTML = zones.primary.join("");
    if (els.secondaryActions) els.secondaryActions.innerHTML = zones.secondary.join("");
  }

  const ringValue = row => config.filters?.ring ? config.filters.ring(row, api) : "";
  const horseValues = row => {
    const value = config.filters?.horse?.values ? config.filters.horse.values(row, api) : [];
    return (Array.isArray(value) ? value : [value]).filter(Boolean);
  };

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
    if (!els.anchors) return;
    const values = [...new Map(state.rows.map(row => [normalize(ringValue(row)), String(ringValue(row) || "").toUpperCase()]).filter(([key]) => key)).entries()].sort((a, b) => a[1].localeCompare(b[1], undefined, { numeric: true }));
    els.anchors.innerHTML = `<button class="lp-filter-toggle ${state.ringAnchor ? "" : "is-active"}" type="button" data-ring="">All</button>` + values.map(([key, label]) => `<button class="lp-filter-toggle ${state.ringAnchor === key ? "is-active" : ""}" type="button" data-ring="${escapeHtml(key)}">${escapeHtml(label)}</button>`).join("");
  }

  function renderFilters() {
    if (!els.filterBar || !els.filterOptions) return;
    const horse = config.filters?.horse;
    els.filterBar.hidden = !state.filterPanelOpen || !horse;
    if (!horse) return;
    if (horse.mode === "text") {
      els.filterOptions.innerHTML = `<input class="ag-filter-input" id="horseFilterText" type="search" value="${escapeHtml(state.horseText)}" placeholder="${escapeHtml(horse.placeholder || "Horse")}"><button class="lp-filter-toggle" type="button" data-filter-apply>Apply</button>`;
      return;
    }
    const options = [...new Map(state.rows.flatMap(horseValues).filter(Boolean).map(value => [normalize(value), value])).entries()].sort((a, b) => String(a[1]).localeCompare(String(b[1])));
    els.filterOptions.innerHTML = options.length ? options.map(([key, label]) => `<button class="lp-filter-toggle ${state.activeHorseFilters.has(key) ? "is-active" : ""}" type="button" data-horse-key="${escapeHtml(key)}">${escapeHtml(label)}</button>`).join("") : '<button class="lp-filter-toggle" disabled>No Horses</button>';
  }

  function updateRows(message) {
    state.filteredRows = applyFilters(state.rows);
    state.visibleRows = config.row?.prepare ? config.row.prepare(state.filteredRows, api) : state.filteredRows;
    if (state.gridApi) {
      state.gridApi.setGridOption("rowData", state.visibleRows);
      state.visibleRows.length ? state.gridApi.hideOverlay() : state.gridApi.showNoRowsOverlay();
    }
    els.rowCount.textContent = String(state.filteredRows.length);
    renderAnchors();
    renderFilters();
    renderActions();
    setStatus(message || (state.filteredRows.length ? "ready" : "no rows"), state.filteredRows.length ? "ready" : "empty");
  }

  function entryLabel(entry) {
    const name = String(entry?.name || entry?.label || "Entry");
    const order = entry?.order;
    return order && !name.trim().endsWith(`(${order})`) ? `${name} (${order})` : name;
  }

  function rssRowShapeRenderer(params) {
    const data = params.data || {};
    const rootElement = document.createElement("div");
    const entryState = data.entryState === "rss-is-hydrated" ? "rss-is-hydrated" : "rss-is-empty";
    const entries = Array.isArray(data.entries) ? data.entries : [];

    rootElement.className = "rss-class-related-data";
    const entryButtons = entries.map((entry, index) => `<button type="button" class="rss-entry-rollup" data-row-key="${escapeHtml(data.rowKey)}" data-entry-key="${escapeHtml(entry.entryKey || `${data.rowKey}|entry|${index + 1}`)}" data-entry-name="${escapeHtml(entry.name || entry.label || "")}" data-entry-order="${escapeHtml(entry.order || "")}" data-trainer="${escapeHtml(entry.trainer || "")}" data-rollup-index="${index + 1}">${escapeHtml(entryLabel(entry))}</button>`).join("");
    rootElement.innerHTML = `<div class="rss-entry-line ${entryState}"><div class="rss-entry-rollups">${entryButtons}</div></div>
      <div class="rss-class-line" role="button" tabindex="0" data-row-key="${escapeHtml(data.rowKey)}" data-class-key="${escapeHtml(data.classKey || data.rowKey)}">
        <div class="rss-time-cell"><span class="rss-class-time">${escapeHtml(data.time || "--")}</span></div>
        <div class="rss-class-ring">${escapeHtml(data.ring || "")}</div>
        <div class="rss-class-name" title="${escapeHtml(data.className || "")}">${escapeHtml(data.className || "")}</div>
        <div class="rss-class-entry">${escapeHtml(data.entryCount ?? entries.length)}</div>
        <div class="rss-class-status"><span class="rss-class-token">${escapeHtml(data.status || "")}</span></div>
      </div>`;

    rootElement.classList.toggle("rss-is-hydrated", entryState === "rss-is-hydrated");
    rootElement.classList.toggle("rss-is-empty", entryState === "rss-is-empty");

    rootElement.querySelectorAll(".rss-entry-rollup").forEach((entryButton, index) => {
      entryButton.addEventListener("click", event => {
        event.stopPropagation();
        openEntryDrawer(data, entries[index]);
      });
    });

    const classLine = rootElement.querySelector(".rss-class-line");
    function runClassInstruction(event) {
      event.stopPropagation();
      openDrawer(data);
    }
    classLine.addEventListener("click", runClassInstruction);
    classLine.addEventListener("keydown", event => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      runClassInstruction(event);
    });

    return rootElement;
  }

  function renderDrawerDetail(detail) {
    const rows = (detail.details || []).map(item => `<div class="lp-detail-row"><div class="lp-detail-label">${escapeHtml(item.label)}</div><div class="lp-detail-value">${escapeHtml(item.value)}</div></div>`).join("");
    const entries = (detail.entries || []).length ? detail.entries.map(item => `<div class="lp-row is-detail packing-control-row"${item.selected ? ' aria-current="true"' : ""}><span class="lp-row-title">${escapeHtml([item.time || "--", item.name || "Entry"].filter(Boolean).join(" | "))}</span><span class="lp-row-meta">${escapeHtml(item.meta || "")}</span></div>`).join("") : '<div class="lp-row is-static"><span class="lp-row-title">No entry detail available.</span></div>';
    const actions = (detail.actions || []).map(action => `<div class="lp-row is-detail packing-control-row"><span class="lp-row-title">${escapeHtml(action.label || action.buttonLabel || "Action")}</span><button class="lp-edit-button" type="button" data-drawer-action="${escapeHtml(action.id)}" data-entry-key="${escapeHtml(action.entryKey || detail.entryKey || "")}">${escapeHtml(action.buttonLabel || action.label || "Apply")}</button></div>`).join("");
    return `<div class="lp-profile-shell packing-detail-shell packing-theme-classes"><div class="lp-profile-head wec-profile-top"><h2 class="lp-profile-title rsa-H1" id="drawerTitle">${escapeHtml(detail.title || "Class Detail")}</h2><p class="lp-profile-subtitle rsa-p">${escapeHtml(detail.subtitle || "")}</p></div><section class="lp-profile-panel packing-detail wec-detail-section"><div class="lp-field-grid lp-profile-tab-panel is-active"><div class="lp-detail-list packing-detail-totals">${rows}</div><div class="lp-edit-panel"><div class="lp-edit-head"><span>${escapeHtml(detail.entriesTitle || "Entries")}</span></div><div class="packing-control-list">${entries}${actions}</div></div></div></section></div>`;
  }

  function showDrawer(detail) {
    if (!detail) return;
    els.drawerContent.innerHTML = renderDrawerDetail(detail);
    els.drawerContent.dataset.drawerMode = detail.mode || "class";
    els.drawerContent.dataset.rowKey = detail.rowKey || "";
    els.drawerContent.dataset.classKey = detail.classKey || "";
    els.drawerContent.dataset.entryKey = detail.entryKey || "";
    els.drawer.hidden = false;
    els.drawer.setAttribute("aria-hidden", "false");
    els.drawerCard.focus();
  }

  function openDrawer(row) {
    if (!row) return;
    const detail = config.row.drawer ? config.row.drawer(row, api) : null;
    if (detail) showDrawer({ mode: "class", rowKey: row.rowKey, classKey: row.classKey, ...detail });
  }

  function openEntryDrawer(row, entry) {
    if (!row || !entry) return;
    const detail = config.row.entryDrawer ? config.row.entryDrawer(row, entry, api) : {
      title: entryLabel(entry),
      subtitle: [row.className, row.ring, row.time].filter(Boolean).join(" | "),
      entriesTitle: "Class entries",
      details: [
        { label: "Entry", value: entryLabel(entry) },
        { label: "Trainer", value: entry.trainer || "" },
        { label: "Class", value: row.className || "" },
        { label: "Ring", value: row.ring || "" },
        { label: "Time", value: row.time || "" }
      ],
      entries: (row.entries || []).map(item => ({ name: entryLabel(item), meta: item.trainer || "", selected: item.entryKey === entry.entryKey }))
    };
    if (detail) showDrawer({ mode: "entry", rowKey: row.rowKey, classKey: row.classKey, entryKey: entry.entryKey, ...detail });
  }

  function closeDrawer() {
    els.drawer.hidden = true;
    els.drawer.setAttribute("aria-hidden", "true");
    els.drawerContent.innerHTML = "";
    delete els.drawerContent.dataset.drawerMode;
    delete els.drawerContent.dataset.rowKey;
    delete els.drawerContent.dataset.classKey;
    delete els.drawerContent.dataset.entryKey;
  }

  function buildPrintSheet() {
    const columns = config.print.columns || [];
    const template = columns.map(column => column.width || "minmax(0,1fr)").join(" ");
    els.printSheet.innerHTML = `<div class="ag-print-title"><h1>${escapeHtml(config.print.title || config.title)}</h1><p>${escapeHtml(config.output)}<br>${escapeHtml(config.print.meta ? config.print.meta(api) : els.meta.textContent)}<br>${escapeHtml(new Date().toISOString())}</p></div><div class="ag-print-columns">${state.filteredRows.map(row => `<div class="ag-print-row" style="--print-columns-template:${escapeHtml(template)}">${config.print.rollup ? `<div class="ag-print-rollup">${escapeHtml(config.print.rollup(row, api) || "")}</div>` : ""}${columns.map(column => `<span>${escapeHtml(column.value(row, api))}</span>`).join("")}</div>`).join("")}</div>`;
  }

  function printReport() {
    buildPrintSheet();
    setStatus("printing " + config.output);
    setTimeout(() => window.print(), 50);
  }

  function openDialog(options) {
    dialogSave = options.onSave || null;
    els.dialogForm.innerHTML = `<p>${escapeHtml(options.title || "")}</p>${options.body || ""}<menu><button class="lp-filter-toggle" type="button" data-dialog-close>Close</button>${dialogSave ? `<button class="lp-filter-toggle is-active" type="button" data-dialog-save>${escapeHtml(options.saveLabel || "Save")}</button>` : ""}</menu>`;
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
    } catch (error) {
      state.rows = [];
      state.visibleRows = [];
      if (state.gridApi) {
        state.gridApi.setGridOption("rowData", []);
        state.gridApi.showNoRowsOverlay();
      }
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
    renderers: { rssRowShapeRenderer },
    setStatus,
    updateRows,
    renderActions,
    openDrawer,
    openEntryDrawer,
    closeDrawer,
    openDialog,
    closeDialog,
    printReport,
    reload: load,
    setRows(rows, message) {
      state.rows = rows;
      updateRows(message);
    }
  };

  const columns = typeof config.columns === "function" ? config.columns(api) : config.columns;
  const gridOptions = {
    theme: "legacy",
    domLayout: "autoHeight",
    rowData: [],
    columnDefs: columns,
    context: api,
    defaultColDef: { sortable: false, filter: false, resizable: false, suppressHeaderMenuButton: true },
    getRowId: params => config.row.getId(params.data, api),
    rowClassRules: {
      "rss-row-is-hydrated": params => params.data.entryState === "rss-is-hydrated"
    },
    animateRows: false,
    ensureDomOrder: true,
    suppressMovableColumns: true,
    suppressCellFocus: true,
    suppressHeaderFocus: true,
    overlayLoadingTemplate: "<span>Loading</span>",
    overlayNoRowsTemplate: "<span>No rows</span>",
    onGridReady(event) {
      state.gridApi = event.api;
      load();
    }
  };
  api.gridOptions = gridOptions;

  root.addEventListener("click", event => {
    const action = event.target.closest("[data-action]");
    if (action) return runAction(action.dataset.action);
    const ring = event.target.closest("[data-ring]");
    if (ring) {
      state.ringAnchor = ring.dataset.ring || "";
      return updateRows(state.ringAnchor ? "ring selected" : "all rings");
    }
    const horse = event.target.closest("[data-horse-key]");
    if (horse) {
      const key = horse.dataset.horseKey;
      state.activeHorseFilters.has(key) ? state.activeHorseFilters.delete(key) : state.activeHorseFilters.add(key);
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
    const drawerAction = event.target.closest("[data-drawer-action]");
    if (drawerAction && config.row?.onDrawerAction) {
      return config.row.onDrawerAction(drawerAction.dataset.drawerAction, drawerAction.dataset.entryKey, api);
    }
    if (event.target.closest("[data-close-drawer]")) closeDrawer();
  });

  els.dialog.addEventListener("click", async event => {
    if (event.target.closest("[data-dialog-close]")) closeDialog();
    if (event.target.closest("[data-dialog-save]") && dialogSave) await dialogSave(api, els.dialogForm);
  });
  document.addEventListener("keydown", event => {
    if (event.key === "Escape") {
      closeDrawer();
      closeDialog();
    }
  });

  if (!window.agGrid || typeof window.agGrid.createGrid !== "function") {
    setStatus("AG Grid 36.0.0 failed to load", "error");
    return;
  }
  window.RS_AG_REPORT = api;
  window.agGrid.createGrid(els.grid, gridOptions);
})();
