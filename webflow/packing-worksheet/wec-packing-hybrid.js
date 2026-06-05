(function () {
  const root = document.getElementById("packing-app");
  if (!root) return;

  const globalConfig = window.WEC_PACKING_HYBRID_CONFIG || {};
  const config = {
    apiUrl: root.dataset.apiUrl || globalConfig.apiUrl || "https://ringstatus.com/test/wec-packing/horse-kits",
    printUrl: root.dataset.printUrl || globalConfig.printUrl || "https://ringstatus.com/test/wec-packing/horse-kits/print",
    packWaveKey: root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one"
  };

  const ui = {
    loading: true,
    error: "",
    viewKey: config.packWaveKey,
    laneKey: "open",
    search: "",
    sortKey: "horse",
    sortDir: "asc",
    selectedHorseId: "",
    drawerOpen: false,
    itemSearch: "",
    itemFilter: "all",
    savingKey: ""
  };

  let state = null;
  let rows = [];
  const optimistic = new Map();

  root.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    if (target.tagName === "A") event.preventDefault();

    const action = target.dataset.action;
    if (action === "set-view") {
      ui.viewKey = target.dataset.viewKey || config.packWaveKey;
      ui.drawerOpen = false;
      await load();
      return;
    }
    if (action === "set-lane") {
      ui.laneKey = target.dataset.laneKey || "open";
      rebuild();
      return;
    }
    if (action === "set-sort") {
      setSort(target.dataset.sortKey || "horse");
      return;
    }
    if (action === "open-horse") {
      ui.selectedHorseId = target.dataset.horseId || "";
      ui.drawerOpen = true;
      render();
      return;
    }
    if (action === "close-drawer") {
      ui.drawerOpen = false;
      render();
      return;
    }
    if (action === "clear-search") {
      ui.search = "";
      rebuild();
      return;
    }
    if (action === "clear-item-search") {
      ui.itemSearch = "";
      render();
      return;
    }
    if (action === "set-item-filter") {
      ui.itemFilter = target.dataset.itemFilter || "all";
      render();
      return;
    }
    if (action === "set-item-state") {
      await setItemState(target);
      return;
    }
    if (action === "print") {
      window.open(printUrl(), "_blank", "noopener");
    }
  });

  root.addEventListener("input", (event) => {
    const input = event.target;
    if (input.matches("[data-search]")) {
      ui.search = input.value || "";
      rebuild();
      focusSearch("[data-search]", ui.search.length);
    }
    if (input.matches("[data-item-search]")) {
      ui.itemSearch = input.value || "";
      render();
      focusSearch("[data-item-search]", ui.itemSearch.length);
    }
  });

  load();

  async function load() {
    ui.loading = true;
    ui.error = "";
    render();
    try {
      const next = await fetchJson(apiUrl());
      assertState(next);
      state = next;
      rebuild(false);
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.loading = false;
      render();
    }
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || `${response.status}`);
    return data;
  }

  function apiUrl() {
    const url = new URL(config.apiUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", ui.viewKey || config.packWaveKey);
    url.searchParams.set("v", "hybrid");
    return url.toString();
  }

  function printUrl() {
    const url = new URL(config.printUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", ui.viewKey || config.packWaveKey);
    return url.toString();
  }

  function rebuild(shouldRender = true) {
    rows = sortRows(filteredHorses().map(recordForHorse).filter(matchesLane));
    if (ui.drawerOpen && !rows.some((row) => row.id === ui.selectedHorseId)) ui.drawerOpen = false;
    if (shouldRender) render();
  }

  function filteredHorses() {
    const horses = state?.horses || [];
    const query = ui.search.trim().toLowerCase();
    if (!query) return horses;
    return horses.filter((horse) => [
      horse.barnName,
      horse.name,
      horse.showName
    ].join(" ").toLowerCase().includes(query));
  }

  function recordForHorse(horse) {
    const items = assignedItems(horse);
    const kit = assignedKit(items);
    const counts = countsFor(horse, kit, items);
    return { id: horse.id, horse, kit, items, counts };
  }

  function assignedItems(horse) {
    const itemById = new Map((state?.kitItems || []).map((item) => [item.id, item]));
    return unique(horse?.pakKitItemIds || [])
      .map((id) => itemById.get(id))
      .filter((item) => item && item.active !== false && item.status !== "inactive")
      .sort(compareItem);
  }

  function assignedKit(items) {
    if (!items.length) return null;
    const itemIds = new Set(items.map((item) => item.id));
    return (state?.kits || [])
      .filter((kit) => kit.active !== false && kit.status !== "inactive")
      .find((kit) => (kit.kitItemIds || []).some((id) => itemIds.has(id))) || null;
  }

  function countsFor(horse, kit, items) {
    if (!kit || !items.length) return { need: 0, packed: 0, notNeeded: 0, left: 0 };
    let packed = 0;
    let notNeeded = 0;
    items.forEach((item) => {
      const value = itemState(horse.id, kit.id, item.id);
      if (value === "packed") packed += 1;
      if (value === "not_needed") notNeeded += 1;
    });
    const need = Math.max(0, items.length - notNeeded);
    return { need, packed, notNeeded, left: Math.max(0, need - packed) };
  }

  function itemState(horseId, kitId, itemId) {
    const key = stateKey(horseId, kitId, itemId);
    if (optimistic.has(key)) return optimistic.get(key);
    const row = packingRow(horseId, kitId, itemId);
    if (!row) return "not_packed";
    if (row.packState === "not_needed" || row.neededState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function packingRow(horseId, kitId, itemId) {
    return packingRowFrom(state, horseId, kitId, itemId);
  }

  function packingRowFrom(source, horseId, kitId, itemId) {
    return (source?.packingRows || []).find((row) =>
      includes(row.horseIds, horseId) && includes(row.kitIds, kitId) && includes(row.kitItemIds, itemId)
    );
  }

  function serverItemState(source, horseId, kitId, itemId) {
    const row = packingRowFrom(source, horseId, kitId, itemId);
    if (!row) return "not_packed";
    if (row.packState === "not_needed" || row.neededState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function refreshAfterPropagation(key, horseId, kitId, itemId, expectedState) {
    window.setTimeout(async () => {
      if (optimistic.get(key) !== expectedState) return;
      try {
        const next = await fetchJson(apiUrl());
        assertState(next);
        state = next;
        if (serverItemState(next, horseId, kitId, itemId) === expectedState) optimistic.delete(key);
        rebuild(false);
        render();
      } catch (error) {
        ui.error = error.message || String(error);
        render();
      }
    }, 2500);
  }

  function matchesLane(record) {
    if (ui.laneKey === "need" || ui.laneKey === "open" || ui.laneKey === "left") return record.counts.left > 0;
    if (ui.laneKey === "packed") return record.counts.packed > 0;
    if (ui.laneKey === "all") return true;
    return true;
  }

  function sortRows(list) {
    const dir = ui.sortDir === "desc" ? -1 : 1;
    return [...list].sort((a, b) => {
      if (ui.sortKey === "kit") return compareText(kitLabel(a.kit), kitLabel(b.kit)) * dir;
      if (ui.sortKey === "need") return compareNumber(a.counts.need, b.counts.need) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (ui.sortKey === "packed") return compareNumber(a.counts.packed, b.counts.packed) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (ui.sortKey === "left") return compareNumber(a.counts.left, b.counts.left) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      return compareText(horseLabel(a.horse), horseLabel(b.horse)) * dir;
    });
  }

  function setSort(key) {
    if (ui.sortKey === key) ui.sortDir = ui.sortDir === "asc" ? "desc" : "asc";
    else {
      ui.sortKey = key;
      ui.sortDir = "asc";
    }
    rebuild();
  }

  async function setItemState(button) {
    const record = selectedRecord();
    if (!record?.horse?.id || !record?.kit?.id) return;
    const itemId = button.dataset.kitItemId || "";
    const nextState = button.dataset.packState || "";
    if (!itemId || !nextState || !state?.source?.packWaveId) return;

    const key = stateKey(record.horse.id, record.kit.id, itemId);
    const previous = itemState(record.horse.id, record.kit.id, itemId);
    if (previous === nextState) return;

    optimistic.set(key, nextState);
    ui.savingKey = key;
    rebuild(false);
    ui.drawerOpen = true;
    render();

    try {
      const existing = packingRow(record.horse.id, record.kit.id, itemId);
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "set_packing_kit_state",
          packingKitId: existing?.id || "",
          horseId: record.horse.id,
          kitId: record.kit.id,
          kitItemId: itemId,
          packWaveId: state.source.packWaveId,
          packState: nextState
        })
      });
      if (!result.state) throw new Error("missing_state_after_item_update");
      assertState(result.state);
      state = result.state;
      if (serverItemState(result.state, record.horse.id, record.kit.id, itemId) === nextState) {
        optimistic.delete(key);
      } else {
        refreshAfterPropagation(key, record.horse.id, record.kit.id, itemId, nextState);
      }
    } catch (error) {
      optimistic.set(key, previous);
      ui.error = error.message || String(error);
    } finally {
      ui.savingKey = "";
      rebuild(false);
      ui.drawerOpen = true;
      render();
    }
  }

  function render() {
    root.className = "rsa-dashboard";
    root.innerHTML = `
      <div class="lp-shell">
        ${headerHtml()}
        ${navHtml()}
        <div class="rs-airtable-shell">
          <div class="rs-page-stack">
            ${summaryAggsHtml()}
            ${secondaryTabsHtml()}
            ${countAggsHtml()}
            ${laneTabsHtml()}
            ${searchHtml()}
            ${tableHtml()}
            ${commentsHtml()}
          </div>
        </div>
      </div>
      ${drawerHtml()}
    `;
  }

  function headerHtml() {
    const title = state?.wave?.reportTitle || "WEC PACK";
    const subtitle = state?.wave?.reportSubtitle || state?.wave?.label || "";
    return `
      <header class="lp-header">
        <div class="lp-header-copy">
          <h1>${escapeHtml(title)}</h1>
          <p>${escapeHtml(subtitle)}</p>
        </div>
      </header>
    `;
  }

  function navHtml() {
    const tabs = ["HOME", "HORSES", "GROOMING", "FEED", "TACK", "SHOW", "BARN"];
    return `<section class="rs-stack-section is-primary-tabs"><nav class="rs-stack-tabs">${tabs.map((tab) => `<button class="rs-stack-pill ${tab === "HORSES" ? "is-active" : ""}" type="button">${escapeHtml(tab)}</button>`).join("")}</nav></section>`;
  }

  function summaryAggsHtml() {
    const records = allCurrentViewRecords();
    const itemIds = new Set(records.flatMap((record) => record.items.map((item) => item.id)));
    const touched = records.reduce((sum, record) => sum + record.items.filter((item) => packingRow(record.horse.id, record.kit?.id, item.id)).length, 0);
    return section("HORSE KITS", `<div class="rs-stack-aggs">${agg(records.length, "HORSES", "horses")}${agg(itemIds.size, "KIT ITEMS", "kit_items")}${agg(touched, "TOUCHED", "touched")}</div>`, "is-summary-aggs");
  }

  function secondaryTabsHtml() {
    const controls = (state?.secondaryControls || []).length ? state.secondaryControls : [
      { key: "all", label: "ALL" },
      { key: "wave_one", label: "WAVE ONE" },
      { key: "wave_two", label: "WAVE TWO" },
      { key: "not_going", label: "NOT GOING" }
    ];
    return `<section class="rs-stack-section is-secondary-controls"><div class="rs-stack-tabs">${controls.map((control) => {
      const key = control.key || "";
      return `<button class="rs-stack-pill ${ui.viewKey === key ? "is-active" : ""}" type="button" data-action="set-view" data-view-key="${escapeAttr(key)}">${escapeHtml(control.label || key)}</button>`;
    }).join("")}</div></section>`;
  }

  function countAggsHtml() {
    const totals = rows.reduce((sum, row) => {
      sum.need += row.counts.need;
      sum.packed += row.counts.packed;
      sum.left += row.counts.left;
      return sum;
    }, { need: 0, packed: 0, left: 0 });
    return `<section class="rs-stack-section is-count-aggs"><div class="rs-secondary-count-aggs"><div class="rs-stack-aggs">${agg(totals.need, "NEED", "need")}${agg(totals.packed, "PACKED", "packed")}${agg(totals.left, "LEFT", "left")}</div></div></section>`;
  }

  function laneTabsHtml() {
    const lanes = [
      { key: "open", label: "OPEN" },
      { key: "need", label: "NEED" },
      { key: "packed", label: "PACKED" },
      { key: "left", label: "LEFT" }
    ];
    return `<section class="rs-stack-section is-lane-controls"><div class="rs-stack-tabs is-compact">${lanes.map((lane) => `<button class="rs-stack-pill ${ui.laneKey === lane.key ? "is-active" : ""}" type="button" data-action="set-lane" data-lane-key="${escapeAttr(lane.key)}">${escapeHtml(lane.label)}</button>`).join("")}</div></section>`;
  }

  function searchHtml() {
    return `<section class="rs-stack-section is-search"><div class="rs-airtable-toolbar"><div class="rs-search-wrap"><input class="rs-search" type="search" data-search autocomplete="off" placeholder="Search horses" value="${escapeAttr(ui.search)}"><button class="rs-search-clear ${ui.search ? "is-active" : ""}" type="button" data-action="clear-search" aria-label="Clear search"><span aria-hidden="true">&times;</span></button></div></div></section>`;
  }

  function tableHtml() {
    if (ui.loading) return `<section class="rs-stack-section is-main-table"><div class="rs-airtable-empty">Loading horse kits...</div></section>`;
    if (ui.error) return `<section class="rs-stack-section is-main-table"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></section>`;
    return `
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">HORSES</div><button class="rs-stack-pill" type="button" data-action="print">PRINT</button></div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-entity"><col class="rs-col-count"><col class="rs-col-count"><col class="rs-col-count"></colgroup>
            <thead><tr>
              <th class="rs-row-gutter">#</th>
              ${sortHead("HORSE", "horse")}
              ${sortHead("KIT", "kit")}
              ${sortHead("NEED", "need")}
              ${sortHead("PACKED", "packed")}
              ${sortHead("LEFT", "left")}
            </tr></thead>
            <tbody>${rows.map(tableRowHtml).join("")}</tbody>
          </table>
        </div>
        ${rows.length ? "" : `<div class="rs-status">No rows.</div>`}
        <div class="rs-status">${escapeHtml(`${rows.length} horses | ${state?.counts?.kits || 0} kits | ${state?.counts?.kitItems || 0} kit items | ${state?.counts?.packingRows || 0} touched rows`)}</div>
      </section>
    `;
  }

  function sortHead(label, key) {
    const mark = ui.sortKey === key ? (ui.sortDir === "asc" ? "ASC" : "DESC") : "";
    return `<th><button class="rs-sort-head ${ui.sortKey === key ? "is-active" : ""}" type="button" data-action="set-sort" data-sort-key="${escapeAttr(key)}"><span>${escapeHtml(label)}</span><span class="rs-sort-mark">${escapeHtml(mark)}</span></button></th>`;
  }

  function tableRowHtml(record, index) {
    return `
      <tr class="${ui.drawerOpen && ui.selectedHorseId === record.id ? "is-selected" : ""}">
        <td class="rs-row-gutter">${index + 1}</td>
        <td class="rs-entity-cell"><div class="rs-entity-main"><span class="rs-entity-horse">${escapeHtml(horseLabel(record.horse))}</span><a class="rs-open-text" href="#" data-action="open-horse" data-horse-id="${escapeAttr(record.id)}">Open</a></div></td>
        <td class="rs-entity-cell rs-entity-kit-cell"><span class="rs-entity-sub">${escapeHtml(kitLabel(record.kit))}</span></td>
        <td class="rs-cell-number">${record.counts.need}</td>
        <td class="rs-cell-number">${record.counts.packed}</td>
        <td class="rs-cell-number">${record.counts.left}</td>
      </tr>
    `;
  }

  function commentsHtml() {
    const comments = (state?.comments || []).slice(0, 5);
    return `<section class="rs-stack-section is-comments"><div class="rs-stack-label">COMMENTS</div>${comments.map((comment) => `<div class="rs-comment-row"><div class="rs-comment-body">${escapeHtml(comment.comment || "")}</div><div class="rs-comment-meta">${escapeHtml(comment.scopeLabel || "")}</div></div>`).join("") || `<div class="rs-airtable-empty">No comments.</div>`}</section>`;
  }

  function drawerHtml() {
    const record = selectedRecord();
    if (!record) return "";
    const percent = record.counts.need ? Math.round((record.counts.packed / record.counts.need) * 100) : 0;
    return `
      <div class="rs-drawer-overlay ${ui.drawerOpen ? "is-open" : ""}" data-action="close-drawer" aria-hidden="true"></div>
      <aside class="rs-record-drawer ${ui.drawerOpen ? "is-open" : ""}" aria-hidden="${ui.drawerOpen ? "false" : "true"}">
        <div class="rs-drawer-head">
          <div class="rs-drawer-title-group"><div class="rs-drawer-title">${escapeHtml(horseLabel(record.horse))}</div>${record.horse.profileUrl ? `<a class="rs-drawer-profile-link" href="${escapeAttr(record.horse.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>` : ""}</div>
          <button class="rs-drawer-close" type="button" data-action="close-drawer" aria-label="Close"><span aria-hidden="true">&times;</span></button>
        </div>
        <div class="rs-drawer-body">
          <div class="rs-kit-progress"><div class="rs-kit-progress-label">${percent}% PACKED</div><div class="rs-kit-progress-track"><div class="rs-kit-progress-bar" style="width:${percent}%"></div></div></div>
          <div class="rs-summary-metrics">${agg(record.counts.need, "NEED", "need")}${agg(record.counts.packed, "PACKED", "packed")}${agg(record.counts.left, "LEFT", "left")}</div>
          <div class="rs-kit-item-search-row"><label class="rs-stack-label">SEARCH KIT ITEMS</label><div class="rs-search-wrap"><input class="rs-kit-item-search" type="search" data-item-search autocomplete="off" placeholder="Search kit items" value="${escapeAttr(ui.itemSearch)}"><button class="rs-search-clear ${ui.itemSearch ? "is-active" : ""}" type="button" data-action="clear-item-search" aria-label="Clear kit item search"><span aria-hidden="true">&times;</span></button></div></div>
          <div class="rs-kit-item-row rs-item-filter-row"><div class="rs-kit-item-main"><div class="rs-stack-label">FILTER KIT ITEMS</div></div><div class="rs-kit-actions rs-item-filter-actions">${itemFilter("All", "all")}${itemFilter("Not Packed", "not_packed")}${itemFilter("Packed", "packed")}${itemFilter("Not Needed", "not_needed")}</div></div>
          <div class="rs-kit-items"><div class="rs-stack-label">KIT ITEMS</div>${kitItemsHead()}${filteredItems(record).map((item) => kitItemHtml(record, item)).join("") || `<div class="rs-airtable-empty">No kit items.</div>`}</div>
        </div>
      </aside>
    `;
  }

  function filteredItems(record) {
    const query = ui.itemSearch.trim().toLowerCase();
    return record.items.filter((item) => {
      const status = itemState(record.horse.id, record.kit?.id, item.id);
      const matchesFilter = ui.itemFilter === "all" || ui.itemFilter === status;
      const matchesQuery = !query || itemLabel(item).toLowerCase().includes(query);
      return matchesFilter && matchesQuery;
    });
  }

  function kitItemsHead() {
    return `<div class="rs-kit-item-row rs-kit-items-head"><div class="rs-kit-item-main"><div class="rs-stack-label">ITEM</div></div><div class="rs-kit-actions"><div class="rs-stack-label">NOT PACKED</div><div class="rs-stack-label">PACKED</div><div class="rs-stack-label">NOT NEEDED</div></div></div>`;
  }

  function kitItemHtml(record, item) {
    const value = itemState(record.horse.id, record.kit?.id, item.id);
    const key = stateKey(record.horse.id, record.kit?.id, item.id);
    return `<div class="rs-kit-item-row"><div class="rs-kit-item-main"><div class="rs-kit-item-title rs-stack-label">${escapeHtml(itemLabel(item))}</div></div><div class="rs-kit-actions">${stateButton("Not Packed", "not_packed", value, item, key)}${stateButton("Packed", "packed", value, item, key)}${stateButton("Not Needed", "not_needed", value, item, key)}</div></div>`;
  }

  function stateButton(label, value, active, item, key) {
    return `<button class="rs-state-button ${active === value ? "is-active" : ""} ${ui.savingKey === key ? "is-saving" : ""}" type="button" data-action="set-item-state" data-kit-item-id="${escapeAttr(item.id)}" data-pack-state="${escapeAttr(value)}">${escapeHtml(label)}</button>`;
  }

  function itemFilter(label, value) {
    return `<button class="rs-item-filter ${ui.itemFilter === value ? "is-active" : ""}" type="button" data-action="set-item-filter" data-item-filter="${escapeAttr(value)}">${escapeHtml(label)}</button>`;
  }

  function section(label, body, modifier) {
    return `<section class="rs-stack-section ${modifier || ""}"><div class="rs-stack-label">${escapeHtml(label)}</div>${body}</section>`;
  }

  function agg(value, label, key) {
    return `<div class="rs-stack-agg is-${escapeAttr(key)}"><div class="rs-stack-agg-value">${escapeHtml(value == null ? 0 : value)}</div><div class="rs-stack-agg-label">${escapeHtml(label)}</div></div>`;
  }

  function allCurrentViewRecords() {
    return (state?.horses || []).map(recordForHorse);
  }

  function selectedRecord() {
    return rows.find((row) => row.id === ui.selectedHorseId) || rows[0] || null;
  }

  function horseLabel(horse) {
    return horse?.barnName || horse?.name || horse?.showName || "";
  }

  function kitLabel(kit) {
    return kit?.displayLabel || kit?.label || kit?.name || "";
  }

  function itemLabel(item) {
    return item?.displayLabel || item?.displayName || item?.label || item?.name || "";
  }

  function compareItem(a, b) {
    return compareNumber(a.sortOrder, b.sortOrder) || compareText(itemLabel(a), itemLabel(b));
  }

  function compareText(a, b) {
    return String(a || "").localeCompare(String(b || ""), undefined, { numeric: true, sensitivity: "base" });
  }

  function compareNumber(a, b) {
    return (Number(a) || 0) - (Number(b) || 0);
  }

  function stateKey(horseId, kitId, itemId) {
    return [horseId || "", kitId || "", itemId || ""].join("::");
  }

  function focusSearch(selector, caret) {
    requestAnimationFrame(() => {
      const input = root.querySelector(selector);
      if (input) {
        input.focus();
        input.setSelectionRange(caret, caret);
      }
    });
  }

  function assertState(data) {
    if (!data || data.ok !== true) throw new Error("invalid_horse_kits_state");
    if (!Array.isArray(data.horses) || !Array.isArray(data.kits) || !Array.isArray(data.kitItems) || !Array.isArray(data.packingRows)) {
      throw new Error("missing_horse_kits_arrays");
    }
  }

  function includes(values, value) {
    return Array.isArray(values) && values.includes(value);
  }

  function unique(values) {
    return [...new Set(values || [])];
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#039;"
    }[char]));
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }
})();
