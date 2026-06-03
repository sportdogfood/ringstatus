(function () {
  const root = document.getElementById("horse-kit-static-proof");
  if (!root) return;

  const globalConfig = window.WEC_HORSE_KITS_CONFIG || {};
  const config = {
    apiUrl: root.dataset.apiUrl || globalConfig.apiUrl || "/wec-packing/horse-kits",
    printUrl: root.dataset.printUrl || globalConfig.printUrl || "",
    pdfWorkerUrl: root.dataset.pdfWorkerUrl || globalConfig.pdfWorkerUrl || "https://ringstatus-pdf.gombcg.workers.dev/",
    usePdfWorker: truthy(root.dataset.enablePrintPdf || globalConfig.enablePrintPdf),
    packWaveKey: root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one"
  };

  const ui = {
    selectedHorseId: "",
    drawerOpen: false,
    loading: false,
    savingKey: "",
    message: "",
    error: "",
    search: "",
    itemSearch: "",
    itemFilter: "all",
    laneKey: "open",
    secondaryView: config.packWaveKey || "wave_one",
    sortKey: "horse",
    sortDir: "asc",
    addItemName: "",
    addItemQty: "1",
    commentText: "",
    commentShortId: "",
    editingCommentId: "",
    commentsOpen: false
  };

  let state = null;
  let records = [];
  const optimisticItemStates = new Map();

  load();

  root.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;

    const action = target.dataset.action;
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
      records = buildRecords();
      render();
      return;
    }

    if (action === "clear-kit-item-search") {
      ui.itemSearch = "";
      render();
      return;
    }

    if (action === "print-list") {
      openPrintPage();
      return;
    }

    if (action === "set-lane") {
      const laneKey = target.dataset.laneKey || "open";
      if (laneKey === "print") {
        openPrintPage();
        return;
      }
      ui.laneKey = laneKey;
      records = buildRecords();
      keepSelectedRecord();
      render();
      return;
    }

    if (action === "set-secondary-view") {
      ui.secondaryView = target.dataset.secondaryView || "all";
      records = buildRecords();
      keepSelectedRecord();
      render();
      return;
    }

    if (action === "set-item-state") {
      await setItemState(target);
      return;
    }

    if (action === "set-item-filter") {
      ui.itemFilter = target.dataset.itemFilter || "all";
      render();
      return;
    }

    if (action === "set-sort") {
      setTableSort(target.dataset.sortKey || "horse");
      return;
    }

    if (action === "add-kit-item") {
      await addKitItem();
      return;
    }

    if (action === "increment-add-qty") {
      incrementAddQuantity();
      return;
    }

    if (action === "edit-comment") {
      const comment = horseComments(selectedHorse()?.id).find((row) => row.id === target.dataset.commentId);
      ui.editingCommentId = comment?.id || "";
      ui.commentText = comment?.comment || "";
      ui.commentShortId = "";
      ui.commentsOpen = true;
      render();
      return;
    }

    if (action === "toggle-comments") {
      ui.commentsOpen = !ui.commentsOpen;
      render();
      return;
    }

    if (action === "save-comment") {
      await saveComment();
      return;
    }

    if (action === "reload") {
      await load();
    }
  });

  root.addEventListener("input", (event) => {
    const input = event.target;
    if (input.matches("[data-search]")) {
      ui.search = input.value || "";
      const caret = input.selectionStart || ui.search.length;
      records = buildRecords();
      render();
      restoreSearchFocus(caret);
    }
    if (input.matches("[data-add-item-name]")) {
      ui.addItemName = input.value || "";
    }
    if (input.matches("[data-add-item-qty]")) {
      ui.addItemQty = input.value || "1";
    }
    if (input.matches("[data-kit-item-search]")) {
      ui.itemSearch = input.value || "";
      const caret = input.selectionStart || ui.itemSearch.length;
      render();
      restoreKitItemSearchFocus(caret);
    }
    if (input.matches("[data-comment-text]")) {
      ui.commentText = input.value || "";
    }
    if (input.matches("[data-comment-short]")) {
      ui.commentShortId = input.value || "";
      const selected = commentShorts().find((row) => row.id === ui.commentShortId);
      if (selected && !ui.commentText.trim()) {
        ui.commentText = selected.label || selected.comment || "";
        render();
      }
    }
  });

  root.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && ui.drawerOpen) {
      ui.drawerOpen = false;
      render();
    }
  });

  async function load() {
    ui.loading = true;
    ui.error = "";
    render();

    try {
      state = await fetchJson(`${config.apiUrl}?packWaveKey=${encodeURIComponent(config.packWaveKey)}`);
      records = buildRecords();
      keepSelectedRecord();
      ui.message = sourceLine();
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
    if (!response.ok || data.ok === false) {
      throw new Error(data.detail || data.error || `${response.status} ${response.statusText}`);
    }
    return data;
  }

  async function postAction(payload) {
    const url = `${config.apiUrl}?packWaveKey=${encodeURIComponent(config.packWaveKey)}`;
    const data = await fetchJson(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    state = data.state || state;
    records = buildRecords();
    return data;
  }

  async function setItemState(button) {
    const horseId = button.dataset.horseId || selectedHorse()?.id || "";
    const kitId = button.dataset.kitId || assignedKit(horseId)?.id || "";
    const kitItemId = button.dataset.kitItemId || "";
    const packState = button.dataset.packState || "";
    if (!horseId || !kitItemId || !packState || !packWaveId()) return;

    const optimisticKey = itemStateKey(kitItemId, horseId, kitId);
    optimisticItemStates.set(optimisticKey, packState);
    records = buildRecords();
    ui.savingKey = `${horseId}:${kitItemId}`;
    ui.message = "Saving...";
    ui.error = "";
    render();

    try {
      await postAction({
        action: "set_static_kit_item_state",
        horseId,
        kitId,
        kitItemId,
        packWaveId: packWaveId(),
        packState
      });
      optimisticItemStates.delete(optimisticKey);
      records = buildRecords();
      ui.message = "Saved.";
    } catch (error) {
      optimisticItemStates.delete(optimisticKey);
      records = buildRecords();
      ui.error = error.message || String(error);
    } finally {
      ui.savingKey = "";
      render();
    }
  }

  async function addKitItem() {
    const kit = assignedKit(selectedHorse()?.id);
    const label = ui.addItemName.trim();
    const quantity = Math.max(1, Number.parseInt(ui.addItemQty || "1", 10) || 1);
    if (!kit?.id || !label) {
      ui.message = "Enter an item name.";
      render();
      return;
    }

    ui.savingKey = "add-kit-item";
    ui.message = "Adding kit item...";
    render();

    try {
      await postAction({
        action: "create_kit_item",
        kitId: kit.id,
        label,
        manualQuantity: quantity
      });
      ui.addItemName = "";
      ui.addItemQty = "1";
      ui.message = "Kit item added.";
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.savingKey = "";
      render();
    }
  }

  async function saveComment() {
    const horse = selectedHorse();
    const comment = ui.commentText.trim();
    if (!horse?.id || !comment) {
      ui.message = "Enter a comment.";
      render();
      return;
    }
    ui.savingKey = "comment";
    ui.message = "Saving comment...";
    render();
    try {
      await postAction({
        action: "save_comment",
        commentId: ui.editingCommentId,
        horseId: horse.id,
        scopeLabel: horseLabel(horse),
        packWaveId: packWaveId(),
        commentShortId: ui.commentShortId,
        comment
      });
      ui.commentText = "";
      ui.commentShortId = "";
      ui.editingCommentId = "";
      ui.commentsOpen = true;
      ui.message = "Comment saved.";
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.savingKey = "";
      render();
    }
  }

  function incrementAddQuantity() {
    const current = Number.parseInt(ui.addItemQty || "1", 10) || 1;
    ui.addItemQty = String(Math.min(6, current + 1));
    render();
  }

  function buildRecords() {
    const rows = visibleHorses().map((horse) => {
      const kit = assignedKit(horse.id);
      const counts = rollup(horse.id, kit?.id);
      return {
        id: horse.id,
        horse,
        kit,
        counts
      };
    });
    return sortRecords(rows.filter(recordMatchesLane));
  }

  function sortRecords(rows) {
    const key = ui.sortKey || "horse";
    const direction = ui.sortDir === "desc" ? -1 : 1;
    return [...rows].sort((a, b) => {
      if (key === "horse") return compareText(horseLabel(a.horse), horseLabel(b.horse)) * direction;
      if (key === "kit") return compareText(kitDisplayLabel(a.kit), kitDisplayLabel(b.kit)) * direction;
      if (key === "need") return compareNumber(a.counts?.needed, b.counts?.needed) * direction || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (key === "packed") return compareNumber(a.counts?.packed, b.counts?.packed) * direction || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (key === "left") return compareNumber(a.counts?.left, b.counts?.left) * direction || compareText(horseLabel(a.horse), horseLabel(b.horse));
      return compareText(horseLabel(a.horse), horseLabel(b.horse));
    });
  }

  function setTableSort(key) {
    if (ui.sortKey === key) {
      ui.sortDir = ui.sortDir === "asc" ? "desc" : "asc";
    } else {
      ui.sortKey = key;
      ui.sortDir = "asc";
    }
    records = sortRecords(records);
    render();
  }

  function visibleHorses() {
    const sourceHorses = state?.allHorses?.length ? state.allHorses : state?.horses || [];
    const horses = sourceHorses
      .filter(matchesSecondaryView)
      .sort((a, b) => String(horseLabel(a)).localeCompare(String(horseLabel(b))));
    const query = ui.search.trim().toLowerCase();
    if (!query) return horses;
    return horses.filter((horse) => [
      horse.name,
      horse.barnName,
      horse.showName,
      horse.barn_name,
      horse.show_name,
      horse.display_horse_barn_name
    ].join(" ").toLowerCase().includes(query));
  }

  function matchesSecondaryView(horse) {
    const key = ui.secondaryView || config.packWaveKey || "wave_one";
    if (!key || key === "all") return true;
    if (key === "wave_one") return horse.waveState === "wave_one" || horse.waveOne === true || horse.wave_one === true;
    if (key === "wave_two") return horse.waveState === "wave_two" || horse.waveTwo === true || horse.wave_two === true;
    if (key === "not_going") return horse.waveState === "not_going" || horse.notGoing === true || horse.not_going === true;
    return horse.waveState === key || horse[key] === true;
  }

  function recordMatchesLane(record) {
    const key = ui.laneKey || "open";
    if (!key || key === "all" || key === "print") return true;
    const counts = effectiveRecordCounts(record);
    if (key === "open" || key === "left") return counts.left > 0;
    if (key === "need" || key === "needed") return counts.needed > 0;
    if (key === "packed" || key === "pack") return counts.packed > 0;
    if (key === "not_needed") return counts.notNeeded > 0;
    return true;
  }

  function effectiveRecordCounts(record) {
    const counts = record?.counts || {};
    const summary = record?.horse?.kitSummary || record?.horse?.kit_summary || {};
    return {
      total: Math.max(countNumber(counts.total), countNumber(summary.total)),
      needed: Math.max(countNumber(counts.needed), countNumber(summary.needed), countNumber(summary.need)),
      packed: Math.max(countNumber(counts.packed), countNumber(summary.packed)),
      notNeeded: Math.max(countNumber(counts.notNeeded), countNumber(summary.notNeeded), countNumber(summary.not_needed)),
      left: Math.max(countNumber(counts.left), countNumber(summary.left))
    };
  }

  function countNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : 0;
  }

  function activeKits() {
    return (state?.kits || [])
      .filter((kit) => kit.status !== "inactive" && kit.active !== false)
      .sort((a, b) => Number(a.sortOrder || 0) - Number(b.sortOrder || 0) || String(kitDisplayLabel(a)).localeCompare(String(kitDisplayLabel(b))));
  }

  function assignedKit(horseId) {
    const rowKitId = (state?.packingRows || []).find((row) => (row.horseIds || []).includes(horseId) && row.kitIds?.length)?.kitIds?.[0] || "";
    const kits = activeKits();
    return kits.find((kit) => kit.id === rowKitId) || kits.find((kit) => kit.kitItemIds?.length) || kits[0] || null;
  }

  function kitItems(kitId) {
    const kit = (state?.kits || []).find((candidate) => candidate.id === kitId);
    const nestedItems = (kit?.items || [])
      .filter((item) => item.status !== "inactive" && item.active !== false)
      .sort(compareKitItemAlpha);
    if (nestedItems.length) return nestedItems;
    const items = (state?.kitItems || [])
      .filter((item) => item.status !== "inactive" && item.active !== false)
      .filter((item) => !kitId || (item.kitIds || []).includes(kitId))
      .sort(compareKitItemAlpha);
    return items.length ? items : (state?.kitItems || []).filter((item) => item.status !== "inactive").sort(compareKitItemAlpha);
  }

  function rowForKitItem(itemId, horseId, kitId) {
    return (state?.packingRows || []).find((row) =>
      (row.horseIds || []).includes(horseId) &&
      (row.kitItemIds || []).includes(itemId) &&
      (!kitId || !row.kitIds?.length || row.kitIds.includes(kitId))
    ) || null;
  }

  function kitItemState(itemId, horseId, kitId) {
    const optimistic = optimisticItemStates.get(itemStateKey(itemId, horseId, kitId));
    if (optimistic) return optimistic;
    const row = rowForKitItem(itemId, horseId, kitId);
    if (!row) return "not_packed";
    if (row.neededState === "not_needed" || row.packState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function rollup(horseId, kitId) {
    const items = kitItems(kitId);
    const counts = { total: items.length, needed: items.length, packed: 0, notNeeded: 0, left: items.length };
    for (const item of items) {
      const stateName = kitItemState(item.id, horseId, kitId);
      if (stateName === "packed") counts.packed += 1;
      if (stateName === "not_needed") counts.notNeeded += 1;
    }
    counts.needed = Math.max(0, counts.total - counts.notNeeded);
    counts.left = Math.max(0, counts.needed - counts.packed);
    return counts;
  }

  function selectedHorse() {
    const horses = state?.allHorses?.length ? state.allHorses : state?.horses || [];
    return horses.find((horse) => horse.id === ui.selectedHorseId) || records[0]?.horse || null;
  }

  function keepSelectedRecord() {
    if (!records.some((record) => record.id === ui.selectedHorseId)) {
      ui.selectedHorseId = records[0]?.id || "";
    }
  }

  function packWaveId() {
    return state?.source?.packWaveId || state?.wave?.id || "";
  }

  function sourceLine() {
    const counts = state?.counts || {};
    return `${counts.visibleHorses || records.length || 0} horses | ${counts.kits || 0} kits | ${counts.kitItems || 0} kit items | ${counts.packingRows || 0} touched rows`;
  }

  function activeStackRows() {
    const rows = state?.groupStack?.activeRows || [];
    const renderOrder = ["header", "primary_tabs", "summary_aggs", "secondary_controls", "count_aggs", "lane_controls", "search", "main_table", "comments"];
    const orderIndex = new Map(renderOrder.map((key, index) => [key, index]));
    const allowed = new Set([...renderOrder, "search_aggs"]);
    const activeRows = rows
      .filter((row) => row && !row.hidden && row.active !== false && allowed.has(row.renderKey))
      .sort((a, b) => {
        const aIndex = orderIndex.has(a.renderKey) ? orderIndex.get(a.renderKey) : renderOrder.length + Number(a.sortOrder || 0);
        const bIndex = orderIndex.has(b.renderKey) ? orderIndex.get(b.renderKey) : renderOrder.length + Number(b.sortOrder || 0);
        return aIndex - bIndex || Number(a.sourceIndex || 0) - Number(b.sourceIndex || 0);
      });
    return activeRows.length ? activeRows : [
      { renderKey: "header", displayLabel: "Header" },
      { renderKey: "primary_tabs", displayLabel: "Primary Tabs" },
      { renderKey: "summary_aggs", displayLabel: "Horse Kits" },
      { renderKey: "secondary_controls", displayLabel: "Views" },
      { renderKey: "count_aggs", displayLabel: "" },
      { renderKey: "lane_controls", displayLabel: "Lane Controls" },
      { renderKey: "search", displayLabel: "Search" },
      { renderKey: "main_table", displayLabel: "Horses" },
      { renderKey: "comments", displayLabel: "Comments" }
    ];
  }

  function render() {
    const scrollState = captureScrollState();
    if (ui.loading) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty">Loading horse kits...</div></div>`;
      restoreScrollState(scrollState);
      return;
    }

    if (ui.error && !state) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></div>`;
      restoreScrollState(scrollState);
      return;
    }

    if (!records.length && ui.error) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></div>`;
      restoreScrollState(scrollState);
      return;
    }

    const selected = selectedHorse();
    const kit = assignedKit(selected?.id);
    const statusText = !records.length ? "No horses found" : (ui.error || ui.message || sourceLine());
    root.innerHTML = `
      <div class="rs-airtable-shell">
        <div class="rs-page-stack">
          ${activeStackRows().map((row) => stackRowHtml(row, statusText)).join("")}
        </div>

        <div class="rs-drawer-overlay ${ui.drawerOpen ? "is-open" : ""}" data-action="close-drawer"></div>
        <div class="rs-record-drawer ${ui.drawerOpen ? "is-open" : ""}" aria-label="Horse kit details">
          <div class="rs-drawer-head">
            <div class="rs-drawer-title-group">
              <div class="rs-drawer-title rs-page-subtitle">${escapeHtml(horseLabel(selected) || "Horse Kit")}</div>
              ${selected?.profileUrl ? `<a class="rs-drawer-profile-link" href="${escapeAttr(selected.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>` : ""}
            </div>
            <button class="rs-drawer-close" type="button" aria-label="Close" data-action="close-drawer"><span aria-hidden="true">&times;</span></button>
          </div>
          <div class="rs-drawer-body">
            ${selected ? detailHtml(selected, kit) : ""}
          </div>
        </div>
      </div>
    `;
    restoreScrollState(scrollState);
  }

  function stackRowHtml(row, statusText) {
    const key = row?.renderKey || "";
    if (key === "header") return pageHeaderHtml(row);
    if (key === "primary_tabs") return primaryTabsHtml(row);
    if (key === "lane_controls") return laneControlsHtml(row);
    if (key === "secondary_controls") return secondaryControlsHtml(row);
    if (key === "summary_aggs") return summaryAggsHtml(row);
    if (key === "count_aggs") return countAggsHtml(row);
    if (key === "search") return searchHtml(row);
    if (key === "search_aggs") return searchAggsHtml(row);
    if (key === "main_table") return tableStackHtml(row, statusText);
    if (key === "comments") return pageCommentsHtml(row);
    return "";
  }

  function stackSectionHtml(row, innerHtml, extraClass = "") {
    return `
      <section class="rs-stack-section ${escapeAttr(extraClass)}" data-render-key="${escapeAttr(row?.renderKey || "")}" data-component-key="${escapeAttr(row?.componentKey || "")}">
        ${innerHtml}
      </section>
    `;
  }

  function pageHeaderHtml(row) {
    const wave = state?.wave || {};
    const title = wave.wecReportTitle || "";
    const subtitle = wave.wecReportSubtitle || "";
    return stackSectionHtml(row, `
      <div class="rs-page-header">
        ${title ? `<div class="rs-page-title">${escapeHtml(title)}</div>` : ""}
        ${subtitle ? `<div class="rs-page-subtitle">${escapeHtml(subtitle)}</div>` : ""}
      </div>
    `, "is-header");
  }

  function primaryTabsHtml(row) {
    const tabs = primaryTabs();
    return stackSectionHtml(row, `
      <div class="rs-stack-tabs" role="navigation" aria-label="${escapeAttr(row?.displayLabel || "Primary Tabs")}">
        ${tabs.map((tab) => `<button class="rs-stack-pill ${tab.key === "horses" ? "is-active" : ""}" type="button" data-primary-tab="${escapeAttr(tab.key)}">${escapeHtml(tab.label)}</button>`).join("")}
      </div>
    `, "is-primary-tabs");
  }

  function laneControlsHtml(row) {
    const lanes = laneControls();
    return stackSectionHtml(row, `
      <div class="rs-stack-tabs is-compact" aria-label="${escapeAttr(row?.displayLabel || "Lane Controls")}">
        ${lanes.map((lane) => {
          const key = lane.key || "";
          const action = key === "print" ? "print-list" : "set-lane";
          return `<button class="rs-stack-pill ${key === ui.laneKey ? "is-active" : ""}" type="button" data-action="${escapeAttr(action)}" data-lane-key="${escapeAttr(key)}">${escapeHtml(lane.label)}</button>`;
        }).join("")}
      </div>
    `, "is-lane-controls");
  }

  function secondaryControlsHtml(row) {
    const views = secondaryControls();
    return stackSectionHtml(row, `
      <div class="rs-stack-tabs is-compact">
        ${views.map((view) => `<button class="rs-stack-pill ${view.key === ui.secondaryView ? "is-active" : ""}" type="button" data-action="set-secondary-view" data-secondary-view="${escapeAttr(view.key)}">${escapeHtml(view.label)}</button>`).join("")}
      </div>
    `, "is-secondary-controls");
  }

  function searchAggsHtml(row) {
    return stackSectionHtml(row, `
      <div class="rs-stack-head">
        <div class="rs-stack-label">${escapeHtml(row?.displayLabel || "Horse Kits")}</div>
      </div>
      <div class="rs-stack-body">
        <div class="rs-stack-aggs">${stackAggsHtml(summaryAggRows(row), summaryAggValues())}</div>
        ${searchControlsHtml()}
      </div>
    `, "is-search-aggs");
  }

  function summaryAggsHtml(row) {
    return stackSectionHtml(row, `
      <div class="rs-stack-head">
        <div class="rs-stack-label">${escapeHtml(row?.displayLabel || "Horse Kits")}</div>
      </div>
      <div class="rs-stack-body">
        <div class="rs-stack-aggs">${stackAggsHtml(summaryAggRows(row), summaryAggValues())}</div>
      </div>
    `, "is-summary-aggs");
  }

  function countAggsHtml(row) {
    return stackSectionHtml(row, `
      <div class="rs-secondary-count-aggs"><div class="rs-stack-aggs is-counts">${stackAggsHtml(countAggRows(row), tableAggValues())}</div></div>
    `, "is-count-aggs");
  }

  function searchHtml(row) {
    return stackSectionHtml(row, `
      <div class="rs-stack-body">
        ${searchControlsHtml()}
      </div>
    `, "is-search");
  }

  function searchControlsHtml() {
    return `
      <div class="rs-airtable-toolbar">
        <div class="rs-search-wrap">
          <input class="rs-search" type="text" autocomplete="off" data-search placeholder="Search horses" value="${escapeAttr(ui.search)}">
          <button class="rs-search-clear ${ui.search ? "is-active" : ""}" type="button" aria-label="Clear search" data-action="clear-search"><span aria-hidden="true">&times;</span></button>
        </div>
      </div>
    `;
  }

  function stackAggsHtml(aggs, values) {
    return aggs.map((agg) => stackAggHtml(aggValue(agg, values), agg.label, agg.key, agg.shade)).join("");
  }

  function stackAggHtml(value, label, key = "", shade = "") {
    return `
      <div class="rs-stack-agg ${key ? `is-${escapeAttr(key)}` : ""} ${shade ? `is-shade-${escapeAttr(shade)}` : ""}">
        <div class="rs-stack-agg-value">${escapeHtml(value)}</div>
        <div class="rs-stack-agg-label">${escapeHtml(label)}</div>
      </div>
    `;
  }

  function summaryAggRows(row) {
    return rowAggRows(sourceAggRow("entity_1") || row, [
      { key: "horses", label: "HORSES", shade: "brown" },
      { key: "kit_items", label: "KIT ITEMS", shade: "green" },
      { key: "touched", label: "TOUCHED", shade: "grey" }
    ]);
  }

  function countAggRows(row) {
    return rowAggRows(sourceAggRow("entity_2") || row || activeStackRows().find((candidate) => candidate.renderKey === "count_aggs"), [
      { key: "need", label: "NEED", shade: "brown" },
      { key: "packed", label: "PACKED", shade: "green" },
      { key: "left", label: "LEFT", shade: "grey" }
    ]);
  }

  function rowAggRows(row, fallback) {
    const aggs = (row?.aggRows || []).filter((agg) => agg && agg.active !== false);
    return aggs.length ? aggs : fallback;
  }

  function sourceAggRow(role) {
    return (state?.groupStack?.activeRows || []).find((row) => row.role === role && row.addAggregates && row.aggRows?.length) || null;
  }

  function summaryAggValues() {
    const counts = state?.counts || {};
    return {
      horses: records.length || counts.visibleHorses || 0,
      kit_items: counts.kitItems || 0,
      touched: counts.packingRows || 0
    };
  }

  function tableAggValues() {
    const counts = state?.counts || {};
    if (Number.isFinite(Number(counts.kitItems))) {
      const total = Number(counts.kitItems || 0);
      const notNeeded = Number(counts.notNeededRows || 0);
      const packed = Number(counts.packedRows || 0);
      const need = Math.max(0, total - notNeeded);
      return {
        need,
        packed,
        left: Math.max(0, need - packed)
      };
    }
    return records.reduce((totals, record) => {
      totals.need += Number(record?.counts?.needed || 0);
      totals.packed += Number(record?.counts?.packed || 0);
      totals.left += Number(record?.counts?.left || 0);
      return totals;
    }, { need: 0, packed: 0, left: 0 });
  }

  function aggValue(agg, values) {
    const key = agg?.key || "";
    if (key === "horses") return values.horses || 0;
    if (key === "kit_items" || key === "items") return values.kit_items || values.items || 0;
    if (key === "touched") return values.touched || 0;
    if (key === "need" || key === "needed") return values.need ?? values.needed ?? 0;
    if (key === "packed" || key === "pack") return values.packed || 0;
    if (key === "left") return values.left || 0;
    if (key === "not_needed") return values.notNeeded || values.not_needed || 0;
    return values[key] || 0;
  }

  function tableStackHtml(row, statusText) {
    return stackSectionHtml(row, `
      <div class="rs-table-stack-head">
        <div class="rs-stack-label">${escapeHtml(row?.displayLabel || "Horses")}</div>
        <button class="rs-stack-pill" type="button" data-action="print-list">Print</button>
      </div>
      <div class="rs-airtable-scroll">
        <table class="rs-airtable-grid">
          <colgroup>
            <col class="rs-col-gutter">
            <col class="rs-col-entity">
            <col class="rs-col-entity">
            <col class="rs-col-count">
            <col class="rs-col-count">
            <col class="rs-col-count">
          </colgroup>
          <thead>
            <tr>
              <th class="rs-row-gutter">#</th>
              <th>${sortableHeaderHtml("Horse", "horse")}</th>
              <th>${sortableHeaderHtml("Kit", "kit")}</th>
              <th>${sortableHeaderHtml("Need", "need")}</th>
              <th>${sortableHeaderHtml("Packed", "packed")}</th>
              <th>${sortableHeaderHtml("Left", "left")}</th>
            </tr>
          </thead>
          <tbody>
            ${records.map(recordRowHtml).join("")}
          </tbody>
        </table>
      </div>
      <div class="rs-status ${ui.error ? "is-error" : ""}">${escapeHtml(statusText)}</div>
    `, "is-main-table");
  }

  function pageCommentsHtml(row) {
    const comments = pageComments();
    return stackSectionHtml(row, `
      <div class="rs-comments is-page-comments">
        <div class="rs-comments-head">
          <div class="rs-stack-label">${escapeHtml(row?.displayLabel || "comments")}</div>
        </div>
        <div class="rs-comment-list">
          ${comments.map(commentRowHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}
        </div>
      </div>
    `, "is-comments");
  }

  function captureScrollState() {
    const tableScroll = root.querySelector(".rs-airtable-scroll");
    const drawerBody = root.querySelector(".rs-drawer-body");
    return {
      tableTop: tableScroll ? tableScroll.scrollTop : 0,
      drawerTop: drawerBody ? drawerBody.scrollTop : 0
    };
  }

  function restoreScrollState(scrollState) {
    if (!scrollState) return;
    requestAnimationFrame(() => {
      const tableScroll = root.querySelector(".rs-airtable-scroll");
      const drawerBody = root.querySelector(".rs-drawer-body");
      if (tableScroll) tableScroll.scrollTop = scrollState.tableTop || 0;
      if (drawerBody) drawerBody.scrollTop = scrollState.drawerTop || 0;
    });
  }

  function recordRowHtml(record, index) {
    const selected = ui.drawerOpen && record.id === ui.selectedHorseId;
    return `
      <tr class="${selected ? "is-selected" : ""}" data-action="open-horse" data-horse-id="${record.id}" tabindex="0">
        <td class="rs-row-gutter">${index + 1}</td>
        <td class="rs-entity-cell rs-entity-horse-cell">
          <div class="rs-entity-main">
            <span class="rs-entity-horse">${escapeHtml(horseLabel(record.horse))}</span>
            <span class="rs-open-text">Open</span>
          </div>
        </td>
        <td class="rs-entity-cell rs-entity-kit-cell"><span class="rs-entity-sub">${escapeHtml(kitDisplayLabel(record.kit))}</span></td>
        <td class="rs-cell-number">${escapeHtml(record.counts.needed)}</td>
        <td class="rs-cell-number">${escapeHtml(record.counts.packed)}</td>
        <td class="rs-cell-number">${escapeHtml(record.counts.left)}</td>
      </tr>
    `;
  }

  function sortableHeaderHtml(label, key) {
    const active = ui.sortKey === key;
    return `
      <button class="rs-sort-head ${active ? "is-active" : ""}" type="button"
        data-action="set-sort"
        data-sort-key="${escapeAttr(key)}"
        aria-label="Sort ${escapeAttr(label)} ${active && ui.sortDir === "asc" ? "descending" : "ascending"}">
        <span>${escapeHtml(label)}</span>
      </button>
    `;
  }

  function detailHtml(horse, kit) {
    const counts = rollup(horse.id, kit?.id);
    const items = filteredKitItems(kit?.id);
    const percentPacked = counts.needed > 0 ? Math.round((counts.packed / counts.needed) * 100) : 0;
    const drawerLabels = drawerItemLabels();
    return `
      <div class="rs-detail-summary">
        <div class="is-hidden" aria-hidden="true">
          <div class="rs-stack-label">Kit</div>
          <div class="rs-field-value">${escapeHtml(kitDisplayLabel(kit) || "No kit")}</div>
        </div>
        <div class="rs-kit-progress" aria-label="${escapeAttr(percentPacked)}% packed">
          <div class="rs-kit-progress-label">${escapeHtml(percentPacked)}% PACKED</div>
          <div class="rs-kit-progress-track">
            <div class="rs-kit-progress-bar" style="width: ${escapeAttr(Math.min(100, Math.max(0, percentPacked)))}%"></div>
          </div>
        </div>
        <div class="rs-summary-metrics">
          ${drawerMetricRows().map((agg) => metricHtml(agg.label, aggValue(agg, counts), agg.key)).join("")}
        </div>
      </div>
      <div class="rs-add-row is-hidden" aria-hidden="true">
        <label class="rs-add-label" for="rs-add-kit-item">add_item</label>
        <input id="rs-add-kit-item" class="rs-add-input" data-add-item-name value="${escapeAttr(ui.addItemName)}" placeholder="Item label">
        <div class="rs-add-controls">
          <input class="rs-add-qty" data-add-item-qty value="${escapeAttr(ui.addItemQty)}" type="number" min="1" max="6" step="1">
          <button class="rs-plain-button" type="button" data-action="increment-add-qty" ${Number(ui.addItemQty || 1) >= 6 ? "disabled" : ""}>ADD +1</button>
          <button class="rs-plain-button is-primary" type="button" data-action="add-kit-item" ${ui.savingKey === "add-kit-item" ? "disabled" : ""}>ADD</button>
        </div>
      </div>
      <div class="rs-decision-row is-hidden" aria-hidden="true">
        <div class="rs-decision-state">
          <button class="rs-plain-button is-primary" type="button" disabled>OPEN</button>
          <div class="rs-field-label">decision</div>
        </div>
        <div class="rs-decision-actions">
          <button class="rs-plain-button" type="button" disabled>CLEAR</button>
          <button class="rs-plain-button" type="button" disabled>MAX</button>
          <button class="rs-plain-button" type="button" disabled>BUY</button>
          <button class="rs-plain-button" type="button" disabled>ATTN</button>
          <button class="rs-plain-button" type="button" disabled>SMS</button>
        </div>
      </div>
      <div class="rs-kit-item-search-row">
        <label class="rs-stack-label" for="rs-kit-item-search">${escapeHtml(drawerLabels.search)}</label>
        <div class="rs-search-wrap">
          <input id="rs-kit-item-search" class="rs-kit-item-search" type="text" autocomplete="off" data-kit-item-search value="${escapeAttr(ui.itemSearch)}" placeholder="${escapeAttr(drawerLabels.searchPlaceholder)}">
          <button class="rs-search-clear ${ui.itemSearch ? "is-active" : ""}" type="button" aria-label="Clear kit item search" data-action="clear-kit-item-search"><span aria-hidden="true">&times;</span></button>
        </div>
      </div>
      <div class="rs-kit-item-row rs-item-filter-row" role="group" aria-label="${escapeAttr(drawerLabels.filter)}">
        <div class="rs-kit-item-main">
          <div class="rs-stack-label">${escapeHtml(drawerLabels.filter)}</div>
        </div>
        <div class="rs-kit-actions rs-item-filter-actions">
          ${itemFilterButton("All", "all")}
          ${itemFilterButton("Not Packed", "not_packed")}
          ${itemFilterButton("Packed", "packed")}
          ${itemFilterButton("Not Needed", "not_needed")}
        </div>
      </div>
      <div class="rs-kit-items">
        <div class="rs-kit-item-row rs-kit-items-head" role="row">
          <div class="rs-kit-item-main">
            <div class="rs-stack-label">${escapeHtml(drawerLabels.items)}</div>
          </div>
          <div class="rs-kit-actions">
            <div class="rs-stack-label">Not Packed</div>
            <div class="rs-stack-label">Packed</div>
            <div class="rs-stack-label">Not Needed</div>
          </div>
        </div>
        ${items.map((item) => kitItemRowHtml(item, horse, kit)).join("") || `<div class="rs-empty-row">No kit items.</div>`}
      </div>
      ${commentsHtml(horse)}
      <div class="rs-drawer-bottom">
        <div class="rs-bottom-field">
          <div class="rs-stack-label">Plan:</div>
          <div class="rs-field-value">Horse Specific</div>
        </div>
        <div class="rs-bottom-field">
          <div class="rs-stack-label">System:</div>
          <div class="rs-field-value">Changes save to Airtable through Webflow Cloud.</div>
        </div>
      </div>
    `;
  }

  function drawerMetricRows() {
    const entityTwoRow = sourceAggRow("entity_2");
    const drawerAggs = entityTwoRow?.includeOnDrawer ? rowAggRows(entityTwoRow, []) : [];
    return drawerAggs.length ? drawerAggs : countAggRows();
  }

  function drawerItemLabels() {
    const row = drawerItemsRow();
    const itemLabel = row?.displayLabel || row?.tableName || row?.physicalTableName || "Kit Items";
    return {
      items: itemLabel,
      search: `Search ${itemLabel}`,
      filter: `Filter ${itemLabel}`,
      searchPlaceholder: `Search ${String(itemLabel).toLowerCase()}`
    };
  }

  function drawerItemsRow() {
    return (state?.groupStack?.activeRows || []).find((row) =>
      row.renderKey === "drawer_items" ||
      row.componentKey === "rs-kit-items" ||
      row.role === "entity_2" ||
      row.tableName === "pak_kit_items" ||
      row.physicalTableName === "pak_kit_items"
    ) || null;
  }

  function metricHtml(label, value, keyOverride = "") {
    const key = keyOverride || String(label || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
    return `
      <div class="rs-metric rs-metric-${escapeAttr(key)}">
        <div class="rs-metric-value">${escapeHtml(value)}</div>
        <div class="rs-metric-label">${escapeHtml(label)}</div>
      </div>
    `;
  }

  function itemFilterButton(label, value) {
    const active = (ui.itemFilter || "all") === value;
    return `
      <button class="rs-item-filter ${active ? "is-active" : ""}" type="button"
        data-action="set-item-filter"
        data-item-filter="${escapeAttr(value)}">${escapeHtml(label)}</button>
    `;
  }

  function kitItemRowHtml(item, horse, kit) {
    const stateName = kitItemState(item.id, horse.id, kit?.id);
    const saving = ui.savingKey === `${horse.id}:${item.id}`;
    return `
      <div class="rs-kit-item-row">
        <div class="rs-kit-item-main">
          <div class="rs-kit-item-title">${escapeHtml(kitItemDisplayLabel(item))}</div>
        </div>
        <div class="rs-kit-actions">
          ${stateButton("Not Packed", "not_packed", stateName, item, horse, kit, saving)}
          ${stateButton("Packed", "packed", stateName, item, horse, kit, saving)}
          ${stateButton("Not Needed", "not_needed", stateName, item, horse, kit, saving)}
        </div>
      </div>
    `;
  }

  function stateButton(label, value, current, item, horse, kit, saving) {
    const active = current === value;
    return `
      <button class="rs-state-button ${active ? "is-active" : ""} ${saving && current === value ? "is-saving" : ""}" type="button"
        data-action="set-item-state"
        data-horse-id="${escapeAttr(horse.id)}"
        data-kit-id="${escapeAttr(kit?.id || "")}"
        data-kit-item-id="${escapeAttr(item.id)}"
        data-pack-state="${escapeAttr(value)}"
        ${saving ? "disabled" : ""}>${escapeHtml(label)}</button>
    `;
  }

  function commentsHtml(horse) {
    const comments = horseComments(horse.id);
    const shorts = commentShorts();
    return `
      <div class="rs-comments">
        <div class="rs-comments-head">
          <div class="rs-stack-label">comments</div>
        </div>
        <div class="rs-comment-form">
          <select class="rs-comment-short" data-comment-short>
            <option value="">comment short</option>
            ${shorts.map((row) => `<option value="${escapeAttr(row.id)}" ${ui.commentShortId === row.id ? "selected" : ""}>${escapeHtml(row.label)}</option>`).join("")}
          </select>
          <textarea class="rs-comment-input" data-comment-text placeholder="Comment">${escapeHtml(ui.commentText)}</textarea>
          <button class="rs-plain-button is-primary" type="button" data-action="save-comment" ${ui.savingKey === "comment" ? "disabled" : ""}>Save</button>
        </div>
        <div class="rs-comment-accordion ${ui.commentsOpen ? "is-open" : ""}">
          <button class="rs-comment-thread-toggle" type="button" data-action="toggle-comments" aria-expanded="${ui.commentsOpen ? "true" : "false"}">
            <span>Comments (${escapeHtml(comments.length)})</span>
            <span class="rs-comment-toggle-state">${ui.commentsOpen ? "Close" : "Open"}</span>
          </button>
          ${ui.commentsOpen ? `
            <div class="rs-comment-list is-thread-list">
              ${comments.map(commentRowHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}
            </div>
          ` : ""}
        </div>
      </div>
    `;
  }

  function commentRowHtml(comment) {
    return `
      <div class="rs-comment-row">
        <div class="rs-comment-text">${escapeHtml(comment.comment)}</div>
        <button class="rs-text-button" type="button" data-action="edit-comment" data-comment-id="${escapeAttr(comment.id)}">Edit</button>
      </div>
    `;
  }

  function horseComments(horseId) {
    return (state?.comments || [])
      .filter((comment) => comment.status !== "deleted")
      .filter((comment) => comment.scopeId === horseId || (comment.horseIds || []).includes(horseId))
      .sort(compareCommentsLatest);
  }

  function pageComments() {
    return (state?.comments || [])
      .filter((comment) => comment.status !== "deleted")
      .filter((comment) => !(comment.horseIds || []).length)
      .sort(compareCommentsLatest);
  }

  function compareCommentsLatest(a, b) {
    return String(b.createdAt || b.createdTime || "").localeCompare(String(a.createdAt || a.createdTime || ""));
  }

  function commentShorts() {
    return (state?.commentShorts || []).filter((row) => row.active !== false && row.status !== "inactive");
  }

  function primaryTabs() {
    return (state?.primaryTabs || []).filter((tab) => tab.active !== false);
  }

  function laneControls() {
    return (state?.laneControls || []).filter((lane) => lane.active !== false);
  }

  function secondaryControls() {
    return state?.secondaryControls || [];
  }

  function horseLabel(horse) {
    return horse?.name || horse?.barnName || horse?.showName || "";
  }

  function waveLabel(wave) {
    const value = wave?.wave || wave?.waveType || wave?.key || config.packWaveKey || "wave_one";
    return String(value).replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
  }

  function openPrintPage() {
    const url = printTargetUrl();
    const opened = window.open(url.toString(), "_blank", "noopener");
    ui.message = opened ? (config.usePdfWorker ? "Creating PDF..." : "Opening print page...") : "Popup blocked. Allow popups and press Print again.";
    render();
  }

  function printTargetUrl() {
    const printUrl = new URL(config.printUrl || `${config.apiUrl.replace(/\/$/, "")}/print`, window.location.href);
    printUrl.searchParams.set("packWaveKey", config.packWaveKey);
    printUrl.searchParams.set("autoprint", "1");
    return config.usePdfWorker ? pdfWorkerUrl(printSafeUrl(printUrl), `horse-kits-${safeFilename(config.packWaveKey)}.pdf`) : printUrl;
  }

  function printSafeUrl(printUrl) {
    const safeUrl = new URL(printUrl.toString());
    if (safeUrl.hostname === "ringstatus.webflow.io") safeUrl.hostname = "ringstatus.com";
    return safeUrl;
  }

  function pdfWorkerUrl(printUrl, filename) {
    const url = new URL(config.pdfWorkerUrl || "https://ringstatus-pdf.gombcg.workers.dev/");
    url.searchParams.set("url", printUrl.toString());
    url.searchParams.set("filename", filename);
    return url;
  }

  function safeFilename(value) {
    return String(value || "horse-kits").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "") || "horse-kits";
  }

  function truthy(value) {
    return value === true || value === "true" || value === "1" || value === 1;
  }

  function formatDate(value) {
    if (!value) return "";
    const [year, month, day] = String(value).split("-");
    if (!year || !month || !day) return String(value);
    return `${Number(month)}/${Number(day)}/${year}`;
  }

  function kitDisplayLabel(kit) {
    return kit?.displayLabel || kit?.displayName || kit?.label || kit?.name || "";
  }

  function kitItemDisplayLabel(item) {
    return item?.displayLabel || item?.displayName || item?.label || item?.name || "";
  }

  function compareKitItemAlpha(a, b) {
    return compareText(kitItemDisplayLabel(a), kitItemDisplayLabel(b));
  }

  function compareText(a, b) {
    return String(a || "").localeCompare(String(b || ""), undefined, { sensitivity: "base", numeric: true });
  }

  function compareNumber(a, b) {
    return (Number(a) || 0) - (Number(b) || 0);
  }

  function filteredKitItems(kitId) {
    const items = kitItems(kitId);
    const query = ui.itemSearch.trim().toLowerCase();
    const filter = ui.itemFilter || "all";
    return items.filter((item) => {
      if (filter !== "all" && kitItemState(item.id, selectedHorse()?.id, kitId) !== filter) return false;
      if (!query) return true;
      return [
        kitItemDisplayLabel(item),
        item.name,
        item.uom,
        item.notes
      ].join(" ").toLowerCase().includes(query);
    });
  }

  function itemQuantityLabel(item) {
    const qty = Number(item.manualQuantity || 1);
    const uom = item.uom || "";
    return `${qty || 1} ${uom}`.trim();
  }

  function itemStateKey(itemId, horseId, kitId) {
    return `${horseId || ""}:${kitId || ""}:${itemId || ""}`;
  }

  function restoreSearchFocus(caret) {
    const search = root.querySelector("[data-search]");
    if (!search) return;
    search.focus();
    const position = Math.min(caret, search.value.length);
    if (typeof search.setSelectionRange === "function") {
      search.setSelectionRange(position, position);
    }
  }

  function restoreKitItemSearchFocus(caret) {
    const search = root.querySelector("[data-kit-item-search]");
    if (!search) return;
    search.focus();
    const position = Math.min(caret, search.value.length);
    if (typeof search.setSelectionRange === "function") {
      search.setSelectionRange(position, position);
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }
})();
