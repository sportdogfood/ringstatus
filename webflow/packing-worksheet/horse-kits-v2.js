(function () {
  const root = document.getElementById("horse-kits") || document.getElementById("horse-kit-static-proof");
  if (!root) return;

  const globalConfig = window.WEC_HORSE_KITS_CONFIG || {};
  const config = {
    apiUrl: root.dataset.apiUrl || globalConfig.apiUrl || "/wec-packing/horse-kits",
    printUrl: root.dataset.printUrl || globalConfig.printUrl || "",
    packWaveKey: root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one"
  };

  const ui = {
    loading: true,
    error: "",
    search: "",
    itemSearch: "",
    itemFilter: "all",
    laneKey: "open",
    secondaryView: config.packWaveKey || "wave_one",
    selectedHorseId: "",
    drawerOpen: false,
    sortKey: "horse",
    sortDir: "asc",
    savingKey: "",
    commentText: "",
    commentShortId: "",
    commentsOpen: true
  };

  let state = null;
  let records = [];
  const optimistic = new Map();

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
      rebuild();
      return;
    }
    if (action === "clear-kit-item-search") {
      ui.itemSearch = "";
      render();
      return;
    }
    if (action === "set-secondary-view") {
      ui.secondaryView = target.dataset.secondaryView || "all";
      config.packWaveKey = ui.secondaryView;
      await load();
      return;
    }
    if (action === "set-lane") {
      const key = target.dataset.laneKey || "open";
      if (key === "print") {
        openPrint();
        return;
      }
      ui.laneKey = key;
      rebuild();
      return;
    }
    if (action === "set-sort") {
      setSort(target.dataset.sortKey || "horse");
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
    if (action === "save-comment") {
      await saveComment();
      return;
    }
    if (action === "print-list") {
      openPrint();
    }
  });

  root.addEventListener("input", (event) => {
    const input = event.target;
    if (input.matches("[data-search]")) {
      ui.search = input.value || "";
      const caret = input.selectionStart || ui.search.length;
      rebuild();
      requestAnimationFrame(() => {
        const next = root.querySelector("[data-search]");
        if (next) {
          next.focus();
          next.setSelectionRange(caret, caret);
        }
      });
    }
    if (input.matches("[data-kit-item-search]")) {
      ui.itemSearch = input.value || "";
      const caret = input.selectionStart || ui.itemSearch.length;
      render();
      requestAnimationFrame(() => {
        const next = root.querySelector("[data-kit-item-search]");
        if (next) {
          next.focus();
          next.setSelectionRange(caret, caret);
        }
      });
    }
    if (input.matches("[data-comment-text]")) ui.commentText = input.value || "";
    if (input.matches("[data-comment-short]")) {
      ui.commentShortId = input.value || "";
      const selected = commentShorts().find((row) => row.id === ui.commentShortId);
      if (selected && !ui.commentText.trim()) {
        ui.commentText = selected.label || selected.comment || "";
        render();
      }
    }
  });

  async function load() {
    ui.loading = true;
    ui.error = "";
    render();
    try {
      state = await fetchJson(apiUrl());
      rebuild(false);
    } catch (error) {
      ui.error = error.message || String(error);
      render();
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
    url.searchParams.set("v", "2");
    return url.toString();
  }

  function rebuild(shouldRender = true) {
    records = sortRows(filteredHorses().map(recordForHorse).filter(matchesLane));
    if (!records.some((row) => row.id === ui.selectedHorseId)) ui.selectedHorseId = records[0]?.id || "";
    if (shouldRender) render();
  }

  function filteredHorses() {
    const horses = state?.horses || [];
    const byView = horses.filter((horse) => {
      const key = ui.secondaryView || "all";
      if (key === "all") return true;
      if (key === "wave_one") return horse.waveState === "wave_one" || horse.waveOne === true;
      if (key === "wave_two") return horse.waveState === "wave_two" || horse.waveTwo === true;
      if (key === "not_going") return horse.waveState === "not_going" || horse.notGoing === true;
      return horse.waveState === key || horse[key] === true;
    });
    const query = ui.search.trim().toLowerCase();
    if (!query) return byView;
    return byView.filter((horse) => [
      horse.name,
      horse.barnName,
      horse.showName,
      horse.display_horse_barn_name
    ].join(" ").toLowerCase().includes(query));
  }

  function recordForHorse(horse) {
    const items = assignedItems(horse);
    const kit = assignedKitFromItems(items);
    const counts = countsFor(horse, kit, items);
    return { id: horse.id, horse, kit, items, counts };
  }

  function assignedItems(horse) {
    const ids = unique([...(horse?.pakKitItemIds || []), ...(horse?.pak_kit_item_ids || [])]);
    if (!ids.length) return [];
    const itemsById = new Map((state?.kitItems || []).map((item) => [item.id, item]));
    return ids
      .map((id) => itemsById.get(id))
      .filter((item) => item && item.active !== false && item.status !== "inactive")
      .sort(compareItem);
  }

  function assignedKitFromItems(items) {
    if (!items.length) return null;
    const itemIds = new Set(items.map((item) => item.id));
    return (state?.kits || [])
      .filter((kit) => kit.active !== false && kit.status !== "inactive")
      .find((kit) => (kit.kitItemIds || []).some((id) => itemIds.has(id))) || null;
  }

  function countsFor(horse, kit, items) {
    if (!kit || !items.length) return { total: 0, needed: 0, packed: 0, notNeeded: 0, left: 0 };
    const counts = { total: items.length, needed: items.length, packed: 0, notNeeded: 0, left: items.length };
    for (const item of items) {
      const value = itemState(horse.id, kit.id, item.id);
      if (value === "packed") counts.packed += 1;
      if (value === "not_needed") counts.notNeeded += 1;
    }
    counts.needed = Math.max(0, counts.total - counts.notNeeded);
    counts.left = Math.max(0, counts.needed - counts.packed);
    return counts;
  }

  function itemState(horseId, kitId, itemId) {
    const key = stateKey(horseId, kitId, itemId);
    if (optimistic.has(key)) return optimistic.get(key);
    const row = packingRow(horseId, kitId, itemId);
    if (!row) return "not_packed";
    if (row.neededState === "not_needed" || row.packState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function packingRow(horseId, kitId, itemId) {
    return (state?.packingRows || []).find((row) =>
      (row.horseIds || []).includes(horseId) &&
      (row.kitIds || []).includes(kitId) &&
      (row.kitItemIds || []).includes(itemId)
    ) || null;
  }

  function matchesLane(record) {
    const key = ui.laneKey || "open";
    if (key === "all") return true;
    if (key === "open" || key === "left") return record.counts.left > 0;
    if (key === "need" || key === "needed") return record.counts.needed > 0;
    if (key === "packed") return record.counts.packed > 0;
    if (key === "not_needed") return record.counts.notNeeded > 0;
    return true;
  }

  function setSort(key) {
    if (ui.sortKey === key) ui.sortDir = ui.sortDir === "asc" ? "desc" : "asc";
    else {
      ui.sortKey = key;
      ui.sortDir = "asc";
    }
    records = sortRows(records);
    render();
  }

  function sortRows(rows) {
    const dir = ui.sortDir === "desc" ? -1 : 1;
    return [...rows].sort((a, b) => {
      if (ui.sortKey === "kit") return compareText(kitLabel(a.kit), kitLabel(b.kit)) * dir;
      if (ui.sortKey === "need") return compareNumber(a.counts.needed, b.counts.needed) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (ui.sortKey === "packed") return compareNumber(a.counts.packed, b.counts.packed) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (ui.sortKey === "left") return compareNumber(a.counts.left, b.counts.left) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      return compareText(horseLabel(a.horse), horseLabel(b.horse)) * dir;
    });
  }

  async function setItemState(button) {
    const horse = selectedHorse();
    const kit = selectedRecord()?.kit;
    const itemId = button.dataset.kitItemId || "";
    const nextState = button.dataset.packState || "not_packed";
    if (!horse?.id || !kit?.id || !itemId) return;
    if (!selectedRecord().items.some((item) => item.id === itemId)) return;
    const key = stateKey(horse.id, kit.id, itemId);
    const previous = itemState(horse.id, kit.id, itemId);
    optimistic.set(key, nextState);
    rebuild();
    ui.drawerOpen = true;
    ui.selectedHorseId = horse.id;
    ui.savingKey = key;
    render();
    try {
      const row = packingRow(horse.id, kit.id, itemId);
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "set_packing_kit_state",
          packingKitId: row?.id || "",
          horseId: horse.id,
          kitId: kit.id,
          kitItemId: itemId,
          packWaveId: state?.source?.packWaveId || "",
          packState: nextState
        })
      });
      state = result.state || state;
    } catch (error) {
      optimistic.set(key, previous);
      ui.error = error.message || String(error);
    } finally {
      ui.savingKey = "";
      rebuild();
      ui.drawerOpen = true;
      ui.selectedHorseId = horse.id;
    }
  }

  async function saveComment() {
    const horse = selectedHorse();
    const comment = ui.commentText.trim();
    if (!horse?.id || !comment) return;
    ui.savingKey = "comment";
    render();
    try {
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "save_comment",
          horseId: horse.id,
          scopeLabel: horseLabel(horse),
          packWaveId: state?.source?.packWaveId || "",
          commentShortId: ui.commentShortId,
          comment
        })
      });
      state = result.state || state;
      ui.commentText = "";
      ui.commentShortId = "";
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.savingKey = "";
      rebuild();
      ui.drawerOpen = true;
      ui.selectedHorseId = horse.id;
    }
  }

  function render() {
    if (ui.loading && !state) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-stack-section"><div class="rs-stack-label">Loading</div></div></div>`;
      return;
    }
    const rows = activeRows();
    root.innerHTML = `
      <div class="rs-airtable-shell">
        <div class="rs-page-stack">
          ${rows.map(stackRow).join("")}
        </div>
        ${drawerHtml()}
      </div>
    `;
  }

  function activeRows() {
    const rows = state?.groupStack?.activeRows || [];
    const allowed = new Set(["header", "primary_tabs", "summary_aggs", "secondary_controls", "count_aggs", "lane_controls", "search", "main_table", "comments"]);
    return rows.filter((row) => allowed.has(row.renderKey));
  }

  function stackRow(row) {
    const key = row.renderKey;
    if (key === "header") return section(row, headerHtml(), "is-header");
    if (key === "primary_tabs") return section(row, pillsHtml(state?.primaryTabs || [], "horses", "primary"), "is-primary-tabs");
    if (key === "lane_controls") return section(row, pillsHtml(laneControls(), ui.laneKey, "lane"), "is-lane-controls");
    if (key === "secondary_controls") return section(row, pillsHtml(state?.secondaryControls || [], ui.secondaryView, "secondary"), "is-secondary-controls");
    if (key === "summary_aggs") return section(row, `<div class="rs-stack-label">${escapeHtml(row.displayLabel || "Horse Kits")}</div><div class="rs-stack-aggs">${summaryAggs()}</div>`, "is-summary-aggs");
    if (key === "count_aggs") return section(row, `<div class="rs-secondary-count-aggs"><div class="rs-stack-aggs is-counts">${countAggs()}</div></div>`, "is-count-aggs");
    if (key === "search") return section(row, searchHtml(), "is-search");
    if (key === "main_table") return section(row, tableHtml(row), "is-main-table");
    if (key === "comments") return section(row, commentsPageHtml(row), "is-comments");
    return "";
  }

  function section(row, html, className) {
    return `<section class="rs-stack-section ${className || ""}" data-render-key="${escapeAttr(row.renderKey || "")}">${html}</section>`;
  }

  function headerHtml() {
    const wave = state?.wave || {};
    return `<div class="rs-page-header"><div class="rs-page-title">${escapeHtml(wave.reportTitle || "WEC PACK")}</div><div class="rs-page-subtitle">${escapeHtml(wave.reportSubtitle || "")}</div></div>`;
  }

  function pillsHtml(rows, activeKey, type) {
    return `<div class="rs-stack-tabs ${type === "secondary" ? "is-compact" : ""}">${rows.map((row) => {
      const key = row.key || row.renderKey || row.id || "";
      const action = type === "lane" ? "set-lane" : type === "secondary" ? "set-secondary-view" : "";
      const data = type === "lane" ? `data-action="set-lane" data-lane-key="${escapeAttr(key)}"` : type === "secondary" ? `data-action="set-secondary-view" data-secondary-view="${escapeAttr(key)}"` : "";
      return `<button class="rs-stack-pill ${activeKey === key || (!activeKey && row.active) ? "is-active" : ""}" type="button" ${data}>${escapeHtml(row.label || key)}</button>`;
    }).join("")}</div>`;
  }

  function laneControls() {
    return (state?.laneControls || []).filter((row) => row.active !== false);
  }

  function summaryAggs() {
    const horses = records.length;
    const itemIds = new Set(records.flatMap((record) => record.items.map((item) => item.id)));
    const touched = records.reduce((sum, record) => sum + record.items.filter((item) => packingRow(record.horse.id, record.kit?.id, item.id)).length, 0);
    return [
      agg(horses, "HORSES", "horses", "brown"),
      agg(itemIds.size, "KIT ITEMS", "kit_items", "green"),
      agg(touched, "TOUCHED", "touched", "grey")
    ].join("");
  }

  function countAggs() {
    const totals = records.reduce((sum, record) => {
      sum.needed += record.counts.needed;
      sum.packed += record.counts.packed;
      sum.left += record.counts.left;
      return sum;
    }, { needed: 0, packed: 0, left: 0 });
    return [
      agg(totals.needed, "NEED", "need", "brown"),
      agg(totals.packed, "PACKED", "packed", "green"),
      agg(totals.left, "LEFT", "left", "grey")
    ].join("");
  }

  function agg(value, label, key, shade) {
    return `<div class="rs-stack-agg is-${escapeAttr(key)} is-shade-${escapeAttr(shade)}"><div class="rs-stack-agg-value">${escapeHtml(value)}</div><div class="rs-stack-agg-label">${escapeHtml(label)}</div></div>`;
  }

  function searchHtml() {
    return `<div class="rs-airtable-toolbar"><div class="rs-search-wrap"><input class="rs-search" type="text" data-search autocomplete="off" placeholder="Search horses" value="${escapeAttr(ui.search)}"><button class="rs-search-clear ${ui.search ? "is-active" : ""}" type="button" aria-label="Clear search" data-action="clear-search"><span aria-hidden="true">&times;</span></button></div></div>`;
  }

  function tableHtml(row) {
    return `
      <div class="rs-table-stack-head"><div class="rs-stack-label">${escapeHtml(row.displayLabel || "Horses")}</div><button class="rs-stack-pill" type="button" data-action="print-list">Print</button></div>
      <div class="rs-airtable-scroll">
        <table class="rs-airtable-grid">
          <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-entity"><col class="rs-col-count"><col class="rs-col-count"><col class="rs-col-count"></colgroup>
          <thead><tr><th class="rs-row-gutter">#</th>${head("HORSE", "horse")}${head("KIT", "kit")}${head("NEED", "need")}${head("PACKED", "packed")}${head("LEFT", "left")}</tr></thead>
          <tbody>${records.map(rowHtml).join("")}</tbody>
        </table>
      </div>
      ${ui.error ? `<div class="rs-status is-error">${escapeHtml(ui.error)}</div>` : ""}
    `;
  }

  function head(label, key) {
    return `<th><button class="rs-sort-head ${ui.sortKey === key ? "is-active" : ""}" type="button" data-action="set-sort" data-sort-key="${escapeAttr(key)}"><span>${escapeHtml(label)}</span></button></th>`;
  }

  function rowHtml(record, index) {
    return `<tr class="${record.id === ui.selectedHorseId && ui.drawerOpen ? "is-selected" : ""}" data-action="open-horse" data-horse-id="${escapeAttr(record.id)}" tabindex="0">
      <td class="rs-row-gutter">${index + 1}</td>
      <td class="rs-entity-cell rs-entity-horse-cell"><div class="rs-entity-main"><span class="rs-entity-horse">${escapeHtml(horseLabel(record.horse))}</span><span class="rs-open-text">Open</span></div></td>
      <td class="rs-entity-cell rs-entity-kit-cell"><span class="rs-entity-sub">${escapeHtml(kitLabel(record.kit))}</span></td>
      <td class="rs-cell-number">${record.counts.needed}</td>
      <td class="rs-cell-number">${record.counts.packed}</td>
      <td class="rs-cell-number">${record.counts.left}</td>
    </tr>`;
  }

  function drawerHtml() {
    const record = selectedRecord();
    if (!record) return "";
    const horse = record.horse;
    const kit = record.kit;
    const percent = record.counts.needed ? Math.round((record.counts.packed / record.counts.needed) * 100) : 0;
    return `<aside class="rs-record-drawer ${ui.drawerOpen ? "is-open" : ""}" aria-hidden="${ui.drawerOpen ? "false" : "true"}">
      <div class="rs-drawer-head">
        <div class="rs-drawer-title-group"><div class="rs-page-subtitle">${escapeHtml(horseLabel(horse))}</div>${horse.profileUrl ? `<a class="rs-profile-link" href="${escapeAttr(horse.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>` : ""}</div>
        <button class="rs-drawer-close" type="button" data-action="close-drawer" aria-label="Close"><span aria-hidden="true">&times;</span></button>
      </div>
      <div class="rs-drawer-body">
        <div class="rs-detail-summary">
          <div class="rs-kit-progress"><div class="rs-kit-progress-label">${percent}% PACKED</div><div class="rs-kit-progress-track"><div class="rs-kit-progress-bar" style="width:${percent}%"></div></div></div>
          <div class="rs-summary-metrics">${agg(record.counts.needed, "NEED", "need", "brown")}${agg(record.counts.packed, "PACKED", "packed", "green")}${agg(record.counts.left, "LEFT", "left", "grey")}</div>
        </div>
        ${drawerComments(horse)}
        <div class="rs-kit-item-search-row"><label class="rs-stack-label" for="rs-kit-search-v2">SEARCH KIT ITEMS</label><div class="rs-search-wrap"><input id="rs-kit-search-v2" class="rs-kit-item-search" data-kit-item-search autocomplete="off" value="${escapeAttr(ui.itemSearch)}" placeholder="Search kit items"><button class="rs-search-clear ${ui.itemSearch ? "is-active" : ""}" type="button" aria-label="Clear kit item search" data-action="clear-kit-item-search"><span aria-hidden="true">&times;</span></button></div></div>
        <div class="rs-kit-item-row rs-item-filter-row"><div class="rs-kit-item-main"><div class="rs-stack-label">FILTER KIT ITEMS</div></div><div class="rs-kit-actions rs-item-filter-actions">${filterButton("All", "all")}${filterButton("Not Packed", "not_packed")}${filterButton("Packed", "packed")}${filterButton("Not Needed", "not_needed")}</div></div>
        <div class="rs-kit-items"><div class="rs-stack-label">KIT ITEMS</div>${filteredItems(record).map((item) => itemRow(item, horse, kit)).join("") || `<div class="rs-empty-row">No kit items.</div>`}</div>
      </div>
    </aside>`;
  }

  function drawerComments(horse) {
    return `<div class="rs-comments"><div class="rs-stack-label">COMMENTS</div><div class="rs-comment-form"><select class="rs-comment-short" data-comment-short><option value="">Comment short</option>${commentShorts().map((row) => `<option value="${escapeAttr(row.id)}" ${ui.commentShortId === row.id ? "selected" : ""}>${escapeHtml(row.label || row.comment || row.id)}</option>`).join("")}</select><textarea class="rs-comment-input" data-comment-text rows="3" placeholder="Add comment">${escapeHtml(ui.commentText)}</textarea><button class="rs-plain-button is-primary" type="button" data-action="save-comment">${ui.savingKey === "comment" ? "Saving" : "Save"}</button></div><div class="rs-comment-list">${horseComments(horse.id).map(commentHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}</div></div>`;
  }

  function filteredItems(record) {
    const query = ui.itemSearch.trim().toLowerCase();
    return record.items.filter((item) => {
      const value = itemState(record.horse.id, record.kit?.id, item.id);
      if (ui.itemFilter !== "all" && value !== ui.itemFilter) return false;
      if (!query) return true;
      return itemLabel(item).toLowerCase().includes(query);
    });
  }

  function filterButton(label, key) {
    return `<button class="rs-item-filter ${ui.itemFilter === key ? "is-active" : ""}" type="button" data-action="set-item-filter" data-item-filter="${escapeAttr(key)}">${escapeHtml(label)}</button>`;
  }

  function itemRow(item, horse, kit) {
    const value = itemState(horse.id, kit?.id, item.id);
    const key = stateKey(horse.id, kit?.id, item.id);
    return `<div class="rs-kit-item-row"><div class="rs-kit-item-main"><div class="rs-kit-item-title rs-stack-label">${escapeHtml(itemLabel(item))}</div></div><div class="rs-kit-actions">${stateButton("Not Packed", "not_packed", value, item, horse, kit, key)}${stateButton("Packed", "packed", value, item, horse, kit, key)}${stateButton("Not Needed", "not_needed", value, item, horse, kit, key)}</div></div>`;
  }

  function stateButton(label, value, active, item, horse, kit, key) {
    return `<button class="rs-state-button ${active === value ? "is-active" : ""} ${ui.savingKey === key ? "is-saving" : ""}" type="button" data-action="set-item-state" data-horse-id="${escapeAttr(horse.id)}" data-kit-id="${escapeAttr(kit?.id || "")}" data-kit-item-id="${escapeAttr(item.id)}" data-pack-state="${escapeAttr(value)}">${escapeHtml(label)}</button>`;
  }

  function commentsPageHtml(row) {
    const comments = state?.comments || [];
    return `<div class="rs-comments is-page-comments"><div class="rs-stack-label">${escapeHtml(row.displayLabel || "Comments")}</div><div class="rs-comment-list">${comments.map(commentHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}</div></div>`;
  }

  function commentHtml(comment) {
    return `<div class="rs-comment-row"><div class="rs-comment-body">${escapeHtml(comment.comment || comment.label || "")}</div><div class="rs-comment-meta">${escapeHtml(comment.scopeLabel || comment.horseName || "")}</div></div>`;
  }

  function selectedRecord() {
    return records.find((record) => record.id === ui.selectedHorseId) || records[0] || null;
  }

  function selectedHorse() {
    return selectedRecord()?.horse || null;
  }

  function horseComments(horseId) {
    return (state?.comments || []).filter((comment) => (comment.horseIds || []).includes(horseId));
  }

  function commentShorts() {
    return (state?.commentShorts || []).filter((row) => row.active !== false);
  }

  function openPrint() {
    const url = config.printUrl || `${config.apiUrl}/print`;
    window.open(url, "_blank", "noopener");
  }

  function stateKey(horseId, kitId, itemId) {
    return `${horseId}:${kitId}:${itemId}`;
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

  function unique(values) {
    return [...new Set((values || []).map((value) => String(value || "").trim()).filter(Boolean))];
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }
})();
