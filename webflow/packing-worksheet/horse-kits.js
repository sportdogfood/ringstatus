(function () {
  const root = document.getElementById("horse-kits") || document.getElementById("horse-kit-static-proof");
  if (!root) return;

  const config = {
    apiUrl: root.dataset.apiUrl || "/wec-packing/horse-kits",
    packWaveKey: root.dataset.packWaveKey || "wave_one"
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
    addItemName: "",
    addItemQty: "1",
    commentText: "",
    commentShortId: "",
    editingCommentId: ""
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

    if (action === "set-item-state") {
      await setItemState(target);
      return;
    }

    if (action === "set-item-filter") {
      ui.itemFilter = target.dataset.itemFilter || "all";
      render();
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
      ui.selectedHorseId = ui.selectedHorseId || records[0]?.id || "";
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
    return visibleHorses().map((horse) => {
      const kit = assignedKit(horse.id);
      const counts = rollup(horse.id, kit?.id);
      return {
        id: horse.id,
        horse,
        kit,
        counts
      };
    });
  }

  function visibleHorses() {
    const sourceHorses = state?.horses?.length ? state.horses : state?.allHorses || [];
    const horses = sourceHorses
      .filter((horse) => state?.horses?.length || horse.waveState === "wave_one" || horse.waveOne || !horse.notGoing)
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
    const items = (state?.kitItems || [])
      .filter((item) => item.status !== "inactive" && item.active !== false)
      .filter((item) => !kitId || (item.kitIds || []).includes(kitId))
      .sort((a, b) => Number(a.sortOrder || 0) - Number(b.sortOrder || 0) || String(kitItemDisplayLabel(a)).localeCompare(String(kitItemDisplayLabel(b))));
    return items.length ? items : (state?.kitItems || []).filter((item) => item.status !== "inactive");
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
    return (state?.horses || state?.allHorses || []).find((horse) => horse.id === ui.selectedHorseId) || records[0]?.horse || null;
  }

  function packWaveId() {
    return state?.source?.packWaveId || state?.wave?.id || "";
  }

  function sourceLine() {
    const counts = state?.counts || {};
    return `${counts.visibleHorses || records.length || 0} horses | ${counts.kits || 0} kits | ${counts.kitItems || 0} kit items | ${counts.packingRows || 0} touched rows`;
  }

  function render() {
    if (ui.loading) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty">Loading horse kits...</div></div>`;
      return;
    }

    if (ui.error && !state) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></div>`;
      return;
    }

    if (!records.length && ui.error) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></div>`;
      return;
    }

    const selected = selectedHorse();
    const kit = assignedKit(selected?.id);
    const statusText = !records.length ? "No horses found" : (ui.error || ui.message || sourceLine());
    root.innerHTML = `
      <div class="rs-airtable-shell">
        <div class="rs-airtable-toolbar">
          <div class="rs-search-wrap">
            <input class="rs-search" type="search" data-search placeholder="Search horses" value="${escapeAttr(ui.search)}">
            <button class="rs-search-clear ${ui.search ? "is-active" : ""}" type="button" aria-label="Clear search" data-action="clear-search">&times;</button>
          </div>
          <button class="rs-plain-button" type="button" data-action="reload">Refresh</button>
        </div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup>
              <col class="rs-col-gutter">
              <col class="rs-col-entities">
              <col class="rs-col-count">
              <col class="rs-col-count">
              <col class="rs-col-count">
            </colgroup>
            <thead>
              <tr>
                <th class="rs-row-gutter">#</th>
                <th>Horse / Kit</th>
                <th>Need</th>
                <th>Packed</th>
                <th>Left</th>
              </tr>
            </thead>
            <tbody>
              ${records.map(recordRowHtml).join("")}
            </tbody>
          </table>
        </div>
        <div class="rs-status ${ui.error ? "is-error" : ""}">${escapeHtml(statusText)}</div>

        <div class="rs-drawer-overlay ${ui.drawerOpen ? "is-open" : ""}" data-action="close-drawer"></div>
        <div class="rs-record-drawer ${ui.drawerOpen ? "is-open" : ""}" aria-label="Horse kit details">
          <div class="rs-drawer-head">
            <div class="rs-drawer-title">${escapeHtml(horseLabel(selected) || "Horse Kit")}</div>
            <button class="rs-drawer-close" type="button" aria-label="Close" data-action="close-drawer"><span aria-hidden="true">&times;</span></button>
          </div>
          <div class="rs-drawer-body">
            ${selected ? detailHtml(selected, kit) : ""}
          </div>
        </div>
      </div>
    `;
  }

  function recordRowHtml(record, index) {
    const selected = ui.drawerOpen && record.id === ui.selectedHorseId;
    return `
      <tr class="${selected ? "is-selected" : ""}" data-action="open-horse" data-horse-id="${record.id}" tabindex="0">
        <td class="rs-row-gutter">${index + 1}</td>
        <td class="rs-entity-cell">
          <div class="rs-entity-main">
            <span class="rs-entity-horse">${escapeHtml(horseLabel(record.horse))}</span>
            <span class="rs-open-text">Open</span>
          </div>
          <div class="rs-entity-sub">${escapeHtml(kitDisplayLabel(record.kit))}</div>
        </td>
        <td class="rs-cell-number">${escapeHtml(record.counts.needed)}</td>
        <td class="rs-cell-number">${escapeHtml(record.counts.packed)}</td>
        <td class="rs-cell-number">${escapeHtml(record.counts.left)}</td>
      </tr>
    `;
  }

  function detailHtml(horse, kit) {
    const counts = rollup(horse.id, kit?.id);
    const items = filteredKitItems(kit?.id);
    const percentPacked = counts.needed > 0 ? Math.round((counts.packed / counts.needed) * 100) : 0;
    return `
      <div class="rs-detail-summary">
        <div>
          <div class="rs-field-label">Kit</div>
          <div class="rs-field-value">${escapeHtml(kitDisplayLabel(kit) || "No kit")}</div>
        </div>
        <div class="rs-summary-metrics">
          ${metricHtml("Need", counts.needed)}
          ${metricHtml("Packed", counts.packed)}
          ${metricHtml("Left", counts.left)}
        </div>
        <div class="rs-kit-progress" aria-label="${escapeAttr(percentPacked)}% packed">
          <div class="rs-kit-progress-label">${escapeHtml(percentPacked)}% PACKED</div>
          <div class="rs-kit-progress-track">
            <div class="rs-kit-progress-bar" style="width: ${escapeAttr(Math.min(100, Math.max(0, percentPacked)))}%"></div>
          </div>
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
        <label class="rs-add-label" for="rs-kit-item-search">search_items</label>
        <div class="rs-search-wrap">
          <input id="rs-kit-item-search" class="rs-kit-item-search" data-kit-item-search value="${escapeAttr(ui.itemSearch)}" placeholder="Search kit items">
          <button class="rs-search-clear ${ui.itemSearch ? "is-active" : ""}" type="button" aria-label="Clear kit item search" data-action="clear-kit-item-search">&times;</button>
        </div>
        <div class="rs-item-filter-row" role="group" aria-label="Kit item filters">
          ${itemFilterButton("All", "all")}
          ${itemFilterButton("Not Packed", "not_packed")}
          ${itemFilterButton("Packed", "packed")}
          ${itemFilterButton("Not Needed", "not_needed")}
        </div>
      </div>
      <div class="rs-kit-items">
        ${items.map((item) => kitItemRowHtml(item, horse, kit)).join("") || `<div class="rs-empty-row">No kit items.</div>`}
      </div>
      ${commentsHtml(horse)}
      <div class="rs-drawer-bottom">
        <div class="rs-bottom-field">
          <div class="rs-field-label">Plan:</div>
          <div class="rs-field-value">Horse Specific</div>
        </div>
        <div class="rs-bottom-field">
          <div class="rs-field-label">System:</div>
          <div class="rs-field-value">Changes save to Airtable through Webflow Cloud.</div>
        </div>
      </div>
    `;
  }

  function metricHtml(label, value) {
    const key = slug(label);
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
          <div class="rs-field-label">comments</div>
        </div>
        <div class="rs-comment-form">
          <select class="rs-comment-short" data-comment-short>
            <option value="">comment short</option>
            ${shorts.map((row) => `<option value="${escapeAttr(row.id)}" ${ui.commentShortId === row.id ? "selected" : ""}>${escapeHtml(row.label)}</option>`).join("")}
          </select>
          <textarea class="rs-comment-input" data-comment-text placeholder="Comment">${escapeHtml(ui.commentText)}</textarea>
          <button class="rs-plain-button is-primary" type="button" data-action="save-comment" ${ui.savingKey === "comment" ? "disabled" : ""}>Save</button>
        </div>
        <div class="rs-comment-list">
          ${comments.map(commentRowHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}
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
      .sort((a, b) => String(b.createdTime || "").localeCompare(String(a.createdTime || "")));
  }

  function commentShorts() {
    return (state?.commentShorts || []).filter((row) => row.active !== false && row.status !== "inactive");
  }

  function horseLabel(horse) {
    return horse?.name || horse?.barnName || horse?.showName || "";
  }

  function kitDisplayLabel(kit) {
    return kit?.displayLabel || kit?.displayName || kit?.label || kit?.name || "";
  }

  function kitItemDisplayLabel(item) {
    return item?.displayLabel || item?.displayName || item?.label || item?.name || "";
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
