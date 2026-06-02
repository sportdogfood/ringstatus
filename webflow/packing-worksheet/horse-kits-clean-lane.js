(function () {
  const root = document.getElementById("horse-kit-lane");
  if (!root) return;

  const config = {
    apiUrl: root.dataset.apiUrl || "/wec-packing/horse-kits",
    packWaveKey: root.dataset.packWaveKey || "wave_one"
  };

  const ui = {
    loading: false,
    status: "",
    error: "",
    horseFilter: "wave_one",
    selectedHorseId: "",
    selectedKitId: "",
    search: ""
  };

  let state = null;
  let searchRenderTimer = 0;

  load();

  root.addEventListener("click", (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    event.preventDefault();
    const action = target.dataset.action;
    if (action === "filter-horses") {
      ui.horseFilter = target.dataset.filter || "wave_one";
      render();
      return;
    }
    if (action === "select-horse") {
      ui.selectedHorseId = target.dataset.horseId || "";
      render();
      return;
    }
    if (action === "select-kit") {
      ui.selectedKitId = target.dataset.kitId || "";
      render();
      return;
    }
    if (action === "reload") {
      load();
      return;
    }
    if (action === "set-wave") {
      runAction({
        action: "set_horse_wave",
        horseId: currentHorse()?.id,
        waveState: target.dataset.waveState
      });
      return;
    }
    if (action === "create-kit") {
      const label = valueByName("kit_label");
      runAction({ action: "create_kit", label });
      return;
    }
    if (action === "create-kit-item") {
      runAction({
        action: "create_kit_item",
        kitId: ui.selectedKitId,
        label: valueByName("kit_item_label"),
        quantity: valueByName("kit_item_quantity") || 1,
        uom: valueByName("kit_item_uom")
      });
      return;
    }
    if (action === "move-kit-item") {
      const row = target.closest("[data-kit-item-id]");
      const select = row?.querySelector("select[name='move_kit_id']");
      runAction({
        action: "move_kit_item",
        kitItemId: row?.dataset.kitItemId,
        kitId: select?.value || ""
      });
      return;
    }
    if (action === "add-missing-rows") {
      runAction({
        action: "add_missing_packing_rows",
        horseId: currentHorse()?.id,
        kitId: ui.selectedKitId,
        packWaveId: state?.wave?.id
      });
      return;
    }
    if (action === "set-pack-state") {
      runAction({
        action: "set_packing_kit_state",
        packingKitId: target.dataset.rowId,
        packState: target.dataset.packState
      });
      return;
    }
    if (action === "set-needed-state") {
      runAction({
        action: "set_packing_kit_needed",
        packingKitId: target.dataset.rowId,
        neededState: target.dataset.neededState
      });
      return;
    }
    if (action === "save-quantity") {
      const row = target.closest("[data-packing-row-id]");
      runAction({
        action: "update_packing_kit_quantity",
        packingKitId: row?.dataset.packingRowId,
        quantityNeeded: row?.querySelector("input[name='quantity_needed']")?.value
      });
    }
  });

  root.addEventListener("input", (event) => {
    if (event.target.matches("[data-search]")) {
      ui.search = event.target.value || "";
      window.clearTimeout(searchRenderTimer);
      searchRenderTimer = window.setTimeout(() => render({ focusSearch: true }), 160);
    }
  });

  async function load() {
    setBusy("Loading lane...");
    try {
      const data = await fetchJson(`${config.apiUrl}?packWaveKey=${encodeURIComponent(config.packWaveKey)}`);
      state = data;
      retainSelection();
      ui.error = "";
      ui.status = `Loaded ${data.counts?.visibleHorses || 0} wave horses, ${data.counts?.kits || 0} kits, ${data.counts?.kitItems || 0} kit items, ${data.counts?.packingRows || 0} packing rows.`;
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.loading = false;
      render();
    }
  }

  async function runAction(payload) {
    if (!payload || !payload.action) return;
    setBusy(`Saving ${payload.action}...`);
    try {
      const data = await fetchJson(`${config.apiUrl}?packWaveKey=${encodeURIComponent(config.packWaveKey)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state = data.state || data;
      applyActionResult(payload.action, data.result);
      retainSelection();
      ui.error = "";
      ui.status = `Saved ${payload.action}.`;
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.loading = false;
      render();
    }
  }

  async function fetchJson(url, options = {}) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) {
      throw new Error(data.detail || data.error || `${response.status} ${response.statusText}`);
    }
    return data;
  }

  function setBusy(message) {
    ui.loading = true;
    ui.status = message;
    ui.error = "";
    render();
  }

  function retainSelection() {
    const horses = filteredHorses();
    if (!horses.some((horse) => horse.id === ui.selectedHorseId)) {
      ui.selectedHorseId = horses[0]?.id || state?.horses?.[0]?.id || "";
    }
    if (!state?.kits?.some((kit) => kit.id === ui.selectedKitId)) {
      ui.selectedKitId = state?.kits?.[0]?.id || "";
    }
  }

  function applyActionResult(action, result) {
    if (action === "create_kit" && result?.created?.id) {
      ui.selectedKitId = result.created.id;
    }
  }

  function filteredHorses() {
    const source = state?.allHorses || state?.horses || [];
    const search = ui.search.trim().toLowerCase();
    return source.filter((horse) => {
      if (ui.horseFilter === "wave_one" && horse.waveState !== "wave_one") return false;
      if (ui.horseFilter === "wave_two" && horse.waveState !== "wave_two") return false;
      if (ui.horseFilter === "not_going" && horse.waveState !== "not_going") return false;
      if (!search) return true;
      return [horse.name, horse.barnName, horse.showName].join(" ").toLowerCase().includes(search);
    });
  }

  function currentHorse() {
    return (state?.allHorses || state?.horses || []).find((horse) => horse.id === ui.selectedHorseId) || null;
  }

  function selectedKit() {
    return (state?.kits || []).find((kit) => kit.id === ui.selectedKitId) || null;
  }

  function selectedKitItems() {
    return (state?.kitItems || []).filter((item) => item.kitIds.includes(ui.selectedKitId));
  }

  function selectedHorseRows() {
    const horseId = ui.selectedHorseId;
    return (state?.packingRows || []).filter((row) => row.horseIds.includes(horseId));
  }

  function valueByName(name) {
    return root.querySelector(`[name='${name}']`)?.value?.trim() || "";
  }

  function render(options = {}) {
    if (!state && ui.loading) {
      root.innerHTML = `<div class="hk-shell"><div class="hk-header">Loading horse kit lane...</div></div>`;
      return;
    }
    if (state) retainSelection();
    const horse = currentHorse();
    const kit = selectedKit();
    const horses = filteredHorses();
    root.innerHTML = `
      <div class="hk-shell">
        ${headerHtml()}
        <div class="hk-status ${ui.error ? "hk-error" : ""}">${escapeHtml(ui.error || ui.status || "")}</div>
        <div class="hk-grid">
          <section class="hk-panel">
            <div class="hk-panel-head">
              <div>
                <div class="hk-panel-title">Horses</div>
                <div class="hk-muted">${horses.length} shown</div>
              </div>
              <button class="hk-link" data-action="reload" type="button">Reload</button>
            </div>
            <div class="hk-form-row">
              <input data-search type="search" value="${escapeAttr(ui.search)}" placeholder="Search barn or show name">
              <span></span>
            </div>
            <div class="hk-tabs">
              ${horseFilterButton("all", "All")}
              ${horseFilterButton("wave_one", "Wave 1")}
              ${horseFilterButton("wave_two", "Wave 2")}
              ${horseFilterButton("not_going", "Not Going")}
            </div>
            ${horses.length ? horses.map(horseRowHtml).join("") : `<div class="hk-empty">No horses in this filter.</div>`}
          </section>

          <section class="hk-panel">
            <div class="hk-panel-head">
              <div>
                <div class="hk-panel-title">${escapeHtml(horse?.name || "Select Horse")}</div>
                <div class="hk-muted">${escapeHtml(horse?.showName || "")}</div>
              </div>
              <div class="hk-actions">
                ${horseWaveButton("wave_one", "Wave 1", horse)}
                ${horseWaveButton("wave_two", "Wave 2", horse)}
                ${horseWaveButton("not_going", "Not Going", horse)}
              </div>
            </div>
            ${kitControlsHtml(kit)}
            ${kitItemsHtml()}
            ${packingRowsHtml()}
            ${changesHtml()}
          </section>
        </div>
      </div>
    `;
    if (options.focusSearch) {
      const search = root.querySelector("[data-search]");
      if (search) {
        search.focus();
        const end = search.value.length;
        search.setSelectionRange?.(end, end);
      }
    }
  }

  function headerHtml() {
    const counts = state?.counts || {};
    return `
      <header class="hk-header">
        <h1 class="hk-title">Horse Kit Test Lane</h1>
        <div class="hk-subtitle">Wave One | actual kit tables only</div>
        <div class="hk-toolbar">
          <span class="hk-pill">${counts.visibleHorses || 0} wave horses</span>
          <span class="hk-pill">${counts.kits || 0} kits</span>
          <span class="hk-pill">${counts.kitItems || 0} kit items</span>
          <span class="hk-pill">${counts.packingRows || 0} packing rows</span>
        </div>
      </header>
    `;
  }

  function horseFilterButton(filter, label) {
    return `<button class="hk-tab ${ui.horseFilter === filter ? "is-active" : ""}" data-action="filter-horses" data-filter="${filter}" type="button">${label}</button>`;
  }

  function horseWaveButton(stateValue, label, horse) {
    const active = horse?.waveState === stateValue;
    return `<button class="hk-tab ${active ? "is-active" : ""}" data-action="set-wave" data-wave-state="${stateValue}" type="button" ${horse ? "" : "disabled"}>${label}</button>`;
  }

  function horseRowHtml(horse) {
    const summary = horse.kitSummary || {};
    return `
      <button class="hk-row hk-row-button ${horse.id === ui.selectedHorseId ? "is-selected" : ""}" data-action="select-horse" data-horse-id="${horse.id}" type="button">
        <div>
          <div class="hk-name">${escapeHtml(horse.name)}</div>
          <div class="hk-muted">${escapeHtml(horse.showName || horse.waveState)}</div>
        </div>
        <div class="hk-values">
          <span class="hk-pill">${summary.needed || 0} need</span>
          <span class="hk-pill">${summary.packed || 0} packed</span>
          <span class="hk-pill">${summary.left || 0} left</span>
        </div>
      </button>
    `;
  }

  function kitControlsHtml(kit) {
    return `
      <div class="hk-panel-head">
        <div>
          <div class="hk-panel-title">Kits</div>
          <div class="hk-muted">${escapeHtml(kit?.label || "No kit selected")}</div>
        </div>
        <div class="hk-actions">
          ${(state?.kits || []).map((item) => `
            <button class="hk-tab ${item.id === ui.selectedKitId ? "is-active" : ""}" data-action="select-kit" data-kit-id="${item.id}" type="button">${escapeHtml(item.label)}</button>
          `).join("")}
        </div>
      </div>
      <div class="hk-form-row">
        <input name="kit_label" type="text" placeholder="New kit name">
        <button class="hk-button" data-action="create-kit" type="button">Add Kit</button>
      </div>
      <div class="hk-form-row">
        <div>
          <div class="hk-label">Create rows for selected horse + selected kit</div>
          <div class="hk-muted">Adds only missing actual horse_packing_kits rows.</div>
        </div>
        <button class="hk-button is-primary" data-action="add-missing-rows" type="button" ${kit && currentHorse() ? "" : "disabled"}>Add Missing Rows</button>
      </div>
    `;
  }

  function kitItemsHtml() {
    const items = selectedKitItems();
    return `
      <div class="hk-panel-head">
        <div>
          <div class="hk-panel-title">Kit Items</div>
          <div class="hk-muted">${items.length} in selected kit</div>
        </div>
      </div>
      <div class="hk-form-row">
        <input name="kit_item_label" type="text" placeholder="New kit item">
        <div class="hk-actions">
          <input name="kit_item_quantity" type="number" min="1" value="1" aria-label="Quantity">
          <input name="kit_item_uom" type="text" placeholder="uom" aria-label="Unit">
          <button class="hk-button" data-action="create-kit-item" type="button" ${ui.selectedKitId ? "" : "disabled"}>Add Item</button>
        </div>
      </div>
      ${items.length ? items.map(kitItemRowHtml).join("") : `<div class="hk-empty">No kit items yet.</div>`}
    `;
  }

  function kitItemRowHtml(item) {
    return `
      <div class="hk-row" data-kit-item-id="${item.id}">
        <div>
          <div class="hk-name">${escapeHtml(item.label)}</div>
          <div class="hk-muted">${item.manualQuantity || 1} ${escapeHtml(item.uom || "")} | ${escapeHtml(item.inlineEdit)} | ${escapeHtml(item.status)}</div>
        </div>
        <div class="hk-actions">
          <select name="move_kit_id" aria-label="Move kit item">
            ${(state?.kits || []).map((kit) => `<option value="${kit.id}" ${item.kitIds.includes(kit.id) ? "selected" : ""}>${escapeHtml(kit.label)}</option>`).join("")}
          </select>
          <button class="hk-button" data-action="move-kit-item" type="button">Move</button>
        </div>
      </div>
    `;
  }

  function packingRowsHtml() {
    const rows = selectedHorseRows();
    const summary = horseKitSummaryFromRows(rows);
    return `
      <div class="hk-panel-head">
        <div>
          <div class="hk-panel-title">Packing Rows</div>
          <div class="hk-muted">${summary.needed} need | ${summary.packed} packed | ${summary.left} left</div>
        </div>
      </div>
      ${rows.length ? rows.map(packingRowHtml).join("") : `<div class="hk-empty">No actual horse_packing_kits rows for this horse yet.</div>`}
    `;
  }

  function horseKitSummaryFromRows(rows) {
    return rows.reduce((summary, row) => {
      summary.needed += Number(row.needed || 0);
      summary.packed += Number(row.packed || 0);
      summary.left += Number(row.left || 0);
      return summary;
    }, { needed: 0, packed: 0, left: 0 });
  }

  function packingRowHtml(row) {
    const isPacked = row.packState === "packed";
    const isNotNeeded = row.packState === "not_needed" || row.neededState === "not_needed";
    return `
      <div class="hk-row" data-packing-row-id="${row.id}">
        <div>
          <div class="hk-name">${escapeHtml(row.itemLabel || row.label)}</div>
          <div class="hk-muted">${escapeHtml(row.kitLabel)} | ${row.quantityNeeded || 0} need | ${row.quantityPacked || 0} packed</div>
        </div>
        <div class="hk-actions">
          <input name="quantity_needed" type="number" min="0" value="${row.quantityNeeded || 0}" aria-label="Quantity needed" ${row.inlineAllowed ? "" : "disabled"}>
          <button class="hk-button" data-action="save-quantity" type="button" ${row.inlineAllowed ? "" : "disabled"}>Qty</button>
          <button class="hk-button ${isPacked ? "hk-state-packed" : ""}" data-action="set-pack-state" data-row-id="${row.id}" data-pack-state="${isPacked ? "not_packed" : "packed"}" type="button">${isPacked ? "Unpack" : "Pack"}</button>
          <button class="hk-button ${isNotNeeded ? "hk-state-not-needed" : ""}" data-action="set-needed-state" data-row-id="${row.id}" data-needed-state="${isNotNeeded ? "needed" : "not_needed"}" type="button">${isNotNeeded ? "Needed" : "Not Needed"}</button>
        </div>
      </div>
    `;
  }

  function changesHtml() {
    const changes = state?.changes || [];
    return `
      <div class="hk-panel-head">
        <div>
          <div class="hk-panel-title">Changes</div>
          <div class="hk-muted">Latest ${changes.length}</div>
        </div>
      </div>
      ${changes.length ? changes.slice(0, 8).map((change) => `
        <div class="hk-row">
          <div>
            <div class="hk-name">${escapeHtml(change.changeType || "change")}</div>
            <div class="hk-muted">${escapeHtml(change.notes || change.label)}</div>
          </div>
          <div class="hk-muted">${escapeHtml(change.createdBy || "")}</div>
        </div>
      `).join("") : `<div class="hk-empty">No change rows yet.</div>`}
    `;
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
