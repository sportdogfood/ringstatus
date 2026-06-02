(function () {
  const root = document.getElementById("horse-kit-static-proof");
  if (!root) return;

  const config = {
    apiUrl: root.dataset.apiUrl || "/wec-packing/horse-kits",
    packWaveKey: root.dataset.packWaveKey || "wave_one"
  };

  const ui = {
    selectedHorseId: "",
    drawerOpen: true,
    loading: false,
    status: "",
    error: ""
  };

  let state = null;

  load();

  root.addEventListener("click", (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    event.preventDefault();
    const action = target.dataset.action;
    if (action === "select-horse") {
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
    if (action === "reload") {
      load();
      return;
    }
    if (action === "set-item-state") {
      runAction({
        action: "set_static_kit_item_state",
        horseId: currentHorse()?.id,
        kitItemId: target.dataset.kitItemId,
        packWaveId: state?.wave?.id,
        packState: target.dataset.packState
      });
    }
  });

  async function load() {
    setBusy("Loading static kit proof...");
    try {
      state = await fetchJson(`${config.apiUrl}?packWaveKey=${encodeURIComponent(config.packWaveKey)}`);
      retainSelection();
      ui.error = "";
      ui.status = sourceStatusText();
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.loading = false;
      render();
    }
  }

  async function runAction(payload) {
    if (!payload?.action) return;
    if (payload.action === "set_static_kit_item_state") {
      applyOptimisticStaticState(payload);
      ui.loading = true;
      ui.status = `Saving ${itemStateLabel(payload.packState)}...`;
      ui.error = "";
      render();
    } else {
      setBusy("Saving kit item state...");
    }
    try {
      const data = await fetchJson(`${config.apiUrl}?packWaveKey=${encodeURIComponent(config.packWaveKey)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state = data.state || data;
      retainSelection();
      ui.error = "";
      ui.status = `Saved ${itemStateLabel(payload.packState)}.`;
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
    const horses = waveOneHorses();
    if (!horses.some((horse) => horse.id === ui.selectedHorseId)) {
      const macho = horses.find((horse) => [horse.name, horse.barnName, horse.showName].join(" ").toLowerCase().includes("macho"));
      ui.selectedHorseId = macho?.id || horses[0]?.id || "";
    }
  }

  function waveOneHorses() {
    return (state?.allHorses || state?.horses || []).filter((horse) => horse.waveState === "wave_one");
  }

  function currentHorse() {
    return (state?.allHorses || state?.horses || []).find((horse) => horse.id === ui.selectedHorseId) || null;
  }

  function staticItems() {
    return (state?.kitItems || [])
      .filter((item) => item.status !== "inactive")
      .sort((a, b) => Number(a.sortOrder || 0) - Number(b.sortOrder || 0) || String(a.label).localeCompare(String(b.label)));
  }

  function activeStackRows() {
    return (state?.groupStack?.activeRows || []).filter((row) => row.active && !row.hidden);
  }

  function stackRowForRole(role) {
    return activeStackRows().find((row) => row.role === role) || null;
  }

  function shouldRenderRole(role) {
    const rows = activeStackRows();
    if (!rows.length) return true;
    return rows.some((row) => row.role === role);
  }

  function rowForItem(itemId, horseId = ui.selectedHorseId) {
    return (state?.packingRows || []).find((row) =>
      row.horseIds.includes(horseId) &&
      row.kitItemIds.includes(itemId)
    ) || null;
  }

  function applyOptimisticStaticState(payload) {
    if (!state) return;
    const horse = currentHorse();
    const item = (state.kitItems || []).find((candidate) => candidate.id === payload.kitItemId);
    if (!horse || !item) return;
    const nextState = payload.packState === "not_needed" ? "not_needed" : payload.packState === "packed" ? "packed" : "not_packed";
    const quantityNeeded = Number(item.manualQuantity || 1) || 1;
    const row = rowForItem(item.id);
    const optimisticRow = {
      ...(row || {}),
      id: row?.id || `temp:${horse.id}:${item.id}`,
      label: `${horse.name} - ${item.label}`,
      horseIds: [horse.id],
      horseName: horse.name,
      kitIds: [],
      kitLabel: "",
      kitItemIds: [item.id],
      itemLabel: item.label,
      packWaveIds: state.wave?.id ? [state.wave.id] : [],
      neededState: nextState === "not_needed" ? "not_needed" : "needed",
      packState: nextState,
      quantityNeeded,
      quantityPacked: nextState === "packed" ? quantityNeeded : 0,
      needed: nextState === "not_needed" ? 0 : quantityNeeded,
      packed: nextState === "packed" ? quantityNeeded : 0,
      left: nextState === "not_needed" || nextState === "packed" ? 0 : quantityNeeded,
      percentPacked: nextState === "packed" ? 100 : 0,
      inlineAllowed: false,
      sortOrder: item.sortOrder,
      notes: "optimistic"
    };
    state.packingRows = (state.packingRows || []).filter((candidate) => candidate.id !== optimisticRow.id);
    if (row?.id) {
      state.packingRows = state.packingRows.filter((candidate) => candidate.id !== row.id);
    }
    state.packingRows.push(optimisticRow);
    state.changes = [{
      id: `pending:${Date.now()}`,
      changeType: nextState === "not_needed" ? "exception_applied" : "quantity_changed",
      notes: `${horse.name} ${item.label} ${nextState}`,
      createdBy: "pending"
    }, ...(state.changes || [])].slice(0, 50);
  }

  function itemState(item, horseId = ui.selectedHorseId) {
    const row = rowForItem(item.id, horseId);
    if (!row) return "not_packed";
    if (row.packState === "not_needed" || row.neededState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function rollup(horseId = ui.selectedHorseId) {
    const items = staticItems();
    const counts = { total: items.length, packed: 0, notNeeded: 0, left: 0 };
    for (const item of items) {
      const stateName = itemState(item, horseId);
      if (stateName === "packed") counts.packed += 1;
      if (stateName === "not_needed") counts.notNeeded += 1;
    }
    counts.left = Math.max(0, counts.total - counts.packed - counts.notNeeded);
    return counts;
  }

  function sourceCounts() {
    const counts = state?.counts || {};
    return {
      horses: Number(counts.visibleHorses ?? waveOneHorses().length) || waveOneHorses().length,
      kitItems: Number(counts.kitItems ?? staticItems().length) || staticItems().length,
      links: Number(counts.packingRows ?? (state?.packingRows || []).length) || 0,
      logs: (state?.changes || []).length
    };
  }

  function sourceStatusText() {
    const counts = sourceCounts();
    const stack = activeStackRows()
      .filter((row) => row.tableName || row.physicalTableName)
      .sort((a, b) => Number(a.stack || 0) - Number(b.stack || 0))
      .map((row) => row.tableName || row.physicalTableName)
      .join(" > ");
    return `Airtable: ${counts.horses} horses | ${counts.kitItems} kit items | ${counts.links} links | ${counts.logs} logs${stack ? ` | ${stack}` : ""}`;
  }

  function selectedHorsePackingRowIds(horseId = ui.selectedHorseId) {
    const horse = (state?.allHorses || state?.horses || []).find((candidate) => candidate.id === horseId) || null;
    const ids = new Set();
    for (const row of state?.packingRows || []) {
      if ((row.horseIds || []).includes(horseId) || (horse?.name && row.horseName === horse.name)) {
        ids.add(row.id);
      }
    }
    return ids;
  }

  function render() {
    if (!state && ui.loading) {
      root.innerHTML = `<div class="rsa-dashboard-block"><div class="rsa-padding"><div class="rsa-text">Loading static horse kit proof...</div></div></div>`;
      return;
    }
    if (state) retainSelection();
    const horse = currentHorse();
    const counts = rollup();
    const horses = waveOneHorses();
    const horseStack = stackRowForRole("entity_1");
    const itemStack = stackRowForRole("entity_2");
    const countsFromSource = sourceCounts();
    const showHorseSection = shouldRenderRole("entity_1");
    const showItemSection = true;
    const showLogSection = shouldRenderRole("logs");
    root.innerHTML = `
      <div class="rsa-dashboard-block hk-proof-root">
        <div class="rsa-padding hk-proof-header">
          <div class="rsa-H1 hk-proof-title">HORSE KITS</div>
          <div class="rsa-text rsa-report-subtitle hk-proof-subtitle">Wave One | ${countsFromSource.horses} horses | ${countsFromSource.kitItems} kit items | ${countsFromSource.links} links</div>
        </div>
        <div class="rsa-padding">
          <div class="rsa-messages">
            <div class="rsa-text is-feedback ${ui.error ? "is-error" : ""}">${escapeHtml(ui.error || ui.status || "")}</div>
          </div>
        </div>
        ${showHorseSection ? horseListHtml(horses, horseStack) : ""}
        ${showItemSection && ui.drawerOpen ? drawerHtml(horse, counts, itemStack, showLogSection) : ""}
      </div>
    `;
  }

  function horseListHtml(horses, horseStack) {
    const source = sourceCounts();
    return `
      <section class="table-module hk-proof-section">
        <div class="rsa-padding">
          <div class="rsa-banner-header">
            <div class="rsa-head-left">
              <div class="rsa-H5 is-caps">${escapeHtml(stackTitle(horseStack, "Horses"))}</div>
              <div class="rsa-text is-xs">${source.horses} Wave One horses | ${source.links} active links</div>
            </div>
            <button class="rs-text-link rsa-text is-link is-xxs" data-action="reload" type="button">RELOAD</button>
          </div>
        </div>
        <div class="hk-table-wrap" role="region" aria-label="Horse kit table">
          <table class="hk-data-table">
            <colgroup>
              <col class="hk-col-gutter">
              <col class="hk-col-horse">
              <col class="hk-col-show">
              <col class="hk-col-packed">
              <col class="hk-col-left">
              <col class="hk-col-open">
            </colgroup>
            <thead>
              <tr>
                <th class="hk-row-gutter rsa-text is-xs">#</th>
                <th class="rsa-table-label rsa-text is-xs is-caps">HORSE</th>
                <th class="rsa-table-label rsa-text is-xs is-caps">SHOW</th>
                <th class="rsa-table-label rsa-text is-xs is-caps">PACKED</th>
                <th class="rsa-table-label rsa-text is-xs is-caps">LEFT</th>
                <th class="rsa-table-label rsa-text is-xs is-caps">OPEN</th>
              </tr>
            </thead>
            <tbody>
              ${horses.map(horseRowHtml).join("") || `<tr><td colspan="6"><div class="rsa-text">No Wave One horses.</div></td></tr>`}
            </tbody>
          </table>
        </div>
      </section>
    `;
  }

  function horseRowHtml(horse, index) {
    const counts = rollup(horse.id);
    const active = horse.id === ui.selectedHorseId;
    return `
      <tr class="hk-proof-hot-row ${active ? "is-active" : ""}" data-action="select-horse" data-horse-id="${horse.id}">
        <td class="hk-row-gutter rsa-text is-xs">${index + 1}</td>
        <td><div class="rsa-text is-line-item">${escapeHtml(horse.name)}</div></td>
        <td><div class="rsa-text is-xs hk-table-secondary">${escapeHtml(horse.showName || horse.barnName || "")}</div></td>
        <td><div class="rsa-text is-number">${counts.packed}/${counts.total}</div></td>
        <td><div class="rsa-text is-number">${counts.left}</div></td>
        <td><div class="rsa-text is-link is-xxs">OPEN</div></td>
      </tr>
    `;
  }

  function drawerHtml(horse, counts, itemStack, showLogSection) {
    if (!horse) {
      return "";
    }
    return `
      <div class="hk-drawer-backdrop" data-action="close-drawer"></div>
      <aside class="hk-drawer" aria-label="Horse kit detail">
        <div class="hk-drawer-header">
          <div class="rsa-H1 hk-drawer-title">${escapeHtml(horse.name)}</div>
          <div class="rsa-text rsa-report-subtitle hk-drawer-subtitle">${escapeHtml(horse.showName || "Horse kit")}</div>
          <button class="hk-drawer-close" data-action="close-drawer" type="button" aria-label="Close">x</button>
        </div>
        <div class="hk-drawer-summary">
          <div class="rsa-item-row-2 hk-metric-row">
            ${metricCellHtml("TOTAL", counts.total)}
            ${metricCellHtml("PACKED", counts.packed)}
            ${metricCellHtml("LEFT", counts.left)}
            ${metricCellHtml("NOT NEEDED", counts.notNeeded)}
          </div>
        </div>
        <div class="rsa-table-head hk-drawer-table-head">
          <div class="rsa-item-row-2 is-grid2">
            <div class="rsa-item-block-left">
              <div class="rsa-table-label rsa-text is-xs is-caps">${escapeHtml(stackTitle(itemStack, "KIT ITEMS"))}</div>
            </div>
            <div class="rsa-item-block-right">
              <div class="rsa-table-label rsa-text is-xs is-caps">STATE</div>
            </div>
          </div>
        </div>
        <div class="rsa-table-body">
          ${staticItems().map(itemRowHtml).join("") || `<div class="rsa-item-row-2"><div class="rsa-text">No active static kit items.</div></div>`}
        </div>
        ${showLogSection ? drawerLogsHtml() : ""}
      </aside>
    `;
  }

  function stackTitle(row, fallback) {
    if (!row?.role) return fallback;
    if (row.role === "entity_1") return "Horses";
    if (row.role === "entity_2") return "Kit Items";
    if (row.role === "kit_list") return "Kit Lists";
    if (row.role === "links") return "Links";
    if (row.role === "logs") return "Active Logs";
    return fallback;
  }

  function itemRowHtml(item, index) {
    const stateName = itemState(item);
    return `
      <div class="rsa-item-row-2 is-grid2 ${index % 2 ? "is-zebra" : ""}" data-static-kit-item-id="${item.id}">
        <div class="rsa-item-block-left">
          <div class="rsa-text is-line-item">${escapeHtml(item.label)}</div>
          <div class="rsa-text is-xs hk-item-state-text">${escapeHtml(displayStateLabel(stateName))}</div>
        </div>
        <div class="rsa-item-block-right hk-state-actions">
          <button class="rs-tab-link rsa-text is-link ${stateName === "packed" ? "is-active is-packed" : ""}" data-action="set-item-state" data-kit-item-id="${item.id}" data-pack-state="${stateName === "packed" ? "not_packed" : "packed"}" type="button">${stateName === "packed" ? "UNPACK" : "PACK"}</button>
          <button class="rs-tab-link rsa-text is-link ${stateName === "not_needed" ? "is-active is-not-needed" : ""}" data-action="set-item-state" data-kit-item-id="${item.id}" data-pack-state="not_needed" type="button">NOT NEEDED</button>
        </div>
      </div>
    `;
  }

  function metricCellHtml(label, value) {
    return `
      <div class="hk-metric-cell">
        <div class="rsa-text is-number">${escapeHtml(value)}</div>
        <div class="rsa-text is-xs is-caps">${escapeHtml(label)}</div>
      </div>
    `;
  }

  function drawerLogsHtml() {
    const rowIds = selectedHorsePackingRowIds();
    const horseName = String(currentHorse()?.name || "").toLowerCase();
    const changes = (state?.changes || [])
      .filter((change) =>
        (change.packingKitIds || []).some((id) => rowIds.has(id)) ||
        (horseName && String(change.notes || change.label || "").toLowerCase().includes(horseName))
      )
      .slice(0, 6);
    const logStack = stackRowForRole("logs");
    return `
      <section class="hk-drawer-logs">
        <div class="rsa-table-head">
          <div class="rsa-item-row-2">
            <div class="rsa-table-label rsa-text is-xs is-caps">${escapeHtml(stackTitle(logStack, "ACTIVE LOGS"))}</div>
          </div>
        </div>
        <div class="rsa-table-body">
          ${changes.map((change, index) => `
          <div class="rsa-item-row-2 is-modal ${index % 2 ? "is-zebra" : ""}">
            <div class="rsa-item-block-left">
              <div class="rsa-text is-line-item">${escapeHtml(changeTitle(change))}</div>
              <div class="rsa-text is-xs">${escapeHtml(changeNotes(change))}</div>
            </div>
          </div>`).join("") || `<div class="rsa-item-row-2 is-modal"><div class="rsa-text">No active logs yet.</div></div>`}
        </div>
      </section>
    `;
  }

  function itemStateLabel(value) {
    if (value === "packed") return "packed";
    if (value === "not_needed") return "not needed";
    if (value === "not_packed") return "not packed";
    return value || "";
  }

  function displayStateLabel(value) {
    if (value === "packed") return "Packed";
    if (value === "not_needed") return "Not needed";
    return "Not packed";
  }

  function changeTitle(change) {
    const raw = String(change.changeType || "").toLowerCase();
    const notes = String(change.notes || change.label || "").toLowerCase();
    if (raw.includes("exception") || notes.includes("not_needed")) return "Not needed";
    if (notes.includes("not_packed") || notes.includes("unpack")) return "Unpacked";
    if (notes.includes("packed")) return "Packed";
    return "Changed";
  }

  function changeNotes(change) {
    return String(change.notes || change.label || "")
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }
})();
