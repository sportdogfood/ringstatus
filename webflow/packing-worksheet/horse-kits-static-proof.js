(function () {
  const root = document.getElementById("horse-kit-static-proof");
  if (!root) return;

  const config = {
    apiUrl: root.dataset.apiUrl || "/wec-packing/horse-kits",
    packWaveKey: root.dataset.packWaveKey || "wave_one"
  };

  const ui = {
    selectedRecordId: "",
    drawerOpen: false,
    loading: false,
    error: ""
  };

  let state = null;
  let records = [];

  load();

  root.addEventListener("click", (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;

    const action = target.dataset.action;
    if (action === "open-record") {
      ui.selectedRecordId = target.dataset.recordId || "";
      ui.drawerOpen = true;
      render();
    }

    if (action === "close-drawer") {
      ui.drawerOpen = false;
      render();
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
      ui.selectedRecordId = records[0]?.id || "";
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.loading = false;
      render();
    }
  }

  async function fetchJson(url) {
    const response = await fetch(url);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) {
      throw new Error(data.detail || data.error || `${response.status} ${response.statusText}`);
    }
    return data;
  }

  function buildRecords() {
    return waveOneHorses().map((horse) => {
      const counts = rollup(horse.id);
      const rowIds = selectedHorsePackingRowIds(horse.id);
      const logs = (state?.changes || []).filter((change) =>
        (change.packingKitIds || []).some((id) => rowIds.has(id)) ||
        String(change.notes || change.label || "").toLowerCase().includes(String(horse.name || "").toLowerCase())
      );

      return {
        id: horse.id,
        title: horse.name || "Untitled",
        fields: [
          { label: "Horse", value: horse.name },
          { label: "Show", value: horse.showName || horse.barnName || "" },
          { label: "Wave", value: "Wave One" },
          { label: "Packed", value: `${counts.packed}/${counts.total}`, numeric: true },
          { label: "Left", value: counts.left, numeric: true },
          { label: "Not Needed", value: counts.notNeeded, numeric: true },
          { label: "Kit Items", value: counts.total, numeric: true },
          { label: "Active Links", value: rowIds.size, numeric: true },
          { label: "Logs", value: logs.length, numeric: true }
        ]
      };
    });
  }

  function waveOneHorses() {
    return (state?.allHorses || state?.horses || []).filter((horse) => horse.waveState === "wave_one");
  }

  function staticItems() {
    return (state?.kitItems || [])
      .filter((item) => item.status !== "inactive")
      .sort((a, b) => Number(a.sortOrder || 0) - Number(b.sortOrder || 0) || String(a.label).localeCompare(String(b.label)));
  }

  function rowForItem(itemId, horseId) {
    return (state?.packingRows || []).find((row) =>
      (row.horseIds || []).includes(horseId) &&
      (row.kitItemIds || []).includes(itemId)
    ) || null;
  }

  function itemState(item, horseId) {
    const row = rowForItem(item.id, horseId);
    if (!row) return "not_packed";
    if (row.packState === "not_needed" || row.neededState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function rollup(horseId) {
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

  function selectedHorsePackingRowIds(horseId) {
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
    if (ui.loading) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty">Loading records...</div></div>`;
      return;
    }

    if (ui.error) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></div>`;
      return;
    }

    if (!records.length) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-airtable-empty">No records found</div></div>`;
      return;
    }

    const selectedRecord = records.find((record) => record.id === ui.selectedRecordId) || records[0];
    root.innerHTML = `
      <div class="rs-airtable-shell">
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup>
              <col class="rs-col-gutter">
              <col class="rs-col-main">
              <col class="rs-col-mid">
              <col class="rs-col-count">
              <col class="rs-col-count">
              <col class="rs-col-count">
              <col class="rs-col-count">
            </colgroup>
            <thead>
              <tr>
                <th class="rs-row-gutter">#</th>
                <th>Horse</th>
                <th>Show</th>
                <th>Packed</th>
                <th>Left</th>
                <th>Items</th>
                <th>Logs</th>
              </tr>
            </thead>
            <tbody>
              ${records.map(recordRowHtml).join("")}
            </tbody>
          </table>
        </div>

        <div class="rs-drawer-overlay ${ui.drawerOpen ? "is-open" : ""}" data-action="close-drawer"></div>

        <div class="rs-record-drawer ${ui.drawerOpen ? "is-open" : ""}" aria-label="Record details">
          <div class="rs-drawer-head">
            <div class="rs-drawer-title">Record Details</div>
            <button class="rs-drawer-close" type="button" aria-label="Close" data-action="close-drawer">x</button>
          </div>
          <div class="rs-drawer-body">
            ${selectedRecord ? selectedRecord.fields.map(fieldHtml).join("") : ""}
          </div>
        </div>
      </div>
    `;
  }

  function recordRowHtml(record, index) {
    const byLabel = new Map(record.fields.map((field) => [field.label, field.value]));
    const selected = ui.drawerOpen && record.id === ui.selectedRecordId;
    return `
      <tr class="${selected ? "is-selected" : ""}" data-action="open-record" data-record-id="${record.id}" tabindex="0">
        <td class="rs-row-gutter">${index + 1}</td>
        <td>${escapeHtml(byLabel.get("Horse") || "")}</td>
        <td>${escapeHtml(byLabel.get("Show") || "")}</td>
        <td class="rs-cell-number">${escapeHtml(byLabel.get("Packed") || "")}</td>
        <td class="rs-cell-number">${escapeHtml(byLabel.get("Left") || "")}</td>
        <td class="rs-cell-number">${escapeHtml(byLabel.get("Kit Items") || "")}</td>
        <td class="rs-cell-number">${escapeHtml(byLabel.get("Logs") || "")}</td>
      </tr>
    `;
  }

  function fieldHtml(field) {
    return `
      <div class="rs-record-field">
        <div class="rs-field-label">${escapeHtml(field.label)}</div>
        <div class="rs-field-value ${field.numeric ? "rs-cell-number" : ""}">${escapeHtml(field.value)}</div>
      </div>
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
})();
