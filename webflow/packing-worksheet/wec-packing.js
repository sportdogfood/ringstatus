(function () {
  const root = document.getElementById("packing-app");
  if (!root) return;

  const config = window.WEC_PACKING_CONFIG || {};
  const apiBaseUrl = String(config.apiBaseUrl || "https://ringstatus.webflow.io/test/wec-packing").replace(/\/$/, "");
  const state = {
    activeTab: "overview",
    data: null,
    error: "",
    loading: true,
    detailType: "",
    detailId: ""
  };

  root.classList.toggle("is-edit-mode", config.mode === "edit");
  root.addEventListener("click", handleClick);
  render();
  loadState();

  async function loadState() {
    state.loading = true;
    state.error = "";
    render();

    try {
      const response = await fetch(stateUrl(), { headers: { Accept: "application/json" } });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.error || `state_${response.status}`);
      }
      state.data = payload;
    } catch (error) {
      state.error = error instanceof Error ? error.message : String(error);
    } finally {
      state.loading = false;
      render();
    }
  }

  function stateUrl() {
    const url = new URL(config.stateUrl || `${apiBaseUrl}/state`);
    if (config.showId) url.searchParams.set("showId", config.showId);
    if (config.packWaveId) url.searchParams.set("packWaveId", config.packWaveId);
    return url.toString();
  }

  function handleClick(event) {
    const close = event.target.closest("[data-close-detail]");
    if (close) {
      closeDetail();
      return;
    }

    const tab = event.target.closest("[data-tab]");
    if (tab) {
      state.activeTab = tab.dataset.tab;
      state.detailType = "";
      state.detailId = "";
      render();
      return;
    }

    const item = event.target.closest("[data-item-id]");
    if (item) {
      state.detailType = "item";
      state.detailId = item.dataset.itemId;
      renderDetail();
      return;
    }

    const horse = event.target.closest("[data-horse-id]");
    if (horse) {
      state.detailType = "horse";
      state.detailId = horse.dataset.horseId;
      renderDetail();
    }
  }

  function closeDetail() {
    state.detailType = "";
    state.detailId = "";
    renderDetail();
  }

  function render() {
    root.innerHTML = `
      <div class="lp-shell">
        <header class="lp-header">
          <div class="lp-header-copy">
            <h1>WEC Ocala Packing</h1>
            <p>${escapeHtml(statusLine())}</p>
          </div>
        </header>
        ${tabsHtml()}
        <main>${panelHtml()}</main>
        <footer class="lp-shell-footer">
          <p>${escapeHtml(footerLine())}</p>
        </footer>
        <div class="lp-modal" id="packingDetail" hidden aria-hidden="true">
          <div class="lp-modal-backdrop" data-close-detail></div>
          <article class="lp-modal-card" role="dialog" aria-modal="true" aria-labelledby="drawerTitle">
            <button class="lp-modal-close" type="button" data-close-detail aria-label="Close detail">x</button>
            <div id="packingDetailContent"></div>
          </article>
        </div>
      </div>
    `;
    renderDetail();
  }

  function statusLine() {
    if (state.loading) return "Loading live Airtable state";
    if (state.error) return "State unavailable";
    const wave = state.data?.wave?.wave;
    if (wave) return wave;
    return "No active pack wave";
  }

  function footerLine() {
    if (state.error) return state.error;
    if (state.loading) return "Checking live state";
    return `Last checked: ${new Date().toLocaleString()}`;
  }

  function tabsHtml() {
    return `
      <nav class="lp-tabs" aria-label="Packing sections">
        ${tabs().map((section) => {
          const count = section.id === "horses"
            ? horses().filter((horse) => horse.active).length
            : section.id === "overview"
              ? totalOpenRows()
              : sectionCount(section.id);
          return `
            <button class="lp-tab packing-tab packing-theme-${themeKey(section.id)} ${state.activeTab === section.id ? "is-active" : ""}" type="button" data-tab="${escapeHtml(section.id)}">
              <span class="lp-tab-value">${count}</span>
              <span class="lp-tab-label">${escapeHtml(displayLabel(section.label))}</span>
            </button>
          `;
        }).join("")}
      </nav>
    `;
  }

  function panelHtml() {
    if (state.loading) return messagePanel("Loading");
    if (state.error) return messagePanel("Unable to load packing state");
    if (!state.data) return messagePanel("No state");
    if (state.activeTab === "overview") return overviewHtml();
    if (state.activeTab === "horses") return horsesHtml();
    return listHtml(state.activeTab);
  }

  function messagePanel(title) {
    return `
      <section class="lp-section-block">
        <div class="lp-list">
          <div class="lp-row is-static">
            <span class="lp-row-title">${escapeHtml(title)}</span>
            <span class="lp-row-meta">${escapeHtml(state.error || statusLine())}</span>
          </div>
        </div>
      </section>
    `;
  }

  function overviewHtml() {
    const rows = lists().map((list) => {
      const summary = listSummary(list.id);
      return `
        <button class="lp-row packing-row" type="button" data-tab="${escapeHtml(list.id)}">
          <span>
            <span class="lp-row-title">${escapeHtml(displayLabel(list.label))}</span>
            <span class="lp-row-meta">Rows: ${summary.rows} | Left ${summary.open}</span>
          </span>
          ${tokenHtml(summary.open === 0 && summary.rows > 0 ? "packed" : "open", summary.open === 0 && summary.rows > 0 ? "PACKED" : `LEFT - ${summary.open}`)}
        </button>
      `;
    }).join("");

    return `
      <section class="lp-section-block packing-theme-overview">
        <div class="lp-section-title packing-section-title">
          <h3>Overview</h3>
          <span class="lp-section-count">${doneRows()}/${totalRows()} done</span>
        </div>
        <div class="lp-list">
          ${state.data.needsGeneration ? noWaveRowHtml() : rows}
        </div>
      </section>
    `;
  }

  function noWaveRowHtml() {
    return `
      <div class="lp-row is-static">
        <span>
          <span class="lp-row-title">No pack wave</span>
          <span class="lp-row-meta">Rows: 0 | Left 0</span>
        </span>
        ${tokenHtml("need", "NEED - 0")}
      </div>
    `;
  }

  function listHtml(listId) {
    const list = lists().find((row) => row.id === listId) || { id: listId, label: listId };
    const rows = items().filter((item) => itemBelongsToList(item, listId));
    return `
      <section class="lp-section-block packing-theme-${themeKey(list.id)}">
        <div class="lp-section-title packing-section-title">
          <h3>${escapeHtml(displayLabel(list.label))}</h3>
          <span class="lp-section-count">Rows: ${rows.length} | Left ${rows.filter((row) => !isDone(row)).length}</span>
        </div>
        <div class="lp-list">
          ${rows.length ? rows.map(itemRowHtml).join("") : emptyRowHtml("No rows")}
        </div>
      </section>
    `;
  }

  function horsesHtml() {
    const rows = horses();
    return `
      <section class="lp-section-block packing-theme-horses">
        <div class="lp-section-title packing-section-title">
          <h3>Horses</h3>
          <span class="lp-section-count">${rows.filter((horse) => horse.active).length}/${rows.length} active</span>
        </div>
        <div class="lp-list">
          ${rows.length ? rows.map(horseRowHtml).join("") : emptyRowHtml("No horses")}
        </div>
      </section>
    `;
  }

  function itemRowHtml(item) {
    return `
      <button class="lp-row packing-row" type="button" data-item-id="${escapeHtml(item.id)}">
          <span>
          <span class="lp-row-title">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</span>
          <span class="lp-row-meta">${escapeHtml(itemMetaLabel(item))}</span>
        </span>
        <span class="packing-state-stack">
          ${rowTokenHtml(item)}
          <span class="packing-token-meta">Need: ${number(item.needed)}</span>
        </span>
      </button>
    `;
  }

  function horseRowHtml(horse) {
    return `
      <button class="lp-row packing-row packing-horse-row" type="button" data-horse-id="${escapeHtml(horse.id)}">
        <span class="lp-row-title">${escapeHtml(horse.name || horse.showName || "Unnamed horse")}</span>
        ${tokenHtml(horse.active ? "packed" : "need", horse.active ? "ACTIVE" : "INACTIVE")}
      </button>
    `;
  }

  function emptyRowHtml(label) {
    return `
      <div class="lp-row is-static">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">${escapeHtml(statusLine())}</span>
      </div>
    `;
  }

  function rowTokenHtml(item) {
    if (item.resolutionState) return tokenHtml("resolved", item.resolutionState.toUpperCase());
    if (item.packState === "packed" || number(item.left) === 0 && number(item.needed) > 0) return tokenHtml("packed", "PACKED");
    if (number(item.packed) > 0) return tokenHtml("open", `LEFT - ${number(item.left)}`);
    return tokenHtml("need", `NEED - ${number(item.needed)}`);
  }

  function tokenHtml(type, text) {
    return `<span class="lp-achievement packing-token is-${type}">${escapeHtml(text)}</span>`;
  }

  function renderDetail() {
    const modal = root.querySelector("#packingDetail");
    const content = root.querySelector("#packingDetailContent");
    if (!modal || !content) return;

    if (!state.detailType || !state.detailId) {
      modal.hidden = true;
      modal.setAttribute("aria-hidden", "true");
      content.innerHTML = "";
      return;
    }

    modal.hidden = false;
    modal.setAttribute("aria-hidden", "false");
    content.innerHTML = state.detailType === "horse"
      ? horseDetailHtml(horses().find((horse) => horse.id === state.detailId))
      : itemDetailHtml(items().find((item) => item.id === state.detailId));
  }

  function itemDetailHtml(item) {
    if (!item) return "";
    return `
      <div class="packing-detail packing-theme-${themeKey(item.packListIds?.[0] || item.section || "overview")}">
        <div class="lp-detail-head">
          <h3 id="drawerTitle">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</h3>
          <p class="lp-muted">${escapeHtml(itemMetaLabel(item))}</p>
        </div>
        ${detailGroupHtml("Location", [["Location", item.location || ""]])}
        ${detailGroupHtml("Totals", [
          ["Need", number(item.needed)],
          ["Packed", number(item.packed)],
          ["Left", number(item.left)]
        ])}
        ${calculationDetailHtml(item.quantityCalculation)}
        ${detailGroupHtml("Horses", [["Horses", item.horseMembers?.length ? `${item.horseMembers.length} members` : "Not horse-specific"]])}
        <div class="lp-edit-panel packing-edit-panel">
          <div class="lp-edit-head"><h4>Worksheet</h4></div>
          <div class="packing-control-list">
            ${controlRowHtml("Status", item.packState || "not_packed")}
            ${controlRowHtml("Packed", `${number(item.packed)}${item.unit ? ` ${item.unit}` : ""}`)}
            ${controlRowHtml("Decision", item.resolutionState || "none")}
            ${controlRowHtml("Notes", item.notes || "")}
          </div>
        </div>
      </div>
    `;
  }

  function horseDetailHtml(horse) {
    if (!horse) return "";
    return `
      <div class="packing-detail packing-theme-horses">
        <div class="lp-detail-head">
          <h3 id="drawerTitle">${escapeHtml(horse.name || "Unnamed horse")}</h3>
          <p class="lp-muted">${escapeHtml(horse.showName || "")}</p>
        </div>
        ${detailGroupHtml("Meta", [
          ["Record State", horse.recordState || "inactive"],
          ["Weeks", horse.weekIds?.length || 0],
          ["Items", horse.sourcePackItemIds?.length || 0]
        ])}
      </div>
    `;
  }

  function detailGroupHtml(title, rows) {
    return `
      <h4 class="packing-detail-group-title">${escapeHtml(title)}</h4>
      <div class="lp-detail-list">
        ${rows.map(([label, value]) => `
          <div class="lp-detail-row">
            <div class="lp-detail-label">${escapeHtml(label)}</div>
            <div class="lp-detail-value">${escapeHtml(value)}</div>
          </div>
        `).join("")}
      </div>
    `;
  }

  function controlRowHtml(label, value) {
    return `
      <div class="lp-row is-static is-detail packing-control-row">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">${escapeHtml(value)}</span>
      </div>
    `;
  }

  function listSummary(listId) {
    const apiSummary = lists().find((list) => list.id === listId);
    if (apiSummary) return apiSummary;
    const rows = items().filter((item) => itemBelongsToList(item, listId));
    const done = rows.filter(isDone).length;
    return { id: listId, rows: rows.length, done, open: rows.length - done };
  }

  function sectionCount(sectionId) {
    return listSummary(sectionId).rows;
  }

  function doneRows() {
    return items().filter(isDone).length;
  }

  function totalRows() {
    return items().length;
  }

  function totalOpenRows() {
    return totalRows() - doneRows();
  }

  function isDone(item) {
    return item.packState === "packed" || !!item.resolutionState;
  }

  function items() {
    return Array.isArray(state.data?.items) ? state.data.items : [];
  }

  function horses() {
    return Array.isArray(state.data?.horses) ? state.data.horses : [];
  }

  function lists() {
    if (Array.isArray(state.data?.lists) && state.data.lists.length) return state.data.lists;
    if (Array.isArray(state.data?.sections)) {
      return state.data.sections.map((section) => ({
        id: section.section,
        label: section.label || section.section,
        rows: section.rows,
        done: section.done,
        open: section.open
      }));
    }
    return [];
  }

  function tabs() {
    return [
      { id: "overview", label: "Overview" },
      ...lists().map((list) => ({ id: list.id, label: list.label })),
      { id: "horses", label: "Horses" }
    ];
  }

  function calculationDetailHtml(calculation) {
    if (!calculation) return "";
    const unit = calculation.unit ? ` ${calculation.unit}` : "";
    const rows = calculation.plan === "per_groom"
      ? [
        ["Formula", calculation.formula],
        ["Per Groom", `${number(calculation.base)}${unit}`],
        ["Grooms", number(calculation.multiplier)],
        ["Calculated", `${number(calculation.calculatedNeeded)}${unit}`],
        ["Worksheet Need", `${number(calculation.frozenNeeded)}${unit}`]
      ]
      : [
        ["Plan", displayLabel(calculation.plan)],
        ["Formula", calculation.formula],
        ["Calculated", `${number(calculation.calculatedNeeded)}${unit}`],
        ["Worksheet Need", `${number(calculation.frozenNeeded)}${unit}`]
      ];
    return detailGroupHtml("Calculation", rows);
  }

  function itemBelongsToList(item, listId) {
    return item.packListIds?.includes(listId) ||
      item.section === listId ||
      (!item.packListIds?.length && !item.section && listId === "unlisted");
  }

  function itemMetaLabel(item) {
    return item.packListLabels?.map(displayLabel).join(", ") ||
      displayLabel(item.category || item.section || item.listPlan || "");
  }

  function number(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function displayLabel(value) {
    const text = String(value || "").replace(/[_-]+/g, " ").trim();
    return text.replace(/\b[a-z]/g, (letter) => letter.toUpperCase());
  }

  function themeKey(value) {
    return String(value || "overview").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "overview";
  }
})();
