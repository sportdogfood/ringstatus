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
    saving: false,
    saveMessage: "",
    detailType: "",
    detailId: "",
    addQty: {},
    actionNotes: {}
  };

  root.classList.toggle("is-edit-mode", config.mode === "edit");
  root.addEventListener("click", handleClick);
  root.addEventListener("input", handleInput);
  render();
  loadState();

  async function loadState() {
    state.loading = true;
    state.error = "";
    render();

    try {
      const response = await fetch(endpointUrl("state"), { headers: { Accept: "application/json" } });
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

  function endpointUrl(kind) {
    const explicit = kind === "state" ? config.stateUrl : kind === "action" ? config.actionUrl : "";
    const url = new URL(explicit || `${apiBaseUrl}/${kind}`);
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

    const packingAction = event.target.closest("[data-packing-action]");
    if (packingAction) {
      event.preventDefault();
      runPackingAction(packingAction);
      return;
    }

    const horseMemberAction = event.target.closest("[data-horse-member-state]");
    if (horseMemberAction) {
      event.preventDefault();
      setHorseMemberState(horseMemberAction);
      return;
    }

    const sourceFlagAction = event.target.closest("[data-source-flag]");
    if (sourceFlagAction) {
      event.preventDefault();
      setSourceFlag(sourceFlagAction);
      return;
    }

    const horseToggle = event.target.closest("[data-horse-toggle]");
    if (horseToggle) {
      event.preventDefault();
      toggleHorseState(horseToggle);
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
    }
  }

  function handleInput(event) {
    const addQty = event.target.closest("[data-add-qty]");
    if (addQty) {
      state.addQty[addQty.dataset.addQty] = addQty.value;
      return;
    }

    const notes = event.target.closest("[data-action-notes]");
    if (notes) {
      state.actionNotes[notes.dataset.actionNotes] = notes.value;
    }
  }

  async function runPackingAction(button) {
    const itemId = button.dataset.itemId || state.detailId;
    const action = button.dataset.packingAction;
    const item = items().find((row) => row.id === itemId);
    if (!item) return;

    if (action === "add_quantity") {
      const quantityDelta = Number(state.addQty[itemId] || 0);
      if (!Number.isFinite(quantityDelta) || quantityDelta <= 0) {
        setSaveMessage("Enter a quantity to add.");
        return;
      }
      await postAction({
        action,
        itemId,
        quantityDelta,
        notes: state.actionNotes[itemId] || ""
      }, () => {
        state.addQty[itemId] = "";
      });
      return;
    }

    if (action === "set_pack_state") {
      const packState = button.dataset.packState;
      if (packState === "packed" && !window.confirm("Mark this item packed and set packed quantity to the full need?")) return;
      await postAction({
        action,
        itemId,
        packState,
        confirmed: packState === "packed",
        notes: state.actionNotes[itemId] || ""
      });
      return;
    }

    if (action === "set_resolution") {
      const resolutionState = button.dataset.resolutionState;
      const label = resolutionState === "clear" ? "clear this decision" : `set decision to ${displayLabel(resolutionState)}`;
      if (!window.confirm(`Confirm ${label}?`)) return;
      await postAction({
        action,
        itemId,
        resolutionState,
        confirmed: true,
        notes: state.actionNotes[itemId] || ""
      });
    }
  }

  async function setHorseMemberState(button) {
    const itemHorseId = button.dataset.itemHorseId;
    const horsePackState = button.dataset.horseMemberState;
    if (!itemHorseId || !horsePackState) return;
    await postAction({
      action: "set_horse_pack_state",
      itemHorseId,
      horsePackState
    });
  }

  async function toggleHorseState(button) {
    const horseId = button.dataset.horseId;
    const nextState = button.dataset.nextState;
    if (!horseId || !nextState) return;
    await postAction({
      action: "set_horse_record_state",
      horseId,
      recordState: nextState
    });
  }

  async function setSourceFlag(button) {
    const sourceItemId = button.dataset.sourceItemId;
    const flagName = button.dataset.sourceFlag;
    const value = button.dataset.nextValue === "true";
    if (!sourceItemId || !flagName) return;
    await postAction({
      action: "set_source_flag",
      sourceItemId,
      flagName,
      value
    });
  }

  async function postAction(payload, afterSave) {
    state.saving = true;
    state.saveMessage = "Saving...";
    render();

    try {
      const response = await fetch(endpointUrl("action"), {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || !result.ok) {
        throw new Error(result.detail || result.error || `save_${response.status}`);
      }
      if (typeof afterSave === "function") afterSave(result);
      state.data = result.state || state.data;
      state.saveMessage = `Saved: ${new Date().toLocaleString()}`;
    } catch (error) {
      state.saveMessage = `Save failed: ${error instanceof Error ? error.message : String(error)}`;
    } finally {
      state.saving = false;
      render();
    }
  }

  function setSaveMessage(message) {
    state.saveMessage = message;
    renderDetail();
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
    const wave = state.data?.wave;
    if (!wave) return "No active pack wave";
    const countSource = wave.countSource === "manual_lock" ? "Manual lock" : "Current wave";
    return `${displayLabel(wave.wave || "Pack wave")} | ${countSource}`;
  }

  function footerLine() {
    if (state.saveMessage) return state.saveMessage;
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
            <button class="lp-tab packing-tab packing-theme-${themeKey(section.id)} ${state.activeTab === section.id ? "is-active" : ""}" type="button" data-tab="${escapeAttr(section.id)}">
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
        <button class="lp-row packing-row" type="button" data-tab="${escapeAttr(list.id)}">
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
      <button class="lp-row packing-row" type="button" data-item-id="${escapeAttr(item.id)}">
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
    const nextState = horse.active ? "inactive" : "active";
    return `
      <button class="lp-row packing-row packing-horse-row" type="button" data-horse-toggle data-horse-id="${escapeAttr(horse.id)}" data-next-state="${escapeAttr(nextState)}">
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
    if (item.resolutionState) return tokenHtml("resolved", displayLabel(item.resolutionState).toUpperCase());
    if (item.packState === "packed" || (number(item.left) === 0 && number(item.needed) > 0)) return tokenHtml("packed", "PACKED");
    if (number(item.packed) > 0) return tokenHtml("open", `LEFT - ${number(item.left)}`);
    return tokenHtml("need", `NEED - ${number(item.needed)}`);
  }

  function tokenHtml(type, text) {
    return `<span class="lp-achievement packing-token is-${escapeAttr(type)}">${escapeHtml(text)}</span>`;
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
      <div class="packing-detail packing-theme-${themeKey(item.packListIds?.[0] || "overview")}">
        <div class="lp-detail-head">
          <h3 id="drawerTitle">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</h3>
          <p class="lp-muted">${escapeHtml(itemMetaLabel(item))}</p>
        </div>
        ${detailGroupHtml("Location", [["Location", item.location || ""]])}
        ${detailGroupHtml("Totals", [
          ["Need", quantityLabel(item.needed, item.unit)],
          ["Packed", quantityLabel(item.packed, item.unit)],
          ["Left", quantityLabel(item.left, item.unit)]
        ])}
        ${calculationDetailHtml(item.quantityCalculation)}
        ${detailGroupHtml("Meta", [
          ["Plan", displayLabel(item.listPlanLabel || item.listPlan || "unresolved")],
          ["Record State", item.recordState || "active"],
          ["Source", item.sourceItems?.[0]?.appName || ""]
        ])}
        ${worksheetPanelHtml(item)}
        ${sourcePanelHtml(item)}
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

  function worksheetPanelHtml(item) {
    return `
      <div class="lp-edit-panel packing-edit-panel">
        <div class="lp-edit-head"><h4>Worksheet</h4></div>
        <div class="lp-edit-grid">
          ${statusControlHtml(item)}
          ${packedControlHtml(item)}
          ${horseMembersControlHtml(item)}
          ${notesControlHtml(item)}
          ${decisionControlHtml(item)}
        </div>
        <p class="lp-edit-status">${escapeHtml(state.saveMessage || "Changes save to Airtable through Webflow Cloud.")}</p>
      </div>
    `;
  }

  function statusControlHtml(item) {
    return editGroupHtml("Status", `
      <span class="lp-edit-choice-row packing-inline-choices">
        ${choiceButtonHtml({
          label: "NOT PACKED",
          active: item.packState !== "packed",
          attrs: `data-packing-action="set_pack_state" data-item-id="${escapeAttr(item.id)}" data-pack-state="not_packed"`
        })}
        ${choiceButtonHtml({
          label: "PACKED",
          active: item.packState === "packed",
          attrs: `data-packing-action="set_pack_state" data-item-id="${escapeAttr(item.id)}" data-pack-state="packed"`
        })}
      </span>
    `);
  }

  function packedControlHtml(item) {
    return editGroupHtml("Packed", `
      <span class="packing-add-control">
        <input class="lp-edit-input" type="number" min="0" step="1" inputmode="numeric" placeholder="Add qty" value="${escapeAttr(state.addQty[item.id] || "")}" data-add-qty="${escapeAttr(item.id)}">
        <button class="lp-edit-button" type="button" data-packing-action="add_quantity" data-item-id="${escapeAttr(item.id)}">ADD</button>
      </span>
    `);
  }

  function horseMembersControlHtml(item) {
    if (!item.horseMembers?.length) return editGroupHtml("Horses", `<span class="lp-row-meta">Not horse-specific</span>`);
    return editGroupHtml("Horses", `
      <span class="packing-horse-bindings">
        ${item.horseMembers.map((member) => {
          const packed = member.horsePackState === "packed" || number(member.packed) >= number(member.needed);
          return `
            <span class="packing-horse-binding-row">
              <span class="packing-horse-binding-name">${escapeHtml(member.barnName || "Unnamed horse")}</span>
              <button class="lp-edit-pill packing-horse-binding-toggle ${packed ? "is-active" : ""}" type="button" data-horse-member-state="${packed ? "not_packed" : "packed"}" data-item-horse-id="${escapeAttr(member.id)}">
                ${packed ? "PACKED" : "NOT PACKED"}
              </button>
            </span>
          `;
        }).join("")}
      </span>
    `, "is-wide");
  }

  function notesControlHtml(item) {
    const value = state.actionNotes[item.id] ?? item.notes ?? "";
    return editGroupHtml("Notes", `<textarea class="lp-edit-textarea" data-action-notes="${escapeAttr(item.id)}">${escapeHtml(value)}</textarea>`, "is-wide");
  }

  function decisionControlHtml(item) {
    const decisions = ["max", "kill", "note", "purchase_onsite", "unresolved"];
    return editGroupHtml("Decision", `
      <span class="lp-edit-choice-row packing-inline-choices packing-decision-choices">
        ${decisions.map((decision) => choiceButtonHtml({
          label: displayLabel(decision).toUpperCase(),
          active: item.resolutionState === decision,
          attrs: `data-packing-action="set_resolution" data-item-id="${escapeAttr(item.id)}" data-resolution-state="${escapeAttr(decision)}"`
        })).join("")}
        <button class="lp-edit-pill packing-decision-clear" type="button" data-packing-action="set_resolution" data-item-id="${escapeAttr(item.id)}" data-resolution-state="clear">CLEAR</button>
      </span>
    `);
  }

  function sourcePanelHtml(item) {
    const source = item.sourceItems?.[0];
    if (!source) return "";
    const flags = source.sourceFlags || {};
    return `
      <div class="lp-edit-panel packing-edit-panel">
        <div class="lp-edit-head"><h4>Source</h4></div>
        <div class="lp-edit-grid">
          ${editGroupHtml("Name", `<span class="lp-row-meta">${escapeHtml(source.appName || "")}</span>`)}
          ${editGroupHtml("Plan", `<span class="lp-row-meta">${escapeHtml(displayLabel(source.listPlanLabel || source.listPlan || ""))}</span>`)}
          ${editGroupHtml("Inputs", `
            <span class="lp-edit-choice-row packing-inline-choices">
              ${sourceFlagButtonHtml(source.id, "ignore", "IGNORE", !!flags.ignore)}
              ${sourceFlagButtonHtml(source.id, "rename", "RENAME", !!flags.rename)}
              ${sourceFlagButtonHtml(source.id, "change_lane", "CHANGE LIST", !!flags.changeLane)}
            </span>
          `)}
        </div>
      </div>
    `;
  }

  function sourceFlagButtonHtml(sourceItemId, flagName, label, active) {
    return `<button class="lp-edit-pill ${active ? "is-active" : ""}" type="button" data-source-flag="${escapeAttr(flagName)}" data-source-item-id="${escapeAttr(sourceItemId)}" data-next-value="${active ? "false" : "true"}">${escapeHtml(label)}</button>`;
  }

  function choiceButtonHtml({ label, active, attrs }) {
    return `<button class="lp-edit-pill ${active ? "is-active" : ""}" type="button" ${attrs}>${escapeHtml(label)}</button>`;
  }

  function editGroupHtml(title, body, extraClass) {
    return `
      <div class="lp-edit-group ${extraClass || ""}">
        <div class="lp-edit-group-title">${escapeHtml(title)}</div>
        <div class="lp-edit-group-fields">${body}</div>
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
    const rows = [
      ["Plan", displayLabel(calculation.plan)],
      ["Formula", calculation.formula],
      ["Base", quantityLabel(calculation.base, unit.trim())],
      ["Multiplier", number(calculation.multiplier)],
      ["Count Source", calculation.countsLocked ? "Manual Lock" : "Current Wave"],
      ["Calculated", quantityLabel(calculation.calculatedNeeded, unit.trim())],
      ["Worksheet Need", quantityLabel(calculation.frozenNeeded, unit.trim())]
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
      displayLabel(item.category || item.section || item.listPlanLabel || item.listPlan || "");
  }

  function quantityLabel(value, unit) {
    const suffix = unit ? ` ${unit}` : "";
    return `${number(value)}${suffix}`;
  }

  function number(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
  }

  function escapeAttr(value) {
    return escapeHtml(value);
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
