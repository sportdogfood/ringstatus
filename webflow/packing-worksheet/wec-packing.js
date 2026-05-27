(function () {
  const root = document.getElementById("packing-app");
  if (!root) return;

  const config = window.WEC_PACKING_CONFIG || {};
  const apiUrl = String(config.apiUrl || "").trim();
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
    didSetInitialTab: false,
    activeListByTab: {},
    searchBySection: {},
    inlineEditByList: {},
    inlineEditValues: {},
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
      if (!state.didSetInitialTab) {
        state.activeTab = "overview";
        state.didSetInitialTab = true;
      }
    } catch (error) {
      state.error = error instanceof Error ? error.message : String(error);
    } finally {
      state.loading = false;
      render();
    }
  }

  function endpointUrl(kind) {
    const explicit = kind === "state" ? config.stateUrl : kind === "action" ? config.actionUrl : "";
    const url = new URL(explicit || apiUrl || `${apiBaseUrl}/${kind}`);
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

    const printHorseAction = event.target.closest("[data-print-horse]");
    if (printHorseAction) {
      event.preventDefault();
      printHorseList(printHorseAction.dataset.printHorse);
      return;
    }

    const printAction = event.target.closest("[data-print-section]");
    if (printAction) {
      event.preventDefault();
      printSection(printAction.dataset.printSection);
      return;
    }

    const listSwitch = event.target.closest("[data-list-switch]");
    if (listSwitch) {
      event.preventDefault();
      state.activeListByTab[listSwitch.dataset.tabId] = listSwitch.dataset.listSwitch;
      render();
      return;
    }

    const listEdit = event.target.closest("[data-list-edit-field]");
    if (listEdit) {
      event.preventDefault();
      toggleListInlineEdit(listEdit.dataset.listId, listEdit.dataset.listEditField);
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

    const horseDetail = event.target.closest("[data-horse-detail]");
    if (horseDetail) {
      state.detailType = "horse";
      state.detailId = horseDetail.dataset.horseDetail;
      renderDetail();
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
    const sectionSearch = event.target.closest("[data-section-search]");
    if (sectionSearch) {
      const selectionStart = sectionSearch.selectionStart ?? sectionSearch.value.length;
      const selectionEnd = sectionSearch.selectionEnd ?? sectionSearch.value.length;
      state.searchBySection[sectionSearch.dataset.sectionSearch] = sectionSearch.value;
      render({
        focusSearchKey: sectionSearch.dataset.sectionSearch,
        selectionStart,
        selectionEnd
      });
      return;
    }

    const addQty = event.target.closest("[data-add-qty]");
    if (addQty) {
      state.addQty[addQty.dataset.addQty] = addQty.value;
      return;
    }

    const notes = event.target.closest("[data-action-notes]");
    if (notes) {
      state.actionNotes[notes.dataset.actionNotes] = notes.value;
      return;
    }

    const inlineEdit = event.target.closest("[data-inline-edit-field]");
    if (inlineEdit) {
      state.inlineEditValues[inlineEditKey(inlineEdit.dataset.itemId, inlineEdit.dataset.inlineEditField)] = inlineEdit.value;
    }
  }

  function toggleListInlineEdit(listId, field) {
    if (!listId || !field) return;
    state.inlineEditByList[listId] = state.inlineEditByList[listId] || {};
    state.inlineEditByList[listId][field] = !state.inlineEditByList[listId][field];
    render();
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
      const label = resolutionState === "clear" ? "clear this decision" : `set decision to ${resolutionDisplayLabel(resolutionState)}`;
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

  function render(options = {}) {
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
          <section class="lp-modal-card" role="dialog" aria-modal="true" aria-labelledby="drawerTitle" tabindex="-1">
            <button class="lp-modal-close" type="button" data-close-detail aria-label="Close detail">x</button>
            <div id="packingDetailContent" data-modal-content></div>
          </section>
        </div>
      </div>
    `;
    renderDetail();
    if (options.focusSearchKey) restoreSearchFocus(options);
  }

  function restoreSearchFocus(options) {
    const input = Array.from(root.querySelectorAll("[data-section-search]"))
      .find((element) => element.dataset.sectionSearch === options.focusSearchKey);
    if (!input) return;
    input.focus({ preventScroll: true });
    if (typeof input.setSelectionRange === "function") {
      input.setSelectionRange(options.selectionStart, options.selectionEnd);
    }
  }

  function statusLine() {
    if (state.loading) return "Loading live Airtable state";
    if (state.error) return "State unavailable";
    const wave = state.data?.wave;
    if (!wave) return "No active pack wave";
    return `${currentWaveLabel()} | departs: ${deadlineDisplay(wave.deadlineDate)} | ${daysRemainingDisplay(wave.daysTill)}`;
  }

  function currentWaveLabel() {
    return displayLabel(state.data?.wave?.wave || "wave_one");
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
          const percent = tabProgressPercent(section);
          return `
            <button class="lp-tab packing-tab ${themeClasses(section.id)} ${state.activeTab === section.id ? "is-active" : ""}" type="button" data-tab="${escapeAttr(section.id)}">
              <span class="packing-tab-percent">${percent}% PACKED</span>
              <span class="packing-tab-progress" aria-label="${escapeAttr(`${percent}% complete`)}">
                <span class="packing-tab-progress-fill" style="width: ${percent}%"></span>
              </span>
              <span class="lp-tab-label packing-tab-label">${escapeHtml(displayLabel(section.label))}</span>
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
    if (isTabGroupId(state.activeTab)) return tabGroupHtml(state.activeTab);
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
    const searchKey = "overview";
    const summaries = filterRows(tabGroups(), searchKey, overviewSearchText);
    const rows = summaries.map((summary) => {
      const percent = progressPercent(summary.done, summary.rows);
      return `
        <div class="lp-row packing-row packing-overview-row">
          <button class="packing-overview-tab-trigger" type="button" data-tab="${escapeAttr(summary.id)}">
            <span class="packing-progress" aria-label="${escapeAttr(`${percent}% complete`)}">
              <span class="packing-progress-fill" style="width: ${percent}%"></span>
            </span>
            <span class="lp-row-title">${escapeHtml(displayLabel(summary.label))}</span>
          </button>
          <button class="lp-filter-toggle packing-print-button" type="button" data-print-section="${escapeAttr(summary.id)}">PRINT LIST</button>
        </div>
      `;
    }).join("");

    return `
      <section class="lp-section-block packing-theme-overview">
        ${sectionTitleHtml(currentWaveLabel(), "overview")}
        <div class="lp-list">
          ${sectionSearchHtml(searchKey)}
          ${waveOverviewCountsHtml()}
          ${state.data.needsGeneration ? noWaveRowHtml() : rows || emptyRowHtml("No rows")}
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

  function waveOverviewCountsHtml() {
    const wave = state.data?.wave;
    if (!wave) return "";
    return `
      <div class="lp-row is-static packing-wave-count-row">
        <span class="packing-wave-counts">
          ${waveCountStatHtml("HORSE COUNT", wave.horseCount)}
          ${waveCountStatHtml("GROOM MANUAL", wave.groomCountManual)}
          ${waveCountStatHtml("GROOM RATIO", wave.groomRatio)}
          ${waveCountStatHtml("GROOM FINAL", wave.groomCountFinal)}
        </span>
      </div>
    `;
  }

  function waveCountStatHtml(label, value) {
    return `
      <span class="packing-wave-count">
        <span class="packing-wave-count-value">${escapeHtml(quantityDisplay(value))}</span>
        <span class="packing-wave-count-label">${escapeHtml(label)}</span>
      </span>
    `;
  }

  function listHtml(listId) {
    return listSectionHtml(listId);
  }

  function tabGroupHtml(tabId) {
    const group = tabGroups().find((row) => row.id === tabId);
    const groupLists = group?.listIds?.length
      ? group.listIds.map((id) => lists().find((list) => list.id === id)).filter(Boolean)
      : [];
    if (!groupLists.length) return emptyGroupHtml(group?.label || "No rows", tabId);
    const activeList = activeListForGroup(group.id, groupLists);
    return `
      <section class="lp-section-block ${themeClasses(group.id)}">
        ${sectionTitleHtml(group.label, group.id)}
        ${listSwitcherHtml(group.id, groupLists, activeList.id)}
        ${listRowsHtml(activeList, group.id)}
      </section>
    `;
  }

  function listSectionHtml(listId) {
    const list = lists().find((row) => row.id === listId) || { id: listId, label: listId };
    return `
      <section class="lp-section-block ${themeClasses(list.id)}">
        ${sectionTitleHtml(list.label, list.id)}
        ${listRowsHtml(list, list.id)}
      </section>
    `;
  }

  function listRowsHtml(list, searchKey) {
    const rows = filterRows(items().filter((item) => itemBelongsToList(item, list.id)), searchKey, itemSearchText);
    const editMode = state.inlineEditByList[list.id] || {};
    return `
      <div class="lp-list">
        ${sectionSearchHtml(searchKey)}
        ${listLabelRowHtml(list)}
        ${rows.length ? rows.map((item) => itemRowHtml(item, editMode)).join("") : emptyRowHtml("No rows")}
      </div>
    `;
  }

  function activeListForGroup(tabId, groupLists) {
    const selectedId = state.activeListByTab[tabId];
    return groupLists.find((list) => list.id === selectedId) ||
      groupLists.find((list) => number(list.open) > 0) ||
      groupLists[0];
  }

  function listSwitcherHtml(tabId, groupLists, activeListId) {
    if (groupLists.length <= 1) return "";
    return `
      <div class="packing-list-switcher" aria-label="Packing lists">
        ${groupLists.map((list) => `
          <button class="lp-filter-toggle packing-list-switch ${list.id === activeListId ? "is-active" : ""}" type="button" data-tab-id="${escapeAttr(tabId)}" data-list-switch="${escapeAttr(list.id)}">
            ${escapeHtml(displayLabel(list.label || list.id))}
          </button>
        `).join("")}
      </div>
    `;
  }

  function listLabelRowHtml(list) {
    const editMode = state.inlineEditByList[list.id] || {};
    return `
      <div class="lp-row is-static packing-list-action-row">
        <span class="lp-row-title">${escapeHtml(displayLabel(list.label || list.id))}</span>
        <span class="packing-list-action-hottext" aria-label="Inline edit fields">
          ${listEditHotText(list.id, "lp-row-title", "TITLE", editMode["lp-row-title"])}
          ${listEditHotText(list.id, "quantity_packed_override", "PACKED", editMode.quantity_packed_override)}
          ${listEditHotText(list.id, "quantity_needed_override", "NEEDED", editMode.quantity_needed_override)}
        </span>
      </div>
    `;
  }

  function listEditHotText(listId, field, label, active) {
    return `
      <span class="packing-hottext ${active ? "is-active" : ""}" role="button" tabindex="0" data-list-id="${escapeAttr(listId)}" data-list-edit-field="${escapeAttr(field)}">
        ${escapeHtml(label)}
      </span>
    `;
  }

  function emptyGroupHtml(label, printTarget) {
    return `
      <section class="lp-section-block">
        ${sectionTitleHtml(label, printTarget || "overview")}
        <div class="lp-list">
          ${emptyRowHtml("No rows")}
        </div>
      </section>
    `;
  }

  function horsesHtml() {
    const searchKey = "horses";
    const rows = filterRows(activeWaveHorses(), searchKey, horseSearchText);
    return `
      <section class="lp-section-block packing-theme-horses">
        ${sectionTitleHtml("Horses", "horses")}
        <div class="lp-list">
          ${sectionSearchHtml(searchKey)}
          <div class="lp-row is-static packing-horse-label-row">
            <span class="lp-row-title">${escapeHtml(displayLabel(`${currentWaveLabel()} horses`))}</span>
          </div>
          ${rows.length ? rows.map(horseRowHtml).join("") : emptyRowHtml("No horses")}
        </div>
      </section>
    `;
  }

  function sectionTitleHtml(title, printTarget, extraHtml) {
    return `
      <div class="lp-section-title packing-section-title">
        <h3>${escapeHtml(displayLabel(title))}</h3>
        <span class="lp-section-actions packing-section-title-actions">
          ${extraHtml || ""}
          <button class="lp-filter-toggle packing-print-button" type="button" data-print-section="${escapeAttr(printTarget)}">PRINT LIST</button>
        </span>
      </div>
    `;
  }

  function sectionSearchHtml(searchKey) {
    return `
      <div class="packing-section-search">
        <input class="lp-edit-input packing-section-search-input" type="search" placeholder="Search this section" value="${escapeAttr(sectionSearchValue(searchKey))}" data-section-search="${escapeAttr(searchKey)}">
      </div>
    `;
  }

  function sectionSearchValue(searchKey) {
    return state.searchBySection[searchKey] || "";
  }

  function filterRows(rows, searchKey, textGetter) {
    const query = sectionSearchValue(searchKey).trim().toLowerCase();
    if (!query) return rows;
    return rows.filter((row) => textGetter(row).toLowerCase().includes(query));
  }

  function overviewSearchText(summary) {
    return [summary.label, summary.id].filter(Boolean).join(" ");
  }

  function itemSearchText(item) {
    return [
      item.name,
      item.itemId,
      item.location,
      item.listPlanLabel,
      ...(Array.isArray(item.packListLabels) ? item.packListLabels : []),
      ...(Array.isArray(item.horseMembers) ? item.horseMembers.map((member) => member.barnName) : []),
      ...(Array.isArray(item.sourceItems) ? item.sourceItems.map((source) => `${source.appName || ""} ${source.longDescription || ""}`) : [])
    ].filter(Boolean).join(" ");
  }

  function horseSearchText(horse) {
    return [horse.name, horse.barnName, horse.showName, horse.notes].filter(Boolean).join(" ");
  }

  function itemRowHtml(item, editMode) {
    const hasInlineEdit = !!(editMode?.["lp-row-title"] || editMode?.quantity_packed_override || editMode?.quantity_needed_override);
    const rowTag = hasInlineEdit ? "div" : "button";
    const rowAttrs = hasInlineEdit ? "" : `type="button" data-item-id="${escapeAttr(item.id)}"`;
    return `
      <${rowTag} class="lp-row packing-row ${hasInlineEdit ? "is-inline-editing" : ""}" ${rowAttrs}>
        <span class="packing-inline-main">
          ${itemTitleHtml(item, editMode)}
          ${inlineEditFieldsHtml(item, editMode)}
        </span>
        <span class="packing-state-stack">
          ${rowTokenHtml(item)}
          <span class="packing-token-meta">Need: ${quantityDisplay(item.needed)}</span>
        </span>
      </${rowTag}>
    `;
  }

  function itemTitleHtml(item, editMode) {
    if (editMode?.["lp-row-title"]) {
      return `<input class="lp-edit-input packing-inline-title-input" type="text" value="${escapeAttr(inlineEditValue(item, "lp-row-title"))}" data-item-id="${escapeAttr(item.id)}" data-inline-edit-field="lp-row-title">`;
    }
    return `<span class="lp-row-title">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</span>`;
  }

  function inlineEditFieldsHtml(item, editMode) {
    const fields = [];
    if (editMode?.quantity_packed_override) fields.push(inlineQuantityInputHtml(item, "quantity_packed_override", "PACKED"));
    if (editMode?.quantity_needed_override) fields.push(inlineQuantityInputHtml(item, "quantity_needed_override", "NEEDED"));
    if (!fields.length) return "";
    return `<span class="packing-inline-edit-fields">${fields.join("")}</span>`;
  }

  function inlineQuantityInputHtml(item, field, label) {
    return `
      <span class="packing-inline-edit-box">
        <input class="lp-edit-input packing-inline-quantity-input" type="number" min="0" step="1" inputmode="numeric" value="${escapeAttr(inlineEditValue(item, field))}" data-item-id="${escapeAttr(item.id)}" data-inline-edit-field="${escapeAttr(field)}">
        <span class="packing-inline-edit-label">${escapeHtml(label)}</span>
      </span>
    `;
  }

  function horseRowHtml(horse) {
    const progress = horseProgress(horse);
    return `
      <div class="lp-row packing-row packing-horse-row" data-horse-detail="${escapeAttr(horse.id)}">
        <span class="packing-overview-tab-trigger packing-horse-detail-trigger">
          <span class="packing-progress" aria-label="${escapeAttr(`${progress.percent}% packed`)}">
            <span class="packing-progress-fill" style="width: ${progress.percent}%"></span>
          </span>
          <span class="lp-row-title">${escapeHtml(horseDisplayName(horse))}</span>
        </span>
        <button class="lp-filter-toggle packing-print-button" type="button" data-print-horse="${escapeAttr(horse.id)}">PRINT LIST</button>
      </div>
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
    if (item.resolutionState) return tokenHtml("resolved", resolutionDisplayLabel(item.resolutionState));
    if (item.packState === "packed" || (number(item.left) === 0 && number(item.needed) > 0)) return tokenHtml("packed", "PACKED");
    if (number(item.packed) > 0) return tokenHtml("open", `LEFT - ${quantityDisplay(item.left)}`);
    return tokenHtml("need", `NEED - ${quantityDisplay(item.needed)}`);
  }

  function tokenHtml(type, text) {
    return `<span class="lp-achievement packing-token is-${escapeAttr(type)}">${escapeHtml(text)}</span>`;
  }

  function printSection(target) {
    if (!state.data) return;
    const title = target === "overview" ? "WEC Packing Report" : `${printTargetTitle(target)} Packing List`;
    const printWindow = window.open("", "_blank");
    if (!printWindow) {
      state.saveMessage = "Allow pop-ups to print this list.";
      render();
      return;
    }

    printWindow.document.open();
    printWindow.document.write(printDocumentHtml(title, printBodyHtml(target)));
    printWindow.document.close();
    printWindow.focus();
    printWindow.setTimeout(() => {
      printWindow.print();
    }, 250);
  }

  function printHorseList(horseId) {
    if (!state.data) return;
    const horse = horses().find((row) => row.id === horseId);
    if (!horse) return;
    const title = `${horseDisplayName(horse)} Packing List`;
    const printWindow = window.open("", "_blank");
    if (!printWindow) {
      state.saveMessage = "Allow pop-ups to print this list.";
      render();
      return;
    }

    printWindow.document.open();
    printWindow.document.write(printDocumentHtml(title, printHorsePackingPageHtml(horse)));
    printWindow.document.close();
    printWindow.focus();
    printWindow.setTimeout(() => {
      printWindow.print();
    }, 250);
  }

  function printBodyHtml(target) {
    if (target === "overview") {
      const pages = tabGroups().map((group) => printPackingPageHtml(group.label, printListSections(group.id))).join("");
      return `${pages}${printHorsesPageHtml()}`;
    }
    if (target === "horses") return printHorsesPageHtml();
    return printPackingPageHtml(printTargetTitle(target), printListSections(target));
  }

  function printTargetTitle(target) {
    if (target === "horses") return "Horses";
    if (isTabGroupId(target)) {
      return displayLabel(tabGroups().find((group) => group.id === target)?.label || target.replace(/^tab:/, ""));
    }
    return displayLabel(lists().find((list) => list.id === target)?.label || target);
  }

  function printListSections(target) {
    if (isTabGroupId(target)) {
      const group = tabGroups().find((row) => row.id === target);
      return (group?.listIds || []).map(printListSection).filter(Boolean);
    }
    return [printListSection(target)].filter(Boolean);
  }

  function printListSection(listId) {
    const list = lists().find((row) => row.id === listId) || { id: listId, label: listId };
    const rows = items().filter((item) => itemBelongsToList(item, list.id));
    return {
      title: displayLabel(list.label || list.id),
      rows
    };
  }

  function printPackingPageHtml(title, sections) {
    const rows = sections.flatMap((section) => section.rows);
    const percent = progressPercent(rows.filter(isDone).length, rows.length);
    return `
      <section class="packing-print-page">
        <header class="packing-print-head">
          <h1>${escapeHtml(displayLabel(title))}</h1>
          <p>${escapeHtml(statusLine())} | ${percent}% complete | ${escapeHtml(new Date().toLocaleDateString())}</p>
        </header>
        <div class="packing-print-columns">
          ${sections.length ? sections.map(printListColumnHtml).join("") : printEmptyPrintSectionHtml("No rows")}
        </div>
      </section>
    `;
  }

  function printListColumnHtml(section) {
    return `
      <section class="packing-print-list">
        <h2>${escapeHtml(section.title)}</h2>
        ${section.rows.length ? section.rows.map(printItemRowHtml).join("") : printEmptyPrintSectionHtml("No rows")}
      </section>
    `;
  }

  function printItemRowHtml(item) {
    const horseNames = item.horseMembers?.map((member) => member.barnName).filter(Boolean).join(", ");
    return `
      <div class="packing-print-item">
        <div class="packing-print-item-main">
          <strong>${escapeHtml(displayLabel(item.name || "Unnamed item"))}</strong>
          <span>Need: ${escapeHtml(quantityDisplay(item.needed))} | Packed: ${escapeHtml(quantityDisplay(item.packed))} | Left: ${escapeHtml(quantityDisplay(item.left))}</span>
          ${horseNames ? `<em>${escapeHtml(horseNames)}</em>` : ""}
        </div>
        <b>${escapeHtml(printItemStatus(item))}</b>
      </div>
    `;
  }

  function printItemStatus(item) {
    if (item.resolutionState) return resolutionDisplayLabel(item.resolutionState);
    if (item.packState === "packed" || (number(item.left) === 0 && number(item.needed) > 0)) return "PACKED";
    if (number(item.packed) > 0) return `LEFT - ${quantityDisplay(item.left)}`;
    return `NEED - ${quantityDisplay(item.needed)}`;
  }

  function printHorsesPageHtml() {
    const rows = activeWaveHorses();
    const columns = splitRows(rows);
    const percent = horsePackingPercent();
    return `
      <section class="packing-print-page">
        <header class="packing-print-head">
          <h1>Horses</h1>
          <p>${escapeHtml(statusLine())} | ${percent}% packed | ${escapeHtml(new Date().toLocaleDateString())}</p>
        </header>
        <div class="packing-print-columns">
          ${printHorseColumnHtml("Horses", columns[0])}
          ${printHorseColumnHtml("Horses", columns[1])}
        </div>
      </section>
    `;
  }

  function printHorsePackingPageHtml(horse) {
    const rows = horseItemRows(horse);
    const columns = splitRows(rows);
    const progress = horseProgress(horse);
    return `
      <section class="packing-print-page">
        <header class="packing-print-head">
          <h1>${escapeHtml(horseDisplayName(horse))}</h1>
          <p>${escapeHtml(currentWaveLabel())} | ${progress.percent}% packed | ${escapeHtml(new Date().toLocaleDateString())}</p>
        </header>
        <div class="packing-print-columns">
          ${printHorsePackingColumnHtml("Items", columns[0])}
          ${printHorsePackingColumnHtml("Items", columns[1])}
        </div>
      </section>
    `;
  }

  function printHorsePackingColumnHtml(title, rows) {
    return `
      <section class="packing-print-list">
        <h2>${escapeHtml(title)}</h2>
        ${rows.length ? rows.map(printHorsePackingItemHtml).join("") : printEmptyPrintSectionHtml("No rows")}
      </section>
    `;
  }

  function printHorsePackingItemHtml(row) {
    const needed = number(row.member.needed);
    const packed = number(row.member.packed);
    const left = Math.max(0, needed - packed);
    return `
      <div class="packing-print-item">
        <div class="packing-print-item-main">
          <strong>${escapeHtml(displayLabel(row.item.name || "Unnamed item"))}</strong>
          <span>Need: ${escapeHtml(quantityDisplay(needed))} | Packed: ${escapeHtml(quantityDisplay(packed))} | Left: ${escapeHtml(quantityDisplay(left))}</span>
        </div>
        <b>${escapeHtml(isHorseMemberPacked(row.member) ? "PACKED" : `LEFT - ${quantityDisplay(left || needed)}`)}</b>
      </div>
    `;
  }

  function printHorseColumnHtml(title, rows) {
    return `
      <section class="packing-print-list">
        <h2>${escapeHtml(title)}</h2>
        ${rows.length ? rows.map((horse) => `
          <div class="packing-print-horse">
            <strong>${escapeHtml(horse.name || horse.showName || "Unnamed horse")}</strong>
          </div>
        `).join("") : printEmptyPrintSectionHtml("No horses")}
      </section>
    `;
  }

  function splitRows(rows) {
    const middle = Math.ceil(rows.length / 2);
    return [rows.slice(0, middle), rows.slice(middle)];
  }

  function printEmptyPrintSectionHtml(label) {
    return `<div class="packing-print-empty">${escapeHtml(label)}</div>`;
  }

  function printDocumentHtml(title, body) {
    return `<!doctype html>
      <html>
        <head>
          <meta charset="utf-8">
          <title>${escapeHtml(title)}</title>
          <style>${printStyles()}</style>
        </head>
        <body>${body}</body>
      </html>`;
  }

  function printStyles() {
    return `
      @import url("https://fonts.googleapis.com/css2?family=Outfit:wght@400;600&display=swap");
      @page { size: Letter; margin: 0.35in; }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        background: #ffffff;
        color: #000000;
        font-family: "Outfit", ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-size: 10px;
        line-height: 1.12;
      }
      .packing-print-page {
        width: 100%;
        min-height: 10.3in;
        break-after: page;
      }
      .packing-print-page:last-child { break-after: auto; }
      .packing-print-head {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 0.2in;
        padding-bottom: 0.12in;
        border-bottom: 2px solid #000000;
        margin-bottom: 0.16in;
      }
      .packing-print-head h1 {
        margin: 0;
        font-size: 24px;
        font-weight: 600;
        line-height: 0.95;
      }
      .packing-print-head p {
        margin: 0;
        font-size: 9px;
        font-weight: 600;
        text-transform: uppercase;
        white-space: nowrap;
      }
      .packing-print-columns {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.16in;
        align-items: start;
      }
      .packing-print-list {
        border: 1px solid #d9d9d9;
        border-radius: 8px;
        overflow: hidden;
        break-inside: avoid;
        background: #ffffff;
      }
      .packing-print-list h2 {
        margin: 0;
        padding: 8px 10px;
        background: #f0f0f0;
        border-bottom: 1px solid #d9d9d9;
        font-size: 12px;
        font-weight: 600;
        line-height: 1;
        text-transform: uppercase;
      }
      .packing-print-item,
      .packing-print-horse {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 8px;
        align-items: start;
        padding: 7px 10px;
        border-bottom: 1px solid #eeeeee;
      }
      .packing-print-item:last-child,
      .packing-print-horse:last-child { border-bottom: 0; }
      .packing-print-item-main {
        display: grid;
        gap: 3px;
        min-width: 0;
      }
      .packing-print-item strong,
      .packing-print-horse strong {
        font-size: 11px;
        font-weight: 600;
        line-height: 1;
        text-transform: uppercase;
      }
      .packing-print-item span,
      .packing-print-item em {
        color: #333333;
        font-style: normal;
        font-size: 9px;
        line-height: 1.1;
      }
      .packing-print-item b,
      .packing-print-horse b {
        padding: 4px 7px;
        border-radius: 999px;
        background: #46332b;
        color: #ffffff;
        font-size: 8px;
        font-weight: 600;
        line-height: 1;
        text-transform: uppercase;
        white-space: nowrap;
      }
      .packing-print-empty {
        padding: 10px;
        color: #333333;
        font-size: 10px;
        font-weight: 600;
        text-transform: uppercase;
      }
    `;
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
      <div class="lp-profile-shell packing-detail-shell ${themeClasses(item.packListIds?.[0] || "overview")}">
        <div class="lp-profile-head th-profile-top">
          <h2 class="lp-profile-title" id="drawerTitle">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</h2>
          <p class="lp-profile-subtitle">${escapeHtml(itemMetaLabel(item))}</p>
        </div>

        <section class="lp-profile-panel packing-detail th-detail-section">
          <div data-th-record="${escapeAttr(item.id)}" data-th-name="${escapeAttr(item.name || "")}">
            <div class="lp-field-grid lp-profile-tab-panel is-active">
              ${statusControlHtml(item)}
              ${totalsRowHtml(item)}
              ${packedControlHtml(item)}
              ${decisionControlHtml(item)}
              ${horseMembersControlHtml(item)}
            </div>
          </div>
        </section>

        <div class="lp-profile-modal-footer th-profile-footer">
          <div class="lp-profile-footer">
            <span>${escapeHtml(state.saveMessage || "Changes save to Airtable through Webflow Cloud.")}</span>
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

  function detailReadRow(label, value) {
    return editGroupHtml(label, `<input class="lp-edit-input th-input th-readonly-input" type="text" value="${escapeAttr(value)}" readonly tabindex="-1">`);
  }

  function totalsRowHtml(item) {
    return editGroupHtml("Totals", `
      <span class="packing-totals">
        ${totalBoxHtml("need", item.needed)}
        ${totalBoxHtml("packed", item.packed)}
        ${totalBoxHtml("left", item.left)}
      </span>
    `, "packing-totals-row");
  }

  function totalBoxHtml(label, value) {
    return `
      <span class="packing-total-box">
        <span class="packing-total-value">${escapeHtml(quantityDisplay(value))}</span>
        <span class="packing-total-label">${escapeHtml(label)}</span>
      </span>
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
        <span class="packing-add-box">
          <input class="lp-edit-input" type="number" min="0" step="1" inputmode="numeric" placeholder="0" value="${escapeAttr(state.addQty[item.id] || "")}" data-add-qty="${escapeAttr(item.id)}">
          <span class="packing-add-label">QUANTITY</span>
        </span>
        <button class="lp-edit-pill" type="button" data-packing-action="add_quantity" data-item-id="${escapeAttr(item.id)}">ADD</button>
      </span>
    `, "packing-add-row");
  }

  function horseMembersControlHtml(item) {
    if (!item.horseMembers?.length) return "";
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
    return editGroupHtml("Notes", `<textarea class="lp-edit-input th-input th-note-input" rows="4" data-action-notes="${escapeAttr(item.id)}">${escapeHtml(value)}</textarea>`, "th-detail-note");
  }

  function decisionControlHtml(item) {
    const maxFlowActive = isMaxConflictState(item.resolutionState);
    return editGroupHtml("Decision", `
      <span class="lp-edit-choice-row packing-inline-choices packing-decision-choices">
        ${choiceButtonHtml({
          label: "MAX",
          active: maxFlowActive,
          attrs: `data-packing-action="set_resolution" data-item-id="${escapeAttr(item.id)}" data-resolution-state="max"`
        })}
      </span>
    `) + (maxFlowActive ? maxConflictControlHtml(item) : "");
  }

  function maxConflictControlHtml(item) {
    const decisions = ["kill", "purchase_onsite", "unresolved"];
    return editGroupHtml("Conflict", `
      <span class="lp-edit-choice-row packing-inline-choices packing-decision-choices packing-conflict-choices">
        ${decisions.map((decision) => choiceButtonHtml({
          label: resolutionDisplayLabel(decision),
          active: item.resolutionState === decision,
          attrs: `data-packing-action="set_resolution" data-item-id="${escapeAttr(item.id)}" data-resolution-state="${escapeAttr(decision)}"`
        })).join("")}
        <a class="lp-edit-pill packing-sms-pill" href="${escapeAttr(smsConflictHref(item))}">SMS</a>
      </span>
    `, "packing-conflict-row");
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
    return `<span class="lp-edit-choice"><button class="lp-edit-pill ${active ? "is-active" : ""}" type="button" ${attrs}>${escapeHtml(label)}</button></span>`;
  }

  function editGroupHtml(title, body, extraClass) {
    return `
      <div class="lp-field-row th-detail-edit ${extraClass || ""}">
        <span class="lp-field-label">${escapeHtml(title)}</span>
        <span class="lp-field-value">${body}</span>
      </div>
    `;
  }

  function detailGroupHtml(title, rows, extraClass) {
    return `
      <h4 class="packing-detail-group-title">${escapeHtml(title)}</h4>
      <div class="lp-detail-list ${extraClass || ""}">
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

  function progressPercent(done, rows) {
    const total = number(rows);
    if (total <= 0) return 0;
    return Math.max(0, Math.min(100, Math.round((number(done) / total) * 100)));
  }

  function tabProgressPercent(section) {
    if (section.id === "horses") {
      return horsePackingPercent();
    }
    if (section.id === "overview") return progressPercent(doneRows(), totalRows());
    return progressPercent(section.done, section.rows ?? sectionCount(section.id));
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

  function activeWaveHorses() {
    const members = horseMemberRows();
    if (!members.length) {
      return horses().filter((horse) => horse.active || String(horse.recordState || "").toLowerCase() === "active");
    }

    const horseIds = new Set();
    const horseKeys = new Set();
    const sortByHorseId = new Map();
    const sortByHorseKey = new Map();

    for (const member of members) {
      const sortOrder = number(member.sortOrder);
      if (Array.isArray(member.horseIds)) {
        for (const horseId of member.horseIds) {
          horseIds.add(horseId);
          if (!sortByHorseId.has(horseId)) sortByHorseId.set(horseId, sortOrder);
        }
      }
      const horseKey = themeKey(member.barnName);
      if (horseKey) {
        horseKeys.add(horseKey);
        if (!sortByHorseKey.has(horseKey)) sortByHorseKey.set(horseKey, sortOrder);
      }
    }

    return horses()
      .filter((horse) => horseIds.has(horse.id) || horseKeys.has(themeKey(horseDisplayName(horse))))
      .map((horse) => ({
        ...horse,
        waveSortOrder: sortByHorseId.get(horse.id) ?? sortByHorseKey.get(themeKey(horseDisplayName(horse))) ?? number(horse.sortOrder)
      }))
      .sort((a, b) => number(a.waveSortOrder) - number(b.waveSortOrder) || horseDisplayName(a).localeCompare(horseDisplayName(b)));
  }

  function horsePackingPercent() {
    const rows = horseMemberRows();
    return progressPercent(rows.filter(isHorseMemberPacked).length, rows.length);
  }

  function horseMemberRows() {
    return items()
      .flatMap((item) => Array.isArray(item.horseMembers) ? item.horseMembers : [])
      .filter(horseMemberBelongsToCurrentWave);
  }

  function horseMemberBelongsToCurrentWave(member) {
    const waveId = currentWaveId();
    if (!waveId || !Array.isArray(member.packWaveIds) || !member.packWaveIds.length) return true;
    return member.packWaveIds.includes(waveId);
  }

  function horseProgress(horse) {
    const rows = horseItemRows(horse);
    return {
      done: rows.filter((row) => isHorseMemberPacked(row.member)).length,
      rows: rows.length,
      percent: progressPercent(rows.filter((row) => isHorseMemberPacked(row.member)).length, rows.length)
    };
  }

  function horseItemRows(horse) {
    return items().flatMap((item) => {
      const members = Array.isArray(item.horseMembers) ? item.horseMembers : [];
      return members
        .filter(horseMemberBelongsToCurrentWave)
        .filter((member) => horseMemberBelongsToHorse(member, horse))
        .map((member) => ({ item, member }));
    });
  }

  function horseMemberBelongsToHorse(member, horse) {
    if (!member || !horse) return false;
    if (Array.isArray(member.horseIds) && member.horseIds.includes(horse.id)) return true;
    return themeKey(member.barnName) === themeKey(horseDisplayName(horse));
  }

  function isHorseMemberPacked(member) {
    return member.horsePackState === "packed" || number(member.packed) >= number(member.needed);
  }

  function horseDisplayName(horse) {
    return horse.name || horse.barnName || horse.showName || "Unnamed horse";
  }

  function currentWaveId() {
    return state.data?.wave?.id || state.data?.wave?.packWaveId || "";
  }

  function lists() {
    if (Array.isArray(state.data?.lists) && state.data.lists.length) return state.data.lists;
    if (Array.isArray(state.data?.sections)) {
      return state.data.sections.map((section) => ({
        id: section.section,
        label: section.label || section.section,
        tabs: [],
        rows: section.rows,
        done: section.done,
        open: section.open
      }));
    }
    return [];
  }

  function tabGroups() {
    if (Array.isArray(state.data?.tabGroups) && state.data.tabGroups.length) {
      return state.data.tabGroups.map(normalizeTabGroup).filter((group) => group.listIds.length);
    }

    const groups = new Map();
    for (const list of lists()) {
      for (const label of listTabLabels(list)) {
        const key = themeKey(label || list.label || list.id);
        const id = `tab:${key}`;
        const group = groups.get(id) || {
          id,
          key,
          label: label || list.label || list.id,
          listIds: [],
          rows: 0,
          done: 0,
          open: 0
        };
        group.listIds.push(list.id);
        group.rows += number(list.rows);
        group.done += number(list.done);
        group.open += number(list.open);
        groups.set(id, group);
      }
    }
    return [...groups.values()];
  }

  function normalizeTabGroup(group) {
    const key = themeKey(group.key || group.label || group.id);
    const id = group.id || `tab:${key}`;
    const explicitListIds = Array.isArray(group.listIds) ? group.listIds : [];
    const inferredListIds = explicitListIds.length
      ? explicitListIds
      : lists()
        .filter((list) => listTabLabels(list).some((label) => `tab:${themeKey(label)}` === id))
        .map((list) => list.id);
    return {
      id,
      key,
      label: group.label || group.key || id.replace(/^tab:/, ""),
      listIds: inferredListIds,
      rows: number(group.rows),
      done: number(group.done),
      open: number(group.open)
    };
  }

  function listTabLabels(list) {
    if (Array.isArray(list.tabs) && list.tabs.length) return list.tabs;
    if (typeof list.tabs === "string" && list.tabs.trim()) return [list.tabs.trim()];
    if (list.tabLabel) return [list.tabLabel];
    return [list.label || list.id];
  }

  function tabs() {
    return [
      { id: "overview", label: currentWaveLabel() },
      { id: "horses", label: "Horses" },
      ...tabGroups()
    ];
  }

  function isTabGroupId(value) {
    return String(value || "").startsWith("tab:");
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
    return `${quantityDisplay(value)}${suffix}`;
  }

  function number(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
  }

  function quantityDisplay(value) {
    const numeric = number(value);
    if (numeric <= 0) return "0";
    const cleaned = Math.abs(numeric - Math.round(numeric)) < 0.000001
      ? Math.round(numeric)
      : Math.ceil(numeric - 0.000001);
    return String(cleaned);
  }

  function deadlineDisplay(value) {
    if (value === null || value === undefined || value === "") return "Not set";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleDateString();
  }

  function daysRemainingDisplay(value) {
    if (value === null || value === undefined || value === "") return "days remaining not set";
    return `${quantityDisplay(value)} days remaining`;
  }

  function inlineEditKey(itemId, field) {
    return `${itemId || ""}:${field || ""}`;
  }

  function inlineEditValue(item, field) {
    const key = inlineEditKey(item.id, field);
    if (Object.prototype.hasOwnProperty.call(state.inlineEditValues, key)) return state.inlineEditValues[key];
    if (field === "lp-row-title") return displayLabel(item.name || "");
    if (field === "quantity_packed_override") {
      return item.quantityPackedOverride ?? item.quantity_packed_override ?? item.packed ?? "";
    }
    if (field === "quantity_needed_override") {
      return item.quantityNeededOverride ?? item.quantity_needed_override ?? item.needed ?? "";
    }
    return "";
  }

  function isMaxConflictState(value) {
    return ["max", "kill", "purchase_onsite", "unresolved"].includes(String(value || ""));
  }

  function resolutionDisplayLabel(value) {
    if (value === "kill") return "REMOVE";
    if (value === "purchase_onsite") return "ONSITE";
    if (value === "unresolved") return "UNRESOLVED";
    return displayLabel(value).toUpperCase();
  }

  function smsConflictHref(item) {
    const body = [
      `WEC Packing Conflict: ${displayLabel(item.name || "Unnamed item")}`,
      `NEED: ${quantityDisplay(item.needed)}, PACKED: ${quantityDisplay(item.packed)}, LEFT ${quantityDisplay(item.left)}`,
      "reply",
      "REMOVE | ONSITE | UNRESOLVED"
    ].join("\n");
    return `sms:?&body=${encodeURIComponent(body)}`;
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

  function themeClasses(value) {
    const key = themeName(value);
    if (key === "overview") return "packing-theme-overview";
    if (key === "horses") return "packing-theme-horses";
    return `packing-group-${key} packing-tone-${toneIndex(value)}`;
  }

  function themeName(value) {
    const raw = String(value || "overview");
    if (raw === "overview" || raw === "horses") return raw;
    if (isTabGroupId(raw)) {
      const group = tabGroups().find((row) => row.id === raw);
      return themeKey(group?.label || raw.replace(/^tab:/, ""));
    }
    const list = lists().find((row) => row.id === raw);
    if (list) return themeKey(listTabLabels(list)[0] || list.label || list.key || raw);
    return themeKey(raw.replace(/^tab:/, ""));
  }

  function toneIndex(value) {
    const groups = tabGroups();
    const groupIndex = isTabGroupId(value)
      ? groups.findIndex((group) => group.id === value)
      : groups.findIndex((group) => group.listIds.includes(value));
    if (groupIndex >= 0) return groupIndex % 10;
    const index = lists().findIndex((list) => list.id === value);
    return index >= 0 ? index % 10 : 0;
  }
})();
