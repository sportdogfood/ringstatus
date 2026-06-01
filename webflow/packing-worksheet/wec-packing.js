(function () {
  const root = document.getElementById("packing-app");
  if (!root) return;

  const config = window.WEC_PACKING_CONFIG || {};
  const apiUrl = String(config.apiUrl || "").trim();
  const defaultApiBaseUrl = window.location.hostname === "ringstatus.com" || window.location.hostname === "www.ringstatus.com"
    ? "https://ringstatus.com/test/wec-packing"
    : "https://ringstatus.webflow.io/test/wec-packing";
  const apiBaseUrl = String(config.apiBaseUrl || defaultApiBaseUrl).replace(/\/$/, "");
  const defaultPrintPageUrl = ["127.0.0.1", "localhost", "::1"].includes(window.location.hostname)
    ? "./wec-packing-print-preview.html"
    : "https://ringstatus.com/wec-packing-print";
  const printPageUrl = String(config.printPageUrl || defaultPrintPageUrl).trim();
  const pdfWorkerUrl = String(config.pdfWorkerUrl || "https://ringstatus-pdf.gombcg.workers.dev/").trim();
  const failedActionStorageKey = "wecPackingFailedActions:v1";
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
    activeHomeModule: "",
    activeOverviewListId: "",
    activeOverviewListSummary: null,
    activeListByTab: {},
    searchBySection: {},
    activeToolByList: {},
    filterByList: {},
    sortByList: {},
    inlineEditByList: {},
    inlineEditByItem: {},
    inlineEditValues: {},
    pendingActions: {},
    addQty: {},
    actionNotes: {},
    commentDraftByScope: {},
    commentEditById: {},
    commentEditValues: {},
    decisionOpenByItem: {},
    failedActions: loadFailedActions(),
    retryingFailedActions: false,
    sessionEventSent: false
  };

  root.classList.toggle("is-edit-mode", config.mode === "edit");
  root.addEventListener("click", handleClick);
  root.addEventListener("input", handleInput);
  window.addEventListener("hashchange", handleHashRoute);
  window.addEventListener("online", () => retryFailedActions({ silent: false }));
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
      state.data = normalizeStatePayload(payload);
      if (!state.didSetInitialTab) {
        state.activeTab = "overview";
        state.didSetInitialTab = true;
      }
      queueSessionStartEvent();
      if (state.failedActions.length) retryFailedActions({ silent: true });
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
    addContextParams(url);
    return url.toString();
  }

  function addContextParams(url) {
    if (config.showId) url.searchParams.set("showId", config.showId);
    if (config.packWaveId) url.searchParams.set("packWaveId", config.packWaveId);
    if (config.packWaveKey || config.packWave) url.searchParams.set("packWaveKey", config.packWaveKey || config.packWave);
  }

  function queueSessionStartEvent() {
    if (state.sessionEventSent || !state.data?.ok) return;
    const session = currentSession();
    if (!session.isNew) return;
    state.sessionEventSent = true;
    fetch(endpointUrl("action"), {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        action: "session_start",
        sessionId: session.id,
        clientUrl: window.location.href
      })
    }).catch(() => {});
  }

  function currentSession() {
    const key = "wecPackingSessionId";
    try {
      const existing = window.sessionStorage.getItem(key);
      if (existing) return { id: existing, isNew: false };
      const id = createSessionId();
      window.sessionStorage.setItem(key, id);
      return { id, isNew: true };
    } catch (error) {
      if (!state.sessionId) state.sessionId = createSessionId();
      return { id: state.sessionId, isNew: true };
    }
  }

  function createSessionId() {
    const random = Math.random().toString(36).slice(2, 10);
    return `wec_${Date.now().toString(36)}_${random}`;
  }

  function normalizeStatePayload(payload) {
    if (!payload || !Array.isArray(payload.items)) return payload;
    const countsLocked = !!payload.wave?.countsLocked;
    return {
      ...payload,
      items: payload.items.map((item) => normalizeStateItem(item, countsLocked))
    };
  }

  function normalizeStateItem(item, countsLocked) {
    if (!item || countsLocked) return item;
    const calculation = item.quantityCalculation || {};
    const plan = themeKey(calculation.plan || item.listPlan);
    const calculatedPlans = ["per_groom", "per_horse", "horse_specific", "horse-specific", "quantity"];
    if (!calculatedPlans.includes(plan) || calculation.calculatedNeeded === undefined) return item;
    const needed = wholeQuantityNumber(calculation.calculatedNeeded);
    const packed = wholeQuantityNumber(item.packed);
    const left = Math.max(0, needed - packed);
    return {
      ...item,
      needed,
      left,
      packState: needed > 0 && packed >= needed ? "packed" : item.packState,
      quantityCalculation: {
        ...calculation,
        appliedNeeded: needed
      }
    };
  }

  function handleClick(event) {
    const close = event.target.closest("[data-close-detail]");
    if (close) {
      closeDetail();
      return;
    }

    const failedRetry = event.target.closest("[data-rsa-retry-failed]");
    if (failedRetry) {
      event.preventDefault();
      retryFailedActions({ silent: false });
      return;
    }

    const failedExport = event.target.closest("[data-rsa-export-failed]");
    if (failedExport) {
      event.preventDefault();
      exportFailedActions();
      return;
    }

    const packingAction = event.target.closest("[data-packing-action]");
    if (packingAction) {
      event.preventDefault();
      runPackingAction(packingAction);
      return;
    }

    const decisionOpen = event.target.closest("[data-decision-open]");
    if (decisionOpen) {
      event.preventDefault();
      toggleDecisionOptions(decisionOpen.dataset.decisionOpen);
      return;
    }

    const onsiteTaskAction = event.target.closest("[data-onsite-task-state]");
    if (onsiteTaskAction) {
      event.preventDefault();
      setOnsiteTaskState(onsiteTaskAction);
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

    const homeModule = event.target.closest("[data-home-module]");
    if (homeModule) {
      event.preventDefault();
      state.activeHomeModule = homeModule.dataset.homeModule;
      state.activeOverviewListId = "";
      state.activeOverviewListSummary = null;
      state.activeTab = "overview";
      state.detailType = "";
      state.detailId = "";
      render();
      return;
    }

    const overviewList = event.target.closest("[data-overview-list]");
    if (overviewList) {
      event.preventDefault();
      openOverviewList(overviewList.dataset.overviewList || "");
      return;
    }

    const listSwitch = event.target.closest("[data-list-switch]");
    if (listSwitch) {
      event.preventDefault();
      state.activeListByTab[listSwitch.dataset.tabId] = listSwitch.dataset.listSwitch;
      render();
      return;
    }

    const toolToggle = event.target.closest("[data-rsa-toggle]");
    if (toolToggle) {
      event.preventDefault();
      const key = toolToggle.dataset.rsaScope || state.activeTab || "overview";
      const nextTool = toolToggle.dataset.rsaToggle;
      state.activeToolByList[key] = state.activeToolByList[key] === nextTool ? "" : nextTool;
      render();
      return;
    }

    const toolClose = event.target.closest("[data-rsa-close]");
    if (toolClose) {
      event.preventDefault();
      const key = toolClose.dataset.rsaScope || state.activeTab || "overview";
      state.activeToolByList[key] = "";
      render();
      return;
    }

    const filterToggle = event.target.closest("[data-rsa-filter]");
    if (filterToggle) {
      event.preventDefault();
      const key = filterToggle.dataset.rsaScope || state.activeTab || "overview";
      state.filterByList[key] = filterToggle.dataset.rsaFilter || "all";
      if (key === "overview") {
        state.activeHomeModule = "";
        state.activeOverviewListId = "";
        state.activeOverviewListSummary = null;
      }
      render();
      return;
    }

    const sortToggle = event.target.closest("[data-rsa-sort]");
    if (sortToggle) {
      event.preventDefault();
      const key = sortToggle.dataset.rsaScope || state.activeTab || "overview";
      const field = sortToggle.dataset.rsaSort || "";
      const current = state.sortByList[key] || {};
      const firstDirection = field === "left" ? "desc" : "asc";
      const direction = current.field === field ? (current.direction === "asc" ? "desc" : "asc") : firstDirection;
      state.sortByList[key] = { field, direction };
      render();
      return;
    }

    const commentAdd = event.target.closest("[data-rsa-comment-add]");
    if (commentAdd) {
      event.preventDefault();
      addScopedComment(commentAdd);
      return;
    }

    const commentSave = event.target.closest("[data-rsa-comment-save]");
    if (commentSave) {
      event.preventDefault();
      saveScopedComment(commentSave);
      return;
    }

    const commentEdit = event.target.closest("[data-rsa-comment-edit]");
    if (commentEdit) {
      event.preventDefault();
      toggleScopedCommentEdit(commentEdit);
      return;
    }

    const listEdit = event.target.closest("[data-list-edit-field]");
    if (listEdit) {
      event.preventDefault();
      if (listEdit.dataset.itemId) {
        toggleItemInlineEdit(listEdit.dataset.itemId, listEdit.dataset.listEditField);
      } else {
        toggleListInlineEdit(listEdit.dataset.listId, listEdit.dataset.listEditField);
      }
      return;
    }

    const inlineSave = event.target.closest("[data-inline-save-item]");
    if (inlineSave) {
      event.preventDefault();
      saveInlineEdit(inlineSave);
      return;
    }

    if (event.target.closest("[data-inline-edit-field]")) {
      return;
    }

    const tab = event.target.closest("[data-tab]");
    if (tab) {
      state.activeTab = tab.dataset.tab;
      if (state.activeTab === "overview") state.activeHomeModule = "";
      if (state.activeTab === "overview") state.activeOverviewListId = "";
      if (state.activeTab === "overview") state.activeOverviewListSummary = null;
      state.detailType = "";
      state.detailId = "";
      render();
      return;
    }

    const onsiteDetail = event.target.closest("[data-onsite-task-detail]");
    if (onsiteDetail) {
      state.detailType = "onsite";
      state.detailId = onsiteDetail.dataset.onsiteTaskDetail;
      renderDetail();
      return;
    }

    const horseDetail = event.target.closest("[data-horse-detail]");
    if (horseDetail) {
      state.detailType = "horse";
      state.detailId = horseDetail.dataset.horseDetail;
      renderDetail();
      return;
    }

    const placeDetail = event.target.closest("[data-place-detail]");
    if (placeDetail) {
      state.detailType = "place";
      state.detailId = placeDetail.dataset.placeDetail;
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

  function openOverviewList(listId) {
    const summary = activeLaneListById(listId);
    const detail = activeListDetailById(listId);
    const storedSummary = summary || (detail ? {
      id: detail.id,
      key: detail.key,
      lane: detail.lane,
      label: detail.label,
      sourceTable: detail.sourceTable,
      sourceView: detail.sourceView,
      rows: detail.rows
    } : null);
    state.activeTab = "overview";
    state.detailType = "";
    state.detailId = "";
    if (summary?.lane || detail?.lane) state.filterByList.overview = summary?.lane || detail?.lane;
    if (summary?.homeModuleId) {
      state.activeHomeModule = summary.homeModuleId;
      state.activeOverviewListId = "";
      state.activeOverviewListSummary = null;
    } else {
      state.activeHomeModule = "";
      state.activeOverviewListId = listId;
      state.activeOverviewListSummary = storedSummary;
    }
    render();
  }

  function handleInput(event) {
    const sectionSearch = event.target.closest("[data-section-search]");
    if (sectionSearch) {
      const value = sectionSearch.value ?? sectionSearch.textContent ?? "";
      const selectionStart = sectionSearch.selectionStart ?? value.length;
      const selectionEnd = sectionSearch.selectionEnd ?? value.length;
      state.searchBySection[sectionSearch.dataset.sectionSearch] = value;
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

    const commentInput = event.target.closest("[data-rsa-comment-input]");
    if (commentInput) {
      const panel = commentInput.closest("[data-rsa-comment-scope]");
      if (panel) state.commentDraftByScope[commentScopeKey(panel.dataset.rsaCommentScope, panel.dataset.rsaCommentId)] = commentInput.value;
      return;
    }

    const commentEditInput = event.target.closest("[data-rsa-comment-edit-input]");
    if (commentEditInput) {
      state.commentEditValues[commentEditInput.dataset.rsaCommentEditInput] = commentEditInput.value;
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
    const editMode = state.inlineEditByList[listId];
    if (field === "quantity_inputs") {
      const nextActive = !(editMode.quantity_packed_override || editMode.quantity_needed_override);
      editMode["lp-row-title"] = false;
      editMode.quantity_packed_override = nextActive;
      editMode.quantity_needed_override = nextActive;
      render();
      return;
    }
    const nextActive = !editMode[field];
    if (field === "lp-row-title") {
      editMode["lp-row-title"] = nextActive;
      if (nextActive) {
        editMode.quantity_packed_override = false;
        editMode.quantity_needed_override = false;
      }
    } else {
      editMode[field] = nextActive;
      if (nextActive) editMode["lp-row-title"] = false;
    }
    render();
  }

  function toggleItemInlineEdit(itemId, field) {
    if (!itemId || !field) return;
    state.inlineEditByItem[itemId] = state.inlineEditByItem[itemId] || {};
    const editMode = state.inlineEditByItem[itemId];
    if (field === "quantity_inputs") {
      const nextActive = !(editMode.quantity_packed_override || editMode.quantity_needed_override);
      editMode["lp-row-title"] = false;
      editMode.quantity_needed_override = nextActive;
      editMode.quantity_packed_override = nextActive;
      render();
      return;
    }
    const nextActive = !editMode[field];
    if (field === "lp-row-title") {
      editMode["lp-row-title"] = nextActive;
      if (nextActive) {
        editMode.quantity_needed_override = false;
        editMode.quantity_packed_override = false;
      }
    } else {
      editMode[field] = nextActive;
      if (nextActive) editMode["lp-row-title"] = false;
    }
    render();
  }

  async function saveInlineEdit(button) {
    const itemId = button.dataset.inlineSaveItem;
    const item = items().find((row) => row.id === itemId);
    if (!item) return;
    const editMode = state.inlineEditByItem[itemId] || {};
    const fields = {};
    if (editMode["lp-row-title"]) {
      const itemName = String(inlineEditValue(item, "lp-row-title") || "").trim();
      if (!itemName) {
        setSaveMessage("Title cannot be blank.");
        return;
      }
      fields.item_name = itemName;
    }
    if (editMode.quantity_packed_override) {
      const packed = wholeQuantityNumber(inlineEditValue(item, "quantity_packed_override"));
      if (!Number.isFinite(packed) || packed < 0) {
        setSaveMessage("Packed must be zero or greater.");
        return;
      }
      fields.quantity_packed = packed;
    }
    if (editMode.quantity_needed_override) {
      const needed = wholeQuantityNumber(inlineEditValue(item, "quantity_needed_override"));
      if (!Number.isFinite(needed) || needed < 0) {
        setSaveMessage("Needed must be zero or greater.");
        return;
      }
      fields.quantity_needed = needed;
    }
    if (!Object.keys(fields).length) {
      setSaveMessage("Choose title, packed, or needed before saving.");
      return;
    }
    const pendingKey = pendingActionKey("update_item_fields", itemId);
    if (state.pendingActions[pendingKey]) return;
    state.pendingActions[pendingKey] = "save";
    await postAction({
      action: "update_item_fields",
      itemId,
      effectiveNeeded: item.needed,
      fields
    }, () => {
      delete state.inlineEditValues[inlineEditKey(itemId, "lp-row-title")];
      delete state.inlineEditValues[inlineEditKey(itemId, "quantity_packed_override")];
      delete state.inlineEditValues[inlineEditKey(itemId, "quantity_needed_override")];
      delete state.inlineEditByItem[itemId];
    }, {
      pendingKey,
      message: "Saving item..."
    });
  }

  async function addScopedComment(button) {
    const panel = button.closest("[data-rsa-comment-scope]");
    if (!panel) return;
    const scopeType = panel.dataset.rsaCommentScope || "";
    const scopeId = panel.dataset.rsaCommentId || "";
    const scopeLabel = panel.dataset.rsaCommentLabel || "";
    const input = panel.querySelector("[data-rsa-comment-input]");
    const comment = String(input?.value || "").trim();
    if (!scopeType || !scopeId) {
      setSaveMessage("Comment scope is missing.");
      return;
    }
    if (!comment) {
      setSaveMessage("Comment cannot be blank.");
      return;
    }
    const scopeKey = commentScopeKey(scopeType, scopeId);
    const pendingKey = pendingActionKey("add_comment", scopeKey);
    if (state.pendingActions[pendingKey]) return;
    state.pendingActions[pendingKey] = "comment";
    await postAction({
      action: "add_comment",
      scopeType,
      scopeId,
      scopeLabel,
      itemId: scopeType === "item" ? scopeId : "",
      showId: state.data?.source?.showId || "",
      packWaveId: currentWaveId(),
      packWaveKey: state.data?.source?.packWaveKey || "",
      comment
    }, () => {
      delete state.commentDraftByScope[scopeKey];
    }, {
      pendingKey,
      message: "Saving comment..."
    });
  }

  function toggleScopedCommentEdit(button) {
    const commentId = button.dataset.rsaCommentEdit || "";
    if (!commentId) return;
    const active = !!state.commentEditById[commentId];
    state.commentEditById = active ? {} : { [commentId]: true };
    if (!active && state.commentEditValues[commentId] === undefined) {
      state.commentEditValues[commentId] = button.dataset.rsaCommentText || "";
    }
    render();
  }

  async function saveScopedComment(button) {
    const commentId = button.dataset.rsaCommentSave || "";
    const panel = button.closest("[data-rsa-comment-scope]");
    if (!commentId || !panel) return;
    const scopeType = panel.dataset.rsaCommentScope || "";
    const scopeId = panel.dataset.rsaCommentId || "";
    const scopeLabel = panel.dataset.rsaCommentLabel || "";
    const comment = String(state.commentEditValues[commentId] || "").trim();
    if (!comment) {
      setSaveMessage("Comment cannot be blank.");
      return;
    }
    const pendingKey = pendingActionKey("update_comment", commentId);
    if (state.pendingActions[pendingKey]) return;
    state.pendingActions[pendingKey] = "comment";
    await postAction({
      action: "update_comment",
      commentId,
      scopeType,
      scopeId,
      scopeLabel,
      comment
    }, () => {
      delete state.commentEditById[commentId];
      delete state.commentEditValues[commentId];
    }, {
      pendingKey,
      message: "Saving comment..."
    });
  }

  async function runPackingAction(button) {
    const itemId = button.dataset.itemId || state.detailId;
    const action = button.dataset.packingAction;
    const item = items().find((row) => row.id === itemId);
    if (!item) return;

    if (action === "add_quantity" || action === "add_one") {
      const pendingKey = pendingActionKey("add_quantity", itemId);
      if (state.pendingActions[pendingKey]) return;
      const quantityDelta = action === "add_one" ? 1 : wholeQuantityNumber(state.addQty[itemId] || 0);
      if (!Number.isFinite(quantityDelta) || quantityDelta <= 0) {
        setSaveMessage("Enter a quantity to add.");
        return;
      }
      const rollback = snapshotItemQuantities(itemId);
      state.pendingActions[pendingKey] = action;
      state.addQty[itemId] = "";
      const optimistic = applyOptimisticAddQuantity(itemId, quantityDelta);
      state.saveMessage = `Adding ${quantityDisplay(quantityDelta)}...`;
      render();
      await postAction({
        action: "add_quantity",
        itemId,
        quantityDelta,
        effectiveNeeded: item.needed,
        notes: state.actionNotes[itemId] || ""
      }, null, {
        pendingKey,
        message: `Adding ${quantityDisplay(quantityDelta)}...`,
        quietStart: true,
        preserveItemQuantities: optimistic ? { itemId, ...optimistic } : null,
        rollback: () => restoreItemQuantities(itemId, rollback)
      });
      return;
    }

    if (action === "set_pack_state") {
      const packState = button.dataset.packState;
      if (packState === "packed" && !window.confirm("Mark this item packed and set packed quantity to the full need?")) return;
      const pendingKey = pendingActionKey("set_pack_state", itemId);
      if (state.pendingActions[pendingKey]) return;
      const snapshot = snapshotPackingItemState(itemId);
      state.pendingActions[pendingKey] = packState;
      applyLocalPackState(itemId, packState);
      state.saveMessage = packState === "packed" ? "Marking packed..." : "Marking not packed...";
      render();
      await postAction({
        action,
        itemId,
        packState,
        effectiveNeeded: item.needed,
        confirmed: packState === "packed",
        notes: state.actionNotes[itemId] || ""
      }, null, {
        pendingKey,
        message: packState === "packed" ? "Marking packed..." : "Marking not packed...",
        quietStart: true,
        rollback: () => restorePackingItemState(itemId, snapshot)
      });
      return;
    }

    if (action === "set_resolution") {
      const resolutionState = button.dataset.resolutionState;
      const label = resolutionState === "clear" ? "clear this decision" : `set decision to ${resolutionDisplayLabel(resolutionState)}`;
      if (!window.confirm(`Confirm ${label}?`)) return;
      const pendingKey = pendingActionKey("set_resolution", itemId);
      if (state.pendingActions[pendingKey]) return;
      const snapshot = snapshotPackingItemState(itemId);
      state.pendingActions[pendingKey] = resolutionState;
      applyLocalResolutionState(itemId, resolutionState);
      state.saveMessage = "Saving decision...";
      render();
      await postAction({
        action,
        itemId,
        resolutionState,
        effectiveNeeded: item.needed,
        confirmed: true,
        notes: state.actionNotes[itemId] || ""
      }, () => {
        delete state.decisionOpenByItem[itemId];
      }, {
        pendingKey,
        message: "Saving decision...",
        quietStart: true,
        rollback: () => restorePackingItemState(itemId, snapshot)
      });
    }
  }

  function toggleDecisionOptions(itemId) {
    if (!itemId) return;
    state.decisionOpenByItem[itemId] = !state.decisionOpenByItem[itemId];
    renderDetail();
  }

  async function setHorseMemberState(button) {
    const itemHorseId = button.dataset.itemHorseId;
    const horsePackState = button.dataset.horseMemberState;
    if (!itemHorseId || !horsePackState) return;
    const pendingKey = pendingActionKey("set_horse_pack_state", itemHorseId);
    if (state.pendingActions[pendingKey]) return;
    const snapshot = snapshotHorseMemberState(itemHorseId);
    state.pendingActions[pendingKey] = horsePackState;
    applyLocalHorseMemberState(itemHorseId, horsePackState);
    state.saveMessage = horsePackState === "packed" ? "Marking packed..." : "Reopening item...";
    render();
    await postAction({
      action: "set_horse_pack_state",
      itemHorseId,
      horsePackState
    }, null, {
      pendingKey,
      message: horsePackState === "packed" ? "Marking packed..." : "Reopening item...",
      quietStart: true,
      rollback: () => restoreHorseMemberState(snapshot)
    });
  }

  async function setOnsiteTaskState(button) {
    const sourceItemId = button.dataset.sourceItemId;
    const taskState = button.dataset.onsiteTaskState;
    if (!sourceItemId || !taskState) return;
    const pendingKey = pendingActionKey("set_onsite_task_state", sourceItemId);
    if (state.pendingActions[pendingKey]) return;
    state.pendingActions[pendingKey] = taskState;
    const snapshot = snapshotOnsiteTaskState(sourceItemId);
    applyLocalOnsiteTaskState(sourceItemId, taskState);
    await postAction({
      action: "set_onsite_task_state",
      sourceItemId,
      taskState,
      showId: state.data?.source?.showId || "",
      packWaveId: state.data?.source?.packWaveId || ""
    }, null, {
      pendingKey,
      message: taskState === "done" ? "Marking done..." : "Reopening task...",
      rollback: () => restoreOnsiteTaskState(snapshot)
    });
  }

  async function toggleHorseState(button) {
    const horseId = button.dataset.horseId;
    const nextState = button.dataset.nextState;
    if (!horseId || !nextState) return;
    const pendingKey = pendingActionKey("set_horse_record_state", horseId);
    if (state.pendingActions[pendingKey]) return;
    const snapshot = snapshotHorseRecordState(horseId);
    state.pendingActions[pendingKey] = nextState;
    applyLocalHorseRecordState(horseId, nextState);
    state.saveMessage = nextState === "active" ? "Activating horse..." : "Deactivating horse...";
    render();
    await postAction({
      action: "set_horse_record_state",
      horseId,
      recordState: nextState
    }, null, {
      pendingKey,
      message: nextState === "active" ? "Activating horse..." : "Deactivating horse...",
      quietStart: true,
      rollback: () => restoreHorseRecordState(snapshot)
    });
  }

  async function setSourceFlag(button) {
    const sourceItemId = button.dataset.sourceItemId;
    const flagName = button.dataset.sourceFlag;
    const value = button.dataset.nextValue === "true";
    if (!sourceItemId || !flagName) return;
    const pendingKey = pendingActionKey(`set_source_flag:${flagName}`, sourceItemId);
    if (state.pendingActions[pendingKey]) return;
    const snapshot = snapshotSourceFlag(sourceItemId);
    state.pendingActions[pendingKey] = value ? "on" : "off";
    applyLocalSourceFlag(sourceItemId, flagName, value);
    state.saveMessage = value ? "Turning on..." : "Turning off...";
    render();
    await postAction({
      action: "set_source_flag",
      sourceItemId,
      flagName,
      value
    }, null, {
      pendingKey,
      message: value ? "Turning on..." : "Turning off...",
      quietStart: true,
      rollback: () => restoreSourceFlag(snapshot)
    });
  }

  async function postAction(payload, afterSave, options = {}) {
    if (!options.quietStart) {
      state.saving = true;
      state.saveMessage = options.message || "Saving...";
      render();
    }

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
      state.data = normalizeStatePayload(result.state || state.data);
      if (options.preserveItemQuantities) preserveItemQuantities(options.preserveItemQuantities);
      removeFailedAction(payload);
      state.saveMessage = `Saved: ${new Date().toLocaleString()}`;
    } catch (error) {
      if (typeof options.rollback === "function") options.rollback();
      queueFailedAction(payload, error);
      state.saveMessage = `Save failed. Saved on this device: ${state.failedActions.length} pending.`;
    } finally {
      if (options.pendingKey) delete state.pendingActions[options.pendingKey];
      state.saving = false;
      render();
    }
  }

  async function retryFailedActions({ silent = false } = {}) {
    if (state.retryingFailedActions) return;
    state.failedActions = loadFailedActions();
    if (!state.failedActions.length) return;
    if (navigator.onLine === false) {
      state.saveMessage = `Offline. Saved on this device: ${state.failedActions.length} pending.`;
      render();
      return;
    }

    state.retryingFailedActions = true;
    if (!silent) {
      state.saveMessage = `Retrying ${state.failedActions.length} saved change${state.failedActions.length === 1 ? "" : "s"}...`;
      render();
    }

    const remaining = [];
    let saved = 0;
    for (const entry of state.failedActions) {
      try {
        const response = await fetch(endpointUrl("action"), {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json"
          },
          body: JSON.stringify(entry.payload || {})
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result.ok) {
          throw new Error(result.detail || result.error || `save_${response.status}`);
        }
        saved += 1;
        state.data = normalizeStatePayload(result.state || state.data);
      } catch (error) {
        remaining.push({
          ...entry,
          attempts: number(entry.attempts) + 1,
          lastError: error instanceof Error ? error.message : String(error),
          updatedAt: new Date().toISOString()
        });
      }
    }

    state.failedActions = remaining;
    saveFailedActions(remaining);
    state.retryingFailedActions = false;
    if (saved || !silent) {
      state.saveMessage = remaining.length
        ? `Saved on this device: ${remaining.length} pending.`
        : `Retried saved changes: ${saved}.`;
    }
    render();
  }

  function queueFailedAction(payload, error) {
    const queue = loadFailedActions();
    const fingerprint = failedActionFingerprint(payload);
    const now = new Date().toISOString();
    const existing = queue.find((entry) => entry.fingerprint === fingerprint);
    const lastError = error instanceof Error ? error.message : String(error);
    if (existing) {
      existing.updatedAt = now;
      existing.lastError = lastError;
      existing.attempts = number(existing.attempts) + 1;
    } else {
      queue.push({
        id: `fail_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
        fingerprint,
        createdAt: now,
        updatedAt: now,
        attempts: 1,
        label: failedActionLabel(payload),
        lastError,
        payload
      });
    }
    state.failedActions = queue.slice(-100);
    saveFailedActions(state.failedActions);
  }

  function removeFailedAction(payload) {
    const fingerprint = failedActionFingerprint(payload);
    const queue = loadFailedActions().filter((entry) => entry.fingerprint !== fingerprint);
    state.failedActions = queue;
    saveFailedActions(queue);
  }

  function loadFailedActions() {
    try {
      const parsed = JSON.parse(window.localStorage.getItem(failedActionStorageKey) || "[]");
      return Array.isArray(parsed) ? parsed.filter((entry) => entry?.payload?.action) : [];
    } catch (error) {
      return [];
    }
  }

  function saveFailedActions(queue) {
    try {
      window.localStorage.setItem(failedActionStorageKey, JSON.stringify(queue || []));
    } catch (error) {}
  }

  function failedActionFingerprint(payload) {
    return stableStringify(payload || {});
  }

  function failedActionLabel(payload) {
    const action = payload?.action || "save";
    const itemId = payload?.itemId || payload?.packingItemId || payload?.sourceItemId || "";
    const item = itemId ? items().find((row) => row.id === itemId) : null;
    const itemName = displayLabel(item?.name || payload?.scopeLabel || payload?.commentId || itemId || "");
    if (action === "add_quantity") return `Add ${quantityDisplay(payload.quantityDelta)}${itemName ? ` - ${itemName}` : ""}`;
    if (action === "add_comment") return `Add comment${itemName ? ` - ${itemName}` : ""}`;
    if (action === "update_comment") return `Edit comment${itemName ? ` - ${itemName}` : ""}`;
    if (action === "update_item_fields") return `Edit item${itemName ? ` - ${itemName}` : ""}`;
    if (action === "set_resolution") return `Decision ${resolutionDisplayLabel(payload.resolutionState)}${itemName ? ` - ${itemName}` : ""}`;
    return `${displayLabel(action)}${itemName ? ` - ${itemName}` : ""}`;
  }

  function stableStringify(value) {
    if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
    if (value && typeof value === "object") {
      return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(",")}}`;
    }
    return JSON.stringify(value);
  }

  function exportFailedActions() {
    state.failedActions = loadFailedActions();
    if (!state.failedActions.length) {
      setSaveMessage("No failed saves to export.");
      return;
    }
    const report = failedActionsReport(state.failedActions);
    const blob = new Blob([JSON.stringify(report, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `wec-packing-failed-saves-${Date.now()}.json`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    setSaveMessage(`Exported ${state.failedActions.length} failed save${state.failedActions.length === 1 ? "" : "s"}.`);
  }

  function failedActionsReport(queue) {
    return {
      app: "wec-packing",
      generatedAt: new Date().toISOString(),
      pageUrl: window.location.href,
      actionUrl: endpointUrl("action"),
      failures: (queue || []).map((entry) => ({
        id: entry.id,
        label: entry.label,
        createdAt: entry.createdAt,
        updatedAt: entry.updatedAt,
        attempts: entry.attempts,
        lastError: entry.lastError,
        payload: entry.payload
      }))
    };
  }

  function failedActionsEmailHref() {
    const queue = state.failedActions || [];
    const body = [
      "WEC Packing failed saves",
      `Generated: ${new Date().toLocaleString()}`,
      `Page: ${window.location.href}`,
      `Pending: ${queue.length}`,
      "",
      ...queue.slice(0, 8).map((entry, index) => `${index + 1}. ${entry.label || entry.payload?.action || "save"} | ${entry.lastError || ""}`),
      queue.length > 8 ? `...${queue.length - 8} more` : "",
      "",
      "Use EXPORT to attach the full JSON file."
    ].filter((line) => line !== "").join("\n");
    return `mailto:?subject=${encodeURIComponent("WEC Packing failed saves")}&body=${encodeURIComponent(body)}`;
  }

  function setSaveMessage(message) {
    state.saveMessage = message;
    render();
  }

  function closeDetail() {
    state.detailType = "";
    state.detailId = "";
    if (window.location.hash.startsWith("#wec-place-")) {
      history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
    }
    renderDetail();
  }

  function render(options = {}) {
    root.innerHTML = `
      <div class="rsa-dashboard">
        <div class="rsa-dashboard-block">
          <div class="rsa-dashboard-container">
            <div class="rsa-main-grid">
              <div class="rsa-top">
                <div class="rsa-padding">
                  <div class="rsa-padding-bottom">
                    <div class="rsa-banner-header">
                      <div class="rsa-head-right">
                        <h5 class="rsa-report-title rsa-H1">WEC PACK LIST</h5>
                        <div class="rsa-report-subtitle rsa-text">${escapeHtml(statusLine())}</div>
                      </div>
                      <div class="rsa-head-left is-hidden">
                        <h5 class="rsa-report-title rsa-H1">WEC PACK LIST</h5>
                        <div class="rsa-report-subtitle rsa-text">${escapeHtml(statusLine())}</div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              <div class="rsa-actions">
                <div class="rsa-padding">
                  ${tabsHtml()}
                </div>
              </div>
              <div class="rsa-body">
                ${panelHtml()}
              </div>
              <div class="rsa-bottom">
                <div class="rsa-padding">
                  <div class="rsa-messages">
                    ${footerStatusHtml()}
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="mobile-bottom-spacing"></div>
          <div class="lp-modal" id="packingDetail" hidden aria-hidden="true">
            <div class="lp-modal-backdrop" data-close-detail></div>
            <section class="lp-modal-card" role="dialog" aria-modal="true" aria-labelledby="drawerTitle" tabindex="-1">
              <button class="lp-modal-close" type="button" data-close-detail aria-label="Close detail">x</button>
              <div id="packingDetailContent" data-modal-content></div>
            </section>
          </div>
        </div>
      </div>
    `;
    root.dataset.activeOverviewListId = state.activeOverviewListId || "";
    root.dataset.activeHomeModule = state.activeHomeModule || "";
    root.dataset.overviewFilter = state.filterByList.overview || "";
    renderDetail();
    if (options.focusSearchKey) restoreSearchFocus(options);
  }

  function handleHashRoute() {
    const hash = window.location.hash || "";
    if (hash.startsWith("#wec-list-")) {
      openOverviewList(hash.replace("#wec-list-", ""));
      return;
    }
    if (hash.startsWith("#wec-place-")) {
      state.detailType = "place";
      state.detailId = hash.replace("#wec-place-", "");
      renderDetail();
    }
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
    if (state.failedActions.length) return `Saved on this device: ${state.failedActions.length} pending.`;
    if (state.error) return state.error;
    if (state.loading) return "Checking live state";
    return `Last checked: ${new Date().toLocaleString()}`;
  }

  function footerStatusHtml(message = footerLine()) {
    const feedbackClass = "rsa-text is-feedback is-xs is-align-right is-align-top";
    if (!state.failedActions.length) return `<div class="${feedbackClass}">${escapeHtml(message)}</div>`;
    return `
      <div class="rsa-footer-status">
        <div class="${feedbackClass}">${escapeHtml(message)}</div>
        <div class="rsa-footer-actions">
          <div class="rs-text-linline rsa-text is-xxs is-inline-edit" data-rsa-retry-failed>${state.retryingFailedActions ? "retrying" : "retry"}</div>
          <div class="rs-text-linline rsa-text is-xxs is-inline-edit" data-rsa-export-failed>export</div>
          <a class="rs-text-linline rsa-text is-xxs is-inline-edit" href="${escapeAttr(failedActionsEmailHref())}">email</a>
        </div>
      </div>
    `;
  }

  function saveMetaClass() {
    if (state.saving) return "is-saving";
    if (state.saveMessage && state.saveMessage.toLowerCase().startsWith("save failed")) return "is-error";
    if (state.saveMessage && state.saveMessage.toLowerCase().startsWith("saved")) return "is-success";
    return "";
  }

  function tabsHtml() {
    return `
      <div class="rsa-list-action-menu packing-section-tabs">
        ${tabs().map((section) => {
          return `
            <div class="rs-tab-link rsa-text is-link is-section-tab ${escapeAttr(rsaSectionClass(section.id))} ${state.activeTab === section.id ? "is-active" : ""}" data-tab="${escapeAttr(section.id)}">
              <div>${escapeHtml(displayLabel(section.label))}</div>
            </div>
          `;
        }).join("")}
      </div>
    `;
  }

  function panelHtml() {
    if (state.loading) return rsaMessagePanel("Loading");
    if (state.error) return rsaMessagePanel("Unable to load packing state");
    if (!state.data) return rsaMessagePanel("No state");
    if (state.activeTab === "overview") return rsaOverviewHtml();
    if (state.activeTab === "horses") return rsaHorsesHtml();
    if (isTabGroupId(state.activeTab)) return rsaTabGroupHtml(state.activeTab);
    return rsaSingleListHtml(state.activeTab);
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

  function rsaMessagePanel(title) {
    return rsaPanelShellHtml(`
      <div class="rsa-content">
        <div class="rsa-top">
          ${rsaPaddedGridRowHtml({
            leftHtml: rsaTitleTextHtml(`<h4 class="rsa-head rsa-H2">${escapeHtml(displayLabel(title))}</h4>`)
          })}
        </div>
        <div class="rsa-bottom">
          <div class="rsa-padding">
            <div class="rsa-messages">
              ${footerStatusHtml(state.error || statusLine())}
            </div>
          </div>
        </div>
      </div>
    `);
  }

  function rsaOverviewHtml() {
    if (state.activeHomeModule) return rsaHomeModuleHtml(state.activeHomeModule);
    if (state.activeOverviewListId) return rsaOverviewListDetailHtml(state.activeOverviewListId);
    const mode = overviewFilterMode();
    const laneMode = activeLaneRows(mode);
    const rows = laneMode
      ? filterRows(laneMode, "overview", overviewSearchText)
      : mode === "search_list"
        ? sortTableRows(sortItemsByName(filterRows(items(), "overview", itemSearchText)), "overview")
        : filterRows(tabGroups(), "overview", overviewSearchText);
    const rowsHtml = laneMode
      ? rows.map(rsaLaneRowHtml).join("")
      : mode === "search_list"
      ? rows.map((item) => rsaItemRowHtml(item, state.inlineEditByItem[item.id] || {}, "overview")).join("")
      : rows.map(rsaOverviewRowHtml).join("");
    const isLaneMode = !!laneMode;
    return rsaPanelShellHtml(rsaDataModuleHtml({
      title: overviewModeLabel(mode),
      printTarget: "overview",
      searchKey: "overview",
      filterKey: "overview",
      commentScope: rsaCommentScope("wave", currentWaveId() || "wave", currentWaveLabel()),
      rowsHtml: rows.length ? rowsHtml : rsaEmptyTableRowHtml("No rows"),
      tableLabel: mode === "search_list" ? "item" : "list",
      tableActionLabel: mode === "search_list" ? "input" : isLaneMode ? "open" : "print",
      tableMetricLabels: isLaneMode ? ["", "", ""] : undefined,
      showFilter: false,
      headerFilter: false,
      headerSearch: false,
      headerPrint: false,
      forceSearchOpen: mode === "search_list"
    }), rsaFilterHtml("overview", true, overviewFilterOptions(), { printTarget: "overview" }));
  }

  function rsaHomeModuleHtml(moduleId) {
    const module = homeModules().find((row) => row.id === moduleId);
    if (!module) return rsaMessagePanel("No rows");
    const moduleLists = sortListsByLabel(module.lists || []);
    const activeList = activeListForGroup(module.id, moduleLists);
    const rows = sortOnsiteTasks(filterRows((module.tasks || []).filter((task) => onsiteTaskBelongsToList(task, activeList?.id)), module.id, onsiteTaskSearchText));
    return `
      ${rsaPanelShellHtml(
        rsaDataModuleHtml({
          title: module.label || "Purchase onsite",
          printTarget: `home:${module.id}`,
          searchKey: module.id,
          filterKey: activeList?.id || module.id,
          commentScope: rsaCommentScope("section", activeList?.id || module.id, activeList?.label || module.label || "Purchase onsite"),
          rowsHtml: rows.length ? rows.map(rsaOnsiteRowHtml).join("") : rsaEmptyTableRowHtml("No tasks"),
          labelActionsHtml: rsaListSwitcherHtml(module.id, moduleLists, activeList?.id || "")
        }),
        rsaFilterHtml("overview", true, overviewFilterOptions(), { printTarget: "overview" })
      )}
    `;
  }

  function rsaOverviewListDetailHtml(listId) {
    const detail = activeListDetailById(listId);
    const storedSummary = state.activeOverviewListSummary?.id === listId ? state.activeOverviewListSummary : null;
    const summary = activeLaneListById(listId) || storedSummary || (detail ? {
      id: detail.id,
      key: detail.key,
      lane: detail.lane,
      label: detail.label,
      sourceTable: detail.sourceTable,
      sourceView: detail.sourceView
    } : null);
    if (!summary) {
      state.activeOverviewListId = "";
      return rsaOverviewHtml();
    }
    if (summary.homeModuleId) return rsaHomeModuleHtml(summary.homeModuleId);

    const rows = overviewListDetailRows(summary);
    const searchKey = `overview:${summary.id}`;
    const filterOptions = overviewDetailFilterOptions(summary, rows);
    const filteredRows = filterOverviewDetailRows(summary, rows, searchKey);
    const rowsHtml = summary.key === "unresolved"
      ? filteredRows.map((item) => rsaItemRowHtml(item, state.inlineEditByItem[item.id] || {}, "overview")).join("")
      : filteredRows.map(rsaSimpleDetailRowHtml).join("");
    return rsaPanelShellHtml(rsaDataModuleHtml({
      title: summary.label || "List",
      printTarget: summary.printTarget || "overview",
      searchKey,
      filterKey: searchKey,
      commentScope: rsaCommentScope("section", summary.id, summary.label || "List"),
      rowsHtml: filteredRows.length ? rowsHtml : rsaEmptyTableRowHtml("No rows"),
      tableLabel: detailTableLabel(summary),
      tableActionLabel: detailTableActionLabel(summary),
      tableMetricLabels: detailTableMetricLabels(summary),
      showFilter: filterOptions.length > 1,
      headerFilter: filterOptions.length > 1,
      headerSearch: true,
      headerPrint: false,
      filterOptions,
      filterVariant: "text-links"
    }), rsaFilterHtml("overview", true, overviewFilterOptions(), { printTarget: "overview" }));
  }

  function rsaTabGroupHtml(tabId) {
    const group = tabGroups().find((row) => row.id === tabId);
    const groupLists = group?.listIds?.length
      ? group.listIds.map((id) => lists().find((list) => list.id === id)).filter(Boolean)
      : [];
    const sortedGroupLists = sortListsByLabel(groupLists);
    if (!sortedGroupLists.length) return rsaMessagePanel(group?.label || "No rows");
    const activeList = activeListForGroup(group.id, sortedGroupLists);
    return `
      ${rsaPanelShellHtml(
        rsaListTableHtml(activeList, group.id),
        rsaListSwitcherHtml(group.id, sortedGroupLists, activeList.id)
      )}
    `;
  }

  function rsaSingleListHtml(listId) {
    const list = lists().find((row) => row.id === listId) || { id: listId, label: listId };
    return `
      ${rsaPanelShellHtml(rsaListTableHtml(list, list.id))}
    `;
  }

  function rsaHorsesHtml() {
    const rows = horseFilterRows();
    return rsaPanelShellHtml(rsaDataModuleHtml({
      title: "Horses",
      printTarget: "horses",
      searchKey: "horses",
      filterKey: "horses",
      commentScope: rsaCommentScope("tab", "horses", "Horses"),
      rowsHtml: rows.length ? rows.map(rsaHorseRowHtml).join("") : rsaEmptyTableRowHtml("No horses"),
      tableLabel: "horse",
      tableActionLabel: "print",
      showFilter: false,
      headerFilter: false,
      headerSearch: false,
      headerPrint: false
    }), rsaFilterHtml("horses", true, horseFilterOptions(), { searchScope: "horses", printTarget: "horses" }));
  }

  function rsaListTableHtml(list, tabId) {
    const rows = sortTableRows(
      rsaApplyListFilter(
        sortItemsByName(filterRows(items().filter((item) => itemBelongsToList(item, list.id)), list.id, itemSearchText)),
        list.id
      ),
      list.id
    );
    return rsaDataModuleHtml({
      title: list.label || list.id,
      printTarget: list.id,
      searchKey: list.id,
      filterKey: list.id,
      commentScope: isTabGroupId(tabId)
        ? rsaCommentScope("tab", tabId, tabGroups().find((group) => group.id === tabId)?.label || list.label || tabId)
        : rsaCommentScope("section", list.id, list.label || list.id),
      rowsHtml: rows.length ? rows.map((item) => rsaItemRowHtml(item, state.inlineEditByItem[item.id] || {}, list.id)).join("") : rsaEmptyTableRowHtml("No rows"),
      labelActionsHtml: `
        <div class="rs-text-link rsa-text is-xs is-label">edit</div>
        <div class="rs-text-link rsa-text is-xs is-label">input</div>
      `
    });
  }

  function rsaPanelShellHtml(contentHtml, actionsHtml = "") {
    return `
      <div class="rsa-actions${actionsHtml ? "" : " is-hidden"}">${actionsHtml}</div>
      <div class="rsa-body">
        ${contentHtml}
      </div>
      <div class="rsa-bottom">
        <div class="rsa-padding">
          <div class="rsa-messages">
        ${footerStatusHtml()}
          </div>
        </div>
      </div>
    `;
  }

  function rsaPaddedGridRowHtml(options = {}) {
    const paddingClass = options.paddingClass ? ` ${options.paddingClass}` : "";
    return `
      <div class="rsa-padding${paddingClass}">
        ${rsaGridRowHtml(options)}
      </div>
    `;
  }

  function rsaGridRowHtml({ leftHtml = "", rightHtml = "", rowClass = "is-grid2", rowAttrs = "", leftAttrs = "", rightAttrs = "" } = {}) {
    const rowClasses = ["rsa-item-row-2", rowClass].filter(Boolean).join(" ");
    const rowAttributeText = rowAttrs ? ` ${rowAttrs}` : "";
    const leftAttributeText = leftAttrs ? ` ${leftAttrs}` : "";
    const rightAttributeText = rightAttrs ? ` ${rightAttrs}` : "";
    return `
      <div class="${rowClasses}"${rowAttributeText}>
        <div class="rsa-item-block-left"${leftAttributeText}>
          ${leftHtml}
        </div>
        <div class="rsa-item-block-right"${rightAttributeText}>
          ${rightHtml}
        </div>
      </div>
    `;
  }

  function rsaHotGridLinkHtml({ href = "#", attrs = "", leftHtml = "", rightHtml = "" } = {}) {
    const attributeText = attrs ? ` ${attrs}` : "";
    return `
      <div class="rsa-item-row-2 is-grid2 is-hot-row"${attributeText}>
        <div class="rsa-item-block-left">
          ${leftHtml}
        </div>
        <div class="rsa-item-block-right">
          ${rightHtml}
        </div>
      </div>
    `;
  }

  function rsaItemTextHtml(innerHtml, extraClass = "") {
    return `<div class="rsa-item-text ${extraClass}">${innerHtml}</div>`;
  }

  function rsaTitleTextHtml(innerHtml) {
    return `<div class="rsa-item-text is-title-row">${innerHtml}</div>`;
  }

  function rsaQuantityBlockHtml(innerHtml, extraClass = "") {
    return `<div class="rs-quantity-block-2 is-grid4 ${extraClass}">${innerHtml}</div>`;
  }

  function rsaDataModuleHtml({ title, printTarget, searchKey, filterKey, commentScope, rowsHtml, labelActionsHtml, tableLabel = "item", tableActionLabel = "input", tableMetricLabels, showFilter = true, headerFilter = false, headerSearch = true, headerPrint = true, forceSearchOpen = false, filterOptions, filterVariant = "" }) {
    const storedActiveTool = state.activeToolByList[filterKey] || "";
    const activeTool = forceSearchOpen ? "search" : showFilter || storedActiveTool !== "filter" ? storedActiveTool : "";
    const showHeaderFilter = showFilter || headerFilter;
    const headerActionCount = [showHeaderFilter, headerSearch, headerPrint].filter(Boolean).length;
    const headerActionClass = headerActionCount === 1 ? " is-one-action" : headerActionCount === 2 ? " is-two-actions" : "";
    const resolvedCommentScope = commentScope || rsaCommentScope("section", filterKey, title);
    const headerToolsHtml = `
      ${showHeaderFilter ? `<div class="rs-text-link-2 rsa-text is-link ${activeTool === "filter" ? "is-active" : ""}" data-rsa-toggle="filter" data-rsa-scope="${escapeAttr(filterKey)}">filter</div>` : ""}
      ${headerSearch ? `<div class="rs-text-link-2 rsa-text is-link ${activeTool === "search" ? "is-active" : ""}" data-rsa-toggle="search" data-rsa-scope="${escapeAttr(filterKey)}">search</div>` : ""}
              ${headerPrint ? `<a class="rs-text-link-2 rsa-text is-link is-print" ${printLinkAttrs({ target: printTarget })}>print</a>` : ""}
    `;
    return `
      <div class="rsa-content">
        <div class="rsa-top">
          ${rsaPaddedGridRowHtml({
            leftHtml: rsaTitleTextHtml(`
              <h4 class="rsa-head rsa-H5 is-caps">${escapeHtml(displayLabel(title))}</h4>
              <div class="rs-text-linline rsa-text is-xxs is-inline-edit" data-list-id="${escapeAttr(filterKey)}" data-list-edit-field="lp-row-title">edit</div>
            `),
            rightHtml: `
              <div class="rsa-action-block is-grid3${headerActionClass}">
                ${headerToolsHtml}
              </div>
            `
          })}
        </div>
        <div class="rsa-body">
          ${labelActionsHtml || ""}
          <div class="rsa-actions">
            ${rsaSearchHtml(searchKey, filterKey, activeTool === "search")}
            ${showFilter ? rsaFilterHtml(filterKey, activeTool === "filter" || filterVariant === "text-links", filterOptions, { variant: filterVariant }) : ""}
          </div>
          <div class="rsa-content">
            <div class="rsa-top is-hidden"></div>
            <div class="rsa-body">
              <div class="rsa-padding is-none">
                <div class="rsa-table">
                  <div class="rsa-table-head">
                    ${rsaTableLabelRowHtml(tableLabel, tableActionLabel, filterKey, tableMetricLabels)}
                  </div>
                  <div class="rsa-table-body">
                    ${rowsHtml}
                  </div>
                </div>
              </div>
            </div>
            <div class="rsa-bottom">
              <div class="rsa-padding">
                <div class="rsa-messages">
                  ${footerStatusHtml()}
                </div>
              </div>
            </div>
            <div class="rsa-comm">
              <div class="rsa-padding is-none">
                ${rsaCommentPanelHtml(resolvedCommentScope)}
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function rsaListSwitcherHtml(tabId, groupLists, activeListId) {
    if (!groupLists.length) return "";
    return `
      <div class="rsa-padding">
        <div class="rsa-list-action-menu">
          ${groupLists.map((list) => `
            <div class="rs-tab-link rsa-text is-link ${list.id === activeListId ? "is-active" : ""}" data-tab-id="${escapeAttr(tabId)}" data-list-switch="${escapeAttr(list.id)}">
              <div>${escapeHtml(displayLabel(list.label || list.id))}</div>
            </div>
          `).join("")}
        </div>
      </div>
    `;
  }

  function rsaSearchHtml(searchKey, scopeKey, active) {
    return rsaPaddedGridRowHtml({
      paddingClass: active ? "" : "is-hidden",
      rowClass: "is-grid2 is-search-grid",
      leftHtml: `
        <div class="rsa-messages-text is-search">
          <input class="rs-search-input" type="search" value="${escapeAttr(sectionSearchValue(searchKey))}" placeholder="search" data-section-search="${escapeAttr(searchKey)}">
        </div>
      `,
      rightHtml: `
        <div class="rsa-action-block is-grid3 is-search-actions">
          <div class="rs-text-link rsa-text is-link" data-rsa-close data-rsa-scope="${escapeAttr(scopeKey)}">close</div>
        </div>
      `
    });
  }

  function rsaFilterHtml(scopeKey, active, options, config = {}) {
    const filters = options || [
      ["all", "ALL"],
      ["need", "NEED"],
      ["packed", "PACKED"]
    ];
    const storedFilter = state.filterByList[scopeKey] || "";
    const activeFilter = filters.some(([key]) => key === storedFilter) ? storedFilter : filters[0][0];
    const variantClass = config.variant === "text-links" ? " is-text-filter" : "";
    return `
      <div class="rsa-padding ${active ? "" : "is-hidden"}">
        <div class="rsa-list-action-menu${variantClass}">
          ${filters.map(([key, label]) => `
            <div data-rsa-filter="${escapeAttr(key)}" data-rsa-scope="${escapeAttr(scopeKey)}" class="rs-tab-link rsa-text is-link ${activeFilter === key ? "is-active" : ""}">
              <div>${escapeHtml(label)}</div>
            </div>
          `).join("")}
          ${config.searchScope ? `
            <div class="rs-tab-link rsa-text is-link ${state.activeToolByList[config.searchScope] === "search" ? "is-active" : ""}" data-rsa-toggle="search" data-rsa-scope="${escapeAttr(config.searchScope)}">
              <div>SEARCH</div>
            </div>
          ` : ""}
          ${config.printTarget ? `
            <a class="rs-tab-link rsa-text is-link is-print" ${printLinkAttrs({ target: config.printTarget })}>
              <div>PRINT</div>
            </a>
          ` : ""}
        </div>
      </div>
    `;
  }

  function rsaTableLabelRowHtml(tableLabel = "item", tableActionLabel = "input", sortScope = "", metricLabels = ["need", "packed", "left"]) {
    const [needLabel = "need", packedLabel = "packed", leftLabel = "left"] = Array.isArray(metricLabels) ? metricLabels : ["need", "packed", "left"];
    const metricLabelHtml = (field, label) => label
      ? rsaSortLabelHtml(sortScope, field, label)
      : `<div class="rsa-table-label"></div>`;
    return rsaGridRowHtml({
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color is-spacer"></div>
        ${rsaSortLabelHtml(sortScope, "item", tableLabel)}
      `),
      rightHtml: rsaQuantityBlockHtml(`
        ${metricLabelHtml("needed", needLabel)}
        ${metricLabelHtml("packed", packedLabel)}
        ${metricLabelHtml("left", leftLabel)}
        <div class="rsa-table-label">${escapeHtml(tableActionLabel)}</div>
      `, "has-inline-qty-action")
    });
  }

  function rsaSortLabelHtml(sortScope, field, label) {
    const activeSort = state.sortByList[sortScope] || {};
    const activeClass = activeSort.field === field ? " is-active" : "";
    const dirAttr = activeSort.field === field ? ` data-sort-dir="${escapeAttr(activeSort.direction)}"` : "";
    return `<button type="button" class="rsa-table-label rsa-text is-xs is-caps${activeClass}" data-rsa-sort="${escapeAttr(field)}" data-rsa-scope="${escapeAttr(sortScope)}"${dirAttr}>${escapeHtml(label)}</button>`;
  }

  function rsaItemRowHtml(item, editMode, listId) {
    const editingTitle = !!editMode?.["lp-row-title"];
    const editingQty = !!(editMode?.quantity_packed_override || editMode?.quantity_needed_override);
    return rsaGridRowHtml({
      rowClass: `is-grid2 ${editingTitle ? "is-title-editing" : ""} ${editingQty ? "is-qty-editing" : ""}`,
      leftAttrs: `data-item-id="${escapeAttr(item.id)}"`,
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color bg-primary-green"></div>
        <div class="rs-table-title rsa-row-title-text rsa-text is-line-item">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</div>
        <input class="rs-table-title rsa-text is-inline-edit is-inline rsa-row-title-input" type="text" value="${escapeAttr(inlineEditValue(item, "lp-row-title"))}" data-item-id="${escapeAttr(item.id)}" data-inline-edit-field="lp-row-title">
        <div class="rs-text-linline rsa-text is-xxs is-inline-edit rsa-row-title-action" data-item-id="${escapeAttr(item.id)}" data-list-id="${escapeAttr(listId)}" data-list-edit-field="lp-row-title">edit</div>
        <div class="rs-text-linline rsa-text is-xxs is-inline-edit is-save is-title-save" data-list-id="${escapeAttr(listId)}" data-inline-save-item="${escapeAttr(item.id)}">save</div>
      `, "has-inline-title-action"),
      rightHtml: rsaQuantityBlockHtml(`
        ${rsaQuantityCellHtml(item, "quantity_needed_override", item.needed, editMode?.quantity_needed_override)}
        ${rsaQuantityCellHtml(item, "quantity_packed_override", item.packed, editMode?.quantity_packed_override)}
        <div class="rs-text-2 rsa-text is-number">${escapeHtml(quantityDisplay(item.left))}</div>
        <div class="rs-input-inline rsa-text is-xxs is-inline-input is-link ${editingQty ? "is-active" : ""}" data-item-id="${escapeAttr(item.id)}" data-list-id="${escapeAttr(listId)}" data-list-edit-field="quantity_inputs">
          <span class="rsa-row-input-action rsa-text is-xxs is-inline-input is-link">input</span>
          <span class="rs-text-link rsa-text is-xxs is-inline-input is-link is-save is-qty-save" data-list-id="${escapeAttr(listId)}" data-inline-save-item="${escapeAttr(item.id)}">save</span>
        </div>
      `, "has-inline-qty-action")
    });
  }

  function rsaQuantityCellHtml(item, field, value, editing) {
    if (!editing) return `<div class="rs-text-2 rsa-row-qty-text rsa-text is-number">${escapeHtml(quantityDisplay(value))}</div>`;
    return `<input class="rs-text-2 rsa-text is-number is-inline rsa-row-qty-input" type="number" min="0" step="1" inputmode="numeric" value="${escapeAttr(inlineEditValue(item, field))}" data-item-id="${escapeAttr(item.id)}" data-inline-edit-field="${escapeAttr(field)}">`;
  }

  function rsaOnsiteRowHtml(task) {
    const done = task.taskState === "done";
    const pending = isPendingAction("set_onsite_task_state", task.id);
    const nextState = done ? "task" : "done";
    return rsaGridRowHtml({
      leftAttrs: `data-onsite-task-detail="${escapeAttr(task.id)}"`,
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color ${done ? "bg-primary-green" : "bg-primary-blue"}"></div>
        <div class="rs-table-title rsa-text is-line-item">${escapeHtml(displayLabel(task.name || "Unnamed task"))}</div>
        <div class="rs-text-linline rsa-text is-xs"></div>
      `),
      rightHtml: rsaQuantityBlockHtml(`
        <div class="rs-text-2 rsa-text is-number"></div>
        <div class="rs-text-2 rsa-text is-number"></div>
        <div class="rs-text-2 rsa-text is-number"></div>
        <div class="rs-input-inline rsa-text is-inline-input is-link ${pending ? "is-active" : ""}" data-onsite-task-state="${escapeAttr(nextState)}" data-source-item-id="${escapeAttr(task.id)}">${pending ? "saving" : done ? "task" : "done"}</div>
      `, "has-inline-qty-action")
    });
  }

  function rsaHorseRowHtml(horse) {
    const progress = horseProgress(horse);
    return rsaGridRowHtml({
      leftAttrs: `data-horse-detail="${escapeAttr(horse.id)}"`,
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color ${progress.percent >= 100 ? "bg-primary-green" : "bg-primary-blue"}"></div>
        <div class="rs-table-title rsa-text is-line-item">${escapeHtml(displayLabel(horseDisplayName(horse)))}</div>
        <div class="rs-text-linline rsa-text is-xs">${escapeHtml(`${progress.percent}% Packed`)}</div>
      `),
      rightHtml: rsaQuantityBlockHtml(`
        <div class="rs-text-2 rsa-text is-number">${escapeHtml(quantityDisplay(progress.rows))}</div>
        <div class="rs-text-2 rsa-text is-number">${escapeHtml(quantityDisplay(progress.done))}</div>
        <div class="rs-text-2 rsa-text is-number">${escapeHtml(quantityDisplay(Math.max(0, progress.rows - progress.done)))}</div>
        <a class="rs-input-inline rsa-text is-inline-input is-link is-print" ${printLinkAttrs({ horseId: horse.id })}>print</a>
      `, "has-inline-qty-action")
    });
  }

  function rsaOverviewRowHtml(summary) {
    const percent = progressPercent(summary.done, summary.rows);
    const triggerAttr = summary.homeModule
      ? `data-home-module="${escapeAttr(summary.id)}"`
      : `data-tab="${escapeAttr(summary.id)}"`;
    const printTarget = summary.homeModule ? `home:${summary.id}` : summary.id;
    return rsaGridRowHtml({
      leftAttrs: triggerAttr,
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color ${percent >= 100 ? "bg-primary-green" : "bg-primary-blue"}"></div>
        <div class="rs-table-title rsa-text is-line-item">${escapeHtml(displayLabel(summary.label || summary.id))}</div>
        <div class="rs-text-linline rsa-text is-xs">${escapeHtml(`${percent}% Packed`)}</div>
      `),
      rightHtml: rsaQuantityBlockHtml(`
        <div class="rs-text-2 rsa-text is-number">${escapeHtml(quantityDisplay(summary.rows))}</div>
        <div class="rs-text-2 rsa-text is-number">${escapeHtml(quantityDisplay(summary.done))}</div>
        <div class="rs-text-2 rsa-text is-number">${escapeHtml(quantityDisplay(summary.open))}</div>
        <a class="rs-input-inline rsa-text is-inline-input is-link is-print" ${printLinkAttrs({ target: printTarget })}>print</a>
      `, "has-inline-qty-action")
    });
  }

  function rsaLaneRowHtml(summary) {
    const triggerAttr = `data-overview-list="${escapeAttr(summary.id)}"`;
    return rsaHotGridLinkHtml({
      href: `#wec-list-${summary.id}`,
      attrs: triggerAttr,
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color bg-primary-blue"></div>
        <div class="rs-table-title rsa-text is-line-item">${escapeHtml(displayLabel(summary.label || summary.id))}</div>
        <div class="rs-text-linline rsa-text is-xs"></div>
      `),
      rightHtml: rsaQuantityBlockHtml(`
        <div class="rs-text-2 rsa-text is-number"></div>
        <div class="rs-text-2 rsa-text is-number"></div>
        <div class="rs-text-2 rsa-text is-number"></div>
        ${rsaOpenActionHtml()}
      `, "has-inline-qty-action")
    });
  }

  function rsaSimpleDetailRowHtml(row) {
    const isPlace = row.type === "place";
    const actionUrl = row.mapsUrl || row.website || "";
    const actionHtml = isPlace
      ? rsaOpenActionHtml()
      : actionUrl
      ? rsaOpenActionHtml(`href="${escapeAttr(actionUrl)}" target="_blank" rel="noopener"`)
      : `<div class="rs-input-inline rsa-text is-inline-input is-link"></div>`;
    const rowOptions = {
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color bg-primary-blue"></div>
        <div class="rs-table-title rsa-text is-line-item">${escapeHtml(displayLabel(row.label || row.id || "Row"))}</div>
        <div class="rs-text-linline rsa-text is-xs">${escapeHtml(row.meta || "")}</div>
      `),
      rightHtml: rsaQuantityBlockHtml(`
        <div class="rs-text-2 rsa-text is-number"></div>
        <div class="rs-text-2 rsa-text is-number"></div>
        <div class="rs-text-2 rsa-text is-number"></div>
        ${actionHtml}
      `, "has-inline-qty-action")
    };
    return isPlace
      ? rsaHotGridLinkHtml({ ...rowOptions, href: `#wec-place-${row.id}`, attrs: `data-place-detail="${escapeAttr(row.id)}"` })
      : rsaGridRowHtml(rowOptions);
  }

  function rsaOpenActionHtml(attrs = "") {
    const attributeText = attrs ? ` ${attrs}` : "";
    const tag = attrs.includes("href=") ? "a" : "div";
    return `<${tag} class="rs-input-inline rsa-text is-inline-input is-link is-open-action"${attributeText}>open <span class="rsa-open-icon" aria-hidden="true"></span></${tag}>`;
  }

  function rsaEmptyTableRowHtml(label) {
    return rsaGridRowHtml({
      leftHtml: rsaItemTextHtml(`
        <div class="indication-color is-spacer"></div>
        <div class="rs-table-title rsa-text is-line-item">${escapeHtml(label)}</div>
        <div class="rs-text-linline rsa-text is-xs"></div>
      `),
      rightHtml: rsaQuantityBlockHtml("")
    });
  }

  function rsaCommentScope(type, id, label) {
    return {
      type: type || "section",
      id: id || type || "comment",
      label: label || id || type || "comment"
    };
  }

  function commentScopeKey(type, id) {
    return `${type || "section"}:${id || ""}`;
  }

  function commentsForScope(scope) {
    const key = commentScopeKey(scope.type, scope.id);
    return (Array.isArray(state.data?.comments) ? state.data.comments : [])
      .filter((comment) => commentScopeKey(comment.scopeType, comment.scopeId) === key)
      .sort((a, b) => String(b.createdTime || "").localeCompare(String(a.createdTime || "")));
  }

  function commentScopeDisplay(scope) {
    if (scope?.type === "wave") return "Wave";
    return displayLabel(scope?.label || scope?.id || scope?.type || "Comments");
  }

  function commentScopeClass(scope) {
    const type = themeKey(scope?.type || "section") || "section";
    return `is-${type}`;
  }

  function rsaCommentPanelHtml(scope) {
    const key = commentScopeKey(scope.type, scope.id);
    const comments = commentsForScope(scope);
    const pending = state.pendingActions[pendingActionKey("add_comment", key)];
    const draft = state.commentDraftByScope[key] || "";
    const scopeName = commentScopeDisplay(scope);
    const title = `All ${scopeName} comments`;
    return `
      <div class="rsa-comment-panel ${escapeAttr(commentScopeClass(scope))}" data-rsa-comment-scope="${escapeAttr(scope.type)}" data-rsa-comment-id="${escapeAttr(scope.id)}" data-rsa-comment-label="${escapeAttr(scope.label)}">
        <div class="rsa-comment rsa-comment-head">
          <div class="rsa-comment-wrapper">
            <div class="rsa-comment-text rsa-text">${escapeHtml(title)}</div>
            <div class="rs-text-linline rsa-text is-link is-inline-edit rsa-comment-action" data-rsa-comment-add>${pending ? "saving" : "add"}</div>
          </div>
        </div>
        <div class="rsa-comment-list">
          ${comments.length ? comments.map(rsaCommentItemHtml).join("") : rsaCommentEmptyHtml(scope)}
        </div>
        <textarea class="rsa-comment-input rsa-text" rows="2" placeholder="${escapeAttr(`${scopeName} comment`)}" data-rsa-comment-input>${escapeHtml(draft)}</textarea>
      </div>
    `;
  }

  function rsaCommentItemHtml(comment) {
    const editable = comment.sourceTable === "wec_commenting";
    const editing = !!state.commentEditById[comment.id];
    const pending = state.pendingActions[pendingActionKey("update_comment", comment.id)];
    const value = state.commentEditValues[comment.id] ?? comment.comment ?? "";
    return `
      <div class="rsa-comment rsa-comment-item">
          <div class="rsa-comment-wrapper">
            ${editing
              ? `<textarea class="rsa-comment-input rsa-text" rows="2" data-rsa-comment-edit-input="${escapeAttr(comment.id)}">${escapeHtml(value)}</textarea>`
              : `<div class="rsa-comment-text rsa-text">${escapeHtml(comment.comment || "")}</div>`}
          <div class="rsa-comment-meta rsa-text">${escapeHtml(comment.createdBy || "webflow")}</div>
          <a class="rs-text-linline rsa-text is-xxs is-inline-edit rsa-comment-action" href="${escapeAttr(smsCommentItemHref(comment))}">sms</a>
          ${editable
            ? editing
              ? `<div class="rs-text-linline rsa-text is-xxs is-inline-edit" data-rsa-comment-save="${escapeAttr(comment.id)}">${pending ? "saving" : "save"}</div>`
              : `<div class="rs-text-linline rsa-text is-xxs is-inline-edit" data-rsa-comment-edit="${escapeAttr(comment.id)}" data-rsa-comment-text="${escapeAttr(comment.comment || "")}">edit</div>`
            : ""}
        </div>
      </div>
    `;
  }

  function rsaCommentEmptyHtml(scope) {
    return `
      <div class="rsa-comment rsa-comment-item">
        <div class="rsa-comment-wrapper">
          <div class="rsa-comment-text rsa-text">No ${escapeHtml(commentScopeDisplay(scope))} comments</div>
        </div>
      </div>
    `;
  }

  function rsaItemStatusLabel(item) {
    if (item.resolutionState) return resolutionDisplayLabel(item.resolutionState);
    if (isDone(item)) return "PACKED";
    if (number(item.packed) > 0) return "OPEN";
    return "NEED";
  }

  function rsaApplyListFilter(rows, filterKey) {
    const filter = state.filterByList[filterKey] || "all";
    if (filter === "packed") return rows.filter(isPackedListItem);
    if (filter === "need") return rows.filter((item) => !isDone(item));
    if (filter === "onsite") return rows.filter((item) => item.resolutionState === "purchase_onsite");
    if (filter === "attn") return rows.filter((item) => item.resolutionState === "note");
    if (filter === "open") return rows.filter((item) => !isDone(item));
    return rows;
  }

  function overviewHtml() {
    if (state.activeHomeModule) return homeModuleHtml(state.activeHomeModule);
    const searchKey = "overview";
    const summaries = filterRows([...homeModuleSummaries(), ...tabGroups()], searchKey, overviewSearchText);
    const rows = summaries.map((summary) => {
      const percent = progressPercent(summary.done, summary.rows);
      const triggerAttr = summary.homeModule
        ? `data-home-module="${escapeAttr(summary.id)}"`
        : `data-tab="${escapeAttr(summary.id)}"`;
      const printTarget = summary.homeModule ? `home:${summary.id}` : summary.id;
      return `
        <div class="lp-row packing-row packing-overview-row">
          <button class="packing-overview-tab-trigger" type="button" ${triggerAttr}>
            <span class="packing-progress" aria-label="${escapeAttr(`${percent}% complete`)}">
              <span class="packing-progress-fill" style="width: ${percent}%"></span>
            </span>
            <span class="lp-row-title">${escapeHtml(displayLabel(summary.label))}</span>
          </button>
          <button class="lp-filter-toggle packing-print-button" type="button" data-print-section="${escapeAttr(printTarget)}">PRINT LIST</button>
        </div>
      `;
    }).join("");

    return `
      <section class="lp-section-block packing-theme-overview">
        ${sectionTitleHtml(currentWaveLabel(), "overview")}
        <div class="lp-list">
          ${waveOverviewCountsHtml()}
          ${sectionSearchHtml(searchKey)}
          ${state.data.needsGeneration ? noWaveRowHtml() : rows || emptyRowHtml("No rows")}
        </div>
      </section>
    `;
  }

  function homeModuleHtml(moduleId) {
    const module = homeModules().find((row) => row.id === moduleId);
    if (!module) return emptyGroupHtml("No rows", "overview");
    const moduleLists = sortListsByLabel(module.lists || []);
    const activeList = activeListForGroup(module.id, moduleLists);
    return `
      <section class="lp-section-block ${themeClasses(module.id)}">
        ${sectionTitleHtml(module.label, `home:${module.id}`)}
        ${listSwitcherHtml(module.id, moduleLists, activeList?.id || "")}
        ${onsiteTaskRowsHtml(module, activeList)}
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
        ${tokenHtml("need", "NEED: 0")}
      </div>
    `;
  }

  function waveOverviewCountsHtml() {
    const wave = state.data?.wave;
    if (!wave) return "";
    return `
      <div class="lp-row is-static packing-wave-count-row">
        <span class="packing-wave-counts">
          ${waveCountStatHtml("HORSE COUNT", wave.effectiveHorseCount ?? wave.currentHorseCount ?? wave.horseCount)}
          ${waveCountStatHtml("GROOM RATIO", wave.groomRatio)}
          ${waveCountStatHtml("GROOM FINAL", wave.effectiveGroomCountFinal ?? wave.currentGroomCountFinal ?? wave.groomCountFinal)}
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
    const sortedGroupLists = sortListsByLabel(groupLists);
    if (!sortedGroupLists.length) return emptyGroupHtml(group?.label || "No rows", tabId);
    const activeList = activeListForGroup(group.id, sortedGroupLists);
    return `
      <section class="lp-section-block ${themeClasses(group.id)}">
        ${sectionTitleHtml(group.label, group.id)}
        ${listSwitcherHtml(group.id, sortedGroupLists, activeList.id)}
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
    const rows = sortItemsByName(filterRows(items().filter((item) => itemBelongsToList(item, list.id)), searchKey, itemSearchText));
    const editMode = state.inlineEditByList[list.id] || {};
    return `
      <div class="lp-list">
        ${listLabelRowHtml(list)}
        ${sectionSearchHtml(searchKey)}
        ${rows.length ? rows.map((item) => itemRowHtml(item, editMode, list.id)).join("") : emptyRowHtml("No rows")}
      </div>
    `;
  }

  function onsiteTaskRowsHtml(module, list) {
    const searchKey = module.id;
    const rows = sortOnsiteTasks(filterRows((module.tasks || []).filter((task) => onsiteTaskBelongsToList(task, list?.id)), searchKey, onsiteTaskSearchText));
    return `
      <div class="lp-list">
        ${onsiteListLabelRowHtml(list || module)}
        ${sectionSearchHtml(searchKey)}
        ${rows.length ? rows.map(onsiteTaskRowHtml).join("") : emptyRowHtml("No tasks")}
      </div>
    `;
  }

  function onsiteListLabelRowHtml(list) {
    return `
      <div class="lp-row is-static packing-list-action-row">
        <span class="lp-row-title">${escapeHtml(displayLabel(list?.label || list?.id || "Purchase onsite"))}</span>
      </div>
    `;
  }

  function onsiteTaskRowHtml(task) {
    const done = task.taskState === "done";
    const pending = isPendingAction("set_onsite_task_state", task.id);
    const nextState = done ? "task" : "done";
    return `
      <div class="lp-row packing-row packing-onsite-row">
        <button class="packing-overview-tab-trigger packing-onsite-detail-trigger" type="button" data-onsite-task-detail="${escapeAttr(task.id)}">
          <span class="lp-row-title">${escapeHtml(displayLabel(task.name || "Unnamed task"))}</span>
        </button>
        <button class="lp-achievement packing-token packing-token-button ${done ? "is-packed" : "is-need"} ${pending ? "is-pending" : ""}" type="button" data-onsite-task-state="${escapeAttr(nextState)}" data-source-item-id="${escapeAttr(task.id)}" ${pending ? `disabled aria-busy="true"` : ""}>
          ${pending ? "SAVING" : done ? "DONE" : "TASK"}
        </button>
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
    const sortedLists = sortListsByLabel(groupLists);
    return `
      <div class="packing-list-switcher" aria-label="Packing lists">
        ${sortedLists.map((list) => `
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
          <span class="packing-hottext-prefix">EDIT:</span>
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
          <div class="lp-row is-static packing-horse-label-row">
            <span class="lp-row-title">${escapeHtml(displayLabel(`${currentWaveLabel()} horses`))}</span>
          </div>
          ${sectionSearchHtml(searchKey)}
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

  function sectionSearchHtml(searchKey, placeholder = "Search this section") {
    return `
      <div class="packing-section-search">
        <input class="lp-edit-input packing-section-search-input" type="search" placeholder="${escapeAttr(placeholder)}" value="${escapeAttr(sectionSearchValue(searchKey))}" data-section-search="${escapeAttr(searchKey)}">
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
      ...(Array.isArray(item.placeLabels) ? item.placeLabels : []),
      ...(Array.isArray(item.localTags) ? item.localTags : []),
      ...(Array.isArray(item.horseMembers) ? item.horseMembers.map((member) => member.barnName) : []),
      ...(Array.isArray(item.sourceItems) ? item.sourceItems.map((source) => `${source.appName || ""} ${source.longDescription || ""}`) : [])
    ].filter(Boolean).join(" ");
  }

  function horseSearchText(horse) {
    return [horse.name, horse.barnName, horse.showName, horse.notes].filter(Boolean).join(" ");
  }

  function horseItemSearchText(row) {
    return [
      row.item?.name,
      row.item?.itemId,
      row.item?.location,
      row.item?.listPlanLabel,
      row.member?.barnName,
      row.member?.notes
    ].filter(Boolean).join(" ");
  }

  function itemRowHtml(item, editMode, listId) {
    const hasInlineEdit = !!(editMode?.["lp-row-title"] || editMode?.quantity_packed_override || editMode?.quantity_needed_override);
    const rowTag = hasInlineEdit ? "div" : "button";
    const rowAttrs = hasInlineEdit ? "" : `type="button" data-item-id="${escapeAttr(item.id)}"`;
    return `
      <${rowTag} class="lp-row packing-row ${hasInlineEdit ? "is-inline-editing" : ""}" ${rowAttrs}>
        <span class="packing-inline-main">
          ${itemTitleHtml(item, editMode)}
          ${inlineEditControlsHtml(item, editMode, listId)}
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

  function inlineEditControlsHtml(item, editMode, listId) {
    const fields = [];
    if (editMode?.quantity_packed_override) fields.push(inlineQuantityInputHtml(item, "quantity_packed_override", "PACKED"));
    if (editMode?.quantity_needed_override) fields.push(inlineQuantityInputHtml(item, "quantity_needed_override", "NEEDED"));
    if (!editMode?.["lp-row-title"] && !fields.length) return "";
    const pending = isPendingAction("update_item_fields", item.id);
    return `
      <span class="packing-inline-edit-fields">
        ${fields.join("")}
        <button class="packing-hottext packing-inline-save ${pending ? "is-pending" : ""}" type="button" data-list-id="${escapeAttr(listId)}" data-inline-save-item="${escapeAttr(item.id)}" ${pending ? "disabled" : ""}>
          ${pending ? "SAVING" : "SAVE"}
        </button>
      </span>
    `;
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
    if (number(item.packed) > 0) return tokenHtml("open", `LEFT: ${quantityDisplay(item.left)}`);
    return tokenHtml("need", `NEED: ${quantityDisplay(item.needed)}`);
  }

  function tokenHtml(type, text) {
    return `<span class="lp-achievement packing-token is-${escapeAttr(type)}">${escapeHtml(text)}</span>`;
  }

  function printSection(target) {
    if (!state.data) return;
    const title = target === "overview" ? "WEC Packing Report" : `${printTargetTitle(target)} Packing List`;
    openPackingPrintPage({
      target,
      filename: `${safeFilename(`${currentWaveLabel()} ${title}`)}.pdf`
    });
  }

  function printHorseList(horseId) {
    if (!state.data) return;
    const horse = horses().find((row) => row.id === horseId);
    if (!horse) return;
    openPackingPrintPage({
      horseId,
      filename: `${safeFilename(`${currentWaveLabel()} ${horseDisplayName(horse)} Packing List`)}.pdf`
    });
  }

  function printLinkAttrs(options) {
    return `href="${escapeAttr(packingPrintTargetUrl(options).toString())}" target="_blank" rel="noopener"`;
  }

  function packingPrintTargetUrl(options = {}) {
    const printUrl = new URL(printPageUrl || config.printUrl || `${apiBaseUrl}/print`, window.location.href);
    addContextParams(printUrl);
    if (options.target) printUrl.searchParams.set("target", options.target);
    if (options.horseId) printUrl.searchParams.set("horseId", options.horseId);
    printUrl.searchParams.set("autoprint", "1");
    return config.usePdfWorker
      ? packingPdfWorkerUrl(pdfWorkerSafePrintUrl(printUrl), options.filename || "wec-packing.pdf")
      : printUrl;
  }

  function openPackingPrintPage(options) {
    const targetUrl = packingPrintTargetUrl(options);
    const opened = window.open(targetUrl.toString(), "_blank", "noopener");
    if (!opened) {
      state.saveMessage = "Popup blocked. Allow popups and press Print again.";
      render();
      return;
    }

    state.saveMessage = config.usePdfWorker ? "Creating PDF..." : "Opening print page...";
    render();
    state.saveMessage = config.usePdfWorker ? "PDF opened." : "Print page opened.";
    render();
  }

  function pdfWorkerSafePrintUrl(printUrl) {
    const safeUrl = new URL(printUrl.toString());
    if (safeUrl.hostname === "ringstatus.webflow.io") safeUrl.hostname = "ringstatus.com";
    return safeUrl;
  }

  function packingPdfWorkerUrl(printUrl, filename) {
    const pdfUrl = new URL(pdfWorkerUrl || "https://ringstatus-pdf.gombcg.workers.dev/");
    pdfUrl.searchParams.set("url", printUrl.toString());
    pdfUrl.searchParams.set("filename", filename);
    return pdfUrl;
  }

  function printBodyHtml(target) {
    if (target === "overview") {
      const pages = tabGroups().map((group) => printPackingPageHtml(group.label, printListSections(group.id))).join("");
      return `${pages}${printHorsesPageHtml()}`;
    }
    if (target === "horses") return printHorsesPageHtml();
    if (String(target || "").startsWith("home:")) return printHomeModulePrintHtml(target);
    return printPackingPageHtml(printTargetTitle(target), printListSections(target));
  }

  function printTargetTitle(target) {
    if (target === "horses") return "Horses";
    if (isTabGroupId(target)) {
      return displayLabel(tabGroups().find((group) => group.id === target)?.label || target.replace(/^tab:/, ""));
    }
    if (String(target || "").startsWith("home:")) {
      return displayLabel(homeModules().find((module) => `home:${module.id}` === target)?.label || target.replace(/^home:/, ""));
    }
    return displayLabel(lists().find((list) => list.id === target)?.label || target);
  }

  function printHomeModulePrintHtml(target) {
    const moduleId = String(target || "").replace(/^home:/, "");
    const module = homeModules().find((row) => row.id === moduleId);
    if (!module) return printPackingPageHtml(printTargetTitle(target), []);
    const sections = (module.lists || []).map((list) => ({
      title: displayLabel(list.label || list.id),
      rows: (module.tasks || []).filter((task) => (task.packListIds || []).includes(list.id))
    })).filter((section) => section.rows.length);
    const rows = sections.flatMap((section) => section.rows);
    const percent = progressPercent(rows.filter((task) => task.taskState === "done").length, rows.length);
    return `
      <section class="packing-print-page">
        <header class="packing-print-head">
          <h1>${escapeHtml(displayLabel(module.label || "Purchase Onsite"))}</h1>
          <p>${escapeHtml(statusLine())} | ${percent}% packed | Printed: ${escapeHtml(printDateDisplay())}</p>
        </header>
        <div class="packing-print-columns">
          ${sections.length ? sections.map(printHomeTaskColumnHtml).join("") : printEmptyPrintSectionHtml("No rows")}
        </div>
      </section>
    `;
  }

  function printHomeTaskColumnHtml(section) {
    return `
      <section class="packing-print-list ${printDensityClass(section.rows)}">
        <h2>${escapeHtml(section.title)}</h2>
        ${section.rows.length ? section.rows.map(printHomeTaskRowHtml).join("") : printEmptyPrintSectionHtml("No rows")}
      </section>
    `;
  }

  function printHomeTaskRowHtml(task) {
    const done = task.taskState === "done";
    return `
      <div class="packing-print-item ${done ? "is-packed" : ""}">
        <div class="packing-print-item-main">
          <strong class="packing-print-item-name">${escapeHtml(displayLabel(task.name || "Unnamed task"))}</strong>
        </div>
        <span class="packing-print-metrics">${done ? "Done" : "Task"}</span>
        <span class="packing-print-scratch" aria-hidden="true"></span>
      </div>
    `;
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
    const chunks = printSectionChunks(sections);
    return chunks.map((chunk) => printPackingPageChunkHtml(title, chunk, percent)).join("");
  }

  function printPackingPageChunkHtml(title, sections, percent) {
    return `
      <section class="packing-print-page">
        <header class="packing-print-head">
          <h1>${escapeHtml(displayLabel(title))}</h1>
          <p>${escapeHtml(statusLine())} | ${percent}% packed | Printed: ${escapeHtml(printDateDisplay())}</p>
        </header>
        <div class="packing-print-columns">
          ${sections.length ? sections.map(printListColumnHtml).join("") : printEmptyPrintSectionHtml("No rows")}
        </div>
      </section>
    `;
  }

  function printListColumnHtml(section) {
    return `
      <section class="packing-print-list ${printDensityClass(section.rows)}">
        <h2>${escapeHtml(section.title)}</h2>
        ${section.rows.length ? section.rows.map(printItemRowHtml).join("") : printEmptyPrintSectionHtml("No rows")}
      </section>
    `;
  }

  function printItemRowHtml(item) {
    const packed = isDone(item);
    return `
      <div class="packing-print-item ${packed ? "is-packed" : ""}">
        <div class="packing-print-item-main">
          <strong class="packing-print-item-name">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</strong>
        </div>
        <span class="packing-print-metrics">${printQuantityMetricsHtml(item.needed, item.packed, item.left)}</span>
        <span class="packing-print-scratch" aria-hidden="true"></span>
      </div>
    `;
  }

  function printQuantityMetricsHtml(needed, packed, left) {
    return [
      `Need: ${quantityDisplay(needed)}`,
      `Packed: ${quantityDisplay(packed)}`,
      `Left: ${quantityDisplay(left)}`
    ].map(escapeHtml).join(" ");
  }

  function printHorsesPageHtml() {
    const rows = activeWaveHorses();
    const columns = splitRows(rows);
    const percent = horsePackingPercent();
    return `
      <section class="packing-print-page">
        <header class="packing-print-head">
          <h1>Horses</h1>
          <p>${escapeHtml(statusLine())} | ${percent}% packed | Printed: ${escapeHtml(printDateDisplay())}</p>
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
          <p>${escapeHtml(currentWaveLabel())} | ${progress.percent}% packed | Printed: ${escapeHtml(printDateDisplay())}</p>
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
      <section class="packing-print-list ${printDensityClass(rows)}">
        <h2>${escapeHtml(title)}</h2>
        ${rows.length ? rows.map(printHorsePackingItemHtml).join("") : printEmptyPrintSectionHtml("No rows")}
      </section>
    `;
  }

  function printHorsePackingItemHtml(row) {
    const needed = number(row.member.needed);
    const packed = number(row.member.packed);
    const left = Math.max(0, needed - packed);
    const done = isHorseMemberPacked(row.member);
    return `
      <div class="packing-print-item ${done ? "is-packed" : ""}">
        <div class="packing-print-item-main">
          <strong class="packing-print-item-name">${escapeHtml(displayLabel(row.item.name || "Unnamed item"))}</strong>
        </div>
        <span class="packing-print-metrics">${printQuantityMetricsHtml(needed, packed, left)}</span>
        <span class="packing-print-scratch" aria-hidden="true"></span>
      </div>
    `;
  }

  function printHorseColumnHtml(title, rows) {
    return `
      <section class="packing-print-list ${printDensityClass(rows)}">
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

  function printSectionChunks(sections) {
    const chunks = [];
    for (let index = 0; index < sections.length; index += 2) {
      chunks.push(sections.slice(index, index + 2));
    }
    return chunks.length ? chunks : [[]];
  }

  function printDensityClass(rows = []) {
    if (rows.length >= 22) return "is-ultra-dense";
    if (rows.length >= 14) return "is-dense";
    return "";
  }

  function printDateDisplay() {
    return new Date().toLocaleDateString("en-US", { timeZone: "America/New_York" });
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
        page-break-inside: avoid;
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
        grid-template-columns: minmax(0, 1fr) auto 0.42in;
        gap: 8px;
        align-items: center;
        padding: 7px 10px;
        border-bottom: 1px solid #eeeeee;
      }
      .packing-print-horse {
        grid-template-columns: minmax(0, 1fr);
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
      .packing-print-item.is-packed .packing-print-item-name {
        opacity: 0.6;
        text-decoration: line-through;
        text-decoration-thickness: 1px;
      }
      .packing-print-metrics {
        color: #333333;
        font-size: 9px;
        font-weight: 600;
        line-height: 1.1;
        text-align: right;
        white-space: nowrap;
      }
      .packing-print-scratch {
        display: block;
        width: 0.42in;
        height: 0.2in;
        border: 1px solid #cfcfcf;
        border-radius: 3px;
        background: #ffffff;
      }
      .packing-print-list.is-dense h2 {
        padding: 6px 8px;
        font-size: 11px;
      }
      .packing-print-list.is-dense .packing-print-item,
      .packing-print-list.is-dense .packing-print-horse {
        padding: 5px 8px;
        gap: 6px;
      }
      .packing-print-list.is-ultra-dense h2 {
        padding: 5px 7px;
        font-size: 10px;
      }
      .packing-print-list.is-ultra-dense .packing-print-item,
      .packing-print-list.is-ultra-dense .packing-print-horse {
        padding: 3px 7px;
        gap: 5px;
      }
      .packing-print-list.is-ultra-dense .packing-print-item strong,
      .packing-print-list.is-ultra-dense .packing-print-horse strong {
        font-size: 9px;
      }
      .packing-print-list.is-ultra-dense .packing-print-metrics {
        font-size: 7px;
      }
      .packing-print-list.is-ultra-dense .packing-print-scratch {
        height: 0.15in;
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
      : state.detailType === "onsite"
        ? onsiteTaskDetailHtml(onsiteTasks().find((task) => task.id === state.detailId))
        : state.detailType === "place"
          ? placeDetailHtml(placeDetailRecord(state.detailId))
          : itemDetailHtml(items().find((item) => item.id === state.detailId));
  }

  function itemDetailHtml(item) {
    if (!item) return "";
    return `
      <div class="lp-profile-shell packing-detail-shell ${themeClasses(item.packListIds?.[0] || "overview")}">
        <div class="lp-profile-head wec-profile-top">
          <h2 class="lp-profile-title rsa-H1" id="drawerTitle">${escapeHtml(displayLabel(item.name || "Unnamed item"))}</h2>
          <p class="lp-profile-subtitle rsa-p">${escapeHtml(itemMetaLabel(item))}</p>
        </div>

        <section class="lp-profile-panel packing-detail wec-detail-section">
          <div data-wec-record="${escapeAttr(item.id)}" data-wec-name="${escapeAttr(item.name || "")}">
            <div class="lp-field-grid lp-profile-tab-panel is-active">
              ${statusControlHtml(item)}
              ${totalsRowHtml(item)}
              ${packedControlHtml(item)}
              ${decisionControlHtml(item)}
              ${horseMembersControlHtml(item)}
              ${detailInfoListControlHtml("Places", itemPlaceLabels(item))}
              ${detailInfoListControlHtml("Tags", itemLocalTags(item))}
            </div>
          </div>
        </section>

        ${rsaCommentPanelHtml(rsaCommentScope("item", item.id, item.name || "Unnamed item"))}

        <div class="lp-profile-modal-footer wec-profile-footer">
          ${planLineHtml(itemPlanText(item))}
          <div class="lp-profile-footer packing-save-meta rsa-p ${saveMetaClass()}">
            <span>${escapeHtml(state.saveMessage || "Changes save to Airtable through Webflow Cloud.")}</span>
          </div>
        </div>
      </div>
    `;
  }

  function horseDetailHtml(horse) {
    if (!horse) return "";
    const progress = horseProgress(horse);
    const searchKey = `horse-detail:${horse.id}`;
    const rows = filterRows(horseItemRows(horse), searchKey, horseItemSearchText);
    const planText = horsePlanText(horse);
    return `
      <div class="lp-profile-shell packing-detail-shell packing-horse-detail-shell packing-theme-horses">
        <div class="lp-profile-head wec-profile-top">
          <h2 class="lp-profile-title rsa-H1" id="drawerTitle">${escapeHtml(horse.name || "Unnamed horse")}</h2>
          ${horse.showName ? `<p class="lp-profile-subtitle rsa-p">${escapeHtml(horse.showName)}</p>` : ""}
        </div>

        <section class="lp-profile-panel packing-detail wec-detail-section">
          <div data-wec-record="${escapeAttr(horse.id)}" data-wec-name="${escapeAttr(horse.name || "")}">
            <div class="lp-field-grid lp-profile-tab-panel is-active">
              <div class="lp-row is-static packing-horse-progress-row">
                <span class="packing-horse-progress-main">
                  <span class="packing-tab-percent packing-horse-progress-percent">${escapeHtml(`${progress.percent}% PACKED`)}</span>
                  <span class="packing-progress" aria-label="${escapeAttr(`${progress.percent}% packed`)}">
                    <span class="packing-progress-fill" style="width: ${progress.percent}%"></span>
                  </span>
                </span>
              </div>
              <div class="lp-row is-static packing-horse-label-row">
                <span class="lp-row-title">PACKING ITEMS</span>
              </div>
              ${sectionSearchHtml(searchKey, "Search packing items")}
              <span class="packing-horse-bindings packing-horse-item-list">
                ${rows.length ? rows.map(horseDetailItemRowHtml).join("") : `<span class="packing-horse-binding-row"><span class="packing-horse-binding-name">No horse-specific items</span></span>`}
              </span>
            </div>
          </div>
        </section>

        <div class="lp-profile-modal-footer wec-profile-footer">
          ${planLineHtml(planText)}
          <div class="lp-profile-footer packing-save-meta rsa-p ${saveMetaClass()}">
            <span>${escapeHtml(state.saveMessage || "Changes save to Airtable through Webflow Cloud.")}</span>
          </div>
        </div>
      </div>
    `;
  }

  function onsiteTaskDetailHtml(task) {
    if (!task) return "";
    return `
      <div class="lp-profile-shell packing-detail-shell packing-theme-overview">
        <div class="lp-profile-head wec-profile-top">
          <h2 class="lp-profile-title rsa-H1" id="drawerTitle">${escapeHtml(displayLabel(task.name || "Unnamed task"))}</h2>
          <p class="lp-profile-subtitle rsa-p">Purchase Onsite</p>
        </div>

        <section class="lp-profile-panel packing-detail wec-detail-section">
          <div data-wec-record="${escapeAttr(task.id)}" data-wec-name="${escapeAttr(task.name || "")}">
            <div class="lp-field-grid lp-profile-tab-panel is-active">
              ${onsiteTaskStatusControlHtml(task)}
              ${task.longDescription ? editGroupHtml("Details", `<span class="lp-row-meta">${escapeHtml(task.longDescription)}</span>`) : ""}
              ${detailInfoListControlHtml("Places", task.placeLabels)}
              ${detailInfoListControlHtml("Tags", task.localTags)}
            </div>
          </div>
        </section>

        <div class="lp-profile-modal-footer wec-profile-footer">
          ${planLineHtml(task.listPlanLabel || task.listPlan)}
          <div class="lp-profile-footer packing-save-meta rsa-p ${saveMetaClass()}">
            <span>${escapeHtml(state.saveMessage || "Changes save to Airtable through Webflow Cloud.")}</span>
          </div>
        </div>
      </div>
    `;
  }

  function placeDetailHtml(place) {
    if (!place) return "";
    const attributes = overviewDetailFilterLabels(place);
    return `
      <div class="lp-profile-shell packing-detail-shell packing-theme-overview">
        <div class="lp-profile-head wec-profile-top">
          <h2 class="lp-profile-title rsa-H1" id="drawerTitle">${escapeHtml(displayLabel(place.label || "Place"))}</h2>
          ${place.meta ? `<p class="lp-profile-subtitle rsa-p">${escapeHtml(displayLabel(place.meta))}</p>` : ""}
        </div>

        <section class="lp-profile-panel packing-detail wec-detail-section">
          <div data-wec-record="${escapeAttr(place.id)}" data-wec-name="${escapeAttr(place.label || "")}">
            <div class="lp-field-grid lp-profile-tab-panel is-active">
              ${detailInfoListControlHtml("Tags", attributes)}
              ${placeAttributeControlsHtml(place.attributes)}
              ${placeTextControlHtml("Phone", place.phone)}
              ${placeClickoutControlHtml("Map", place.mapsUrl)}
              ${placeClickoutControlHtml("Website", place.website)}
              ${placeTextControlHtml("Record", place.id)}
            </div>
          </div>
        </section>

        <div class="lp-profile-modal-footer wec-profile-footer">
          <div class="lp-profile-footer packing-save-meta rsa-p ${saveMetaClass()}">
            <span>${escapeHtml(state.saveMessage || "Place details are from Airtable.")}</span>
          </div>
        </div>
      </div>
    `;
  }

  function placeAttributeControlsHtml(attributes) {
    const rows = Array.isArray(attributes) ? attributes : [];
    return rows.map((row) => placeTextControlHtml(row.label, row.value)).join("");
  }

  function placeTextControlHtml(label, value) {
    const text = String(value || "").trim();
    if (!text) return "";
    return editGroupHtml(label, `<span class="lp-row-meta">${escapeHtml(text)}</span>`);
  }

  function placeClickoutControlHtml(label, url) {
    const href = String(url || "").trim();
    if (!href) return "";
    return editGroupHtml(label, rsaOpenActionHtml(`href="${escapeAttr(href)}" target="_blank" rel="noopener"`));
  }

  function onsiteTaskStatusControlHtml(task) {
    const done = task.taskState === "done";
    return editGroupHtml("Task", `
      <span class="lp-edit-choice-row packing-inline-choices">
        ${choiceButtonHtml({
          label: "TASK",
          active: !done,
          attrs: `data-onsite-task-state="task" data-source-item-id="${escapeAttr(task.id)}"`
        })}
        ${choiceButtonHtml({
          label: "DONE",
          active: done,
          attrs: `data-onsite-task-state="done" data-source-item-id="${escapeAttr(task.id)}"`
        })}
      </span>
    `);
  }

  function detailInfoListControlHtml(label, values) {
    const list = Array.isArray(values) ? values.filter(Boolean) : [];
    if (!list.length) return "";
    return editGroupHtml(label, `
      <span class="packing-horse-bindings packing-detail-info-list">
        ${uniqueDisplayValues(list).map((value) => `
          <span class="packing-horse-binding-row packing-detail-info-row">
            <span class="packing-horse-binding-name">${escapeHtml(displayLabel(value))}</span>
          </span>
        `).join("")}
      </span>
    `);
  }

  function itemPlaceLabels(item) {
    return uniqueDisplayValues([
      ...(Array.isArray(item?.placeLabels) ? item.placeLabels : []),
      ...(Array.isArray(item?.places) ? item.places.map((place) => place.label || place.name) : []),
      ...(Array.isArray(item?.sourceItems) ? item.sourceItems.flatMap((source) => [
        ...(Array.isArray(source.placeLabels) ? source.placeLabels : []),
        ...(Array.isArray(source.places) ? source.places.map((place) => place.label || place.name) : [])
      ]) : [])
    ]);
  }

  function itemLocalTags(item) {
    return uniqueDisplayValues([
      ...(Array.isArray(item?.localTags) ? item.localTags : []),
      ...(Array.isArray(item?.places) ? item.places.flatMap((place) => place.localTags || []) : []),
      ...(Array.isArray(item?.sourceItems) ? item.sourceItems.flatMap((source) => [
        ...(Array.isArray(source.localTags) ? source.localTags : []),
        ...(Array.isArray(source.places) ? source.places.flatMap((place) => place.localTags || []) : [])
      ]) : [])
    ]);
  }

  function uniqueDisplayValues(values) {
    const seen = new Set();
    const result = [];
    for (const value of values || []) {
      const text = String(value || "").trim();
      const key = text.toLowerCase();
      if (!text || seen.has(key)) continue;
      seen.add(key);
      result.push(text);
    }
    return result;
  }

  function horseDetailItemRowHtml(row) {
    const packed = isHorseMemberPacked(row.member);
    const nextState = packed ? "not_packed" : "packed";
    return `
      <span class="packing-horse-binding-row packing-horse-pack-row">
        <span class="packing-horse-binding-name">${escapeHtml(displayLabel(row.item?.name || "Unnamed item"))}</span>
        <button class="lp-achievement packing-token ${packed ? "is-packed" : "is-need"}" type="button" data-horse-member-state="${escapeAttr(nextState)}" data-item-horse-id="${escapeAttr(row.member.id)}">
          ${packed ? "PACKED" : "NOT PACKED"}
        </button>
      </span>
    `;
  }

  function planLineHtml(value) {
    const text = String(value || "").trim();
    if (!text) return "";
    return `<div class="packing-plan-line rsa-text is-caps">PLAN: ${escapeHtml(text)}</div>`;
  }

  function itemPlanText(item) {
    return displayLabel(
      item?.listPlanLabel ||
      item?.listPlan ||
      item?.quantityCalculation?.plan ||
      item?.sourceItems?.[0]?.listPlanLabel ||
      item?.sourceItems?.[0]?.listPlan ||
      ""
    );
  }

  function horsePlanText(horse) {
    const plans = [];
    const seen = new Set();
    for (const row of horseItemRows(horse)) {
      const label = itemPlanText(row.item);
      const key = themeKey(label);
      if (!label || seen.has(key)) continue;
      seen.add(key);
      plans.push(label);
    }
    return plans.join(", ");
  }

  function detailReadRow(label, value) {
    return editGroupHtml(label, `<input class="lp-edit-input wec-input wec-readonly-input rsa-text" type="text" value="${escapeAttr(value)}" readonly tabindex="-1">`);
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
    const pending = isPendingAction("add_quantity", item.id);
    const pendingSource = pendingActionSource("add_quantity", item.id);
    return editGroupHtml("Packed", `
      <span class="packing-add-control">
        <span class="packing-add-box">
          <input class="lp-edit-input rsa-text" type="number" min="0" step="1" inputmode="numeric" placeholder="0" value="${escapeAttr(state.addQty[item.id] || "")}" data-add-qty="${escapeAttr(item.id)}">
          <span class="packing-add-label rsa-text is-caps">QUANTITY</span>
        </span>
        <button class="lp-edit-pill rsa-text is-caps ${pending && pendingSource === "add_quantity" ? "is-active is-pending" : ""}" type="button" data-packing-action="add_quantity" data-item-id="${escapeAttr(item.id)}" ${pending ? `disabled aria-busy="true"` : ""}>ADD</button>
        <button class="lp-edit-pill rsa-text is-caps ${pending && pendingSource === "add_one" ? "is-active is-pending" : ""}" type="button" data-packing-action="add_one" data-item-id="${escapeAttr(item.id)}" ${pending ? `disabled aria-busy="true"` : ""}>ADD + 1</button>
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
              <button class="lp-edit-pill packing-horse-binding-toggle rsa-text is-caps ${packed ? "is-active" : ""}" type="button" data-horse-member-state="${packed ? "not_packed" : "packed"}" data-item-horse-id="${escapeAttr(member.id)}">
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
    return editGroupHtml("Notes", `<textarea class="lp-edit-input wec-input wec-note-input rsa-text" rows="4" data-action-notes="${escapeAttr(item.id)}">${escapeHtml(value)}</textarea>`, "wec-detail-note");
  }

  function decisionControlHtml(item) {
    const optionsOpen = !!state.decisionOpenByItem[item.id] || isMaxConflictState(item.resolutionState);
    return editGroupHtml("Decision", `
      <span class="lp-edit-choice-row packing-inline-choices packing-decision-open">
        ${choiceButtonHtml({
          label: "OPEN",
          active: optionsOpen,
          attrs: `data-decision-open="${escapeAttr(item.id)}"`
        })}
      </span>
    `) + (optionsOpen ? maxConflictControlHtml(item) : "");
  }

  function maxConflictControlHtml(item) {
    const decisions = [
      ["clear", "CLEAR"],
      ["max", "MAX"],
      ["purchase_onsite", "BUY"],
      ["note", "ATTN"]
    ];
    return editGroupHtml("Options", `
      <span class="lp-edit-choice-row packing-inline-choices packing-decision-choices packing-conflict-choices">
        ${decisions.map(([decision, label]) => choiceButtonHtml({
          label,
          active: item.resolutionState === decision,
          attrs: `data-packing-action="set_resolution" data-item-id="${escapeAttr(item.id)}" data-resolution-state="${escapeAttr(decision)}"`
        })).join("")}
        <a class="lp-edit-pill packing-sms-pill rsa-text is-caps" href="${escapeAttr(smsConflictHref(item))}">SMS</a>
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
          ${editGroupHtml("Name", `<span class="lp-row-meta rsa-p">${escapeHtml(source.appName || "")}</span>`)}
          ${editGroupHtml("Plan", `<span class="lp-row-meta rsa-p">${escapeHtml(displayLabel(source.listPlanLabel || source.listPlan || ""))}</span>`)}
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
    return `<button class="lp-edit-pill rsa-text is-caps ${active ? "is-active" : ""}" type="button" data-source-flag="${escapeAttr(flagName)}" data-source-item-id="${escapeAttr(sourceItemId)}" data-next-value="${active ? "false" : "true"}">${escapeHtml(label)}</button>`;
  }

  function choiceButtonHtml({ label, active, attrs }) {
    return `<span class="lp-edit-choice"><button class="lp-edit-pill rsa-text is-caps ${active ? "is-active" : ""}" type="button" ${attrs}>${escapeHtml(label)}</button></span>`;
  }

  function editGroupHtml(title, body, extraClass) {
    return `
      <div class="lp-field-row wec-detail-edit ${extraClass || ""}">
        <span class="lp-field-label rsa-text is-caps">${escapeHtml(title)}</span>
        <span class="lp-field-value rsa-p">${body}</span>
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
    return isPackedListItem(item) || !!item.resolutionState;
  }

  function isPackedListItem(item) {
    const needed = number(item.needed);
    return (needed > 0 && number(item.packed) >= needed) || item.resolutionState === "max";
  }

  function items() {
    return Array.isArray(state.data?.items) ? state.data.items : [];
  }

  function homeModules() {
    return Array.isArray(state.data?.homeModules) ? state.data.homeModules : [];
  }

  function homeModuleSummaries() {
    return homeModules().map((module) => ({
      id: module.id,
      label: module.label,
      rows: module.rows,
      done: module.done,
      open: module.open,
      homeModule: true
    }));
  }

  function activeListLanes() {
    return Array.isArray(state.data?.activeListLanes) ? state.data.activeListLanes : [];
  }

  function activeListDetails() {
    return Array.isArray(state.data?.activeListDetails) ? state.data.activeListDetails : [];
  }

  function activeLaneGroup(lane) {
    return activeListLanes().find((group) => group.lane === lane || group.id === lane) || null;
  }

  function activeLaneRows(lane) {
    const group = activeLaneGroup(lane);
    return group ? (Array.isArray(group.lists) ? group.lists : []) : null;
  }

  function activeLaneListById(listId) {
    for (const group of activeListLanes()) {
      const row = (group.lists || []).find((list) => list.id === listId);
      if (row) return row;
    }
    return null;
  }

  function activeListDetailById(listId) {
    return activeListDetails().find((detail) => detail.id === listId) || null;
  }

  function placeDetailRecord(placeId) {
    for (const detail of activeListDetails()) {
      const place = (detail.rows || []).find((row) => row.type === "place" && row.id === placeId);
      if (place) return place;
    }
    return null;
  }

  function overviewListDetailRows(summary) {
    if (summary.key === "unresolved") return sortTableRows(items().filter((item) => !isDone(item)), "overview");
    if (Array.isArray(summary.rows)) return summary.rows;
    return activeListDetailById(summary.id)?.rows || [];
  }

  function filterOverviewDetailRows(summary, rows, searchKey) {
    const searchedRows = filterRows(rows, searchKey, overviewDetailSearchText);
    const activeFilter = state.filterByList[searchKey] || "all";
    if (summary.lane !== "locale" || activeFilter === "all") return searchedRows;
    return searchedRows.filter((row) => overviewDetailFilterKeys(row).includes(activeFilter));
  }

  function overviewDetailFilterOptions(summary, rows) {
    if (summary.lane !== "locale") return [];
    const labels = [];
    const seen = new Set();
    for (const row of rows || []) {
      for (const label of overviewDetailFilterLabels(row)) {
        const key = themeKey(label);
        if (!key || seen.has(key)) continue;
        seen.add(key);
        labels.push([key, displayLabel(label)]);
      }
    }
    labels.sort((a, b) => a[1].localeCompare(b[1], undefined, { sensitivity: "base" }));
    return [["all", "ALL"], ...labels];
  }

  function overviewDetailFilterKeys(row) {
    return overviewDetailFilterLabels(row).map(themeKey).filter(Boolean);
  }

  function overviewDetailFilterLabels(row) {
    return String(row?.meta || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
  }

  function overviewDetailSearchText(row) {
    return [
      row?.label,
      row?.meta,
      row?.phone,
      row?.website,
      row?.mapsUrl,
      row?.name,
      row?.itemId,
      row?.location,
      row?.listPlanLabel
    ].filter(Boolean).join(" ");
  }

  function detailTableLabel(summary) {
    if (summary.key === "unresolved" || summary.key === "item_labels") return "item";
    if (summary.key === "list_labels") return "list";
    if (summary.lane === "locale") return "place";
    return "list";
  }

  function detailTableActionLabel(summary) {
    return summary.key === "unresolved" ? "input" : "open";
  }

  function detailTableMetricLabels(summary) {
    return summary.key === "unresolved" ? undefined : ["", "", ""];
  }

  function overviewModeLabel(mode) {
    if (mode === "packing") return "Pack Lists";
    if (mode === "task_lists") return "Item Tasks";
    if (mode === "custom") return "Custom";
    if (mode === "locale") return "Locale";
    if (mode === "search_list") return "Search";
    return currentWaveLabel();
  }

  function onsiteTasks() {
    return homeModules().flatMap((module) => Array.isArray(module.tasks) ? module.tasks : []);
  }

  function onsiteTaskBelongsToList(task, listId) {
    if (!listId) return true;
    return Array.isArray(task?.packListIds) && task.packListIds.includes(listId);
  }

  function onsiteTaskSearchText(task) {
    return [
      task?.name,
      task?.longDescription,
      task?.note,
      ...(Array.isArray(task?.packListLabels) ? task.packListLabels : []),
      ...(Array.isArray(task?.placeLabels) ? task.placeLabels : []),
      ...(Array.isArray(task?.localTags) ? task.localTags : [])
    ].filter(Boolean).join(" ");
  }

  function sortOnsiteTasks(rows) {
    return [...rows].sort((a, b) => {
      const nameCompare = displayLabel(a.name || "").localeCompare(displayLabel(b.name || ""), undefined, { sensitivity: "base" });
      if (nameCompare) return nameCompare;
      return String(a.id || "").localeCompare(String(b.id || ""), undefined, { sensitivity: "base" });
    });
  }

  function snapshotOnsiteTaskState(sourceItemId) {
    const modules = homeModules();
    const task = onsiteTasks().find((row) => row.id === sourceItemId);
    if (!task) return null;
    return {
      sourceItemId,
      taskState: task.taskState,
      done: task.done,
      modules: modules.map((module) => ({
        id: module.id,
        rows: module.rows,
        done: module.done,
        open: module.open,
        lists: (module.lists || []).map((list) => ({
          id: list.id,
          rows: list.rows,
          done: list.done,
          open: list.open
        }))
      }))
    };
  }

  function applyLocalOnsiteTaskState(sourceItemId, taskState) {
    for (const module of homeModules()) {
      const task = (module.tasks || []).find((row) => row.id === sourceItemId);
      if (!task) continue;
      task.taskState = taskState;
      task.done = taskState === "done";
      recomputeOnsiteModule(module);
    }
  }

  function restoreOnsiteTaskState(snapshot) {
    if (!snapshot) return;
    const task = onsiteTasks().find((row) => row.id === snapshot.sourceItemId);
    if (task) {
      task.taskState = snapshot.taskState;
      task.done = snapshot.done;
    }
    for (const moduleSnapshot of snapshot.modules || []) {
      const module = homeModules().find((row) => row.id === moduleSnapshot.id);
      if (!module) continue;
      module.rows = moduleSnapshot.rows;
      module.done = moduleSnapshot.done;
      module.open = moduleSnapshot.open;
      for (const listSnapshot of moduleSnapshot.lists || []) {
        const list = (module.lists || []).find((row) => row.id === listSnapshot.id);
        if (!list) continue;
        list.rows = listSnapshot.rows;
        list.done = listSnapshot.done;
        list.open = listSnapshot.open;
      }
    }
  }

  function recomputeOnsiteModule(module) {
    const tasks = Array.isArray(module.tasks) ? module.tasks : [];
    module.rows = tasks.length;
    module.done = tasks.filter((task) => task.taskState === "done").length;
    module.open = tasks.length - module.done;
    for (const list of module.lists || []) {
      const rows = tasks.filter((task) => onsiteTaskBelongsToList(task, list.id));
      list.rows = rows.length;
      list.done = rows.filter((task) => task.taskState === "done").length;
      list.open = rows.length - list.done;
    }
  }

  function pendingActionKey(action, id) {
    return `${action || ""}:${id || ""}`;
  }

  function isPendingAction(action, id) {
    return !!state.pendingActions[pendingActionKey(action, id)];
  }

  function pendingActionSource(action, id) {
    return state.pendingActions[pendingActionKey(action, id)] || "";
  }

  function snapshotItemQuantities(itemId) {
    const item = items().find((row) => row.id === itemId);
    if (!item) return null;
    return {
      packed: item.packed,
      left: item.left,
      packState: item.packState
    };
  }

  function snapshotPackingItemState(itemId) {
    const item = items().find((row) => row.id === itemId);
    if (!item) return null;
    return {
      packed: item.packed,
      left: item.left,
      packState: item.packState,
      resolutionState: item.resolutionState
    };
  }

  function restorePackingItemState(itemId, snapshot) {
    if (!snapshot) return;
    const item = items().find((row) => row.id === itemId);
    if (!item) return;
    item.packed = snapshot.packed;
    item.left = snapshot.left;
    item.packState = snapshot.packState;
    item.resolutionState = snapshot.resolutionState;
  }

  function applyLocalPackState(itemId, packState) {
    const item = items().find((row) => row.id === itemId);
    if (!item) return;
    const needed = number(item.needed);
    item.packState = packState;
    if (packState === "packed") {
      item.packed = needed;
      item.left = 0;
    } else {
      item.left = Math.max(0, needed - number(item.packed));
    }
  }

  function applyLocalResolutionState(itemId, resolutionState) {
    const item = items().find((row) => row.id === itemId);
    if (!item) return;
    if (resolutionState === "clear") {
      item.resolutionState = "";
      item.packState = isPackedListItem(item) ? "packed" : "not_packed";
      return;
    }
    item.resolutionState = resolutionState;
    item.packState = resolutionState === "max" ? "packed" : "not_packed";
  }

  function restoreItemQuantities(itemId, snapshot) {
    if (!snapshot) return;
    const item = items().find((row) => row.id === itemId);
    if (!item) return;
    item.packed = snapshot.packed;
    item.left = snapshot.left;
    item.packState = snapshot.packState;
  }

  function applyOptimisticAddQuantity(itemId, quantityDelta) {
    const item = items().find((row) => row.id === itemId);
    if (!item) return null;
    const needed = number(item.needed);
    const currentPacked = number(item.packed);
    const nextPacked = needed > 0
      ? Math.min(needed, currentPacked + quantityDelta)
      : currentPacked + quantityDelta;
    item.packed = nextPacked;
    item.left = Math.max(0, needed - nextPacked);
    if (needed > 0 && item.left === 0) item.packState = "packed";
    return {
      packed: item.packed,
      left: item.left,
      packState: item.packState
    };
  }

  function preserveItemQuantities(snapshot) {
    if (!snapshot?.itemId) return;
    const item = items().find((row) => row.id === snapshot.itemId);
    if (!item) return;
    if (number(item.packed) >= number(snapshot.packed)) return;
    item.packed = snapshot.packed;
    item.left = snapshot.left;
    item.packState = snapshot.packState;
  }

  function snapshotHorseMemberState(itemHorseId) {
    for (const item of items()) {
      const member = (item.horseMembers || []).find((row) => row.id === itemHorseId);
      if (!member) continue;
      return {
        itemId: item.id,
        itemPacked: item.packed,
        itemLeft: item.left,
        itemPackState: item.packState,
        itemHorseId,
        memberPacked: member.packed,
        memberPackState: member.horsePackState
      };
    }
    return null;
  }

  function applyLocalHorseMemberState(itemHorseId, horsePackState) {
    for (const item of items()) {
      const member = (item.horseMembers || []).find((row) => row.id === itemHorseId);
      if (!member) continue;
      member.horsePackState = horsePackState;
      member.packed = horsePackState === "packed" ? number(member.needed || 1) || 1 : 0;
      const members = item.horseMembers || [];
      const packedTotal = members.reduce((sum, row) => sum + number(row.packed), 0);
      const neededTotal = members.reduce((sum, row) => sum + (number(row.needed) || 1), 0);
      item.packed = packedTotal;
      item.left = Math.max(0, neededTotal - packedTotal);
      item.packState = neededTotal > 0 && packedTotal >= neededTotal ? "packed" : "not_packed";
      return;
    }
  }

  function restoreHorseMemberState(snapshot) {
    if (!snapshot) return;
    const item = items().find((row) => row.id === snapshot.itemId);
    const member = item?.horseMembers?.find((row) => row.id === snapshot.itemHorseId);
    if (!item || !member) return;
    member.packed = snapshot.memberPacked;
    member.horsePackState = snapshot.memberPackState;
    item.packed = snapshot.itemPacked;
    item.left = snapshot.itemLeft;
    item.packState = snapshot.itemPackState;
  }

  function snapshotHorseRecordState(horseId) {
    const horse = horses().find((row) => row.id === horseId);
    if (!horse) return null;
    return {
      horseId,
      active: horse.active,
      recordState: horse.recordState
    };
  }

  function applyLocalHorseRecordState(horseId, recordState) {
    const horse = horses().find((row) => row.id === horseId);
    if (!horse) return;
    horse.recordState = recordState;
    horse.active = recordState === "active";
  }

  function restoreHorseRecordState(snapshot) {
    if (!snapshot) return;
    const horse = horses().find((row) => row.id === snapshot.horseId);
    if (!horse) return;
    horse.recordState = snapshot.recordState;
    horse.active = snapshot.active;
  }

  function snapshotSourceFlag(sourceItemId) {
    const refs = sourceItemRefs(sourceItemId).map((source) => ({
      source,
      flags: { ...(source.sourceFlags || {}) }
    }));
    return { sourceItemId, refs };
  }

  function applyLocalSourceFlag(sourceItemId, flagName, value) {
    const key = sourceFlagStateKey(flagName);
    if (!key) return;
    for (const source of sourceItemRefs(sourceItemId)) {
      source.sourceFlags = source.sourceFlags || {};
      source.sourceFlags[key] = value;
    }
  }

  function restoreSourceFlag(snapshot) {
    if (!snapshot) return;
    for (const ref of snapshot.refs || []) {
      ref.source.sourceFlags = { ...ref.flags };
    }
  }

  function sourceItemRefs(sourceItemId) {
    const refs = [];
    for (const item of items()) {
      for (const source of item.sourceItems || []) {
        if (source.id === sourceItemId) refs.push(source);
      }
    }
    return refs;
  }

  function sourceFlagStateKey(flagName) {
    if (flagName === "change_lane") return "changeLane";
    return ["ignore", "rename"].includes(flagName) ? flagName : "";
  }

  function horses() {
    return Array.isArray(state.data?.horses) ? state.data.horses : [];
  }

  function horseFilterOptions() {
    return [
      ["all", "ALL"],
      ["wave_one", "WAVE 1"],
      ["wave_two", "WAVE 2"],
      ["not_going", "NOT GOING"]
    ];
  }

  function overviewFilterOptions() {
    const lanes = new Set(activeListLanes().map((group) => group.lane || group.id));
    const options = [["packing", "PACK LISTS"]];
    if (lanes.has("task_lists")) options.push(["task_lists", "ITEM TASKS"]);
    if (lanes.has("custom")) options.push(["custom", "CUSTOM"]);
    if (lanes.has("locale")) options.push(["locale", "LOCALE"]);
    options.push(["search_list", "SEARCH"]);
    return options;
  }

  function overviewFilterMode() {
    const value = state.filterByList.overview || "packing";
    return overviewFilterOptions().some(([key]) => key === value) ? value : "packing";
  }

  function horseFilterRows() {
    const rows = horses();
    const filter = state.filterByList.horses || "all";
    const hasRosterFlags = rows.some((horse) => horse.waveOne || horse.waveTwo || horse.notGoing);
    if (filter === "all" || !hasRosterFlags) {
      return filterRows(activeWaveHorses(), "horses", horseSearchText).sort(compareHorseNames);
    }
    const filtered = rows.filter((horse) => {
      if (filter === "wave_one") return !!horse.waveOne && !horse.notGoing;
      if (filter === "wave_two") return !!horse.waveTwo && !horse.notGoing;
      if (filter === "not_going") return !!horse.notGoing;
      return true;
    });
    return filterRows(filtered, "horses", horseSearchText).sort(compareHorseNames);
  }

  function activeWaveHorses() {
    const members = horseMemberRows();
    if (!members.length) {
      return horses()
        .filter((horse) => horse.active || String(horse.recordState || "").toLowerCase() === "active")
        .sort(compareHorseNames);
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
      .sort(compareHorseNames);
  }

  function compareHorseNames(a, b) {
    return horseDisplayName(a).localeCompare(horseDisplayName(b), undefined, { sensitivity: "base" });
  }

  function sortItemsByName(rows) {
    return [...rows].sort((a, b) => {
      const nameCompare = displayLabel(a.name || "").localeCompare(displayLabel(b.name || ""), undefined, { sensitivity: "base" });
      if (nameCompare) return nameCompare;
      return String(a.id || "").localeCompare(String(b.id || ""), undefined, { sensitivity: "base" });
    });
  }

  function sortTableRows(rows, sortScope) {
    const activeSort = state.sortByList[sortScope] || {};
    if (!activeSort.field) return rows;
    const direction = activeSort.direction === "desc" ? -1 : 1;
    return [...rows].sort((a, b) => {
      const aValue = tableSortValue(a, activeSort.field);
      const bValue = tableSortValue(b, activeSort.field);
      if (typeof aValue === "number" || typeof bValue === "number") {
        const numericCompare = (number(aValue) - number(bValue)) * direction;
        if (numericCompare) return numericCompare;
      } else {
        const textCompare = String(aValue || "").localeCompare(String(bValue || ""), undefined, { sensitivity: "base" }) * direction;
        if (textCompare) return textCompare;
      }
      return String(a.id || "").localeCompare(String(b.id || ""), undefined, { sensitivity: "base" });
    });
  }

  function tableSortValue(row, field) {
    if (field === "item") return displayLabel(row.name || row.label || row.id || "");
    if (field === "needed") return row.needed ?? row.need ?? row.rows ?? 0;
    if (field === "packed") return row.packed ?? row.done ?? 0;
    if (field === "left") return row.left ?? row.open ?? 0;
    return "";
  }

  function sortListsByLabel(rows) {
    return [...rows].sort((a, b) => {
      const labelCompare = displayLabel(a.label || a.id || "").localeCompare(displayLabel(b.label || b.id || ""), undefined, { sensitivity: "base" });
      if (labelCompare) return labelCompare;
      return String(a.id || "").localeCompare(String(b.id || ""), undefined, { sensitivity: "base" });
    });
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
    }).sort(compareHorseItemRows);
  }

  function compareHorseItemRows(a, b) {
    return displayLabel(a.item?.name || "").localeCompare(displayLabel(b.item?.name || ""), undefined, { sensitivity: "base" });
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
      { id: "horses", label: horsesTabLabel() },
      ...tabGroups()
    ];
  }

  function horsesTabLabel() {
    if (state.detailType === "horse" && state.detailId) {
      const horse = horses().find((row) => row.id === state.detailId);
      if (horse) return horseDisplayName(horse);
    }
    return "Horses";
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
    const parts = itemBreadcrumbParts(item);
    return parts.length ? parts.join(" > ") : displayLabel(item.category || item.section || item.listPlanLabel || item.listPlan || "");
  }

  function itemBreadcrumbParts(item) {
    const list = primaryListForItem(item);
    const tabLabel = list ? listTabLabels(list)[0] : "";
    const listLabel = list?.label || item.packListLabels?.[0] || item.section || item.category || "";
    const itemLabel = item.name || "";
    const seen = new Set();
    return [tabLabel, listLabel, itemLabel]
      .map(displayLabel)
      .filter((part) => {
        const key = part.toLowerCase();
        if (!part || seen.has(key)) return false;
        seen.add(key);
        return true;
      });
  }

  function primaryListForItem(item) {
    const ids = Array.isArray(item.packListIds) ? item.packListIds : [];
    for (const id of ids) {
      const list = lists().find((row) => row.id === id);
      if (list) return list;
    }
    const labels = Array.isArray(item.packListLabels) ? item.packListLabels : [];
    for (const label of labels) {
      const normalized = String(label || "").trim().toLowerCase();
      const list = lists().find((row) => String(row.label || row.id || "").trim().toLowerCase() === normalized);
      if (list) return list;
    }
    return null;
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
    const cleaned = wholeQuantityNumber(value);
    return String(cleaned);
  }

  function wholeQuantityNumber(value) {
    const numeric = number(value);
    if (numeric <= 0) return 0;
    return Math.abs(numeric - Math.round(numeric)) < 0.000001
      ? Math.round(numeric)
      : Math.ceil(numeric - 0.000001);
  }

  function deadlineDisplay(value) {
    if (value === null || value === undefined || value === "") return "Not set";
    const dateOnly = String(value).match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (dateOnly) return `${Number(dateOnly[2])}/${Number(dateOnly[3])}/${dateOnly[1]}`;
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
      return quantityDisplay(item.quantityPackedOverride ?? item.quantity_packed_override ?? item.packed ?? "");
    }
    if (field === "quantity_needed_override") {
      return quantityDisplay(item.quantityNeededOverride ?? item.quantity_needed_override ?? item.needed ?? "");
    }
    return "";
  }

  function isMaxConflictState(value) {
    return ["max", "kill", "note", "purchase_onsite", "unresolved"].includes(String(value || ""));
  }

  function resolutionDisplayLabel(value) {
    if (value === "max") return "MAX";
    if (value === "note") return "ATTN";
    if (value === "kill") return "REMOVE";
    if (value === "purchase_onsite") return "BUY";
    if (value === "unresolved") return "UNRESOLVED";
    return displayLabel(value).toUpperCase();
  }

  function smsCommentItemHref(comment) {
    const label = displayLabel(comment.scopeLabel || comment.scopeType || "Comment");
    const body = [
      `WEC Packing Comment: ${label}`,
      comment.comment || "",
      "Reply with note:"
    ].filter(Boolean).join("\n");
    return `sms:?&body=${encodeURIComponent(body)}`;
  }

  function smsConflictHref(item) {
    const body = [
      `WEC Packing ATTN: ${displayLabel(item.name || "Unnamed item")}`,
      `NEED: ${quantityDisplay(item.needed)}, PACKED: ${quantityDisplay(item.packed)}, LEFT ${quantityDisplay(item.left)}`,
      "Reply with note:"
    ].join("\n");
    return `sms:?&body=${encodeURIComponent(body)}`;
  }

  function safeFilename(value) {
    return String(value || "wec-packing")
      .trim()
      .replace(/[^\w.-]+/g, "_")
      .replace(/^_+|_+$/g, "") || "wec-packing";
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

  function rsaSectionClass(value) {
    const key = themeName(value);
    if (key.includes("barn")) return "is-barn";
    if (key.includes("tack")) return "is-tack";
    if (key.includes("health")) return "is-health";
    if (key.includes("show")) return "is-show";
    if (key.includes("groom")) return "is-grooming";
    if (key.includes("feed")) return "is-feed";
    if (key.includes("horse")) return "is-horses";
    return "is-overview";
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
