(async () => {
  const root = document.getElementById("hps-app");
  if (!root) return;
  if (root.dataset.hpsMounted === "true") return;
  root.dataset.hpsMounted = "true";

  const config = window.HPS_CONFIG || {};
  const tenantId = String(config.tenantId || "").trim();
  const apiUrl = config.apiUrl || "/hps/horses";
  const refreshIntervalMinutes = Number(config.refreshIntervalMinutes || 5);
  const stallCardUrl = config.stallCardUrl || "https://ringstatus.com/hps-stall-card";
  const pdfWorkerUrl = config.pdfWorkerUrl || "https://ringstatus-pdf.gombcg.workers.dev/";
  const state = {
    records: [],
    query: "",
    saveTimers: new Map(),
    activeRecordId: "",
    detailTab: "overview",
    moduleOpen: false,
    detailStatus: "",
    activeGroup: "active",
    activePrints: new Set(),
    sessionPrefs: loadSessionPrefs()
  };

  root.innerHTML = shell();

  const els = {
    count: Array.from(root.querySelectorAll("[data-th-count]")),
    status: root.querySelector("[data-th-status]"),
    list: root.querySelector("[data-th-list]"),
    listMeta: root.querySelector("[data-th-list-meta]"),
    moduleShell: root.querySelector("[data-hps-module-shell]"),
    moduleToggle: root.querySelector("[data-hps-toggle]"),
    modal: root.querySelector("[data-modal]"),
    modalCard: root.querySelector(".lp-modal-card"),
    modalContent: root.querySelector("[data-modal-content]")
  };

  document.addEventListener("click", handleDocumentClick);
  root.addEventListener("click", handleClick);
  root.addEventListener("input", handleInput);
  root.addEventListener("change", handleChange);
  updateModuleOpen();

  await load();
  startAutoRefresh();

  async function load(options = {}) {
    if (!tenantId) {
      els.list.innerHTML = `<div class="lp-row is-static">HPS tenant id is missing from the Webflow embed.</div>`;
      setStatus("Missing tenant id.");
      return;
    }

    if (!options.silent) setStatus(`Loading ww_horses for tenant ${tenantId}...`);
    try {
      const response = await fetch(requestUrl());
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.detail || data.error || `Load failed: ${response.status}`);
      }
      state.records = data.records || [];
      render();
      refreshActiveDetail();
      const sourceView = data.source?.view || `hps_${tenantId}`;
      const sourceTable = data.source?.table || "ww_horses";
      setStatus(`Updated ${formatTimestamp(new Date())} | ${state.records.length} horses | ${sourceView}`);
    } catch (error) {
      console.error("[hps]", error);
      els.list.innerHTML = `<div class="lp-row is-static">Horses failed to load. Check console for [hps].</div>`;
      setStatus("Load failed.");
    }
  }

  function startAutoRefresh() {
    if (!Number.isFinite(refreshIntervalMinutes) || refreshIntervalMinutes <= 0) return;
    window.setInterval(() => {
      if (document.hidden) return;
      load({ silent: true });
    }, refreshIntervalMinutes * 60 * 1000);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) load({ silent: true });
    });
  }

  function refreshActiveDetail() {
    if (!state.activeRecordId || els.modal.hidden) return;
    const record = state.records.find((item) => item.id === state.activeRecordId);
    if (!record) {
      closeModal();
      return;
    }
    els.modalContent.innerHTML = detail(record);
  }

  function render() {
    const records = filteredRecords();
    setCounts(records.length);
    els.list.innerHTML = records.length
      ? groupedRows(records)
      : `<div class="lp-row is-static">No horses found.</div>`;
  }

  function groupedRows(records) {
    return groupRows(
      state.activeGroup === "inactive" ? "Inactive" : "Active",
      records.filter((record) => recordState(record) === (state.activeGroup === "inactive" ? "inactive" : "active"))
    );
  }

  function groupRows(label, records) {
    if (!records.length) return "";
    const groupKey = label.toLowerCase();
    return `
      <div class="th-group" data-hps-group="${escapeAttr(groupKey)}">
        <div class="th-group-label">${escapeHtml(label)}</div>
        ${records.map((record) => row(record)).join("")}
      </div>
    `;
  }

  function row(record) {
    const fields = record.fields || {};
    const name = firstValue(fields, ["barn_name", "Barn Name", "barn", "show_name", "horse", "name", "Horse", "Name"]) || "Unnamed horse";
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;
    const currentState = recordState(record);
    const nextState = currentState === "active" ? "inactive" : "active";

    return `
      <div class="lp-row packing-row packing-horse-row th-horse-row" data-th-key="${escapeAttr(recordKey)}" data-th-name="${escapeAttr(name)}">
          <button class="packing-horse-detail-trigger" type="button" data-open-horse="${escapeAttr(record.id)}">
            <span class="lp-row-title">${escapeHtml(name)}</span>
          </button>
        <span class="lp-achievement packing-token ${currentState === "active" ? "is-packed" : "is-need"} th-state-pill" aria-label="App status">${escapeHtml(currentState)}</span>
      </div>
    `;
  }

  function detail(record) {
    const fields = record.fields || {};
    const name = firstValue(fields, ["show_name", "horse", "name", "Horse", "Name"]) || "Unnamed horse";
    const showName = firstValue(fields, ["show_name", "horse", "name", "Horse", "Name"]);
    const barnName = firstValue(fields, ["barn_name", "Barn Name", "barn"]);
    const ignore = firstValue(fields, ["ignore", "Ignore"]);
    const usef = firstValue(fields, ["usef", "USEF", "usef_id", "USEF ID"]);
    const color = firstValue(fields, ["horse_colors", "color", "horse_color", "Color"]);
    const gender = firstValue(fields, ["horse_genders", "gender", "horse_gender", "Gender"]);
    const type = firstValue(fields, ["horse_types", "horse_type", "type", "Type"]);
    const disciplines = firstValue(fields, ["horse_disciplines", "disciplines", "discipline", "Discipline", "Disciplines"]);
    const age = firstValue(fields, ["horse_age", "age", "Age"]);
    const hands = firstValue(fields, ["hands", "Hands"]);
    const note = firstValue(fields, ["horse_note", "Horse Note"]);
    const emergencyContact = firstValue(fields, ["emergency_contacts", "emergency_contact", "Emergency Contact"]);
    const emergencyPhone = firstValue(fields, ["emergency_phone", "emergency_no", "Emergency Phone"]);
    const printBatch = truthy(fields.print_batch);
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;
    const currentState = recordState(record);
    const sessionState = sessionRecordState(record.id);
    const subtitle = [usef ? `USEF ${usef}` : "", recordKey].filter(Boolean).join(" - ");
    const activeTab = validProfileTab(state.detailTab);

    return `
      <div class="lp-profile-shell">
        <div class="lp-profile-head th-profile-top">
          <h2 class="lp-profile-title">${escapeHtml(name)}</h2>
          <p class="lp-profile-subtitle">${escapeHtml(subtitle)}</p>
        </div>

        <div class="lp-profile-tabs th-profile-tabs" role="tablist" aria-label="Horse profile sections">
          ${profileTab("overview", "Overview", activeTab)}
          ${profileTab("profile", "Profile", activeTab)}
          ${profileTab("feed", "Feed", activeTab)}
          ${profileTab("contacts", "Contacts", activeTab)}
          ${profileTab("print", "Print", activeTab)}
        </div>

        <section class="lp-profile-panel packing-theme-horses packing-detail th-detail-section">
          <div data-th-record="${escapeAttr(record.id)}" data-th-key="${escapeAttr(recordKey)}" data-th-name="${escapeAttr(name)}">
            <div class="lp-field-grid lp-profile-tab-panel${activeTab === "overview" ? " is-active" : ""}" data-profile-panel="overview">
              ${detailTextRow("Show name", showName || "-")}
              ${detailEditRow("barn_name", "Barn name", barnName)}
              ${detailSessionStateRow(record.id, sessionState)}
              ${detailLongTextRow("horse_note", "Note", note)}
              ${detailAppStateRow(record.id, currentState)}
            </div>
            <div class="lp-field-grid lp-profile-tab-panel${activeTab === "profile" ? " is-active" : ""}" data-profile-panel="profile">
              ${detailChoiceRow("horse_genders", "Gender", gender, ["Gelding", "Mare"])}
              ${detailMultiChoiceRow("horse_disciplines", "Discipline", disciplines, ["Hunter", "Jumper", "Equitation"])}
              ${detailChoiceRow("horse_colors", "Color", color, ["Black", "Bay", "Chestnut", "Grey", "Paint", "Palomino", "Liverchestnut"])}
              ${detailEditRow("horse_age", "Age", age, "number")}
              ${detailEditRow("hands", "Hands", hands, "number")}
            </div>
            <div class="lp-field-grid lp-profile-tab-panel${activeTab === "feed" ? " is-active" : ""}" data-profile-panel="feed">
              ${detailFeedTable(record.feedPlan || [])}
            </div>
            <div class="lp-field-grid lp-profile-tab-panel${activeTab === "contacts" ? " is-active" : ""}" data-profile-panel="contacts">
              ${detailEditRow("emergency_contacts", "Emergency contact", emergencyContact)}
              ${detailEditRow("emergency_phone", "Emergency phone", emergencyPhone)}
            </div>
            <div class="lp-field-grid lp-profile-tab-panel${activeTab === "print" ? " is-active" : ""}" data-profile-panel="print">
              ${detailPrintRow(record.id, printBatch)}
            </div>
          </div>
        </section>

        <div class="lp-profile-modal-footer th-profile-footer">
          <div class="lp-field-grid lp-profile-state-grid">
            ${detailStateRow(record.id, currentState)}
          </div>
          ${detailSaveStatus(record.id)}
        </div>
      </div>
    `;
  }

  function profileTab(tabId, label, activeTab) {
    const isActive = activeTab === tabId;
    return `
      <button class="lp-profile-tab${isActive ? " is-active" : ""}" type="button" role="tab" aria-selected="${isActive ? "true" : "false"}" data-profile-tab="${escapeAttr(tabId)}">
        ${escapeHtml(label)}
      </button>
    `;
  }

  function detailTextRow(label, value) {
    return detailRow(label, `<input class="lp-edit-input th-input th-readonly-input" type="text" value="${escapeAttr(value)}" readonly tabindex="-1">`);
  }

  function detailPrintRow(recordId, printBatch) {
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">
          <span>Stall card</span>
          <span class="th-print-status" data-stall-card-status="${escapeAttr(recordId)}">${printBatch ? "Requested" : ""}</span>
        </span>
        <span class="lp-field-value">
          <span class="lp-edit-choice-row packing-inline-choices">
            <button class="lp-edit-pill th-action-pill" type="button" data-stall-card-print="${escapeAttr(recordId)}">PRINT NOW</button>
            <label class="lp-edit-choice">
              <input type="checkbox" data-th-field="print_batch" data-th-boolean="true" value="true"${printBatch ? " checked" : ""}>
              <span class="lp-edit-pill">REQUEST TO PRINT</span>
            </label>
          </span>
        </span>
      </div>
    `;
  }

  function detailFeedTable(feedPlan) {
    if (!feedPlan.length) {
      return `
        <div class="lp-field-row th-detail-edit th-feed-empty">
          <span class="lp-field-label">Feed</span>
          <span class="lp-field-value">
            <span class="th-feed-empty-text">No feed plan.</span>
          </span>
        </div>
      `;
    }

    return `
      <div class="th-feed-table-wrap" aria-label="Feed plan">
        <table class="th-feed-table">
          <thead>
            <tr>
              <th scope="col">Slot</th>
              <th scope="col">Type</th>
              <th scope="col">Feed</th>
              <th scope="col">Qty</th>
              <th scope="col">Unit</th>
              <th scope="col">Notes</th>
            </tr>
          </thead>
          <tbody>
            ${feedPlan.map((item) => feedRow(item.fields || {})).join("")}
          </tbody>
        </table>
      </div>
    `;
  }

  function feedRow(fields) {
    return `
      <tr>
        <td>${escapeHtml(feedValue(fields, ["slot", "feed_slot", "time"]))}</td>
        <td>${escapeHtml(feedValue(fields, ["feed_type", "type", "ration_type"]))}</td>
        <td>${escapeHtml(feedValue(fields, ["feed", "feed_name", "ration", "item"]))}</td>
        <td>${escapeHtml(feedValue(fields, ["qty", "quantity", "amount", "scoops", "dirty_qty", "dirty_scoops"]))}</td>
        <td>${escapeHtml(feedValue(fields, ["unit", "measure", "default_measure"]))}</td>
        <td>${escapeHtml(feedValue(fields, ["note", "notes", "feed_note"]))}</td>
      </tr>
    `;
  }

  function feedValue(fields, names) {
    const value = firstValue(fields, names);
    if (Array.isArray(value)) return value.join(", ");
    return value || "-";
  }

  function detailSaveStatus(recordId) {
    return `
      <div class="lp-profile-footer th-save-status-row">
        <span data-th-detail-status>${escapeHtml(state.detailStatus || "Changes save to Airtable.")}</span>
        ${recordId ? `<a class="th-link" href="https://airtable.com/${escapeAttr(recordId)}" target="_blank" rel="noopener">Airtable</a>` : ""}
      </div>
    `;
  }

  function detailStateRow(recordId, currentState) {
    return detailAppStateRow(recordId, currentState);
  }

  function detailAppStateRow(recordId, currentState) {
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">App status</span>
        <span class="lp-field-value">
          <span class="lp-edit-choice-row packing-inline-choices">
            ${["active", "inactive"].map((choice) => `
              <span class="lp-edit-choice">
                <button class="lp-edit-pill${currentState === choice ? " is-active" : ""}" type="button" data-app-state="${escapeAttr(choice)}" data-app-state-record="${escapeAttr(recordId)}">${escapeHtml(choice)}</button>
              </span>
            `).join("")}
          </span>
        </span>
      </div>
    `;
  }

  function detailSessionStateRow(recordId, currentState) {
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">Session view</span>
        <span class="lp-field-value">
          <span class="lp-edit-choice-row packing-inline-choices">
            ${["include", "ignore"].map((choice) => `
              <span class="lp-edit-choice">
                <button class="lp-edit-pill${currentState === choice ? " is-active" : ""}" type="button" data-session-state="${escapeAttr(choice)}" data-session-state-record="${escapeAttr(recordId)}">${escapeHtml(choice)}</button>
              </span>
            `).join("")}
          </span>
        </span>
      </div>
    `;
  }

  function detailRow(label, value) {
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">${escapeHtml(label)}</span>
        <span class="lp-field-value">${value}</span>
      </div>
    `;
  }

  function detailEditRow(fieldName, label, value, type = "text") {
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">${escapeHtml(label)}</span>
        <span class="lp-field-value">
          <input class="lp-edit-input th-input" type="${escapeAttr(type)}" data-th-field="${escapeAttr(fieldName)}" value="${escapeAttr(value)}">
        </span>
      </div>
    `;
  }

  function detailLongTextRow(fieldName, label, value) {
    return `
      <div class="lp-field-row th-detail-edit th-detail-note">
        <span class="lp-field-label">${escapeHtml(label)}</span>
        <span class="lp-field-value">
          <textarea class="lp-edit-input th-input th-note-input" data-th-field="${escapeAttr(fieldName)}" rows="4">${escapeHtml(value)}</textarea>
        </span>
      </div>
    `;
  }

  function detailChoiceRow(fieldName, label, value, choices) {
    const current = String(value || "").trim().toLowerCase();
    const name = `choice-${fieldName}`;
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">${escapeHtml(label)}</span>
        <span class="lp-field-value">
          <span class="lp-edit-choice-row packing-inline-choices">
            ${choices.map((choice) => {
              const checked = current === choice.toLowerCase() ? " checked" : "";
              return `
                <label class="lp-edit-choice">
                  <input type="radio" name="${escapeAttr(name)}" data-th-field="${escapeAttr(fieldName)}" value="${escapeAttr(choice)}"${checked}>
                  <span class="lp-edit-pill">${escapeHtml(choice)}</span>
                </label>
              `;
            }).join("")}
          </span>
        </span>
      </div>
    `;
  }

  function detailMultiChoiceRow(fieldName, label, value, choices) {
    const current = new Set(String(value || "").split(",").map((item) => item.trim().toLowerCase()).filter(Boolean));
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">${escapeHtml(label)}</span>
        <span class="lp-field-value">
          <span class="lp-edit-choice-row packing-inline-choices">
            ${choices.map((choice) => {
              const checked = current.has(choice.toLowerCase()) ? " checked" : "";
              return `
                <label class="lp-edit-choice">
                  <input type="checkbox" data-th-field="${escapeAttr(fieldName)}" data-th-multi="true" value="${escapeAttr(choice)}"${checked}>
                  <span class="lp-edit-pill">${escapeHtml(choice)}</span>
                </label>
              `;
            }).join("")}
          </span>
        </span>
      </div>
    `;
  }

  function handleClick(event) {
    const groupAnchor = event.target.closest("[data-hps-group-jump]");
    if (groupAnchor) {
      setListGroup(groupAnchor.dataset.hpsGroupJump);
      event.stopPropagation();
      return;
    }

    if (event.target.closest("[data-th-refresh]")) {
      load();
      return;
    }

    const appStateButton = event.target.closest("[data-app-state]");
    if (appStateButton) {
      setAppState(appStateButton.dataset.appStateRecord, appStateButton.dataset.appState);
      return;
    }

    const sessionStateButton = event.target.closest("[data-session-state]");
    if (sessionStateButton) {
      setSessionState(sessionStateButton.dataset.sessionStateRecord, sessionStateButton.dataset.sessionState);
      return;
    }

    if (event.target.closest("[data-modal-close]")) {
      closeModal();
      return;
    }

    const profileTabButton = event.target.closest("[data-profile-tab]");
    if (profileTabButton) {
      switchProfileTab(profileTabButton.dataset.profileTab);
      return;
    }

    const stallCardPrint = event.target.closest("[data-stall-card-print]");
    if (stallCardPrint) {
      event.preventDefault();
      event.stopPropagation();
      if (event.stopImmediatePropagation) event.stopImmediatePropagation();
      openStallCardPdf(stallCardPrint.dataset.stallCardPrint, stallCardPrint);
      return;
    }

    const horseButton = event.target.closest("[data-open-horse]");
    if (horseButton) {
      openHorse(horseButton.dataset.openHorse);
    }
  }

  function handleInput(event) {
    if (event.target.matches("[data-th-search]")) {
      state.query = event.target.value.trim().toLowerCase();
      render();
      return;
    }

    const input = event.target.closest("[data-th-field]");
    if (!input) return;
    const rowEl = input.closest("[data-th-record]");
    if (!rowEl) return;

    const timerKey = `${rowEl.dataset.thRecord}:${input.dataset.thField}`;
    clearTimeout(state.saveTimers.get(timerKey));
    state.saveTimers.set(timerKey, setTimeout(() => saveField(rowEl, input), 700));
  }

  function handleChange(event) {
    const input = event.target.closest("[data-th-field]");
    if (!input) return;
    const rowEl = input.closest("[data-th-record]");
    if (!rowEl) return;
    saveField(rowEl, input);
  }

  async function saveField(rowEl, input) {
    const record = state.records.find((item) => item.id === rowEl.dataset.thRecord);
    if (!record) return;

    const fieldName = input.dataset.thField;
    const oldValue = record.fields?.[fieldName] ?? "";
    const newValue = input.dataset.thBoolean
      ? input.checked
      : input.dataset.thMulti
      ? Array.from(rowEl.querySelectorAll(`[data-th-field="${cssEscape(fieldName)}"][data-th-multi]:checked`)).map((item) => item.value).join(", ")
      : input.value;
    if (String(oldValue) === String(newValue)) return;

    await saveRecordChange(record, {
      fieldName,
      oldValue,
      newValue,
      horseKey: rowEl.dataset.thKey,
      horseName: rowEl.dataset.thName
    });
  }

  async function saveRecordChange(record, change) {
    setStatus("Saving change...");
    setDetailStatus("Saving change...");
    try {
      const response = await fetch(apiUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          horseRecordId: record.id,
          horseKey: change.horseKey,
          horseName: change.horseName,
          fieldName: change.fieldName,
          oldValue: change.oldValue,
          newValue: change.newValue,
          tenantId,
          source: "hps"
        })
      });
      const result = await response.json();
      if (!response.ok || !result.ok) {
        throw new Error(result.detail || result.error || `Save failed: ${response.status}`);
      }
      record.fields[change.fieldName] = result.updated?.value ?? change.newValue;
      if (change.fieldName === "print_batch") {
        setPrintStatus(record.id, change.newValue ? `Requested: ${formatTimestamp(new Date())}` : "");
      }
      const message = `Saved to Airtable at ${new Date().toLocaleTimeString()} (updated, logged).`;
      setStatus(message);
      setDetailStatus(message);
    } catch (error) {
      console.error("[hps]", error);
      const detail = error instanceof Error ? error.message : String(error);
      const message = `Save failed: ${detail}`;
      setStatus(message);
      setDetailStatus(message);
    }
  }

  function openHorse(recordId) {
    const record = state.records.find((item) => item.id === recordId);
    if (!record) return;
    state.activeRecordId = recordId;
    state.detailTab = "overview";
    state.detailStatus = "Changes save to Airtable.";
    els.modalContent.innerHTML = detail(record);
    els.modal.hidden = false;
    document.body.style.overflow = "hidden";
    requestAnimationFrame(() => els.modalCard?.focus());
  }

  function closeModal() {
    state.activeRecordId = "";
    state.detailTab = "overview";
    els.modal.hidden = true;
    els.modalContent.innerHTML = "";
    document.body.style.overflow = "";
  }

  function switchProfileTab(tabId) {
    state.detailTab = validProfileTab(tabId);
    root.querySelectorAll("[data-profile-tab]").forEach((button) => {
      const isActive = button.dataset.profileTab === state.detailTab;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    root.querySelectorAll("[data-profile-panel]").forEach((panel) => {
      panel.classList.toggle("is-active", panel.dataset.profilePanel === state.detailTab);
    });
  }

  function validProfileTab(tabId) {
    return ["overview", "profile", "feed", "contacts", "print"].includes(tabId) ? tabId : "overview";
  }

  function updateModuleOpen() {
    root.classList.toggle("is-hps-open", state.moduleOpen);
    document.querySelectorAll("[data-hps-toggle]").forEach((toggle) => {
      toggle.setAttribute("aria-expanded", state.moduleOpen ? "true" : "false");
      toggle.classList.toggle("is-active", state.moduleOpen);
    });
  }

  async function setAppState(recordId, nextState) {
    const record = state.records.find((item) => item.id === recordId);
    if (!record) return;
    const currentState = recordState(record);
    if (currentState === nextState) return;
    const activeValue = nextState === "active";
    await saveRecordChange(record, {
      fieldName: "app_active",
      oldValue: record.fields?.app_active ?? "",
      newValue: activeValue,
      horseKey: record.id,
      horseName: firstValue(record.fields || {}, ["barn_name", "show_name", "horse"]) || record.id
    });
    await saveRecordChange(record, {
      fieldName: "app_inactive",
      oldValue: record.fields?.app_inactive ?? "",
      newValue: !activeValue,
      horseKey: record.id,
      horseName: firstValue(record.fields || {}, ["barn_name", "show_name", "horse"]) || record.id
    });
    record.fields.app_active = activeValue;
    record.fields.app_inactive = !activeValue;
    render();
    refreshActiveDetail();
  }

  function setSessionState(recordId, nextState) {
    state.sessionPrefs[recordId] = nextState === "ignore" ? "ignore" : "include";
    saveSessionPrefs();
    refreshActiveDetail();
  }

  function handleDocumentClick(event) {
    const toggle = event.target.closest("[data-hps-toggle]");
    if (!toggle) return;
    event.preventDefault();
    state.moduleOpen = !state.moduleOpen;
    updateModuleOpen();
  }

  function setListGroup(groupKey) {
    state.activeGroup = groupKey || "active";
    updateGroupJumpState();
    render();
    root.querySelector(".lp-content")?.scrollTo({ top: 0, behavior: "smooth" });
  }

  function updateGroupJumpState() {
    root.querySelectorAll("[data-hps-group-jump]").forEach((button) => {
      const isActive = button.dataset.hpsGroupJump === state.activeGroup;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  }

  function filteredRecords() {
    const records = state.records.filter((record) => recordState(record) === (state.activeGroup === "inactive" ? "inactive" : "active"));
    if (!state.query) return records;
    return records.filter((record) => {
      const fields = record.fields || {};
      return Object.values(fields).some((value) => String(value).toLowerCase().includes(state.query));
    });
  }

  function shell() {
    return `
      <div class="th-hps-opener">
        <button class="th-hps-toggle" type="button" data-hps-toggle aria-expanded="false">
          <span class="th-hps-toggle-count" data-th-count>0</span>
          <span class="th-hps-toggle-label">Horses</span>
        </button>
      </div>

      <div class="th-hps-module">
        <div class="lp-shell th-hps-shell" data-hps-module-shell>
          <header class="lp-header">
            <div class="lp-header-copy">
              <h1>HPS Horses</h1>
              <p class="lp-subtitle">Horse profiles and status</p>
            </div>
          </header>

          <nav class="lp-tabs" aria-label="Tack horse sections">
            <button class="lp-tab packing-tab packing-theme-horses is-active" type="button" aria-selected="true">
              <span class="lp-tab-value" data-th-count>0</span>
              <span class="lp-tab-label">Horses</span>
            </button>
          </nav>

          <main class="lp-content">
            <section class="lp-panel is-active">
              <section class="lp-section-block packing-theme-horses">
                <div class="lp-section-title packing-section-title">
                  <h3>Horses</h3>
                </div>
                <div class="packing-tools th-toolbar">
                  <div class="th-hps-controls" aria-label="Horse list controls">
                    <button class="th-hps-control is-active" type="button" data-hps-group-jump="active" aria-pressed="true">Active</button>
                    <button class="th-hps-control" type="button" data-hps-group-jump="inactive" aria-pressed="false">Inactive</button>
                    <button class="th-hps-control" type="button" data-th-refresh>Refresh</button>
                  </div>
                  <input class="lp-edit-input th-search" type="search" placeholder="Search horses" data-th-search>
                </div>
                <div id="sectionRows" class="lp-list" data-th-list></div>
                <div class="th-hps-list-meta" data-th-list-meta>Loading...</div>
              </section>
            </section>
          </main>
        </div>
      </div>

      <div class="lp-modal" data-modal hidden>
        <div class="lp-modal-backdrop" data-modal-close></div>
        <section class="lp-modal-card" role="dialog" aria-modal="true" tabindex="-1">
          <button class="lp-modal-close" type="button" data-modal-close aria-label="Close detail">x</button>
          <div data-modal-content></div>
        </section>
      </div>
    `;
  }

  function setCounts(count) {
    els.count.forEach((el) => {
      el.textContent = el.classList.contains("lp-tab-value") || el.classList.contains("th-hps-toggle-count") ? String(count) : `${count} shown`;
    });
  }

  function requestUrl() {
    const url = new URL(apiUrl, window.location.href);
    url.searchParams.set("tenantId", tenantId);
    return url.toString();
  }

  function setStatus(message) {
    if (els.status) els.status.textContent = message;
    if (els.listMeta) els.listMeta.textContent = message;
  }

  function formatTimestamp(date) {
    return date.toLocaleString([], {
      month: "numeric",
      day: "numeric",
      year: "2-digit",
      hour: "numeric",
      minute: "2-digit"
    });
  }

  function setDetailStatus(message) {
    state.detailStatus = message;
    const detailStatus = root.querySelector("[data-th-detail-status]");
    if (detailStatus) detailStatus.textContent = message;
  }

  function openStallCardPdf(recordId, button) {
    if (button?.dataset.hpsPrintOpening === "true") {
      setPrintStatus(recordId, "PDF is already opening...");
      return;
    }

    window.__HPS_PRINT_LOCKS = window.__HPS_PRINT_LOCKS || new Map();
    const lockKey = `${tenantId}:${recordId}`;
    const lastPrintAt = window.__HPS_PRINT_LOCKS.get(lockKey) || 0;
    if (Date.now() - lastPrintAt < 7000) {
      setPrintStatus(recordId, "PDF is already opening...");
      return;
    }

    if (state.activePrints.has(recordId)) {
      setPrintStatus(recordId, "PDF is already opening...");
      return;
    }

    const record = state.records.find((item) => item.id === recordId);
    if (!record) return;
    if (button) button.dataset.hpsPrintOpening = "true";
    state.activePrints.add(recordId);
    window.__HPS_PRINT_LOCKS.set(lockKey, Date.now());

    const fields = record.fields || {};
    const horseName = firstValue(fields, ["barn_name", "Barn Name", "barn", "show_name", "horse", "name", "Horse", "Name"]) || "horse";
    const printUrl = new URL(stallCardUrl, window.location.href);
    printUrl.searchParams.set("tenantId", tenantId);
    printUrl.searchParams.set("horseRecordId", record.id);

    const pdfUrl = new URL(pdfWorkerUrl);
    pdfUrl.searchParams.set("url", printUrl.toString());
    pdfUrl.searchParams.set("filename", `${safeFilename(horseName)}-stall-card.pdf`);

    setPrintStatus(record.id, "Creating PDF...");
    const opened = window.open(pdfUrl.toString(), "_blank");
    if (opened) {
      setPrintStatus(record.id, "PDF opened.");
      setDetailStatus("PDF opened.");
      window.setTimeout(() => {
        state.activePrints.delete(recordId);
        window.__HPS_PRINT_LOCKS.delete(lockKey);
        if (button) delete button.dataset.hpsPrintOpening;
      }, 7000);
      return;
    }

    setPrintStatus(record.id, "Popup blocked. Allow popups and press Print again.");
    setDetailStatus("Popup blocked. HPS stayed open.");
    window.setTimeout(() => {
      state.activePrints.delete(recordId);
      window.__HPS_PRINT_LOCKS.delete(lockKey);
      if (button) delete button.dataset.hpsPrintOpening;
    }, 7000);
  }

  function setPrintStatus(recordId, message) {
    const status = root.querySelector(`[data-stall-card-status="${cssEscape(recordId)}"]`);
    if (status) status.textContent = message;
  }

  function safeFilename(value) {
    return String(value || "horse")
      .trim()
      .replace(/[^\w.-]+/g, "_")
      .replace(/^_+|_+$/g, "") || "horse";
  }

  function firstValue(fields, names) {
    for (const name of names) {
      if (fields[name] !== undefined && fields[name] !== null && fields[name] !== "") return fields[name];
    }
    return "";
  }

  function recordState(record) {
    const fields = record.fields || {};
    if (checkboxValue(fields.app_inactive)) return "inactive";
    if (checkboxValue(fields.app_active)) return "active";
    return "active";
  }

  function sessionRecordState(recordId) {
    return state.sessionPrefs[recordId] === "ignore" ? "ignore" : "include";
  }

  function sessionStorageKey() {
    return `hps_session_prefs_${tenantId || "default"}`;
  }

  function loadSessionPrefs() {
    try {
      return JSON.parse(window.sessionStorage.getItem(sessionStorageKey()) || "{}") || {};
    } catch {
      return {};
    }
  }

  function saveSessionPrefs() {
    try {
      window.sessionStorage.setItem(sessionStorageKey(), JSON.stringify(state.sessionPrefs));
    } catch {
      /* Session preferences are optional. */
    }
  }

  function truthy(value) {
    if (typeof value === "boolean") return value;
    const normalized = String(value || "").trim().toLowerCase();
    return ["1", "true", "yes", "y", "inactive"].includes(normalized);
  }

  function checkboxValue(value) {
    if (typeof value === "boolean") return value;
    const normalized = String(value || "").trim().toLowerCase();
    return ["1", "true", "yes", "y"].includes(normalized);
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }

  function cssEscape(value) {
    if (window.CSS && typeof window.CSS.escape === "function") return window.CSS.escape(value);
    return String(value).replace(/"/g, '\\"');
  }
})();
