(async () => {
  const root = document.getElementById("hps-app");
  if (!root) return;

  const config = window.HPS_CONFIG || {};
  const tenantId = String(config.tenantId || "").trim();
  const apiUrl = config.apiUrl || "/hps/horses";
  const state = {
    records: [],
    query: "",
    saveTimers: new Map(),
    activeRecordId: "",
    detailTab: "overview",
    listDrawerOpen: false,
    detailStatus: ""
  };

  root.innerHTML = shell();

  const els = {
    count: Array.from(root.querySelectorAll("[data-th-count]")),
    status: root.querySelector("[data-th-status]"),
    list: root.querySelector("[data-th-list]"),
    listDrawer: root.querySelector("[data-list-drawer]"),
    listDrawerToggle: root.querySelector("[data-toggle-list-drawer]"),
    drawerDetail: root.querySelector("[data-drawer-detail]")
  };

  root.addEventListener("click", handleClick);
  root.addEventListener("input", handleInput);
  root.addEventListener("change", handleChange);
  updateListDrawer();

  await load();

  async function load() {
    if (!tenantId) {
      els.list.innerHTML = `<div class="lp-row is-static">HPS tenant id is missing from the Webflow embed.</div>`;
      setStatus("Missing tenant id.");
      return;
    }

    setStatus(`Loading ww_horses for tenant ${tenantId}...`);
    try {
      const response = await fetch(requestUrl());
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.detail || data.error || `Load failed: ${response.status}`);
      }
      state.records = data.records || [];
      render();
      const sourceView = data.source?.view || `hps_${tenantId}`;
      const sourceTable = data.source?.table || "ww_horses";
      setStatus(`${sourceTable} - ${sourceView} | Loaded ${state.records.length} horses from ${sourceView} for tenant ${data.tenantId || tenantId}.`);
    } catch (error) {
      console.error("[hps]", error);
      els.list.innerHTML = `<div class="lp-row is-static">Horses failed to load. Check console for [hps].</div>`;
      setStatus("Load failed.");
    }
  }

  function render() {
    const records = filteredRecords();
    setCounts(records.length);
    els.list.innerHTML = records.length
      ? groupedRows(records)
      : `<div class="lp-row is-static">No horses found.</div>`;
    updateListDrawer();
  }

  function groupedRows(records) {
    const active = records.filter((record) => recordState(record) === "active");
    const inactive = records.filter((record) => recordState(record) !== "active");
    return [
      groupRows("Active", active),
      groupRows("Inactive", inactive)
    ].filter(Boolean).join("");
  }

  function groupRows(label, records) {
    if (!records.length) return "";
    return `
      <div class="th-group">
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
        <button class="lp-achievement packing-token ${currentState === "active" ? "is-packed" : "is-need"} th-state-pill" type="button" data-toggle-state="${escapeAttr(record.id)}" data-next-state="${escapeAttr(nextState)}">${escapeHtml(currentState)}</button>
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
    const color = firstValue(fields, ["color", "horse_color", "Color"]);
    const gender = firstValue(fields, ["gender", "horse_gender", "Gender"]);
    const type = firstValue(fields, ["horse_type", "type", "Type"]);
    const disciplines = firstValue(fields, ["disciplines", "discipline", "horse_disciplines", "Discipline", "Disciplines"]);
    const age = firstValue(fields, ["horse_age", "age", "Age"]);
    const emergencyContact = firstValue(fields, ["emergency_contact", "emergency_contacts", "Emergency Contact"]);
    const emergencyPhone = firstValue(fields, ["emergency_phone", "emergency_no", "Emergency Phone"]);
    const riderList = firstValue(fields, ["rider_list", "Rider List"]);
    const trainer = firstValue(fields, ["trainer_id", "trainer", "Trainer"]);
    const stallCardInput = firstValue(fields, ["stall_card_input_print", "Stall Card Input Print"]);
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;
    const currentState = recordState(record);
    const subtitle = [usef ? `USEF ${usef}` : "", recordKey].filter(Boolean).join(" - ");
    const activeTab = state.detailTab || "overview";

    return `
      <div class="lp-profile-head">
        <h2 class="lp-profile-title">${escapeHtml(name)}</h2>
        <p class="lp-profile-subtitle">${escapeHtml(subtitle)}</p>
      </div>

      <section class="lp-profile-panel packing-theme-horses packing-detail th-detail-section">
        <div class="lp-profile-tabs" role="tablist" aria-label="Horse profile sections">
          ${profileTab("overview", "Overview", activeTab)}
          ${profileTab("profile", "Profile", activeTab)}
          ${profileTab("contacts", "Contacts", activeTab)}
          ${profileTab("team", "Team", activeTab)}
          ${profileTab("print", "Print", activeTab)}
        </div>
        <div data-th-record="${escapeAttr(record.id)}" data-th-key="${escapeAttr(recordKey)}" data-th-name="${escapeAttr(name)}">
          <div class="lp-field-grid lp-profile-tab-panel${activeTab === "overview" ? " is-active" : ""}" data-profile-panel="overview">
            ${detailEditRow("show_name", "Show name", showName)}
            ${detailEditRow("barn_name", "Barn name", barnName)}
            ${detailChoiceRow("ignore", "Ignore", truthy(ignore) ? "Ignore" : "Include", ["Include", "Ignore"])}
          </div>
          <div class="lp-field-grid lp-profile-tab-panel${activeTab === "profile" ? " is-active" : ""}" data-profile-panel="profile">
            ${detailChoiceRow("horse_type", "Type", type, ["Pony", "Horse"])}
            ${detailChoiceRow("gender", "Gender", gender, ["Gelding", "Mare"])}
            ${detailMultiChoiceRow("disciplines", "Discipline", disciplines, ["Hunters", "Jumpers", "Equitation"])}
            ${detailChoiceRow("color", "Color", color, ["Black", "Bay", "Chestnut", "Grey", "Paint", "Palomino", "Liverchestnut"])}
            ${detailEditRow("horse_age", "Age", age, "number")}
          </div>
          <div class="lp-field-grid lp-profile-tab-panel${activeTab === "contacts" ? " is-active" : ""}" data-profile-panel="contacts">
            ${detailEditRow("emergency_contact", "Emergency contact", emergencyContact)}
            ${detailEditRow("emergency_phone", "Emergency phone", emergencyPhone)}
          </div>
          <div class="lp-field-grid lp-profile-tab-panel${activeTab === "team" ? " is-active" : ""}" data-profile-panel="team">
            ${detailEditRow("rider_list", "Rider list", riderList)}
            ${detailEditRow("trainer_id", "Trainer", trainer)}
            ${detailTextRow("USEF", usef || "-")}
          </div>
          <div class="lp-field-grid lp-profile-tab-panel${activeTab === "print" ? " is-active" : ""}" data-profile-panel="print">
            ${detailPrintRow(record.id, {
              barnName: barnName || name,
              showName: showName || name,
              colorGender: [color, gender].filter(Boolean).join(" "),
              emergencyContact,
              emergencyPhone,
              stallCardInput
            })}
          </div>
          <div class="lp-field-grid lp-profile-state-grid">
            ${detailStateRow(record.id, currentState)}
          </div>
        </div>
        ${detailSaveStatus(record.id)}
      </section>
    `;
  }

  function detailSheet(record) {
    return `
      <div class="th-detail-sheet-head">
        <button class="lp-modal-close th-detail-sheet-close" type="button" data-close-detail-sheet aria-label="Close detail">x</button>
      </div>
      ${detail(record)}
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
    return detailRow(label, escapeHtml(value));
  }

  function detailPrintRow(recordId, values) {
    return `
      <div class="lp-field-row">
        <span class="lp-field-label">Stall card</span>
        <span class="lp-field-value">
          <span class="lp-edit-choice-row packing-inline-choices">
            <button class="lp-edit-pill th-action-pill" type="button" data-stall-card-toggle="${escapeAttr(recordId)}">Print</button>
          </span>
          <div class="th-stall-card-panel" data-stall-card-panel="${escapeAttr(recordId)}" hidden>
            <input class="lp-edit-input" type="text" value="${escapeAttr(values.barnName)}" data-stall-card-field="barnName" aria-label="Barn name">
            <input class="lp-edit-input" type="text" value="${escapeAttr(values.showName)}" data-stall-card-field="showName" aria-label="Show name">
            <input class="lp-edit-input" type="text" value="${escapeAttr(values.colorGender)}" data-stall-card-field="colorGender" aria-label="Color gender">
            <input class="lp-edit-input" type="text" value="${escapeAttr(values.emergencyContact)}" data-stall-card-field="emergencyContact" aria-label="Emergency contact">
            <input class="lp-edit-input" type="text" value="${escapeAttr(values.emergencyPhone)}" data-stall-card-field="emergencyPhone" aria-label="Emergency phone">
            <button class="lp-edit-button th-print-button" type="button" data-stall-card-print="${escapeAttr(recordId)}">Print</button>
          </div>
        </span>
      </div>
    `;
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
    return `
      <div class="lp-field-row th-detail-edit">
        <span class="lp-field-label">State</span>
        <span class="lp-field-value">
          <span class="lp-edit-choice-row packing-inline-choices">
            ${["active", "inactive"].map((choice) => `
              <label class="lp-edit-choice">
                <input type="radio" name="record-state-${escapeAttr(recordId)}" data-toggle-state="${escapeAttr(recordId)}" data-next-state="${escapeAttr(choice)}"${currentState === choice ? " checked" : ""}>
                <span class="lp-edit-pill">${escapeHtml(choice)}</span>
              </label>
            `).join("")}
          </span>
        </span>
      </div>
    `;
  }

  function detailRow(label, value) {
    return `
      <div class="lp-field-row">
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
    if (event.target.closest("[data-close-detail-sheet]")) {
      closeDetailSheet();
      return;
    }

    if (event.target.closest("[data-toggle-list-drawer]")) {
      state.listDrawerOpen = !state.listDrawerOpen;
      updateListDrawer();
      return;
    }

    if (event.target.closest("[data-close-list-drawer]")) {
      state.listDrawerOpen = false;
      updateListDrawer();
      return;
    }

    const stateButton = event.target.closest("[data-toggle-state]");
    if (stateButton) {
      toggleRecordState(stateButton.dataset.toggleState, stateButton.dataset.nextState);
      return;
    }

    const profileTabButton = event.target.closest("[data-profile-tab]");
    if (profileTabButton) {
      switchProfileTab(profileTabButton.dataset.profileTab);
      return;
    }

    const stallCardToggle = event.target.closest("[data-stall-card-toggle]");
    if (stallCardToggle) {
      toggleStallCardPanel(stallCardToggle.dataset.stallCardToggle);
      return;
    }

    const stallCardPrint = event.target.closest("[data-stall-card-print]");
    if (stallCardPrint) {
      window.print();
      return;
    }

    const horseButton = event.target.closest("[data-open-horse]");
    if (horseButton) {
      openHorse(horseButton.dataset.openHorse);
      return;
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
    const newValue = input.dataset.thMulti
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

  async function toggleRecordState(recordId, nextState) {
    const record = state.records.find((item) => item.id === recordId);
    if (!record) return;
    const fields = record.fields || {};
    const fieldName = recordStateField(fields);
    const oldValue = fields[fieldName] ?? recordState(record);
    const name = firstValue(fields, ["barn_name", "Barn Name", "barn", "show_name", "horse", "name", "Horse", "Name"]) || "Unnamed horse";
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;
    const nextValue = fieldName === "inactive" ? nextState === "inactive" : nextState;

    fields[fieldName] = nextValue;
    render();
    if (state.activeRecordId === record.id && root.classList.contains("is-detail-sheet-open")) {
      els.drawerDetail.innerHTML = detailSheet(record);
    }

    await saveRecordChange(record, {
      fieldName,
      oldValue,
      newValue: nextValue,
      horseKey: recordKey,
      horseName: name
    });
  }

  function openHorse(recordId) {
    const record = state.records.find((item) => item.id === recordId);
    if (!record) return;
    state.activeRecordId = recordId;
    state.detailTab = "overview";
    state.detailStatus = "Changes save to Airtable.";
    state.listDrawerOpen = true;
    els.drawerDetail.innerHTML = detailSheet(record);
    root.classList.add("is-detail-sheet-open");
    updateListDrawer();
  }

  function closeDetailSheet() {
    state.activeRecordId = "";
    state.detailTab = "overview";
    root.classList.remove("is-detail-sheet-open");
    els.drawerDetail.innerHTML = "";
  }

  function switchProfileTab(tabId) {
    state.detailTab = tabId || "overview";
    root.querySelectorAll("[data-profile-tab]").forEach((button) => {
      const isActive = button.dataset.profileTab === state.detailTab;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    root.querySelectorAll("[data-profile-panel]").forEach((panel) => {
      panel.classList.toggle("is-active", panel.dataset.profilePanel === state.detailTab);
    });
  }

  function toggleStallCardPanel(recordId) {
    const panel = root.querySelector(`[data-stall-card-panel="${cssEscape(recordId)}"]`);
    if (panel) panel.hidden = !panel.hidden;
  }

  function updateListDrawer() {
    root.classList.toggle("is-list-drawer-open", state.listDrawerOpen);
    const toggle = root.querySelector("[data-toggle-list-drawer]");
    const drawer = root.querySelector("[data-list-drawer]");
    if (toggle) {
      toggle.setAttribute("aria-expanded", state.listDrawerOpen ? "true" : "false");
    }
    if (drawer) drawer.setAttribute("aria-hidden", state.listDrawerOpen ? "false" : "true");
    if (!state.listDrawerOpen) closeDetailSheet();
  }

  function filteredRecords() {
    if (!state.query) return state.records;
    return state.records.filter((record) => {
      const fields = record.fields || {};
      return Object.values(fields).some((value) => String(value).toLowerCase().includes(state.query));
    });
  }

  function shell() {
    return `
      <div class="th-module-shell">
        <button class="lp-tab packing-tab packing-theme-horses th-drawer-toggle" type="button" data-toggle-list-drawer aria-expanded="false">
          <span class="lp-tab-value" data-th-count>0</span>
          <span class="lp-tab-label">Horses</span>
        </button>
        <p class="th-module-status" data-th-status>Loading...</p>
      </div>

      <div class="th-drawer-backdrop" data-close-list-drawer></div>
      <section class="lp-section-block packing-theme-horses th-list-drawer" data-list-drawer aria-hidden="true">
        <div class="lp-section-title packing-section-title th-drawer-head">
          <h3>Horses</h3>
          <button class="lp-edit-button th-drawer-head-button" type="button" data-toggle-list-drawer>Hide</button>
        </div>
        <div class="packing-tools th-toolbar">
          <input class="lp-edit-input th-search" type="search" placeholder="Search horses" data-th-search>
        </div>
        <div id="sectionRows" class="lp-list" data-th-list></div>
        <div class="th-drawer-detail-sheet" data-drawer-detail></div>
      </section>
    `;
  }

  function setCounts(count) {
    els.count.forEach((el) => {
      el.textContent = el.classList.contains("lp-tab-value") ? String(count) : `${count} shown`;
    });
  }

  function requestUrl() {
    const url = new URL(apiUrl, window.location.href);
    url.searchParams.set("tenantId", tenantId);
    return url.toString();
  }

  function setStatus(message) {
    if (els.status) els.status.textContent = message;
  }

  function setDetailStatus(message) {
    state.detailStatus = message;
    const detailStatus = root.querySelector("[data-th-detail-status]");
    if (detailStatus) detailStatus.textContent = message;
  }

  function firstValue(fields, names) {
    for (const name of names) {
      if (fields[name] !== undefined && fields[name] !== null && fields[name] !== "") return fields[name];
    }
    return "";
  }

  function recordState(record) {
    const fields = record.fields || {};
    if (!Object.prototype.hasOwnProperty.call(fields, "record_state")) return truthy(fields.inactive) ? "inactive" : "active";
    const value = String(firstValue(fields, ["record_state", "Record State", "state", "State", "status", "Status"]) || "inactive").trim().toLowerCase();
    return value === "active" ? "active" : "inactive";
  }

  function recordStateField(fields) {
    if (!Object.prototype.hasOwnProperty.call(fields, "record_state")) return "inactive";
    return ["inactive", "record_state", "Record State", "state", "State", "status", "Status"].find((name) => Object.prototype.hasOwnProperty.call(fields, name)) || "record_state";
  }

  function truthy(value) {
    if (typeof value === "boolean") return value;
    const normalized = String(value || "").trim().toLowerCase();
    return ["1", "true", "yes", "y", "inactive"].includes(normalized);
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
