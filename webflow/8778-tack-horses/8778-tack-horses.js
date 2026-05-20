(async () => {
  const root = document.getElementById("tack-horses-app") || document.getElementById("packing-app") || document.getElementById("lp-history-app");
  if (!root) return;

  const config = window.TACK_HORSES_CONFIG || {};
  const apiUrl = config.apiUrl || "/8778-tack-horses/horses";
  const state = {
    records: [],
    query: "",
    saveTimers: new Map(),
    activeRecordId: ""
  };

  root.innerHTML = shell();

  const els = {
    count: Array.from(root.querySelectorAll("[data-th-count]")),
    status: root.querySelector("[data-th-status]"),
    list: root.querySelector("[data-th-list]"),
    modal: root.querySelector("[data-modal]"),
    modalCard: root.querySelector(".lp-modal-card"),
    modalContent: root.querySelector("[data-modal-content]")
  };

  root.addEventListener("click", handleClick);
  root.addEventListener("input", handleInput);
  root.addEventListener("change", handleChange);

  await load();

  async function load() {
    setStatus("Loading ww_horses...");
    try {
      const response = await fetch(apiUrl);
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.detail || data.error || `Load failed: ${response.status}`);
      }
      state.records = data.records || [];
      render();
      setStatus(`Loaded ${state.records.length} horses from ${data.source?.view || "view"}.`);
    } catch (error) {
      console.error("[8778-tack-horses]", error);
      els.list.innerHTML = `<div class="lp-row is-static">Horses failed to load. Check console for [8778-tack-horses].</div>`;
      setStatus("Load failed.");
    }
  }

  function render() {
    const records = filteredRecords();
    setCounts(records.length);
    els.list.innerHTML = records.length
      ? groupedRows(records)
      : `<div class="lp-row is-static">No horses found.</div>`;
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
    const usef = firstValue(fields, ["usef", "USEF", "usef_id", "USEF ID"]);
    const color = firstValue(fields, ["horse_color", "color", "Color"]);
    const gender = firstValue(fields, ["horse_gender", "gender", "Gender"]);
    const type = firstValue(fields, ["horse_type", "type", "Type"]);
    const disciplines = firstValue(fields, ["disciplines", "horse_disciplines", "Discipline", "Disciplines"]);
    const age = firstValue(fields, ["horse_age", "age", "Age"]);
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;
    const currentState = recordState(record);

    return `
      <div class="lp-detail-head">
        <h2 class="lp-modal-title">${escapeHtml(name)}</h2>
        <p class="lp-row-meta">${[usef ? `USEF ${usef}` : "", recordKey].filter(Boolean).map(escapeHtml).join(" - ")}</p>
      </div>

      <section class="lp-section-block packing-theme-horses packing-detail th-detail-section">
        <div class="lp-section-title packing-section-title">
          <h3>Detail</h3>
        </div>
        <div class="lp-list" data-th-record="${escapeAttr(record.id)}" data-th-key="${escapeAttr(recordKey)}" data-th-name="${escapeAttr(name)}">
          ${detailStateRow(record.id, currentState)}
          ${detailEditRow("show_name", "Show name", showName)}
          ${detailEditRow("barn_name", "Barn name", barnName)}
          ${detailChoiceRow("horse_color", "Color", color, ["Black", "Bay", "Chestnut", "Grey", "Paint", "Palomino", "Liverchestnut"])}
          ${detailChoiceRow("horse_gender", "Gender", gender, ["Gelding", "Mare"])}
          ${detailChoiceRow("horse_type", "Type", type, ["Pony", "Horse"])}
          ${detailMultiChoiceRow("disciplines", "Discipline", disciplines, ["Hunters", "Jumpers", "Equitation"])}
          ${detailEditRow("horse_age", "Age", age, "number")}
          ${detailTextRow("USEF", usef || "-")}
          ${record.id ? detailRow("Airtable", `<a class="th-link" href="https://airtable.com/${escapeAttr(record.id)}" target="_blank" rel="noopener">airtable</a>`) : ""}
        </div>
      </section>
    `;
  }

  function detailTextRow(label, value) {
    return detailRow(label, escapeHtml(value));
  }

  function detailStateRow(recordId, currentState) {
    return `
      <div class="lp-row is-static is-detail packing-control-row th-detail-edit">
        <span class="lp-row-title">State</span>
        <span class="lp-row-meta">
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
      <div class="lp-row is-static is-detail packing-control-row">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">${value}</span>
      </div>
    `;
  }

  function detailEditRow(fieldName, label, value, type = "text") {
    return `
      <div class="lp-row is-static is-detail packing-control-row th-detail-edit">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">
          <input class="lp-edit-input th-input" type="${escapeAttr(type)}" data-th-field="${escapeAttr(fieldName)}" value="${escapeAttr(value)}">
        </span>
      </div>
    `;
  }

  function detailChoiceRow(fieldName, label, value, choices) {
    const current = String(value || "").trim().toLowerCase();
    const name = `choice-${fieldName}`;
    return `
      <div class="lp-row is-static is-detail packing-control-row th-detail-edit">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">
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
      <div class="lp-row is-static is-detail packing-control-row th-detail-edit">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">
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
    if (event.target.closest("[data-modal-close]")) {
      closeModal();
      return;
    }

    const stateButton = event.target.closest("[data-toggle-state]");
    if (stateButton) {
      toggleRecordState(stateButton.dataset.toggleState, stateButton.dataset.nextState);
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
          source: "8778-tack-horses"
        })
      });
      const result = await response.json();
      if (!response.ok || !result.ok) {
        throw new Error(result.detail || result.error || `Save failed: ${response.status}`);
      }
      record.fields[change.fieldName] = change.newValue;
      setStatus(`Saved to horses_change_log at ${new Date().toLocaleTimeString()}.`);
    } catch (error) {
      console.error("[8778-tack-horses]", error);
      setStatus("Save failed. Check console.");
    }
  }

  async function toggleRecordState(recordId, nextState) {
    const record = state.records.find((item) => item.id === recordId);
    if (!record) return;
    const fields = record.fields || {};
    const fieldName = recordStateField(fields);
    const oldValue = fields[fieldName] || recordState(record);
    const name = firstValue(fields, ["show_name", "horse", "name", "Horse", "Name"]) || "Unnamed horse";
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;

    fields[fieldName] = nextState;
    render();
    if (state.activeRecordId === record.id && !els.modal.hidden) {
      els.modalContent.innerHTML = detail(record);
    }

    await saveRecordChange(record, {
      fieldName,
      oldValue,
      newValue: nextState,
      horseKey: recordKey,
      horseName: name
    });
  }

  function openHorse(recordId) {
    const record = state.records.find((item) => item.id === recordId);
    if (!record) return;
    state.activeRecordId = recordId;
    els.modalContent.innerHTML = detail(record);
    els.modal.hidden = false;
    document.body.style.overflow = "hidden";
    requestAnimationFrame(() => els.modalCard?.focus());
  }

  function closeModal() {
    state.activeRecordId = "";
    els.modal.hidden = true;
    els.modalContent.innerHTML = "";
    document.body.style.overflow = "";
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
      <div class="lp-shell">
        <header class="lp-header">
          <div class="lp-header-copy">
            <h1>8778 Tack Horses</h1>
            <p class="lp-subtitle">ww_horses - 8778-tack-horses</p>
          </div>
          <div class="lp-header-tools">
            <div class="lp-summary-row">
              <p data-th-status>Loading...</p>
            </div>
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
                <div class="lp-section-actions">
                  <button class="lp-filter-toggle th-section-pill" type="button">PDF SECTION</button>
                </div>
              </div>
              <div class="packing-tools th-toolbar">
                <input class="lp-edit-input th-search" type="search" placeholder="Search horses" data-th-search>
              </div>
              <div id="sectionRows" class="lp-list" data-th-list></div>
            </section>
          </section>
        </main>
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
      el.textContent = el.classList.contains("lp-tab-value") ? String(count) : `${count} shown`;
    });
  }

  function setStatus(message) {
    if (els.status) els.status.textContent = message;
  }

  function firstValue(fields, names) {
    for (const name of names) {
      if (fields[name] !== undefined && fields[name] !== null && fields[name] !== "") return fields[name];
    }
    return "";
  }

  function recordState(record) {
    const fields = record.fields || {};
    const value = String(firstValue(fields, ["record_state", "Record State", "state", "State", "status", "Status"]) || "inactive").trim().toLowerCase();
    return value === "active" ? "active" : "inactive";
  }

  function recordStateField(fields) {
    return ["record_state", "Record State", "state", "State", "status", "Status"].find((name) => Object.prototype.hasOwnProperty.call(fields, name)) || "record_state";
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
