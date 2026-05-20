(async () => {
  const root = document.getElementById("tack-horses-app") || document.getElementById("lp-history-app");
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
      ? records.map((record) => row(record)).join("")
      : `<div class="lp-row is-static">No horses found.</div>`;
  }

  function row(record) {
    const fields = record.fields || {};
    const name = firstValue(fields, ["show_name", "horse", "name", "Horse", "Name"]) || "Unnamed horse";
    const barnName = firstValue(fields, ["barn_name", "Barn Name", "barn"]);
    const usef = firstValue(fields, ["usef", "USEF", "usef_id", "USEF ID"]);
    const color = firstValue(fields, ["horse_color", "color", "Color"]);
    const gender = firstValue(fields, ["horse_gender", "gender", "Gender"]);
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;
    const meta = [barnName, usef ? `USEF ${usef}` : ""].filter(Boolean).map(escapeHtml).join(" - ");

    return `
      <button class="lp-row th-horse-row" type="button" data-open-horse="${escapeAttr(record.id)}" data-th-key="${escapeAttr(recordKey)}" data-th-name="${escapeAttr(name)}">
        <span class="th-row-main">
          <span class="lp-row-title">${escapeHtml(name)}</span>
          ${meta ? `<span class="lp-row-meta">${meta}</span>` : ""}
        </span>
        <span class="lp-pill">Detail</span>
      </button>
    `;
  }

  function detail(record) {
    const fields = record.fields || {};
    const name = firstValue(fields, ["show_name", "horse", "name", "Horse", "Name"]) || "Unnamed horse";
    const barnName = firstValue(fields, ["barn_name", "Barn Name", "barn"]);
    const usef = firstValue(fields, ["usef", "USEF", "usef_id", "USEF ID"]);
    const color = firstValue(fields, ["horse_color", "color", "Color"]);
    const gender = firstValue(fields, ["horse_gender", "gender", "Gender"]);
    const type = firstValue(fields, ["horse_type", "type", "Type"]);
    const age = firstValue(fields, ["horse_age", "age", "Age"]);
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;

    return `
      <div class="lp-detail-head">
        <h2 class="lp-modal-title">${escapeHtml(name)}</h2>
        <p class="lp-row-meta">${[usef ? `USEF ${usef}` : "", recordKey].filter(Boolean).map(escapeHtml).join(" - ")}</p>
      </div>

      <section class="lp-section-block lp-theme-horses th-detail-section">
        <div class="lp-section-title">
          <h3>Detail</h3>
        </div>
        <div class="lp-list" data-th-record="${escapeAttr(record.id)}" data-th-key="${escapeAttr(recordKey)}" data-th-name="${escapeAttr(name)}">
          ${detailTextRow("Horse", name)}
          ${detailTextRow("USEF", usef || "-")}
          ${detailEditRow("barn_name", "Barn name", barnName)}
          ${detailEditRow("horse_color", "Color", color)}
          ${detailEditRow("horse_gender", "Gender", gender)}
          ${detailEditRow("horse_type", "Type", type)}
          ${detailEditRow("horse_age", "Age", age)}
          ${record.id ? detailRow("Airtable", `<a class="th-link" href="https://airtable.com/${escapeAttr(record.id)}" target="_blank" rel="noopener">airtable</a>`) : ""}
        </div>
      </section>
    `;
  }

  function detailTextRow(label, value) {
    return detailRow(label, escapeHtml(value));
  }

  function detailRow(label, value) {
    return `
      <div class="lp-row is-static is-detail">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">${value}</span>
      </div>
    `;
  }

  function detailEditRow(fieldName, label, value) {
    return `
      <div class="lp-row is-static is-detail th-detail-edit">
        <span class="lp-row-title">${escapeHtml(label)}</span>
        <span class="lp-row-meta">
          <input class="th-input" data-th-field="${escapeAttr(fieldName)}" value="${escapeAttr(value)}">
        </span>
      </div>
    `;
  }

  function handleClick(event) {
    if (event.target.closest("[data-modal-close]")) {
      closeModal();
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
    const newValue = input.value;
    if (String(oldValue) === String(newValue)) return;

    setStatus("Saving change...");
    try {
      const response = await fetch(apiUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          horseRecordId: record.id,
          horseKey: rowEl.dataset.thKey,
          horseName: rowEl.dataset.thName,
          fieldName,
          oldValue,
          newValue,
          source: "8778-tack-horses"
        })
      });
      const result = await response.json();
      if (!response.ok || !result.ok) {
        throw new Error(result.detail || result.error || `Save failed: ${response.status}`);
      }
      record.fields[fieldName] = newValue;
      setStatus(`Saved to horses_change_log at ${new Date().toLocaleTimeString()}.`);
    } catch (error) {
      console.error("[8778-tack-horses]", error);
      setStatus("Save failed. Check console.");
    }
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
          <button class="lp-tab lp-theme-horses is-active" type="button" aria-selected="true">
            <span class="lp-tab-value" data-th-count>0</span>
            <span class="lp-tab-label">Horses</span>
          </button>
        </nav>

        <main class="lp-content">
          <section class="lp-panel is-active">
            <section class="lp-section-block lp-theme-horses">
              <div class="lp-section-title">
                <h3>Horses</h3>
                <div class="lp-section-actions">
                  <span class="lp-section-count" data-th-count>0 shown</span>
                </div>
              </div>
              <div class="th-toolbar">
                <input class="th-search" type="search" placeholder="Search horses" data-th-search>
              </div>
              <div class="lp-list" data-th-list></div>
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
})();
