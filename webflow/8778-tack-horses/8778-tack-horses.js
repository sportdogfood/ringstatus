(async () => {
  const root = document.getElementById("tack-horses-app");
  if (!root) return;

  const config = window.TACK_HORSES_CONFIG || {};
  const apiUrl = config.apiUrl || "/8778-tack-horses/horses";
  const state = {
    records: [],
    query: "",
    saveTimers: new Map()
  };

  root.innerHTML = shell();

  const els = {
    count: root.querySelector("[data-th-count]"),
    status: root.querySelector("[data-th-status]"),
    list: root.querySelector("[data-th-list]"),
    search: root.querySelector("[data-th-search]")
  };

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
      els.list.innerHTML = `<li class="th-error">Horses failed to load. Check console for [8778-tack-horses].</li>`;
      setStatus("Load failed.");
    }
  }

  function render() {
    const records = filteredRecords();
    els.count.textContent = `${records.length} shown`;
    if (!records.length) {
      els.list.innerHTML = `<li class="th-empty">No horses found.</li>`;
      return;
    }

    els.list.innerHTML = records.map((record, index) => row(record, index)).join("");
  }

  function row(record, index) {
    const fields = record.fields || {};
    const name = firstValue(fields, ["show_name", "horse", "name", "Horse", "Name"]) || "Unnamed horse";
    const barnName = firstValue(fields, ["barn_name", "Barn Name", "barn"]);
    const usef = firstValue(fields, ["usef", "USEF", "usef_id", "USEF ID"]);
    const color = firstValue(fields, ["horse_color", "color", "Color"]);
    const gender = firstValue(fields, ["horse_gender", "gender", "Gender"]);
    const recordKey = firstValue(fields, ["record_key", "horse_key", "source_id"]) || record.id;
    const stripe = index % 2 ? "is-even" : "is-odd";

    return `
      <li class="th-row ${stripe}" data-th-record="${escapeAttr(record.id)}" data-th-key="${escapeAttr(recordKey)}" data-th-name="${escapeAttr(name)}">
        <div>
          <div class="th-name">${escapeHtml(name)}</div>
          <div class="th-meta">${[barnName, usef ? `USEF ${usef}` : ""].filter(Boolean).map(escapeHtml).join(" · ")}</div>
          ${record.id ? `<a class="th-link" href="https://airtable.com/${escapeAttr(record.id)}" target="_blank" rel="noopener">airtable</a>` : ""}
        </div>
        ${editField(record, "barn_name", "Barn name", barnName)}
        <div class="th-field">
          <span class="th-label">Profile</span>
          <input class="th-input" data-th-field="horse_color" value="${escapeAttr(color)}" placeholder="Color">
          <input class="th-input" data-th-field="horse_gender" value="${escapeAttr(gender)}" placeholder="Gender">
        </div>
      </li>
    `;
  }

  function editField(record, fieldName, label, value) {
    return `
      <label class="th-field">
        <span class="th-label">${escapeHtml(label)}</span>
        <input class="th-input" data-th-field="${escapeAttr(fieldName)}" value="${escapeAttr(value)}">
      </label>
    `;
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

  function filteredRecords() {
    if (!state.query) return state.records;
    return state.records.filter((record) => {
      const fields = record.fields || {};
      return Object.values(fields).some((value) => String(value).toLowerCase().includes(state.query));
    });
  }

  function setStatus(message) {
    els.status.textContent = message;
  }

  function firstValue(fields, names) {
    for (const name of names) {
      if (fields[name] !== undefined && fields[name] !== null && fields[name] !== "") return fields[name];
    }
    return "";
  }

  function shell() {
    return `
      <section class="th-shell">
        <header class="th-header">
          <div>
            <h1 class="th-title">8778 Tack Horses</h1>
            <p class="th-subtitle">ww_horses · 8778-tack-horses</p>
          </div>
          <div>
            <span class="th-pill" data-th-count>0 shown</span>
            <p class="th-status" data-th-status>Loading...</p>
          </div>
        </header>
        <div class="th-toolbar">
          <input class="th-search" type="search" placeholder="Search horses" data-th-search>
        </div>
        <ul class="th-list" data-th-list></ul>
      </section>
    `;
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
