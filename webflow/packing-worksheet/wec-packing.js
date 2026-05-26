(function () {
  const rootId = "wec-packing-app";
  const legacyRootId = "packing-app";
  const defaultConfig = {
    mode: "edit",
    apiBaseUrl: "https://ringstatus.webflow.io/test/wec-packing",
    showId: "",
    packWaveId: "",
    groomCount: ""
  };

  const config = { ...defaultConfig, ...(window.WEC_PACKING_CONFIG || {}) };
  const root = document.getElementById(rootId) || document.getElementById(legacyRootId);
  if (!root) return;

  root.innerHTML = `
    <div class="lp-shell packing-shell">
      <header class="lp-hero">
        <h1>WEC Ocala Packing</h1>
        <p class="packing-save-state" data-wec-status>Loading live worksheet...</p>
      </header>
      <main class="lp-main">
        <section class="lp-section-block packing-theme-overview">
          <div class="lp-section-head">
            <span>
              <span class="lp-section-title">Overview</span>
              <span class="lp-row-meta" data-wec-summary>Loading</span>
            </span>
          </div>
          <div data-wec-content></div>
        </section>
      </main>
    </div>
  `;

  loadState();

  async function loadState() {
    try {
      const state = await fetchState();
      if (!state.ok) throw new Error(state.error || "state_failed");
      setStatus("Live Airtable worksheet");
      renderState(state);
    } catch (error) {
      setStatus("Live worksheet unavailable");
      setSummary("Unable to load live worksheet");
      content().innerHTML = `<div class="lp-row is-static"><span><span class="lp-row-title">Live data unavailable</span><span class="lp-row-meta">${escapeHtml(error.message || String(error))}</span></span></div>`;
    }
  }

  async function fetchState() {
    const url = new URL(`${config.apiBaseUrl.replace(/\/$/, "")}/state`);
    if (config.showId) url.searchParams.set("showId", config.showId);
    if (config.packWaveId) url.searchParams.set("packWaveId", config.packWaveId);
    if (config.groomCount) url.searchParams.set("groomCount", config.groomCount);
    const response = await fetch(url.toString(), { headers: { Accept: "application/json" } });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.detail || body.error || `HTTP ${response.status}`);
    return body;
  }

  function renderState(state) {
    const packedCount = state.items.filter((item) => item.pack_state === "packed" || item.resolution_state).length;
    const openCount = Math.max(0, state.items.length - packedCount);
    setSummary(`Rows: ${state.items.length} | Left ${openCount}`);
    content().innerHTML = [
      renderGateNotice(state),
      renderSectionGroups(state)
    ].filter(Boolean).join("");
  }

  function renderGateNotice(state) {
    const planned = state.gates?.plannedTables || [];
    if (!planned.length) return "";
    return `
      <div class="lp-row is-static">
        <span>
          <span class="lp-row-title">Implementation gates</span>
          <span class="lp-row-meta">Planned tables: ${planned.map(escapeHtml).join(", ")}</span>
        </span>
      </div>
    `;
  }

  function renderSectionGroups(state) {
    const groups = groupBy(state.items, (item) => item.section_label || item.section || "Unassigned");
    return Object.entries(groups).map(([label, items]) => {
      const packed = items.filter((item) => item.pack_state === "packed" || item.resolution_state).length;
      const left = Math.max(0, items.length - packed);
      return `
        <button class="lp-row packing-row" type="button">
          <span>
            <span class="lp-row-title">${escapeHtml(label)}</span>
            <span class="lp-row-meta">Rows: ${items.length} | Left ${left}</span>
          </span>
          ${token(left === 0 ? "packed" : "open", left === 0 ? "PACKED" : `LEFT - ${left}`)}
        </button>
      `;
    }).join("");
  }

  function token(value, label) {
    const className = value === "packed" ? "is-packed" : value === "need" ? "is-need" : "is-open";
    return `<span class="lp-achievement packing-token ${className}">${escapeHtml(label)}</span>`;
  }

  function groupBy(items, getter) {
    return items.reduce((groups, item) => {
      const key = getter(item);
      groups[key] = groups[key] || [];
      groups[key].push(item);
      return groups;
    }, {});
  }

  function setSummary(value) {
    const node = root.querySelector("[data-wec-summary]");
    if (node) node.textContent = value;
  }

  function setStatus(value) {
    const node = root.querySelector("[data-wec-status]");
    if (node) node.textContent = value;
  }

  function content() {
    return root.querySelector("[data-wec-content]");
  }

  function escapeHtml(value) {
    return String(value || "").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    }[char]));
  }
})();
