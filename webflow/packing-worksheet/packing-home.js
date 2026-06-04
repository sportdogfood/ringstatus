(function () {
  const root = document.getElementById("packing-home") || document.querySelector("[data-rs-home]");
  if (!root) return;

  const globalConfig = window.WEC_PACKING_HOME_CONFIG || {};
  const config = {
    apiUrl: root.dataset.apiUrl || globalConfig.apiUrl || "/wec-packing/home",
    sessionUrl: root.dataset.sessionUrl || globalConfig.sessionUrl || "/wec-packing/session",
    pageBaseUrl: root.dataset.pageBaseUrl || globalConfig.pageBaseUrl || "",
    packWaveKey: root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one",
    viewKey: root.dataset.viewKey || globalConfig.viewKey || root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one",
    pollMs: Number(root.dataset.pollMs || globalConfig.pollMs || 5000)
  };

  const ui = {
    loading: true,
    error: "",
    sessionKey: sessionKey(),
    sessionEngaged: false
  };

  let state = null;
  let loadInFlight = false;
  let pollTimer = null;
  let pollInFlight = false;

  root.addEventListener("click", (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    engageSession();
    if (target.dataset.action === "open-module") {
      const href = target.dataset.href || "";
      if (href) window.location.href = href;
    }
  });

  load();

  async function load(options = {}) {
    if (loadInFlight) return;
    loadInFlight = true;
    const silent = options.silent === true;
    if (!silent) {
      ui.loading = true;
      ui.error = "";
      render();
    }
    try {
      state = await fetchJson(apiUrl());
      pingSession();
    } catch (error) {
      if (!silent) ui.error = error.message || String(error);
    } finally {
      loadInFlight = false;
      if (!silent) ui.loading = false;
      render();
    }
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, {
      cache: "no-store",
      ...(options || {}),
      headers: {
        "Cache-Control": "no-cache",
        "X-RS-Session-Key": ui.sessionKey,
        ...((options && options.headers) || {})
      }
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || `${response.status}`);
    return data;
  }

  function apiUrl() {
    const url = new URL(config.apiUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", config.viewKey);
    url.searchParams.set("_", String(Date.now()));
    return url.toString();
  }

  function sessionUrl() {
    const url = new URL(config.sessionUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", config.viewKey);
    return url.toString();
  }

  function engageSession() {
    if (ui.sessionEngaged) return;
    ui.sessionEngaged = true;
    pingSession();
    if (!pollTimer) pollTimer = window.setInterval(pollState, config.pollMs || 5000);
  }

  async function pollState() {
    if (!ui.sessionEngaged || pollInFlight || ui.loading || document.hidden) return;
    pollInFlight = true;
    try {
      await load({ silent: true });
    } finally {
      pollInFlight = false;
    }
  }

  function pingSession() {
    fetchJson(sessionUrl(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "session_ping",
        sessionKey: ui.sessionKey,
        deviceId: deviceId(),
        packWaveKey: config.packWaveKey,
        viewKey: config.viewKey,
        currentLane: "home",
        currentList: "home",
        currentFilter: config.viewKey
      })
    }).catch(() => {});
  }

  function render() {
    if (ui.loading && !state) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-stack-section"><div class="rs-stack-label">LOADING</div></div></div>`;
      return;
    }
    root.innerHTML = `
      <div class="rs-airtable-shell">
        <div class="rs-page-stack">
          ${headerHtml()}
          ${summaryHtml()}
          ${moduleTableHtml()}
          ${ui.error ? `<section class="rs-stack-section"><div class="rs-status is-error">${escapeHtml(ui.error)}</div></section>` : ""}
        </div>
      </div>
    `;
  }

  function headerHtml() {
    const title = "WEC PACK";
    const subtitle = displayLabel(state?.source?.viewKey || config.viewKey);
    return `
      <section class="rs-stack-section is-header">
        <div class="rs-page-header">
          <div class="rs-page-title">${escapeHtml(title)}</div>
          <div class="rs-page-subtitle">${escapeHtml(subtitle)}</div>
        </div>
      </section>
    `;
  }

  function summaryHtml() {
    const modules = state?.modules || [];
    const need = modules.reduce((sum, row) => sum + number(row.counts?.need), 0);
    const packed = modules.reduce((sum, row) => sum + number(row.counts?.packed), 0);
    const left = modules.reduce((sum, row) => sum + number(row.counts?.left), 0);
    return `
      <section class="rs-stack-section is-summary-aggs">
        <div class="rs-stack-head"><div class="rs-stack-label">OVERVIEW</div></div>
        <div class="rs-stack-aggs">
          ${agg(need, "NEED", "need", "brown")}
          ${agg(packed, "PACKED", "packed", "green")}
          ${agg(left, "LEFT", "left", "grey")}
        </div>
      </section>
    `;
  }

  function moduleTableHtml() {
    const modules = state?.modules || [];
    return `
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">MODULES</div></div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup>
              <col class="rs-col-gutter">
              <col class="rs-col-entity">
              <col class="rs-col-count">
              <col class="rs-col-count">
              <col class="rs-col-count">
            </colgroup>
            <thead><tr><th class="rs-row-gutter">#</th><th>MODULE</th><th>NEED</th><th>PACKED</th><th>LEFT</th></tr></thead>
            <tbody>${modules.map(moduleRowHtml).join("") || `<tr><td class="rs-empty-row" colspan="5">No modules.</td></tr>`}</tbody>
          </table>
        </div>
      </section>
    `;
  }

  function moduleRowHtml(row, index) {
    const route = routeFor(row.key);
    return `
      <tr data-action="open-module" data-href="${escapeAttr(route)}" tabindex="0">
        <td class="rs-row-gutter">${index + 1}</td>
        <td class="rs-entity-cell"><div class="rs-entity-main"><span class="rs-entity-horse">${escapeHtml(row.label)}</span><span class="rs-open-text">Open</span></div></td>
        <td class="rs-cell-number">${escapeHtml(row.counts?.need || 0)}</td>
        <td class="rs-cell-number">${escapeHtml(row.counts?.packed || 0)}</td>
        <td class="rs-cell-number">${escapeHtml(row.counts?.left || 0)}</td>
      </tr>
    `;
  }

  function routeFor(key) {
    const route = key === "horse_kits" ? "horse-kits" : key === "per_horse" ? "per-horse" : key === "per_groom" ? "per-groom" : "quantity";
    if (config.pageBaseUrl) {
      const url = new URL(config.pageBaseUrl, window.location.href);
      if (route === "horse-kits") {
        url.pathname = url.pathname.replace(/[^/]*$/, "horse-kits-static-proof-preview.html");
      } else {
        url.pathname = url.pathname.replace(/[^/]*$/, "packing-plan-preview.html");
        url.searchParams.set("plan", route);
      }
      url.searchParams.set("packWaveKey", config.packWaveKey);
      url.searchParams.set("viewKey", config.viewKey);
      return url.toString();
    }
    return `/wec-packing/${route}?packWaveKey=${encodeURIComponent(config.packWaveKey)}&viewKey=${encodeURIComponent(config.viewKey)}`;
  }

  function agg(value, label, key, shade) {
    return `<div class="rs-stack-agg is-${escapeAttr(key)} is-shade-${escapeAttr(shade)}"><div class="rs-stack-agg-value">${escapeHtml(value)}</div><div class="rs-stack-agg-label">${escapeHtml(label)}</div></div>`;
  }

  function sessionKey() {
    const key = "rs_home_session";
    let value = sessionStorage.getItem(key);
    if (!value) {
      value = `home:${Date.now()}:${Math.random().toString(16).slice(2)}`;
      sessionStorage.setItem(key, value);
    }
    return value;
  }

  function deviceId() {
    const key = "rs_device_id";
    let value = localStorage.getItem(key);
    if (!value) {
      value = `device:${Date.now()}:${Math.random().toString(16).slice(2)}`;
      localStorage.setItem(key, value);
    }
    return value;
  }

  function number(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function displayLabel(value) {
    return String(value || "").replace(/[_-]+/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }
})();
