(function () {
  const root = document.getElementById("packing-plan") || document.querySelector("[data-rs-plan]");
  if (!root) return;

  const globalConfig = window.WEC_PACKING_PLAN_CONFIG || {};
  const config = {
    planKey: normalizePlanKey(root.dataset.planKey || globalConfig.planKey || "quantity"),
    apiUrl: root.dataset.apiUrl || globalConfig.apiUrl || "",
    sessionUrl: root.dataset.sessionUrl || globalConfig.sessionUrl || "",
    printUrl: root.dataset.printUrl || globalConfig.printUrl || "",
    packWaveKey: root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one",
    viewKey: root.dataset.viewKey || globalConfig.viewKey || root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one",
    pollMs: Number(root.dataset.pollMs || globalConfig.pollMs || 5000)
  };
  if (!config.apiUrl) config.apiUrl = `/wec-packing/${routeName(config.planKey)}`;
  if (!config.sessionUrl) config.sessionUrl = "/wec-packing/session";

  const ui = {
    loading: true,
    error: "",
    search: "",
    laneKey: "open",
    secondaryView: config.viewKey || "wave_one",
    selectedItemId: "",
    drawerOpen: false,
    sortKey: "item",
    sortDir: "asc",
    savingKey: "",
    commentText: "",
    commentShortId: "",
    sessionKey: sessionKey(),
    sessionEngaged: false,
    retryingFailedActions: false
  };

  let state = null;
  let records = [];
  let loadInFlight = false;
  let pendingLoadOptions = null;
  let pollTimer = null;
  let pollInFlight = false;
  const pending = new Set();
  const failedActionStorageKey = `rsPlanFailedActions:${config.planKey}:v1`;
  let failedActions = loadFailedActions();

  load();
  window.addEventListener("online", () => retryFailedActions());

  root.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    const action = target.dataset.action;
    engageSession();

    if (action === "open-item") {
      ui.selectedItemId = target.dataset.itemId || "";
      ui.drawerOpen = true;
      render();
      return;
    }
    if (action === "close-drawer") {
      ui.drawerOpen = false;
      render();
      return;
    }
    if (action === "clear-search") {
      ui.search = "";
      rebuild();
      return;
    }
    if (action === "set-secondary-view") {
      ui.secondaryView = target.dataset.secondaryView || "all";
      config.viewKey = ui.secondaryView;
      if (isWaveView(ui.secondaryView)) config.packWaveKey = ui.secondaryView;
      await load();
      return;
    }
    if (action === "set-lane") {
      ui.laneKey = target.dataset.laneKey || "open";
      rebuild();
      return;
    }
    if (action === "set-sort") {
      setSort(target.dataset.sortKey || "item");
      return;
    }
    if (action === "print-list") {
      openPrint();
      return;
    }
    if (action === "adjust-packed") {
      await adjustPacked(target, captureScroll());
      return;
    }
    if (action === "set-needed") {
      await setNeeded(target, captureScroll());
      return;
    }
    if (action === "save-comment") {
      await saveComment(target, captureScroll());
    }
  });

  root.addEventListener("input", (event) => {
    const input = event.target;
    engageSession();
    if (input.matches("[data-search]")) {
      const scroll = captureScroll();
      ui.search = input.value || "";
      const caret = input.selectionStart || ui.search.length;
      rebuild();
      requestAnimationFrame(() => {
        const next = root.querySelector("[data-search]");
        if (next) {
          next.focus();
          next.setSelectionRange(caret, caret);
        }
        restoreScroll(scroll);
      });
      return;
    }
    if (input.matches("[data-comment-text]")) ui.commentText = input.value || "";
    if (input.matches("[data-comment-short]")) {
      ui.commentShortId = input.value || "";
      const selected = commentShorts().find((row) => row.id === ui.commentShortId);
      if (selected && !ui.commentText.trim()) {
        ui.commentText = selected.label || selected.comment || "";
        render();
      }
    }
  });

  async function load(options = {}) {
    if (loadInFlight) {
      pendingLoadOptions = options;
      if (options.silent !== true) {
        ui.loading = true;
        ui.error = "";
        render();
      }
      return;
    }
    loadInFlight = true;
    const silent = options.silent === true;
    if (!silent) {
      ui.loading = true;
      ui.error = "";
      render();
    }
    try {
      const requestPackWaveKey = config.packWaveKey;
      const requestViewKey = config.viewKey;
      const nextState = await fetchJson(apiUrl());
      if (requestPackWaveKey !== config.packWaveKey || requestViewKey !== config.viewKey) {
        pendingLoadOptions = options;
        return;
      }
      state = nextState;
      rebuild(false);
      pingSession();
    } catch (error) {
      if (!silent) {
        ui.error = error.message || String(error);
        render();
      }
    } finally {
      loadInFlight = false;
      if (pendingLoadOptions) {
        const nextOptions = pendingLoadOptions;
        pendingLoadOptions = null;
        await load(nextOptions);
        return;
      }
      if (!silent) {
        ui.loading = false;
        render();
      }
    }
  }

  async function pingSession() {
    if (!ui.sessionKey) return;
    fetchJson(sessionUrl(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(sessionPayload({ action: "session_ping" }))
    }).catch(() => {});
  }

  async function fetchJson(url, options) {
    const fetchOptions = {
      cache: "no-store",
      ...(options || {}),
      headers: {
        "Cache-Control": "no-cache",
        "X-RS-Session-Key": ui.sessionKey,
        ...((options && options.headers) || {})
      }
    };
    const response = await fetch(url, fetchOptions);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || `${response.status}`);
    return data;
  }

  function apiUrl() {
    const url = new URL(config.apiUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", config.viewKey || ui.secondaryView || "");
    url.searchParams.set("v", "1");
    return url.toString();
  }

  function sessionUrl() {
    const url = new URL(config.sessionUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", config.viewKey || ui.secondaryView || "");
    return url.toString();
  }

  function engageSession() {
    if (ui.sessionEngaged) return;
    ui.sessionEngaged = true;
    pingSession();
    startPolling();
  }

  function startPolling() {
    if (pollTimer) return;
    pollTimer = window.setInterval(pollState, config.pollMs || 5000);
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

  function requestStateRefresh(delay = 0) {
    if (delay > 0) {
      window.setTimeout(() => { void pollState(); }, delay);
      return;
    }
    void pollState();
  }

  function rebuild(shouldRender = true) {
    records = sortRows(filteredItems().filter(matchesLane));
    if (!records.some((row) => row.id === ui.selectedItemId) && !ui.drawerOpen) ui.selectedItemId = records[0]?.id || "";
    if (shouldRender) renderPreservingScroll();
  }

  function filteredItems() {
    const query = ui.search.trim().toLowerCase();
    const rows = state?.items || [];
    if (!query) return rows;
    return rows.filter((item) => [
      item.label,
      item.sourceLabel,
      item.sourceItemKey,
      item.unit,
      item.notes
    ].join(" ").toLowerCase().includes(query));
  }

  function matchesLane(record) {
    const key = ui.laneKey || "open";
    if (key === "all") return true;
    if (key === "open" || key === "left") return record.left > 0;
    if (key === "need" || key === "needed") return record.need > 0;
    if (key === "packed") return record.packed > 0;
    return true;
  }

  function setSort(key) {
    if (ui.sortKey === key) ui.sortDir = ui.sortDir === "asc" ? "desc" : "asc";
    else {
      ui.sortKey = key;
      ui.sortDir = "asc";
    }
    records = sortRows(records);
    render();
  }

  function sortRows(rows) {
    const dir = ui.sortDir === "desc" ? -1 : 1;
    return [...rows].sort((a, b) => {
      if (ui.sortKey === "source") return compareText(a.sourceLabel, b.sourceLabel) * dir || compareText(a.label, b.label);
      if (ui.sortKey === "need") return compareNumber(a.need, b.need) * dir || compareText(a.label, b.label);
      if (ui.sortKey === "packed") return compareNumber(a.packed, b.packed) * dir || compareText(a.label, b.label);
      if (ui.sortKey === "left") return compareNumber(a.left, b.left) * dir || compareText(a.label, b.label);
      return compareText(a.label, b.label) * dir;
    });
  }

  async function adjustPacked(button, scroll) {
    const item = selectedItem();
    if (!item) return;
    const key = `packed:${item.id}`;
    if (pending.has(key)) return;
    const delta = Number(button.dataset.delta || 0);
    const exceptionState = button.dataset.exceptionState || "";
    const nextPacked = exceptionState === "packed_max" ? item.need : clamp(item.packed + delta, 0, item.need);
    pending.add(key);
    ui.savingKey = key;
    updateLocalItem(item.id, { packed: nextPacked, left: Math.max(0, item.need - nextPacked), exceptionState: exceptionState || item.exceptionState });
    renderPreservingScroll(scroll);
    try {
      const payload = sessionPayload({
        action: "set_item_count",
        itemId: item.id,
        delta,
        exceptionState
      });
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state = result.state || state;
      rebuild(false);
      requestStateRefresh(1000);
    } catch (error) {
      ui.error = error.message || String(error);
      saveFailedAction({
        url: apiUrl(),
        payload: sessionPayload({
          action: "set_item_count",
          itemId: item.id,
          delta,
          exceptionState
        })
      });
      await load();
    } finally {
      pending.delete(key);
      ui.savingKey = "";
      ui.drawerOpen = true;
      ui.selectedItemId = item.id;
      renderPreservingScroll(scroll);
    }
  }

  async function setNeeded(button, scroll) {
    const item = selectedItem();
    if (!item) return;
    const input = root.querySelector("[data-needed-input]");
    const nextNeed = Math.max(0, Math.round(Number(input?.value || item.need) || 0));
    const key = `needed:${item.id}`;
    if (pending.has(key)) return;
    pending.add(key);
    ui.savingKey = key;
    updateLocalItem(item.id, { need: nextNeed, packed: Math.min(item.packed, nextNeed), left: Math.max(0, nextNeed - Math.min(item.packed, nextNeed)) });
    renderPreservingScroll(scroll);
    try {
      const payload = sessionPayload({
        action: "adjust_needed",
        itemId: item.id,
        needed: nextNeed,
        reason: "manual_adjustment"
      });
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state = result.state || state;
      rebuild(false);
      requestStateRefresh(1000);
    } catch (error) {
      ui.error = error.message || String(error);
      saveFailedAction({
        url: apiUrl(),
        payload: sessionPayload({
          action: "adjust_needed",
          itemId: item.id,
          needed: nextNeed,
          reason: "manual_adjustment"
        })
      });
      await load();
    } finally {
      pending.delete(key);
      ui.savingKey = "";
      ui.drawerOpen = true;
      ui.selectedItemId = item.id;
      renderPreservingScroll(scroll);
    }
  }

  async function saveComment(button, scroll) {
    const item = ui.drawerOpen ? selectedItem() : null;
    const comment = ui.commentText.trim();
    if (!comment) return;
    ui.savingKey = "comment";
    renderPreservingScroll(scroll);
    try {
      const payload = sessionPayload({
        action: "save_comment",
        scopeType: item ? "item" : "plan",
        scopeId: item?.id || state?.plan?.key || config.planKey,
        scopeLabel: item?.label || state?.plan?.label || config.planKey,
        commentShortId: ui.commentShortId,
        comment
      });
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state = result.state || state;
      ui.commentText = "";
      ui.commentShortId = "";
      rebuild(false);
      requestStateRefresh(1000);
    } catch (error) {
      ui.error = error.message || String(error);
      saveFailedAction({
        url: apiUrl(),
        payload: sessionPayload({
          action: "save_comment",
          scopeType: item ? "item" : "plan",
          scopeId: item?.id || state?.plan?.key || config.planKey,
          scopeLabel: item?.label || state?.plan?.label || config.planKey,
          commentShortId: ui.commentShortId,
          comment
        })
      });
    } finally {
      ui.savingKey = "";
      renderPreservingScroll(scroll);
    }
  }

  function updateLocalItem(itemId, patch) {
    if (!state?.items) return;
    state.items = state.items.map((item) => item.id === itemId ? { ...item, ...patch } : item);
    records = records.map((item) => item.id === itemId ? { ...item, ...patch } : item);
  }

  function renderPreservingScroll(scroll = captureScroll()) {
    render();
    restoreScroll(scroll);
  }

  function captureScroll() {
    const table = root.querySelector(".rs-airtable-scroll");
    const drawer = root.querySelector(".rs-drawer-body");
    return {
      windowX: window.scrollX || 0,
      windowY: window.scrollY || 0,
      tableTop: table ? table.scrollTop : 0,
      tableLeft: table ? table.scrollLeft : 0,
      drawerTop: drawer ? drawer.scrollTop : 0
    };
  }

  function restoreScroll(scroll) {
    const apply = () => {
      window.scrollTo(scroll.windowX || 0, scroll.windowY || 0);
      const table = root.querySelector(".rs-airtable-scroll");
      const drawer = root.querySelector(".rs-drawer-body");
      if (table) {
        table.scrollTop = scroll.tableTop || 0;
        table.scrollLeft = scroll.tableLeft || 0;
      }
      if (drawer) drawer.scrollTop = scroll.drawerTop || 0;
    };
    requestAnimationFrame(apply);
    setTimeout(apply, 0);
  }

  function render() {
    if (ui.loading && !state) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-stack-section"><div class="rs-stack-label">LOADING</div></div></div>`;
      return;
    }
    const rows = activeRows();
    root.innerHTML = `
      <div class="rs-airtable-shell">
        <div class="rs-page-stack">${rows.map(stackRow).join("")}</div>
        ${drawerHtml()}
      </div>
    `;
  }

  function activeRows() {
    const rows = state?.groupStack?.activeRows || [];
    const order = new Map([
      ["header", 0],
      ["primary_tabs", 1],
      ["summary_aggs", 2],
      ["secondary_controls", 3],
      ["secondary_count_aggs", 4],
      ["count_aggs", 4],
      ["lane_controls", 5],
      ["search", 6],
      ["main_table", 7],
      ["comments", 8]
    ]);
    const seen = new Set();
    return rows
      .filter((row) => order.has(row.renderKey))
      .sort((a, b) => {
        const byOrder = (order.get(a.renderKey) ?? 99) - (order.get(b.renderKey) ?? 99);
        if (byOrder) return byOrder;
        return (Number(a.sortOrder) || 0) - (Number(b.sortOrder) || 0);
      })
      .filter((row) => {
        if (seen.has(row.renderKey)) return false;
        seen.add(row.renderKey);
        return true;
      });
  }

  function stackRow(row) {
    const key = row.renderKey;
    if (key === "header") return section(row, headerHtml(), "is-header");
    if (key === "primary_tabs") return section(row, pillsHtml(state?.primaryTabs || [], "", "primary"), "is-primary-tabs");
    if (key === "summary_aggs") return section(row, `<div class="rs-stack-label">${escapeHtml(row.displayLabel || state?.plan?.label || "")}</div><div class="rs-stack-aggs">${summaryAggs(row)}</div>`, "is-summary-aggs");
    if (key === "secondary_controls") return section(row, pillsHtml(state?.secondaryControls || [], ui.secondaryView, "secondary"), "is-secondary-controls");
    if (key === "secondary_count_aggs" || key === "count_aggs") return section(row, `<div class="rs-secondary-count-aggs"><div class="rs-stack-aggs is-counts">${countAggs(row)}</div></div>`, "is-count-aggs");
    if (key === "lane_controls") return section(row, pillsHtml(state?.laneControls || [], ui.laneKey, "lane"), "is-lane-controls");
    if (key === "search") return section(row, searchHtml(), "is-search");
    if (key === "main_table") return section(row, tableHtml(row), "is-main-table");
    if (key === "comments") return section(row, commentsPageHtml(row), "is-comments");
    return "";
  }

  function section(row, html, className) {
    return `<section class="rs-stack-section ${className || ""}" data-render-key="${escapeAttr(row.renderKey || "")}">${html}</section>`;
  }

  function headerHtml() {
    const wave = state?.wave || {};
    return `<div class="rs-page-header"><div class="rs-page-title">${escapeHtml(wave.reportTitle || state?.plan?.label || "WEC PACK")}</div><div class="rs-page-subtitle">${escapeHtml(wave.reportSubtitle || wave.label || "")}</div></div>`;
  }

  function pillsHtml(rows, activeKey, type) {
    return `<div class="rs-stack-tabs ${type === "secondary" ? "is-compact" : ""}">${(rows || []).map((row) => {
      const key = row.key || row.id || "";
      const data = type === "lane"
        ? `data-action="set-lane" data-lane-key="${escapeAttr(key)}"`
        : type === "secondary"
          ? `data-action="set-secondary-view" data-secondary-view="${escapeAttr(key)}"`
          : "";
      return `<button class="rs-stack-pill ${activeKey === key || (!activeKey && row.active) ? "is-active" : ""}" type="button" ${data}>${escapeHtml(row.label || key)}</button>`;
    }).join("")}</div>`;
  }

  function summaryAggs(row) {
    const values = { items: records.length, need: totals().need, touched: records.filter((item) => item.packed > 0 || item.exceptionState).length };
    return aggList(row, values);
  }

  function countAggs(row) {
    return aggList(row, totals());
  }

  function aggList(row, values) {
    const defs = row.aggs || [];
    return defs.map((def) => agg(values[def.key] ?? values[normalizePlanKey(def.key)] ?? 0, def.label || def.key, def.key, def.shade)).join("");
  }

  function totals() {
    return records.reduce((sum, item) => {
      sum.need += item.need || 0;
      sum.packed += item.packed || 0;
      sum.left += item.left || 0;
      return sum;
    }, { need: 0, packed: 0, left: 0 });
  }

  function agg(value, label, key, shade) {
    return `<div class="rs-stack-agg is-${escapeAttr(key)} is-shade-${escapeAttr(shade || "")}"><div class="rs-stack-agg-value">${escapeHtml(value)}</div><div class="rs-stack-agg-label">${escapeHtml(label)}</div></div>`;
  }

  function searchHtml() {
    return `<div class="rs-airtable-toolbar"><div class="rs-search-wrap"><input class="rs-search" type="text" data-search autocomplete="off" placeholder="Search ${escapeAttr((state?.plan?.label || "items").toLowerCase())}" value="${escapeAttr(ui.search)}"><button class="rs-search-clear ${ui.search ? "is-active" : ""}" type="button" aria-label="Clear search" data-action="clear-search"><span aria-hidden="true">&times;</span></button></div></div>`;
  }

  function tableHtml(row) {
    return `
      <div class="rs-table-stack-head"><div class="rs-stack-label">${escapeHtml(row.displayLabel || state?.plan?.label || "Items")}</div><button class="rs-stack-pill" type="button" data-action="print-list">PRINT</button></div>
      <div class="rs-airtable-scroll">
        <table class="rs-airtable-grid">
          <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-entity"><col class="rs-col-count"><col class="rs-col-count"><col class="rs-col-count"></colgroup>
          <thead><tr><th class="rs-row-gutter">#</th>${head("ITEM", "item")}${head("SOURCE", "source")}${head("NEED", "need")}${head("PACKED", "packed")}${head("LEFT", "left")}</tr></thead>
          <tbody>${records.map(rowHtml).join("") || `<tr><td class="rs-empty-row" colspan="6">No items.</td></tr>`}</tbody>
        </table>
      </div>
      ${ui.error ? `<div class="rs-status is-error">${escapeHtml(ui.error)}</div>` : ""}
    `;
  }

  function head(label, key) {
    return `<th><button class="rs-sort-head ${ui.sortKey === key ? "is-active" : ""}" type="button" data-action="set-sort" data-sort-key="${escapeAttr(key)}"><span>${escapeHtml(label)}</span></button></th>`;
  }

  function rowHtml(item, index) {
    return `<tr class="${item.id === ui.selectedItemId && ui.drawerOpen ? "is-selected" : ""}" data-action="open-item" data-item-id="${escapeAttr(item.id)}" tabindex="0">
      <td class="rs-row-gutter">${index + 1}</td>
      <td class="rs-entity-cell"><div class="rs-entity-main"><span class="rs-entity-horse">${escapeHtml(item.label)}</span><span class="rs-open-text">Open</span></div></td>
      <td class="rs-entity-cell"><span class="rs-entity-sub">${escapeHtml(item.sourceLabel || item.sourceItemKey || "")}</span></td>
      <td class="rs-cell-number">${item.need}</td>
      <td class="rs-cell-number">${item.packed}</td>
      <td class="rs-cell-number">${item.left}</td>
    </tr>`;
  }

  function drawerHtml() {
    const item = selectedItem();
    if (!item) return "";
    const percent = item.need ? Math.round((item.packed / item.need) * 100) : 0;
    const canAdjustNeed = state?.plan?.key === "quantity";
    return `<div class="rs-drawer-overlay ${ui.drawerOpen ? "is-open" : ""}" data-action="close-drawer" aria-hidden="true"></div>
    <aside class="rs-record-drawer ${ui.drawerOpen ? "is-open" : ""}" aria-hidden="${ui.drawerOpen ? "false" : "true"}">
      <div class="rs-drawer-head">
        <div class="rs-drawer-title-group"><div class="rs-page-subtitle">${escapeHtml(item.label)}</div></div>
        <button class="rs-drawer-close" type="button" data-action="close-drawer" aria-label="Close"><span aria-hidden="true">&times;</span></button>
      </div>
      <div class="rs-drawer-body">
        <div class="rs-detail-summary">
          <div class="rs-kit-progress"><div class="rs-kit-progress-label">${percent}% PACKED</div><div class="rs-kit-progress-track"><div class="rs-kit-progress-bar" style="width:${percent}%"></div></div></div>
          <div class="rs-summary-metrics">${agg(item.need, "NEED", "need", "brown")}${agg(item.packed, "PACKED", "packed", "green")}${agg(item.left, "LEFT", "left", "grey")}</div>
        </div>
        <div class="rs-kit-item-row">
          <div class="rs-kit-item-main"><div class="rs-stack-label">PACKED</div></div>
          <div class="rs-kit-actions">${countButton("-1", -1)}${countButton("+1", 1)}${countButton("MAX", 0, "packed_max")}</div>
        </div>
        ${canAdjustNeed ? `<div class="rs-add-row"><label class="rs-stack-label" for="rs-needed-input">NEED</label><input id="rs-needed-input" class="rs-add-input" data-needed-input inputmode="numeric" value="${item.need}"><button class="rs-plain-button" type="button" data-action="set-needed">${ui.savingKey === `needed:${item.id}` ? "SAVING" : "SAVE"}</button></div>` : ""}
        <div class="rs-decision-row"><div class="rs-stack-label">EXCEPTIONS</div><div class="rs-decision-actions">${exceptionButton("UNRESOLVED", "unresolved")}${exceptionButton("PURCHASE ONSITE", "purchase_onsite")}${exceptionButton("NEEDS ATTN", "needs_attention")}</div></div>
        ${drawerComments(item)}
      </div>
    </aside>`;
  }

  function countButton(label, delta, exceptionState) {
    const item = selectedItem();
    return `<button class="rs-state-button ${ui.savingKey === `packed:${item?.id}` ? "is-saving" : ""}" type="button" data-action="adjust-packed" data-delta="${escapeAttr(delta)}" data-exception-state="${escapeAttr(exceptionState || "")}">${escapeHtml(label)}</button>`;
  }

  function exceptionButton(label, exceptionState) {
    const item = selectedItem();
    return `<button class="rs-state-button ${item?.exceptionState === exceptionState ? "is-active" : ""}" type="button" data-action="adjust-packed" data-delta="0" data-exception-state="${escapeAttr(exceptionState)}">${escapeHtml(label)}</button>`;
  }

  function drawerComments(item) {
    return `<div class="rs-comments"><div class="rs-stack-label">COMMENTS</div>${commentFormHtml("item")}<div class="rs-comment-list">${itemComments(item.id).map(commentHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}</div></div>`;
  }

  function commentsPageHtml(row) {
    const comments = state?.comments || [];
    return `<div class="rs-comments is-page-comments"><div class="rs-stack-label">${escapeHtml(row.displayLabel || "Comments")}</div>${commentFormHtml("plan")}<div class="rs-comment-list">${comments.map(commentHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}</div></div>`;
  }

  function commentFormHtml(scope) {
    return `<div class="rs-comment-form"><select class="rs-comment-short" data-comment-short><option value="">Comment short</option>${commentShorts().map((row) => `<option value="${escapeAttr(row.id)}" ${ui.commentShortId === row.id ? "selected" : ""}>${escapeHtml(row.label || row.comment || row.id)}</option>`).join("")}</select><textarea class="rs-comment-input" data-comment-text rows="3" placeholder="Add comment">${escapeHtml(ui.commentText)}</textarea><button class="rs-plain-button is-primary" type="button" data-action="save-comment" data-comment-scope="${escapeAttr(scope)}">${ui.savingKey === "comment" ? "SAVING" : "ADD"}</button></div>`;
  }

  function commentHtml(comment) {
    return `<div class="rs-comment-row"><div class="rs-comment-body">${escapeHtml(comment.comment || comment.label || "")}</div><div class="rs-comment-meta">${escapeHtml(comment.scopeLabel || comment.createdAt || "")}</div></div>`;
  }

  function selectedItem() {
    return records.find((row) => row.id === ui.selectedItemId) || (state?.items || []).find((row) => row.id === ui.selectedItemId) || records[0] || null;
  }

  function itemComments(itemId) {
    return (state?.comments || []).filter((comment) => comment.scopeId === itemId);
  }

  function commentShorts() {
    return (state?.commentShorts || []).filter((row) => row.active !== false);
  }

  function openPrint() {
    const url = new URL(config.printUrl || `${config.apiUrl}/print`, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", config.viewKey || ui.secondaryView || "");
    window.open(url.toString(), "_blank", "noopener");
  }

  function sessionPayload(extra) {
    return {
      ...extra,
      sessionKey: ui.sessionKey,
      deviceId: deviceId(),
      packWaveId: state?.source?.packWaveId || "",
      currentLane: ui.laneKey,
      currentList: state?.plan?.key || config.planKey,
      currentFilter: ui.secondaryView
    };
  }

  function saveFailedAction(action) {
    failedActions.push({
      ...action,
      id: `failed:${Date.now()}:${Math.random().toString(16).slice(2)}`,
      createdAt: new Date().toISOString()
    });
    storeFailedActions();
  }

  async function retryFailedActions() {
    if (!failedActions.length || ui.retryingFailedActions) return;
    ui.retryingFailedActions = true;
    const remaining = [];
    for (const action of failedActions) {
      try {
        await fetchJson(action.url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(action.payload || {})
        });
      } catch (error) {
        remaining.push(action);
      }
    }
    failedActions = remaining;
    storeFailedActions();
    ui.retryingFailedActions = false;
    if (!failedActions.length) requestStateRefresh(250);
  }

  function loadFailedActions() {
    try {
      const parsed = JSON.parse(localStorage.getItem(failedActionStorageKey) || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  }

  function storeFailedActions() {
    try {
      localStorage.setItem(failedActionStorageKey, JSON.stringify(failedActions.slice(-50)));
    } catch (error) {}
  }

  function sessionKey() {
    const key = `rs_plan_session_${config.planKey}`;
    let value = sessionStorage.getItem(key);
    if (!value) {
      value = `${config.planKey}:${Date.now()}:${Math.random().toString(16).slice(2)}`;
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

  function routeName(planKey) {
    if (planKey === "per_horse") return "per-horse";
    if (planKey === "per_groom") return "per-groom";
    return "quantity";
  }

  function normalizePlanKey(value) {
    return String(value || "").trim().toLowerCase().replace(/-/g, "_");
  }

  function isWaveView(value) {
    return ["wave_one", "wave_two"].includes(normalizePlanKey(value));
  }

  function compareText(a, b) {
    return String(a || "").localeCompare(String(b || ""), undefined, { numeric: true, sensitivity: "base" });
  }

  function compareNumber(a, b) {
    return (Number(a) || 0) - (Number(b) || 0);
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, Math.max(0, Math.round(Number(value) || 0))));
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }
})();
