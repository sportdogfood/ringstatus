(() => {
  const DEFAULT_SHOW_NO = "14909";
  const FUNCTION_ORIGIN = "https://horseshowing-700800454.development.catalystserverless.com";
  const PRIMARY_API = window.RS_WEC_GRID_API || FUNCTION_ORIGIN + "/server/wec_live_grid/execute";
  const LEGACY_FALLBACK_API = FUNCTION_ORIGIN + "/server/horseshowing_sync/";
  const HIDDEN_KEY = "rs-wec-webflow-v2-hidden";
  const SAMPLE_ROWS = [
    { row_key:"sample|grand|804", ring:"Grand", ring_name_prioritized:"Grand", ring_no:640, ring_visual_key:"640|grand", ring_raw_name:"WEC Grand Arena - sample", class_no:26676, class_number:"804", class_name:"C.Jarvis Insurance Welcome Prix 1.40m II.2b", time:"8:00 am", class_start_time:"08:00:00", entry_count:25, n_gone:0, n_to_go:25, current_horse:"", status:"upcoming", horse_items:[], entries:[] },
    { row_key:"sample|indoor_1|781", ring:"Indoor 1", ring_name_prioritized:"Indoor 1", ring_no:644, ring_visual_key:"644|indoor_1", ring_raw_name:"Indoor 1- sample", class_no:26788, class_number:"781", class_name:"1.40m Junior/Amateur Jumper II.1", time:"1:15 pm", class_start_time:"13:15:00", entry_count:11, n_gone:0, n_to_go:11, current_horse:"", status:"upcoming", horse_items:[{ name:"Bee", label:"Bee (3)", order:"3", trainer:"CWF" }, { name:"Insider", label:"Insider (6)", order:"6", trainer:"CWF" }], entries:[] }
  ];

  let allRings = rowsToRings(SAMPLE_ROWS);
  let allRows = flattenRingRows(allRings);
  let visibleRings = [];
  let visibleRows = [];
  let activeRingFilters = new Set();
  let activeHorseFilters = new Set();
  let hiddenRows = readHiddenRows();
  let pendingHiddenRows = new Set();
  let focusHiddenRows = new Set();
  let hideMode = false;
  let focusMode = false;
  let showHiddenMode = false;
  let filterOpen = false;
  let currentMeta = { source: "sample", show_no: "", focus_date: "", last_updated: "" };

  const baseApp = document.querySelector(".rs-app.is-base");
  const showName = baseApp.querySelector(".rs-show-name");
  const focusDay = baseApp.querySelector(".rs-focus-day");
  const topMenu = baseApp.querySelector(".rs-top-menu");
  const toolsPanel = baseApp.querySelector(".rs-app-tools");
  const toolsRail = toolsPanel.querySelector(".rs-action-block");
  const anchorsRail = baseApp.querySelector(".rs-anchor-block");
  const filtersPanel = baseApp.querySelector(".rs-app-filters");
  const filtersRail = filtersPanel.querySelector(".rs-filters");
  const stack = baseApp.querySelector(".rs-app-collections-stack");
  const listContainer = baseApp.querySelector(".rs-app-collection-lists");
  const riderFlyup = document.querySelector(".rs-app.rider-flyup");
  const entryFlyup = document.querySelector(".rs-app.entry-flyup");
  const printSheet = document.createElement("div");
  const statusLine = document.createElement("div");

  printSheet.className = "print-sheet";
  statusLine.className = "rs-v2-status";
  baseApp.appendChild(statusLine);
  document.body.appendChild(printSheet);

  function text(value) { return String(value ?? ""); }
  function escapeHtml(value) { return text(value).replace(/[&<>"']/g, char => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", "\"":"&quot;", "'":"&#39;" }[char])); }
  function numberOrNull(value) { const n = Number(value); return Number.isFinite(n) ? n : null; }
  function readHiddenRows() { try { return new Set(JSON.parse(localStorage.getItem(HIDDEN_KEY) || "[]")); } catch { return new Set(); } }
  function writeHiddenRows() { localStorage.setItem(HIDDEN_KEY, JSON.stringify(Array.from(hiddenRows))); }
  function hiddenUnion(...sets) { const out = new Set(); sets.forEach(set => set && set.forEach(value => out.add(value))); return out; }
  function dataUrl(baseUrl) { const url = new URL(baseUrl); const params = new URLSearchParams(location.search); url.searchParams.set("action", "wec-mobile-live"); url.searchParams.set("show_no", params.get("show_no") || DEFAULT_SHOW_NO); if (params.get("focus_day")) url.searchParams.set("focus_day", params.get("focus_day")); return url.toString(); }
  async function fetchJson(url) { const res = await fetch(url, { cache: "no-store" }); const txt = await res.text(); if (!res.ok) throw new Error("Fetch failed " + res.status + ": " + txt.slice(0, 160)); return JSON.parse(txt); }

  async function refreshRows() {
    setStatus("Loading...", false);
    try {
      allRings = normalizePayload(await fetchJson(dataUrl(PRIMARY_API)), "function");
      allRows = flattenRingRows(allRings);
    } catch (primaryError) {
      try {
        allRings = normalizePayload(await fetchJson(dataUrl(LEGACY_FALLBACK_API)), "legacy-fallback");
        allRows = flattenRingRows(allRings);
      } catch {
        currentMeta = { source: "sample", show_no: "", focus_date: "", last_updated: "" };
        allRings = rowsToRings(SAMPLE_ROWS);
        allRows = flattenRingRows(allRings);
        applyFilters();
        setStatus("Live sources failed; showing sample fallback. " + primaryError.message, true);
        return;
      }
    }
    applyFilters();
  }

  function normalizePayload(payload, source) {
    const payloadRings = Array.isArray(payload) ? payload : payload && payload.rings;
    if (!payload || payload.ok === false || !Array.isArray(payloadRings)) throw new Error(payload && payload.error ? payload.error : "Unexpected WEC payload shape");
    const normalizedRings = [];
    for (const ring of payloadRings) {
      const ringDisplay = text(ring.ring_display || ring.ring_name_normalized || ring.ring_name || "Ring").trim();
      const ringVisualKey = text(ring.ring_visual_key || [ring.ring_no, ring.ring_name_normalized].filter(Boolean).join("|") || ringDisplay).trim();
      const normalizedRing = { key: ringVisualKey, title: ringDisplay, sort: numberOrNull(ring.ring_no) ?? 999999, name: text(ring.ring_name_prioritized || ring.ring_name_normalized || ringDisplay).trim(), ring_no: ring.ring_no, ring_name: text(ring.ring_name).trim(), ring_name_normalized: text(ring.ring_name_normalized).trim(), ring_visual_key: ringVisualKey, ring_display: ringDisplay, classes: [] };
      for (const c of Array.isArray(ring.classes) ? ring.classes : []) {
        const classNumber = text(c.class_number).trim();
        let className = text(c.class_name || c.class_label).trim();
        if (classNumber && className.startsWith(classNumber + " - ")) className = className.slice((classNumber + " - ").length).trim();
        const classRingVisualKey = text(c.ring_visual_key || ringVisualKey).trim();
        const row = { row_key: classRingVisualKey + "|" + text(c.class_no || classNumber || normalizedRing.classes.length), ring_visual_key: classRingVisualKey, ring_name_normalized: text(c.ring_name_normalized || ring.ring_name_normalized).trim(), ring_name_prioritized: text(c.ring_name_prioritized || ring.ring_name_prioritized || ring.ring_name_normalized || ringDisplay).trim(), ring_raw_name: text(ring.ring_name || c.ring_name_prioritized).trim(), ring: ringDisplay, ring_no: ring.ring_no, ring_sort: numberOrNull(ring.ring_no) ?? 999999, show_no: text(c.show_no).trim(), focus_day: text(c.focus_day).trim(), show_day: text(c.show_day).trim(), class_no: c.class_no, class_number: classNumber, class_name: className, class_start_time: text(c.class_start_time).trim(), time: text(c.display_time || c.time_text || c.class_time).trim(), sort_time: numberOrNull(c.time_sort) ?? 999999999, entry_count: numberOrNull(c.entry_count), n_gone: numberOrNull(c.n_gone), n_to_go: numberOrNull(c.n_to_go), elapsed_seconds: numberOrNull(c.elapsed_seconds), current_entry_no: text(c.current_entry_no).trim(), current_horse: text(c.current_horse).trim(), rollup_label: text(c.rollup_label).trim(), horse_items: flattenRollups(c.rollups, c.rollup_label), entries: Array.isArray(c.entries) ? c.entries : [] };
        row.status = computeStatus(row);
        normalizedRing.classes.push(row);
      }
      normalizedRings.push(normalizedRing);
    }
    const firstClass = normalizedRings.flatMap(ring => ring.classes).find(Boolean) || {};
    currentMeta = { source, show_no: text(payload.show_no || firstClass.show_no).trim(), focus_date: text(payload.show_focus_date || payload.focus_day || firstClass.focus_day || firstClass.show_day).trim(), last_updated: text(payload.last_updated).trim() };
    return normalizedRings;
  }

  function rowsToRings(rows) {
    const rings = [], byKey = new Map();
    for (const row of rows) {
      const key = row.ring_visual_key || row.ring || "Unassigned";
      if (!byKey.has(key)) { const ring = { key, title: row.ring || "Unassigned", sort: row.ring_sort, name: row.ring_name_prioritized || row.ring || "", ring_no: row.ring_no, ring_name: row.ring_raw_name || "", ring_name_normalized: row.ring_name_normalized || "", ring_visual_key: key, ring_display: row.ring || "Unassigned", classes: [] }; byKey.set(key, ring); rings.push(ring); }
      byKey.get(key).classes.push(row);
    }
    return rings;
  }
  function flattenRingRows(rings) { return rings.flatMap(ring => Array.isArray(ring.classes) ? ring.classes : []); }
  function flattenRollups(rollups, fallbackLabel) {
    const items = [];
    for (const rollup of Array.isArray(rollups) ? rollups : []) {
      const trainer = text(rollup.trainer_display || rollup.trainer).trim();
      for (const horse of Array.isArray(rollup.horses) ? rollup.horses : []) {
        const label = text(horse.label || horse.display || horse.horse).trim();
        const name = text(horse.display || horse.barn_name || horse.horse || label).trim();
        const order = text(horse.entry_order || horse.order).trim();
        if (label || name) items.push({ label: label || name, name: name || label, order, trainer });
      }
    }
    if (!items.length && fallbackLabel) text(fallbackLabel).split(",").map(x => x.trim()).filter(Boolean).forEach(part => items.push({ label: part, name: part, order: "", trainer: "" }));
    return items;
  }
  function computeStatus(row) { if (row.current_horse || (Number(row.n_gone || 0) > 0 && Number(row.n_to_go || 0) > 0)) return "underway"; if (Number(row.entry_count || 0) > 0 && Number(row.n_to_go) === 0) return "completed"; return "upcoming"; }
  function statusReference(row) { if (row.status === "completed") return { label: "done", text: "done" }; if (row.status === "underway") return { label: "now", text: "now" }; if (row.status === "upcoming") return { label: "soon", text: "soon" }; return { label: "today", text: "today" }; }
  function classLabel(row) { return [row.class_number, row.class_name].filter(Boolean).join(" - "); }
  function rollupLabel(item) { return item.order ? text(item.name || item.label) + " (" + item.order + ")" : text(item.name || item.label); }
  function rollupText(row) { return (Array.isArray(row.horse_items) ? row.horse_items : []).map(rollupLabel).filter(Boolean).join("   "); }
  function compareRows(a, b) { const ringName = text(a.ring_name_prioritized || a.ring || "").localeCompare(text(b.ring_name_prioritized || b.ring || ""), undefined, { numeric: true }); if (ringName) return ringName; const ring = (numberOrNull(a.ring_sort) ?? 999999) - (numberOrNull(b.ring_sort) ?? 999999); if (ring) return ring; const time = (numberOrNull(a.sort_time) ?? 999999999) - (numberOrNull(b.sort_time) ?? 999999999); if (time) return time; return text(a.class_number).localeCompare(text(b.class_number), undefined, { numeric: true }); }
  function horseKey(value) { return text(value).trim().toLowerCase(); }
  function rowHorseKeys(row) { return new Set((Array.isArray(row.horse_items) ? row.horse_items : []).map(item => horseKey(item.name || item.label)).filter(Boolean)); }
  function rowMatchesHorseFilters(row) { if (!activeHorseFilters.size) return true; const keys = rowHorseKeys(row); for (const selected of activeHorseFilters) if (keys.has(selected)) return true; return false; }
  function currentHiddenSet() { return hideMode ? pendingHiddenRows : hiddenRows; }
  function effectiveHiddenRows() { return focusMode ? hiddenUnion(currentHiddenSet(), focusHiddenRows) : currentHiddenSet(); }
  function shouldIncludeHiddenRows() { return hideMode ? true : showHiddenMode; }
  function focusGroupKey(row) { const timeKey = text(row.time || row.class_start_time).trim().toLowerCase() || text(row.sort_time).trim(); return [row.ring_visual_key || row.ring_name_prioritized || row.ring || "", timeKey, text(row.class_name).trim().toLowerCase()].join("|"); }
  function focusKeepValue(row) { return numberOrNull(row.class_number) ?? numberOrNull(row.class_no) ?? 999999999; }
  function recomputeFocusHiddenRows() { const next = new Set(); if (!focusMode) { focusHiddenRows = next; return; } const groups = new Map(); allRows.forEach(row => { const key = focusGroupKey(row); if (!text(row.class_name).trim() || !text(row.time || row.class_start_time).trim()) return; if (!groups.has(key)) groups.set(key, []); groups.get(key).push(row); }); groups.forEach(rows => { if (rows.length < 2) return; rows.slice().sort((a, b) => focusKeepValue(a) - focusKeepValue(b) || compareRows(a, b)).slice(1).forEach(row => next.add(row.row_key)); }); focusHiddenRows = next; }
  function horseFilterOptions() { const options = new Map(), hiddenSet = effectiveHiddenRows(); allRows.forEach(row => { if (hiddenSet.has(row.row_key) && !shouldIncludeHiddenRows()) return; (Array.isArray(row.horse_items) ? row.horse_items : []).forEach(item => { const label = text(item.name || item.label).trim(); const key = horseKey(label); if (key && !options.has(key)) options.set(key, label); }); }); return Array.from(options, ([key, label]) => ({ key, label })).sort((a, b) => a.label.localeCompare(b.label, undefined, { numeric: true })); }
  function metadataStatusPrefix() { const sourceLabel = currentMeta.source === "function" ? "Catalyst function" : currentMeta.source === "legacy-fallback" ? "Legacy fallback" : "Sample fallback"; return [sourceLabel, currentMeta.show_no ? "show " + currentMeta.show_no : "", currentMeta.focus_date || "", currentMeta.last_updated ? "updated " + currentMeta.last_updated : ""].filter(Boolean).join(" · "); }

  function initShell() {
    [...baseApp.querySelectorAll(".rs-app-collections-stack > .rs-app-collection-lists")].slice(1).forEach(el => el.remove());
    document.querySelectorAll(".rs-app.rider-flyup, .rs-app.entry-flyup").forEach(el => { el.classList.remove("is-open"); el.setAttribute("aria-hidden", "true"); });
    topMenu.innerHTML = [
      actionButton("refresh", "REFRESH"),
      actionButton("print", "PRINT"),
      actionButton("focus", "FOCUS"),
      actionButton("filter", "FILTER"),
      actionButton("hide", "HIDE"),
      actionButton("show-hidden", "SHOW HIDDEN")
    ].join("");
    toolsRail.innerHTML = [actionButton("hide-clear", "CLEAR ALL"), actionButton("hide-save", "SAVE")].join("");
    toolsPanel.classList.add("is-hidden");
    filtersPanel.classList.add("is-hidden");
  }
  function actionButton(action, label) { return '<button class="rs-tab-link" type="button" data-action="' + action + '" aria-pressed="false"><div>' + label + '</div></button>'; }

  function applyFilters() {
    recomputeFocusHiddenRows();
    const showHidden = shouldIncludeHiddenRows();
    const hiddenSet = effectiveHiddenRows();
    visibleRings = allRings.filter(ring => !activeRingFilters.size || activeRingFilters.has(ring.key)).map(ring => ({ ...ring, classes: ring.classes.filter(row => (showHidden || !hiddenSet.has(row.row_key)) && rowMatchesHorseFilters(row)).sort(compareRows) })).filter(ring => ring.classes.length);
    visibleRows = flattenRingRows(visibleRings);
    render();
  }
  function render() {
    showName.textContent = currentMeta.show_no ? "WEC Live Schedule " + currentMeta.show_no : "WEC Live Schedule";
    focusDay.textContent = [currentMeta.focus_date, currentMeta.last_updated ? "updated " + currentMeta.last_updated : ""].filter(Boolean).join(" · ") || "Live schedule";
    baseApp.classList.toggle("is-hide", hideMode);
    toolsPanel.classList.toggle("is-hidden", !hideMode);
    filtersPanel.classList.toggle("is-hidden", !filterOpen);
    setPressed("focus", focusMode); setPressed("filter", filterOpen); setPressed("hide", hideMode); setPressed("show-hidden", showHiddenMode);
    renderControls();
    renderSchedule();
    const ringText = activeRingFilters.size ? " · rings " + activeRingFilters.size : "";
    const horseText = activeHorseFilters.size ? " · horses " + activeHorseFilters.size : "";
    const pendingText = hideMode ? " · pending hidden " + pendingHiddenRows.size : "";
    const focusText = focusMode ? " · focus hidden " + focusHiddenRows.size : "";
    setStatus(metadataStatusPrefix() + " · rows " + visibleRows.length + " · hidden " + hiddenRows.size + focusText + ringText + horseText + pendingText, false);
  }
  function setPressed(action, value) { const btn = topMenu.querySelector('[data-action="' + action + '"]'); if (btn) btn.setAttribute("aria-pressed", String(value)); }
  function renderControls() {
    anchorsRail.innerHTML = allRings.length ? allRings.map(ring => '<button class="rs-anchor-link" type="button" aria-pressed="' + String(activeRingFilters.has(ring.key)) + '" data-ring-key="' + escapeHtml(ring.key) + '"><div class="rs-ring-name-normalized' + (activeRingFilters.has(ring.key) ? " is-active" : "") + '">' + escapeHtml(ring.title) + '</div></button>').join("") : "";
    const horses = horseFilterOptions();
    filtersRail.innerHTML = '<button class="rs-tab-link" type="button" data-horse-clear><div>CLEAR</div></button>' + horses.map(horse => '<button class="rs-tab-link" type="button" aria-pressed="' + String(activeHorseFilters.has(horse.key)) + '" data-horse-key="' + escapeHtml(horse.key) + '"><div>' + escapeHtml(horse.label) + '</div></button>').join("");
  }
  function renderSchedule() {
    listContainer.innerHTML = visibleRings.length ? visibleRings.map(renderRing).join("") : '<div class="rs-app-collection-list"><div class="rs-empty">No rows available.</div></div>';
  }
  function renderRing(ring) {
    return '<div class="rs-app-collection-list" data-ring-key="' + escapeHtml(ring.key) + '"><div class="rs-app-collection-list-head"><div class="rs-app-list-head-grid"><div class="rs-app-list-head-grid-1"><div class="rs-ring-name-normalized-head">' + escapeHtml(ring.title.toUpperCase()) + '</div></div><div class="rs-app-list-head-grid-2"><div class="rs-ring-status-token ring-status-ontime">on time</div></div></div></div><div class="rs-app-collection-list-items">' + ring.classes.map(renderClassRow).join("") + '</div></div>';
  }
  function renderClassRow(row) {
    const hiddenSet = effectiveHiddenRows();
    const hasRollup = Array.isArray(row.horse_items) && row.horse_items.length;
    const classes = ["rs-app-collection-list-item", hasRollup ? "is-rollups" : "", hideMode && hiddenSet.has(row.row_key) ? "is-pending-hidden" : "", !hideMode && showHiddenMode && hiddenSet.has(row.row_key) ? "is-hidden-row" : ""].filter(Boolean).join(" ");
    const status = statusReference(row);
    return '<div class="' + classes + '" data-row-key="' + escapeHtml(row.row_key) + '"><div class="rs-app-litem-stack is-rollup-line' + (hasRollup ? "" : " is-empty") + '"><div class="rs-app-item-rollups' + (hasRollup ? "" : " is-hidden") + '">' + (hasRollup ? row.horse_items.map((item, i) => '<button class="rs-rollup" type="button" data-rollup-row="' + escapeHtml(row.row_key) + '" data-rollup-index="' + i + '"><div class="rs-barn-name">' + escapeHtml(text(item.name || item.label).toUpperCase()) + '</div><div class="rs-class-oog">' + escapeHtml(item.order ? "{" + item.order + "}" : "") + '</div></button>').join("") : "") + '</div></div><div class="rs-app-litem-stack is-class-line"><div class="rs-app-item-grid" role="button" tabindex="0" data-class-row="' + escapeHtml(row.row_key) + '"><div class="rs-app-item-grid-1"><div class="rs-app-item-slot-1">' + (hideMode ? '<input class="rs-hide-check" type="checkbox" data-hide-row="' + escapeHtml(row.row_key) + '"' + (hiddenSet.has(row.row_key) ? " checked" : "") + '>' : "") + '<div class="rs-class-start-time">' + escapeHtml(row.time || "--") + '</div></div><div class="rs-app-item-slot-2"><div class="rs-ring-name-normalized">' + escapeHtml(row.ring || "") + '</div></div></div><div class="rs-class-dense"><div class="rs-app-item-slot-2-1"><div class="rs-class-number">' + escapeHtml(row.class_number || "") + '</div></div><div class="rs-app-item-slot-2-2"><div class="rs-class-class-name">' + escapeHtml(row.class_name || classLabel(row)) + '</div></div><div class="rs-app-item-slot-2-3"><div class="rs-tag-1">' + escapeHtml(row.entry_count ?? "") + '</div><div class="rs-tag-2">US</div><div class="rs-tag-3">C</div></div></div><div class="rs-ring-stauts-class"><div class="rs-ring-status-now">' + escapeHtml(status.text) + '</div></div></div></div></div>';
  }
  function setStatus(message, isError) { statusLine.textContent = message; statusLine.classList.toggle("is-error", !!isError); }
  function findRow(rowKey) { return allRows.find(row => row.row_key === rowKey); }
  function toggleRingFilter(key) { activeRingFilters.has(key) ? activeRingFilters.delete(key) : activeRingFilters.add(key); applyFilters(); }
  function toggleHorseFilter(key) { activeHorseFilters.has(key) ? activeHorseFilters.delete(key) : activeHorseFilters.add(key); applyFilters(); }
  function setHideMode(open) { hideMode = open; pendingHiddenRows = open ? new Set(effectiveHiddenRows()) : new Set(); applyFilters(); }
  function saveHiddenRows() { hiddenRows = new Set(pendingHiddenRows); writeHiddenRows(); setHideMode(false); }
  function togglePendingHidden(rowKey) { if (!hideMode || !rowKey) return; pendingHiddenRows.has(rowKey) ? pendingHiddenRows.delete(rowKey) : pendingHiddenRows.add(rowKey); applyFilters(); }
  function openFlyout(row, rollupIndex = null) {
    if (!row) return;
    const item = rollupIndex == null ? null : row.horse_items[rollupIndex];
    const fly = item ? entryFlyup : riderFlyup;
    fly.querySelector(".rs-show-name").textContent = item ? rollupLabel(item) : classLabel(row);
    fly.querySelector(".rs-focus-day").textContent = item ? [row.ring, row.time, classLabel(row)].filter(Boolean).join(" · ") : [row.ring, row.time, statusReference(row).text].filter(Boolean).join(" · ");
    const lists = fly.querySelector(".rs-app-collection-lists");
    lists.innerHTML = item ? buildRollupFlyout(row, item) : buildClassFlyout(row);
    fly.classList.add("is-open");
    fly.setAttribute("aria-hidden", "false");
  }
  function closeFlyups() { [riderFlyup, entryFlyup].forEach(fly => { fly.classList.remove("is-open"); fly.setAttribute("aria-hidden", "true"); }); }
  function section(title, rows) { return '<div class="rs-app-collection-list"><div class="rs-app-collection-list-head"><div class="rs-app-list-head-grid"><div class="rs-app-list-head-grid-1"><div class="rs-ring-name-normalized-head">' + escapeHtml(title.toUpperCase()) + '</div></div></div></div><div class="rs-app-collection-list-items">' + rows + '</div></div>'; }
  function line(a, b, c, d, e) { return '<div class="rs-app-collection-list-item"><div class="rs-app-litem-stack is-rollup-line is-empty"></div><div class="rs-app-litem-stack is-class-line"><div class="rs-app-item-grid"><div class="rs-app-item-grid-1"><div class="rs-app-item-slot-1"><div class="rs-class-start-time">' + escapeHtml(a || "") + '</div></div><div class="rs-app-item-slot-2"><div class="rs-ring-name-normalized">' + escapeHtml(b || "") + '</div></div></div><div class="rs-class-dense"><div class="rs-app-item-slot-2-1"><div class="rs-class-number">' + escapeHtml(d || "") + '</div></div><div class="rs-app-item-slot-2-2"><div class="rs-class-class-name">' + escapeHtml(c || "") + '</div></div><div class="rs-app-item-slot-2-3"></div></div><div class="rs-ring-stauts-class"><div class="rs-ring-status-now">' + escapeHtml(e || "") + '</div></div></div></div></div>'; }
  function buildClassFlyout(row) { const entries = Array.isArray(row.entries) && row.entries.length ? row.entries.slice(0, 30).map(entry => line(entry.entry_order || entry.entry_no || "", row.ring, entry.horse || entry.barn_name || entry.display || JSON.stringify(entry).slice(0, 80), entry.result || "", entry.status || "")).join("") : '<div class="rs-empty">No rows available.</div>'; return section("Class", line(row.time, row.ring, classLabel(row), row.entry_count ?? "", statusReference(row).text)) + section("Now", line("live", row.ring, (row.n_gone ?? 0) + " gone · " + (row.n_to_go ?? 0) + " to go", row.entry_count ?? "", statusReference(row).text) + line("now", row.ring, row.current_horse || "No current horse", row.current_entry_no || "", statusReference(row).text)) + section("Entries", entries); }
  function buildRollupFlyout(row, item) { const related = allRows.filter(other => other.row_key !== row.row_key && rowHorseKeys(other).has(horseKey(item.name || item.label))).slice(0, 20); return section("Now", line(row.time, row.ring, rollupLabel(item), item.order || "", statusReference(row).text)) + section("Classes", related.length ? related.map(other => line(other.time, other.ring, classLabel(other), other.entry_count ?? "", statusReference(other).text)).join("") : '<div class="rs-empty">No related classes.</div>'); }
  function buildPrintSheet() { const generatedAt = new Date().toISOString(); const body = visibleRings.map(ring => '<section class="print-ring-group"><div class="print-ring">' + escapeHtml(ring.title) + "</div>" + ring.classes.map((row, index) => '<div class="print-row' + (index % 2 ? " is-zebra" : "") + (row.horse_items && row.horse_items.length ? " has-rollup" : "") + '">' + (rollupText(row) ? '<div class="print-rollup">' + escapeHtml(rollupText(row)) + '</div>' : "") + '<span class="print-cell time">' + escapeHtml(row.time || "--") + '</span><span class="print-cell class">' + escapeHtml(classLabel(row)) + "</span></div>").join("") + "</section>").join(""); printSheet.innerHTML = '<div class="print-title"><h1>WEC Print Review</h1><p>' + escapeHtml(metadataStatusPrefix()) + "<br>" + escapeHtml(generatedAt) + '</p></div><div class="print-columns">' + body + "</div>"; }
  function printGrid() { buildPrintSheet(); setTimeout(() => window.print(), 50); }

  topMenu.addEventListener("click", event => { const action = event.target.closest("[data-action]")?.getAttribute("data-action"); if (!action) return; if (action === "refresh") refreshRows(); if (action === "print") printGrid(); if (action === "focus") { focusMode = !focusMode; applyFilters(); } if (action === "filter") { filterOpen = !filterOpen; applyFilters(); } if (action === "hide") setHideMode(!hideMode); if (action === "show-hidden") { showHiddenMode = !showHiddenMode; applyFilters(); } });
  toolsRail.addEventListener("click", event => { const action = event.target.closest("[data-action]")?.getAttribute("data-action"); if (action === "hide-clear") { pendingHiddenRows = new Set(); applyFilters(); } if (action === "hide-save") saveHiddenRows(); });
  anchorsRail.addEventListener("click", event => { const key = event.target.closest("[data-ring-key]")?.getAttribute("data-ring-key"); if (key) toggleRingFilter(key); });
  filtersRail.addEventListener("click", event => { if (event.target.closest("[data-horse-clear]")) { activeHorseFilters = new Set(); applyFilters(); return; } const key = event.target.closest("[data-horse-key]")?.getAttribute("data-horse-key"); if (key) toggleHorseFilter(key); });
  listContainer.addEventListener("click", event => { const rollup = event.target.closest("[data-rollup-row]"); if (rollup) { openFlyout(findRow(rollup.getAttribute("data-rollup-row")), Number(rollup.getAttribute("data-rollup-index"))); return; } const check = event.target.closest("[data-hide-row]"); if (check) { togglePendingHidden(check.getAttribute("data-hide-row")); return; } const classLine = event.target.closest("[data-class-row]"); if (!classLine) return; const row = findRow(classLine.getAttribute("data-class-row")); if (hideMode) togglePendingHidden(row && row.row_key); else openFlyout(row); });
  document.addEventListener("keydown", event => { if (event.key === "Escape") closeFlyups(); });
  [riderFlyup, entryFlyup].forEach(fly => fly.addEventListener("click", event => { if (event.target === fly) closeFlyups(); }));

  initShell();
  refreshRows();
})();
