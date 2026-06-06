(function () {
  const root = document.getElementById("packing-app");
  if (!root) return;

  const globalConfig = window.WEC_PACKING_HYBRID_CONFIG || {};
  const config = {
    apiUrl: root.dataset.apiUrl || globalConfig.apiUrl || "https://ringstatus.com/test/wec-packing/horse-kits",
    homeUrl: root.dataset.homeUrl || globalConfig.homeUrl || "",
    quantityUrl: root.dataset.quantityUrl || globalConfig.quantityUrl || "",
    perHorseUrl: root.dataset.perHorseUrl || globalConfig.perHorseUrl || "",
    perGroomUrl: root.dataset.perGroomUrl || globalConfig.perGroomUrl || "",
    horsesUrl: root.dataset.horsesUrl || globalConfig.horsesUrl || "",
    sessionUrl: root.dataset.sessionUrl || globalConfig.sessionUrl || "",
    sessionEnabled: root.dataset.sessionEnabled === "false" || globalConfig.sessionEnabled === false ? false : true,
    pollMs: number(root.dataset.pollMs || globalConfig.pollMs || 30000),
    sessionIdleMs: number(root.dataset.sessionIdleMs || globalConfig.sessionIdleMs || 600000),
    printUrl: root.dataset.printUrl || globalConfig.printUrl || "https://ringstatus.com/test/wec-packing/horse-kits/print",
    packWaveKey: root.dataset.packWaveKey || globalConfig.packWaveKey || "wave_one"
  };

  const ui = {
    loading: true,
    error: "",
    activeModule: "home",
    viewKey: config.packWaveKey,
    laneKey: "open",
    search: "",
    sortKey: "horse",
    sortDir: "asc",
    activePrimaryTab: "home",
    navTrayKey: "",
    selectedHorseId: "",
    drawerOpen: false,
    entityAddOpen: false,
    entityDraft: {},
    entitySaving: false,
    entityError: "",
    itemSearch: "",
    itemFilter: "all",
    savingKey: ""
  };

  let state = null;
  let homeState = null;
  let planState = null;
  let horseEntityState = null;
  let rows = [];
  let silentRefreshInFlight = false;
  const optimistic = new Map();
  const session = {
    key: sessionKey(),
    deviceId: deviceId(),
    pingInFlight: false,
    pingAgain: false,
    lastInteractionAt: Date.now(),
    pollTimer: 0
  };
  root.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    if (target.tagName === "A") event.preventDefault();

    const action = target.dataset.action;
    queueSessionPing("click", { action });
    if (action === "set-view") {
      ui.viewKey = target.dataset.viewKey || config.packWaveKey;
      ui.drawerOpen = false;
      if (moduleBelongsToTab(ui.activeModule, "horses")) {
        await ensureHorseEntityState();
        render();
        return;
      }
      await load();
      return;
    }
    if (action === "set-primary-tab") {
      const tabKey = target.dataset.tabKey || "home";
      if (["horses", "counts"].includes(tabKey)) {
        ui.navTrayKey = ui.navTrayKey === tabKey ? "" : tabKey;
        ui.activePrimaryTab = tabKey;
        const defaultModule = defaultModuleForTab(tabKey);
        if (!moduleBelongsToTab(ui.activeModule, tabKey) && defaultModule) {
          if (["horse_kits", "quantity", "per_horse", "per_groom"].includes(defaultModule)) {
            await setModule(defaultModule);
          } else {
            ui.activeModule = defaultModule;
            ui.drawerOpen = false;
            applyActiveModuleFromHome();
            render();
            if (moduleBelongsToTab(defaultModule, "horses")) {
              await ensureHorseEntityState();
              render();
            }
          }
          return;
        }
        render();
        return;
      }
      ui.navTrayKey = "";
      ui.activePrimaryTab = tabKey;
      if (tabKey === "home") await setModule("home");
      else {
        ui.activeModule = tabKey;
        ui.drawerOpen = false;
        if (tabKey === "comments") await ensureHorseEntityState();
        render();
      }
      return;
    }
    if (action === "set-nav-child") {
      const moduleKey = target.dataset.moduleKey || "";
      ui.navTrayKey = "";
      if (["horse_kits", "quantity", "per_horse", "per_groom"].includes(moduleKey)) {
        await setModule(moduleKey);
        ui.navTrayKey = "";
        render();
      } else {
        ui.activeModule = moduleKey;
        ui.drawerOpen = false;
        applyActiveModuleFromHome();
        render();
        if (moduleBelongsToTab(moduleKey, "horses")) {
          await ensureHorseEntityState();
          render();
        }
      }
      return;
    }
    if (action === "set-module") {
      await setModule(target.dataset.moduleKey || "home");
      return;
    }
    if (action === "open-plan-item") {
      ui.selectedHorseId = target.dataset.itemId || "";
      ui.drawerOpen = false;
      render();
      return;
    }
    if (action === "set-lane") {
      ui.laneKey = target.dataset.laneKey || "open";
      rebuild();
      return;
    }
    if (action === "set-sort") {
      setSort(target.dataset.sortKey || "horse");
      return;
    }
    if (action === "open-horse") {
      ui.selectedHorseId = target.dataset.horseId || "";
      ui.drawerOpen = true;
      render();
      return;
    }
    if (action === "open-entity-horse") {
      ui.selectedHorseId = target.dataset.horseId || "";
      await ensureHorseEntityState();
      ui.entityAddOpen = false;
      ui.entityDraft = entityDraftFromHorse(selectedEntityHorse());
      ui.entityError = "";
      ui.drawerOpen = true;
      render();
      return;
    }
    if (action === "open-add-horse") {
      ui.selectedHorseId = "";
      await ensureHorseEntityState();
      ui.entityAddOpen = true;
      ui.entityDraft = entityDraftFromHorse(null);
      ui.entityError = "";
      ui.drawerOpen = true;
      render();
      return;
    }
    if (action === "close-drawer") {
      ui.drawerOpen = false;
      ui.entityAddOpen = false;
      ui.entityError = "";
      render();
      return;
    }
    if (action === "clear-search") {
      ui.search = "";
      rebuild();
      return;
    }
    if (action === "clear-item-search") {
      ui.itemSearch = "";
      render();
      return;
    }
    if (action === "set-item-filter") {
      ui.itemFilter = target.dataset.itemFilter || "all";
      render();
      return;
    }
    if (action === "set-item-state") {
      await setItemState(target);
      return;
    }
    if (action === "set-entity-status") {
      const active = target.dataset.status !== "inactive";
      ui.entityDraft.active = active;
      ui.entityDraft.inactive = !active;
      render();
      return;
    }
    if (action === "set-entity-wave") {
      const wave = target.dataset.wave || "";
      ui.entityDraft.wec_wave_1 = wave === "wave_one";
      ui.entityDraft.wec_wave_2 = wave === "wave_two";
      ui.entityDraft.wec_not_going = wave === "not_going";
      render();
      return;
    }
    if (action === "save-entity-horse") {
      await saveEntityHorse();
      return;
    }
    if (action === "print") {
      window.open(printUrl(), "_blank", "noopener");
    }
  });

  root.addEventListener("input", (event) => {
    const input = event.target;
    if (input.matches("[data-search]")) {
      queueSessionPing("input", { action: "search" });
      ui.search = input.value || "";
      rebuild();
      focusSearch("[data-search]", ui.search.length);
    }
    if (input.matches("[data-item-search]")) {
      queueSessionPing("input", { action: "item_search" });
      ui.itemSearch = input.value || "";
      render();
      focusSearch("[data-item-search]", ui.itemSearch.length);
    }
    if (input.matches("[data-entity-field]")) {
      const field = input.dataset.entityField || "";
      if (!field) return;
      ui.entityDraft[field] = input.value || "";
    }
  });

  load();
  if (config.sessionEnabled) {
    queueSessionPing("session_start");
    startSessionPolling();
  }

  async function load() {
    ui.loading = true;
    ui.error = "";
    render();
    try {
      await loadHome();
      applyActiveModuleFromHome();
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.loading = false;
      render();
    }
  }

  async function setModule(moduleKey) {
    ui.activeModule = moduleKey;
    ui.activePrimaryTab = moduleKey === "home" ? "home" : ["horse_kits", "quantity", "per_horse", "per_groom"].includes(moduleKey) ? "counts" : ui.activePrimaryTab;
    ui.navTrayKey = ui.activePrimaryTab === "home" ? "" : ui.activePrimaryTab;
    ui.drawerOpen = false;
    ui.error = "";
    if (!homeState) {
      await load();
      return;
    }
    if (moduleBelongsToTab(moduleKey, "horses")) {
      await ensureHorseEntityState();
    }
    applyActiveModuleFromHome();
    render();
  }

  async function loadHome() {
    const nextHome = await fetchJson(homeUrl());
    if (!nextHome?.ok || !nextHome.reports) throw new Error("invalid_hybrid_home_state");
    homeState = nextHome;
  }

  async function ensureHorseEntityState() {
    if (horseEntityState?.ok) return;
    await loadHorseEntity();
  }

  async function loadHorseEntity() {
    const next = await fetchJson(horsesUrl());
    if (!next?.ok || !Array.isArray(next.horses)) throw new Error("invalid_horse_entity_state");
    horseEntityState = next;
  }

  async function silentRefresh() {
    if (silentRefreshInFlight || ui.savingKey) return;
    silentRefreshInFlight = true;
    try {
      await loadHome();
      applyActiveModuleFromHome();
      render();
    } catch (error) {
      console.warn("[wec-packing] silent refresh failed", error);
    } finally {
      silentRefreshInFlight = false;
    }
  }

  function applyActiveModuleFromHome() {
    if (ui.activeModule === "home") {
      state = homeState?.reports?.horse_kits || null;
      planState = null;
      rows = [];
      return;
    }
    if (ui.activeModule === "horse_kits") {
      const next = homeState?.reports?.horse_kits;
      assertState(next);
      state = next;
      planState = null;
      rebuild(false);
      return;
    }
    const key = normalizeModuleKey(ui.activeModule);
    const nextPlan = homeState?.reports?.[key];
    if (!nextPlan?.ok || !Array.isArray(nextPlan.items)) {
      state = homeState?.reports?.horse_kits || null;
      planState = null;
      rows = [];
      return;
    }
    planState = nextPlan;
    state = homeState?.reports?.horse_kits || null;
    rows = [];
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || `${response.status}`);
    return data;
  }

  function apiUrl() {
    const url = new URL(config.apiUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", ui.viewKey || config.packWaveKey);
    url.searchParams.set("v", "hybrid");
    return url.toString();
  }

  function homeUrl() {
    if (config.homeUrl) return withWaveParams(config.homeUrl);
    return withWaveParams(config.apiUrl.replace(/\/horse-kits\/?$/, "/home"));
  }

  function sessionUrl() {
    if (config.sessionUrl) return withWaveParams(config.sessionUrl);
    return withWaveParams(config.apiUrl.replace(/\/horse-kits\/?$/, "/session"));
  }

  function horsesUrl() {
    if (config.horsesUrl) return withWaveParams(config.horsesUrl);
    return withWaveParams(config.apiUrl.replace(/\/horse-kits\/?$/, "/horses"));
  }

  function planUrl(planKey) {
    const key = normalizeModuleKey(planKey);
    const configured = key === "quantity" ? config.quantityUrl : key === "per_horse" ? config.perHorseUrl : key === "per_groom" ? config.perGroomUrl : "";
    if (configured) return withWaveParams(configured);
    const route = key === "per_horse" ? "per-horse" : key === "per_groom" ? "per-groom" : "quantity";
    return withWaveParams(config.apiUrl.replace(/\/horse-kits\/?$/, `/${route}`));
  }

  function withWaveParams(urlValue) {
    const url = new URL(urlValue, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", ui.viewKey || config.packWaveKey);
    url.searchParams.set("v", "hybrid");
    return url.toString();
  }

  function printUrl() {
    const url = new URL(config.printUrl, window.location.href);
    url.searchParams.set("packWaveKey", config.packWaveKey);
    url.searchParams.set("viewKey", ui.viewKey || config.packWaveKey);
    return url.toString();
  }

  function startSessionPolling() {
    const interval = Math.max(10000, config.pollMs || 30000);
    if (session.pollTimer) window.clearInterval(session.pollTimer);
    session.pollTimer = window.setInterval(() => {
      if (document.visibilityState === "hidden") return;
      if (Date.now() - session.lastInteractionAt > (config.sessionIdleMs || 600000)) return;
      queueSessionPing("poll");
      silentRefresh();
    }, interval);
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") {
        queueSessionPing("visible");
        silentRefresh();
      }
    });
  }

  function queueSessionPing(reason, extra = {}) {
    if (!config.sessionEnabled) return;
    session.lastInteractionAt = Date.now();
    if (session.pingInFlight) {
      session.pingAgain = true;
      return;
    }
    sendSessionPing(reason, extra);
  }

  async function sendSessionPing(reason, extra = {}) {
    session.pingInFlight = true;
    session.pingAgain = false;
    try {
      await fetchJson(sessionUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "session_ping",
          sessionKey: session.key,
          deviceId: session.deviceId,
          packWaveKey: config.packWaveKey,
          viewKey: ui.viewKey || config.packWaveKey,
          currentLane: ui.laneKey || "",
          currentList: ui.activeModule || "",
          currentFilter: ui.itemFilter || "",
          reason,
          notes: `module:${ui.activeModule || ""};tab:${ui.activePrimaryTab || ""};tray:${ui.navTrayKey || ""}`,
          ...extra
        })
      });
    } catch (error) {
      console.warn("[wec-packing] session ping failed", error);
    } finally {
      session.pingInFlight = false;
      if (session.pingAgain) {
        sendSessionPing("queued");
      }
    }
  }

  function rebuild(shouldRender = true) {
    rows = sortRows(filteredHorses().map(recordForHorse).filter(matchesLane));
    if (ui.drawerOpen && !rows.some((row) => row.id === ui.selectedHorseId)) ui.drawerOpen = false;
    if (shouldRender) render();
  }

  function filteredHorses() {
    const horses = state?.horses || [];
    const query = ui.search.trim().toLowerCase();
    if (!query) return horses;
    return horses.filter((horse) => [
      horse.barnName,
      horse.name,
      horse.showName
    ].join(" ").toLowerCase().includes(query));
  }

  function recordForHorse(horse) {
    const items = assignedItems(horse);
    const kit = assignedKit(items);
    const counts = countsFor(horse, kit, items);
    return { id: horse.id, horse, kit, items, counts };
  }

  function assignedItems(horse) {
    const itemById = new Map((state?.kitItems || []).map((item) => [item.id, item]));
    return unique(horse?.pakKitItemIds || [])
      .map((id) => itemById.get(id))
      .filter((item) => item && item.active !== false && item.status !== "inactive")
      .sort(compareItem);
  }

  function assignedKit(items) {
    if (!items.length) return null;
    const itemIds = new Set(items.map((item) => item.id));
    return (state?.kits || [])
      .filter((kit) => kit.active !== false && kit.status !== "inactive")
      .find((kit) => (kit.kitItemIds || []).some((id) => itemIds.has(id))) || null;
  }

  function countsFor(horse, kit, items) {
    if (!kit || !items.length) return { need: 0, packed: 0, notNeeded: 0, left: 0 };
    let packed = 0;
    let notNeeded = 0;
    items.forEach((item) => {
      const value = itemState(horse.id, kit.id, item.id);
      if (value === "packed") packed += 1;
      if (value === "not_needed") notNeeded += 1;
    });
    const need = Math.max(0, items.length - notNeeded);
    return { need, packed, notNeeded, left: Math.max(0, need - packed) };
  }

  function itemState(horseId, kitId, itemId) {
    const key = stateKey(horseId, kitId, itemId);
    if (optimistic.has(key)) return optimistic.get(key);
    const row = packingRow(horseId, kitId, itemId);
    if (!row) return "not_packed";
    if (row.packState === "not_needed" || row.neededState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function packingRow(horseId, kitId, itemId) {
    return packingRowFrom(state, horseId, kitId, itemId);
  }

  function packingRowFrom(source, horseId, kitId, itemId) {
    return (source?.packingRows || []).find((row) =>
      includes(row.horseIds, horseId) && includes(row.kitIds, kitId) && includes(row.kitItemIds, itemId)
    );
  }

  function serverItemState(source, horseId, kitId, itemId) {
    const row = packingRowFrom(source, horseId, kitId, itemId);
    if (!row) return "not_packed";
    if (row.packState === "not_needed" || row.neededState === "not_needed") return "not_needed";
    if (row.packState === "packed") return "packed";
    return "not_packed";
  }

  function refreshAfterPropagation(key, horseId, kitId, itemId, expectedState) {
    window.setTimeout(async () => {
      if (optimistic.get(key) !== expectedState) return;
      try {
        const next = await fetchJson(apiUrl());
        assertState(next);
        state = next;
        if (serverItemState(next, horseId, kitId, itemId) === expectedState) optimistic.delete(key);
        rebuild(false);
        render();
      } catch (error) {
        ui.error = error.message || String(error);
        render();
      }
    }, 2500);
  }

  function matchesLane(record) {
    if (ui.laneKey === "need" || ui.laneKey === "open" || ui.laneKey === "left") return record.counts.left > 0;
    if (ui.laneKey === "packed") return record.counts.packed > 0;
    if (ui.laneKey === "all") return true;
    return true;
  }

  function sortRows(list) {
    const dir = ui.sortDir === "desc" ? -1 : 1;
    return [...list].sort((a, b) => {
      if (ui.sortKey === "kit") return compareText(kitLabel(a.kit), kitLabel(b.kit)) * dir;
      if (ui.sortKey === "need") return compareNumber(a.counts.need, b.counts.need) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (ui.sortKey === "packed") return compareNumber(a.counts.packed, b.counts.packed) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      if (ui.sortKey === "left") return compareNumber(a.counts.left, b.counts.left) * dir || compareText(horseLabel(a.horse), horseLabel(b.horse));
      return compareText(horseLabel(a.horse), horseLabel(b.horse)) * dir;
    });
  }

  function setSort(key) {
    if (ui.sortKey === key) ui.sortDir = ui.sortDir === "asc" ? "desc" : "asc";
    else {
      ui.sortKey = key;
      ui.sortDir = "asc";
    }
    rebuild();
  }

  async function setItemState(button) {
    const record = selectedRecord();
    if (!record?.horse?.id || !record?.kit?.id) return;
    const itemId = button.dataset.kitItemId || "";
    const nextState = button.dataset.packState || "";
    if (!itemId || !nextState || !state?.source?.packWaveId) return;

    const key = stateKey(record.horse.id, record.kit.id, itemId);
    const previous = itemState(record.horse.id, record.kit.id, itemId);
    if (previous === nextState) return;

    optimistic.set(key, nextState);
    ui.savingKey = key;
    rebuild(false);
    ui.drawerOpen = true;
    render();

    try {
      const existing = packingRow(record.horse.id, record.kit.id, itemId);
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "set_packing_kit_state",
          packingKitId: existing?.id || "",
          horseId: record.horse.id,
          kitId: record.kit.id,
          kitItemId: itemId,
          packWaveId: state.source.packWaveId,
          packState: nextState,
          sessionKey: session.key,
          deviceId: session.deviceId,
          currentLane: ui.laneKey || "",
          currentList: ui.activeModule || "",
          currentFilter: ui.itemFilter || ""
        })
      });
      if (result.state) {
        assertState(result.state);
        state = result.state;
        if (serverItemState(result.state, record.horse.id, record.kit.id, itemId) === nextState) {
          optimistic.delete(key);
        } else {
          refreshAfterPropagation(key, record.horse.id, record.kit.id, itemId, nextState);
        }
      } else {
        refreshAfterPropagation(key, record.horse.id, record.kit.id, itemId, nextState);
      }
    } catch (error) {
      optimistic.set(key, previous);
      ui.error = error.message || String(error);
    } finally {
      ui.savingKey = "";
      rebuild(false);
      ui.drawerOpen = true;
      render();
    }
  }

  async function saveEntityHorse() {
    const fields = entityChangedFields();
    if (!Object.keys(fields).length) {
      ui.entityError = "No changes to save.";
      render();
      return;
    }
    ui.entitySaving = true;
    ui.entityError = "";
    render();
    try {
      const payload = ui.entityAddOpen
        ? { action: "add_horse", fields, sessionId: session.key, deviceId: session.deviceId }
        : { action: "edit_horse", horseId: ui.selectedHorseId, fields, sessionId: session.key, deviceId: session.deviceId };
      const result = await fetchJson(horsesUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      horseEntityState = result.state || horseEntityState;
      ui.entityAddOpen = false;
      ui.selectedHorseId = result.result?.created?.id || result.result?.updated?.id || ui.selectedHorseId;
      ui.entityDraft = entityDraftFromHorse(selectedEntityHorse());
      ui.drawerOpen = !!ui.selectedHorseId;
      render();
    } catch (error) {
      ui.entityError = error.message || String(error);
      render();
    } finally {
      ui.entitySaving = false;
      render();
    }
  }

  function render() {
    root.className = "rsa-dashboard";
    root.innerHTML = `
      <div class="rs-shell">
        ${headerHtml()}
        ${navHtml()}
        <div class="rs-airtable-shell">
          ${primaryPanelHtml()}
        </div>
      </div>
      ${drawerHtml()}
    `;
  }

  function headerHtml() {
    const title = state?.wave?.reportTitle || "WEC PACK";
    const subtitle = state?.wave?.reportSubtitle || state?.wave?.label || "";
    return `
      <header class="rs-app-header">
        <div class="rs-header-copy">
          <h1>${escapeHtml(title)}</h1>
          <p>${escapeHtml(subtitle)}</p>
        </div>
      </header>
    `;
  }

  function navHtml() {
    const tabs = [
      { key: "home", label: "HOME" },
      { key: "horses", label: "HORSES" },
      { key: "counts", label: "COUNTS" },
      { key: "comments", label: "COMMENTS" }
    ];
    const trays = {
      horses: [
        { key: "horse_roster", label: "ROSTERS" },
        { key: "horse_profiles", label: "PROFILE" },
        { key: "horse_attributes", label: "ATTRIBUTES" }
      ],
      counts: [
        { key: "horse_kits", label: "HORSE KITS" },
        { key: "quantity", label: "QUANTITY COUNTS" },
        { key: "per_horse", label: "PER-HORSE ITEMS" },
        { key: "per_groom", label: "GROOM SUPPLIES" }
      ]
    };
    const trayRows = trays[ui.navTrayKey] || [];
    return `<section class="rs-stack-section is-primary-tabs"><nav class="rs-stack-tabs is-nav-top">${tabs.map((tab) => {
      const key = tab.key || "";
      return `<button class="rs-stack-pill ${ui.activePrimaryTab === key ? "is-active" : ""}" type="button" data-action="set-primary-tab" data-tab-key="${escapeAttr(key)}">${escapeHtml(tab.label || key)}</button>`;
    }).join("")}</nav>${trayRows.length ? `<nav class="rs-stack-tabs is-compact is-nav-tray">${trayRows.map((row) => `<button class="rs-stack-pill ${ui.activeModule === row.key ? "is-active" : ""}" type="button" data-action="set-nav-child" data-module-key="${escapeAttr(row.key)}">${escapeHtml(row.label)}</button>`).join("")}</nav>` : ""}</section>`;
  }

  function primaryPanelHtml() {
    if (ui.loading && !homeState) return `<div class="rs-page-stack"><section class="rs-stack-section"><div class="rs-airtable-empty">Loading WEC packing...</div></section></div>`;
    if (ui.error) return `<div class="rs-page-stack"><section class="rs-stack-section"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></section></div>`;
    if (ui.activeModule === "home") return homePanelHtml();
    if (ui.activeModule === "horse_kits") {
      return `<div class="rs-page-stack">
        ${summaryAggsHtml()}
        ${countAggsHtml()}
        ${searchHtml()}
        ${tableHtml()}
        ${commentsHtml()}
        ${laneTabsHtml()}
      </div>`;
    }
    if (["quantity", "per_horse", "per_groom"].includes(normalizeModuleKey(ui.activeModule))) return planPanelHtml();
    if (ui.activeModule === "horse_roster") return horseRosterPanelHtml();
    if (ui.activeModule === "horse_profiles") return horseProfilesPanelHtml();
    if (ui.activeModule === "horse_attributes") return horseAttributesPanelHtml();
    if (ui.activeModule === "comments") return commentsPanelHtml();
    const tab = (state?.primaryTabs || []).find((row) => row.key === ui.activePrimaryTab);
    const label = tab?.label || ui.activePrimaryTab || "Section";
    return `<div class="rs-page-stack">
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">${escapeHtml(label)}</div></div>
        <div class="rs-airtable-empty">No connected hybrid module for ${escapeHtml(label)}.</div>
      </section>
    </div>`;
  }

  function homePanelHtml() {
    const modules = homeState?.modules || [];
    const totals = modules.reduce((sum, module) => {
      sum.need += number(module.counts?.need);
      sum.packed += number(module.counts?.packed);
      sum.left += number(module.counts?.left);
      return sum;
    }, { need: 0, packed: 0, left: 0 });
    return `<div class="rs-page-stack">
      <section class="rs-stack-section is-summary-aggs"><div class="rs-stack-label">HOME</div><div class="rs-stack-aggs">${agg(totals.need, "NEED", "need")}${agg(totals.packed, "PACKED", "packed")}${agg(totals.left, "LEFT", "left")}</div></section>
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">MODULES</div></div>
        <div class="rs-kit-items">${modules.map(homeModuleRow).join("")}</div>
      </section>
    </div>`;
  }

  function homeModuleRow(module) {
    return `<div class="rs-kit-item-row">
      <div class="rs-kit-item-main">
        <div class="rs-kit-item-title rs-stack-label">${escapeHtml(module.label || module.key)}</div>
        <div class="rs-kit-item-meta">${number(module.counts?.need)} NEED / ${number(module.counts?.packed)} PACKED / ${number(module.counts?.left)} LEFT</div>
      </div>
      <div class="rs-kit-actions rs-item-filter-actions"><button class="rs-item-filter" type="button" data-action="set-module" data-module-key="${escapeAttr(module.key)}">OPEN</button></div>
    </div>`;
  }

  function planPanelHtml() {
    const report = planState;
    const planRows = filteredPlanItems(report);
    const counts = report?.counts || {};
    return `<div class="rs-page-stack">
      <section class="rs-stack-section is-summary-aggs"><div class="rs-stack-label">${escapeHtml(report?.plan?.label || ui.activeModule)}</div><div class="rs-stack-aggs">${agg(counts.need, "NEED", "need")}${agg(counts.packed, "PACKED", "packed")}${agg(counts.left, "LEFT", "left")}</div></section>
      ${planCountAggsHtml(report)}
      ${searchHtml()}
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">${escapeHtml(report?.plan?.label || "ITEMS")}</div></div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-count"><col class="rs-col-count"><col class="rs-col-count"></colgroup>
            <thead><tr><th class="rs-row-gutter">#</th><th>ITEM</th><th class="rs-count-head">NEED</th><th class="rs-count-head">PACKED</th><th class="rs-count-head">LEFT</th></tr></thead>
            <tbody>${planRows.map((item, index) => planItemRow(item, index)).join("") || ""}</tbody>
          </table>
        </div>
        ${planRows.length ? "" : `<div class="rs-status">No rows.</div>`}
      </section>
      ${laneTabsHtml()}
    </div>`;
  }

  function horseRosterPanelHtml() {
    const horses = filteredEntityHorses();
    return `<div class="rs-page-stack">
      ${secondaryTabsHtml()}
      ${searchHtml()}
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">PAK HORSES / ROSTERS</div><button class="rs-stack-pill" type="button" data-action="open-add-horse">ADD HORSE</button></div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-entity"><col class="rs-col-count"></colgroup>
            <thead><tr><th class="rs-row-gutter">#</th><th>HORSE</th><th>STATUS</th><th class="rs-count-head">ITEMS</th></tr></thead>
            <tbody>${horses.filter(matchesRosterSearch).map(rosterRowHtml).join("")}</tbody>
          </table>
        </div>
      </section>
    </div>`;
  }

  function rosterRowHtml(horse, index) {
    return `<tr>
      <td class="rs-row-gutter">${index + 1}</td>
      <td class="rs-entity-cell"><div class="rs-entity-main"><a class="rs-entity-horse" href="#" data-action="open-entity-horse" data-horse-id="${escapeAttr(horse.id)}">${escapeHtml(entityHorseLabel(horse))}</a>${horse.profileUrl ? `<a class="rs-open-text" href="${escapeAttr(horse.profileUrl)}" target="_blank" rel="noopener">Profile</a>` : ""}</div></td>
      <td class="rs-entity-cell"><span class="rs-entity-sub">${escapeHtml(entityHorseStatus(horse))}</span></td>
      <td class="rs-cell-number">${number((horse.memberships?.kitItemIds || horse.pakKitItemIds || []).length)}</td>
    </tr>`;
  }

  function matchesRosterSearch(horse) {
    const query = ui.search.trim().toLowerCase();
    if (!query) return true;
    return [horse.barnName, horse.name, horse.showName, horse.status, horse.notes].join(" ").toLowerCase().includes(query);
  }

  function horseProfilesPanelHtml() {
    const horses = filteredEntityHorses();
    return `<div class="rs-page-stack">
      ${secondaryTabsHtml()}
      ${searchHtml()}
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">PAK HORSES / PROFILE</div><button class="rs-stack-pill" type="button" data-action="open-add-horse">ADD HORSE</button></div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-entity"><col class="rs-col-count"></colgroup>
            <thead><tr><th class="rs-row-gutter">#</th><th>HORSE</th><th>PROFILE</th><th class="rs-count-head">COMMENTS</th></tr></thead>
            <tbody>${horses.map(profileRowHtml).join("")}</tbody>
          </table>
        </div>
      </section>
    </div>`;
  }

  function profileRowHtml(horse, index) {
    const comments = entityHorseComments(horse.id);
    return `<tr>
      <td class="rs-row-gutter">${index + 1}</td>
      <td class="rs-entity-cell"><div class="rs-entity-main"><a class="rs-entity-horse" href="#" data-action="open-entity-horse" data-horse-id="${escapeAttr(horse.id)}">${escapeHtml(entityHorseLabel(horse))}</a><span class="rs-entity-sub">${escapeHtml(horse.showName || horse.barnName || "")}</span></div></td>
      <td class="rs-entity-cell">${horse.profileUrl ? `<a class="rs-open-text" href="${escapeAttr(horse.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>` : `<span class="rs-entity-sub">No profile link</span>`}</td>
      <td class="rs-cell-number">${comments.length}</td>
    </tr>`;
  }

  function horseAttributesPanelHtml() {
    const attrs = horseEntityState?.attributes || {};
    const horses = filteredEntityHorses();
    return `<div class="rs-page-stack">
      ${secondaryTabsHtml()}
      ${searchHtml()}
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">PAK HORSES / ATTRIBUTES</div><button class="rs-stack-pill" type="button" data-action="open-add-horse">ADD HORSE</button></div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-entity"><col class="rs-col-count"></colgroup>
            <thead><tr><th class="rs-row-gutter">#</th><th>HORSE</th><th>WAVE</th><th class="rs-count-head">COMMENTS</th></tr></thead>
            <tbody>${horses.map(attributeHorseRowHtml).join("")}</tbody>
          </table>
        </div>
        <div class="rs-status">${number(attrs.gender?.length)} gender options | ${number(attrs.disciplines?.length)} discipline options | ${number(attrs.colors?.length)} color options</div>
      </section>
    </div>`;
  }

  function attributeHorseRowHtml(horse, index) {
    return `<tr>
      <td class="rs-row-gutter">${index + 1}</td>
      <td class="rs-entity-cell"><div class="rs-entity-main"><a class="rs-entity-horse" href="#" data-action="open-entity-horse" data-horse-id="${escapeAttr(horse.id)}">${escapeHtml(entityHorseLabel(horse))}</a><span class="rs-entity-sub">${escapeHtml(horse.notes || "")}</span></div></td>
      <td class="rs-entity-cell"><span class="rs-entity-sub">${escapeHtml(entityHorseWaveLabel(horse))}</span></td>
      <td class="rs-cell-number">${entityHorseComments(horse.id).length}</td>
    </tr>`;
  }

  function commentsPanelHtml() {
    const comments = combinedComments();
    return `<div class="rs-page-stack">
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">COMMENTS</div></div>
        <div class="rs-comments is-page-comments">${comments.map(commentRowHtml).join("") || `<div class="rs-airtable-empty">No comments.</div>`}</div>
      </section>
    </div>`;
  }

  function filteredPlanItems(report) {
    const query = ui.search.trim().toLowerCase();
    return (report?.items || [])
      .filter(matchesPlanLane)
      .filter((item) => !query || [item.label, item.displayLabel, item.itemLabel].join(" ").toLowerCase().includes(query));
  }

  function matchesPlanLane(item) {
    if (ui.laneKey === "open" || ui.laneKey === "all") return true;
    if (ui.laneKey === "need" || ui.laneKey === "left") return number(item.left) > 0;
    if (ui.laneKey === "packed") return number(item.packed) > 0;
    return true;
  }

  function planItemRow(item, index) {
    return `<tr>
      <td class="rs-row-gutter">${index + 1}</td>
      <td class="rs-entity-cell"><div class="rs-entity-main"><span class="rs-entity-horse">${escapeHtml(item.label || item.displayLabel || item.itemLabel || "")}</span></div></td>
      <td class="rs-cell-number">${number(item.need)}</td>
      <td class="rs-cell-number">${number(item.packed)}</td>
      <td class="rs-cell-number">${number(item.left)}</td>
    </tr>`;
  }

  function summaryAggsHtml() {
    const records = allCurrentViewRecords();
    const itemIds = new Set(records.flatMap((record) => record.items.map((item) => item.id)));
    const touched = records.reduce((sum, record) => sum + record.items.filter((item) => packingRow(record.horse.id, record.kit?.id, item.id)).length, 0);
    return section("HORSE KITS", `<div class="rs-stack-aggs">${agg(records.length, "HORSES", "horses")}${agg(itemIds.size, "KIT ITEMS", "kit_items")}${agg(touched, "TOUCHED", "touched")}</div>`, "is-summary-aggs");
  }

  function secondaryTabsHtml() {
    const controls = (state?.secondaryControls || []).length ? state.secondaryControls : [
      { key: "all", label: "ALL" },
      { key: "wave_one", label: "WAVE ONE" },
      { key: "wave_two", label: "WAVE TWO" },
      { key: "not_going", label: "NOT GOING" }
    ];
    return `<section class="rs-stack-section is-secondary-controls"><div class="rs-stack-tabs">${controls.map((control) => {
      const key = control.key || "";
      return `<button class="rs-stack-pill ${ui.viewKey === key ? "is-active" : ""}" type="button" data-action="set-view" data-view-key="${escapeAttr(key)}">${escapeHtml(control.label || key)}</button>`;
    }).join("")}</div></section>`;
  }

  function countAggsHtml() {
    const totals = rows.reduce((sum, row) => {
      sum.need += row.counts.need;
      sum.packed += row.counts.packed;
      sum.left += row.counts.left;
      return sum;
    }, { need: 0, packed: 0, left: 0 });
    return `<section class="rs-stack-section is-count-aggs"><div class="rs-secondary-count-aggs"><div class="rs-stack-aggs">${agg(totals.need, "NEED", "need")}${agg(totals.packed, "PACKED", "packed")}${agg(totals.left, "LEFT", "left")}</div></div></section>`;
  }

  function planCountAggsHtml(report) {
    const counts = report?.counts || {};
    return `<section class="rs-stack-section is-count-aggs"><div class="rs-secondary-count-aggs"><div class="rs-stack-aggs">${agg(counts.need, "NEED", "need")}${agg(counts.packed, "PACKED", "packed")}${agg(counts.left, "LEFT", "left")}</div></div></section>`;
  }

  function laneTabsHtml() {
    const lanes = [
      { key: "open", label: "OPEN" },
      { key: "need", label: "NEED" },
      { key: "packed", label: "PACKED" },
      { key: "left", label: "LEFT" }
    ];
    return `<section class="rs-stack-section is-lane-controls"><div class="rs-stack-tabs is-compact">${lanes.map((lane) => `<button class="rs-stack-pill ${ui.laneKey === lane.key ? "is-active" : ""}" type="button" data-action="set-lane" data-lane-key="${escapeAttr(lane.key)}">${escapeHtml(lane.label)}</button>`).join("")}</div></section>`;
  }

  function searchHtml() {
    return `<section class="rs-stack-section is-search"><div class="rs-airtable-toolbar"><div class="rs-search-wrap"><input class="rs-search" type="search" data-search autocomplete="off" placeholder="${escapeAttr(searchPlaceholder())}" value="${escapeAttr(ui.search)}"><button class="rs-search-clear ${ui.search ? "is-active" : ""}" type="button" data-action="clear-search" aria-label="Clear search"><span aria-hidden="true">&times;</span></button></div></div></section>`;
  }

  function searchPlaceholder() {
    if (["quantity", "per_horse", "per_groom"].includes(normalizeModuleKey(ui.activeModule))) return "Search items";
    return "Search horses";
  }

  function tableHtml() {
    if (ui.loading) return `<section class="rs-stack-section is-main-table"><div class="rs-airtable-empty">Loading horse kits...</div></section>`;
    if (ui.error) return `<section class="rs-stack-section is-main-table"><div class="rs-airtable-empty is-error">${escapeHtml(ui.error)}</div></section>`;
    return `
      <section class="rs-stack-section is-main-table">
        <div class="rs-table-stack-head"><div class="rs-stack-label">HORSES</div><button class="rs-stack-pill" type="button" data-action="print">PRINT</button></div>
        <div class="rs-airtable-scroll">
          <table class="rs-airtable-grid">
            <colgroup><col class="rs-col-gutter"><col class="rs-col-entity"><col class="rs-col-entity"><col class="rs-col-count"><col class="rs-col-count"><col class="rs-col-count"></colgroup>
            <thead><tr>
              <th class="rs-row-gutter">#</th>
              ${sortHead("HORSE", "horse")}
              ${sortHead("KIT", "kit")}
              ${sortHead("NEED", "need", "rs-count-head")}
              ${sortHead("PACKED", "packed", "rs-count-head")}
              ${sortHead("LEFT", "left", "rs-count-head")}
            </tr></thead>
            <tbody>${rows.map(tableRowHtml).join("")}</tbody>
          </table>
        </div>
        ${rows.length ? "" : `<div class="rs-status">No rows.</div>`}
        <div class="rs-status">${escapeHtml(`${rows.length} horses | ${state?.counts?.kits || 0} kits | ${state?.counts?.kitItems || 0} kit items | ${state?.counts?.packingRows || 0} touched rows`)}</div>
      </section>
    `;
  }

  function sortHead(label, key, className = "") {
    const mark = ui.sortKey === key ? (ui.sortDir === "asc" ? "ASC" : "DESC") : "";
    return `<th class="${escapeAttr(className)}"><button class="rs-sort-head ${ui.sortKey === key ? "is-active" : ""}" type="button" data-action="set-sort" data-sort-key="${escapeAttr(key)}"><span>${escapeHtml(label)}</span><span class="rs-sort-mark">${escapeHtml(mark)}</span></button></th>`;
  }

  function tableRowHtml(record, index) {
    return `
      <tr class="${ui.drawerOpen && ui.selectedHorseId === record.id ? "is-selected" : ""}">
        <td class="rs-row-gutter">${index + 1}</td>
        <td class="rs-entity-cell"><div class="rs-entity-main"><span class="rs-entity-horse">${escapeHtml(horseLabel(record.horse))}</span><a class="rs-open-text" href="#" data-action="open-horse" data-horse-id="${escapeAttr(record.id)}">Open</a></div></td>
        <td class="rs-entity-cell rs-entity-kit-cell"><span class="rs-entity-sub">${escapeHtml(kitLabel(record.kit))}</span></td>
        <td class="rs-cell-number">${record.counts.need}</td>
        <td class="rs-cell-number">${record.counts.packed}</td>
        <td class="rs-cell-number">${record.counts.left}</td>
      </tr>
    `;
  }

  function commentsHtml() {
    const comments = (state?.comments || []).slice(0, 5);
    return `<section class="rs-stack-section is-comments"><div class="rs-stack-label">COMMENTS</div>${comments.map(commentRowHtml).join("") || `<div class="rs-airtable-empty">No comments.</div>`}</section>`;
  }

  function combinedComments() {
    const rows = [
      ...(state?.comments || []),
      ...(horseEntityState?.comments || [])
    ];
    const seen = new Set();
    return rows
      .filter((comment) => {
        const key = comment.id || `${comment.scopeType || ""}:${comment.scopeId || ""}:${comment.comment || ""}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .sort((a, b) => compareText(b.createdAt || "", a.createdAt || ""));
  }

  function commentRowHtml(comment) {
    return `<div class="rs-comment-row"><div class="rs-comment-body">${escapeHtml(comment.comment || "")}</div><div class="rs-comment-meta">${escapeHtml(comment.scopeLabel || comment.scopeType || comment.createdBy || "")}</div></div>`;
  }

  function drawerHtml() {
    if (moduleBelongsToTab(ui.activeModule, "horses")) return entityDrawerHtml();
    const record = selectedRecord();
    if (!record) return "";
    const percent = record.counts.need ? Math.round((record.counts.packed / record.counts.need) * 100) : 0;
    return `
      <div class="rs-drawer-overlay ${ui.drawerOpen ? "is-open" : ""}" data-action="close-drawer" aria-hidden="true"></div>
      <aside class="rs-record-drawer ${ui.drawerOpen ? "is-open" : ""}" aria-hidden="${ui.drawerOpen ? "false" : "true"}">
        <div class="rs-drawer-head">
          <div class="rs-drawer-title-group"><div class="rs-drawer-title">${escapeHtml(horseLabel(record.horse))}</div>${record.horse.profileUrl ? `<a class="rs-drawer-profile-link" href="${escapeAttr(record.horse.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>` : ""}</div>
          <button class="rs-drawer-close" type="button" data-action="close-drawer" aria-label="Close"><span aria-hidden="true">&times;</span></button>
        </div>
        <div class="rs-drawer-body">
          <div class="rs-kit-progress"><div class="rs-kit-progress-label">${percent}% PACKED</div><div class="rs-kit-progress-track"><div class="rs-kit-progress-bar" style="width:${percent}%"></div></div></div>
          <div class="rs-summary-metrics">${agg(record.counts.need, "NEED", "need")}${agg(record.counts.packed, "PACKED", "packed")}${agg(record.counts.left, "LEFT", "left")}</div>
          <div class="rs-kit-item-search-row"><label class="rs-stack-label">SEARCH KIT ITEMS</label><div class="rs-search-wrap"><input class="rs-kit-item-search" type="search" data-item-search autocomplete="off" placeholder="Search kit items" value="${escapeAttr(ui.itemSearch)}"><button class="rs-search-clear ${ui.itemSearch ? "is-active" : ""}" type="button" data-action="clear-item-search" aria-label="Clear kit item search"><span aria-hidden="true">&times;</span></button></div></div>
          <div class="rs-kit-item-row rs-item-filter-row"><div class="rs-kit-item-main"><div class="rs-stack-label">FILTER KIT ITEMS</div></div><div class="rs-kit-actions rs-item-filter-actions">${itemFilter("All", "all")}${itemFilter("Not Packed", "not_packed")}${itemFilter("Packed", "packed")}${itemFilter("Not Needed", "not_needed")}</div></div>
          <div class="rs-kit-items"><div class="rs-stack-label">KIT ITEMS</div>${kitItemsHead()}${filteredItems(record).map((item) => kitItemHtml(record, item)).join("") || `<div class="rs-airtable-empty">No kit items.</div>`}</div>
        </div>
      </aside>
    `;
  }

  function entityDrawerHtml() {
    if (!ui.drawerOpen) return "";
    const horse = selectedEntityHorse();
    if (!horse && !ui.entityAddOpen) return "";
    const draft = ui.entityDraft || {};
    const active = draft.active !== false && !draft.inactive;
    const wave = draft.wec_not_going ? "not_going" : draft.wec_wave_2 ? "wave_two" : draft.wec_wave_1 ? "wave_one" : "";
    const allowed = entityAllowedDraftFields();
    return `
      <div class="rs-drawer-overlay is-open" data-action="close-drawer" aria-hidden="true"></div>
      <aside class="rs-record-drawer is-open" aria-hidden="false">
        <div class="rs-drawer-head">
          <div class="rs-drawer-title-group">
            <div class="rs-drawer-title">${escapeHtml(ui.entityAddOpen ? "Add Horse" : entityHorseLabel(horse))}</div>
            ${horse?.profileUrl ? `<a class="rs-drawer-profile-link" href="${escapeAttr(horse.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>` : ""}
          </div>
          <button class="rs-drawer-close" type="button" data-action="close-drawer" aria-label="Close"><span aria-hidden="true">&times;</span></button>
        </div>
        <div class="rs-drawer-body">
          ${allowed.has("barn_name") ? `<div class="rs-kit-item-row">
            <div class="rs-kit-item-main">
              <label class="rs-stack-label" for="rs-entity-barn-name">BARN NAME</label>
              <input id="rs-entity-barn-name" class="rs-search" type="text" data-entity-field="barn_name" value="${escapeAttr(draft.barn_name || "")}">
            </div>
          </div>` : ""}
          ${allowed.has("show_name") ? `<div class="rs-kit-item-row">
            <div class="rs-kit-item-main">
              <label class="rs-stack-label" for="rs-entity-show-name">SHOW NAME</label>
              <input id="rs-entity-show-name" class="rs-search" type="text" data-entity-field="show_name" value="${escapeAttr(draft.show_name || "")}">
            </div>
          </div>` : ""}
          ${allowed.has("notes") ? `<div class="rs-kit-item-row">
            <div class="rs-kit-item-main">
              <label class="rs-stack-label" for="rs-entity-notes">NOTES</label>
              <textarea id="rs-entity-notes" class="rs-search" data-entity-field="notes">${escapeHtml(draft.notes || "")}</textarea>
            </div>
          </div>` : ""}
          ${allowed.has("active") || allowed.has("inactive") ? `<div class="rs-kit-item-row">
            <div class="rs-kit-item-main"><div class="rs-stack-label">STATUS</div></div>
            <div class="rs-kit-actions rs-item-filter-actions">
              <button class="rs-item-filter ${active ? "is-active" : ""}" type="button" data-action="set-entity-status" data-status="active">ACTIVE</button>
              <button class="rs-item-filter ${!active ? "is-active" : ""}" type="button" data-action="set-entity-status" data-status="inactive">INACTIVE</button>
            </div>
          </div>` : ""}
          ${allowed.has("wec_wave_1") || allowed.has("wec_wave_2") || allowed.has("wec_not_going") ? `<div class="rs-kit-item-row">
            <div class="rs-kit-item-main"><div class="rs-stack-label">WAVE</div></div>
            <div class="rs-kit-actions rs-item-filter-actions">
              <button class="rs-item-filter ${wave === "wave_one" ? "is-active" : ""}" type="button" data-action="set-entity-wave" data-wave="wave_one">WAVE ONE</button>
              <button class="rs-item-filter ${wave === "wave_two" ? "is-active" : ""}" type="button" data-action="set-entity-wave" data-wave="wave_two">WAVE TWO</button>
              <button class="rs-item-filter ${wave === "not_going" ? "is-active" : ""}" type="button" data-action="set-entity-wave" data-wave="not_going">NOT GOING</button>
            </div>
          </div>` : ""}
          <div class="rs-kit-item-row">
            <div class="rs-kit-item-main">
              <div class="rs-stack-label">SAVE</div>
              <div class="rs-kit-item-meta">${escapeHtml(ui.entityError || (ui.entitySaving ? "Saving..." : "Changes save to Airtable."))}</div>
            </div>
            <div class="rs-kit-actions rs-item-filter-actions">
              <button class="rs-item-filter is-active" type="button" data-action="save-entity-horse">${ui.entitySaving ? "SAVING" : "SAVE"}</button>
            </div>
          </div>
          ${horse ? `<div class="rs-comments"><div class="rs-stack-label">COMMENTS</div>${entityHorseComments(horse.id).map(commentRowHtml).join("") || `<div class="rs-airtable-empty">No comments.</div>`}</div>` : ""}
        </div>
      </aside>
    `;
  }

  function filteredItems(record) {
    const query = ui.itemSearch.trim().toLowerCase();
    return record.items.filter((item) => {
      const status = itemState(record.horse.id, record.kit?.id, item.id);
      const matchesFilter = ui.itemFilter === "all" || ui.itemFilter === status;
      const matchesQuery = !query || itemLabel(item).toLowerCase().includes(query);
      return matchesFilter && matchesQuery;
    });
  }

  function kitItemsHead() {
    return `<div class="rs-kit-item-row rs-kit-items-head"><div class="rs-kit-item-main"><div class="rs-stack-label">ITEM</div></div><div class="rs-kit-actions"><div class="rs-stack-label">NOT PACKED</div><div class="rs-stack-label">PACKED</div><div class="rs-stack-label">NOT NEEDED</div></div></div>`;
  }

  function kitItemHtml(record, item) {
    const value = itemState(record.horse.id, record.kit?.id, item.id);
    const key = stateKey(record.horse.id, record.kit?.id, item.id);
    return `<div class="rs-kit-item-row"><div class="rs-kit-item-main"><div class="rs-kit-item-title rs-stack-label">${escapeHtml(itemLabel(item))}</div></div><div class="rs-kit-actions">${stateButton("Not Packed", "not_packed", value, item, key)}${stateButton("Packed", "packed", value, item, key)}${stateButton("Not Needed", "not_needed", value, item, key)}</div></div>`;
  }

  function stateButton(label, value, active, item, key) {
    return `<button class="rs-state-button ${active === value ? "is-active" : ""} ${ui.savingKey === key ? "is-saving" : ""}" type="button" data-action="set-item-state" data-kit-item-id="${escapeAttr(item.id)}" data-pack-state="${escapeAttr(value)}">${escapeHtml(label)}</button>`;
  }

  function itemFilter(label, value) {
    return `<button class="rs-item-filter ${ui.itemFilter === value ? "is-active" : ""}" type="button" data-action="set-item-filter" data-item-filter="${escapeAttr(value)}">${escapeHtml(label)}</button>`;
  }

  function moduleLabel(key) {
    const labels = {
      per_groom: "GROOM SUPPLIES"
    };
    return labels[key] || String(key || "MODULE").replace(/_/g, " ").toUpperCase();
  }

  function filteredEntityHorses() {
    const horses = horseEntityState?.horses || state?.horses || [];
    return horses.filter(matchesEntityView).filter(matchesRosterSearch);
  }

  function selectedEntityHorse() {
    return (horseEntityState?.horses || []).find((horse) => horse.id === ui.selectedHorseId)
      || (state?.horses || []).find((horse) => horse.id === ui.selectedHorseId)
      || null;
  }

  function entityAllowedDraftFields() {
    return new Set(horseEntityState?.allowedFields?.[ui.entityAddOpen ? "create" : "write"] || []);
  }

  function entityDraftFromHorse(horse) {
    return {
      barn_name: horse?.barnName || "",
      show_name: horse?.showName || "",
      notes: horse?.notes || "",
      active: horse?.active !== false,
      inactive: !!horse?.inactive,
      wec_wave_1: !!horse?.waveOne,
      wec_wave_2: !!horse?.waveTwo,
      wec_not_going: !!horse?.notGoing
    };
  }

  function entityChangedFields() {
    const allowed = entityAllowedDraftFields();
    const horse = selectedEntityHorse();
    const fields = {};
    const draft = ui.entityDraft || {};
    const candidates = ["barn_name", "show_name", "notes", "active", "inactive", "wec_wave_1", "wec_wave_2", "wec_not_going"];
    for (const key of candidates) {
      if (!allowed.has(key) || !Object.prototype.hasOwnProperty.call(draft, key)) continue;
      const value = normalizeEntityFieldValue(draft[key]);
      if (ui.entityAddOpen || value !== normalizeEntityFieldValue(entityCurrentValue(horse, key))) {
        fields[key] = draft[key];
      }
    }
    return fields;
  }

  function entityCurrentValue(horse, key) {
    if (key === "barn_name") return horse?.barnName || "";
    if (key === "show_name") return horse?.showName || "";
    if (key === "notes") return horse?.notes || "";
    if (key === "active") return horse?.active !== false;
    if (key === "inactive") return !!horse?.inactive;
    if (key === "wec_wave_1") return !!horse?.waveOne;
    if (key === "wec_wave_2") return !!horse?.waveTwo;
    if (key === "wec_not_going") return !!horse?.notGoing;
    return "";
  }

  function normalizeEntityFieldValue(value) {
    if (typeof value === "boolean") return value ? "true" : "false";
    return String(value ?? "").trim();
  }

  function matchesEntityView(horse) {
    if (ui.viewKey === "wave_one") return !!horse.waveOne || includes(horse.memberships?.waveKeys, "wave_one");
    if (ui.viewKey === "wave_two") return !!horse.waveTwo || includes(horse.memberships?.waveKeys, "wave_two");
    if (ui.viewKey === "not_going") return !!horse.notGoing;
    return true;
  }

  function entityHorseLabel(horse) {
    return horse?.barnName || horse?.name || horse?.showName || "";
  }

  function entityHorseStatus(horse) {
    if (horse?.notGoing) return "not_going";
    if (horse?.inactive || horse?.active === false) return "inactive";
    return "active";
  }

  function entityHorseWaveLabel(horse) {
    if (horse?.notGoing) return "not_going";
    if (horse?.waveOne) return "wave_one";
    if (horse?.waveTwo) return "wave_two";
    const keys = horse?.memberships?.waveKeys || [];
    return keys.length ? keys.join(", ") : "";
  }

  function entityHorseComments(horseId) {
    return (horseEntityState?.comments || []).filter((comment) => includes(comment.horseIds, horseId));
  }

  function defaultModuleForTab(tabKey) {
    return {
      horses: "horse_roster",
      counts: "horse_kits"
    }[tabKey] || "";
  }

  function moduleBelongsToTab(moduleKey, tabKey) {
    const modules = {
      horses: ["horse_roster", "horse_profiles", "horse_attributes"],
      counts: ["horse_kits", "quantity", "per_horse", "per_groom"]
    };
    return (modules[tabKey] || []).includes(moduleKey);
  }

  function section(label, body, modifier) {
    return `<section class="rs-stack-section ${modifier || ""}"><div class="rs-stack-label">${escapeHtml(label)}</div>${body}</section>`;
  }

  function agg(value, label, key) {
    return `<div class="rs-stack-agg is-${escapeAttr(key)}"><div class="rs-stack-agg-value">${escapeHtml(value == null ? 0 : value)}</div><div class="rs-stack-agg-label">${escapeHtml(label)}</div></div>`;
  }

  function allCurrentViewRecords() {
    return (state?.horses || []).map(recordForHorse);
  }

  function selectedRecord() {
    return rows.find((row) => row.id === ui.selectedHorseId) || rows[0] || null;
  }

  function horseLabel(horse) {
    return horse?.barnName || horse?.name || horse?.showName || "";
  }

  function kitLabel(kit) {
    return kit?.displayLabel || kit?.label || kit?.name || "";
  }

  function itemLabel(item) {
    return item?.displayLabel || item?.displayName || item?.label || item?.name || "";
  }

  function compareItem(a, b) {
    return compareNumber(a.sortOrder, b.sortOrder) || compareText(itemLabel(a), itemLabel(b));
  }

  function compareText(a, b) {
    return String(a || "").localeCompare(String(b || ""), undefined, { numeric: true, sensitivity: "base" });
  }

  function compareNumber(a, b) {
    return (Number(a) || 0) - (Number(b) || 0);
  }

  function stateKey(horseId, kitId, itemId) {
    return [horseId || "", kitId || "", itemId || ""].join("::");
  }

  function focusSearch(selector, caret) {
    requestAnimationFrame(() => {
      const input = root.querySelector(selector);
      if (input) {
        input.focus();
        input.setSelectionRange(caret, caret);
      }
    });
  }

  function assertState(data) {
    if (!data || data.ok !== true) throw new Error("invalid_horse_kits_state");
    if (!Array.isArray(data.horses) || !Array.isArray(data.kits) || !Array.isArray(data.kitItems) || !Array.isArray(data.packingRows)) {
      throw new Error("missing_horse_kits_arrays");
    }
  }

  function includes(values, value) {
    return Array.isArray(values) && values.includes(value);
  }

  function unique(values) {
    return [...new Set(values || [])];
  }

  function number(value) {
    const next = Number(value);
    return Number.isFinite(next) ? next : 0;
  }

  function normalizeModuleKey(value) {
    const key = String(value || "").trim().toLowerCase().replace(/-/g, "_");
    if (key === "horse_kits" || key === "horsekits") return "horse_kits";
    if (key === "perhorse") return "per_horse";
    if (key === "pergroom") return "per_groom";
    if (key === "byqty" || key === "by_quantity") return "quantity";
    return key;
  }

  function sessionKey() {
    return storedKey("wecPackingHybridSessionKey", "session");
  }

  function deviceId() {
    return storedKey("wecPackingHybridDeviceId", "device");
  }

  function storedKey(storageKey, prefix) {
    try {
      const existing = window.sessionStorage.getItem(storageKey) || window.localStorage.getItem(storageKey);
      if (existing) return existing;
      const next = `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
      window.sessionStorage.setItem(storageKey, next);
      if (prefix === "device") window.localStorage.setItem(storageKey, next);
      return next;
    } catch (error) {
      return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    }
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#039;"
    }[char]));
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }
})();
