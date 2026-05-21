(async () => {
  const debug = (...args) => console.info("[lp-history]", ...args);
  const fail = (stage, error) => {
    console.error("[lp-history]", stage, error);
    const root = document.getElementById("lp-history-app");
    if (root) root.textContent = "LP history failed to load. Check console for [lp-history].";
  };

  try {
  const root = document.getElementById("lp-history-app");
  if (!root) return;
  debug("boot", { href: window.location.href });

  const embeddedConfig = root.querySelector("#lp-history-config");
  const embeddedGlobalTagRules = root.querySelector("#lp-global-tag-rules")?.textContent || "[]";
  const config = window.LP_HISTORY_CONFIG || JSON.parse(embeddedConfig?.textContent || "{}");
  const enrichmentUrl = config.enrichmentUrl || "/lp-history/enrichment";
  const profileUrl = config.profileUrl || "";
  debug("config", config);
  root.innerHTML = appShellMarkup();
  const [payload, layer, profilePayload] = await Promise.all([
    fetch(config.historyUrl).then((response) => {
      if (!response.ok) throw new Error("History feed failed: " + response.status);
      debug("history fetch", response.status, config.historyUrl);
      return response.json();
    }),
    fetch(config.layerUrl).then((response) => {
      debug("layer fetch", response.status, config.layerUrl);
      return response.ok ? response.json() : emptyLayer();
    }).catch((error) => {
      console.warn("[lp-history] layer fetch failed, using empty layer", error);
      return emptyLayer();
    }),
    profileUrl ? fetch(profileUrl).then((response) => {
      debug("profile fetch", response.status, profileUrl);
      return response.ok ? response.json() : emptyProfileContent();
    }).catch((error) => {
      console.warn("[lp-history] profile fetch failed, using static profile panels", error);
      return emptyProfileContent();
    }) : Promise.resolve(emptyProfileContent())
  ]);
  const searchParams = new URLSearchParams(window.location.search || "");
  const hashParams = new URLSearchParams(String(window.location.hash || "").replace(/^#/, ""));
  const editKey = config.editKey || searchParams.get("key") || hashParams.get("key") || "";
  const editMode = config.mode === "edit" || !!editKey;
  debug("edit mode", editMode ? "on" : "off");
  const layerStorageKey = "lp-history-layer-draft";
  const themeStorageKey = "lp-history-theme-colors";
  const themeColors = loadThemeColors();
  const state = normalize(payload, loadStoredLayer(layer));
  const profileContent = normalizeProfileContent(profilePayload);
  const globalTagRules = JSON.parse(embeddedGlobalTagRules);
  root.classList.toggle("is-edit-mode", editMode);
  root.classList.add("is-overview-active");
  applyThemeColors();
  const overviewControls = {
    competitions: { sort: "desc", month: "all", year: "all" },
    classes: { sort: "desc", month: "all", year: "all" }
  };
  const overviewYears = new Set(defaultOverviewYears());
  const globalTags = new Set();
  const allControls = {
    competitions: { sort: "desc", month: "all", year: "all" },
    classes: { sort: "desc", month: "all", year: "all" },
    videos: { sort: "desc", month: "all", year: "all" },
    horses: { sort: "desc", month: "all", year: "all", search: "", type: "all" }
  };
  const saveTimers = new Map();
  const sectionFilterState = {};
  let singleFieldCounter = 0;
  const viewControls = {
    overviewCompetitions: "list",
    overviewClasses: "list",
    horses: "list",
    competitions: "list",
    classes: "list"
  };
  const panels = {
    overview: root.querySelector('[data-panel="overview"]'),
    videos: root.querySelector('[data-panel="videos"]'),
    horses: root.querySelector('[data-panel="horses"]'),
    competitions: root.querySelector('[data-panel="competitions"]'),
    classes: root.querySelector('[data-panel="classes"]')
  };
  const modal = root.querySelector("[data-modal]");
  const modalCard = root.querySelector(".lp-modal-card");
  const modalContent = root.querySelector("[data-modal-content]");
  const renderedPanels = new Set();

  renderShell();
  renderProfilePanels();
  renderOverview();
  renderRidingNestedPanels();
  debug("render complete", currentCounts());

  root.addEventListener("click", (event) => {
    if (event.target.closest("[data-modal-close]")) {
      closeModal();
      return;
    }

    if (event.target.closest("[data-theme-color]")) {
      event.stopPropagation();
      return;
    }

    const editChoice = event.target.closest(".lp-edit-choice, .lp-edit-checkbox");
    if (editChoice) {
      const input = editChoice.querySelector("[data-layer-field]");
      if (input && editMode) {
        event.preventDefault();
        event.stopPropagation();
        const isMulti = !!input.dataset.layerMulti;
        if (input.type === "radio") {
          input.checked = true;
          setLayerValue(input.dataset.layerKind, input.dataset.layerId, input.dataset.layerField, input.value);
        } else if (input.type === "checkbox") {
          input.checked = !input.checked;
          if (isMulti) {
            setLayerMultiValue(input.dataset.layerKind, input.dataset.layerId, input.dataset.layerField, input.value, input.checked);
          } else {
            setLayerValue(input.dataset.layerKind, input.dataset.layerId, input.dataset.layerField, input.checked);
          }
        }
        if (["status", "recordState"].includes(input.dataset.layerField)) {
          renderLayerScope(input.dataset.layerKind);
        }
      }
      return;
    }

    const nestedTab = event.target.closest("[data-nested-tab]");
    if (nestedTab) {
      selectNestedTab(nestedTab.dataset.nestedGroup, nestedTab.dataset.nestedTab);
      return;
    }

    const profileTab = event.target.closest("[data-profile-tab]");
    if (profileTab) {
      selectProfileTab(profileTab.dataset.profileTab);
      return;
    }

    const tab = event.target.closest("[data-tab]");
    if (tab) {
      selectTab(tab.dataset.tab);
      return;
    }

    const tabLink = event.target.closest("[data-select-tab]");
    if (tabLink) {
      selectTab(tabLink.dataset.selectTab);
      return;
    }

    const videoNav = event.target.closest("[data-video-nav]");
    if (videoNav) {
      moveVideoRail(videoNav);
      return;
    }

    const overviewYear = event.target.closest("[data-overview-year]");
    if (overviewYear) {
      const year = overviewYear.dataset.overviewYear;
      if (overviewYears.has(year)) {
        overviewYears.delete(year);
      } else {
        overviewYears.add(year);
      }
      renderDataScope();
      return;
    }

    const globalTag = event.target.closest("[data-global-tag]");
    if (globalTag) {
      const tag = globalTag.dataset.globalTag;
      if (globalTags.has(tag)) {
        globalTags.delete(tag);
      } else {
        globalTags.add(tag);
      }
      renderDataScope();
      return;
    }

    const sectionFilterToggle = event.target.closest("[data-section-filter-toggle]");
    if (sectionFilterToggle) {
      const key = sectionFilterToggle.dataset.sectionFilterToggle;
      sectionFilterState[key] = !sectionFilterState[key];
      renderFilterScope(key);
      return;
    }

    const sectionFilterClose = event.target.closest("[data-section-filter-close]");
    if (sectionFilterClose) {
      const key = sectionFilterClose.dataset.sectionFilterClose;
      sectionFilterState[key] = false;
      renderFilterScope(key);
      return;
    }

    const viewButton = event.target.closest("[data-view-toggle]");
    if (viewButton) {
      const target = viewButton.dataset.viewToggle;
      viewControls[target] = viewButton.dataset.viewMode;
      renderViewTarget(target);
      return;
    }

    const overviewSort = event.target.closest("[data-overview-sort]");
    if (overviewSort) {
      const target = overviewSort.dataset.overviewSort;
      overviewControls[target].sort = overviewControls[target].sort === "desc" ? "asc" : "desc";
      renderOverview();
      return;
    }

    const overviewClear = event.target.closest("[data-overview-clear]");
    if (overviewClear) {
      const target = overviewClear.dataset.overviewClear;
      overviewControls[target].month = "all";
      overviewControls[target].year = "all";
      renderOverview();
      return;
    }

    const allSort = event.target.closest("[data-all-sort]");
    if (allSort) {
      const target = allSort.dataset.allSort;
      allControls[target].sort = allControls[target].sort === "desc" ? "asc" : "desc";
      renderAllPanel(target);
      return;
    }

    const allClear = event.target.closest("[data-all-clear]");
    if (allClear) {
      const target = allClear.dataset.allClear;
      allControls[target].month = "all";
      allControls[target].year = "all";
      if ("search" in allControls[target]) allControls[target].search = "";
      if ("type" in allControls[target]) allControls[target].type = "all";
      renderAllPanel(target);
      return;
    }

    const layerAction = event.target.closest("[data-layer-action]");
    if (layerAction) {
      handleLayerAction(layerAction);
      return;
    }

    const layerToggle = event.target.closest("[data-layer-toggle]");
    if (layerToggle) {
      setLayerValue(layerToggle.dataset.layerKind, layerToggle.dataset.layerId, layerToggle.dataset.layerField, layerToggle.checked);
      renderLayerScope(layerToggle.dataset.layerKind);
      return;
    }

    const horseButton = event.target.closest("[data-open-horse]");
    if (horseButton) {
      openHorse(horseButton.dataset.openHorse);
      return;
    }

    const competitionButton = event.target.closest("[data-open-competition]");
    if (competitionButton) {
      openCompetition(competitionButton.dataset.openCompetition);
      return;
    }

    const classButton = event.target.closest("[data-open-class]");
    if (classButton) {
      openClass(classButton.dataset.openClass);
      return;
    }

    const videoButton = event.target.closest("[data-open-video]");
    if (videoButton) {
      openVideo(videoButton.dataset.openVideo);
      return;
    }

  });

  root.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !modal.hidden) closeModal();
  });

  root.addEventListener("change", (event) => {
    const overviewFilter = event.target.closest("[data-overview-filter]");
    if (overviewFilter) {
      const target = overviewFilter.dataset.overviewFilter;
      const field = overviewFilter.dataset.filterField;
      overviewControls[target][field] = overviewFilter.value;
      renderOverview();
      return;
    }

    const themeColor = event.target.closest("[data-theme-color]");
    if (themeColor) {
      setThemeColor(themeColor.dataset.themeColor, themeColor.value);
      return;
    }

    const allFilter = event.target.closest("[data-all-filter]");
    if (allFilter) {
      const target = allFilter.dataset.allFilter;
      const field = allFilter.dataset.filterField;
      allControls[target][field] = allFilter.value;
      renderAllPanel(target);
      return;
    }

    const layerField = event.target.closest("[data-layer-field]");
    if (layerField) {
      if (layerField.type === "file") {
        handleLayerFile(layerField);
      } else if (layerField.dataset.layerMulti) {
        setLayerMultiValue(layerField.dataset.layerKind, layerField.dataset.layerId, layerField.dataset.layerField, layerField.value, layerField.checked);
      } else {
        setLayerValue(layerField.dataset.layerKind, layerField.dataset.layerId, layerField.dataset.layerField, layerInputValue(layerField));
      }
      if (["status", "recordState"].includes(layerField.dataset.layerField)) {
        renderLayerScope(layerField.dataset.layerKind);
      }
      return;
    }
  });

  root.addEventListener("input", (event) => {
    const themeColor = event.target.closest("[data-theme-color]");
    if (themeColor) {
      setThemeColor(themeColor.dataset.themeColor, themeColor.value);
      return;
    }
    const allFilter = event.target.closest("[data-all-filter]");
    if (allFilter) {
      const target = allFilter.dataset.allFilter;
      const field = allFilter.dataset.filterField;
      allControls[target][field] = allFilter.value;
      renderAllPanel(target);
      return;
    }
    const layerField = event.target.closest("[data-layer-field]");
    if (layerField) {
      if (layerField.type === "file") {
        handleLayerFile(layerField);
      } else if (layerField.dataset.layerMulti) {
        setLayerMultiValue(layerField.dataset.layerKind, layerField.dataset.layerId, layerField.dataset.layerField, layerField.value, layerField.checked);
      } else {
        setLayerValue(layerField.dataset.layerKind, layerField.dataset.layerId, layerField.dataset.layerField, layerInputValue(layerField));
      }
      if (["status", "recordState"].includes(layerField.dataset.layerField)) {
        renderLayerScope(layerField.dataset.layerKind);
      }
    }
  });

  root.addEventListener("change", (event) => {
    const allFilter = event.target.closest("[data-all-filter]");
    if (allFilter) {
      const target = allFilter.dataset.allFilter;
      const field = allFilter.dataset.filterField;
      allControls[target][field] = allFilter.value;
      renderAllPanel(target);
    }
  });

  function appShellMarkup() {
    return [
      '<div class="lp-profile-shell">',
      profileTabsMarkup(),
      '<main class="lp-content lp-profile-content">',
      profilePanelMarkup("home", false, "Home", "Profile", [
        ["Name", "Lainey in the Ring"],
        ["Title", "All USEF Ride History"],
        ["Subtitle", "Profile overview"],
        ["Location", "Wellington, FL"],
        ["Bio main photo", "Ready for profile media"]
      ]),
      profilePanelMarkup("bio", false, "Bio", "Profile", [
        ["Education", "Field ready"],
        ["Awards", "Field ready"],
        ["Story", "Field ready"]
      ]),
      '<section class="lp-panel lp-profile-panel is-active" data-profile-panel="riding">',
      '<div class="lp-shell">',
      '<section class="lp-section-block lp-riding-shell-section lp-theme-competitions">',
      ridingTopSection(),
      "</section>",
      profileInlineTabsSection("riding"),
      bottomNavOffset(),
      "</div>",
      "</section>",
      '<section class="lp-panel lp-profile-panel" data-profile-panel="horses">',
      profileTopSection("Horses", "LP History", "", true, "horses"),
      profileInlineTabsSection("horses"),
      bottomNavOffset(),
      "</section>",
      '<section class="lp-panel lp-profile-panel" data-profile-panel="videos">',
      profileTopSection("Videos", "LP History", "", true, "videos"),
      profileInlineTabsSection("videos"),
      bottomNavOffset(),
      "</section>",
      profilePanelMarkup("contact", false, "Contact", "Profile", [
        ["Email", "Field ready"],
        ["Phone", "Field ready"],
        ["Contact form", "Submit label ready"]
      ]),
      profilePanelMarkup("blank", false, "Blank", "Tab", [
        ["Blank tab", "Ready for next section"]
      ]),
      "</main>",
      "</div>",
      '<div class="lp-modal" data-modal hidden>',
      '<div class="lp-modal-backdrop" data-modal-close></div>',
      '<section class="lp-modal-card" role="dialog" aria-modal="true" aria-labelledby="lp-modal-title" tabindex="-1">',
      '<button class="lp-modal-close" type="button" data-modal-close aria-label="Close detail">x</button>',
      '<div data-modal-content></div>',
      "</section>",
      "</div>"
    ].join("");
  }

  function profileTabsMarkup(variant = "") {
    const inlineClass = variant === "inline" ? " lp-profile-tabs-inline" : "";
    return [
      '<nav class="lp-tabs lp-profile-tabs' + inlineClass + '" aria-label="Profile sections">',
      '<button class="lp-tab lp-profile-tab lp-theme-overview" type="button" data-profile-tab="home" aria-selected="false"><span class="lp-tab-value">Home</span><span class="lp-tab-label">Profile</span></button>',
      '<button class="lp-tab lp-profile-tab lp-theme-overview" type="button" data-profile-tab="bio" aria-selected="false"><span class="lp-tab-value">Bio</span><span class="lp-tab-label">Profile</span></button>',
      '<button class="lp-tab lp-profile-tab lp-theme-competitions is-active" type="button" data-profile-tab="riding" aria-selected="true"><span class="lp-tab-value">Riding</span><span class="lp-tab-label">History</span></button>',
      '<button class="lp-tab lp-profile-tab lp-theme-horses" type="button" data-profile-tab="horses" aria-selected="false"><span class="lp-tab-value">Horses</span><span class="lp-tab-label">LP History</span></button>',
      '<button class="lp-tab lp-profile-tab lp-theme-videos" type="button" data-profile-tab="videos" aria-selected="false"><span class="lp-tab-value">Videos</span><span class="lp-tab-label">LP History</span></button>',
      '<button class="lp-tab lp-profile-tab lp-theme-classes" type="button" data-profile-tab="contact" aria-selected="false"><span class="lp-tab-value">Contact</span><span class="lp-tab-label">Info</span></button>',
      '<button class="lp-tab lp-profile-tab lp-theme-overview" type="button" data-profile-tab="blank" aria-selected="false"><span class="lp-tab-value">Blank</span><span class="lp-tab-label">Tab</span></button>',
      "</nav>"
    ].join("");
  }

  function profilePanelMarkup(key, isActive, title, count, rows) {
    return [
      '<section class="lp-panel lp-profile-panel' + (isActive ? " is-active" : "") + '" data-profile-panel="' + escapeHtml(key) + '">',
      profileTopSection(title, count, "", ["home", "bio", "contact"].includes(key)),
      profileInlineTabsSection(key),
      bottomNavOffset(),
      "</section>"
    ].join("");
  }

  function profileTopSection(title, label, body = "", hasHero = false, theme = "overview") {
    return [
      '<section class="lp-section-block lp-section-top lp-theme-' + escapeAttr(theme) + '">',
      '<div class="lp-section-top-inner">',
      hasHero ? '<div class="lp-section-hero-placeholder" aria-hidden="true"></div>' : "",
      '<div class="lp-section-hero-copy">',
      '<h1>' + escapeHtml(title) + "</h1>",
      '<p>' + escapeHtml(label || "") + "</p>",
      body ? '<p class="lp-section-hero-body">' + escapeHtml(body) + "</p>" : "",
      "</div>",
      "</div>",
      "</section>"
    ].join("");
  }

  function ridingTopSection() {
    return [
      '<section class="lp-section-block lp-section-top lp-theme-competitions">',
      '<div class="lp-section-top-inner">',
      '<div class="lp-section-hero-placeholder" aria-hidden="true"></div>',
      '<div class="lp-riding-hero-filters" aria-label="Global history filters">',
      '<div class="lp-year-filter"><div class="lp-year-pills" data-overview-years></div><span>Years</span></div>',
      '<div class="lp-year-filter lp-tag-filter"><div class="lp-year-pills lp-tag-pills" data-global-tags></div><span>Tags</span></div>',
      "</div>",
      '<div class="lp-section-hero-copy">',
      "<h1>Riding</h1>",
      "<p>History</p>",
      "</div>",
      "</div>",
      "</section>"
    ].join("");
  }

  function profileInlineTabsSection(group) {
    const tabs = group === "riding" ? [
      ["competitions", "Competitions", "LP History", "competitions"],
      ["classes", "Classes", "LP History", "classes"],
      ["horses", "Horses", "LP History", "horses"],
      ["videos", "Videos", "LP History", "videos"]
    ] : group === "horses" ? [
      ["ponies", "Ponies", "LP History", "horses"],
      ["horses", "Horses", "LP History", "horses"],
      ["owned", "Owned", "LP History", "horses"],
      ["hacked", "Hacked", "LP History", "horses"]
    ] : group === "videos" ? [
      ["featured", "Featured", "Videos", "videos"],
      ["training", "Training", "Videos", "videos"],
      ["shows", "Shows", "Videos", "videos"]
    ] : group === "bio" ? [
      ["about", "About me", "Profile", "overview"],
      ["working", "Working", "Profile", "overview"],
      ["school", "School", "Profile", "overview"],
      ["fun", "For Fun", "Profile", "overview"]
    ] : [
      ["home", "Home", "Profile", "overview"],
      ["bio", "Bio", "Profile", "overview"],
      ["riding", "Riding", "History", "competitions"],
      ["horses", "Horses", "LP History", "horses"],
      ["videos", "Videos", "LP History", "videos"],
      ["contact", "Contact", "Info", "classes"],
      ["blank", "Blank", "Tab", "overview"]
    ];
    return [
      '<section class="lp-section-block lp-home-inline-tabs lp-theme-overview" data-nested-tabs="' + escapeAttr(group) + '">',
      '<nav class="lp-tabs lp-profile-tabs lp-profile-tabs-inline" aria-label="' + escapeAttr(group) + ' nested sections">',
      tabs.map(([key, title, label, theme], index) => (
        '<button class="lp-tab lp-profile-tab lp-theme-' + escapeAttr(theme) + (index === 0 ? " is-active" : "") + '" type="button" data-nested-group="' + escapeAttr(group) + '" data-nested-tab="' + escapeAttr(key) + '" aria-selected="' + (index === 0 ? "true" : "false") + '"><span class="lp-tab-value">' + escapeHtml(title) + '</span><span class="lp-tab-label">' + escapeHtml(label) + "</span></button>"
      )).join(""),
      "</nav>",
      '<div class="lp-nested-panels">',
      tabs.map(([key], index) => (
        '<div class="lp-nested-panel' + (index === 0 ? " is-active" : "") + '" data-nested-panel="' + escapeAttr(key) + '"></div>'
      )).join(""),
      "</div>",
      "</section>"
    ].join("");
  }

  function bottomNavOffset() {
    return '<div class="lp-bottom-nav-offset" aria-hidden="true"></div>';
  }

  function emptyLayer() {
    return { version: 1, updatedAt: "", horses: {}, competitions: {}, classes: {}, videos: {} };
  }

  function emptyProfileContent() {
    return { ok: true, records: [] };
  }

  function normalizeArray(value) {
    if (Array.isArray(value)) return value;
    if (!value) return [];
    return String(value).split(",").map((item) => item.trim()).filter(Boolean);
  }

  function normalizeProfileContent(payload) {
    const groups = {};
    const records = Array.isArray(payload?.records) ? payload.records : [];
    records.forEach((record) => {
      const fields = record.fields || {};
      const type = String(fields.record_type || fields.type || fields.Page || "").trim().toLowerCase();
      if (!type) return;
      const stateValue = String(fields.state || fields.record_state || "active").trim().toLowerCase();
      if (stateValue === "inactive" || stateValue === "ignore") return;
      if (!groups[type]) groups[type] = [];
      groups[type].push({
        id: record.id,
        recordKey: String(fields.record_key || fields.Field || record.id || "").trim(),
        type,
        title: String(fields.title || fields.name || fields.Field || fields.record_key || record.id || "").trim(),
        subtitle: String(fields.subtitle || "").trim(),
        body: String(fields.body || fields.description || fields.notes || fields.Note || "").trim(),
        field: String(fields.Field || fields.record_key || "").trim(),
        note: String(fields.Note || fields.body || fields.description || fields.notes || "").trim(),
        kind: String(fields["Input or styling"] || fields.kind || "").trim(),
        imageUrl: String(fields.image_url || "").trim(),
        videoUrl: String(fields.video_url || "").trim(),
        sortOrder: Number(fields.sort_order || 0),
        status: normalizeArray(fields.status),
        tags: normalizeArray(fields.tags),
        fields
      });
    });
    Object.values(groups).forEach((items) => {
      items.sort((a, b) => (a.sortOrder - b.sortOrder) || a.title.localeCompare(b.title));
    });
    return groups;
  }

  function normalizeLayer(layer) {
    return {
      ...emptyLayer(),
      ...(layer || {}),
      horses: { ...((layer || {}).horses || {}) },
      competitions: { ...((layer || {}).competitions || {}) },
      classes: { ...((layer || {}).classes || {}) },
      videos: { ...((layer || {}).videos || {}) }
    };
  }

  function loadStoredLayer(seed) {
    const base = normalizeLayer(seed);
    try {
      const stored = window.localStorage.getItem(layerStorageKey);
      if (!stored) return base;
      return mergeLayer(base, JSON.parse(stored));
    } catch (error) {
      return base;
    }
  }

  function mergeLayer(base, draft) {
    const normalizedBase = normalizeLayer(base);
    const normalizedDraft = normalizeLayer(draft);
    return {
      ...normalizedBase,
      ...normalizedDraft,
      horses: { ...normalizedBase.horses, ...normalizedDraft.horses },
      competitions: { ...normalizedBase.competitions, ...normalizedDraft.competitions },
      classes: { ...normalizedBase.classes, ...normalizedDraft.classes },
      videos: { ...normalizedBase.videos, ...normalizedDraft.videos }
    };
  }

  function defaultThemeColors() {
    return {
      overview: "#46332b",
      videos: "#003d80",
      horses: "#005c2a",
      competitions: "#4e1f76",
      classes: "#8f1116"
    };
  }

  function loadThemeColors() {
    const defaults = defaultThemeColors();
    try {
      const stored = JSON.parse(window.localStorage.getItem(themeStorageKey) || "{}");
      return { ...defaults, ...stored };
    } catch (error) {
      return defaults;
    }
  }

  function setThemeColor(key, value) {
    if (!themeColors[key] || !/^#[0-9a-f]{6}$/i.test(value)) return;
    themeColors[key] = value;
    try {
      window.localStorage.setItem(themeStorageKey, JSON.stringify(themeColors));
    } catch (error) {}
    applyThemeColors();
  }

  function applyThemeColors() {
    Object.entries(themeColors).forEach(([key, color]) => {
      root.style.setProperty("--lp-active-" + key, color);
      root.style.setProperty("--lp-shade-" + key, hexToRgba(color, key === "classes" ? 0.035 : 0.06));
      root.style.setProperty("--lp-shade-" + key + "-hover", hexToRgba(color, key === "classes" ? 0.08 : 0.11));
      if (key === "classes") root.style.setProperty("--lp-shade-classes-row", hexToRgba(color, 0.025));
    });
  }

  function hexToRgba(hex, alpha) {
    const value = String(hex || "").replace("#", "");
    const number = parseInt(value, 16);
    if (!Number.isFinite(number)) return "rgba(0, 0, 0, " + alpha + ")";
    const r = (number >> 16) & 255;
    const g = (number >> 8) & 255;
    const b = number & 255;
    return "rgba(" + r + ", " + g + ", " + b + ", " + alpha + ")";
  }

  function normalize(data, layer = emptyLayer()) {
    layer = normalizeLayer(layer);
    const source = data.state || data;
    const competitions = (source.competitions || []).map((competition) => ({
      ...competition,
      layer: layer.competitions?.[competition.competitionId] || {},
      sortDate: parseDate(competition.endDate || competition.startDate),
      sections: competition.sections || []
    }));

    competitions.sort((a, b) => b.sortDate - a.sortDate);

    const classRows = [];
    const horseMap = new Map();
    const sectionMap = new Map();

    competitions.forEach((competition) => {
      competition.sections.forEach((section) => {
        const sectionName = section.sectionName || "Uncategorized";
        sectionMap.set(sectionName, (sectionMap.get(sectionName) || 0) + (section.classes || []).length);
        (section.classes || []).forEach((classItem, index) => {
          const horse = classItem.horse || {};
          const horseId = horse.usefHorseId || horse.name || "unknown-horse";
          const classId = stableId([
            competition.competitionId,
            sectionName,
            classItem.classUrl || classItem.classCode || classItem.classTitle || index,
            horseId
          ]);
          const row = {
            id: classId,
            layer: layer.classes?.[classId] || {},
            competitionId: competition.competitionId,
            competitionName: competition.competitionName,
            competition,
            sectionName: displaySectionName(sectionName),
            rawSectionName: sectionName,
            horse,
            horseId,
            classCode: classItem.classCode,
            classTitle: classItem.classTitle || classItem.classCode || "Untitled class",
            classUrl: classItem.classUrl,
            entries: classItem.entries,
            placing: classItem.placing,
            natlPoints: classItem.natlPoints,
            zonePoints: classItem.zonePoints,
            backNumber: classItem.backNumber,
            sortDate: competition.sortDate
          };

          classRows.push(row);

          if (!horseMap.has(horseId)) {
            horseMap.set(horseId, {
              id: horseId,
              layer: layer.horses?.[horseId] || {},
              name: horse.name || "Unknown horse",
              link: horse.link,
              classes: [],
              competitions: new Map(),
              totalNatlPoints: 0,
              totalZonePoints: 0
            });
          }

          const horseEntry = horseMap.get(horseId);
          horseEntry.classes.push(row);
          horseEntry.competitions.set(competition.competitionId, competition);
          horseEntry.sortDate = Math.max(horseEntry.sortDate || 0, competition.sortDate || 0);
          horseEntry.totalNatlPoints += numberValue(classItem.natlPoints);
          horseEntry.totalZonePoints += numberValue(classItem.zonePoints);
        });
      });
    });

    classRows.sort((a, b) => b.sortDate - a.sortDate);

    const horses = Array.from(horseMap.values()).map((horse) => ({
      ...horse,
      type: horse.layer.type || horseType(horse),
      gender: horse.layer.gender || ""
    })).sort((a, b) => {
      if (b.classes.length !== a.classes.length) return b.classes.length - a.classes.length;
      return a.name.localeCompare(b.name);
    });

    const sections = Array.from(sectionMap, ([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count);

    return {
      competitions,
      classRows,
      horses,
      sections,
      layer,
      videos: mockVideos(classRows, horses).map((video) => ({
        ...video,
        layer: layer.videos?.[video.id] || {}
      })),
      counts: {
        competitions: competitions.length,
        classes: classRows.length,
        horses: horses.length,
        sections: competitions.reduce((total, competition) => total + competition.sections.length, 0)
      },
      dateRange: {
        start: competitions.length ? formatDate(new Date(Math.min(...competitions.map((c) => c.sortDate)))) : "",
        end: competitions.length ? formatDate(new Date(Math.max(...competitions.map((c) => c.sortDate)))) : "",
        startIso: competitions.length ? inputDate(new Date(Math.min(...competitions.map((c) => c.sortDate)))) : "",
        endIso: competitions.length ? inputDate(new Date(Math.max(...competitions.map((c) => c.sortDate)))) : ""
      }
    };
  }

  function renderShell() {
    const scopedDateRange = currentDateRange();
    setText("[data-lp-summary]", scopedDateRange.start && scopedDateRange.end
      ? scopedDateRange.start + " to " + scopedDateRange.end
      : "Competition results");

    const yearWrap = root.querySelector("[data-overview-years]");
    if (yearWrap) {
      yearWrap.innerHTML = overviewYearOptions().map((year) => (
        '<button class="lp-year-pill' + (overviewYears.has(year) ? " is-active" : "") + '" type="button" data-overview-year="' + escapeAttr(year) + '" aria-pressed="' + (overviewYears.has(year) ? "true" : "false") + '">' + escapeHtml(year) + "</button>"
      )).join("");
    }

    const tagWrap = root.querySelector("[data-global-tags]");
    const tagSection = tagWrap ? tagWrap.closest(".lp-tag-filter") : null;
    if (tagWrap) {
      const tags = globalTagOptions();
      tagWrap.innerHTML = tags.map((tag) => (
        '<button class="lp-year-pill lp-tag-pill' + (globalTags.has(tag.name) ? " is-active" : "") + '" type="button" data-global-tag="' + escapeAttr(tag.name) + '" aria-pressed="' + (globalTags.has(tag.name) ? "true" : "false") + '" style="' + escapeAttr(tagStyle(tag.tagClass)) + '">' + escapeHtml(tag.name) + "</button>"
      )).join("");
      if (tagSection) tagSection.hidden = !tags.length;
    }

    const counts = currentCounts();
    setText('[data-tab-count="horses"]', counts.horses);
    setText('[data-tab-count="videos"]', counts.videos);
    setText('[data-tab-count="competitions"]', counts.competitions);
    setText('[data-tab-count="classes"]', counts.classes);
    root.querySelectorAll("[data-theme-color]").forEach((input) => {
      const key = input.dataset.themeColor;
      if (themeColors[key]) input.value = themeColors[key];
    });
  }

  function setText(selector, value) {
    const el = root.querySelector(selector);
    if (el) el.textContent = value;
  }

  function renderProfilePanels() {
    renderProfilePanelFromContent("home", "Home", "Profile");
    renderProfilePanelFromContent("bio", "Bio", "Profile");
    renderProfilePanelFromContent("contact", "Contact", "Info");
    renderProfilePanelFromContent("blank", "Blank", "Tab");
  }

  function renderProfilePanelFromContent(type, title, countLabel) {
    const panel = root.querySelector('[data-profile-panel="' + type + '"]');
    const records = profileContent[type] || [];
    if (!panel) return;
    if (type === "home") {
      panel.innerHTML = [
        profileTopSection("Home", "Profile", "", true),
        profileInlineTabsSection("home"),
        bottomNavOffset()
      ].join("");
      return;
    }
    if (type === "bio") {
      panel.innerHTML = [
        profileTopSection("Bio", "Profile", "", true),
        profileInlineTabsSection("bio"),
        bottomNavOffset()
      ].join("");
      return;
    }
    if (type === "contact") {
      panel.innerHTML = [
        profileTopSection("Contact", "Info", "", true),
        contactFormSection(),
        bottomNavOffset()
      ].join("");
      return;
    }
    if (type === "blank") {
      panel.innerHTML = [
        profileTopSection("Blank", "Tab", "", true),
        profileInlineTabsSection("blank"),
        bottomNavOffset()
      ].join("");
      return;
    }
    const inputRecords = records.filter((record) => String(record.kind || "").toLowerCase() !== "styling");
    const byField = new Map(records.map((record) => [String(record.field || "").toLowerCase(), record]));
    const h1Record = byField.get("name text") || byField.get("story title") || inputRecords[0];
    const pRecord = byField.get("class text") || byField.get("awards title") || inputRecords.find((record) => record !== h1Record);
    const h1 = h1Record?.note || title;
    const p = pRecord?.note || countLabel;
    const bodyRecord = byField.get("story content") || inputRecords.find((record) => record !== h1Record && record !== pRecord);
    const details = bodyRecord?.note || "";
    panel.innerHTML = [
      profileTopSection(h1, p, details, ["home", "bio", "contact"].includes(type))
    ].join("");
  }

  function profileContentRow(record) {
    const meta = [record.subtitle, record.body, record.tags.join(", ")].filter(Boolean).join(" - ");
    return [
      '<div class="lp-row is-static">',
      '<span>',
      '<span class="lp-row-title">' + escapeHtml(record.title || record.recordKey || "Untitled") + "</span>",
      meta ? '<span class="lp-row-meta">' + escapeHtml(meta) + "</span>" : "",
      "</span>",
      "</div>"
    ].join("");
  }

  function renderOverview() {
    renderedPanels.add("overview");
    const rangedClasses = currentClasses();
    const rangedCompetitions = currentCompetitions();
    const rangedVideos = currentVideos();
    const rangedHorses = currentHorses();
    const topHorses = overviewSubset("horses", rangedHorses, rangedHorses.filter((horse) => hasRibbon(horse.classes))).slice(0, 5);
    const overviewVideos = overviewSubset("videos", rangedVideos, rangedVideos).slice(0, 5);
    const recentCompetitions = filterOverviewItems(overviewSubset("competitions", rangedCompetitions, rangedCompetitions), overviewControls.competitions).slice(0, 5);
    const notableClasses = filterOverviewItems(overviewSubset("classes", rangedClasses, rangedClasses), overviewControls.classes).slice(0, 5);

    if (panels.overview) {
      panels.overview.innerHTML = [
        '<section class="lp-section-block lp-overview-section lp-theme-videos">',
        sectionTitle("Videos", overviewVideos.length + " shown", "", videoNavMarkup("overview")),
        videoCarousel(overviewVideos.slice(0, 5), "overview", { hideControls: true }),
        seeAll("videos", "More videos ->"),
        "</section>",
        '<section class="lp-section-block lp-overview-section lp-theme-horses">',
        sectionTitle("Horses", topHorses.length + " shown", "", videoNavMarkup("overview-horses")),
        horseCarousel(topHorses, "overview-horses"),
        seeAll("horses", "See all horses ->"),
        "</section>",
        '<section class="lp-section-block lp-overview-section lp-theme-competitions">',
        sectionTitle("Latest competitions", "", "", filterToggleMarkup("overview:competitions")),
        overviewControlsMarkup("competitions", rangedCompetitions),
        competitionCollection(recentCompetitions, viewControls.overviewCompetitions),
        seeAll("competitions", "See all competitions ->"),
        "</section>",
        '<section class="lp-section-block lp-overview-section lp-theme-classes">',
        sectionTitle("Latest classes", "", "", filterToggleMarkup("overview:classes")),
        overviewControlsMarkup("classes", rangedClasses),
        classCollection(notableClasses, viewControls.overviewClasses, { detailMode: "dateRange" }),
        seeAll("classes", "See all classes ->"),
        "</section>"
      ].join("");
    }
    renderRidingNestedPanels();
  }

  function renderRidingNestedPanels() {
    const section = root.querySelector('[data-nested-tabs="riding"]');
    if (!section) return;
    const videosPanel = section.querySelector('[data-nested-panel="videos"]');
    const horsesPanel = section.querySelector('[data-nested-panel="horses"]');
    if (videosPanel) {
      const videos = overviewSubset("videos", currentVideos(), currentVideos()).slice(0, 5);
      videosPanel.innerHTML = [
        '<section class="lp-section-block lp-theme-videos">',
        sectionTitle("Videos", videos.length + " shown", "", videoNavMarkup("riding-videos")),
        videoCarousel(videos, "riding-videos", { hideControls: true }),
        "</section>"
      ].join("");
    }
    if (horsesPanel) {
      const current = currentHorses();
      const ribbonHorses = current.filter((horse) => hasRibbon(horse.classes));
      const horses = overviewSubset("horses", current, ribbonHorses.length ? ribbonHorses : current).slice(0, 5);
      horsesPanel.innerHTML = [
        '<section class="lp-section-block lp-theme-horses">',
        sectionTitle("Horses", horses.length + " shown", "", videoNavMarkup("riding-horses")),
        horseCarousel(horses, "riding-horses"),
        "</section>"
      ].join("");
    }
    const competitionsPanel = section.querySelector('[data-nested-panel="competitions"]');
    if (competitionsPanel) {
      const competitions = filterOverviewItems(currentCompetitions(), allControls.competitions).slice(0, 12);
      competitionsPanel.innerHTML = [
        '<section class="lp-section-block lp-theme-competitions">',
        sectionTitle("Competitions", competitions.length + " shown", "", filterToggleMarkup("all:competitions")),
        competitionCollection(competitions, viewControls.competitions),
        "</section>"
      ].join("");
    }
    const classesPanel = section.querySelector('[data-nested-panel="classes"]');
    if (classesPanel) {
      const classes = filterOverviewItems(currentClasses(), allControls.classes).slice(0, 12);
      classesPanel.innerHTML = [
        '<section class="lp-section-block lp-theme-classes">',
        sectionTitle("Classes", classes.length + " shown", "", filterToggleMarkup("all:classes")),
        classCollection(classes, viewControls.classes),
        "</section>"
      ].join("");
    }
  }

  function renderHorses() {
    renderedPanels.add("horses");
    const baseHorses = currentHorses();
    const horses = filterHorses(baseHorses, allControls.horses);
    if (panels.horses) {
      panels.horses.innerHTML = [
        '<section class="lp-section-block lp-theme-horses">',
        sectionTitle("Horses", horses.length + " shown", "", filterToggleMarkup("all:horses")),
        allControlsMarkup("horses", baseHorses),
        horseGrid(horses),
        "</section>"
      ].join("");
    }
    renderHorseNestedPanels();
  }

  function renderHorseNestedPanels() {
    const section = root.querySelector('[data-nested-tabs="horses"]');
    if (!section) return;
    const horses = filterHorses(currentHorses(), allControls.horses);
    const groups = {
      ponies: horses.filter(isKnownPony),
      horses: horses.filter((horse) => !isKnownPony(horse)),
      owned: horses.filter(isOwnedHorse),
      hacked: horses.filter((horse) => !isOwnedHorse(horse))
    };
    Object.entries(groups).forEach(([key, rows]) => {
      const panel = section.querySelector('[data-nested-panel="' + key + '"]');
      if (!panel) return;
      panel.innerHTML = [
        '<section class="lp-section-block lp-theme-horses">',
        sectionTitle(key === "ponies" ? "Ponies" : key === "owned" ? "Owned" : key === "hacked" ? "Hacked" : "Horses", rows.length + " shown"),
        horseGrid(rows),
        "</section>"
      ].join("");
    });
  }

  function renderVideos() {
    renderedPanels.add("videos");
    const baseVideos = currentVideos();
    const favoriteVideos = favoriteSubset("videos", baseVideos, baseVideos).slice(0, 5);
    const videos = filterOverviewItems(baseVideos, allControls.videos);
    if (panels.videos) {
      panels.videos.innerHTML = [
        '<section class="lp-section-block lp-theme-videos">',
        sectionTitle("All videos", videos.length + " shown", "", filterToggleMarkup("all:videos")),
        allControlsMarkup("videos", baseVideos),
        videoGrid(videos),
        "</section>"
      ].join("");
    }
    renderVideoNestedPanels();
  }

  function renderVideoNestedPanels() {
    const section = root.querySelector('[data-nested-tabs="videos"]');
    if (!section) return;
    const videos = currentVideos();
    const groups = {
      featured: mockVideoSet(videos, 0),
      training: mockVideoSet(videos, 1),
      shows: mockVideoSet(videos, 2)
    };
    Object.entries(groups).forEach(([key, rows]) => {
      const panel = section.querySelector('[data-nested-panel="' + key + '"]');
      if (!panel) return;
      panel.innerHTML = [
        '<section class="lp-section-block lp-theme-videos">',
        sectionTitle(key === "featured" ? "Featured" : key === "training" ? "Training" : "Shows", rows.length + " shown"),
        videoGrid(rows),
        "</section>"
      ].join("");
    });
  }

  function mockVideoSet(videos, offset) {
    const source = videos.length ? videos : state.videos;
    return source.slice(offset, offset + 6);
  }

  function renderCompetitions() {
    renderedPanels.add("competitions");
    const baseCompetitions = currentCompetitions();
    const competitions = filterOverviewItems(baseCompetitions, allControls.competitions);
    panels.competitions.innerHTML = [
      '<section class="lp-section-block lp-theme-competitions">',
      sectionTitle("Competitions", "", "", filterToggleMarkup("all:competitions")),
      allControlsMarkup("competitions", baseCompetitions),
      competitionCollection(competitions, viewControls.competitions),
      "</section>"
    ].join("");
  }

  function renderClasses() {
    renderedPanels.add("classes");
    const baseClasses = currentClasses();
    const classes = filterOverviewItems(baseClasses, allControls.classes);
    panels.classes.innerHTML = [
      '<section class="lp-section-block lp-theme-classes">',
      sectionTitle("Classes", "", "", filterToggleMarkup("all:classes")),
      allControlsMarkup("classes", baseClasses),
      classCollection(classes, viewControls.classes),
      "</section>"
    ].join("");
  }

  function renderAllPanel(target) {
    if (target === "competitions") renderCompetitions();
    if (target === "classes") renderClasses();
    if (target === "videos") renderVideos();
    if (target === "horses") renderHorses();
  }

  function renderDataScope() {
    renderShell();
    renderedPanels.delete("overview");
    renderedPanels.delete("videos");
    renderedPanels.delete("horses");
    renderedPanels.delete("competitions");
    renderedPanels.delete("classes");
    renderOverview();
    renderRidingNestedPanels();
    if (root.querySelector('[data-panel="videos"]')?.classList.contains("is-active")) renderVideos();
    if (root.querySelector('[data-panel="horses"]')?.classList.contains("is-active")) renderHorses();
    if (root.querySelector('[data-panel="competitions"]')?.classList.contains("is-active")) renderCompetitions();
    if (root.querySelector('[data-panel="classes"]')?.classList.contains("is-active")) renderClasses();
  }

  function renderFilterScope(key) {
    const parts = String(key || "").split(":");
    const scope = parts[0];
    const target = parts[1];
    if (scope === "overview") {
      renderOverview();
      return;
    }
    if (scope === "all") {
      renderAllPanel(target);
    }
  }

  function renderViewTarget(target) {
    if (target.startsWith("overview")) renderOverview();
    if (target === "horses") renderHorses();
    if (target === "competitions") renderCompetitions();
    if (target === "classes") renderClasses();
  }

  function selectTab(tabName) {
    ensurePanelRendered(tabName);
    root.classList.toggle("is-overview-active", tabName === "overview");
    root.querySelectorAll("[data-tab]").forEach((tab) => {
      const isActive = tab.dataset.tab === tabName;
      tab.classList.toggle("is-active", isActive);
      tab.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    root.querySelectorAll("[data-panel]").forEach((panel) => {
      panel.classList.toggle("is-active", panel.dataset.panel === tabName);
    });
  }

  function selectProfileTab(tabName) {
    if (tabName === "horses") ensurePanelRendered("horses");
    if (tabName === "videos") ensurePanelRendered("videos");
    root.querySelectorAll("[data-profile-tab]").forEach((tab) => {
      const isActive = tab.dataset.profileTab === tabName;
      tab.classList.toggle("is-active", isActive);
      tab.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    root.querySelectorAll("[data-profile-panel]").forEach((panel) => {
      panel.classList.toggle("is-active", panel.dataset.profilePanel === tabName);
    });
    scrollProfileToTop();
  }

  function selectNestedTab(group, tabName) {
    const section = Array.from(root.querySelectorAll("[data-nested-tabs]")).find((item) => item.dataset.nestedTabs === group);
    if (!section) return;
    section.querySelectorAll("[data-nested-tab]").forEach((tab) => {
      const isActive = tab.dataset.nestedTab === tabName;
      tab.classList.toggle("is-active", isActive);
      tab.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    section.querySelectorAll("[data-nested-panel]").forEach((panel) => {
      panel.classList.toggle("is-active", panel.dataset.nestedPanel === tabName);
    });
  }

  function scrollProfileToTop() {
    requestAnimationFrame(() => {
      const top = root.getBoundingClientRect().top + window.pageYOffset;
      window.scrollTo({ top, behavior: "auto" });
    });
  }

  function ensurePanelRendered(tabName) {
    if (renderedPanels.has(tabName)) return;
    if (tabName === "videos") renderVideos();
    if (tabName === "horses") renderHorses();
    if (tabName === "competitions") renderCompetitions();
    if (tabName === "classes") renderClasses();
  }

  function openHorse(horseId) {
    const horse = currentHorses().find((item) => item.id === horseId) || state.horses.find((item) => item.id === horseId);
    if (!horse) return;
    const horseLayer = layerFor("horses", horse.id);
    const imageUrl = horseImage(horse);
    const head = [
      '<div class="lp-detail-head">',
      '<h3 id="lp-modal-title">' + escapeHtml(horse.name) + "</h3>",
      '<p class="lp-muted">USEF ' + escapeHtml(horse.id) + outboundLink(horse.link, "Horse profile") + "</p>",
      placementGrid(horse.classes),
      "</div>"
    ].join("");
    openModal([
      detailHero(horseDetailMedia(imageUrl), ""),
      head,
      '<div class="lp-metric-line">',
      miniMetric(horse.classes.length, "Classes"),
      miniMetric(horse.competitions.size, "Competitions"),
      "</div>",
      '<section class="lp-section-block" style="margin-top:16px">',
      sectionTitle("Classes", horse.classes.length + " rows"),
      classList(horse.classes, { showHorse: false }),
      "</section>",
      editDetail("horses", horse.id, [
        ["imageUrl", "Image URL", "url", horseLayer.imageUrl || ""],
        ["imageUrl_2", "Image URL 2", "url", horseLayer.imageUrl_2 || ""],
        ["image_upload", "Upload image", "file", ""],
        ["barn_name", "Barn name", "text", horseLayer.barn_name || ""],
        ["show_name", "Show name", "text", horseLayer.show_name || horse.name || ""],
        ["horseType", "Type", "single", horseLayer.horseType || "", ["Pony", "Horse"]],
        ["color", "Color", "single", horseLayer.color || "", ["Black", "Bay", "Chestnut", "Grey", "Paint", "Palomino", "Liverchestnut"]],
        ["gender", "Gender", "single", horseLayer.gender || "", ["Gelding", "Mare"]],
        ["disciplines", "Disciplines", "multi", Array.isArray(horseLayer.disciplines) ? horseLayer.disciplines : [], ["Hunters", "Jumpers", "Equitation"]],
        ["age", "Age", "number", horseLayer.age || ""],
        ["recordState", "Record state", "single", recordState("horses", horse.id), [["active", "Active"], ["inactive", "Inactive"]]],
        ["status", "Status", "multi", layerStatusValues("horses", horse.id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]]
      ])
    ].join(""));
  }

  function openCompetition(competitionId) {
    const competition = currentCompetitions().find((item) => item.competitionId === competitionId) || state.competitions.find((item) => item.competitionId === competitionId);
    if (!competition) return;
    const competitionLayer = layerFor("competitions", competition.competitionId);
    const rows = currentClasses().filter((row) => row.competitionId === competitionId);
    openModal([
      '<div class="lp-detail-head">',
      '<h3 id="lp-modal-title">' + escapeHtml(competition.competitionName) + "</h3>",
      '<p class="lp-muted">' + escapeHtml(dateRange(competition)) + "  -  " + escapeHtml(competition.state || "") + "  -  Zone " + escapeHtml(competition.zone || "") + outboundLink(competition.viewUrl, "Competition page") + "</p>",
      placementGrid(rows),
      "</div>",
      '<div class="lp-metric-line">',
      miniMetric(rows.length, "Classes"),
      miniMetric(unique(rows.map((row) => row.horseId)).length, "Horses"),
      "</div>",
      '<section class="lp-section-block" style="margin-top:16px">',
      sectionTitle("Classes", rows.length + " rows"),
      classList(rows, { showCompetition: false }),
      "</section>",
      editDetail("competitions", competition.competitionId, [
        ["recordState", "Record state", "single", recordState("competitions", competition.competitionId), [["active", "Active"], ["inactive", "Inactive"]]],
        ["status", "Status", "multi", layerStatusValues("competitions", competition.competitionId), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]],
        ["type", "Type", "multi", competitionLayer.type || [], ["Hunters", "Jumpers", "Equitation"]],
        ["tags", "Tags", "multi", competitionLayer.tags || [], ["seat", "maclay", "uset", "ushja", "wihs", "3'3\"", "3'6\"", "classic", "handy"]]
      ])
    ].join(""));
  }

  function openClass(classId) {
    const row = currentClasses().find((item) => item.id === classId) || state.classRows.find((item) => item.id === classId);
    if (!row) return;
    const classLayer = layerFor("classes", row.id);
    openModal([
      '<div class="lp-detail-head">',
      '<h3 id="lp-modal-title">' + escapeHtml(row.classTitle) + "</h3>",
      '<p class="lp-muted">' + escapeHtml(row.competitionName) + outboundLink(row.classUrl, "Class page") + "</p>",
      placementGrid([row]),
      "</div>",
      '<section class="lp-section-block lp-class-detail-list" style="margin-top:16px">',
      sectionTitle("Details"),
      staticRowList([
        ["Horse", escapeHtml(row.horse.name || "Unknown") + outboundLink(row.horse.link, "Horse profile")],
        ["USEF #", escapeHtml(row.horseId || "-")],
        ["Competition", escapeHtml(row.competitionName) + outboundLink(row.competition.viewUrl, "Competition page")],
        ["Date", escapeHtml(dateRange(row.competition))],
        ["Entries", escapeHtml(valueOrDash(row.entries))],
        ["USEF link", row.classUrl ? '<a class="lp-link" href="' + escapeAttr(row.classUrl) + '" target="_blank" rel="noopener noreferrer">usef</a>' : "-"]
      ]),
      "</section>",
      editDetail("classes", row.id, [
        ["recordState", "Record state", "single", recordState("classes", row.id), [["active", "Active"], ["inactive", "Inactive"]]],
        ["status", "Status", "multi", layerStatusValues("classes", row.id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]],
        ["type", "Type", "multi", classLayer.type || [], ["Hunters", "Jumpers", "Equitation"]],
        ["class_sequences", "Class sequences", "single", classLayer.class_sequences || "", ["Over Fences", "Under Saddle/Flat"]],
        ["tags", "Tags", "multi", classLayer.tags || [], ["seat", "maclay", "uset", "ushja", "wihs", "3'3\"", "3'6\"", "classic", "handy"]]
      ])
    ].join(""));
  }

  function openVideo(videoId) {
    const video = currentVideos().find((item) => item.id === videoId) || state.videos.find((item) => item.id === videoId);
    if (!video) return;
    const videoLayer = layerFor("videos", video.id);
    openModal([
      '<div class="lp-video-detail-media">',
      videoEmbed(video),
      "</div>",
      '<div class="lp-detail-head">',
      '<h3 id="lp-modal-title">' + escapeHtml(video.title) + "</h3>",
      '<p class="lp-muted">' + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</p>",
      "</div>",
      detailList([
        ["Time", escapeHtml(video.time)],
        ["Horse", escapeHtml(video.horse)],
        ["Competition", escapeHtml(video.competition)],
        ["Class", escapeHtml(video.classTitle)]
      ]),
      editDetail("videos", video.id, [
        ["videoUrl", "Video URL", "url", videoLayer.videoUrl || ""],
        ["embedUrl", "Embed URL", "url", videoLayer.embedUrl || ""],
        ["thumbnailUrl", "Thumbnail URL", "url", videoLayer.thumbnailUrl || ""],
        ["playlist", "Playlist", "url", videoLayer.playlist || "https://www.youtube.com/playlist?list=PLO6hUJNO-oM0BL4s8Y60jDoDr43q8T5tD"],
        ["recordState", "Record state", "single", recordState("videos", video.id), [["active", "Active"], ["inactive", "Inactive"]]],
        ["status", "Status", "multi", layerStatusValues("videos", video.id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]],
        ["tags", "Tags", "multi", videoLayer.tags || [], ["seat", "maclay", "uset", "ushja", "wihs", "3'3\"", "3'6\"", "classic", "handy"]]
      ])
    ].join(""));
  }

  function openModal(content) {
    modalContent.innerHTML = content;
    modal.hidden = false;
    document.documentElement.style.overflow = "hidden";
    modalCard.focus();
  }

  function closeModal() {
    modal.hidden = true;
    modalContent.innerHTML = "";
    document.documentElement.style.overflow = "";
  }

  function visibleItems(kind, items) {
    return items.filter((item) => {
      const id = itemId(kind, item);
      return isActiveRecord(kind, id) && !isIgnored(kind, id);
    });
  }

  function visibleClasses(rows) {
    return rows.filter((row) =>
      isActiveRecord("classes", row.id) &&
      isActiveRecord("horses", row.horseId) &&
      isActiveRecord("competitions", row.competitionId) &&
      !isIgnored("classes", row.id) &&
      !isIgnored("horses", row.horseId) &&
      !isIgnored("competitions", row.competitionId)
    );
  }

  function currentClasses() {
    return filterDataScope(visibleClasses(state.classRows));
  }

  function currentCompetitions() {
    const classCompetitionIds = new Set(currentClasses().map((row) => row.competitionId));
    return visibleItems("competitions", state.competitions)
      .filter((competition) => inSelectedYears(competition))
      .filter((competition) => matchesGlobalTags(competition) || state.classRows.some((row) => row.competitionId === competition.competitionId && inSelectedYears(row) && matchesGlobalTags(row)))
      .filter((competition) => classCompetitionIds.has(competition.competitionId));
  }

  function currentHorses() {
    return horsesForRows(currentClasses())
      .filter((horse) => isActiveRecord("horses", horse.id) && !isIgnored("horses", horse.id));
  }

  function isOwnedHorse(horse) {
    const name = normalizeText(horse.name);
    return name.includes("oddur") || name.includes("troubleshoot");
  }

  function isKnownPony(horse) {
    const layerType = normalizeText(layerFor("horses", horse.id).horseType || layerFor("horses", horse.id).type || horse.type || "");
    if (layerType === "pony") return true;
    if (layerType === "horse") return false;
    return horse.classes.some((row) => normalizeText([row.classTitle, row.sectionName].filter(Boolean).join(" ")).includes("pony"));
  }

  function normalizeText(value) {
    return String(value || "").trim().toLowerCase();
  }

  function currentVideos() {
    return visibleItems("videos", state.videos)
      .filter((video) => inSelectedYears(video))
      .filter((video) => matchesGlobalTags(video) || state.classRows.some((row) => row.id === video.classId && inSelectedYears(row) && matchesGlobalTags(row)));
  }

  function currentCounts() {
    return {
      horses: currentHorses().length,
      videos: currentVideos().length,
      competitions: currentCompetitions().length,
      classes: currentClasses().length
    };
  }

  function currentDateRange() {
    const items = [
      ...currentClasses(),
      ...currentCompetitions(),
      ...currentVideos()
    ].filter((item) => Number.isFinite(Number(item.sortDate)) && Number(item.sortDate) > 0);
    if (!items.length) return { start: "", end: "" };
    const dates = items.map((item) => item.sortDate);
    return {
      start: formatDate(new Date(Math.min(...dates))),
      end: formatDate(new Date(Math.max(...dates)))
    };
  }

  function itemId(kind, item) {
    if (kind === "horses") return item.id;
    if (kind === "competitions") return item.competitionId;
    if (kind === "classes") return item.id;
    if (kind === "videos") return item.id;
    return item.id;
  }

  function itemTitle(kind, item) {
    if (kind === "horses") return item.name;
    if (kind === "competitions") return item.competitionName;
    if (kind === "classes") return item.classTitle;
    if (kind === "videos") return item.title;
    return "Item";
  }

  function layerFor(kind, id) {
    const bucket = state.layer[kind] || (state.layer[kind] = {});
    return bucket[id] || (bucket[id] = {});
  }

  function layerStatus(kind, id) {
    const entry = state.layer[kind] && state.layer[kind][id];
    if (!entry) return "";
    if (entry.status) return Array.isArray(entry.status) ? entry.status[0] || "" : entry.status;
    if (entry.ignore) return "ignore";
    if (entry.favorite) return "favorite";
    if (entry.overview) return "overview";
    return "";
  }

  function layerStatusValues(kind, id) {
    const entry = state.layer[kind] && state.layer[kind][id];
    if (!entry) return [];
    if (Array.isArray(entry.status)) return entry.status;
    if (entry.status) return [entry.status];
    return [
      entry.overview ? "overview" : "",
      entry.favorite ? "favorite" : "",
      entry.ignore ? "ignore" : ""
    ].filter(Boolean);
  }

  function recordState(kind, id) {
    const entry = state.layer[kind] && state.layer[kind][id];
    if (!entry) return "active";
    if (entry.recordState) return entry.recordState;
    return "active";
  }

  function isActiveRecord(kind, id) {
    return recordState(kind, id) !== "inactive";
  }

  function isIgnored(kind, id) {
    return layerStatusValues(kind, id).includes("ignore");
  }

  function isFavorite(kind, id) {
    return layerStatusValues(kind, id).includes("favorite");
  }

  function isOverviewStatus(kind, id) {
    return layerStatusValues(kind, id).includes("overview");
  }

  function overviewSubset(kind, items, fallback) {
    const hasExplicitOverview = Object.keys(state.layer[kind] || {}).some((id) => isOverviewStatus(kind, id));
    if (!hasExplicitOverview) return fallback;
    return items.filter((item) => isOverviewStatus(kind, itemId(kind, item)));
  }

  function favoriteSubset(kind, items, fallback) {
    const selected = items.filter((item) => isFavorite(kind, itemId(kind, item)));
    return selected.length ? selected : fallback;
  }

  function layerInputValue(input) {
    if (input.type === "checkbox") return input.checked;
    if (input.type === "radio") return input.checked ? input.value : "";
    return input.value;
  }

  function setLayerValue(kind, id, field, value) {
    if (!editMode || !kind || !id || !field) return;
    const entry = layerFor(kind, id);
    if (field === "status") {
      delete entry.overview;
      delete entry.favorite;
      delete entry.ignore;
    }
    if (field === "recordState") {
      delete entry.active;
      delete entry.inactive;
    }
    if (value === false || value === "" || value === null || value === undefined) {
      delete entry[field];
    } else {
      entry[field] = value;
    }
    state.layer.updatedAt = new Date().toISOString();
    persistLayer();
    queueEnrichmentSave(kind, id);
  }

  function favoriteMarker(kind, id) {
    return isFavorite(kind, id) ? '<span class="lp-status-icon" aria-label="Favorite" title="Favorite"></span>' : "";
  }

  function titleWithStatus(label, kind, id) {
    return '<span class="lp-row-title">' + escapeHtml(label) + favoriteMarker(kind, id) + "</span>";
  }

  function setLayerMultiValue(kind, id, field, value, checked) {
    if (!editMode || !kind || !id || !field) return;
    const entry = layerFor(kind, id);
    if (field === "status") {
      delete entry.overview;
      delete entry.favorite;
      delete entry.ignore;
    }
    const existing = Array.isArray(entry[field]) ? entry[field] : [];
    const next = checked
      ? unique(existing.concat(value))
      : existing.filter((item) => item !== value);
    if (next.length) {
      entry[field] = next;
    } else {
      delete entry[field];
    }
    state.layer.updatedAt = new Date().toISOString();
    persistLayer();
    queueEnrichmentSave(kind, id);
  }

  function handleLayerFile(input) {
    if (!editMode || !input.files || !input.files[0]) return;
    const file = input.files[0];
    if (!file.type || !file.type.startsWith("image/")) {
      updateEditStatus("Choose an image file.");
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      setLayerValue(input.dataset.layerKind, input.dataset.layerId, input.dataset.layerField, String(reader.result || ""));
      renderLayerScope(input.dataset.layerKind);
      updateEditStatus("Saving image to Airtable...");
    };
    reader.onerror = () => updateEditStatus("Image upload could not be read.");
    reader.readAsDataURL(file);
  }

  function persistLayer() {
    try {
      window.localStorage.setItem(layerStorageKey, JSON.stringify(state.layer));
    } catch (error) {
      updateEditStatus("Could not save draft in this browser.");
    }
  }

  function queueEnrichmentSave(kind, id) {
    if (!editMode || !kind || !id) {
      debug("enrichment queue skipped", { editMode, kind, id });
      return;
    }
    if (!enrichmentUrl && !config.airtable) {
      debug("enrichment queue skipped: no save endpoint", { kind, id });
      return;
    }
    const key = kind + ":" + id;
    window.clearTimeout(saveTimers.get(key));
    saveTimers.set(key, window.setTimeout(() => saveEnrichment(kind, id), 450));
    updateEditStatus("Saving to Airtable...");
  }

  async function saveEnrichment(kind, id) {
    const payload = enrichmentPayload(kind, id);
    if (!payload) return;
    debug("enrichment save", payload.recordType, payload.recordKey);
    try {
      if (config.airtable) {
        await saveEnrichmentToAirtable(payload);
        updateEditStatus("Saved to Airtable at " + statusTime() + ".");
        return;
      }

      const headers = {
        "Content-Type": "application/json"
      };
      if (editKey) headers["X-Edit-Key"] = editKey;
      debug("enrichment post", { url: enrichmentUrl, recordKey: payload.recordKey, recordType: payload.recordType });
      const response = await fetch(enrichmentUrl, {
        method: "POST",
        headers,
        body: JSON.stringify(payload)
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || !result.ok) {
        throw new Error("Enrichment save failed: " + response.status + " " + JSON.stringify(result));
      }
      debug("enrichment saved", result.record);
      updateEditStatus("Saved to Airtable at " + statusTime() + saveActionLabel(result.record, result.log) + ".");
    } catch (error) {
      console.error("[lp-history] enrichment save failed", error);
      updateEditStatus("Saved in browser only. Airtable save failed at " + statusTime() + "; check console [lp-history].");
    }
  }

  async function saveEnrichmentToAirtable(payload) {
    const airtable = config.airtable || {};
    const baseId = airtable.baseId;
    const tableName = airtable.tableName;
    const token = airtable.token;
    if (!baseId || !tableName || !token) {
      throw new Error("Missing Airtable config: baseId, tableName, and token are required.");
    }

    const fields = compactFields({
      ...(payload.data || {}),
      record_key: payload.recordKey,
      record_type: payload.recordType,
      record_state: payload.recordState || "active",
      status: payload.status || [],
      payload_json: JSON.stringify(payload.data || {}),
      raw_payload: JSON.stringify(payload),
      updated_at: new Date().toISOString()
    });
    const baseUrl = "https://api.airtable.com/v0/" + encodeURIComponent(baseId) + "/" + encodeURIComponent(tableName);
    const headers = {
      Authorization: "Bearer " + token,
      "Content-Type": "application/json"
    };
    const formula = "{record_key} = " + airtableFormulaString(payload.recordKey);
    const lookupUrl = baseUrl + "?maxRecords=1&filterByFormula=" + encodeURIComponent(formula);
    const lookup = await fetch(lookupUrl, { headers });
    const lookupJson = await lookup.json().catch(() => ({}));
    if (!lookup.ok) {
      throw new Error("Airtable lookup failed: " + lookup.status + " " + JSON.stringify(lookupJson));
    }

    const existingId = lookupJson.records?.[0]?.id;
    const saveUrl = existingId ? baseUrl + "/" + encodeURIComponent(existingId) : baseUrl;
    const save = await fetch(saveUrl, {
      method: existingId ? "PATCH" : "POST",
      headers,
      body: JSON.stringify(existingId ? { fields } : { records: [{ fields }] })
    });
    const saveJson = await save.json().catch(() => ({}));
    if (!save.ok) {
      throw new Error("Airtable save failed: " + save.status + " " + JSON.stringify(saveJson));
    }
    debug("airtable saved", existingId ? existingId : saveJson.records?.[0]?.id);
  }

  function compactFields(fields) {
    return Object.fromEntries(Object.entries(fields).filter(([, value]) => (
      value !== undefined &&
      value !== null &&
      value !== "" &&
      (!Array.isArray(value) || value.length > 0)
    )));
  }

  function airtableFormulaString(value) {
    return '"' + String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
  }

  function enrichmentPayload(kind, id) {
    const recordType = singularKind(kind);
    const layer = { ...layerFor(kind, id) };
    const base = {
      kind: "lp-history",
      source: "lp-history-webflow",
      source_id: id,
      record_state: recordState(kind, id),
      status: layerStatusValues(kind, id)
    };

    if (kind === "horses") {
      const horse = state.horses.find((item) => item.id === id) || {};
      return payloadFor(recordType, id, {
        ...base,
        horse: horse.name || layer.show_name || id,
        barn_name: layer.barn_name || "",
        show_name: layer.show_name || horse.name || "",
        horse_type: layer.horseType || "",
        horse_disciplines: layer.disciplines || [],
        horse_color: layer.color || "",
        horse_gender: layer.gender || "",
        horse_age: layer.age || "",
        image_url: layer.imageUrl || layer.imageUrl_2 || "",
        notes: layer.notes || ""
      });
    }

    if (kind === "competitions") {
      const competition = state.competitions.find((item) => item.competitionId === id) || {};
      return payloadFor(recordType, id, {
        ...base,
        competition: competition.competitionName || id,
        competition_type: layer.type || [],
        tags: layer.tags || [],
        notes: layer.notes || ""
      });
    }

    if (kind === "classes") {
      const row = state.classRows.find((item) => item.id === id) || {};
      return payloadFor(recordType, id, {
        ...base,
        class: row.classTitle || id,
        horse: row.horse?.name || "",
        competition: row.competitionName || "",
        class_type: layer.type || [],
        class_sequence: layer.class_sequences || "",
        tags: layer.tags || [],
        notes: layer.notes || ""
      });
    }

    if (kind === "videos") {
      const video = state.videos.find((item) => item.id === id) || {};
      return payloadFor(recordType, id, {
        ...base,
        video: video.title || id,
        horse: video.horse || "",
        competition: video.competition || "",
        class: video.classTitle || "",
        video_url: layer.videoUrl || "",
        embed_url: layer.embedUrl || "",
        thumbnail_url: layer.thumbnailUrl || "",
        playlist: layer.playlist || "",
        tags: layer.tags || [],
        notes: layer.notes || ""
      });
    }

    return null;
  }

  function payloadFor(recordType, id, data) {
    return {
      recordType,
      recordKey: recordType + ":" + id,
      recordState: data.record_state || "active",
      status: data.status || [],
      data
    };
  }

  function singularKind(kind) {
    return { horses: "horse", competitions: "competition", classes: "class", videos: "video" }[kind] || kind;
  }

  function handleLayerAction(button) {
    if (!editMode) return;
    const action = button.dataset.layerAction;
    if (action === "export") {
      downloadLayer();
      updateEditStatus("Exported layer.json.");
      return;
    }
    if (action === "copy") {
      copyLayer(button);
      return;
    }
    if (action === "clear-draft") {
      window.localStorage.removeItem(layerStorageKey);
      updateEditStatus("Local draft cleared. Reload to restore the repo layer.");
    }
  }

  function downloadLayer() {
    const blob = new Blob([JSON.stringify(state.layer, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "lp-history-layer.json";
    link.click();
    URL.revokeObjectURL(url);
  }

  async function copyLayer(button) {
    const text = JSON.stringify(state.layer, null, 2);
    try {
      await navigator.clipboard.writeText(text);
      updateEditStatus("Copied layer JSON.");
    } catch (error) {
      button.closest(".lp-edit-panel, .lp-edit-detail")?.querySelector("[data-layer-output]")?.removeAttribute("hidden");
      updateEditStatus("Clipboard blocked. Copy the visible JSON.");
    }
  }

  function updateEditStatus(message) {
    root.querySelectorAll("[data-edit-status]").forEach((status) => {
      status.textContent = message;
    });
  }

  function initialEditStatus() {
    if (config.airtable) return "Changes save to Airtable.";
    if (enrichmentUrl) return "Changes save to Airtable through Webflow Cloud.";
    return "Changes save in this browser.";
  }

  function statusTime() {
    return new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
  }

  function saveActionLabel(record, log) {
    const parts = [];
    if (record && record.action) parts.push(record.action);
    if (log && log.action) parts.push(log.action);
    return parts.length ? " (" + parts.join(", ") + ")" : "";
  }

  function renderLayerScope(kind) {
    if (kind === "horses") renderHorses();
    if (kind === "competitions") renderCompetitions();
    if (kind === "classes") renderClasses();
    if (kind === "videos") renderVideos();
    renderOverview();
  }

  function editPanel(kind, items) {
    if (!editMode) return "";
    const rows = items.map((item) => {
      const id = itemId(kind, item);
      return [
        '<div class="lp-edit-row' + (isIgnored(kind, id) ? " lp-ignored" : "") + '">',
        '<div class="lp-edit-title">' + escapeHtml(itemTitle(kind, item)) + "</div>",
        singleField(kind, id, "recordState", recordState(kind, id), [["active", "Active"], ["inactive", "Inactive"]]),
        multiField(kind, id, "status", layerStatusValues(kind, id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]),
        "</div>"
      ].join("");
    }).join("");
    return [
      '<section class="lp-edit-panel" aria-label="' + escapeAttr(kind) + ' edit layer">',
      '<div class="lp-edit-head"><h4>Edit ' + escapeHtml(kind) + '</h4><div class="lp-edit-actions">',
      '<button class="lp-edit-button" type="button" data-layer-action="copy">Copy JSON</button>',
      '<button class="lp-edit-button" type="button" data-layer-action="clear-draft">Clear draft</button>',
      "</div></div>",
      '<div class="lp-edit-rows">',
      rows,
      "</div>",
      '<p class="lp-edit-status" data-edit-status>' + escapeHtml(initialEditStatus()) + "</p>",
      '<textarea class="lp-edit-textarea" data-layer-output hidden readonly>' + escapeHtml(JSON.stringify(state.layer, null, 2)) + "</textarea>",
      "</section>"
    ].join("");
  }

  function editDetail(kind, id, fields) {
    if (!editMode) return "";
    return [
      '<section class="lp-edit-detail lp-edit-panel">',
      '<div class="lp-edit-head"><h4>Enrichment</h4></div>',
      '<div class="lp-edit-grid">',
      editGroups(kind, id, fields),
      "</div>",
      '<p class="lp-edit-status" data-edit-status>' + escapeHtml(initialEditStatus()) + "</p>",
      "</section>"
    ].join("");
  }

  function editGroups(kind, id, fields) {
    const grouped = [
      ["Record state", fields.filter(([field]) => field === "recordState")],
      ["Status", fields.filter(([field]) => field === "status")],
      ["Media", fields.filter(([field]) => ["imageUrl", "imageUrl_2", "image_upload", "videoUrl", "embedUrl", "thumbnailUrl", "playlist"].includes(field))],
      ["Profile", fields.filter(([field]) => ["barn_name", "show_name", "horseType", "color", "gender", "disciplines", "age"].includes(field))],
      ["Type", fields.filter(([field]) => field === "type")],
      ["Class sequence", fields.filter(([field]) => field === "class_sequences")],
      ["Tags", fields.filter(([field]) => field === "tags")],
      ["Notes", fields.filter(([field]) => field === "notes" || field === "favoriteLabel")]
    ];
    return grouped
      .filter(([, groupFields]) => groupFields.length)
      .map(([title, groupFields]) => editGroupRows(kind, id, title, groupFields))
      .join("");
  }

  function editGroupRows(kind, id, title, fields) {
    if (title === "Status") {
      return editRow(title, fields.map(([field, label, type, value, choices]) => editControl(kind, id, field, label, type, value, choices)).join(""));
    }
    if (fields.length === 1 && fields[0][2] === "multi") {
      const [field, label, type, value, choices] = fields[0];
      return editRow(title || label, editControl(kind, id, field, label, type, value, choices));
    }
    return fields.map(([field, label, type, value, choices]) => editRow(label, editControl(kind, id, field, label, type, value, choices))).join("");
  }

  function editRow(label, control) {
    return [
      '<div class="lp-row is-static is-detail lp-edit-row-field">',
      '<span class="lp-row-title">' + escapeHtml(label) + "</span>",
      '<span class="lp-row-meta lp-edit-row-value">' + control + "</span>",
      "</div>"
    ].join("");
  }

  function editControl(kind, id, field, label, type, value, choices = []) {
    const isTextArea = type === "textarea";
    const isCheckbox = type === "checkbox";
    const isMulti = type === "multi";
    const isSingle = type === "single";
    const isFile = type === "file";
    const attrs = ' data-layer-field="' + escapeAttr(field) + '" data-layer-kind="' + escapeAttr(kind) + '" data-layer-id="' + escapeAttr(id) + '"';
    if (isMulti) return multiField(kind, id, field, Array.isArray(value) ? value : [], choices);
    if (isSingle) return singleField(kind, id, field, value || "", choices);
    if (isCheckbox) return '<label class="lp-edit-checkbox"><input type="checkbox"' + attrs + (value ? " checked" : "") + '><span class="lp-edit-pill">' + escapeHtml(label) + "</span></label>";
    if (isFile) return '<input class="lp-edit-input" type="file" accept="image/*"' + attrs + ">";
    if (isTextArea) return '<textarea class="lp-edit-textarea"' + attrs + ">" + escapeHtml(value || "") + "</textarea>";
    return '<input class="lp-edit-input" type="' + escapeAttr(type || "text") + '" value="' + escapeAttr(value || "") + '"' + attrs + ">";
  }

  function multiField(kind, id, field, selected, choices) {
    return [
      '<span class="lp-edit-choice-row">',
      choices.map((choice) => {
        const value = Array.isArray(choice) ? choice[0] : choice;
        const label = Array.isArray(choice) ? choice[1] : choice;
        return [
          '<label class="lp-edit-choice">',
          '<input type="checkbox" data-layer-multi="true" data-layer-field="' + escapeAttr(field) + '" data-layer-kind="' + escapeAttr(kind) + '" data-layer-id="' + escapeAttr(id) + '" value="' + escapeAttr(value) + '"' + (selected.includes(value) ? " checked" : "") + ">",
          '<span class="lp-edit-pill">' + escapeHtml(label) + "</span>",
          "</label>"
        ].join("");
      }).join(""),
      "</span>"
    ].join("");
  }

  function singleField(kind, id, field, selected, choices) {
    const name = "lp-" + slugify(kind + "-" + id + "-" + field) + "-" + (++singleFieldCounter);
    const selectedValue = Array.isArray(selected) ? (selected[0] || "") : selected;
    return [
      '<span class="lp-edit-choice-row">',
      choices.map((choice) => {
        const value = Array.isArray(choice) ? choice[0] : choice;
        const label = Array.isArray(choice) ? choice[1] : choice;
        return [
          '<label class="lp-edit-choice">',
          '<input type="radio" data-layer-field="' + escapeAttr(field) + '" data-layer-kind="' + escapeAttr(kind) + '" data-layer-id="' + escapeAttr(id) + '" name="' + escapeAttr(name) + '" value="' + escapeAttr(value) + '"' + (selectedValue === value ? " checked" : "") + ">",
          '<span class="lp-edit-pill">' + escapeHtml(label) + "</span>",
          "</label>"
        ].join("");
      }).join(""),
      "</span>"
    ].join("");
  }

  function classTable(rows, options = {}) {
    if (!rows.length) return '<p class="lp-empty">No class rows available.</p>';
    const showHorse = options.showHorse !== false;
    const showCompetition = options.showCompetition !== false;
    const headers = ["Class"];
    if (showHorse) headers.push("Horse");
    if (showCompetition) headers.push("Competition");
    headers.push("Result");
    return [
      '<table class="lp-table"><thead><tr>',
      headers.map((header) => "<th>" + escapeHtml(header) + "</th>").join(""),
      "</tr></thead><tbody>",
      rows.map((row) => {
        const cells = [
          '<td><button class="lp-class-button" type="button" data-open-class="' + escapeAttr(row.id) + '">' + escapeHtml(row.classTitle) + "</button></td>"
        ];
        if (showHorse) {
          cells.push('<td><button class="lp-class-button" type="button" data-open-horse="' + escapeAttr(row.horseId) + '">' + escapeHtml(row.horse.name || "Unknown") + "</button></td>");
        }
        if (showCompetition) {
          cells.push('<td><button class="lp-class-button" type="button" data-open-competition="' + escapeAttr(row.competitionId) + '">' + escapeHtml(row.competitionName) + '</button><br><span class="lp-row-meta">' + escapeHtml(dateRange(row.competition)) + "</span></td>");
        }
        cells.push("<td>" + placementToken(row) + '<span class="lp-row-meta">' + escapeHtml(valueOrDash(row.entries)) + " entries  -  #" + escapeHtml(valueOrDash(row.backNumber)) + "</span></td>");
        return "<tr>" + cells.join("") + "</tr>";
      }).join(""),
      "</tbody></table>"
    ].join("");
  }

  function classDetailTable(row) {
    return [
      '<table class="lp-table"><tbody>',
      detailTableRow("Result", placementToken(row) || ""),
      detailTableRow("Entries", escapeHtml(valueOrDash(row.entries))),
      detailTableRow("Back #", escapeHtml(valueOrDash(row.backNumber))),
      detailTableRow("Horse", '<button class="lp-class-button" type="button" data-open-horse="' + escapeAttr(row.horseId) + '">' + escapeHtml(row.horse.name || "Unknown") + '</button><br><span class="lp-row-meta">USEF ' + escapeHtml(row.horseId) + "</span>" + outboundLink(row.horse.link, "Horse profile")),
      detailTableRow("Competition", '<button class="lp-class-button" type="button" data-open-competition="' + escapeAttr(row.competitionId) + '">' + escapeHtml(row.competitionName) + '</button><br><span class="lp-row-meta">' + escapeHtml(dateRange(row.competition)) + "  -  " + escapeHtml(row.competition.state || "") + "  -  Zone " + escapeHtml(row.competition.zone || "") + "</span>" + outboundLink(row.competition.viewUrl, "Competition page")),
      detailTableRow("Class code", escapeHtml(valueOrDash(row.classCode))),
      detailTableRow("USEF link", row.classUrl ? '<a class="lp-link" href="' + escapeAttr(row.classUrl) + '" target="_blank" rel="noopener noreferrer">usef</a>' : "-"),
      "</tbody></table>"
    ].join("");
  }

  function detailTableRow(label, value) {
    return "<tr><th>" + escapeHtml(label) + "</th><td>" + value + "</td></tr>";
  }

  function classList(rows, options = {}) {
    if (!rows.length) return '<p class="lp-empty">No class rows available.</p>';
    const showHorse = options.showHorse !== false;
    const showCompetition = options.showCompetition !== false;
    return rows.map((row) => {
      const meta = [];
      if (showHorse) meta.push(escapeHtml(row.horse.name || "Unknown"));
      if (showCompetition) meta.push(escapeHtml(row.competitionName));
      const detail = escapeHtml(dateRange(row.competition));
      return rowWithActions("classes", row.id, [
        '<button class="lp-row" type="button" data-open-class="' + escapeAttr(row.id) + '">',
        "<span>",
        titleWithStatus(row.classTitle, "classes", row.id),
        meta.length ? '<span class="lp-row-meta">' + meta.join("  -  ") + "</span>" : "",
        '<span class="lp-row-meta">' + detail + "</span>",
        "</span>",
        ribbonForRow(row),
        "</button>"
      ].join(""));
    }).join("");
  }

  function horseCollection(horses) {
    if (!horses.length) return '<p class="lp-empty">No horses available.</p>';
    return horses.map(compactHorseButton).join("");
  }

  function horseGrid(horses) {
    if (!horses.length) return '<p class="lp-empty">No horses available.</p>';
    return '<div class="lp-horse-grid">' + horses.map(horseCard).join("") + "</div>";
  }

  function horseCarousel(horses, scope) {
    if (!horses.length) return '<p class="lp-empty">No ribboned horses available.</p>';
    return [
      '<div class="lp-video-shell lp-horse-carousel-shell">',
      '<div class="lp-video-rail lp-horse-rail" data-video-rail="' + escapeAttr(scope) + '">',
      horses.map(horseCard).join(""),
      "</div>",
      "</div>"
    ].join("");
  }

  function competitionCollection(competitions, view) {
    if (!competitions.length) return '<p class="lp-empty">No competitions available.</p>';
    if (view === "grid") {
      return '<div class="lp-card-grid">' + competitions.map(competitionCard).join("") + "</div>";
    }
    return competitions.map((competition) => competitionRow(competition)).join("");
  }

  function classCollection(rows, view, options = {}) {
    if (!rows.length) return '<p class="lp-empty">No class rows available.</p>';
    if (view === "grid") return '<div class="lp-card-grid">' + rows.map(classCard).join("") + "</div>";
    return classList(rows, options);
  }

  function overviewControlsMarkup(target, items) {
    const controls = overviewControls[target];
    return controlsMarkup("overview", target, items, controls, { hideHead: true });
  }

  function allControlsMarkup(target, items) {
    const controls = allControls[target];
    const hideHead = target === "competitions" || target === "classes" || target === "videos" || target === "horses";
    return controlsMarkup("all", target, items, controls, { hideHead });
  }

  function controlsMarkup(scope, target, items, controls, options = {}) {
    const key = scope + ":" + target;
    const isOpen = !!sectionFilterState[key];
    const isHorseFilter = target === "horses";
    return [
      '<div class="lp-section-filter">',
      options.hideHead ? "" : [
      '<div class="lp-section-filter-head">',
      filterToggleMarkup(key),
      "</div>"
      ].join(""),
      '<div class="lp-overview-controls" data-section-filter-panel="' + escapeAttr(key) + '"' + (isOpen ? "" : " hidden") + ">",
      '<button class="lp-section-filter-close" type="button" data-section-filter-close="' + escapeAttr(key) + '" aria-label="Close filter">x</button>',
      isHorseFilter ? [
        '<input class="lp-control lp-control-input" type="search" data-' + scope + '-filter="' + escapeAttr(target) + '" data-filter-field="search" value="' + escapeAttr(controls.search || "") + '" placeholder="Search horse name" aria-label="Search horses by name">',
        '<select class="lp-control lp-control-select" data-' + scope + '-filter="' + escapeAttr(target) + '" data-filter-field="type" aria-label="Filter horses by type">',
        option("all", "All types", controls.type),
        horseTypeOptions(items, controls.type),
        "</select>"
      ].join("") : "",
      '<button class="lp-control lp-control-button" type="button" data-' + scope + '-sort="' + escapeAttr(target) + '">' + (controls.sort === "desc" ? "Sort down" : "Sort up") + "</button>",
      '<select class="lp-control lp-control-select" data-' + scope + '-filter="' + escapeAttr(target) + '" data-filter-field="month" aria-label="Filter ' + escapeAttr(target) + ' by month">',
      option("all", "All months", controls.month),
      monthOptions(items, controls.month),
      "</select>",
      '<select class="lp-control lp-control-select" data-' + scope + '-filter="' + escapeAttr(target) + '" data-filter-field="year" aria-label="Filter ' + escapeAttr(target) + ' by year">',
      option("all", "All years", controls.year),
      yearOptions(items, controls.year),
      "</select>",
      '<button class="lp-control lp-control-button" type="button" data-' + scope + '-clear="' + escapeAttr(target) + '">Clear filters</button>',
      "</div>",
      "</div>"
    ].join("");
  }

  function filterOverviewItems(items, controls) {
    return [...items]
      .filter((item) => {
        const date = new Date(item.sortDate || 0);
        const monthOk = controls.month === "all" || String(date.getMonth() + 1) === controls.month;
        const yearOk = controls.year === "all" || String(date.getFullYear()) === controls.year;
        return monthOk && yearOk;
      })
      .sort((a, b) => controls.sort === "desc" ? b.sortDate - a.sortDate : a.sortDate - b.sortDate);
  }

  function filterHorses(horses, controls) {
    const search = String(controls.search || "").trim().toLowerCase();
    return [...horses]
      .filter((horse) => {
        const nameOk = !search || horse.name.toLowerCase().includes(search);
        const typeOk = controls.type === "all" || horse.type === controls.type;
        const dateOk = horse.classes.some((row) => {
          const date = new Date(row.sortDate || 0);
          const monthOk = controls.month === "all" || String(date.getMonth() + 1) === controls.month;
          const yearOk = controls.year === "all" || String(date.getFullYear()) === controls.year;
          return monthOk && yearOk;
        });
        return nameOk && typeOk && dateOk;
      })
      .sort((a, b) => controls.sort === "desc" ? b.sortDate - a.sortDate : a.sortDate - b.sortDate);
  }

  function filterDataScope(items) {
    return items.filter((item) => inSelectedYears(item) && matchesGlobalTags(item));
  }

  function filterOverviewRange(items) {
    return filterDataScope(items);
  }

  function inSelectedYears(item) {
    const selected = Array.from(overviewYears);
    if (!selected.length) return true;
    const date = new Date(item.sortDate || 0);
    if (!Number.isFinite(date.getTime())) return false;
    return selected.includes(String(date.getFullYear()));
  }

  function matchesGlobalTags(item) {
    const selected = Array.from(globalTags);
    if (!selected.length) return true;
    const tags = itemTags(item);
    return selected.some((tag) => tags.has(tag));
  }

  function itemTags(item) {
    const values = new Set();
    if (!item) return values;
    collectTagText(values, item.autoTags);
    collectTagText(values, item.layer?.tags);
    collectTagText(values, item.layer?.autoTags);
    collectTagText(values, item.layer?.type);
    collectTagText(values, item.layer?.class_sequences);
    collectTagText(values, item.layer?.disciplines);
    collectTagText(values, item.layer?.horseType);
    collectAutoTags(values, [
      item.classTitle,
      item.competitionName,
      item.name,
      item.title,
      item.horse?.name,
      item.horse,
      item.competition,
      item.classTitle
    ].filter(Boolean).join(" "));
    if (item.classes) {
      item.classes.forEach((row) => itemTags(row).forEach((tag) => values.add(tag)));
    }
    return values;
  }

  function collectTagText(values, raw) {
    const list = Array.isArray(raw) ? raw : raw ? [raw] : [];
    list.forEach((value) => {
      const text = normalizeTag(value);
      if (text) values.add(text);
    });
  }

  function collectAutoTags(values, text) {
    const haystack = String(text || "");
    globalTagRules.forEach((rule) => {
      const name = normalizeTag(rule.name_tag);
      if (!name) return;
      if (rule.match_type === "regex" && rule.match_pattern) {
        try {
          if (new RegExp(rule.match_pattern, "i").test(haystack)) values.add(name);
        } catch (error) {}
      }
    });
  }

  function normalizeTag(value) {
    return String(value || "").trim().toLowerCase();
  }

  function globalTagOptions() {
    const tagClassByName = new Map();
    globalTagRules.forEach((rule) => {
      const name = normalizeTag(rule.name_tag);
      if (name) tagClassByName.set(name, normalizeTag(rule.tag_class) || "tag");
    });
    const fromRules = Array.from(tagClassByName.keys());
    const fromLayer = [
      ...Object.values(state.layer.horses || {}),
      ...Object.values(state.layer.competitions || {}),
      ...Object.values(state.layer.classes || {}),
      ...Object.values(state.layer.videos || {})
    ].flatMap((entry) => [
      ...(Array.isArray(entry.tags) ? entry.tags : []),
      ...(Array.isArray(entry.autoTags) ? entry.autoTags : [])
    ].map(normalizeTag).filter(Boolean));
    return unique([...fromRules, ...fromLayer]).sort().slice(0, 48).map((name) => ({
      name,
      tagClass: tagClassByName.get(name) || "manual"
    }));
  }

  function tagStyle(tagClass) {
    const color = tagClassColor(tagClass);
    return "--lp-tag-bg:" + hexToRgba(color, 0.12) + ";--lp-tag-border:" + hexToRgba(color, 0.24) + ";--lp-tag-active:" + color + ";--lp-tag-active-text:" + tagClassTextColor(color) + ";";
  }

  function tagClassColor(tagClass) {
    const palette = {
      age: "#6c5ce7",
      type: "#0f766e",
      key: "#0057B8",
      height: "#8B5A2B",
      size: "#b45309",
      level: "#D71920",
      skill: "#4e1f76",
      target: "#00843D",
      class_types: "#005c2a",
      class_sequences: "#8f1116",
      manual: "#46332b",
      tag: "#46332b"
    };
    return palette[normalizeTag(tagClass)] || "#46332b";
  }

  function tagClassTextColor(hex) {
    const value = String(hex || "").replace("#", "");
    const number = parseInt(value, 16);
    if (!Number.isFinite(number)) return "#ffffff";
    const r = (number >> 16) & 255;
    const g = (number >> 8) & 255;
    const b = number & 255;
    return ((r * 299 + g * 587 + b * 114) / 1000) > 150 ? "#000000" : "#ffffff";
  }

  function horsesForRows(rows) {
    const map = new Map();
    rows.forEach((row) => {
      const horseId = row.horseId || row.horse?.usefHorseId || row.horse?.name || "unknown-horse";
      if (!map.has(horseId)) {
        map.set(horseId, {
          id: horseId,
          name: row.horse?.name || "Unknown horse",
          link: row.horse?.link,
          classes: [],
          competitions: new Map()
        });
      }
      const horse = map.get(horseId);
      horse.classes.push(row);
      horse.competitions.set(row.competitionId, row.competition);
    });
    return Array.from(map.values()).sort((a, b) => {
      if (b.classes.length !== a.classes.length) return b.classes.length - a.classes.length;
      return a.name.localeCompare(b.name);
    });
  }

  function overviewYearOptions() {
    const years = unique([
      ...state.classRows,
      ...state.competitions,
      ...state.videos
    ].map((item) => {
      const date = new Date(item.sortDate || 0);
      return Number.isFinite(date.getTime()) ? String(date.getFullYear()) : null;
    }).filter(Boolean)).sort((a, b) => Number(b) - Number(a));
    return years.length ? years : ["2026", "2025"];
  }

  function defaultOverviewYears() {
    const available = new Set(overviewYearOptions());
    const defaults = ["2026", "2025"].filter((year) => available.has(year));
    return defaults.length ? defaults : overviewYearOptions().slice(0, 2);
  }

  function monthOptions(items, selected) {
    const months = unique(items.map((item) => {
      const date = new Date(item.sortDate || 0);
      return Number.isFinite(date.getTime()) ? date.getMonth() + 1 : null;
    }).filter(Boolean)).sort((a, b) => a - b);
    return months.map((month) => option(String(month), monthName(month), selected)).join("");
  }

  function yearOptions(items, selected) {
    const years = unique(items.map((item) => {
      const date = new Date(item.sortDate || 0);
      return Number.isFinite(date.getTime()) ? date.getFullYear() : null;
    }).filter(Boolean)).sort((a, b) => b - a);
    return years.map((year) => option(String(year), String(year), selected)).join("");
  }

  function horseTypeOptions(horses, selected) {
    const types = unique(horses.map((horse) => horse.type).filter(Boolean)).sort();
    return types.map((type) => option(type, type, selected)).join("");
  }

  function horseType(horse) {
    const text = horse.classes.map((row) => [
      row.classTitle,
      row.rawSectionName,
      row.sectionName
    ].filter(Boolean).join(" ")).join(" ").toLowerCase();
    if (text.includes("pony")) return "Pony";
    if (text.includes("jumper")) return "Jumper";
    if (text.includes("equitation") || text.includes("medal") || text.includes("maclay")) return "Equitation";
    if (text.includes("dressage")) return "Dressage";
    if (text.includes("hunter")) return "Hunter";
    return "Other";
  }

  function option(value, label, selected) {
    return '<option value="' + escapeAttr(value) + '"' + (String(value) === String(selected) ? " selected" : "") + ">" + escapeHtml(label) + "</option>";
  }

  function monthName(month) {
    return new Date(2026, Number(month) - 1, 1).toLocaleString("en-US", { month: "short" });
  }

  function videoCarousel(videos, scope, options = {}) {
    const railId = "lp-video-rail-" + scope;
    return [
      '<div class="lp-video-shell">',
      '<div class="lp-video-rail" id="' + escapeAttr(railId) + '" data-video-rail="' + escapeAttr(scope) + '">',
      videos.map(videoCard).join(""),
      "</div>",
      options.hideControls ? "" : '<div class="lp-video-controls">' + videoNavMarkup(scope) + "</div>",
      "</div>"
    ].join("");
  }

  function videoNavMarkup(scope) {
    return [
      '<button class="lp-video-nav" type="button" data-video-nav="prev" data-video-target="' + escapeAttr(scope) + '">Prev</button>',
      '<button class="lp-video-nav" type="button" data-video-nav="next" data-video-target="' + escapeAttr(scope) + '">Next</button>'
    ].join("");
  }

  function videoCard(video) {
    const thumbnail = layerFor("videos", video.id).thumbnailUrl;
    return [
      '<button class="lp-video-card" type="button" data-open-video="' + escapeAttr(video.id) + '">',
      '<div class="lp-video-thumb" aria-hidden="true">' + (thumbnail ? '<img src="' + escapeAttr(thumbnail) + '" alt="">' : "") + "</div>",
      '<div class="lp-video-body">',
      "<h4>" + escapeHtml(video.title) + favoriteMarker("videos", video.id) + "</h4>",
      '<p class="lp-row-meta">' + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</p>",
      "</div>",
      "</button>"
    ].join("");
  }

  function videoList(videos) {
    if (!videos.length) return '<p class="lp-empty">No videos available.</p>';
    return videos.map((video) => rowWithActions("videos", video.id, [
      '<button class="lp-row" type="button" data-open-video="' + escapeAttr(video.id) + '">',
      '<span>' + titleWithStatus(video.title, "videos", video.id),
      '<span class="lp-row-meta">' + escapeHtml(video.time) + "  -  " + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</span>",
      "</span>",
      "</button>"
    ].join(""))).join("");
  }

  function videoGrid(videos) {
    if (!videos.length) return '<p class="lp-empty">No videos available.</p>';
    return '<div class="lp-video-grid">' + videos.map(videoCard).join("") + "</div>";
  }

  function competitionRow(competition) {
    const rows = currentClasses().filter((row) => row.competitionId === competition.competitionId);
    return rowWithActions("competitions", competition.competitionId, [
      '<button class="lp-row" type="button" data-open-competition="' + escapeAttr(competition.competitionId) + '">',
      '<span>' + titleWithStatus(competition.competitionName, "competitions", competition.competitionId),
      '<span class="lp-row-meta">' + escapeHtml(dateRange(competition)) + "  -  " + escapeHtml(competition.state || "") + "  -  Zone " + escapeHtml(competition.zone || "") + "</span></span>",
      ribbonForRows(rows),
      "</button>"
    ].join(""));
  }

  function competitionCard(competition) {
    const rows = currentClasses().filter((row) => row.competitionId === competition.competitionId);
    return [
      '<button class="lp-click-card" type="button" data-open-competition="' + escapeAttr(competition.competitionId) + '">',
      '<div class="lp-metric-line">',
      miniMetric(rows.length, "Classes"),
      "</div>",
      "<h3>" + escapeHtml(competition.competitionName) + favoriteMarker("competitions", competition.competitionId) + "</h3>",
      '<p class="lp-row-meta">' + escapeHtml(dateRange(competition)) + "  -  " + escapeHtml(competition.state || "") + "  -  Zone " + escapeHtml(competition.zone || "") + "</p>",
      "</button>"
    ].join("");
  }

  function classCard(row) {
    return [
      '<button class="lp-click-card" type="button" data-open-class="' + escapeAttr(row.id) + '">',
      '<div class="lp-card-head">',
      "<h3>" + escapeHtml(row.classTitle) + favoriteMarker("classes", row.id) + "</h3>",
      placementToken(row),
      "</div>",
      '<div class="lp-metric-line">',
      miniMetric(valueOrDash(row.entries), "Entries"),
      miniMetric(valueOrDash(row.backNumber), "Back #"),
      "</div>",
      '<p class="lp-row-meta">' + escapeHtml(row.horse.name || "Unknown") + "  -  " + escapeHtml(row.competitionName) + "</p>",
      "</button>"
    ].join("");
  }

  function compactHorseButton(horse) {
    return rowWithActions("horses", horse.id, [
      '<button class="lp-row lp-horse-row" type="button" data-open-horse="' + escapeAttr(horse.id) + '">',
      '<span>' + titleWithStatus(horse.name, "horses", horse.id),
      '<span class="lp-row-meta">' + horse.competitions.size + " shows  -  " + horse.classes.length + " classes</span></span>",
      placementStrip(horse.classes),
      "</button>"
    ].join(""));
  }

  function horseCard(horse) {
    const imageUrl = horseImage(horse);
    return [
      '<button class="lp-video-card lp-horse-video-card" type="button" data-open-horse="' + escapeAttr(horse.id) + '">',
      '<div class="lp-video-thumb" aria-hidden="true">',
      imageUrl ? '<img src="' + escapeAttr(imageUrl) + '" alt="">' : '<div class="lp-horse-thumb" aria-hidden="true"></div>',
      placementStrip(horse.classes),
      "</div>",
      '<div class="lp-video-body">',
      "<h4>" + escapeHtml(horse.name) + favoriteMarker("horses", horse.id) + "</h4>",
      '<p class="lp-row-meta">' + horse.competitions.size + " shows  -  " + horse.classes.length + " classes</p>",
      "</div>",
      "</button>"
    ].join("");
  }

  function rowWithActions(kind, id, rowMarkup) {
    return rowMarkup;
  }

  function horseImage(horse) {
    const layer = layerFor("horses", horse.id);
    return layer.imageUrl || layer.image_upload || layer.imageUrl_2 || "";
  }

  function horseDetailMedia(imageUrl) {
    return imageUrl
      ? '<img src="' + escapeAttr(imageUrl) + '" alt="">'
      : '<div class="lp-horse-detail-placeholder" aria-hidden="true"></div>';
  }

  function placementGrid(rows) {
    const counts = placementCounts(rows);
    const hasNumericPlacement = Object.values(counts).some((count) => count > 0);
    if (!hasNumericPlacement) return "";
    return [
      '<div class="lp-placement-grid' + (Object.values(counts).filter((count) => count > 0).length === 1 ? " is-single" : "") + '" aria-label="Placings">',
      [1, 2, 3, 4, 5, 6, 7, 8].filter((place) => counts[place] > 0).map((place) => [
        '<div class="lp-placement-box lp-place-' + place + '">',
        "<strong>" + escapeHtml(counts[place]) + "</strong>",
        "<span>" + escapeHtml(ordinal(place)) + "</span>",
        "</div>"
      ].join("")).join(""),
      "</div>"
    ].join("");
  }

  function placementCounts(rows) {
    return rows.reduce((counts, row) => {
      const place = placingNumber(row);
      if (place >= 1 && place <= 8) counts[place] = (counts[place] || 0) + 1;
      return counts;
    }, {});
  }

  function hasRibbon(rows) {
    return rows.some((row) => {
      const place = placingNumber(row);
      return place >= 1 && place <= 8;
    });
  }

  function placementStrip(rows) {
    const counts = placementCounts(rows);
    const places = [1, 2, 3, 4, 5, 6, 7, 8].filter((place) => counts[place] > 0);
    if (!places.length) return "";
    return '<div class="lp-placement-strip" aria-label="Placings achieved">' + places.map((place) =>
      '<span class="lp-placement-dot lp-place-' + place + '" title="' + escapeAttr(ordinal(place)) + '"></span>'
    ).join("") + "</div>";
  }

  function numericPlacing(row) {
    const place = placingNumber(row);
    return Number.isFinite(place) && place >= 1;
  }

  function ribbonForRow(row) {
    return placementToken(row) || "<span></span>";
  }

  function placementToken(row) {
    const place = placingNumber(row);
    return place >= 1
      ? '<span class="lp-achievement lp-place-token-' + escapeAttr(place) + '">' + escapeHtml(ordinal(place)) + "</span>"
      : "";
  }

  function ribbonForRows(rows) {
    const bestPlace = rows
      .map(placingNumber)
      .filter(Number.isFinite)
      .sort((a, b) => a - b)[0];
    return bestPlace >= 1
      ? '<span class="lp-achievement lp-place-token-' + escapeAttr(bestPlace) + '">' + escapeHtml(ordinal(bestPlace)) + "</span>"
      : "<span></span>";
  }

  function placingNumber(row) {
    const placing = String(row.placing || "").trim();
    const match = placing.match(/^([0-9]+)/);
    return match ? Number(match[1]) : NaN;
  }

  function displayPlacing(row) {
    const place = placingNumber(row);
    if (String(row.placing || "").trim().toUpperCase() === "DNP") return "";
    return Number.isFinite(place) ? ordinal(place) : valueOrDash(row.placing);
  }

  function displaySectionName(value) {
    return String(value || "")
      .replace(/No points earned/gi, "")
      .replace(/(s*)/g, "")
      .replace(/s{2,}/g, " ")
      .trim();
  }

  function ordinal(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return value;
    const mod100 = number % 100;
    if (mod100 >= 11 && mod100 <= 13) return number + "th";
    switch (number % 10) {
      case 1: return number + "st";
      case 2: return number + "nd";
      case 3: return number + "rd";
      default: return number + "th";
    }
  }

  function seeAll(tabName, label) {
    return '<div class="lp-list-footer"><button class="lp-see-all" type="button" data-select-tab="' + escapeAttr(tabName) + '">' + escapeHtml(label) + "</button></div>";
  }

  function moveVideoRail(button) {
    const target = button.dataset.videoTarget;
    const direction = button.dataset.videoNav === "prev" ? -1 : 1;
    const rail = Array.from(root.querySelectorAll("[data-video-rail]")).find((item) => item.dataset.videoRail === target);
    if (!rail) return;
    const card = rail.querySelector(".lp-video-card");
    const step = card ? card.getBoundingClientRect().width + 12 : rail.clientWidth * 0.8;
    rail.scrollBy({ left: direction * step, behavior: "smooth" });
  }

  function detailHero(mediaMarkup, headMarkup) {
    return [
      '<div class="lp-detail-hero">',
      '<div class="lp-detail-hero-media">',
      mediaMarkup,
      "</div>",
      headMarkup || "",
      "</div>"
    ].join("");
  }

  function mockVideos(classRows, horses) {
    return classRows.slice(0, 16).map((row, index) => ({
      id: "mock-video-" + (index + 1),
      classId: row.id,
      horseId: row.horseId,
      competitionId: row.competitionId,
      title: row.classTitle,
      horse: row.horse.name || horses[index % horses.length]?.name || "Horse",
      competition: row.competitionName,
      classTitle: row.classTitle,
      sortDate: row.sortDate,
      time: mockVideoTime(index)
    }));
  }

  function mockVideoPlayer(video) {
    const thumbnail = layerFor("videos", video.id).thumbnailUrl;
    return [
      '<div class="lp-video-player" role="img" aria-label="Mock video preview">',
      thumbnail ? '<img src="' + escapeAttr(thumbnail) + '" alt="">' : "",
      '<div class="lp-video-play">Play</div>',
      '<div class="lp-video-time">' + escapeHtml(video.time) + "</div>",
      "</div>"
    ].join("");
  }

  function videoEmbed(video) {
    const videoLayer = layerFor("videos", video.id);
    const videoUrl = videoLayer.embedUrl || toYouTubeEmbedUrl(videoLayer.videoUrl) || videoLayer.videoUrl;
    if (!videoUrl) return mockVideoPlayer(video);
    if (/\.(mp4|webm|ogg)(\?|#|$)/i.test(videoUrl)) {
      return '<video src="' + escapeAttr(videoUrl) + '" controls playsinline></video>';
    }
    return '<iframe src="' + escapeAttr(videoUrl) + '" title="' + escapeAttr(video.title) + '" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>';
  }

  function toYouTubeEmbedUrl(url) {
    if (!url) return "";
    const text = String(url).trim();
    const watch = text.match(/[?&]v=([A-Za-z0-9_-]+)/);
    if (watch) return "https://www.youtube.com/embed/" + watch[1];
    const short = text.match(/youtu\.be\/([A-Za-z0-9_-]+)/);
    if (short) return "https://www.youtube.com/embed/" + short[1];
    return text.includes("youtube.com/embed/") ? text : "";
  }

  function mockVideoTime(index) {
    const minutes = 1 + (index % 4);
    const seconds = String(18 + ((index * 7) % 42)).padStart(2, "0");
    return minutes + ":" + seconds;
  }

  function sectionTitle(title, count, viewKey = "", customActions = "") {
    const countMarkup = count ? '<span class="lp-section-count">' + escapeHtml(count) + "</span>" : "";
    return [
      '<div class="lp-section-title"><h3>' + escapeHtml(title) + "</h3>",
      '<div class="lp-section-actions">',
      customActions || [
        countMarkup
      ].join(""),
      "</div></div>"
    ].join("");
  }

  function filterToggleMarkup(key) {
    const isOpen = !!sectionFilterState[key];
    return '<button class="lp-filter-toggle' + (isOpen ? " is-active" : "") + '" type="button" data-section-filter-toggle="' + escapeAttr(key) + '" aria-expanded="' + (isOpen ? "true" : "false") + '">Filter</button>';
  }

  function viewToggle(viewKey) {
    const current = viewControls[viewKey] || "list";
    return [
      '<span class="lp-view-toggle" aria-label="View mode">',
      '<button class="lp-view-button' + (current === "grid" ? " is-active" : "") + '" type="button" data-view-toggle="' + escapeAttr(viewKey) + '" data-view-mode="grid" aria-label="Grid view">Grid</button>',
      '<button class="lp-view-button' + (current === "list" ? " is-active" : "") + '" type="button" data-view-toggle="' + escapeAttr(viewKey) + '" data-view-mode="list" aria-label="List view">List</button>',
      "</span>"
    ].join("");
  }

  function miniMetric(value, label) {
    return '<div class="lp-mini-metric"><strong>' + escapeHtml(value) + "</strong><span>" + escapeHtml(label) + "</span></div>";
  }

  function detailCard(title, body) {
    return '<section class="lp-card lp-span-6"><h3>' + escapeHtml(title) + "</h3><p>" + body + "</p></section>";
  }

  function detailList(rows) {
    return '<div class="lp-detail-list">' + staticRowList(rows) + "</div>";
  }

  function staticRowList(rows) {
    return rows.filter(Boolean).map(([label, value]) => [
      '<div class="lp-row is-static is-detail">',
      '<span class="lp-row-title">' + escapeHtml(label) + "</span>",
      '<span class="lp-row-meta">' + value + "</span>",
      "</div>"
    ].join("")).join("");
  }

  function outboundLink(url, label) {
    return url ? '  -  <a class="lp-link" href="' + escapeAttr(url) + '" target="_blank" rel="noopener noreferrer">usef</a>' : "";
  }

  function dateRange(competition) {
    if (!competition.startDate && !competition.endDate) return "Date unavailable";
    if (competition.startDate === competition.endDate || !competition.endDate) return competition.startDate;
    return competition.startDate + " - " + competition.endDate;
  }

  function parseDate(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? 0 : date.getTime();
  }

  function inputDate(date) {
    return date.toISOString().slice(0, 10);
  }

  function formatDate(date) {
    return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  }

  function numberValue(value) {
    const number = Number(String(value ?? "").replace(/[$,]/g, ""));
    return Number.isFinite(number) ? number : 0;
  }

  function valueOrDash(value) {
    return value === null || value === undefined || value === "" ? "-" : String(value);
  }

  function unique(values) {
    return Array.from(new Set(values));
  }

  function uniqueById(item, index, items) {
    return items.findIndex((candidate) => candidate.id === item.id) === index;
  }

  function stableId(parts) {
    return parts.map((part) => encodeURIComponent(String(part ?? ""))).join("__");
  }

  function slugify(value) {
    return String(value ?? "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "") || "group";
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
    return escapeHtml(value).replace(/\x60/g, "&#096;");
  }
  } catch (error) {
    fail("boot failed", error);
  }
})();
