(async () => {
  const root = document.getElementById("lp-history-app");
  if (!root) return;

  const config = window.LP_HISTORY_CONFIG || JSON.parse(root.querySelector("#lp-history-config").textContent);
  const [payload, layer] = await Promise.all([
    fetch(config.historyUrl).then((response) => {
      if (!response.ok) throw new Error("History feed failed: " + response.status);
      return response.json();
    }),
    fetch(config.layerUrl).then((response) => response.ok ? response.json() : emptyLayer()).catch(emptyLayer)
  ]);
  const editMode = new URLSearchParams(window.location.search).has("key");
  const layerStorageKey = "lp-history-layer-draft";
  const themeStorageKey = "lp-history-theme-colors";
  const themeColors = loadThemeColors();
  const state = normalize(payload, loadStoredLayer(layer));
  root.classList.toggle("is-edit-mode", editMode);
  root.classList.add("is-overview-active");
  applyThemeColors();
  const overviewControls = {
    competitions: { sort: "desc", month: "all", year: "all" },
    classes: { sort: "desc", month: "all", year: "all" }
  };
  const overviewYears = new Set(defaultOverviewYears());
  const allControls = {
    competitions: { sort: "desc", month: "all", year: "all" },
    classes: { sort: "desc", month: "all", year: "all" },
    videos: { sort: "desc", month: "all", year: "all" },
    horses: { sort: "desc", month: "all", year: "all", search: "", type: "all" }
  };
  const sectionFilterState = {};
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
  renderOverview();

  root.addEventListener("click", (event) => {
    if (event.target.closest("[data-theme-color]")) {
      event.stopPropagation();
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
      renderShell();
      renderOverview();
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

    if (event.target.closest("[data-modal-close]")) {
      closeModal();
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
      updateEditStatus("Draft saved in this browser. Export layer.json when ready.");
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
      updateEditStatus("Draft saved in this browser. Export layer.json when ready.");
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

  function emptyLayer() {
    return { version: 1, updatedAt: "", horses: {}, competitions: {}, classes: {}, videos: {} };
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
    root.querySelector("[data-lp-summary]").textContent =
      state.dateRange.start && state.dateRange.end
        ? state.dateRange.start + " to " + state.dateRange.end
        : "Competition results";

    const yearWrap = root.querySelector("[data-overview-years]");
    if (yearWrap) {
      yearWrap.innerHTML = overviewYearOptions().map((year) => (
        '<button class="lp-year-pill' + (overviewYears.has(year) ? " is-active" : "") + '" type="button" data-overview-year="' + escapeAttr(year) + '" aria-pressed="' + (overviewYears.has(year) ? "true" : "false") + '">' + escapeHtml(year) + "</button>"
      )).join("");
    }

    root.querySelector('[data-tab-count="horses"]').textContent = state.counts.horses;
    root.querySelector('[data-tab-count="videos"]').textContent = state.videos.length;
    root.querySelector('[data-tab-count="competitions"]').textContent = state.counts.competitions;
    root.querySelector('[data-tab-count="classes"]').textContent = state.counts.classes;
    root.querySelectorAll("[data-theme-color]").forEach((input) => {
      const key = input.dataset.themeColor;
      if (themeColors[key]) input.value = themeColors[key];
    });
  }

  function renderOverview() {
    renderedPanels.add("overview");
    const rangedClasses = filterOverviewRange(visibleClasses(state.classRows));
    const rangedCompetitions = filterOverviewRange(visibleItems("competitions", state.competitions));
    const rangedVideos = filterOverviewRange(visibleItems("videos", state.videos));
    const rangedHorses = horsesForRows(rangedClasses).filter((horse) => isActiveRecord("horses", horse.id) && !isIgnored("horses", horse.id));
    const topHorses = overviewSubset("horses", rangedHorses, rangedHorses.filter((horse) => hasRibbon(horse.classes))).slice(0, 5);
    const overviewVideos = overviewSubset("videos", rangedVideos, rangedVideos).slice(0, 5);
    const recentCompetitions = filterOverviewItems(overviewSubset("competitions", rangedCompetitions, rangedCompetitions), overviewControls.competitions).slice(0, 5);
    const notableClasses = filterOverviewItems(overviewSubset("classes", rangedClasses, rangedClasses), overviewControls.classes).slice(0, 5);

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
      overviewControlsMarkup("competitions", state.competitions),
      competitionCollection(recentCompetitions, viewControls.overviewCompetitions),
      seeAll("competitions", "See all competitions ->"),
      "</section>",
      '<section class="lp-section-block lp-overview-section lp-theme-classes">',
      sectionTitle("Latest classes", "", "", filterToggleMarkup("overview:classes")),
      overviewControlsMarkup("classes", state.classRows),
      classCollection(notableClasses, viewControls.overviewClasses, { detailMode: "dateRange" }),
      seeAll("classes", "See all classes ->"),
      "</section>"
    ].join("");
  }

  function renderHorses() {
    renderedPanels.add("horses");
    const baseHorses = visibleItems("horses", state.horses);
    const favoriteHorses = favoriteSubset("horses", baseHorses, baseHorses.filter((horse) => hasRibbon(horse.classes))).slice(0, 5);
    const horses = filterHorses(baseHorses, allControls.horses);
    panels.horses.innerHTML = [
      '<section class="lp-section-block lp-theme-horses">',
      sectionTitle("Horses", state.horses.length + " total", "", videoNavMarkup("all-horses")),
      horseCarousel(favoriteHorses, "all-horses"),
      sectionTitle("All horses", horses.length + " shown", "", filterToggleMarkup("all:horses")),
      allControlsMarkup("horses", state.horses),
      horseCollection(horses),
      editPanel("horses", state.horses),
      "</section>"
    ].join("");
  }

  function renderVideos() {
    renderedPanels.add("videos");
    const baseVideos = visibleItems("videos", state.videos);
    const favoriteVideos = favoriteSubset("videos", baseVideos, baseVideos).slice(0, 5);
    const videos = filterOverviewItems(baseVideos, allControls.videos);
    panels.videos.innerHTML = [
      '<section class="lp-section-block lp-theme-videos">',
      sectionTitle("Videos", state.videos.length + " total", "", videoNavMarkup("all-favorites")),
      videoCarousel(favoriteVideos, "all-favorites", { hideControls: true }),
      sectionTitle("All videos", "", "", filterToggleMarkup("all:videos")),
      allControlsMarkup("videos", state.videos),
      videoList(videos),
      editPanel("videos", state.videos),
      "</section>"
    ].join("");
  }

  function renderCompetitions() {
    renderedPanels.add("competitions");
    const competitions = filterOverviewItems(visibleItems("competitions", state.competitions), allControls.competitions);
    panels.competitions.innerHTML = [
      '<section class="lp-section-block lp-theme-competitions">',
      sectionTitle("Competitions", "", "", filterToggleMarkup("all:competitions")),
      allControlsMarkup("competitions", state.competitions),
      competitionCollection(competitions, viewControls.competitions),
      editPanel("competitions", state.competitions),
      "</section>"
    ].join("");
  }

  function renderClasses() {
    renderedPanels.add("classes");
    const classes = filterOverviewItems(visibleClasses(state.classRows), allControls.classes);
    panels.classes.innerHTML = [
      '<section class="lp-section-block lp-theme-classes">',
      sectionTitle("Classes", "", "", filterToggleMarkup("all:classes")),
      allControlsMarkup("classes", state.classRows),
      classCollection(classes, viewControls.classes),
      editPanel("classes", state.classRows),
      "</section>"
    ].join("");
  }

  function renderAllPanel(target) {
    if (target === "competitions") renderCompetitions();
    if (target === "classes") renderClasses();
    if (target === "videos") renderVideos();
    if (target === "horses") renderHorses();
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

  function ensurePanelRendered(tabName) {
    if (renderedPanels.has(tabName)) return;
    if (tabName === "videos") renderVideos();
    if (tabName === "horses") renderHorses();
    if (tabName === "competitions") renderCompetitions();
    if (tabName === "classes") renderClasses();
  }

  function openHorse(horseId) {
    const horse = state.horses.find((item) => item.id === horseId);
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
        ["color", "Color", "text", horseLayer.color || ""],
        ["gender", "Gender", "text", horseLayer.gender || ""],
        ["disciplines", "Disciplines", "text", horseLayer.disciplines || horse.type || ""],
        ["age", "Age", "number", horseLayer.age || ""],
        ["recordState", "Record state", "single", recordState("horses", horse.id), [["active", "Active"], ["inactive", "Inactive"]]],
        ["status", "Status", "single", layerStatus("horses", horse.id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]]
      ])
    ].join(""));
  }

  function openCompetition(competitionId) {
    const competition = state.competitions.find((item) => item.competitionId === competitionId);
    if (!competition) return;
    const competitionLayer = layerFor("competitions", competition.competitionId);
    const rows = state.classRows.filter((row) => row.competitionId === competitionId);
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
        ["status", "Status", "single", layerStatus("competitions", competition.competitionId), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]],
        ["type", "Type", "multi", competitionLayer.type || [], ["Hunters", "Jumpers", "Equitation"]],
        ["class_sequences", "Class sequences", "multi", competitionLayer.class_sequences || [], ["Over Fences", "Under Saddle/Flat"]],
        ["tags", "Tags", "multi", competitionLayer.tags || [], ["seat", "maclay", "uset", "ushja", "wihs", "3'3\"", "3'6\"", "classic", "handy"]]
      ])
    ].join(""));
  }

  function openClass(classId) {
    const row = state.classRows.find((item) => item.id === classId);
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
        ["status", "Status", "single", layerStatus("classes", row.id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]],
        ["type", "Type", "multi", classLayer.type || [], ["Hunters", "Jumpers", "Equitation"]],
        ["class_sequences", "Class sequences", "multi", classLayer.class_sequences || [], ["Over Fences", "Under Saddle/Flat"]],
        ["tags", "Tags", "multi", classLayer.tags || [], ["seat", "maclay", "uset", "ushja", "wihs", "3'3\"", "3'6\"", "classic", "handy"]]
      ])
    ].join(""));
  }

  function openVideo(videoId) {
    const video = state.videos.find((item) => item.id === videoId);
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
        ["status", "Status", "single", layerStatus("videos", video.id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]],
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
    if (entry.status) return entry.status;
    if (entry.ignore) return "ignore";
    if (entry.favorite) return "favorite";
    if (entry.overview) return "overview";
    return "";
  }

  function recordState(kind, id) {
    const entry = state.layer[kind] && state.layer[kind][id];
    if (!entry) return "active";
    if (entry.recordState) return entry.recordState;
    if (entry.inactive) return "inactive";
    return "active";
  }

  function isActiveRecord(kind, id) {
    return recordState(kind, id) !== "inactive";
  }

  function isIgnored(kind, id) {
    return layerStatus(kind, id) === "ignore";
  }

  function isFavorite(kind, id) {
    return layerStatus(kind, id) === "favorite";
  }

  function isOverviewStatus(kind, id) {
    return layerStatus(kind, id) === "overview";
  }

  function overviewSubset(kind, items, fallback) {
    const selected = items.filter((item) => isOverviewStatus(kind, itemId(kind, item)));
    return selected.length ? selected : fallback;
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
  }

  function setLayerMultiValue(kind, id, field, value, checked) {
    if (!editMode || !kind || !id || !field) return;
    const entry = layerFor(kind, id);
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
      updateEditStatus("Image saved in draft layer. Export layer.json when ready.");
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
        singleField(kind, id, "status", layerStatus(kind, id), [["overview", "Overview"], ["favorite", "Favorite"], ["ignore", "Ignore"]]),
        "</div>"
      ].join("");
    }).join("");
    return [
      '<section class="lp-edit-panel" aria-label="' + escapeAttr(kind) + ' edit layer">',
      '<div class="lp-edit-head"><h4>Edit ' + escapeHtml(kind) + '</h4><div class="lp-edit-actions">',
      '<button class="lp-edit-button" type="button" data-layer-action="copy">Copy JSON</button>',
      '<button class="lp-edit-button" type="button" data-layer-action="export">Export layer.json</button>',
      '<button class="lp-edit-button" type="button" data-layer-action="clear-draft">Clear draft</button>',
      "</div></div>",
      '<div class="lp-edit-rows">',
      rows,
      "</div>",
      '<p class="lp-edit-status" data-edit-status>Draft saves in this browser. Export layer.json when ready.</p>',
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
      '<p class="lp-edit-status" data-edit-status>Draft saves in this browser.</p>',
      "</section>"
    ].join("");
  }

  function editGroups(kind, id, fields) {
    const grouped = [
      ["Record state", fields.filter(([field]) => field === "recordState")],
      ["Status", fields.filter(([field]) => field === "status")],
      ["Media", fields.filter(([field]) => ["imageUrl", "imageUrl_2", "image_upload", "videoUrl", "embedUrl", "thumbnailUrl", "playlist"].includes(field))],
      ["Profile", fields.filter(([field]) => ["barn_name", "show_name", "color", "gender", "disciplines", "age"].includes(field))],
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
      return editRow(title, fields.map(([field, label, type, value]) => editControl(kind, id, field, label, type, value)).join(""));
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
      choices.map((choice) => [
        '<label class="lp-edit-choice">',
        '<input type="checkbox" data-layer-multi="true" data-layer-field="' + escapeAttr(field) + '" data-layer-kind="' + escapeAttr(kind) + '" data-layer-id="' + escapeAttr(id) + '" value="' + escapeAttr(choice) + '"' + (selected.includes(choice) ? " checked" : "") + ">",
        '<span class="lp-edit-pill">' + escapeHtml(choice) + "</span>",
        "</label>"
      ].join("")).join(""),
      "</span>"
    ].join("");
  }

  function singleField(kind, id, field, selected, choices) {
    const name = "lp-" + slugify(kind + "-" + id + "-" + field);
    return [
      '<span class="lp-edit-choice-row">',
      choices.map((choice) => {
        const value = Array.isArray(choice) ? choice[0] : choice;
        const label = Array.isArray(choice) ? choice[1] : choice;
        return [
          '<label class="lp-edit-choice">',
          '<input type="radio" data-layer-field="' + escapeAttr(field) + '" data-layer-kind="' + escapeAttr(kind) + '" data-layer-id="' + escapeAttr(id) + '" name="' + escapeAttr(name) + '" value="' + escapeAttr(value) + '"' + (selected === value ? " checked" : "") + ">",
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
        '<span class="lp-row-title">' + escapeHtml(row.classTitle) + "</span>",
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

  function filterOverviewRange(items) {
    const selected = Array.from(overviewYears);
    if (!selected.length) return items;
    return items.filter((item) => {
      const date = new Date(item.sortDate || 0);
      if (!Number.isFinite(date.getTime())) return false;
      return selected.includes(String(date.getFullYear()));
    });
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
      "<h4>" + escapeHtml(video.title) + "</h4>",
      '<p class="lp-row-meta">' + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</p>",
      "</div>",
      "</button>"
    ].join("");
  }

  function videoList(videos) {
    if (!videos.length) return '<p class="lp-empty">No videos available.</p>';
    return videos.map((video) => rowWithActions("videos", video.id, [
      '<button class="lp-row" type="button" data-open-video="' + escapeAttr(video.id) + '">',
      '<span><span class="lp-row-title">' + escapeHtml(video.title) + "</span>",
      '<span class="lp-row-meta">' + escapeHtml(video.time) + "  -  " + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</span>",
      "</span>",
      "</button>"
    ].join(""))).join("");
  }

  function competitionRow(competition) {
    const rows = state.classRows.filter((row) => row.competitionId === competition.competitionId);
    return rowWithActions("competitions", competition.competitionId, [
      '<button class="lp-row" type="button" data-open-competition="' + escapeAttr(competition.competitionId) + '">',
      '<span><span class="lp-row-title">' + escapeHtml(competition.competitionName) + "</span>",
      '<span class="lp-row-meta">' + escapeHtml(dateRange(competition)) + "  -  " + escapeHtml(competition.state || "") + "  -  Zone " + escapeHtml(competition.zone || "") + "</span></span>",
      ribbonForRows(rows),
      "</button>"
    ].join(""));
  }

  function competitionCard(competition) {
    const rows = state.classRows.filter((row) => row.competitionId === competition.competitionId);
    return [
      '<button class="lp-click-card" type="button" data-open-competition="' + escapeAttr(competition.competitionId) + '">',
      '<div class="lp-metric-line">',
      miniMetric(rows.length, "Classes"),
      "</div>",
      "<h3>" + escapeHtml(competition.competitionName) + "</h3>",
      '<p class="lp-row-meta">' + escapeHtml(dateRange(competition)) + "  -  " + escapeHtml(competition.state || "") + "  -  Zone " + escapeHtml(competition.zone || "") + "</p>",
      "</button>"
    ].join("");
  }

  function classCard(row) {
    return [
      '<button class="lp-click-card" type="button" data-open-class="' + escapeAttr(row.id) + '">',
      '<div class="lp-card-head">',
      "<h3>" + escapeHtml(row.classTitle) + "</h3>",
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
      '<span><span class="lp-row-title">' + escapeHtml(horse.name) + "</span>",
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
      "<h4>" + escapeHtml(horse.name) + "</h4>",
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
    return '<div class="lp-detail-list">' + rows.filter(Boolean).map(([label, value]) => [
      '<div class="lp-detail-row">',
      '<div class="lp-detail-label">' + escapeHtml(label) + "</div>",
      '<div class="lp-detail-value">' + value + "</div>",
      "</div>"
    ].join("")).join("") + "</div>";
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
})();
