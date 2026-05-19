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
  const state = normalize(payload, layer);
  const overviewControls = {
    competitions: { sort: "desc", month: "all", year: "all" },
    classes: { sort: "desc", month: "all", year: "all" }
  };
  const overviewRange = {
    start: defaultOverviewStart(),
    end: state.dateRange.endIso
  };
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
  const filterToggle = root.querySelector("[data-filter-toggle]");
  const dateFilter = root.querySelector("[data-date-filter]");
  const renderedPanels = new Set();

  renderShell();
  renderOverview();

  root.addEventListener("click", (event) => {
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

    const filterToggleButton = event.target.closest("[data-filter-toggle]");
    if (filterToggleButton) {
      setDateFilterOpen(dateFilter.hidden);
      return;
    }

    if (event.target.closest("[data-filter-close]")) {
      setDateFilterOpen(false);
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

    const overviewStart = event.target.closest("[data-overview-start]");
    if (overviewStart) {
      overviewRange.start = overviewStart.value || state.dateRange.startIso;
      renderShell();
      renderOverview();
      return;
    }

    const allFilter = event.target.closest("[data-all-filter]");
    if (allFilter) {
      const target = allFilter.dataset.allFilter;
      const field = allFilter.dataset.filterField;
      allControls[target][field] = allFilter.value;
      renderAllPanel(target);
    }
  });

  root.addEventListener("input", (event) => {
    const overviewStart = event.target.closest("[data-overview-start]");
    if (overviewStart) {
      overviewRange.start = overviewStart.value || state.dateRange.startIso;
      renderShell();
      renderOverview();
    }
    const allFilter = event.target.closest("[data-all-filter]");
    if (allFilter) {
      const target = allFilter.dataset.allFilter;
      const field = allFilter.dataset.filterField;
      allControls[target][field] = allFilter.value;
      renderAllPanel(target);
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
    return { version: 1, horses: {}, competitions: {}, classes: {}, videos: {} };
  }

  function normalize(data, layer = emptyLayer()) {
    const source = data.state || data;
    const competitions = (source.competitions || []).map((competition) => ({
      ...competition,
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
      type: horseType(horse)
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
      videos: mockVideos(classRows, horses),
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

    const startInput = root.querySelector("[data-overview-start]");
    const endLabel = root.querySelector("[data-overview-end]");
    if (startInput) {
      startInput.min = state.dateRange.startIso;
      startInput.max = state.dateRange.endIso;
      startInput.value = overviewRange.start || state.dateRange.startIso;
    }
    if (endLabel) endLabel.textContent = state.dateRange.end || "Latest";

    root.querySelector('[data-tab-count="horses"]').textContent = state.counts.horses;
    root.querySelector('[data-tab-count="videos"]').textContent = state.videos.length;
    root.querySelector('[data-tab-count="competitions"]').textContent = state.counts.competitions;
    root.querySelector('[data-tab-count="classes"]').textContent = state.counts.classes;
  }

  function setDateFilterOpen(isOpen) {
    dateFilter.hidden = !isOpen;
    filterToggle.classList.toggle("is-active", isOpen);
    filterToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
  }

  function renderOverview() {
    renderedPanels.add("overview");
    const rangedClasses = filterOverviewRange(state.classRows);
    const rangedCompetitions = filterOverviewRange(state.competitions);
    const rangedVideos = filterOverviewRange(state.videos);
    const topHorses = horsesForRows(rangedClasses).filter((horse) => hasRibbon(horse.classes)).slice(0, 5);
    const overviewVideos = rangedVideos.slice(0, 5);
    const recentCompetitions = filterOverviewItems(rangedCompetitions, overviewControls.competitions).slice(0, 5);
    const notableClasses = filterOverviewItems(rangedClasses, overviewControls.classes).slice(0, 5);

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
    const favoriteHorses = state.horses.filter((horse) => hasRibbon(horse.classes)).slice(0, 5);
    const horses = filterHorses(state.horses, allControls.horses);
    panels.horses.innerHTML = [
      '<section class="lp-section-block lp-theme-horses">',
      sectionTitle("Horses", state.horses.length + " total", "", videoNavMarkup("all-horses")),
      horseCarousel(favoriteHorses, "all-horses"),
      sectionTitle("All horses", horses.length + " shown", "", filterToggleMarkup("all:horses")),
      allControlsMarkup("horses", state.horses),
      horseCollection(horses),
      "</section>"
    ].join("");
  }

  function renderVideos() {
    renderedPanels.add("videos");
    const favoriteVideos = state.videos.slice(0, 5);
    const videos = filterOverviewItems(state.videos, allControls.videos);
    panels.videos.innerHTML = [
      '<section class="lp-section-block lp-theme-videos">',
      sectionTitle("Videos", state.videos.length + " total", "", videoNavMarkup("all-favorites")),
      videoCarousel(favoriteVideos, "all-favorites", { hideControls: true }),
      sectionTitle("All videos", "", "", filterToggleMarkup("all:videos")),
      allControlsMarkup("videos", state.videos),
      videoList(videos),
      "</section>"
    ].join("");
  }

  function renderCompetitions() {
    renderedPanels.add("competitions");
    const competitions = filterOverviewItems(state.competitions, allControls.competitions);
    panels.competitions.innerHTML = [
      '<section class="lp-section-block lp-theme-competitions">',
      sectionTitle("Competitions", "", "", filterToggleMarkup("all:competitions")),
      allControlsMarkup("competitions", state.competitions),
      competitionCollection(competitions, viewControls.competitions),
      "</section>"
    ].join("");
  }

  function renderClasses() {
    renderedPanels.add("classes");
    const classes = filterOverviewItems(state.classRows, allControls.classes);
    panels.classes.innerHTML = [
      '<section class="lp-section-block lp-theme-classes">',
      sectionTitle("Classes", "", "", filterToggleMarkup("all:classes")),
      allControlsMarkup("classes", state.classRows),
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
    openModal([
      '<div class="lp-detail-head">',
      '<h3 id="lp-modal-title">' + escapeHtml(horse.name) + "</h3>",
      '<p class="lp-muted">USEF ' + escapeHtml(horse.id) + outboundLink(horse.link, "Horse profile") + "</p>",
      placementGrid(horse.classes),
      "</div>",
      '<div class="lp-metric-line">',
      miniMetric(horse.classes.length, "Classes"),
      miniMetric(horse.competitions.size, "Competitions"),
      "</div>",
      '<section class="lp-section-block" style="margin-top:16px">',
      sectionTitle("Classes", horse.classes.length + " rows"),
      classList(horse.classes, { showHorse: false }),
      "</section>"
    ].join(""));
  }

  function openCompetition(competitionId) {
    const competition = state.competitions.find((item) => item.competitionId === competitionId);
    if (!competition) return;
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
      "</section>"
    ].join(""));
  }

  function openClass(classId) {
    const row = state.classRows.find((item) => item.id === classId);
    if (!row) return;
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
      "</section>"
    ].join(""));
  }

  function openVideo(videoId) {
    const video = state.videos.find((item) => item.id === videoId);
    if (!video) return;
    openModal([
      '<div class="lp-detail-head">',
      '<h3 id="lp-modal-title">' + escapeHtml(video.title) + "</h3>",
      '<p class="lp-muted">' + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</p>",
      "</div>",
      mockVideoPlayer(video),
      detailList([
        ["Time", escapeHtml(video.time)],
        ["Horse", escapeHtml(video.horse)],
        ["Competition", escapeHtml(video.competition)],
        ["Class", escapeHtml(video.classTitle)]
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
      return [
        '<button class="lp-row" type="button" data-open-class="' + escapeAttr(row.id) + '">',
        "<span>",
        '<span class="lp-row-title">' + escapeHtml(row.classTitle) + "</span>",
        meta.length ? '<span class="lp-row-meta">' + meta.join("  -  ") + "</span>" : "",
        '<span class="lp-row-meta">' + detail + "</span>",
        "</span>",
        ribbonForRow(row),
        "</button>"
      ].join("");
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
    const start = parseDate(overviewRange.start || state.dateRange.startIso);
    const end = parseDate(overviewRange.end || state.dateRange.endIso);
    return items.filter((item) => {
      const value = Number(item.sortDate || 0);
      return (!start || value >= start) && (!end || value <= end);
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

  function defaultOverviewStart() {
    const requested = "2026-01-01";
    const min = parseDate(state.dateRange.startIso);
    const max = parseDate(state.dateRange.endIso);
    const target = parseDate(requested);
    if (!target || !min || !max) return state.dateRange.startIso;
    if (target < min) return state.dateRange.startIso;
    if (target > max) return state.dateRange.startIso;
    return requested;
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
    return [
      '<button class="lp-video-card" type="button" data-open-video="' + escapeAttr(video.id) + '">',
      '<div class="lp-video-thumb" aria-hidden="true"></div>',
      '<div class="lp-video-body">',
      "<h4>" + escapeHtml(video.title) + "</h4>",
      '<p class="lp-row-meta">' + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</p>",
      "</div>",
      "</button>"
    ].join("");
  }

  function videoList(videos) {
    if (!videos.length) return '<p class="lp-empty">No videos available.</p>';
    return videos.map((video) => [
      '<button class="lp-row" type="button" data-open-video="' + escapeAttr(video.id) + '">',
      '<span><span class="lp-row-title">' + escapeHtml(video.title) + "</span>",
      '<span class="lp-row-meta">' + escapeHtml(video.time) + "  -  " + escapeHtml(video.horse) + "  -  " + escapeHtml(video.competition) + "</span>",
      "</span>",
      "</button>"
    ].join("")).join("");
  }

  function competitionRow(competition) {
    const rows = state.classRows.filter((row) => row.competitionId === competition.competitionId);
    return [
      '<button class="lp-row" type="button" data-open-competition="' + escapeAttr(competition.competitionId) + '">',
      '<span><span class="lp-row-title">' + escapeHtml(competition.competitionName) + "</span>",
      '<span class="lp-row-meta">' + escapeHtml(dateRange(competition)) + "  -  " + escapeHtml(competition.state || "") + "  -  Zone " + escapeHtml(competition.zone || "") + "</span></span>",
      ribbonForRows(rows),
      "</button>"
    ].join("");
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
    return [
      '<button class="lp-row lp-horse-row" type="button" data-open-horse="' + escapeAttr(horse.id) + '">',
      '<span><span class="lp-row-title">' + escapeHtml(horse.name) + "</span>",
      '<span class="lp-row-meta">' + horse.competitions.size + " shows  -  " + horse.classes.length + " classes</span></span>",
      placementStrip(horse.classes),
      "</button>"
    ].join("");
  }

  function horseCard(horse) {
    return [
      '<button class="lp-video-card lp-horse-video-card" type="button" data-open-horse="' + escapeAttr(horse.id) + '">',
      '<div class="lp-video-thumb" aria-hidden="true">',
      '<div class="lp-horse-thumb" aria-hidden="true"></div>',
      placementStrip(horse.classes),
      "</div>",
      '<div class="lp-video-body">',
      "<h4>" + escapeHtml(horse.name) + "</h4>",
      '<p class="lp-row-meta">' + horse.competitions.size + " shows  -  " + horse.classes.length + " classes</p>",
      "</div>",
      "</button>"
    ].join("");
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
    return [
      '<div class="lp-video-player" role="img" aria-label="Mock video preview">',
      '<div class="lp-video-play">Play</div>',
      '<div class="lp-video-time">' + escapeHtml(video.time) + "</div>",
      "</div>"
    ].join("");
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

  function stableId(parts) {
    return parts.map((part) => encodeURIComponent(String(part ?? ""))).join("__");
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
