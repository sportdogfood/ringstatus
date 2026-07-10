const DEFAULT_SHOW_NO = "14909";
    const FUNCTION_ORIGIN = "https://horseshowing-700800454.development.catalystserverless.com";
    const PRIMARY_API = window.RS_WEC_GRID_API || FUNCTION_ORIGIN + "/server/wec_live_grid/execute";
    const LEGACY_FALLBACK_API = FUNCTION_ORIGIN + "/server/horseshowing_sync/";

    const SAMPLE_ROWS = [
      {
        row_key: "class|indoor_1|173",
        ring_name_normalized: "indoor_1",
        display_ring: "INDOOR 1",
        time: "8:30 AM",
        class_number: 173,
        class_name: "1.05m Junior Classic II.2b",
        entry_count: 11,
        status: "soon",
        barn_name: "Beechwood",
        horse_items: [
          { label: "Bee", order: 3, trainer: "CWF" },
          { label: "Insider", order: 6, trainer: "CWF" }
        ]
      },
      {
        row_key: "class|indoor_1|174",
        ring_name_normalized: "indoor_1",
        display_ring: "INDOOR 1",
        time: "9:15 AM",
        class_number: 174,
        class_name: "1.10m Jumper",
        entry_count: 9,
        status: "soon",
        barn_name: "North Run",
        horse_items: []
      },
      {
        row_key: "class|indoor_5|201",
        ring_name_normalized: "indoor_5",
        display_ring: "INDOOR 5",
        time: "10:00 AM",
        class_number: 201,
        class_name: "Junior Hunter 3'6",
        entry_count: 18,
        status: "soon",
        barn_name: "Beechwood",
        horse_items: [
          { label: "Carapaccio", order: 1, trainer: "SBS" }
        ]
      },
      {
        row_key: "class|grand|220",
        ring_name_normalized: "grand",
        display_ring: "GRAND",
        time: "11:15 AM",
        class_number: 220,
        class_name: "Yeti Grand Prix 1.45m II.2a American Std",
        entry_count: 32,
        status: "soon",
        barn_name: "Cedar Lane",
        horse_items: []
      }
    ];

    let sourceRows = SAMPLE_ROWS.slice();
    let currentMeta = { source: "sample", show_no: "", focus_date: "", last_updated: "" };
    let gridApi = null;
    let focusMode = false;
    let horseFilterOpen = false;
    let activeHorseFilters = new Set();

    function text(value) {
      return value == null ? "" : String(value);
    }

    function escapeHtml(value) {
      return text(value).replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char]));
    }

    function numberOrNull(value) {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    }

    function dataUrl(baseUrl) {
      const url = new URL(baseUrl);
      const params = new URLSearchParams(location.search);
      url.searchParams.set("action", "wec-mobile-live");
      url.searchParams.set("show_no", params.get("show_no") || DEFAULT_SHOW_NO);
      if (params.get("focus_day")) url.searchParams.set("focus_day", params.get("focus_day"));
      return url.toString();
    }

    async function fetchJson(url) {
      const response = await fetch(url, { cache: "no-store" });
      const body = await response.text();
      if (!response.ok) throw new Error("Fetch failed " + response.status + ": " + body.slice(0, 160));
      return JSON.parse(body);
    }

    function flattenRollups(rollups, rollupLabel) {
      if (Array.isArray(rollups) && rollups.length) {
        return rollups.map((item) => ({
          label: text(item.label || item.name || item.horse || item.barn_name).trim(),
          order: text(item.order || item.entry_order || item.in).trim(),
          trainer: text(item.trainer || item.trainer_name || item.barn_name).trim()
        })).filter(item => item.label);
      }
      return text(rollupLabel).trim()
        ? [{ label: text(rollupLabel).trim(), order: "", trainer: "" }]
        : [];
    }

    function normalizePayload(payload, source) {
      const payloadRings = Array.isArray(payload) ? payload : payload && payload.rings;
      if (!payload || payload.ok === false || !Array.isArray(payloadRings)) {
        throw new Error(payload && payload.error ? payload.error : "Unexpected WEC payload shape");
      }

      const rows = [];
      payloadRings.forEach((ring) => {
        const ringDisplay = text(ring.ring_display || ring.ring_name_normalized || ring.ring_name || "Ring").trim();
        const ringKey = text(ring.ring_name_normalized || ring.ring_name_prioritized || ringDisplay).trim().toLowerCase();
        const ringVisualKey = text(ring.ring_visual_key || [ring.ring_no, ringKey].filter(Boolean).join("|") || ringKey).trim();
        (Array.isArray(ring.classes) ? ring.classes : []).forEach((classRow, index) => {
          const classNumber = text(classRow.class_number).trim();
          let className = text(classRow.class_name || classRow.class_label).trim();
          if (classNumber && className.startsWith(classNumber + " - ")) {
            className = className.slice((classNumber + " - ").length).trim();
          }
          rows.push({
            row_key: text(classRow.row_key || ringVisualKey + "|" + text(classRow.class_no || classNumber || index)).trim(),
            ring_name_normalized: text(classRow.ring_name_normalized || ring.ring_name_normalized || ringKey).trim().toLowerCase(),
            display_ring: ringDisplay.toUpperCase(),
            time: text(classRow.display_time || classRow.time_text || classRow.class_time || classRow.class_start_time).trim(),
            class_number: classNumber,
            class_name: className,
            entry_count: numberOrNull(classRow.entry_count),
            status: text(classRow.status || classRow.class_status || "soon").trim(),
            barn_name: text(classRow.barn_name || classRow.trainer || classRow.trainer_name).trim(),
            show_no: text(classRow.show_no).trim(),
            focus_day: text(classRow.focus_day).trim(),
            show_day: text(classRow.show_day).trim(),
            horse_items: flattenRollups(classRow.rollups, classRow.rollup_label),
            entries: Array.isArray(classRow.entries) ? classRow.entries : []
          });
        });
      });

      const first = rows.find(Boolean) || {};
      currentMeta = {
        source,
        show_no: text(payload.show_no || first.show_no).trim(),
        focus_date: text(payload.show_focus_date || payload.focus_day || first.focus_day || first.show_day).trim(),
        last_updated: text(payload.last_updated).trim()
      };
      return rows;
    }

    function ringGroupKey(row) {
      return text(row.ring_name_normalized).trim().toLowerCase();
    }

    function ringGroupLabel(row) {
      return text(row.display_ring || row.ring_name_normalized).trim();
    }

    function rowMatchesHorseFilters(row) {
      if (!activeHorseFilters.size) return true;
      return activeHorseFilters.has(text(row.barn_name).trim().toLowerCase());
    }

    function metadataStatusPrefix() {
      const sourceLabel = currentMeta.source === "function"
        ? "Catalyst function"
        : currentMeta.source === "legacy-fallback"
          ? "Legacy fallback"
          : "Sample fallback";
      return [
        sourceLabel,
        currentMeta.show_no ? "show " + currentMeta.show_no : "",
        currentMeta.focus_date || "",
        currentMeta.last_updated ? "updated " + currentMeta.last_updated : ""
      ].filter(Boolean).join(" · ");
    }

    function updateHeader() {
      const titleParts = ["Ring Classes"];
      if (currentMeta.show_no) titleParts.push("Show " + currentMeta.show_no);
      if (currentMeta.focus_date) titleParts.push(currentMeta.focus_date);
      document.getElementById("appTitle").textContent = titleParts.join(" · ");
      document.getElementById("appSubtitle").textContent = metadataStatusPrefix() || "Grouped by ring_name_normalized";
    }

    function focusKey(row) {
      return [
        ringGroupKey(row),
        text(row.time).trim().toLowerCase(),
        text(row.class_name).trim().toLowerCase()
      ].join("|");
    }

    function visibleClassRows() {
      const rows = sourceRows.filter(rowMatchesHorseFilters);
      if (!focusMode) return rows;
      const seen = new Set();
      return rows.filter((row) => {
        const key = focusKey(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    function groupedRows() {
      const groups = new Map();
      visibleClassRows().forEach((row) => {
        const key = ringGroupKey(row);
        if (!groups.has(key)) {
          groups.set(key, { key, label: ringGroupLabel(row), rows: [] });
        }
        groups.get(key).rows.push(row);
      });
      return Array.from(groups.values());
    }

    function buildGridRows() {
      const rows = [];
      groupedRows().forEach((group) => {
        rows.push({
          row_type: "ring",
          row_key: "ring|" + group.key,
          ring_name_normalized: group.key,
          ring_title: group.label
        });
        group.rows.forEach((row) => {
          rows.push({
            ...row,
            row_type: "class",
            has_rollup: Array.isArray(row.horse_items) && row.horse_items.length > 0
          });
        });
      });
      return rows;
    }

    function renderRingAnchors() {
      const groups = groupedRows();
      const root = document.getElementById("ringAnchors");
      root.innerHTML = groups.length
        ? groups.map(group => `<button class="rs-button tap" type="button" data-ring-name-normalized="${group.key}">${group.label}</button>`).join("")
        : `<button class="rs-button tap" type="button" disabled>No Rings</button>`;
    }

    function renderHorseFilters() {
      const barns = Array.from(new Set(sourceRows.map(row => text(row.barn_name).trim()).filter(Boolean))).sort();
      const root = document.getElementById("horseFilters");
      root.innerHTML = barns.map((barn) => {
        const key = barn.toLowerCase();
        const active = activeHorseFilters.has(key) ? " tap-active" : "";
        return `<button class="rs-button tap${active}" type="button" data-barn-name="${key}">${barn}</button>`;
      }).join("");
    }

    function ringFullWidthRenderer(params) {
      const row = params.data || {};
      return `<div class="ag-ring-group" data-ring-name-normalized="${text(row.ring_name_normalized)}">${text(row.ring_title || row.ring_name_normalized)}</div>`;
    }

    function classFullWidthRenderer(params) {
      const row = params.data || {};
      const shell = document.createElement("div");
      const rollupLine = document.createElement("div");
      const rollupWrap = document.createElement("div");
      const classLine = document.createElement("div");

      shell.className = "ag-full-width-anchor";
      const data = document.createElement("div");
      data.className = "class-related-data" + (row.has_rollup ? " has-rollup" : "");

      rollupLine.className = "rollup-line" + (row.has_rollup ? "" : " is-hidden");
      rollupWrap.className = "class-related-rollup";
      rollupWrap.setAttribute("data-rollups", "");

      if (row.has_rollup) {
        row.horse_items.forEach((item, index) => {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "rs-button tap rollup-item";
          button.dataset.rowKey = row.row_key;
          button.dataset.entryOrder = text(item.order);
          button.dataset.trainer = text(item.trainer);
          button.dataset.rollupIndex = String(index + 1);
          button.innerHTML = `<span class="rollup-label">${text(item.label)}${item.order ? " (" + text(item.order) + ")" : ""}</span>`;
          rollupWrap.appendChild(button);
        });
      }

      classLine.className = "class-line";
      classLine.innerHTML =
        `<span class="time-cell"><span class="class-time">${text(row.time || "--")}</span></span>` +
        `<span class="class-ring">${text(row.display_ring || row.ring_name_normalized)}</span>` +
        `<span class="class-name">${text(row.class_number)} - ${text(row.class_name)}</span>` +
        `<span class="class-entry"><button class="rs-button tap class-token" type="button">${text(row.entry_count)}</button></span>` +
        `<span class="class-status"><button class="rs-button tap class-token" type="button">${row.has_rollup ? "soon" : "no detail"}</button></span>`;

      rollupLine.appendChild(rollupWrap);
      data.appendChild(rollupLine);
      data.appendChild(classLine);
      shell.appendChild(data);
      return shell;
    }

    function fullWidthRenderer(params) {
      return params.data && params.data.row_type === "ring"
        ? ringFullWidthRenderer(params)
        : classFullWidthRenderer(params);
    }

    function updateRows() {
      const rows = buildGridRows();
      renderRingAnchors();
      renderHorseFilters();
      updateHeader();
      if (gridApi) gridApi.setGridOption("rowData", rows);
      document.getElementById("rowCount").textContent = String(visibleClassRows().length);
      document.getElementById("statusText").textContent = metadataStatusPrefix() || "grouped by ring_name_normalized";
    }

    async function refreshRows() {
      document.getElementById("statusText").textContent = "loading WEC live grid";
      try {
        sourceRows = normalizePayload(await fetchJson(dataUrl(PRIMARY_API)), "function");
      } catch (primaryError) {
        try {
          sourceRows = normalizePayload(await fetchJson(dataUrl(LEGACY_FALLBACK_API)), "legacy-fallback");
        } catch (fallbackError) {
          sourceRows = SAMPLE_ROWS.slice();
          currentMeta = { source: "sample", show_no: "", focus_date: "", last_updated: "" };
          updateRows();
          document.getElementById("statusText").textContent = "Sample fallback; live endpoint failed";
          return;
        }
      }
      updateRows();
    }

    function printRollup(row) {
      const value = Array.isArray(row.horse_items)
        ? row.horse_items.map(item => `${text(item.label)}${item.order ? " (" + text(item.order) + ")" : ""}`).join("  ")
        : "";
      return value ? `<div class="print-rollup">${escapeHtml(value)}</div>` : "";
    }

    function buildPrintSheet() {
      const generatedAt = new Date().toISOString();
      const body = groupedRows().map(group => {
        const rows = group.rows.map((row, index) => {
          const rowClass = "print-row" +
            (index % 2 ? " is-zebra" : "") +
            (Array.isArray(row.horse_items) && row.horse_items.length ? " has-rollup" : "");
          return `<div class="${rowClass}">` +
            printRollup(row) +
            `<span class="print-cell time">${escapeHtml(row.time || "--")}</span>` +
            `<span class="print-cell class">${escapeHtml(text(row.class_number) + " - " + text(row.class_name))}</span>` +
            `</div>`;
        }).join("");
        return `<section class="print-ring-group">` +
          `<div class="print-ring">${escapeHtml(group.label)}</div>` +
          rows +
          `</section>`;
      }).join("");
      document.querySelector(".print-sheet").innerHTML =
        `<div class="print-title"><h1>${escapeHtml(document.getElementById("appTitle").textContent)}</h1><p>${escapeHtml(metadataStatusPrefix())}<br>${escapeHtml(generatedAt)}</p></div>` +
        `<div class="print-columns">${body}</div>`;
    }

    function printGrid() {
      buildPrintSheet();
      setTimeout(() => window.print(), 50);
    }

    const columnDefs = [
      { headerName: "TIME", field: "time", minWidth: 78, width: 88 },
      { headerName: "RING", field: "display_ring", minWidth: 78, width: 88 },
      { headerName: "NO", field: "class_number", minWidth: 56, width: 60 },
      { headerName: "CLASS", field: "class_name", minWidth: 170, flex: 1 }
    ];

    const gridOptions = {
      rowData: [],
      columnDefs,
      defaultColDef: {
        sortable: false,
        filter: false,
        resizable: false,
        suppressHeaderMenuButton: true
      },
      getRowId: (params) => params.data.row_key,
      isFullWidthRow: (params) => {
        const type = params.rowNode && params.rowNode.data && params.rowNode.data.row_type;
        return type === "ring" || type === "class";
      },
      fullWidthCellRenderer: fullWidthRenderer,
      getRowHeight: (params) => {
        const type = params.data && params.data.row_type;
        if (type === "ring") return 34;
        if (type === "class") return params.data && params.data.has_rollup ? 72 : 42;
        return undefined;
      },
      animateRows: false,
      ensureDomOrder: true,
      suppressMovableColumns: true,
      suppressCellFocus: true,
      suppressHeaderFocus: true,
      onGridReady: (event) => {
        gridApi = event.api;
        refreshRows();
      }
    };

    const gridDiv = document.getElementById("agBaseGrid");
    if (window.agGrid && typeof window.agGrid.createGrid === "function") {
      window.agGrid.createGrid(gridDiv, gridOptions);
    }

    document.getElementById("ringAnchors").addEventListener("click", (event) => {
      const button = event.target.closest("button[data-ring-name-normalized]");
      if (!button || !gridApi) return;
      const key = button.getAttribute("data-ring-name-normalized");
      const group = groupedRows().find(item => item.key === key);
      const first = group && group.rows[0];
      if (!first) return;
      const node = gridApi.getRowNode(first.row_key);
      if (node) gridApi.ensureNodeVisible(node, "top");
    });

    document.getElementById("focusBtn").addEventListener("click", (event) => {
      focusMode = !focusMode;
      event.currentTarget.classList.toggle("tap-active", focusMode);
      event.currentTarget.setAttribute("aria-pressed", String(focusMode));
      updateRows();
    });

    document.getElementById("horseBtn").addEventListener("click", (event) => {
      horseFilterOpen = !horseFilterOpen;
      document.getElementById("horseFilters").hidden = !horseFilterOpen;
      event.currentTarget.classList.toggle("tap-active", horseFilterOpen);
      event.currentTarget.setAttribute("aria-pressed", String(horseFilterOpen));
    });

    document.getElementById("horseFilters").addEventListener("click", (event) => {
      const button = event.target.closest("button[data-barn-name]");
      if (!button) return;
      const key = button.getAttribute("data-barn-name");
      if (activeHorseFilters.has(key)) activeHorseFilters.delete(key);
      else activeHorseFilters.add(key);
      updateRows();
    });

    document.getElementById("printBtn").addEventListener("click", printGrid);
