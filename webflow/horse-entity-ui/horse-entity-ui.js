(function () {
  const root = document.getElementById("horse-entity-ui");
  if (!root) return;

  const globalConfig = window.HORSE_ENTITY_UI_CONFIG || {};
  const config = {
    apiUrl: root.dataset.apiUrl || globalConfig.apiUrl || "/horse-entity-ui"
  };

  const ui = {
    loading: true,
    error: "",
    search: "",
    tab: "all",
    drawerOpen: false,
    selectedHorseId: "",
    addOpen: false,
    saving: false,
    draft: {}
  };

  let state = null;
  let records = [];

  load();

  root.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    const action = target.dataset.action;

    if (action === "set-tab") {
      ui.tab = target.dataset.tab || "all";
      rebuild();
      return;
    }
    if (action === "open-horse") {
      ui.selectedHorseId = target.dataset.horseId || "";
      ui.drawerOpen = true;
      ui.addOpen = false;
      ui.draft = draftFromHorse(selectedHorse());
      render();
      return;
    }
    if (action === "close-drawer") {
      ui.drawerOpen = false;
      ui.addOpen = false;
      render();
      return;
    }
    if (action === "open-add") {
      ui.addOpen = true;
      ui.drawerOpen = true;
      ui.selectedHorseId = "";
      ui.draft = {};
      render();
      return;
    }
    if (action === "clear-search") {
      ui.search = "";
      rebuild();
      return;
    }
    if (action === "save-horse") {
      await saveHorse();
    }
  });

  root.addEventListener("input", (event) => {
    const input = event.target;
    if (input.matches("[data-search]")) {
      ui.search = input.value || "";
      rebuild();
      requestAnimationFrame(() => {
        const next = root.querySelector("[data-search]");
        if (next) {
          next.focus();
          next.setSelectionRange(ui.search.length, ui.search.length);
        }
      });
      return;
    }
    if (input.matches("[data-field]")) {
      const key = input.dataset.field;
      ui.draft[key] = input.type === "checkbox" ? input.checked : input.value;
    }
  });

  async function load() {
    ui.loading = true;
    ui.error = "";
    render();
    try {
      state = await fetchJson(apiUrl());
      rebuild(false);
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.loading = false;
      render();
    }
  }

  async function saveHorse() {
    const fields = {};
    for (const key of state?.allowedFields?.write || []) {
      if (Object.prototype.hasOwnProperty.call(ui.draft, key)) fields[key] = ui.draft[key];
    }
    if (!Object.keys(fields).length) return;
    ui.saving = true;
    render();
    try {
      const payload = ui.addOpen
        ? { action: "add_horse", fields, sessionId: sessionId() }
        : { action: "edit_horse", horseId: ui.selectedHorseId, fields, sessionId: sessionId() };
      const result = await fetchJson(apiUrl(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state = result.state || state;
      ui.addOpen = false;
      ui.selectedHorseId = result.result?.created?.id || ui.selectedHorseId;
      ui.drawerOpen = !!ui.selectedHorseId;
      rebuild(false);
      ui.draft = draftFromHorse(selectedHorse());
    } catch (error) {
      ui.error = error.message || String(error);
    } finally {
      ui.saving = false;
      render();
    }
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || `${response.status}`);
    return data;
  }

  function apiUrl() {
    const url = new URL(config.apiUrl, window.location.href);
    if (ui.search.trim()) url.searchParams.set("q", ui.search.trim());
    return url.toString();
  }

  function rebuild(shouldRender = true) {
    records = (state?.horses || []).filter(matchesTab).filter(matchesSearch);
    if (!records.some((row) => row.id === ui.selectedHorseId)) ui.selectedHorseId = records[0]?.id || "";
    if (shouldRender) render();
  }

  function render() {
    if (ui.loading && !state) {
      root.innerHTML = `<div class="rs-airtable-shell"><div class="rs-stack-section"><div class="rs-stack-label">Loading</div></div></div>`;
      return;
    }
    root.innerHTML = `
      <div class="rs-airtable-shell">
        <div class="rs-page-stack">
          ${section("header", headerHtml(), "is-header")}
          ${section("primary_tabs", tabsHtml(), "is-primary-tabs")}
          ${section("summary_aggs", summaryHtml(), "is-summary-aggs")}
          ${section("search", searchHtml(), "is-search")}
          ${section("main_table", tableHtml(), "is-main-table")}
          ${section("comments", commentsPageHtml(), "is-comments")}
          ${section("change_log", changeLogPageHtml(), "is-change-log")}
        </div>
        ${drawerHtml()}
      </div>
    `;
  }

  function section(key, html, className) {
    return `<section class="rs-stack-section ${className || ""}" data-render-key="${escapeAttr(key)}">${html}</section>`;
  }

  function headerHtml() {
    return `<div class="rs-page-header"><div><div class="rs-page-title">Horse Entity</div><div class="rs-page-subtitle">Roster review, profile edits, memberships, comments, and audit</div></div><button class="rs-stack-pill is-active" type="button" data-action="open-add">Add Horse</button></div>`;
  }

  function tabsHtml() {
    const tabs = [
      ["all", "All"],
      ["active", "Active"],
      ["inactive", "Inactive"],
      ["wave_one", "Wave One"],
      ["wave_two", "Wave Two"],
      ["not_going", "Not Going"]
    ];
    return `<div class="rs-stack-tabs">${tabs.map(([key, label]) => `<button class="rs-stack-pill ${ui.tab === key ? "is-active" : ""}" type="button" data-action="set-tab" data-tab="${escapeAttr(key)}">${escapeHtml(label)}</button>`).join("")}</div>`;
  }

  function summaryHtml() {
    const counts = state?.counts || {};
    return `<div class="rs-stack-label">Summary</div><div class="rs-stack-aggs">${agg(records.length, "VISIBLE", "visible", "brown")}${agg(counts.active || 0, "ACTIVE", "active", "green")}${agg(counts.changes || 0, "CHANGES", "changes", "grey")}</div>`;
  }

  function agg(value, label, key, shade) {
    return `<div class="rs-stack-agg is-${escapeAttr(key)} is-shade-${escapeAttr(shade)}"><div class="rs-stack-agg-value">${escapeHtml(value)}</div><div class="rs-stack-agg-label">${escapeHtml(label)}</div></div>`;
  }

  function searchHtml() {
    return `<div class="rs-airtable-toolbar"><div class="rs-search-wrap"><input class="rs-search" type="text" data-search autocomplete="off" placeholder="Search horses" value="${escapeAttr(ui.search)}"><button class="rs-search-clear ${ui.search ? "is-active" : ""}" type="button" aria-label="Clear search" data-action="clear-search"><span aria-hidden="true">&times;</span></button></div></div>${ui.error ? `<div class="rs-status is-error">${escapeHtml(ui.error)}</div>` : ""}`;
  }

  function tableHtml() {
    return `
      <div class="rs-table-stack-head"><div class="rs-stack-label">Horses</div><div class="rs-stack-label">${escapeHtml(records.length)} records</div></div>
      <div class="rs-airtable-scroll">
        <table class="rs-airtable-grid">
          <thead><tr><th class="rs-row-gutter">#</th><th>HORSE</th><th>STATUS</th><th>MEMBERSHIP</th><th>COMMENTS</th></tr></thead>
          <tbody>${records.map(rowHtml).join("") || `<tr><td colspan="5"><div class="rs-empty-row">No horses.</div></td></tr>`}</tbody>
        </table>
      </div>
    `;
  }

  function rowHtml(horse, index) {
    return `<tr class="${horse.id === ui.selectedHorseId && ui.drawerOpen ? "is-selected" : ""}" data-action="open-horse" data-horse-id="${escapeAttr(horse.id)}" tabindex="0">
      <td class="rs-row-gutter">${index + 1}</td>
      <td class="rs-entity-cell"><div class="rs-entity-main"><span class="rs-entity-horse">${escapeHtml(horse.name)}</span><span class="rs-entity-sub">${escapeHtml(horse.showName || horse.barnName || "")}</span></div></td>
      <td>${horse.active ? "Active" : "Inactive"}</td>
      <td>${escapeHtml(membershipLabel(horse))}</td>
      <td>${horseComments(horse.id).length}</td>
    </tr>`;
  }

  function drawerHtml() {
    if (!ui.drawerOpen) return "";
    const horse = selectedHorse();
    return `<div class="rs-drawer-overlay is-open" data-action="close-drawer" aria-hidden="true"></div>
    <aside class="rs-record-drawer is-open" aria-hidden="false">
      <div class="rs-drawer-head">
        <div class="rs-drawer-title-group"><div class="rs-page-subtitle">${escapeHtml(ui.addOpen ? "Add Horse" : horse?.name || "Horse")}</div>${horse?.profileUrl ? `<a class="rs-drawer-profile-link" href="${escapeAttr(horse.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>` : ""}</div>
        <button class="rs-drawer-close" type="button" data-action="close-drawer" aria-label="Close"><span aria-hidden="true">&times;</span></button>
      </div>
      <div class="rs-drawer-body">
        ${formHtml(horse)}
        ${!ui.addOpen ? membershipsHtml(horse) : ""}
        ${!ui.addOpen ? attributesHtml() : ""}
        ${!ui.addOpen ? drawerCommentsHtml(horse) : ""}
        ${!ui.addOpen ? drawerChangeLogHtml(horse) : ""}
      </div>
    </aside>`;
  }

  function formHtml(horse) {
    const fields = state?.allowedFields?.write || [];
    return `<div class="rs-detail-panel"><div class="rs-stack-label">Details</div>${fields.map((field) => fieldInput(field)).join("")}<button class="rs-plain-button is-primary" type="button" data-action="save-horse">${ui.saving ? "Saving" : ui.addOpen ? "Add Horse" : "Save Changes"}</button></div>`;
  }

  function fieldInput(field) {
    const value = ui.draft[field];
    if (field === "active" || field === "inactive" || field === "wec_wave_1" || field === "wec_wave_2" || field === "wec_not_going") {
      return `<label class="rs-check-row"><span>${escapeHtml(fieldLabel(field))}</span><input type="checkbox" data-field="${escapeAttr(field)}" ${value ? "checked" : ""}></label>`;
    }
    return `<label class="rs-field-row"><span class="rs-stack-label">${escapeHtml(fieldLabel(field))}</span><input class="rs-field-input" type="text" data-field="${escapeAttr(field)}" value="${escapeAttr(value || "")}"></label>`;
  }

  function membershipsHtml(horse) {
    if (!horse) return "";
    const rows = [
      ["Waves", horse.memberships?.waveKeys || []],
      ["Plans", horse.memberships?.planIds || []],
      ["Pack Lists", horse.memberships?.packListIds || []],
      ["Pack Waves", horse.memberships?.packWaveIds || []],
      ["Kits", horse.memberships?.kitIds || []],
      ["Kit Items", horse.memberships?.kitItemIds || []]
    ];
    return `<div class="rs-detail-panel"><div class="rs-stack-label">List Memberships</div>${rows.map(([label, values]) => `<div class="rs-meta-row"><span>${escapeHtml(label)}</span><strong>${escapeHtml(values.length ? values.join(", ") : "None")}</strong></div>`).join("")}</div>`;
  }

  function attributesHtml() {
    const attrs = state?.attributes || {};
    return `<div class="rs-detail-panel"><div class="rs-stack-label">Attributes</div>${attributeGroup("Gender", attrs.gender)}${attributeGroup("Disciplines", attrs.disciplines)}${attributeGroup("Colors", attrs.colors)}</div>`;
  }

  function attributeGroup(label, rows) {
    return `<div class="rs-attribute-group"><div class="rs-stack-label">${escapeHtml(label)}</div><div class="rs-chip-row">${(rows || []).map((row) => `<span class="rs-chip">${escapeHtml(row.label)}</span>`).join("") || `<span class="rs-empty-row">No options.</span>`}</div></div>`;
  }

  function drawerCommentsHtml(horse) {
    const comments = horseComments(horse?.id);
    return `<div class="rs-comments"><div class="rs-stack-label">Comments</div><div class="rs-comment-list">${comments.map(commentHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}</div></div>`;
  }

  function drawerChangeLogHtml(horse) {
    const changes = horseChanges(horse?.id);
    return `<div class="rs-detail-panel"><div class="rs-stack-label">Change Log</div>${changes.slice(0, 12).map(changeHtml).join("") || `<div class="rs-empty-row">No changes.</div>`}</div>`;
  }

  function commentsPageHtml() {
    const comments = state?.comments || [];
    return `<div class="rs-comments is-page-comments"><div class="rs-stack-label">Comments</div><div class="rs-comment-list">${comments.slice(0, 20).map(commentHtml).join("") || `<div class="rs-empty-row">No comments.</div>`}</div></div>`;
  }

  function changeLogPageHtml() {
    const changes = state?.changeLog || [];
    return `<div class="rs-comments is-page-comments"><div class="rs-stack-label">Change Log</div><div class="rs-comment-list">${changes.slice(0, 20).map(changeHtml).join("") || `<div class="rs-empty-row">No changes.</div>`}</div></div>`;
  }

  function commentHtml(comment) {
    return `<div class="rs-comment-row"><div class="rs-comment-body">${escapeHtml(comment.comment || "")}</div><div class="rs-comment-meta">${escapeHtml(comment.scopeLabel || comment.createdBy || "")}</div></div>`;
  }

  function changeHtml(change) {
    return `<div class="rs-comment-row"><div class="rs-comment-body">${escapeHtml(change.fieldName || "field")} changed from ${escapeHtml(change.oldValue)} to ${escapeHtml(change.newValue)}</div><div class="rs-comment-meta">${escapeHtml(change.changedAt || change.changedBy || "")}</div></div>`;
  }

  function matchesSearch(horse) {
    const q = ui.search.trim().toLowerCase();
    if (!q) return true;
    return [horse.name, horse.barnName, horse.showName, horse.notes].join(" ").toLowerCase().includes(q);
  }

  function matchesTab(horse) {
    if (ui.tab === "active") return horse.active;
    if (ui.tab === "inactive") return !horse.active;
    if (ui.tab === "wave_one") return horse.waveOne;
    if (ui.tab === "wave_two") return horse.waveTwo;
    if (ui.tab === "not_going") return horse.notGoing;
    return true;
  }

  function selectedHorse() {
    return (state?.horses || []).find((horse) => horse.id === ui.selectedHorseId) || null;
  }

  function horseComments(horseId) {
    return (state?.comments || []).filter((comment) => (comment.horseIds || []).includes(horseId));
  }

  function horseChanges(horseId) {
    return (state?.changeLog || []).filter((change) => change.horseId === horseId);
  }

  function membershipLabel(horse) {
    const keys = horse.memberships?.waveKeys || [];
    return keys.length ? keys.join(", ") : "Review";
  }

  function draftFromHorse(horse) {
    return {
      horse: horse?.name || "",
      barn_name: horse?.barnName || "",
      show_name: horse?.showName || "",
      active: horse?.active !== false,
      inactive: !!horse?.inactive,
      wec_wave_1: !!horse?.waveOne,
      wec_wave_2: !!horse?.waveTwo,
      wec_not_going: !!horse?.notGoing,
      notes: horse?.notes || ""
    };
  }

  function fieldLabel(field) {
    const labels = {
      horse: "Horse",
      barn_name: "Barn name",
      show_name: "Show name",
      active: "Active",
      inactive: "Inactive",
      wec_wave_1: "Wave One",
      wec_wave_2: "Wave Two",
      wec_not_going: "Not Going",
      notes: "Notes"
    };
    return labels[field] || field.replace(/_/g, " ");
  }

  function sessionId() {
    try {
      const key = "horse_entity_ui_session";
      const existing = window.sessionStorage.getItem(key);
      if (existing) return existing;
      const next = `hui_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      window.sessionStorage.setItem(key, next);
      return next;
    } catch (_) {
      return `hui_${Date.now()}`;
    }
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }
})();
