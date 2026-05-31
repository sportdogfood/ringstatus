(function () {
  const config = window.RSA_DASHBOARD_CONFIG || {};
  const apiUrl = config.apiUrl || "";
  let root = document.querySelector("[data-rsa-dashboard]");
  let state;
  let els;

  if (!root) {
    mountDashboard().then((mountedRoot) => {
      root = mountedRoot;
      if (root) bindDashboard();
    });
    return;
  }

  bindDashboard();

  function bindDashboard() {
    state = {
      records: new Map(),
      activeRecordId: "",
      saveTimers: new Map()
    };

    els = {
      modal: root.querySelector("[data-rsa-modal]"),
      modalCard: root.querySelector(".rsa-modal-card"),
      modalContent: root.querySelector("[data-rsa-modal-content]"),
      status: root.querySelector("[data-rsa-status]")
    };

    root.querySelectorAll("[data-rsa-row]").forEach((row) => {
      const record = readRowRecord(row);
      if (record.id) state.records.set(record.id, record);
    });

    root.addEventListener("click", handleClick);
    root.addEventListener("focusin", handleFocusIn);
    root.addEventListener("keydown", handleKeyDown);
    root.addEventListener("focusout", handleFocusOut);
    root.addEventListener("input", handleInput);
    root.addEventListener("change", handleChange);

    setupStickyHead();
  }

  async function mountDashboard() {
    const mount = document.querySelector("#rsa-dashboard-app");
    const templateUrl = config.templateUrl || scriptSiblingUrl("rsa-dashboard.html");
    if (!mount || !templateUrl) return null;

    try {
      const response = await fetch(templateUrl, { cache: "no-store" });
      if (!response.ok) throw new Error(`Template failed: ${response.status}`);

      const html = await response.text();
      const doc = new DOMParser().parseFromString(html, "text/html");
      const templateRoot = doc.querySelector("[data-rsa-dashboard]");
      if (!templateRoot) throw new Error("Template missing data-rsa-dashboard");

      mount.replaceWith(templateRoot);
      return templateRoot;
    } catch (error) {
      mount.innerHTML = `<div class="rsa-text is-xs">RSA dashboard failed to load: ${escapeHtml(error instanceof Error ? error.message : String(error))}</div>`;
      return null;
    }
  }

  function scriptSiblingUrl(fileName) {
    const scripts = Array.from(document.scripts);
    const script = scripts.find((item) => (item.src || "").includes("rsa-dashboard.js"));
    if (!script || !script.src) return "";
    return new URL(fileName, script.src).toString();
  }

  function handleClick(event) {
    const openPanel = event.target.closest("[data-rsa-open]");
    if (openPanel) {
      togglePanel(openPanel.getAttribute("data-rsa-open"));
      return;
    }

    if (event.target.closest("[data-rsa-close]")) {
      hidePanels();
      return;
    }

    const tab = event.target.closest("[data-rsa-tab]");
    if (tab) {
      setActiveTab(tab);
      return;
    }

    if (event.target.closest("[data-rsa-run-search]")) {
      runSearch();
      return;
    }

    if (event.target.closest("[data-rsa-print]")) {
      window.print();
      return;
    }

    const edit = event.target.closest("[data-rsa-edit]");
    if (edit) {
      const row = edit.closest("[data-rsa-row]");
      if (row) openDetail(row.getAttribute("data-record-id"));
      return;
    }

    if (event.target.closest("[data-rsa-modal-close]")) {
      closeDetail();
      return;
    }

    if (event.target.closest("[data-rsa-comment-add]")) {
      addComment();
    }
  }

  function handleFocusIn(event) {
    const input = event.target.closest("[data-input]");
    if (input && input.textContent.trim().toLowerCase() === "input") {
      input.textContent = "";
    }

    const comment = event.target.closest("[data-rsa-comment-input]");
    if (comment && comment.textContent.trim().toLowerCase() === "comment") {
      comment.textContent = "";
    }
  }

  function handleKeyDown(event) {
    if (event.key !== "Enter") return;
    if (event.target.closest("[data-input]") || event.target.closest("[data-rsa-comment-input]")) {
      event.preventDefault();
      event.target.blur();
    }
  }

  function handleFocusOut(event) {
    const input = event.target.closest("[data-input]");
    if (input) updateInlineQuantity(input);
  }

  function handleInput(event) {
    const field = event.target.closest("[data-rsa-field]");
    if (!field) return;

    const recordId = field.getAttribute("data-record-id") || state.activeRecordId;
    const fieldName = field.getAttribute("data-rsa-field");
    const value = field.value ?? field.textContent.trim();
    queueWrite(recordId, { [fieldName]: value }, field);
  }

  function handleChange(event) {
    const choice = event.target.closest("[data-rsa-choice]");
    if (!choice) return;

    const recordId = choice.getAttribute("data-record-id") || state.activeRecordId;
    const fieldName = choice.getAttribute("data-rsa-choice");
    queueWrite(recordId, { [fieldName]: choice.value }, choice);
    refreshChoiceState(choice.name);
  }

  function hidePanels() {
    root.querySelectorAll("[data-rsa-panel]").forEach((panel) => {
      panel.hidden = true;
      panel.classList.remove("is-open");
    });
  }

  function togglePanel(name) {
    const panel = root.querySelector(`[data-rsa-panel="${cssEscape(name)}"]`);
    const willOpen = panel && panel.hidden;
    hidePanels();
    if (panel && willOpen) {
      panel.hidden = false;
      panel.classList.add("is-open");
    }
  }

  function setActiveTab(tab) {
    const row = tab.closest(".rsa-list-action-menu");
    if (!row) return;
    row.querySelectorAll(".rsa-tab-link").forEach((item) => item.classList.remove("is-current"));
    tab.classList.add("is-current");
  }

  function updateInlineQuantity(input) {
    const row = input.closest("[data-rsa-row]");
    if (!row) return;

    const record = readRowRecord(row);
    const value = Number(input.textContent.trim());
    if (Number.isFinite(value) && input.textContent.trim() !== "") {
      const packed = Math.max(0, Math.min(record.need, value));
      const left = Math.max(0, record.need - packed);
      row.querySelector("[data-packed]").textContent = String(packed);
      row.querySelector("[data-left]").textContent = String(left);
      input.classList.add("is-dirty");
      record.packed = packed;
      record.left = left;
      state.records.set(record.id, record);
      queueWrite(record.id, { quantity_packed: packed }, input);
    } else {
      input.textContent = "input";
      input.classList.remove("is-dirty");
    }
  }

  function runSearch() {
    const box = root.querySelector(".rsa-search-input");
    const query = (box && box.textContent || "").trim().toLowerCase();
    root.querySelectorAll("[data-rsa-row]").forEach((row) => {
      const name = (row.querySelector("[data-rsa-name]") || {}).textContent || "";
      row.hidden = query && query !== "search" ? !name.toLowerCase().includes(query) : false;
    });
  }

  function readRowRecord(row) {
    return {
      id: row.getAttribute("data-record-id") || "",
      name: text(row, "[data-rsa-name]"),
      need: number(row, "[data-need]"),
      packed: number(row, "[data-packed]"),
      left: number(row, "[data-left]"),
      status: row.getAttribute("data-status") || "active",
      commentCount: Number(row.getAttribute("data-comment-count") || 0)
    };
  }

  function openDetail(recordId) {
    const record = state.records.get(recordId);
    if (!record || !els.modal || !els.modalContent) return;

    state.activeRecordId = recordId;
    els.modalContent.innerHTML = detailHtml(record);
    els.modal.hidden = false;
    els.modal.setAttribute("aria-hidden", "false");
    requestAnimationFrame(() => els.modalCard?.focus());
  }

  function closeDetail() {
    if (!els.modal || !els.modalContent) return;
    state.activeRecordId = "";
    els.modal.hidden = true;
    els.modal.setAttribute("aria-hidden", "true");
    els.modalContent.innerHTML = "";
  }

  function detailHtml(record) {
    const checked = (value) => record.status === value ? " checked" : "";
    return `
      <div class="rsa-detail" data-rsa-detail="${escapeAttr(record.id)}">
        <div class="rsa-detail-head">
          <div class="rsa-H2">${escapeHtml(record.name)}</div>
          <div class="rsa-text is-xs" data-rsa-detail-status>Changes save to Airtable.</div>
        </div>

        <div class="rsa-detail-list">
          ${detailRow("Need", record.need)}
          ${detailRow("Packed", record.packed)}
          ${detailRow("Left", record.left)}
        </div>

        <div class="rsa-edit-panel">
          <div class="rsa-edit-row">
            <div class="rsa-edit-label rsa-text is-xs">packed</div>
            <input class="rsa-edit-field rsa-text is-number" type="number" min="0" max="${record.need}" value="${record.packed}" data-rsa-field="quantity_packed" data-record-id="${escapeAttr(record.id)}">
          </div>
          <div class="rsa-edit-row">
            <div class="rsa-edit-label rsa-text is-xs">state</div>
            <div class="rsa-edit-choices">
              <label class="rsa-edit-choice"><input type="radio" name="state-${escapeAttr(record.id)}" value="active" data-rsa-choice="record_state" data-record-id="${escapeAttr(record.id)}"${checked("active")}><span class="rsa-edit-pill rsa-text is-link">active</span></label>
              <label class="rsa-edit-choice"><input type="radio" name="state-${escapeAttr(record.id)}" value="inactive" data-rsa-choice="record_state" data-record-id="${escapeAttr(record.id)}"${checked("inactive")}><span class="rsa-edit-pill rsa-text is-link">inactive</span></label>
              <label class="rsa-edit-choice"><input type="radio" name="state-${escapeAttr(record.id)}" value="ignore" data-rsa-choice="record_state" data-record-id="${escapeAttr(record.id)}"${checked("ignore")}><span class="rsa-edit-pill rsa-text is-link">ignore</span></label>
            </div>
          </div>
        </div>

        <div class="rsa-comment-panel" data-rsa-comments>
          <div class="rsa-comment-head">
            <div class="rsa-text is-xs">comments</div>
            <div class="rsa-text is-link" data-rsa-comment-add>add</div>
          </div>
          <div class="rsa-comment-list" data-rsa-comment-list>
            ${record.commentCount ? `<div class="rsa-comment-item"><div class="rsa-text">Existing Airtable comments: ${record.commentCount}</div><div class="rsa-text is-xs">source</div></div>` : ""}
          </div>
          <div class="rsa-comment-input rsa-text" contenteditable="true" data-rsa-comment-input>comment</div>
        </div>
      </div>
    `;
  }

  function detailRow(label, value) {
    return `
      <div class="rsa-detail-row">
        <div class="rsa-detail-label rsa-text is-xs">${escapeHtml(label)}</div>
        <div class="rsa-detail-value rsa-text">${escapeHtml(String(value))}</div>
      </div>
    `;
  }

  function addComment() {
    const input = root.querySelector("[data-rsa-comment-input]");
    const list = root.querySelector("[data-rsa-comment-list]");
    if (!input || !list || !state.activeRecordId) return;

    const comment = input.textContent.trim();
    if (!comment || comment.toLowerCase() === "comment") return;

    const item = document.createElement("div");
    item.className = "rsa-comment-item";
    item.innerHTML = `<div class="rsa-text">${escapeHtml(comment)}</div><div class="rsa-text is-xs">pending</div>`;
    list.appendChild(item);
    input.textContent = "comment";
    queueWrite(state.activeRecordId, { comment }, item);
  }

  function queueWrite(recordId, fields, source) {
    if (!recordId) return;
    const key = `${recordId}:${Object.keys(fields).sort().join(",")}`;
    clearTimeout(state.saveTimers.get(key));
    state.saveTimers.set(key, setTimeout(() => writeChange(recordId, fields, source), 350));
  }

  async function writeChange(recordId, fields, source) {
    setSaveState("saving", "Saving change...");
    source?.classList?.add("is-dirty");

    try {
      if (!apiUrl) {
        throw new Error("Missing RSA_DASHBOARD_CONFIG.apiUrl");
      }

      const response = await fetch(apiUrl, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ recordId, fields })
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(result.detail || result.error || `Save failed: ${response.status}`);
      }

      setSaveState("success", "Saved to Airtable.");
      source?.classList?.remove("is-dirty");
    } catch (error) {
      setSaveState("error", `Save failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  function setSaveState(kind, message) {
    const targets = [els.status, root.querySelector("[data-rsa-detail-status]")].filter(Boolean);
    targets.forEach((target) => {
      target.textContent = message;
      target.classList.remove("is-saving", "is-success", "is-error");
      target.classList.add(`is-${kind}`);
    });
  }

  function refreshChoiceState(name) {
    root.querySelectorAll(`input[name="${cssEscape(name)}"]`).forEach((input) => {
      const pill = input.nextElementSibling;
      if (pill) pill.classList.toggle("is-active", input.checked);
    });
  }

  function setupStickyHead() {
    const tableHead = root.querySelector(".rsa-item-row.is-table-head");
    const tableHeadHolder = tableHead ? tableHead.parentElement : null;
    const tableRegion = tableHead ? tableHead.closest(".rsa-content") : null;
    const dashboard = root.querySelector(".rsa-dashboard") || root;

    function updateStickyHead() {
      if (!tableHead || !tableHeadHolder || !tableRegion) return;

      const wasStuck = tableHead.classList.contains("is-stuck");
      if (wasStuck) tableHead.classList.remove("is-stuck");
      tableHead.style.removeProperty("--rsa-sticky-left");
      tableHead.style.removeProperty("--rsa-sticky-width");

      const holderRect = tableHeadHolder.getBoundingClientRect();
      const regionRect = tableRegion.getBoundingClientRect();
      const dashRect = dashboard.getBoundingClientRect();
      const headHeight = tableHead.offsetHeight;
      const shouldStick = holderRect.top <= 0 && regionRect.bottom > headHeight;

      if (shouldStick) {
        tableHeadHolder.style.height = `${headHeight}px`;
        tableHead.style.setProperty("--rsa-sticky-left", `${dashRect.left}px`);
        tableHead.style.setProperty("--rsa-sticky-width", `${dashRect.width}px`);
        tableHead.classList.add("is-stuck");
      } else {
        tableHeadHolder.style.height = "";
      }
    }

    window.addEventListener("scroll", updateStickyHead, { passive: true });
    window.addEventListener("resize", updateStickyHead);
    updateStickyHead();
  }

  function text(scope, selector) {
    return ((scope.querySelector(selector) || {}).textContent || "").trim();
  }

  function number(scope, selector) {
    return Number(text(scope, selector)) || 0;
  }

  function cssEscape(value) {
    if (window.CSS && typeof window.CSS.escape === "function") return window.CSS.escape(value || "");
    return String(value || "").replace(/"/g, '\\"');
  }

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    })[char]);
  }

  function escapeAttr(value) {
    return escapeHtml(value).replace(/`/g, "&#96;");
  }
})();
