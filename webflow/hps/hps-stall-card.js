(async () => {
  const root = document.getElementById("hps-stall-card");
  if (!root) return;

  const config = window.HPS_STALL_CARD_CONFIG || {};
  const apiUrl = config.apiUrl || "/test/hps/horses";
  const logoUrl = config.logoUrl || "";
  const params = new URL(window.location.href).searchParams;
  const tenantId = String(params.get("tenantId") || params.get("tenant_id") || config.tenantId || "").trim();
  const horseRecordId = String(params.get("horseRecordId") || params.get("recordId") || params.get("id") || "").trim();
  const autoPrint = truthy(params.get("autoprint"));

  if (!tenantId || !horseRecordId) {
    renderMessage("Missing tenantId or horseRecordId.");
    return;
  }

  try {
    renderMessage("Loading stall card...");
    const url = new URL(apiUrl, window.location.href);
    url.searchParams.set("tenantId", tenantId);
    const response = await fetchWithTimeout(url.toString(), 15000);
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.detail || data.error || `Load failed: ${response.status}`);
    }

    const record = (data.records || []).find((item) => item.id === horseRecordId);
    if (!record) throw new Error(`Horse not found in tenant view: ${horseRecordId}`);

    renderCard(record.fields || {});
    if (autoPrint) window.setTimeout(() => window.print(), 250);
  } catch (error) {
    console.error("[hps-stall-card]", error);
    renderMessage(error instanceof Error ? error.message : String(error));
  }

  function renderCard(fields) {
    const showName = firstValue(fields, ["show_name", "horse", "name", "Horse", "Name"]);
    const barnName = firstValue(fields, ["barn_name", "Barn Name", "barn"]) || showName;
    const color = firstValue(fields, ["horse_colors", "color", "horse_color", "Color"]);
    const gender = firstValue(fields, ["horse_genders", "gender", "horse_gender", "Gender"]);
    const emergencyName = firstValue(fields, ["emergency_contacts", "emergency_contact", "Emergency Contact"]);
    const emergencyPhone = firstValue(fields, ["emergency_phone", "emergency_no", "Emergency Phone"]);
    const colorGender = [color, gender].filter(Boolean).join(" ");

    root.innerHTML = `
      <div class="hps-stall-sheet">
        <div class="hps-stall-page">
          <div class="hps-stall-card">
            <div class="hps-stall-logo">
              ${logoUrl ? `<img src="${escapeAttr(logoUrl)}" alt="Castlewood Farm" onerror="this.style.display='none'">` : ""}
            </div>
            <div class="hps-stall-main">
              <div class="hps-stall-barn">${escapeHtml(barnName || "Horse")}</div>
              <div class="hps-stall-show">${escapeHtml(showName || "")}</div>
              <div class="hps-stall-meta">${escapeHtml(colorGender || "")}</div>
            </div>
            <div class="hps-stall-emergency">
              <div class="hps-stall-em-label">*Emergency Contact*</div>
              <div class="hps-stall-em-name">${escapeHtml(emergencyName || "")}</div>
              <div class="hps-stall-em-phone">${escapeHtml(emergencyPhone || "")}</div>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function renderMessage(message) {
    root.innerHTML = `<div class="hps-stall-message">${escapeHtml(message)}</div>`;
  }

  function fetchWithTimeout(url, timeoutMs) {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), timeoutMs);
    return fetch(url, { signal: controller.signal }).finally(() => window.clearTimeout(timer));
  }

  function firstValue(fields, names) {
    for (const name of names) {
      if (fields[name] !== undefined && fields[name] !== null && fields[name] !== "") return fields[name];
    }
    return "";
  }

  function truthy(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return ["1", "true", "yes", "y"].includes(normalized);
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
    return escapeHtml(value);
  }
})();
