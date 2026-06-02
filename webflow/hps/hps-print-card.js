(async () => {
  const root = document.getElementById("hps-print-card");
  if (!root) return;

  const config = window.HPS_PRINT_CARD_CONFIG || {};
  const apiUrl = config.apiUrl || "/test/hps/horses";
  const logoUrl = config.logoUrl || "";
  const params = new URL(window.location.href).searchParams;
  const tenantId = String(params.get("tenantId") || params.get("tenant_id") || config.tenantId || "").trim();
  const horseRecordId = String(params.get("horseRecordId") || params.get("recordId") || params.get("id") || config.horseRecordId || "").trim();

  if (!tenantId || !horseRecordId) {
    renderMessage("Missing tenantId or horseRecordId.");
    return;
  }

  try {
    renderMessage("Preparing stall card PDF...");
    const url = new URL(apiUrl, window.location.href);
    url.searchParams.set("tenantId", tenantId);
    url.searchParams.set("horseRecordId", horseRecordId);
    const response = await fetchWithTimeout(url.toString(), 15000);
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.detail || data.error || `Load failed: ${response.status}`);
    }

    const record = (data.records || []).find((item) => item.id === horseRecordId);
    if (!record) throw new Error(`Horse not found in tenant view: ${horseRecordId}`);

    renderCard(record.fields || {});
    await waitForPrintAssets();
    document.documentElement.dataset.hpsPrintReady = "true";
  } catch (error) {
    console.error("[hps-print-card]", error);
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
      <div class="hps-print-page">
        <div class="hps-print-card">
          <div class="hps-print-logo">
            ${logoUrl ? `<img src="${escapeAttr(logoUrl)}" alt="Castlewood Farm" onerror="this.style.display='none'">` : ""}
          </div>
          <div class="hps-print-main">
            <div class="hps-print-barn">${escapeHtml(barnName || "Horse")}</div>
            <div class="hps-print-show">${escapeHtml(showName || "")}</div>
            <div class="hps-print-meta">${escapeHtml(colorGender || "")}</div>
          </div>
          <div class="hps-print-emergency">
            <div class="hps-print-em-label">*Emergency Contact*</div>
            <div class="hps-print-em-name">${escapeHtml(emergencyName || "")}</div>
            <div class="hps-print-em-phone">${escapeHtml(emergencyPhone || "")}</div>
          </div>
        </div>
      </div>
    `;
  }

  function renderMessage(message) {
    root.innerHTML = `<div class="hps-print-message">${escapeHtml(message)}</div>`;
  }

  function fetchWithTimeout(url, timeoutMs) {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), timeoutMs);
    return fetch(url, { signal: controller.signal }).finally(() => window.clearTimeout(timer));
  }

  async function waitForPrintAssets() {
    const images = Array.from(root.querySelectorAll("img"));
    await Promise.all(images.map((image) => waitForImage(image, 3000)));
    if (document.fonts?.ready) {
      await Promise.race([document.fonts.ready.catch(() => undefined), delay(1500)]);
    }
    await nextFrame();
    await nextFrame();
  }

  function waitForImage(image, timeoutMs) {
    if (!image || (image.complete && image.naturalWidth > 0)) return Promise.resolve();
    if (image.decode) {
      return Promise.race([image.decode().catch(() => undefined), delay(timeoutMs)]);
    }
    return new Promise((resolve) => {
      const done = () => resolve();
      image.addEventListener("load", done, { once: true });
      image.addEventListener("error", done, { once: true });
      window.setTimeout(done, timeoutMs);
    });
  }

  function nextFrame() {
    return new Promise((resolve) => window.requestAnimationFrame(() => resolve()));
  }

  function delay(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  function firstValue(fields, names) {
    for (const name of names) {
      if (fields[name] !== undefined && fields[name] !== null && fields[name] !== "") return fields[name];
    }
    return "";
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
