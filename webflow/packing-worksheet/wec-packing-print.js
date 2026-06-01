(async () => {
  const root = document.getElementById("wec-packing-print");
  if (!root) return;

  const config = window.WEC_PACKING_PRINT_CONFIG || {};
  const apiUrl = config.apiUrl || "/test/wec-packing/print";
  const params = new URL(window.location.href).searchParams;
  const autoPrint = truthy(params.get("autoprint"));

  try {
    renderMessage("Preparing print template...");
    const html = await loadPrintHtml();
    renderPrintHtml(html);
    if (autoPrint) {
      await waitForPrintAssets();
      window.setTimeout(() => window.print(), 100);
    }
  } catch (error) {
    console.error("[wec-packing-print]", error);
    renderMessage(error instanceof Error ? error.message : String(error));
  }

  async function loadPrintHtml() {
    const url = new URL(apiUrl, window.location.href);
    copyParam(url, "showId");
    copyParam(url, "packWaveId");
    copyParam(url, "packWaveKey");
    copyParam(url, "packWave", "packWaveKey");
    copyParam(url, "wave", "packWaveKey");
    copyParam(url, "target");
    copyParam(url, "horseId");
    const response = await fetchWithTimeout(url.toString(), 30000);
    const text = await response.text();
    if (!response.ok) throw new Error(text || `Print load failed: ${response.status}`);
    return text;
  }

  function renderPrintHtml(html) {
    const parsed = new DOMParser().parseFromString(html, "text/html");
    const title = parsed.querySelector("title")?.textContent || "WEC Packing Print";
    const styles = Array.from(parsed.querySelectorAll("style")).map((style) => style.textContent || "").join("\n");
    document.title = title;
    if (styles) {
      const style = document.createElement("style");
      style.textContent = styles;
      document.head.appendChild(style);
    }
    root.innerHTML = parsed.body?.innerHTML || html;
  }

  function renderMessage(message) {
    root.innerHTML = `<div class="wec-print-message">${escapeHtml(message)}</div>`;
  }

  function copyParam(url, sourceName, targetName = sourceName) {
    const value = params.get(sourceName);
    if (value && !url.searchParams.has(targetName)) url.searchParams.set(targetName, value);
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

  function truthy(value) {
    return ["1", "true", "yes", "y"].includes(String(value || "").trim().toLowerCase());
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }
})();
