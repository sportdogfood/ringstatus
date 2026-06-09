(function () {
  var config = window.RS_DYNAMIC_SECTION_CONFIG || {};
  var root = document.querySelector(config.rootSelector || "#rs-dynamic-section");
  if (!root) return;

  var endpointUrl = config.endpointUrl || root.getAttribute("data-endpoint-url") || "/rs-dynamic-section/content";
  var pageKey = config.pageKey || root.getAttribute("data-page-key") || "home";
  var blockKey = config.blockKey || root.getAttribute("data-block-key") || "";

  root.setAttribute("data-rs-dynamic-status", "loading");
  loadSection(endpointUrl, pageKey, blockKey)
    .then(function (payload) {
      if (!payload || !payload.ok) {
        throw new Error((payload && (payload.error || payload.detail)) || "rs_dynamic_section_failed");
      }
      root.innerHTML = payload.html || "";
      root.setAttribute("data-rs-dynamic-status", "ready");
      root.dispatchEvent(new CustomEvent("rs:dynamic-section-ready", {
        bubbles: true,
        detail: payload
      }));
    })
    .catch(function (error) {
      root.setAttribute("data-rs-dynamic-status", "failed");
      root.innerHTML = '<div class="rs-dynamic-error">Dynamic section failed: ' + escapeHtml(error.message || error) + "</div>";
    });

  function loadSection(endpointUrl, pageKey, blockKey) {
    var url = new URL(endpointUrl, window.location.href);
    url.searchParams.set("pageKey", pageKey);
    if (blockKey) url.searchParams.set("blockKey", blockKey);
    url.searchParams.set("_", Date.now());
    return fetch(url.toString(), { cache: "no-store" }).then(function (response) {
      return response.json();
    });
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, function (char) {
      return {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char];
    });
  }
})();
