export const config = {
  runtime: "edge"
};

const PAINTER_JS = String.raw`(function () {
  "use strict";

  var script = document.currentScript;
  var targetSelector = readAttr("data-rs-target", "#this-test-content");
  var contentKey = readAttr("data-rs-content-key", "rs_home_section_1_content_1");
  var endpointUrl = readAttr("data-rs-endpoint", "https://ringstatus.com/test/rs-dom-painter-content");
  var root = document.querySelector(targetSelector);

  if (!root) {
    document.documentElement.setAttribute("data-rs-dom-painter-status", "missing_target");
    return;
  }

  root.setAttribute("data-rs-dom-painter-status", "loading");

  var endpoint = new URL(endpointUrl, window.location.origin);
  endpoint.searchParams.set("contentKey", contentKey);
  endpoint.searchParams.set("_", Date.now());

  fetch(endpoint.toString(), { cache: "no-store" })
    .then(function (response) { return response.json(); })
    .then(function (payload) {
      if (!payload || !payload.ok) {
        throw new Error((payload && (payload.detail || payload.error)) || "Content failed");
      }

      paint(root, "h5", payload.content && payload.content.eyebrow);
      paint(root, "h1", payload.content && payload.content.headline);
      paint(root, "p", payload.content && payload.content.body);

      root.setAttribute("data-rs-dom-painter-status", "ready");
      root.setAttribute("data-rs-content-key", contentKey);
    })
    .catch(function (error) {
      root.setAttribute("data-rs-dom-painter-status", "failed");
      root.setAttribute("data-rs-dom-painter-error", error && error.message ? error.message : String(error));
    });

  function readAttr(name, fallback) {
    return (script && script.getAttribute(name)) || fallback;
  }

  function paint(scope, selector, value) {
    var node = scope.querySelector(selector);
    if (!node) return;
    node.textContent = value == null ? "" : String(value);
  }
})();`;

export const GET = async () => new Response(PAINTER_JS, {
  status: 200,
  headers: {
    "Content-Type": "application/javascript; charset=utf-8",
    "Cache-Control": "no-cache"
  }
});
