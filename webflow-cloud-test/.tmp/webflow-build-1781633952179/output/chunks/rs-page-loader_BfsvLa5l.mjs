globalThis.process ??= {};
globalThis.process.env ??= {};
const config = {
  runtime: "edge"
};
const LOADER_JS = String.raw`(function () {
  "use strict";

  var DEFAULT_ENDPOINT = "https://ringstatus.com/test/rs-page-render";
  var DEFAULT_PAYLOAD_ENDPOINT = "https://ringstatus.com/test/rs-page-payload";
  var ROOT_SELECTOR = "#rs-page-root,[data-rs-page-root]";
  var memoryCache = Object.create(null);
  var inflight = Object.create(null);
  var pagePathMap = {
    "/rs": "rs_home",
    "/rs/": "rs_home",
    "/rs/home": "rs_home",
    "/rs/about-me": "rs_about_me",
    "/rs/company": "rs_about_company",
    "/rs/apps": "rs_apps",
    "/rs/contact": "rs_contact",
    "/rs/members": "rs_members"
  };

  function boot() {
    var root = document.querySelector(ROOT_SELECTOR);
    if (!root || root.__rsPageLoaderReady) return;
    root.__rsPageLoaderReady = true;

    var config = window.RS_PAGE_RENDER_CONFIG || {};
    var endpoint = config.endpointUrl || root.getAttribute("data-rs-endpoint") || DEFAULT_ENDPOINT;
    var payloadEndpoint = config.payloadUrl || root.getAttribute("data-rs-payload-url") || DEFAULT_PAYLOAD_ENDPOINT;
    var initialKey = config.pageKey || root.getAttribute("data-rs-page-key") || pagePathMap[window.location.pathname] || "rs_home";

    root.setAttribute("data-rs-status", "loading");
    root.setAttribute("data-rs-page-key", initialKey);

    if (findRenderedPage(root)) {
      var renderedKey = findRenderedPage(root).getAttribute("data-rs-page") || initialKey;
      root.setAttribute("data-rs-status", "ready");
      root.setAttribute("data-rs-page-key", renderedKey);
      writeCache(renderedKey, root.innerHTML);
      preloadFromRendered(root, renderedKey, endpoint, payloadEndpoint);
    } else {
      loadPage(root, initialKey, endpoint, payloadEndpoint, null, true);
    }

    document.addEventListener("click", function (event) {
      var link = event.target && event.target.closest ? event.target.closest(".rs-main .rs-nav-link[data-rs-page-key]") : null;
      if (!link || !root.contains(link)) return;
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button) return;

      var pageKey = link.getAttribute("data-rs-page-key");
      var href = link.getAttribute("href");
      if (!pageKey) return;

      event.preventDefault();
      loadPage(root, pageKey, endpoint, payloadEndpoint, href, false);
    });

    window.addEventListener("popstate", function () {
      var key = pagePathMap[window.location.pathname];
      if (!key) return;
      loadPage(root, key, endpoint, payloadEndpoint, null, false);
    });
  }

  function loadPage(root, pageKey, endpoint, payloadEndpoint, href, replaceOnly) {
    var cached = readCache(pageKey);
    if (cached) {
      applyPage(root, pageKey, cached, href, replaceOnly);
      return Promise.resolve(cached);
    }

    root.setAttribute("data-rs-status", "loading");
    return fetchPage(pageKey, endpoint, payloadEndpoint)
      .then(function (html) {
        applyPage(root, pageKey, html, href, replaceOnly);
        return html;
      })
      .catch(function (error) {
        root.setAttribute("data-rs-status", "failed");
        root.textContent = "Render failed: " + (error && error.message ? error.message : error);
        throw error;
      });
  }

  function fetchPage(pageKey, endpoint, payloadEndpoint) {
    if (inflight[pageKey]) return inflight[pageKey];

    var url = new URL(payloadEndpoint || DEFAULT_PAYLOAD_ENDPOINT, window.location.origin);
    url.searchParams.set("pageKey", pageKey);

    inflight[pageKey] = fetch(url.toString(), { cache: "force-cache" })
      .then(function (response) {
        return response.json();
      })
      .catch(function () {
        return fetchRenderPage(pageKey, endpoint);
      })
      .then(function (data) {
        if (!data || !data.ok) {
          return fetchRenderPage(pageKey, endpoint).then(htmlFromPayload);
        }
        var html = htmlFromPayload(data);
        writeCache(pageKey, html);
        return html;
      })
      .finally(function () {
        delete inflight[pageKey];
      });

    return inflight[pageKey];
  }

  function fetchRenderPage(pageKey, endpoint) {
    var url = new URL(endpoint, window.location.origin);
    url.searchParams.set("pageKey", pageKey);
    return fetch(url.toString(), { cache: "no-store" }).then(function (response) {
      return response.json();
    });
  }

  function htmlFromPayload(data) {
    if (!data || !data.ok) throw new Error((data && (data.detail || data.error)) || "Render failed");
    return data.html || "";
  }

  function applyPage(root, pageKey, html, href, replaceOnly) {
    root.innerHTML = html || "";
    root.setAttribute("data-rs-status", "ready");
    root.setAttribute("data-rs-page-key", pageKey);
    writeCache(pageKey, root.innerHTML);
    if (href && !replaceOnly && window.history && window.history.pushState) {
      window.history.pushState({ rsPageKey: pageKey }, "", href);
    }
    preloadFromRendered(root, pageKey, findEndpoint(root), findPayloadEndpoint(root));
  }

  function preloadFromRendered(root, activeKey, endpoint, payloadEndpoint) {
    var keys = collectNavKeys(root);
    keys.forEach(function (key) {
      if (!key || key === activeKey || readCache(key) || inflight[key]) return;
      fetchPage(key, endpoint, payloadEndpoint).catch(function () {});
    });
  }

  function collectNavKeys(root) {
    var keys = [];
    root.querySelectorAll(".rs-main .rs-nav-link[data-rs-page-key]").forEach(function (link) {
      var key = link.getAttribute("data-rs-page-key");
      if (key && keys.indexOf(key) === -1) keys.push(key);
    });
    return keys;
  }

  function findRenderedPage(root) {
    return root.querySelector("main.rs-page[data-rs-page]");
  }

  function findEndpoint(root) {
    var config = window.RS_PAGE_RENDER_CONFIG || {};
    var rendered = findRenderedPage(root);
    return config.endpointUrl || root.getAttribute("data-rs-endpoint") || (rendered && rendered.getAttribute("data-rs-endpoint")) || DEFAULT_ENDPOINT;
  }

  function findPayloadEndpoint(root) {
    var config = window.RS_PAGE_RENDER_CONFIG || {};
    return config.payloadUrl || root.getAttribute("data-rs-payload-url") || DEFAULT_PAYLOAD_ENDPOINT;
  }

  function cacheKey(pageKey) {
    return "rs_page_html:" + pageKey;
  }

  function writeCache(pageKey, html) {
    if (!pageKey) return;
    memoryCache[pageKey] = html || "";
    try {
      window.sessionStorage.setItem(cacheKey(pageKey), html || "");
    } catch (error) {}
  }

  function readCache(pageKey) {
    if (!pageKey) return null;
    if (Object.prototype.hasOwnProperty.call(memoryCache, pageKey)) return memoryCache[pageKey];
    try {
      return window.sessionStorage.getItem(cacheKey(pageKey));
    } catch (error) {
      return null;
    }
  }

  window.RSPageLoader = {
    boot: boot,
    loadPage: function (pageKey) {
      var root = document.querySelector(ROOT_SELECTOR);
      if (!root) return Promise.reject(new Error("missing_root"));
      return loadPage(root, pageKey, findEndpoint(root), findPayloadEndpoint(root), null, false);
    },
    cacheKeys: function () {
      return Object.keys(memoryCache);
    }
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();`;
const GET = async () => new Response(LOADER_JS, {
  status: 200,
  headers: {
    "Content-Type": "application/javascript; charset=utf-8",
    "Cache-Control": "no-cache"
  }
});
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
