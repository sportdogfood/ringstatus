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
    "/rs/schedules": "rs_schedules",
    "/rs/waze": "rs_waze",
    "/rs/onsite": "rs_onsite",
    "/rs/twoway": "rs_twoway",
    "/rs/pak": "rs-pak",
    "/rs/reminders": "rs-reminders",
    "/rs/boards": "rs-boards",
    "/rs/alerts": "rs-alerts",
    "/rs/contact": "rs_contact",
    "/rs/members": "rs_members"
  };

  function boot() {
    var root = document.querySelector(ROOT_SELECTOR);
    if (!root || root.__rsPageLoaderReady) return;
    root.__rsPageLoaderReady = true;
    ensureRuntimeStyle();

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
      var megaTrigger = event.target && event.target.closest ? event.target.closest("[data-rs-mega-trigger]") : null;
      if (megaTrigger && root.contains(megaTrigger)) {
        event.preventDefault();
        var nav = megaTrigger.closest(".rs-nav");
        var isMegaOpen = !!(nav && nav.classList.contains("is-mega-open"));
        closeMenus(root);
        if (nav && !isMegaOpen) {
          nav.classList.add("is-mega-open");
          megaTrigger.setAttribute("aria-expanded", "true");
          var mega = nav.querySelector("#data-mega-menu");
          if (mega) mega.setAttribute("aria-hidden", "false");
        } else {
          megaTrigger.setAttribute("aria-expanded", "false");
        }
        return;
      }

      var drawerTrigger = event.target && event.target.closest ? event.target.closest("[data-rs-drawer-trigger]") : null;
      if (drawerTrigger && root.contains(drawerTrigger)) {
        event.preventDefault();
        closeMenus(root);
        openDrawer(root, drawerTrigger);
        return;
      }

      var drawerClose = event.target && event.target.closest ? event.target.closest("[data-rs-drawer-close],.rs-nav-scrim") : null;
      if (drawerClose && root.contains(drawerClose)) {
        event.preventDefault();
        closeMenus(root);
        return;
      }

      var link = event.target && event.target.closest ? event.target.closest(".rs-nav-link[data-rs-page-key],.rs-mega-link[data-rs-page-key],.rs-drawer-list a[data-rs-page-key],.rs-logo[data-rs-page-key]") : null;
      if (!link || !root.contains(link)) return;
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button) return;

      var pageKey = link.getAttribute("data-rs-page-key");
      var href = link.getAttribute("href");
      if (!pageKey) return;

      event.preventDefault();
      closeMenus(root);
      loadPage(root, pageKey, endpoint, payloadEndpoint, href, false);
    });

    document.addEventListener("click", function (event) {
      if (!root.contains(event.target)) closeMenus(root);
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

    inflight[pageKey] = fetch(url.toString(), { cache: "no-store" })
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
    ensureRuntimeStyle();
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
    root.querySelectorAll(".rs-nav-link[data-rs-page-key],.rs-mega-link[data-rs-page-key],.rs-drawer-list a[data-rs-page-key],.rs-logo[data-rs-page-key]").forEach(function (link) {
      var key = link.getAttribute("data-rs-page-key");
      if (key && keys.indexOf(key) === -1) keys.push(key);
    });
    return keys;
  }

  function closeMenus(root) {
    root.querySelectorAll(".rs-nav.is-mega-open").forEach(function (nav) {
      nav.classList.remove("is-mega-open");
      var trigger = nav.querySelector("[data-rs-mega-trigger]");
      if (trigger) trigger.setAttribute("aria-expanded", "false");
      var mega = nav.querySelector("#data-mega-menu");
      if (mega) mega.setAttribute("aria-hidden", "true");
    });
    root.querySelectorAll(".rs-drawer.is-open").forEach(function (drawer) {
      drawer.classList.remove("is-open");
      drawer.setAttribute("aria-hidden", "true");
    });
    root.querySelectorAll(".rs-nav-scrim.is-open").forEach(function (scrim) {
      scrim.classList.remove("is-open");
      scrim.setAttribute("aria-hidden", "true");
    });
    root.querySelectorAll("[data-rs-drawer-trigger]").forEach(function (trigger) {
      trigger.setAttribute("aria-expanded", "false");
    });
  }

  function openDrawer(root, trigger) {
    var drawer = root.querySelector("#data-drawer-menu");
    var scrim = root.querySelector("#data-nav-scrim");
    if (drawer) {
      drawer.classList.add("is-open");
      drawer.setAttribute("aria-hidden", "false");
    }
    if (scrim) {
      scrim.classList.add("is-open");
      scrim.setAttribute("aria-hidden", "false");
    }
    if (trigger) trigger.setAttribute("aria-expanded", "true");
  }

  function ensureRuntimeStyle() {
    if (document.getElementById("rs-page-loader-runtime-style")) return;
    var style = document.createElement("style");
    style.id = "rs-page-loader-runtime-style";
    style.textContent = [
      ".rs-page #data-drawer-menu.rs-drawer.is-open{transform:translateX(0);}",
      ".rs-page #data-nav-scrim.rs-nav-scrim.is-open{display:block;}"
    ].join("");
    (document.body || document.documentElement).appendChild(style);
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
  }

  function readCache(pageKey) {
    if (!pageKey) return null;
    if (Object.prototype.hasOwnProperty.call(memoryCache, pageKey)) return memoryCache[pageKey];
    return null;
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
