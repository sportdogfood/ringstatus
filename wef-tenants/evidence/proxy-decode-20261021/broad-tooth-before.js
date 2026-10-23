// worker.js — ROOT SGL PROXY + ISOLATED /api LANE
//
// Final locked URI shape:
// /classes/:id?show_id=...&customer_id=...
//
// Root SGL lane:
// - root routes do NOT require /api
// - no default query params
// - only minimal caller-passed params
// - debug/local params are stripped before upstream
//
// FULL auth routes:
// - /classes/:id
// - /schedule
// - /entries/:id
//
// LITE routes:
// - /classsignup
// - /classsignup/:id passes eid/class_number/show_id/customer_id
// - /ring starts lite, retries full on 401
// - everything else unless explicitly mapped later
//
// /api/* lane:
// - isolated service-binding lane
// - does not affect root SGL routes
//
// Proof headers added on normal responses:
// - X-RS-Upstream-URL
// - X-RS-Upstream-Status
// - X-RS-Body-Length
// - X-RS-Body-Is-Empty-Object
// - X-RS-Header-Mode
// - X-RS-Header-Retried
// - X-RS-Retry-Header-Mode
// - X-RS-Passthrough
// - X-RS-Has-Auth
// - X-RS-Has-Cookie

export default {
  async fetch(request, env) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type,Authorization,x-crt-key",
      "Access-Control-Expose-Headers": [
        "X-RS-Upstream-URL",
        "X-RS-Upstream-Status",
        "X-RS-Body-Length",
        "X-RS-Body-Is-Empty-Object",
        "X-RS-Header-Mode",
        "X-RS-Header-Retried",
        "X-RS-Retry-Header-Mode",
        "X-RS-Passthrough",
        "X-RS-Has-Auth",
        "X-RS-Has-Cookie",
      ].join(","),
    };

    function withCors(res) {
      const h = new Headers(res.headers);
      for (const [k, v] of Object.entries(corsHeaders)) h.set(k, v);

      return new Response(res.body, {
        status: res.status,
        headers: h,
      });
    }

    function json(data, status = 200, extraHeaders = {}) {
      return new Response(JSON.stringify(data, null, 2) + "\n", {
        status,
        headers: {
          "Content-Type": "application/json; charset=utf-8",
          "Cache-Control": "no-store, no-cache, max-age=0",
          Pragma: "no-cache",
          ...extraHeaders,
          ...corsHeaders,
        },
      });
    }

    function text(body, status = 200, contentType = "text/plain; charset=utf-8") {
      return new Response(String(body), {
        status,
        headers: {
          "Content-Type": contentType,
          "Cache-Control": "no-store, no-cache, max-age=0",
          Pragma: "no-cache",
          ...corsHeaders,
        },
      });
    }

    function normalizePath(value) {
      return String(value || "/")
        .replace(/^\/+/, "/")
        .replace(/\/+$/, "") || "/";
    }

    function isLocalParam(key) {
      return ["debug", "trace", "local_cb", "_cb", "header_mode"].includes(key);
    }

    function buildHeaders(mode, env) {
      const headers = {
        Accept: "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
        Expires: "0",
      };

      if (mode !== "open") {
        headers["sgl-request-origin"] = "SGL-API";
        headers.Referer = "https://www.wellingtoninternational.com/";
      }

      if (mode === "token" || mode === "full") {
        if (env.SGL_TOKEN) {
          headers.Authorization = `Bearer ${env.SGL_TOKEN}`;
        }
      }

      if (mode === "full") {
        if (env.SGL_COOKIE) {
          headers.Cookie = String(env.SGL_COOKIE);
        }

        headers.Origin = "https://www.wellingtoninternational.com";
      }

      return headers;
    }

    async function fetchSgl(outUrl, headers) {
      const resp = await fetch(outUrl.toString(), {
        method: "GET",
        headers,
        cf: {
          cacheTtl: 0,
          cacheEverything: false,
        },
      });

      const body = await resp.text();

      return { resp, body };
    }

    function shouldRetryFull(path, status, originalHeaderMode, hasHeaderOverride) {
      if (hasHeaderOverride) return false;
      if (status !== 401) return false;
      if (originalHeaderMode === "full") return false;

      return [
        "/ring",
        "/shows",
        "/schedule/my",
        "/people",
        "/entries",
        "/entries/my",
        "/classes",
        "/classsignup",
      ].some((basePath) => path === basePath || path.startsWith(`${basePath}/`));
    }

    const method = request.method.toUpperCase();

    if (method === "OPTIONS") {
      return text("OK", 204);
    }

    const inUrl = new URL(request.url);
    const rawPath = inUrl.pathname || "/";
    const path = normalizePath(rawPath);

    const debug = ["1", "true", "yes"].includes(
      String(inUrl.searchParams.get("debug") || "").toLowerCase()
    );

    const requestedHeaderMode = String(
      inUrl.searchParams.get("header_mode") || ""
    ).toLowerCase();

    const hasHeaderOverride = ["lite", "token", "full", "open"].includes(requestedHeaderMode);

    if (method === "GET" && path === "/health") {
      return json({
        ok: true,
        message: "worker_alive",
      }, 200);
    }

    // ----------------------------
    // /api/* isolated lane
    // ----------------------------
    if (path.startsWith("/api/")) {
      if (!env.SGL_API || typeof env.SGL_API.fetch !== "function") {
        return json({
          statusCode: 500,
          message: "missing_service_binding",
          required_binding: "SGL_API",
          path,
        }, 500);
      }

      const forwardPath = path.replace(/^\/api/, "") || "/";
      const upstreamUrl = new URL(`https://sgl-api.example${forwardPath}`);

      for (const [k, v] of inUrl.searchParams.entries()) {
        if (isLocalParam(k)) continue;
        upstreamUrl.searchParams.append(k, v);
      }

      if (debug) {
        return json({
          statusCode: 200,
          message: "debug_api_service_binding_lane",
          inbound_path: path,
          forward_path: forwardPath,
          upstream_url: upstreamUrl.toString(),
          has_service_binding: true,
        }, 200);
      }

      const proxiedReq = new Request(upstreamUrl.toString(), request);
      const res = await env.SGL_API.fetch(proxiedReq);

      return withCors(res);
    }

    if (method !== "GET") {
      return json({
        statusCode: 405,
        message: "method_not_allowed",
        method,
      }, 405);
    }

    // ----------------------------
    // Root SGL routes
    // ----------------------------
    const BASIC_PARAMS = [
      "show_id",
      "customer_id",
      "company_id",
    ];

    const BASIC_WITH_DATE_PARAMS = [
      "show_id",
      "customer_id",
      "company_id",
      "date",
      "show_date",
    ];

    const CLASS_PARAMS = [
      "show_id",
      "customer_id",
      "company_id",
      "cgid",
    ];

    const CLASS_SIGNUP_PARAMS = [
      "show_id",
      "customer_id",
      "company_id",
      "eid",
      "class_number",
    ];

    const routes = {
      "/shows": {
        upstream: "/shows",
        allow: BASIC_WITH_DATE_PARAMS,
        auth: "lite",
      },

      "/schedule": {
        upstream: "/schedule",
        allow: BASIC_WITH_DATE_PARAMS,
        auth: "full",
      },

      "/schedule/my": {
        upstream: "/schedule/my",
        allow: BASIC_WITH_DATE_PARAMS,
        auth: "lite",
      },

      "/ring": {
        upstream: "/ring",
        allow: BASIC_WITH_DATE_PARAMS,
        auth: "lite",
      },

      "/people": {
        upstream: "/people",
        allow: BASIC_PARAMS,
        auth: "lite",
      },

      "/entries": {
        upstream: "/entries",
        allow: BASIC_WITH_DATE_PARAMS,
        auth: "lite",
      },

      "/entries/my": {
        upstream: "/entries/my",
        allow: BASIC_WITH_DATE_PARAMS,
        auth: "lite",
      },

      "/classes": {
        upstream: "/classes",
        allow: CLASS_PARAMS,
        auth: "lite",
      },

      "/classsignup": {
        upstream: "/classsignup",
        allow: CLASS_SIGNUP_PARAMS,
        auth: "lite",
      },
    };

    const passthrough = [
      {
        name: "/ring/:id",
        re: /^\/ring\/\d+$/,
        base: "/ring",
        auth: "lite",
      },
      {
        name: "/people/:id",
        re: /^\/people\/\d+$/,
        base: "/people",
        auth: "lite",
      },
      {
        name: "/entries/:id",
        re: /^\/entries\/\d+$/,
        base: "/entries",
        auth: "full",
      },
      {
        name: "/classes/:id",
        re: /^\/classes\/\d+$/,
        base: "/classes",
        auth: "full",
      },
      {
        name: "/classsignup/:id",
        re: /^\/classsignup\/\d+$/,
        base: "/classsignup",
        auth: "lite",
        allow: CLASS_SIGNUP_PARAMS,
      },
    ];

    let route = routes[path];
    let upstreamPath = route ? route.upstream : null;
    let passthroughName = null;
    let defaultHeaderMode = route ? route.auth : "lite";

    if (!route) {
      const hit = passthrough.find((p) => p.re.test(path));

      if (hit) {
        route = {
          ...routes[hit.base],
          allow: hit.allow || routes[hit.base].allow,
        };

        upstreamPath = path;
        passthroughName = hit.name;
        defaultHeaderMode = hit.auth || route.auth || "lite";
      }
    }

    if (!route) {
      return json({
        statusCode: 404,
        message: "route_not_found",
        path,
        available_routes: [
          "/api/*",
          ...Object.keys(routes),
          ...passthrough.map((p) => p.name),
        ],
      }, 404);
    }

    let headerMode = hasHeaderOverride ? requestedHeaderMode : defaultHeaderMode;

    const upstreamBase = String(
      env.SGL_API_BASE || "https://sglapi.wellingtoninternational.com"
    ).replace(/\/+$/, "");

    const outUrl = new URL(`${upstreamBase}${upstreamPath}`);

    // Only pass explicitly allowed caller params.
    // No defaults. No page/sort/search/term.
    for (const k of route.allow) {
      if (inUrl.searchParams.has(k)) {
        outUrl.searchParams.set(k, inUrl.searchParams.get(k));
      }
    }

    for (const k of ["debug", "trace", "local_cb", "_cb", "header_mode"]) {
      outUrl.searchParams.delete(k);
    }

    let headers = buildHeaders(headerMode, env);

    let resp;
    let body;
    let retried = false;
    let retryHeaderMode = "";

    try {
      const firstResult = await fetchSgl(outUrl, headers);
      resp = firstResult.resp;
      body = firstResult.body;

      if (shouldRetryFull(path, resp.status, headerMode, hasHeaderOverride)) {
        retried = true;
        retryHeaderMode = "full";
        headerMode = "full";
        headers = buildHeaders(headerMode, env);

        const retryResult = await fetchSgl(outUrl, headers);
        resp = retryResult.resp;
        body = retryResult.body;
      }
    } catch (error) {
      return json({
        statusCode: 502,
        message: "upstream_fetch_exception",
        inbound_path: path,
        upstream_path: upstreamPath,
        upstream_url: outUrl.toString(),
        passthrough_route: passthroughName,
        header_mode: headerMode,
        header_mode_default: defaultHeaderMode,
        header_mode_overridden: hasHeaderOverride,
        header_retried: retried,
        retry_header_mode: retryHeaderMode,
        error: String(error?.message || error),
      }, 502, {
        "X-RS-Upstream-URL": outUrl.toString(),
        "X-RS-Upstream-Status": "fetch_exception",
        "X-RS-Body-Length": "0",
        "X-RS-Body-Is-Empty-Object": "false",
        "X-RS-Header-Mode": headerMode,
        "X-RS-Header-Retried": String(retried),
        "X-RS-Retry-Header-Mode": retryHeaderMode,
        "X-RS-Passthrough": passthroughName || "",
        "X-RS-Has-Auth": String(Boolean(headers.Authorization)),
        "X-RS-Has-Cookie": String(Boolean(headers.Cookie)),
      });
    }

    const contentType = resp.headers.get("content-type") || "application/json; charset=utf-8";

    const proofHeaders = {
      "X-RS-Upstream-URL": outUrl.toString(),
      "X-RS-Upstream-Status": String(resp.status),
      "X-RS-Body-Length": String(body.length),
      "X-RS-Body-Is-Empty-Object": String(body.trim() === "{}"),
      "X-RS-Header-Mode": headerMode,
      "X-RS-Header-Retried": String(retried),
      "X-RS-Retry-Header-Mode": retryHeaderMode,
      "X-RS-Passthrough": passthroughName || "",
      "X-RS-Has-Auth": String(Boolean(headers.Authorization)),
      "X-RS-Has-Cookie": String(Boolean(headers.Cookie)),
    };

    if (debug) {
      let parsedBody = null;

      try {
        parsedBody = JSON.parse(body);
      } catch {
        parsedBody = null;
      }

      return json({
        statusCode: resp.status,
        message: "debug_root_sgl_lane",
        inbound: {
          method,
          path,
          search: inUrl.search,
          passthrough_route: passthroughName,
        },
        resolved: {
          upstream_base: upstreamBase,
          upstream_path: upstreamPath,
          upstream_url: outUrl.toString(),
        },
        upstream: {
          ok: resp.ok,
          status: resp.status,
          status_text: resp.statusText,
          content_type: contentType,
          body_length: body.length,
          body_preview: body.slice(0, 1500),
          parsed_top_level_keys:
            parsedBody && typeof parsedBody === "object" && !Array.isArray(parsedBody)
              ? Object.keys(parsedBody).slice(0, 50)
              : null,
        },
        headers_sent: {
          mode: headerMode,
          default_mode: defaultHeaderMode,
          overridden: hasHeaderOverride,
          retried,
          retry_header_mode: retryHeaderMode,
          accept: headers.Accept,
          has_authorization: Boolean(headers.Authorization),
          has_cookie: Boolean(headers.Cookie),
          has_origin: Boolean(headers.Origin),
          sgl_request_origin: headers["sgl-request-origin"] || null,
          referer: headers.Referer || null,
          cache_control: headers["Cache-Control"],
          pragma: headers.Pragma,
          expires: headers.Expires,
        },
        proof_headers: proofHeaders,
      }, 200, proofHeaders);
    }

    return new Response(body, {
      status: resp.status,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": "no-store, no-cache, max-age=0",
        Pragma: "no-cache",
        ...proofHeaders,
        ...corsHeaders,
      },
    });
  },
};
