// worker.js — TEMP: WORKER_KEY DISABLED (open access) while building
// NOTE: re-enable the x-crt-key check before production.

export default {
  async fetch(request, env) {
    // ----------------------------
    // 0) TEMP: proxy is OPEN (no WORKER_KEY)
    // ----------------------------
    // const key = request.headers.get("x-crt-key");
    // if (!key || key !== env.WORKER_KEY) {
    //   return new Response(JSON.stringify({ statusCode: 401, message: "unauthorized" }), {
    //     status: 401,
    //     headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    //   });
    // }

    // ----------------------------
    // helpers
    // ----------------------------
    const jsonHeaders = { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" };

    function buildHeaders(route, env) {
      const headers = {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0",
      };

      if (route.auth === "LIGHT") {
        headers.Authorization = `Bearer ${env.SGL_TOKEN}`;
      }

      if (route.auth === "FULL") {
        headers.Authorization = `Bearer ${env.SGL_TOKEN}`;
        headers["sgl-request-origin"] = "SGL-API";
        headers.Cookie = env.SGL_COOKIE || "";
        headers.Origin = "https://www.wellingtoninternational.com";
        headers.Referer = "https://www.wellingtoninternational.com/";
      }

      return headers;
    }

    function toJsonResponse(data, status = 200) {
      return new Response(JSON.stringify(data), { status, headers: jsonHeaders });
    }

    async function fetchText(url, headers) {
      const resp = await fetch(url, { method: "GET", headers });
      const body = await resp.text();
      return { resp, body };
    }

    async function fetchJson(url, headers) {
      const resp = await fetch(url, { method: "GET", headers });
      const body = await resp.text();
      let data = null;
      try {
        data = JSON.parse(body);
      } catch {
        data = null;
      }
      return { resp, body, data };
    }

    function getCollectionConfig(path) {
      if (path === "/entries") {
        return {
          key: "entries",
          totalKey: "total_entries",
          perPageKey: "records_per_page",
          pageKey: "page",
        };
      }
      if (path === "/people") {
        return {
          key: "people",
          totalKey: "total_people",
          perPageKey: "records_per_page",
          pageKey: "page",
        };
      }
      if (path === "/classes") {
        return {
          key: "classes",
          totalKey: "total_classes",
          perPageKey: "records_per_page",
          pageKey: "page",
        };
      }
      return null;
    }

    function shouldFetchAll(inUrl, path, isPassthrough) {
      if (isPassthrough) return false;
      if (!getCollectionConfig(path)) return false;

      const all =
        inUrl.searchParams.get("all") ||
        inUrl.searchParams.get("fetch_all") ||
        inUrl.searchParams.get("paginate") ||
        "";

      return all === "1" || all === "true" || all === "all";
    }

    async function fetchAllPages({ outUrl, headers, config }) {
      const firstUrl = new URL(outUrl.toString());
      if (!firstUrl.searchParams.has(config.pageKey)) firstUrl.searchParams.set(config.pageKey, "1");

      const first = await fetchJson(firstUrl.toString(), headers);

      if (!first.resp.ok) {
        return new Response(first.body, {
          status: first.resp.status,
          headers: {
            "Content-Type": first.resp.headers.get("content-type") || "application/json; charset=utf-8",
            "Cache-Control": "no-store",
          },
        });
      }

      if (!first.data || typeof first.data !== "object") {
        return new Response(first.body, {
          status: first.resp.status,
          headers: {
            "Content-Type": first.resp.headers.get("content-type") || "application/json; charset=utf-8",
            "Cache-Control": "no-store",
          },
        });
      }

      const items = Array.isArray(first.data[config.key]) ? [...first.data[config.key]] : [];
      const totalRaw = Number(first.data?.[config.totalKey]);
      const perPageRaw = Number(first.data?.[config.perPageKey]);
      const currentPageRaw = Number(first.data?.[config.pageKey] || 1);

      const totalItems = Number.isFinite(totalRaw) && totalRaw >= 0 ? totalRaw : items.length;
      const perPage = Number.isFinite(perPageRaw) && perPageRaw > 0 ? perPageRaw : items.length || 0;
      const totalPages = perPage > 0 ? Math.ceil(totalItems / perPage) : 1;

      if (totalPages <= 1) {
        const merged = {
          ...first.data,
          [config.key]: items,
          [config.pageKey]: 1,
          total_pages: totalPages,
          fetched_pages: 1,
          fetched_records: items.length,
        };
        return toJsonResponse(merged, 200);
      }

      for (let page = Math.max(2, currentPageRaw + 1 > 1 ? 2 : 2); page <= totalPages; page++) {
        const pageUrl = new URL(outUrl.toString());
        pageUrl.searchParams.set(config.pageKey, String(page));

        const next = await fetchJson(pageUrl.toString(), headers);

        if (!next.resp.ok) {
          return toJsonResponse(
            {
              statusCode: next.resp.status,
              message: "pagination_fetch_failed",
              failed_page: page,
              total_pages: totalPages,
              upstream_status: next.resp.status,
              upstream_body: next.body,
            },
            next.resp.status
          );
        }

        if (!next.data || typeof next.data !== "object") {
          return toJsonResponse(
            {
              statusCode: 502,
              message: "pagination_invalid_json",
              failed_page: page,
              total_pages: totalPages,
            },
            502
          );
        }

        const pageItems = Array.isArray(next.data[config.key]) ? next.data[config.key] : [];
        items.push(...pageItems);
      }

      const merged = {
        ...first.data,
        [config.key]: items,
        [config.pageKey]: 1,
        total_pages: totalPages,
        fetched_pages: totalPages,
        fetched_records: items.length,
      };

      return toJsonResponse(merged, 200);
    }

    // ----------------------------
    // 1) Parse inbound request
    // ----------------------------
    const inUrl = new URL(request.url);
    const path = (inUrl.pathname || "/").replace(/\/+$/, "") || "/";

    // ----------------------------
    // 2) Route map (exact + explicit ID-in-path variants)
    //    IMPORTANT: /schedule and /schedule/my are left unchanged (working).
    // ----------------------------
    const routes = {
      "/health": { upstream: "/health", auth: "NONE", allow: [] },

      "/shows": {
        upstream: "/shows",
        auth: "LIGHT",
        allow: ["customer_id", "company_id", "date"],
      },

      // DO NOT MODIFY (per your note)
      "/schedule": {
        upstream: "/schedule",
        auth: "FULL",
        allow: ["date", "show_id", "customer_id", "company_id", "login_user_id", "team_id"],
      },
      // DO NOT MODIFY (per your note)
      "/schedule/my": {
        upstream: "/schedule/my",
        auth: "FULL",
        allow: ["date", "show_id", "customer_id", "company_id", "login_user_id", "team_id"],
      },

      // Base routes (query-driven)
      "/ring": {
        upstream: "/ring",
        auth: "FULL",
        allow: ["ring_id", "show_id", "customer_id", "company_id", "date", "show_date"],
      },
      "/people": {
        upstream: "/people",
        auth: "FULL",
        allow: [
          "people_id",
          "pid",
          "rider_id",
          "trainer_id",
          "show_id",
          "customer_id",
          "company_id",
          "search_text",
          "term",
          "sort_on",
          "sort_type",
          "page",
        ],
        defaults: { page: "1", sort_on: "lf_name", sort_type: "asc" },
      },
      "/entries": {
        upstream: "/entries",
        auth: "FULL",
        allow: [
          "sort_on",
          "sort_type",
          "page",
          "search_text",
          "show_id",
          "customer_id",
          "company_id",
          "date",
          "show_date",
          "ring_id",
          "class_id",
          "horse_id",
          "rider_id",
          "trainer_id",
          "entry_id",
          "eid",
          "team_id",
        ],
        defaults: { sort_on: "number", sort_type: "asc", page: "1", search_text: "" },
      },
      "/entries/my": {
        upstream: "/entries/my",
        auth: "FULL",
        allow: [
          "show_id",
          "customer_id",
          "company_id",
          "login_user_id",
          "team_id",
          "date",
          "page",
          "sort_on",
          "sort_type",
          "search_text",
        ],
        defaults: { page: "1", sort_on: "number", sort_type: "asc", search_text: "" },
      },
      "/classes": {
        upstream: "/classes",
        auth: "FULL",
        allow: [
          "show_id",
          "customer_id",
          "company_id",
          "show_date",
          "date",
          "class_id",
          "class_group_id",
          "cgid",
          "ring_id",
          "login_user_id",
          "team_id",
          "sort_on",
          "sort_type",
          "page",
          "search_text",
          "term",
        ],
        defaults: { page: "1", sort_on: "number", sort_type: "asc", search_text: "" },
      },

      "/classsignup": {
        upstream: "/classsignup",
        auth: "LIGHT", // per your working tests
        allow: ["show_date", "show_id", "customer_id", "company_id", "team_id", "login_user_id", "class_id", "entry_id"],
      },
    };

    // ----------------------------
    // 2b) Explicit ID-in-path variants
    //     These proxy the PATH through unchanged to upstream, and reuse auth/allowlists.
    // ----------------------------
    const passthrough = [
      { name: "/ring/:id", re: /^\/ring\/\d+$/, base: "/ring" },
      { name: "/people/:id", re: /^\/people\/\d+$/, base: "/people" },
      { name: "/entries/:id", re: /^\/entries\/\d+$/, base: "/entries" },
      { name: "/classes/:id", re: /^\/classes\/\d+$/, base: "/classes" },
    ];

    // ----------------------------
    // 2c) Resolve route (exact first, then passthrough)
    // ----------------------------
    let route = routes[path];
    let upstreamPath = route ? route.upstream : null;
    let isPassthrough = false;

    if (!route) {
      const hit = passthrough.find((p) => p.re.test(path));
      if (hit) {
        route = routes[hit.base];
        upstreamPath = path; // pass through EXACT path (/ring/51, /people/123, etc.)
        isPassthrough = true;
      }
    }

    if (!route) {
      return new Response(
        JSON.stringify({
          statusCode: 404,
          message: "route_not_found",
          path,
          available_routes: [...Object.keys(routes), ...passthrough.map((p) => p.name)],
        }),
        { status: 404, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } }
      );
    }

    // ----------------------------
    // 3) Build upstream URL (defaults + allowlisted params only)
    //    IMPORTANT: no alias-renaming here; we forward both styles (pid/people_id, eid/entry_id, cgid/class_group_id).
    // ----------------------------
    const outUrl = new URL(`https://sglapi.wellingtoninternational.com${upstreamPath}`);

    if (route.defaults) {
      for (const [k, v] of Object.entries(route.defaults)) outUrl.searchParams.set(k, v);
    }

    // Only map ring show_date -> date when show_date looks like YYYY-MM-DD AND date is missing
    // (prevents show_date=00/00/00 from creating a junk date param)
    const showDate = inUrl.searchParams.get("show_date");
    const hasDate = inUrl.searchParams.has("date");
    if (!hasDate && typeof showDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(showDate)) {
      // Add "date" for compatibility; still pass show_date if allowlisted
      outUrl.searchParams.set("date", showDate);
    }

    for (const k of route.allow) {
      if (inUrl.searchParams.has(k)) outUrl.searchParams.set(k, inUrl.searchParams.get(k));
    }

    // ----------------------------
    // 4) Headers (NONE / LIGHT / FULL)
    // ----------------------------
    const headers = buildHeaders(route, env);

    // ----------------------------
    // 5) Fetch upstream and return raw body
    //    Optional full pagination only for collection routes:
    //      ?all=1
    //      ?fetch_all=1
    //      ?paginate=all
    // ----------------------------
    if (shouldFetchAll(inUrl, path, isPassthrough)) {
      return await fetchAllPages({
        outUrl,
        headers,
        config: getCollectionConfig(path),
      });
    }

    const { resp, body } = await fetchText(outUrl.toString(), headers);
    const contentType = resp.headers.get("content-type") || "application/json; charset=utf-8";

    return new Response(body, {
      status: resp.status,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": "no-store",
      },
    });
  },
};
