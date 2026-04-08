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
          "page",
        ],
        defaults: { page: "1" },
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
        ],
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

    if (!route) {
      const hit = passthrough.find((p) => p.re.test(path));
      if (hit) {
        route = routes[hit.base];
        upstreamPath = path; // pass through EXACT path (/ring/51, /people/123, etc.)
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

    // ----------------------------
    // 5) Fetch upstream and return raw body
    // ----------------------------
    const resp = await fetch(outUrl.toString(), { method: "GET", headers });
    const body = await resp.text();
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
