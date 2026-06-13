export default {
  async fetch(request, env) {
    try {
      if (request.method !== "GET" && request.method !== "POST") {
        return json({ ok: false, error: "Method not allowed. Use GET or POST." }, 405);
      }

      const cfgError = validateEnv(env);
      if (cfgError) return json({ ok: false, error: cfgError }, 500);

      const input = await readInput(request);
      const targetUrl = input.url;
      if (!targetUrl) {
        return json(
          { ok: false, error: "Missing url. Use ?url=https://... or POST {\"url\":\"https://...\"}." },
          400
        );
      }

      const parsed = safeUrl(targetUrl);
      if (!parsed) return json({ ok: false, error: "Invalid url." }, 400);
      if (parsed.protocol !== "https:") return json({ ok: false, error: "Only https URLs are allowed." }, 400);

      const ALLOWED_HOSTS = new Set([
        "ringstatus.com",
        "www.ringstatus.com",
        "clearroundtravel.com",
        "www.clearroundtravel.com",
        "blog.clearroundtravel.com"
      ]);

      if (!ALLOWED_HOSTS.has(parsed.hostname)) {
        return json({ ok: false, error: `Host not allowed: ${parsed.hostname}` }, 403);
      }

      const apiUrl =
        `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(env.CF_ACCOUNT_ID)}/browser-rendering/pdf`;

      const body = {
        url: parsed.toString(),
        gotoOptions: {
          waitUntil: input.waitUntil || "networkidle2",
          timeout: 45000
        },
        pdfOptions: buildPdfOptions(input)
      };

      const waitForSelector = input.waitForSelector || defaultWaitForSelector(parsed);
      if (waitForSelector) body.waitForSelector = { selector: waitForSelector };

      const cache = caches.default;
      const cacheTtl = positiveInt(input.cacheTtl || input.ttl, 300);
      const cacheKey = new Request(request.url, request);
      const cached = await cache.match(cacheKey);
      if (cached) return cached;

      const cfResp = await fetch(apiUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.CF_BROWSER_TOKEN}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
      });

      if (!cfResp.ok) {
        const errText = await cfResp.text();
        return json(
          {
            ok: false,
            error: "Cloudflare PDF request failed.",
            status: cfResp.status,
            details: errText.slice(0, 2000)
          },
          502
        );
      }

      const filename = buildFilename(parsed, input.filename);
      const response = new Response(cfResp.body, {
        status: 200,
        headers: {
          "Content-Type": "application/pdf",
          "Content-Disposition": `inline; filename="${filename}"`,
          "Cache-Control": `public, max-age=${cacheTtl}`
        }
      });

      await cache.put(cacheKey, response.clone());
      return response;
    } catch (err) {
      return json({ ok: false, error: err && err.message ? err.message : String(err) }, 500);
    }
  }
};

function buildPdfOptions(input) {
  const margin = {
    top: "0in",
    right: "0in",
    bottom: "0in",
    left: "0in"
  };

  if (String(input.lane || input.pageSize || "").trim().toLowerCase() === "css") {
    return {
      preferCSSPageSize: true,
      printBackground: true,
      margin
    };
  }

  return {
    format: input.format || "letter",
    printBackground: true,
    margin
  };
}

function validateEnv(env) {
  if (!env || !env.CF_BROWSER_TOKEN) return "Missing secret: CF_BROWSER_TOKEN";
  if (!env || !env.CF_ACCOUNT_ID) return "Missing variable: CF_ACCOUNT_ID";
  return "";
}

async function readInput(request) {
  const url = new URL(request.url);
  const fromQuery = {
    url: url.searchParams.get("url") || "",
    filename: url.searchParams.get("filename") || "",
    lane: url.searchParams.get("lane") || "",
    pageSize: url.searchParams.get("pageSize") || "",
    format: url.searchParams.get("format") || "",
    waitUntil: url.searchParams.get("waitUntil") || "",
    waitForSelector: url.searchParams.get("waitForSelector") || "",
    ttl: url.searchParams.get("ttl") || "",
    cacheTtl: url.searchParams.get("cacheTtl") || ""
  };

  if (request.method === "GET") return fromQuery;

  const contentType = request.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    const body = await request.json().catch(() => ({}));
    return {
      url: body?.url || fromQuery.url,
      filename: body?.filename || fromQuery.filename,
      lane: body?.lane || fromQuery.lane,
      pageSize: body?.pageSize || fromQuery.pageSize,
      format: body?.format || fromQuery.format,
      waitUntil: body?.waitUntil || fromQuery.waitUntil,
      waitForSelector: body?.waitForSelector || fromQuery.waitForSelector,
      ttl: body?.ttl || fromQuery.ttl,
      cacheTtl: body?.cacheTtl || fromQuery.cacheTtl
    };
  }

  return fromQuery;
}

function defaultWaitForSelector(parsedUrl) {
  if (parsedUrl.hostname === "ringstatus.com" || parsedUrl.hostname === "www.ringstatus.com") {
    if (parsedUrl.pathname.replace(/\/+$/, "") === "/wec-print") return 'html[data-rs-pdf-ready="1"]';
  }
  return "";
}

function positiveInt(value, fallback) {
  const n = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function safeUrl(value) {
  try {
    return new URL(String(value || "").trim());
  } catch {
    return null;
  }
}

function buildFilename(parsedUrl, requestedName) {
  const clean = String(requestedName || "").trim();
  if (clean) return clean.replace(/[^\w.-]+/g, "_").replace(/\.pdf$/i, "") + ".pdf";

  const pathPart = parsedUrl.pathname.split("/").filter(Boolean).pop() || "stall-card";
  return `${pathPart.replace(/[^\w.-]+/g, "_")}.pdf`;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}
