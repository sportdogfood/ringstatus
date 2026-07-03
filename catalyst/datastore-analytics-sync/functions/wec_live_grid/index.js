"use strict";

const DEFAULT_SOURCE_URL = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store, max-age=0"
  });
  res.end(JSON.stringify(data));
}

function requestUrl(req) {
  const host = req.headers.host || "localhost";
  return new URL(req.url || "/", `https://${host}`);
}

function sourceUrl(req) {
  const incoming = requestUrl(req);
  const source = new URL(process.env.WEC_GRID_SOURCE_URL || DEFAULT_SOURCE_URL);
  source.searchParams.set("action", "wec-mobile-live");
  source.searchParams.set("show_no", incoming.searchParams.get("show_no") || "14909");
  const focusDay = incoming.searchParams.get("focus_day");
  if (focusDay) source.searchParams.set("focus_day", focusDay);
  return source;
}

module.exports = async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, { "Cache-Control": "no-store, max-age=0" });
    res.end();
    return;
  }

  if (req.method !== "GET") {
    sendJson(res, 405, { ok: false, error: "Method not allowed" });
    return;
  }

  try {
    const upstreamUrl = sourceUrl(req);
    const upstream = await fetch(upstreamUrl, { cache: "no-store" });
    const text = await upstream.text();
    let payload;
    try {
      payload = JSON.parse(text);
    } catch (_) {
      sendJson(res, 502, { ok: false, error: "Upstream returned non-JSON", preview: text.slice(0, 240) });
      return;
    }

    if (!upstream.ok || payload.ok === false) {
      sendJson(res, upstream.status || 502, {
        ok: false,
        error: payload.error || `Upstream failed ${upstream.status}`,
        upstream_status: upstream.status
      });
      return;
    }

    sendJson(res, 200, {
      ...payload,
      served_by: "wec_live_grid",
      source_mode: "server_bridge",
      source_last_checked: new Date().toISOString()
    });
  } catch (error) {
    sendJson(res, 500, { ok: false, error: error.message });
  }
};
