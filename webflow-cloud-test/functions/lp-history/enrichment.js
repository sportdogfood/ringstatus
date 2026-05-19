const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

export async function onRequest({ request, env }) {
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== "POST") {
    return json({ ok: false, error: "method_not_allowed" }, 405);
  }

  const body = await readJson(request);
  const auth = validateKey(request, body, env);
  if (!auth.ok) return json({ ok: false, error: auth.error }, auth.status);

  const normalized = normalizeEdit(body);
  const validation = validateEdit(normalized);
  if (!validation.ok) return json({ ok: false, error: validation.error }, 400);

  if (env.AIRTABLE_WEBHOOK_URL) {
    const upstream = await fetch(env.AIRTABLE_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(normalized)
    });
    return json({
      ok: upstream.ok,
      forwarded: true,
      upstreamStatus: upstream.status,
      recordKey: normalized.recordKey
    }, upstream.ok ? 200 : 502);
  }

  return json({
    ok: true,
    forwarded: false,
    recordKey: normalized.recordKey,
    received: normalized
  });
}

async function readJson(request) {
  const text = await request.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}

function validateKey(request, body, env) {
  const expected = env.EDIT_KEY || "dev-only";
  const header = request.headers.get("Authorization") || "";
  const bearer = header.startsWith("Bearer ") ? header.slice(7) : "";
  const supplied = bearer || body.key || new URL(request.url).searchParams.get("key") || "";
  if (supplied !== expected) return { ok: false, status: 401, error: "unauthorized" };
  return { ok: true };
}

function normalizeEdit(body) {
  const kind = String(body.kind || "").trim();
  const sourceId = String(body.sourceId || body.source_id || body.id || "").trim();
  return {
    version: 1,
    receivedAt: new Date().toISOString(),
    kind,
    sourceId,
    recordKey: kind && sourceId ? `${kind}:${sourceId}` : "",
    changes: body.changes && typeof body.changes === "object" ? body.changes : {},
    context: body.context && typeof body.context === "object" ? body.context : {}
  };
}

function validateEdit(edit) {
  const validKinds = new Set(["horses", "competitions", "classes", "videos"]);
  if (!validKinds.has(edit.kind)) return { ok: false, error: "invalid_kind" };
  if (!edit.sourceId) return { ok: false, error: "missing_source_id" };
  if (!edit.changes || !Object.keys(edit.changes).length) return { ok: false, error: "missing_changes" };
  return { ok: true };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}

