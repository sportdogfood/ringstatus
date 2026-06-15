globalThis.process ??= {};
globalThis.process.env ??= {};
import { c as corsHeaders, a as airtableConfig, j as json, s as sessionPingReport } from "./wec-plan-modules_BsQGnEh2.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const POST = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const payload = await request.json().catch(() => ({}));
    const url = new URL(request.url);
    url.searchParams.set("packWaveKey", url.searchParams.get("packWaveKey") || payload.packWaveKey || "wave_one");
    url.searchParams.set("viewKey", url.searchParams.get("viewKey") || payload.viewKey || "wave_one");
    const report = await sessionPingReport(airtable, url.toString(), {
      ...payload,
      action: "session_ping"
    });
    return json({
      ok: true,
      session: report.result,
      source: report.source
    });
  } catch (error) {
    console.error("[wec-packing-session] failed", error);
    return json({
      ok: false,
      error: "wec_packing_session_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  OPTIONS,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
