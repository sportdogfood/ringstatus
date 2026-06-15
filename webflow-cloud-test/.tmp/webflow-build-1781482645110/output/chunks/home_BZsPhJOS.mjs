globalThis.process ??= {};
globalThis.process.env ??= {};
import { a as airtableConfig, j as json, p as planReport, c as corsHeaders } from "./wec-plan-modules_BsQGnEh2.mjs";
import { h as horseKitReport } from "./wec-horse-kits_nKY3K1Ah.mjs";
const config = {
  runtime: "edge"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = airtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const url = new URL(request.url);
    const packWaveKey = url.searchParams.get("packWaveKey") || "wave_one";
    const viewKey = url.searchParams.get("viewKey") || packWaveKey;
    const scopedUrl = new URL(request.url);
    scopedUrl.searchParams.set("packWaveKey", packWaveKey);
    scopedUrl.searchParams.set("viewKey", viewKey);
    const [kits, quantity, perHorse, perGroom] = await Promise.all([
      horseKitReport(airtable, scopedUrl.toString()),
      planReport(airtable, scopedUrl.toString(), "quantity"),
      planReport(airtable, scopedUrl.toString(), "per_horse"),
      planReport(airtable, scopedUrl.toString(), "per_groom")
    ]);
    return json({
      ok: true,
      v: 1,
      source: { packWaveKey, viewKey },
      modules: [
        homeModule("horse_kits", "Horse Kits", kits.counts, kits.source),
        homeModule("quantity", "Quantity Counts", quantity.counts, quantity.source),
        homeModule("per_horse", "Per-Horse Items", perHorse.counts, perHorse.source),
        homeModule("per_groom", "Groom Supplies", perGroom.counts, perGroom.source)
      ],
      reports: { horse_kits: kits, quantity, per_horse: perHorse, per_groom: perGroom }
    });
  } catch (error) {
    console.error("[wec-packing-home] failed", error);
    return json({
      ok: false,
      error: "wec_packing_home_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
function homeModule(key, label, counts = {}, source = {}) {
  return {
    key,
    label,
    source,
    counts: {
      need: counts.need ?? counts.needed ?? counts.kitItems ?? 0,
      packed: counts.packed ?? 0,
      left: counts.left ?? 0,
      horses: counts.horses ?? counts.visibleHorses ?? 0,
      rows: counts.rows ?? counts.items ?? counts.packingRows ?? 0
    }
  };
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
