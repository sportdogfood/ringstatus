globalThis.process ??= {};
globalThis.process.env ??= {};
import { env } from "cloudflare:workers";
const __vite_import_meta_env__ = { "ASSETS_PREFIX": "https://110f06dd-c1ea-4839-98af-d829cbe77941.wf-app-prod.cosmic.webflow.services/test", "BASE_URL": "/test", "DEV": false, "MODE": "production", "PROD": true, "SITE": void 0, "SSR": true };
const config = {
  runtime: "edge"
};
const DEFAULT_BASE_ID = "app6XS1RvsPNRT6os";
const TABLES = {
  ringGroups: "ring_groups",
  printMeta: "wec_print_meta"
};
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ request }) => {
  const airtable = getAirtableConfig();
  if (!airtable.ok) return json({ ok: false, error: airtable.error }, 500);
  try {
    const url = new URL(request.url);
    const showNo = clean(url.searchParams.get("show_no")) || "14906";
    const focusDay = isoDate(url.searchParams.get("focus_day"));
    if (!focusDay) return json({ ok: false, error: "missing_focus_day" }, 400);
    const printMetaKey = `${showNo}|${focusDay}`;
    const [metaRows, ringRows] = await Promise.all([
      listAirtableRecords(airtable, airtable.printMetaTable, `{print_meta_key}='${escapeFormulaString(printMetaKey)}'`),
      listAirtableRecords(
        airtable,
        airtable.ringGroupsTable,
        `AND({show_no}=${Number(showNo)},IS_SAME({focus_day},DATETIME_PARSE('${escapeFormulaString(focusDay)}'),'day'))`
      )
    ]);
    const placement = {};
    const rings = ringRows.map((record) => normalizeRingGroup(record.fields || {})).filter((ring) => ring.ring_no).sort((a, b) => a.portrait_col - b.portrait_col || a.print_rows - b.print_rows || a.ring_name.localeCompare(b.ring_name));
    for (const ring of rings) {
      placement[String(ring.ring_no)] = {
        portrait_col: ring.portrait_col,
        landscape_col: ring.landscape_col,
        print_rows: ring.print_rows,
        ring_name: ring.ring_name
      };
    }
    return json({
      ok: true,
      source: "airtable.wec_print_meta+ring_groups",
      show_no: showNo,
      focus_day: focusDay,
      print_meta_key: printMetaKey,
      print_meta: metaRows[0]?.fields || null,
      ring_count: rings.length,
      rings,
      placement
    });
  } catch (error) {
    console.error("[wec-schedule] print-layout failed", error);
    return json({
      ok: false,
      error: "wec_print_layout_failed",
      detail: error instanceof Error ? error.message : String(error)
    }, 502);
  }
};
function getAirtableConfig() {
  const runtime = { ...globalThis.process?.env || {}, ...Object.assign(__vite_import_meta_env__, { AIRTABLE_TOKEN: "patDeqY9NAQsuYx6q.7fd75026f0820373f62a72ca063f99b2203b9d873cb77aa3962637ab7bb0ec37", AIRTABLE_BASE_ID: "apptdhhNzduxm5gjn" }) || {}, ...env || {} };
  const token = runtime.AIRTABLE_TOKEN;
  const baseId = runtime.WEC_AIRTABLE_BASE_ID || runtime.AIRTABLE_WEC_SCHEDULES_BASE_ID || runtime.AIRTABLE_BASE_ID || runtime.AIRTABLE_BASE || DEFAULT_BASE_ID;
  if (!token) return { ok: false, error: "missing_airtable_token" };
  return {
    ok: true,
    token,
    baseId,
    ringGroupsTable: runtime.AIRTABLE_WEC_RING_GROUPS_TABLE || TABLES.ringGroups,
    printMetaTable: runtime.AIRTABLE_WEC_PRINT_META_TABLE || TABLES.printMeta
  };
}
async function listAirtableRecords(airtable, table, filterByFormula = "") {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${encodeURIComponent(airtable.baseId)}/${encodeURIComponent(table)}`);
    url.searchParams.set("pageSize", "100");
    if (filterByFormula) url.searchParams.set("filterByFormula", filterByFormula);
    if (offset) url.searchParams.set("offset", offset);
    const response = await fetch(url, { headers: { Authorization: `Bearer ${airtable.token}` } });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`list ${table} ${response.status}: ${JSON.stringify(result)}`);
    records.push(...result.records || []);
    offset = result.offset || "";
  } while (offset);
  return records;
}
function normalizeRingGroup(fields) {
  return {
    ring_group_key: clean(fields.ring_group_key),
    show_no: clean(fields.show_no),
    focus_day: isoDate(fields.focus_day),
    ring_day_no: clean(fields.ring_day_no),
    ring_no: clean(fields.ring_no),
    ring_name: clean(fields.ring_name),
    source_rows: numberOrZero(fields.source_rows),
    hidden_rows: numberOrZero(fields.hidden_rows),
    visible_classes: numberOrZero(fields.visible_classes),
    visible_rollups: numberOrZero(fields.visible_rollups),
    print_rows: numberOrZero(fields.print_rows),
    portrait_col: numberOrZero(fields.portrait_col),
    landscape_col: numberOrZero(fields.landscape_col)
  };
}
function clean(value) {
  if (value == null) return "";
  if (Array.isArray(value)) return clean(value[0]);
  if (typeof value === "object" && value.name) return clean(value.name);
  return String(value).trim();
}
function isoDate(value) {
  const raw = clean(value);
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  return match ? `${match[1]}-${match[2]}-${match[3]}` : "";
}
function numberOrZero(value) {
  const n = Number(clean(value));
  return Number.isFinite(n) ? n : 0;
}
function escapeFormulaString(value) {
  return String(value || "").replace(/'/g, "\\'");
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
      "Content-Type": "application/json; charset=utf-8"
    }
  });
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
