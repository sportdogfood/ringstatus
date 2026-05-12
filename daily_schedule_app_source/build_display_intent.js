#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const LOCAL_ROOT = __dirname;
const DEFAULT_SOURCE = path.join(LOCAL_ROOT, "samples", "latest_daily_schedule_app_source.json");
const DEFAULT_JSON = path.join(LOCAL_ROOT, "reports", "display_intent_report.json");
const DEFAULT_MD = path.join(LOCAL_ROOT, "reports", "display_intent_report.md");

const DISPLAY_FIELDS = new Set([
  "app_sql_date",
  "app_dow_raw",
  "mode",
  "ring_number",
  "ringName",
  "ring_nickname",
  "group_name",
  "group_name_tags",
  "class_number",
  "class_name",
  "class_type",
  "schedule_sequencetype",
  "entry_number",
  "pid",
  "rider_name",
  "horse",
  "horse_name",
  "status",
  "scope_status",
  "completed_trips",
  "total_trips",
  "gone_in",
  "last_score",
]);

const TIMING_FIELDS = new Set([
  "estimated_start_time",
  "estimated_end_time",
  "estimated_go_time",
  "actual_time",
  "actual_go",
  "start_display",
  "start_display_short",
]);

function classifyField(fieldName) {
  const field = String(fieldName || "");
  if (
    field.includes("log") ||
    field.includes("endpoint") ||
    field.includes("evidence") ||
    field.includes("source") ||
    field.includes("reason") ||
    field.includes("diff") ||
    field === "getLiveClassData"
  ) {
    return "evidence";
  }
  if (
    field === "id" ||
    field.endsWith("_key") ||
    field.endsWith("_instance_key") ||
    field.endsWith("_record_id") ||
    field === "record_id" ||
    field === "trip_record_id" ||
    field === "schedule_record_id" ||
    field === "full_nesting_key"
  ) {
    return "key";
  }
  if (
    field.startsWith("rs_") ||
    field === "rs_current" ||
    field === "rs_latest_log" ||
    field === "actual_order" ||
    field === "trip_tie_breaker" ||
    field === "schedule_tie_breaker" ||
    field.endsWith("_tie_breaker") ||
    field.endsWith("_tie_breaker_source")
  ) {
    return "calculator_state";
  }
  if (
    field.includes("manual") ||
    field.includes("target") ||
    field.includes("inactive") ||
    field.includes("dropped") ||
    field.includes("created") ||
    field.includes("modified") ||
    field.includes("ingested") ||
    field === "last_seen_at"
  ) {
    return "operational";
  }
  if (TIMING_FIELDS.has(field)) return "timing_candidate";
  if (DISPLAY_FIELDS.has(field)) return "display_candidate";
  return "data_candidate";
}

function collectFields(records) {
  const fields = new Set();
  for (const record of records || []) {
    for (const field of Object.keys(record || {})) fields.add(field);
  }
  return [...fields].sort();
}

function groupByClassification(fields) {
  const grouped = {
    display_candidates: [],
    timing_candidates: [],
    data_candidates: [],
    not_for_primary_display: {
      key: [],
      evidence: [],
      calculator_state: [],
      operational: [],
    },
  };

  for (const field of fields) {
    const classification = classifyField(field);
    if (classification === "display_candidate") grouped.display_candidates.push(field);
    else if (classification === "timing_candidate") grouped.timing_candidates.push(field);
    else if (classification === "data_candidate") grouped.data_candidates.push(field);
    else grouped.not_for_primary_display[classification].push(field);
  }

  return grouped;
}

function buildDisplayIntent(payload) {
  const lanes = {};
  const laneOrder = payload?.lane_order || Object.keys(payload?.lanes || {});
  for (const laneName of laneOrder) {
    const records = payload?.lanes?.[laneName] || [];
    const fields = collectFields(records);
    lanes[laneName] = {
      record_count: records.length,
      field_count: fields.length,
      ...groupByClassification(fields),
    };
  }

  return {
    generated_at: new Date().toISOString(),
    source_generated_at: payload?.meta?.generated_at || null,
    purpose: "Mark which source fields are display candidates versus keys, evidence, calculator state, or operational metadata.",
    note: "This is not a render specification. It prevents raw source JSON from being mistaken for display shape.",
    lanes,
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Daily Schedule App Display Intent");
  lines.push("");
  lines.push(`Generated: ${report.generated_at}`);
  lines.push("");
  lines.push(report.note);
  lines.push("");
  for (const [laneName, lane] of Object.entries(report.lanes)) {
    lines.push(`## ${laneName}`);
    lines.push("");
    lines.push(`Records: ${lane.record_count}`);
    lines.push("");
    lines.push(`Display candidates: ${lane.display_candidates.join(", ") || "none"}`);
    lines.push("");
    lines.push(`Timing candidates: ${lane.timing_candidates.join(", ") || "none"}`);
    lines.push("");
    lines.push("Not for primary display:");
    lines.push(`- keys: ${lane.not_for_primary_display.key.join(", ") || "none"}`);
    lines.push(`- evidence: ${lane.not_for_primary_display.evidence.join(", ") || "none"}`);
    lines.push(`- calculator state: ${lane.not_for_primary_display.calculator_state.join(", ") || "none"}`);
    lines.push(`- operational: ${lane.not_for_primary_display.operational.join(", ") || "none"}`);
    lines.push("");
    lines.push(`Other data candidates: ${lane.data_candidates.join(", ") || "none"}`);
    lines.push("");
  }
  return lines.join("\n");
}

function parseArgs(argv) {
  const args = {
    source: DEFAULT_SOURCE,
    out: DEFAULT_JSON,
    markdown: DEFAULT_MD,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--source") args.source = path.resolve(argv[++i]);
    else if (arg === "--out") args.out = path.resolve(argv[++i]);
    else if (arg === "--markdown") args.markdown = path.resolve(argv[++i]);
    else if (arg === "--help") args.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log("Usage: node daily_schedule_app_source/build_display_intent.js");
    return;
  }

  const payload = JSON.parse(fs.readFileSync(args.source, "utf8"));
  const report = buildDisplayIntent(payload);
  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  fs.writeFileSync(args.out, JSON.stringify(report, null, 2) + "\n", "utf8");
  fs.writeFileSync(args.markdown, toMarkdown(report), "utf8");

  console.log(JSON.stringify({
    ok: true,
    out: args.out,
    markdown: args.markdown,
    lanes: Object.fromEntries(Object.entries(report.lanes).map(([name, lane]) => [
      name,
      {
        display_candidates: lane.display_candidates.length,
        timing_candidates: lane.timing_candidates.length,
        not_for_primary_display:
          lane.not_for_primary_display.key.length +
          lane.not_for_primary_display.evidence.length +
          lane.not_for_primary_display.calculator_state.length +
          lane.not_for_primary_display.operational.length,
      },
    ])),
  }, null, 2));
}

if (require.main === module) {
  try {
    main();
  } catch (err) {
    console.error(err && err.stack ? err.stack : String(err));
    process.exitCode = 1;
  }
}

module.exports = {
  buildDisplayIntent,
  classifyField,
};
