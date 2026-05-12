#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const LOCAL_ROOT = __dirname;
const DEFAULT_SOURCE = path.join(LOCAL_ROOT, "samples", "latest_daily_schedule_app_source.json");
const DEFAULT_JSON = path.join(LOCAL_ROOT, "reports", "lane_preview_report.json");
const DEFAULT_MD = path.join(LOCAL_ROOT, "reports", "lane_preview_report.md");

function isBlank(value) {
  if (value === undefined || value === null) return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "object") return false;
  return String(value).trim() === "";
}

function summarizeRecords(records, sampleSize = 3) {
  const fields = {};
  for (const record of records || []) {
    for (const key of Object.keys(record || {})) {
      if (!fields[key]) fields[key] = { populated: 0, blank: 0 };
      if (isBlank(record[key])) fields[key].blank += 1;
      else fields[key].populated += 1;
    }
  }

  return {
    record_count: Array.isArray(records) ? records.length : 0,
    field_count: Object.keys(fields).length,
    fields: Object.fromEntries(Object.entries(fields).sort(([a], [b]) => a.localeCompare(b))),
    samples: (records || []).slice(0, sampleSize),
  };
}

function buildLanePreview(payload, options = {}) {
  const sampleSize = Number(options.sampleSize || 3);
  const lanes = {};
  const laneOrder = payload?.lane_order || Object.keys(payload?.lanes || {});

  for (const laneName of laneOrder) {
    lanes[laneName] = summarizeRecords(payload?.lanes?.[laneName] || [], sampleSize);
  }

  const sideLanes = {};
  for (const [sideName, value] of Object.entries(payload?.side_lanes || {})) {
    if (Array.isArray(value)) {
      sideLanes[sideName] = summarizeRecords(value, sampleSize);
    } else if (value && typeof value === "object") {
      sideLanes[sideName] = {};
      for (const [childName, childValue] of Object.entries(value)) {
        sideLanes[sideName][childName] = summarizeRecords(Array.isArray(childValue) ? childValue : [], sampleSize);
      }
    }
  }

  return {
    generated_at: new Date().toISOString(),
    source_generated_at: payload?.meta?.generated_at || null,
    purpose: "Review what data exists per lane before making render, nesting, or flyup decisions.",
    render_decisions_made: false,
    render_decisions: [],
    lane_order: laneOrder,
    lanes,
    side_lanes: sideLanes,
  };
}

function toMarkdown(preview) {
  const lines = [];
  lines.push("# Daily Schedule App Lane Preview");
  lines.push("");
  lines.push(`Generated: ${preview.generated_at}`);
  lines.push(`Source generated: ${preview.source_generated_at || "unknown"}`);
  lines.push("");
  lines.push("This report does not make render, nesting, or flyup decisions. It only shows what the current source payload contains.");
  lines.push("");
  lines.push("## Primary Lanes");
  lines.push("");
  for (const laneName of preview.lane_order) {
    const lane = preview.lanes[laneName];
    lines.push(`### ${laneName}`);
    lines.push("");
    lines.push(`Records: ${lane.record_count}`);
    lines.push(`Fields: ${lane.field_count}`);
    lines.push("");
    lines.push("| Field | Populated | Blank |");
    lines.push("| --- | ---: | ---: |");
    for (const [field, counts] of Object.entries(lane.fields)) {
      lines.push(`| ${field} | ${counts.populated} | ${counts.blank} |`);
    }
    lines.push("");
    if (lane.samples.length) {
      lines.push("Sample:");
      lines.push("");
      lines.push("```json");
      lines.push(JSON.stringify(lane.samples[0], null, 2));
      lines.push("```");
      lines.push("");
    }
  }

  lines.push("## Side Lanes");
  lines.push("");
  for (const [sideName, value] of Object.entries(preview.side_lanes)) {
    if (value.record_count !== undefined) {
      lines.push(`### ${sideName}`);
      lines.push(`Records: ${value.record_count}`);
      lines.push("");
      continue;
    }
    for (const [childName, child] of Object.entries(value)) {
      lines.push(`### ${sideName}.${childName}`);
      lines.push(`Records: ${child.record_count}`);
      lines.push(`Fields: ${child.field_count}`);
      lines.push("");
    }
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
    console.log("Usage: node daily_schedule_app_source/build_lane_preview.js");
    return;
  }

  const payload = JSON.parse(fs.readFileSync(args.source, "utf8"));
  const preview = buildLanePreview(payload);
  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  fs.writeFileSync(args.out, JSON.stringify(preview, null, 2) + "\n", "utf8");
  fs.writeFileSync(args.markdown, toMarkdown(preview), "utf8");

  console.log(JSON.stringify({
    ok: true,
    out: args.out,
    markdown: args.markdown,
    lanes: Object.fromEntries(Object.entries(preview.lanes).map(([name, lane]) => [name, lane.record_count])),
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
  buildLanePreview,
};
