#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { classifyField } = require("./build_display_intent");

const LOCAL_ROOT = __dirname;
const DEFAULT_SOURCE = path.join(LOCAL_ROOT, "samples", "latest_daily_schedule_app_source.json");
const DEFAULT_OUT = path.join(LOCAL_ROOT, "samples", "latest_bucketed_daily_schedule_app_source.json");
const DEFAULT_REPORT = path.join(LOCAL_ROOT, "reports", "bucketed_payload_report.json");

function isBlank(value) {
  if (value === undefined || value === null) return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "object") return false;
  return String(value).trim() === "";
}

function setIfPresent(target, key, value) {
  if (!isBlank(value)) target[key] = value;
}

function bucketRecord(record) {
  const out = {
    identity: {},
    display: {},
    timing: {},
    state: {},
    evidence: {},
    operational: {},
    data: {},
  };

  for (const [field, value] of Object.entries(record || {})) {
    const bucket = classifyField(field);
    if (bucket === "key") setIfPresent(out.identity, field, value);
    else if (bucket === "display_candidate") setIfPresent(out.display, field, value);
    else if (bucket === "timing_candidate") setIfPresent(out.timing, field, value);
    else if (bucket === "calculator_state") setIfPresent(out.state, field, value);
    else if (bucket === "evidence") setIfPresent(out.evidence, field, value);
    else if (bucket === "operational") setIfPresent(out.operational, field, value);
    else setIfPresent(out.data, field, value);
  }

  return out;
}

function bucketCollection(records) {
  return (records || []).map(bucketRecord);
}

function summarizeBuckets(records) {
  const summary = {
    records: records.length,
    identity_fields: new Set(),
    display_fields: new Set(),
    timing_fields: new Set(),
    state_fields: new Set(),
    evidence_fields: new Set(),
    operational_fields: new Set(),
    data_fields: new Set(),
  };

  for (const record of records) {
    for (const field of Object.keys(record.identity || {})) summary.identity_fields.add(field);
    for (const field of Object.keys(record.display || {})) summary.display_fields.add(field);
    for (const field of Object.keys(record.timing || {})) summary.timing_fields.add(field);
    for (const field of Object.keys(record.state || {})) summary.state_fields.add(field);
    for (const field of Object.keys(record.evidence || {})) summary.evidence_fields.add(field);
    for (const field of Object.keys(record.operational || {})) summary.operational_fields.add(field);
    for (const field of Object.keys(record.data || {})) summary.data_fields.add(field);
  }

  return Object.fromEntries(
    Object.entries(summary).map(([key, value]) => [key, value instanceof Set ? [...value].sort() : value])
  );
}

function buildBucketedPayload(payload) {
  const lanes = {};
  const laneOrder = payload?.lane_order || Object.keys(payload?.lanes || {});
  for (const laneName of laneOrder) {
    lanes[laneName] = bucketCollection(payload?.lanes?.[laneName] || []);
  }

  const sideLanes = {};
  for (const [sideName, value] of Object.entries(payload?.side_lanes || {})) {
    if (Array.isArray(value)) {
      sideLanes[sideName] = bucketCollection(value);
    } else if (value && typeof value === "object") {
      sideLanes[sideName] = {};
      for (const [childName, childValue] of Object.entries(value)) {
        sideLanes[sideName][childName] = bucketCollection(Array.isArray(childValue) ? childValue : []);
      }
    }
  }

  return {
    meta: {
      generated_at: new Date().toISOString(),
      source_generated_at: payload?.meta?.generated_at || null,
      source_field_contract_version: payload?.meta?.field_contract_version || null,
      note: "Bucketed flat lane payload. This is not a nested render model.",
    },
    shape: "bucketed_flat_lanes",
    lane_order: laneOrder,
    lanes,
    side_lanes: sideLanes,
  };
}

function buildBucketedReport(bucketed) {
  const lanes = {};
  for (const [laneName, records] of Object.entries(bucketed.lanes || {})) {
    lanes[laneName] = summarizeBuckets(records);
  }

  const sideLanes = {};
  for (const [sideName, value] of Object.entries(bucketed.side_lanes || {})) {
    if (Array.isArray(value)) sideLanes[sideName] = summarizeBuckets(value);
    else {
      sideLanes[sideName] = {};
      for (const [childName, records] of Object.entries(value || {})) {
        sideLanes[sideName][childName] = summarizeBuckets(records);
      }
    }
  }

  return {
    generated_at: new Date().toISOString(),
    shape: bucketed.shape,
    lanes,
    side_lanes: sideLanes,
  };
}

function parseArgs(argv) {
  const args = {
    source: DEFAULT_SOURCE,
    out: DEFAULT_OUT,
    report: DEFAULT_REPORT,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--source") args.source = path.resolve(argv[++i]);
    else if (arg === "--out") args.out = path.resolve(argv[++i]);
    else if (arg === "--report") args.report = path.resolve(argv[++i]);
    else if (arg === "--help") args.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log("Usage: node daily_schedule_app_source/build_bucketed_payload.js");
    return;
  }

  const payload = JSON.parse(fs.readFileSync(args.source, "utf8"));
  const bucketed = buildBucketedPayload(payload);
  const report = buildBucketedReport(bucketed);
  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  fs.writeFileSync(args.out, JSON.stringify(bucketed, null, 2) + "\n", "utf8");
  fs.writeFileSync(args.report, JSON.stringify(report, null, 2) + "\n", "utf8");

  console.log(JSON.stringify({
    ok: true,
    out: args.out,
    report: args.report,
    lanes: Object.fromEntries(Object.entries(bucketed.lanes).map(([name, records]) => [name, records.length])),
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
  bucketRecord,
  buildBucketedPayload,
};
