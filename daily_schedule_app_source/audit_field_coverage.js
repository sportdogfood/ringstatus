#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const ROOT = path.resolve(__dirname, "..");
const LOCAL_ROOT = __dirname;
const DEFAULT_CONTRACT = path.join(LOCAL_ROOT, "field_contract.json");
const DEFAULT_REPORT = path.join(LOCAL_ROOT, "reports", "field_coverage_report.json");
const DEFAULT_MARKDOWN = path.join(LOCAL_ROOT, "reports", "field_coverage_report.md");
const DEFAULT_INVENTORIES = {
  watch_schedule: path.join(ROOT, "docs", "watch_schedule_pro_field_inventory_2026-05-11.csv"),
  watch_trips: path.join(ROOT, "docs", "watch_trips_pro_field_inventory_2026-05-11.csv"),
};

function parseCsv(text) {
  const rows = [];
  let cell = "";
  let row = [];
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];

    if (ch === '"' && inQuotes && next === '"') {
      cell += '"';
      i += 1;
      continue;
    }
    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      continue;
    }
    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && next === "\n") i += 1;
      row.push(cell);
      cell = "";
      if (row.some((value) => String(value).trim() !== "")) rows.push(row);
      row = [];
      continue;
    }
    cell += ch;
  }

  if (cell !== "" || row.length) {
    row.push(cell);
    if (row.some((value) => String(value).trim() !== "")) rows.push(row);
  }

  if (!rows.length) return [];
  const headers = rows[0].map((value) => String(value || "").trim());
  return rows.slice(1).map((values) => {
    const out = {};
    headers.forEach((header, index) => {
      out[header] = values[index] === undefined ? "" : values[index];
    });
    return out;
  });
}

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function loadInventory(filePath) {
  return parseCsv(fs.readFileSync(filePath, "utf8"));
}

function normalize(value) {
  return String(value || "").trim();
}

function asNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function fieldSummary(row) {
  return {
    field: normalize(row.field),
    status: normalize(row.status),
    group: normalize(row.group),
    populated: asNumber(row.populated),
    blank: asNumber(row.blank),
    unique: asNumber(row.unique),
    publisher_default: normalize(row.publisher_default),
    action: normalize(row.action),
    top_values: normalize(row.top_values),
  };
}

function sortFields(left, right) {
  return left.group.localeCompare(right.group) || left.field.localeCompare(right.field);
}

function buildTableCoverage(tableName, contractFields, inventoryRows) {
  const includedSet = new Set((contractFields || []).map(normalize).filter(Boolean));
  const inventory = (inventoryRows || []).map(fieldSummary).filter((row) => row.field);
  const inventoryFieldSet = new Set(inventory.map((row) => row.field));
  const included = inventory.filter((row) => includedSet.has(row.field)).sort(sortFields);
  const missingFromInventory = [...includedSet]
    .filter((field) => !inventoryFieldSet.has(field))
    .sort()
    .map((field) => ({ field, status: "missing_from_inventory" }));

  const unused = inventory.filter((row) => !includedSet.has(row.field));
  const byStatus = {};
  for (const row of inventory) {
    byStatus[row.status] = (byStatus[row.status] || 0) + 1;
  }

  return {
    table: tableName,
    inventory_field_count: inventory.length,
    contract_field_count: includedSet.size,
    status_counts: byStatus,
    included_fields: included,
    contract_fields_missing_from_inventory: missingFromInventory,
    unused_value_of_truth: unused.filter((row) => row.status === "value_of_truth").sort(sortFields),
    unused_active_publisher_fields: unused.filter((row) => row.status === "active_publisher_field").sort(sortFields),
    unused_calculator_outputs: unused.filter((row) => row.status === "derived_calculator_output").sort(sortFields),
    unused_source_evidence: unused.filter((row) => row.status === "source_evidence").sort(sortFields),
    excluded_duplicates: unused.filter((row) => row.status.startsWith("inactive_")).sort(sortFields),
    flyup_detail_candidates: unused
      .filter((row) => ["source_evidence", "derived_calculator_output", "operational_control"].includes(row.status))
      .filter((row) => row.populated > 0 || row.publisher_default === "yes")
      .sort(sortFields),
    outside_lane_candidates: unused
      .filter((row) => ["operational_control", "source_evidence"].includes(row.status))
      .filter((row) => /log|alert|result|score|calc|error|reason|run|created|modified|ingested|endpoint/i.test(row.field))
      .sort(sortFields),
  };
}

function buildCoverageReport({ contract, inventories }) {
  const tables = {};
  for (const [tableName, rows] of Object.entries(inventories || {})) {
    const contractFields = contract?.tables?.[tableName]?.fields || [];
    tables[tableName] = buildTableCoverage(tableName, contractFields, rows);
  }
  return {
    generated_at: new Date().toISOString(),
    contract_version: contract?.contract_version || null,
    purpose: "Compare audited PRO inventories against the local daily schedule app source field contract.",
    tables,
  };
}

function markdownList(items, limit = 30) {
  if (!items.length) return "- none\n";
  return items.slice(0, limit).map((item) => `- ${item.field} (${item.status}${item.group ? ` / ${item.group}` : ""})`).join("\n") + "\n";
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Daily Schedule App Source Field Coverage");
  lines.push("");
  lines.push(`Generated: ${report.generated_at}`);
  lines.push(`Contract: ${report.contract_version || "unknown"}`);
  lines.push("");

  for (const table of Object.values(report.tables)) {
    lines.push(`## ${table.table}`);
    lines.push("");
    lines.push(`Inventory fields: ${table.inventory_field_count}`);
    lines.push(`Contract fields: ${table.contract_field_count}`);
    lines.push("");
    lines.push("### Unused Value Of Truth");
    lines.push(markdownList(table.unused_value_of_truth));
    lines.push("### Unused Calculator Outputs");
    lines.push(markdownList(table.unused_calculator_outputs));
    lines.push("### Unused Source Evidence");
    lines.push(markdownList(table.unused_source_evidence));
    lines.push("### Flyup Detail Candidates");
    lines.push(markdownList(table.flyup_detail_candidates, 40));
    lines.push("### Outside Lane Candidates");
    lines.push(markdownList(table.outside_lane_candidates, 40));
    lines.push("### Contract Fields Missing From Inventory");
    lines.push(markdownList(table.contract_fields_missing_from_inventory));
    lines.push("");
  }

  return lines.join("\n");
}

function parseArgs(argv) {
  const args = {
    contract: DEFAULT_CONTRACT,
    out: DEFAULT_REPORT,
    markdown: DEFAULT_MARKDOWN,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--contract") args.contract = path.resolve(argv[++i]);
    else if (arg === "--out") args.out = path.resolve(argv[++i]);
    else if (arg === "--markdown") args.markdown = path.resolve(argv[++i]);
    else if (arg === "--help") args.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function printHelp() {
  console.log(`Usage:
  node daily_schedule_app_source/audit_field_coverage.js

Outputs:
  daily_schedule_app_source/reports/field_coverage_report.json
  daily_schedule_app_source/reports/field_coverage_report.md
`);
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    printHelp();
    return;
  }
  const contract = loadJson(args.contract);
  const inventories = Object.fromEntries(
    Object.entries(DEFAULT_INVENTORIES).map(([table, filePath]) => [table, loadInventory(filePath)])
  );
  const report = buildCoverageReport({ contract, inventories });

  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  fs.writeFileSync(args.out, JSON.stringify(report, null, 2) + "\n", "utf8");
  fs.writeFileSync(args.markdown, toMarkdown(report), "utf8");

  console.log(JSON.stringify({
    ok: true,
    out: args.out,
    markdown: args.markdown,
    tables: Object.fromEntries(Object.entries(report.tables).map(([name, table]) => [
      name,
      {
        unused_value_of_truth: table.unused_value_of_truth.length,
        flyup_detail_candidates: table.flyup_detail_candidates.length,
        outside_lane_candidates: table.outside_lane_candidates.length,
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
  buildCoverageReport,
  parseCsv,
};
