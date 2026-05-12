const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildCoverageReport,
  parseCsv,
} = require("./audit_field_coverage");

test("parseCsv handles quoted commas and simple rows", () => {
  const rows = parseCsv("field,status,action\nclass_name,value_of_truth,\"keep, canonical\"\n");
  assert.deepEqual(rows, [
    { field: "class_name", status: "value_of_truth", action: "keep, canonical" },
  ]);
});

test("coverage report separates included and unused value-of-truth fields", () => {
  const contract = {
    tables: {
      watch_schedule: {
        fields: ["class_number", "estimated_start_time"],
      },
    },
  };
  const inventories = {
    watch_schedule: [
      { field: "class_number", status: "value_of_truth", group: "class_number", publisher_default: "yes" },
      { field: "class_id", status: "value_of_truth", group: "class", publisher_default: "yes" },
      { field: "status (from groups_live)", status: "inactive_duplicate_or_lookup", group: "status", publisher_default: "no" },
    ],
  };

  const report = buildCoverageReport({ contract, inventories });
  const table = report.tables.watch_schedule;

  assert.deepEqual(table.included_fields.map((item) => item.field), ["class_number"]);
  assert.deepEqual(table.unused_value_of_truth.map((item) => item.field), ["class_id"]);
  assert.deepEqual(table.excluded_duplicates.map((item) => item.field), ["status (from groups_live)"]);
});

test("coverage report identifies flyup candidates from evidence and calculator fields", () => {
  const contract = {
    tables: {
      watch_trips: {
        fields: ["status"],
      },
    },
  };
  const inventories = {
    watch_trips: [
      { field: "status", status: "value_of_truth", group: "status", publisher_default: "yes" },
      { field: "getLiveClassData", status: "source_evidence", group: "evidence", populated: "1", publisher_default: "no" },
      { field: "rs_min_till_go", status: "derived_calculator_output", group: "calculator", publisher_default: "yes" },
    ],
  };

  const report = buildCoverageReport({ contract, inventories });
  const candidates = report.tables.watch_trips.flyup_detail_candidates.map((item) => item.field).sort();

  assert.deepEqual(candidates, ["getLiveClassData", "rs_min_till_go"].sort());
});
