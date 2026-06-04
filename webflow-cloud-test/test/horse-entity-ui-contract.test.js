import assert from "node:assert/strict";
import test from "node:test";

import {
  HORSE_ENTITY_ALLOWED_WRITE_FIELDS,
  horseEntityActionReport,
  horseEntityReport
} from "../src/lib/horse-entity-ui.js";

function adapter(recordsByTable = {}) {
  const calls = [];
  return {
    calls,
    async schema() {
      calls.push(["schema"]);
      return {
        tables: Object.entries(recordsByTable).map(([name, records]) => ({
          id: `tbl_${name}`,
          name,
          fields: Object.keys(records[0]?.fields || {}).map((fieldName) => ({ id: `fld_${fieldName}`, name: fieldName }))
        }))
      };
    },
    async listRecords(tableName, options = {}) {
      calls.push(["listRecords", tableName, options]);
      return recordsByTable[tableName] || [];
    },
    async createRecord(tableName, fields) {
      calls.push(["createRecord", tableName, fields]);
      return { id: "rec_new", fields };
    },
    async patchRecord(tableName, recordId, fields) {
      calls.push(["patchRecord", tableName, recordId, fields]);
      return { id: recordId, fields };
    }
  };
}

test("loads horse entity state without reading packing state tables", async () => {
  const fake = adapter({
    pak_horses_roster: [
      {
        id: "recHorse1",
        fields: {
          horse: "Blue",
          barn_name: "Blue",
          show_name: "Blue Moon",
          active: true,
          wec_wave_1: true,
          wec_list_plans: ["recList1"],
          wec_pack_lists: ["recPackList1"],
          pak_kits: ["recKit1"],
          comments: ["recComment1"]
        }
      }
    ],
    horse_genders: [{ id: "recGender1", fields: { gender: "Gelding" } }],
    horse_disciplines: [{ id: "recDiscipline1", fields: { discipline: "Hunter" } }],
    horse_colors: [{ id: "recColor1", fields: { color: "Bay" } }],
    horses_change_log: [{ id: "recLog1", fields: { horse_record_id: "recHorse1", field_name: "barn_name" } }],
    wec_commenting: [{ id: "recComment1", fields: { scope_type: "horse", horse: ["recHorse1"], comment: "Needs review" } }]
  });

  const report = await horseEntityReport({ ok: true }, "https://example.com/test/horse-entity-ui", fake);

  assert.equal(report.ok, true);
  assert.equal(report.moduleKey, "horse_entity_ui");
  assert.equal(report.horses[0].name, "Blue");
  assert.deepEqual(report.horses[0].memberships.waveKeys, ["wave_one"]);
  assert.deepEqual(report.horses[0].memberships.planIds, ["recList1"]);
  assert.equal(report.attributes.gender[0].label, "Gelding");
  assert.equal(report.comments[0].comment, "Needs review");
  assert.equal(report.changeLog[0].horseId, "recHorse1");

  const touchedTables = fake.calls
    .filter((call) => call[0] === "listRecords")
    .map((call) => call[1]);
  assert.deepEqual(new Set(touchedTables), new Set([
    "pak_horses_roster",
    "horse_genders",
    "horse_disciplines",
    "horse_colors",
    "horses_change_log",
    "wec_commenting"
  ]));
  assert.equal(touchedTables.includes("pak_kits"), false);
  assert.equal(touchedTables.includes("pak_kit_items"), false);
  assert.equal(touchedTables.includes("horse_packing_kits"), false);
});

test("add and edit actions write only allowed horse fields and append audit rows", async () => {
  const fake = adapter({
    pak_horses_roster: [
      {
        id: "recHorse1",
        fields: {
          horse: "Blue",
          barn_name: "Blue",
          show_name: "Blue Moon",
          active: true
        }
      }
    ],
    horse_genders: [],
    horse_disciplines: [],
    horse_colors: [],
    horses_change_log: [],
    wec_commenting: []
  });

  const edit = await horseEntityActionReport(
    { ok: true },
    "https://example.com/test/horse-entity-ui",
    {
      action: "edit_horse",
      horseId: "recHorse1",
      fields: {
        barn_name: "Blueberry",
        quantity_packed: 3
      },
      sessionId: "sess-1",
      user: "manager"
    },
    fake
  );

  assert.equal(edit.ok, true);
  assert.deepEqual(edit.result.updated.fields, { barn_name: "Blueberry" });
  const patchCall = fake.calls.find((call) => call[0] === "patchRecord");
  assert.deepEqual(patchCall, ["patchRecord", "pak_horses_roster", "recHorse1", { barn_name: "Blueberry" }]);
  const logCalls = fake.calls.filter((call) => call[0] === "createRecord" && call[1] === "horses_change_log");
  assert.equal(logCalls.length, 1);
  assert.equal(logCalls[0][2].field_name, "barn_name");
  assert.equal(logCalls[0][2].old_value, "Blue");
  assert.equal(logCalls[0][2].new_value, "Blueberry");
  assert.equal(logCalls[0][2].created_by, "manager");
  assert.equal(logCalls[0][2].app_sid, "sess-1");

  assert.equal(HORSE_ENTITY_ALLOWED_WRITE_FIELDS.includes("quantity_packed"), false);
});
