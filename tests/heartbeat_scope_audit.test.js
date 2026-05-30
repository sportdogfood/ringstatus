const assert = require("assert");

const {
  resolveShowHeartbeatAuditScope,
} = require("../lib/heartbeat_scope_audit");

const scope = resolveShowHeartbeatAuditScope({
  id: "recShow",
  fields: {
    show_id: 200000063,
    customer_id: 15,
    focus_day: "2026-05-30",
    focus_day_test: "2026-05-30",
    shifted_to_next_day: false,
    shifted_to_next_day_test: true,
    mode_control: null,
    mode_control_test: "NIGHT",
    show_name: "2026 ESP June I (#5029)",
    start_date: "2026-05-29",
    end_date: "2026-05-31",
  },
});

assert.strictEqual(scope.show_record_id, "recShow");
assert.strictEqual(scope.show_id, 200000063);
assert.strictEqual(scope.customer_id, 15);
assert.strictEqual(scope.focus_day, "2026-05-30");
assert.strictEqual(scope.mode_control, "NIGHT");
assert.strictEqual(scope.shifted_to_next_day, true);
assert.strictEqual(scope.show_scope_key, "15|200000063|2026-05-30");
assert.strictEqual(scope.sources.focus_day, "focus_day_test");
assert.strictEqual(scope.sources.mode_control, "mode_control_test");
assert.strictEqual(scope.sources.shifted_to_next_day, "shifted_to_next_day_test");
assert.ok(scope.notes.includes("shifted_to_next_day ignored for date"));
assert.ok(scope.notes.includes("mode_control cadence only"));

const missingFocus = resolveShowHeartbeatAuditScope({
  id: "recMissing",
  fields: {
    show_id: 200000063,
    customer_id: 15,
    shifted_to_next_day_test: true,
    mode_control_test: "NIGHT",
  },
});

assert.strictEqual(missingFocus.focus_day, null);
assert.strictEqual(missingFocus.show_scope_key, "");
assert.ok(missingFocus.errors.includes("missing_focus_day"));

console.log("heartbeat_scope_audit tests passed");
