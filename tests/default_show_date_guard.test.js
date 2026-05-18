const assert = require("assert");

const {
  computeDefaultShowDateGuard,
  decideEffectiveMode,
  modeForDateContext,
} = require("../lib/default_show_date_guard");

const guard = computeDefaultShowDateGuard({
  rawSqlDate: "2026-05-11",
  appSqlDate: "2026-05-28",
  defaultAppSqlDateIs: "2026-05-28",
  showAppSqlStartDate: "2026-05-28",
  showAppSqlEndDate: "2026-05-28",
  setToDefaultAppSqlDate: true,
});

assert.strictEqual(guard.check_show_date, true);
assert.strictEqual(guard.default_show_date_status, "needs_manual_confirmation");
assert.strictEqual(guard.default_show_date_reason, "short_show_window");

assert.deepStrictEqual(
  decideEffectiveMode({
    clockMode: "DAY",
    defaultShowDateGuard: guard,
    showControl: { is_default_show_manual_override: false },
  }),
  {
    mode: "OFF",
    mode_source: "default_show_date_guard",
    mode_reason: "short_show_window",
    default_show_date_status: "needs_manual_confirmation",
  }
);

assert.deepStrictEqual(
  decideEffectiveMode({
    clockMode: "DAY",
    defaultShowDateGuard: guard,
    showControl: { is_default_show_manual_override: true },
  }),
  {
    mode: "DAY",
    mode_source: "clock",
    mode_reason: "clock_mode",
    default_show_date_status: "confirmed_default_show_date",
  }
);

assert.deepStrictEqual(
  decideEffectiveMode({
    clockMode: "DAY",
    forcedMode: "NIGHT",
    defaultShowDateGuard: guard,
    showControl: { mode_control: "AUTO", is_default_show_manual_override: false },
  }).mode,
  "NIGHT"
);

assert.deepStrictEqual(
  decideEffectiveMode({
    clockMode: "DAY",
    defaultShowDateGuard: guard,
    showControl: { mode_control: "IDLE", is_default_show_manual_override: true },
  }).mode,
  "IDLE"
);

assert.strictEqual(modeForDateContext("OFF", "NIGHT"), "NIGHT");
assert.strictEqual(modeForDateContext("OVERNIGHT", "DAY"), "OVERNIGHT");

{
  const matchingManualCount = computeDefaultShowDateGuard({
    rawSqlDate: "2026-05-14",
    appSqlDate: "2026-05-15",
    defaultAppSqlDateIs: "2026-05-15",
    showAppSqlStartDate: "2026-05-14",
    showAppSqlEndDate: "2026-05-18",
    manualDayCount: 5,
    setToDefaultAppSqlDate: true,
  });
  assert.strictEqual(matchingManualCount.default_show_date_metrics.actual_day_count, 5);
  assert.strictEqual(matchingManualCount.default_show_date_reason, "ok");
}

{
  const mismatchedManualCount = computeDefaultShowDateGuard({
    rawSqlDate: "2026-05-14",
    appSqlDate: "2026-05-15",
    defaultAppSqlDateIs: "2026-05-15",
    showAppSqlStartDate: "2026-05-14",
    showAppSqlEndDate: "2026-05-18",
    manualDayCount: 4,
    setToDefaultAppSqlDate: true,
  });
  assert.ok(mismatchedManualCount.default_show_date_reason.includes("manual_day_count_mismatch"));
}

console.log("default_show_date_guard tests passed");
