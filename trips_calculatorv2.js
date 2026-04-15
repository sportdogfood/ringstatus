/**
 * trips_calculatorv2.js
 *
 * Thin v2 wrapper around the proven trip calculator logic.
 * It keeps the existing rs_* calculation path intact, but points it at the
 * v2 watch_trips lane by default.
 */

const defaults = {
  WATCH_TABLE: "watch_trips",
  WATCH_VIEW: "heartbeat",
  TRIP_LOGS_TABLE: "trip_logs",
  FIELD_SCHEDULE_RID: "watch_schedule_rid",
  CALC_VERSION: "trips_calculator_v2_1",
};

for (const [name, value] of Object.entries(defaults)) {
  if (!process.env[name]) process.env[name] = value;
}

require("./trips_calculator.js");
