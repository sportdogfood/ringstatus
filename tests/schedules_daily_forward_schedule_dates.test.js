const assert = require("assert");
const { forwardScheduleDates } = require("../schedules_dailyv2");

assert.deepEqual(
  forwardScheduleDates("2026-05-30", "2026-05-31"),
  ["2026-05-31"],
  "forward schedule cache must not include dates before the focus/current schedule date"
);

assert.deepEqual(
  forwardScheduleDates("2026-05-30", "2026-05-30"),
  [],
  "single-day focus should not prefetch another schedule date"
);

assert.deepEqual(
  forwardScheduleDates("2026-05-31", "2026-05-30"),
  [],
  "out-of-window focus should not prefetch backward"
);

console.log("schedules_daily_forward_schedule_dates tests passed");
