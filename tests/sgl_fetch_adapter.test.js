const assert = require("assert");

const {
  resolveSglDirectUrl,
  shouldUsePowerShellSglFetch,
} = require("../lib/sgl_fetch_adapter");

const workerSchedule = "https://broad-tooth-b8ed.gombcg.workers.dev/schedule?date=2026-05-03&show_id=200000060&customer_id=15";
const directSchedule = "https://sglapi.wellingtoninternational.com/schedule?date=2026-05-03&show_id=200000060&customer_id=15";

assert.strictEqual(resolveSglDirectUrl(workerSchedule), directSchedule);
assert.strictEqual(resolveSglDirectUrl(directSchedule), directSchedule);
assert.strictEqual(
  resolveSglDirectUrl("https://broad-tooth-b8ed.gombcg.workers.dev/classes/200024756/?show_id=200000060&customer_id=15"),
  "https://sglapi.wellingtoninternational.com/classes/200024756/?show_id=200000060&customer_id=15"
);
assert.strictEqual(
  resolveSglDirectUrl("https://broad-tooth-b8ed.gombcg.workers.dev/people/123?pid=123&show_id=200000060&customer_id=15"),
  "https://sglapi.wellingtoninternational.com/people/123?pid=123&show_id=200000060&customer_id=15"
);
assert.strictEqual(
  resolveSglDirectUrl("https://broad-tooth-b8ed.gombcg.workers.dev/classsignup?show_date=2026-05-03&show_id=200000060&customer_id=15"),
  "https://sglapi.wellingtoninternational.com/classsignup?show_date=2026-05-03&show_id=200000060&customer_id=15"
);
assert.strictEqual(
  resolveSglDirectUrl("https://broad-tooth-b8ed.gombcg.workers.dev/entries/200230238?eid=200230238&show_id=200000060&customer_id=15"),
  "https://sglapi.wellingtoninternational.com/entries/200230238?eid=200230238&show_id=200000060&customer_id=15"
);

assert.strictEqual(shouldUsePowerShellSglFetch(workerSchedule, { platform: "win32", env: {} }), true);
assert.strictEqual(shouldUsePowerShellSglFetch(directSchedule, { platform: "win32", env: {} }), true);
assert.strictEqual(
  shouldUsePowerShellSglFetch(
    "https://sglapi.wellingtoninternational.com/classsignup?show_date=2026-05-03&show_id=200000060&customer_id=15",
    { platform: "win32", env: {} }
  ),
  true
);
assert.strictEqual(
  shouldUsePowerShellSglFetch(
    "https://sglapi.wellingtoninternational.com/entries/200230238?eid=200230238&show_id=200000060&customer_id=15",
    { platform: "win32", env: {} }
  ),
  true
);
assert.strictEqual(
  shouldUsePowerShellSglFetch(workerSchedule, {
    platform: "win32",
    env: { SGL_FETCH_TRANSPORT: "node" },
  }),
  false
);
assert.strictEqual(
  shouldUsePowerShellSglFetch("https://api.airtable.com/v0/app/table", { platform: "win32", env: {} }),
  false
);
assert.strictEqual(
  shouldUsePowerShellSglFetch(workerSchedule, { platform: "linux", env: {} }),
  false
);

console.log("sgl_fetch_adapter tests passed");
