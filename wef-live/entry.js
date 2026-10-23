"use strict";
const fs = require("node:fs"),
  path = require("node:path"),
  settings = require("./settings");
const { acquireLock } = require("../wef-tenants/entry");
async function scheduled({
  runtime = settings.RUNTIME,
  run = () => require("./collector").runLive(),
  api = require("./airtable").airtable(),
  lock = acquireLock,
} = {}) {
  const release = lock(path.join(runtime, "collector.lock"));
  try {
    const started = new Date().toISOString();
    let report;
    try { report = await run(); }
    catch (error) {
      report = { run_id: require("node:crypto").randomUUID(), started,
        completed: new Date().toISOString(), ok: false, errors: [error.message] };
    }
    // Persist locally before attempting Airtable, so an Airtable outage cannot hide the error.
    const save = () => {
      const body = JSON.stringify(report, null, 2);
      fs.writeFileSync(path.join(runtime, "last-run.json"), body);
      fs.writeFileSync(path.join(runtime, report.run_id + ".json"), body);
    };
    save();
    if (report.errors?.length) {
      const errors = report.errors.map((message, i) => ({
        "error-key": `${report.run_id}|${i}`, "run-id": report.run_id,
        "occurred-at": report.completed, "show-id": report.scope?.show ?? null,
        date: report.scope?.date || "", message,
      }));
      fs.appendFileSync(path.join(runtime, "errs-log.jsonl"), errors.map(e => JSON.stringify(e)).join("\n") + "\n");
      try { await api.upsert("errs-log", "error-key", errors); }
      catch (error) {
        report.ok = false;
        report.errors.push("errs-log unavailable; local receipt retained: " + error.message);
        save();
      }
    }
    const body = JSON.stringify(report, null, 2);
    // A failed run remains failed, but never prevents the next scheduled invocation.
    // Older halt receipts are retained as history and no longer act as execution gates.
    console.log(body);
    process.exitCode = report.ok ? 0 : 1;
    return report;
  } finally {
    release();
  }
}
if (require.main === module)
  scheduled().catch((e) => {
    console.error(e.message);
    process.exitCode = 1;
  });
module.exports = { scheduled };
