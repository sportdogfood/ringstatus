"use strict";
// Read-only operational verification. Never invokes a collector or source endpoint.
const fs = require("node:fs"),
  path = require("node:path"),
  crypto = require("node:crypto"),
  zlib = require("node:zlib");
const settings = require("./settings");
async function verify() {
  const receipt = JSON.parse(
    fs.readFileSync(path.join(settings.RUNTIME, "last-run.json")),
  );
  if (!receipt.ok)
    throw Error("Scheduled run failed: " + receipt.errors.join("; "));
  const api = require("./airtable").airtable(),
    data = {};
  for (const table of [
    "live-rings",
    "live-groups",
    "live-classes",
    "live-entries",
    "collections",
    "run-logs",
    "heartbeat",
    "wef-schedules.schedules",
  ])
    data[table] = await api.list(table);
  const keys = {
    "live-rings": "ring-key",
    "live-groups": "group-key",
    "live-classes": "class-key",
    "live-entries": "live-entry-key",
    collections: "collection-id",
    "run-logs": "run-id",
    heartbeat: "heartbeat-key",
  };
  for (const [table, key] of Object.entries(keys))
    require("./runtime-utils").unique(data[table], key);
  const log = data["run-logs"].find(
    (r) => r.fields["run-id"] === receipt.run_id,
  );
  if (
    log?.fields.result !== "Success" ||
    log.fields.completed !== receipt.completed
  )
    throw Error("Run-log completion mismatch");
  if (
    data.heartbeat.find((r) => r.fields["heartbeat-key"] === receipt.run_id)
      ?.fields["checked-at"] !== receipt.completed
  )
    throw Error("Heartbeat mismatch");
  let archiveCount = 0;
  for (const a of receipt.archives) {
    const bytes = zlib.gunzipSync(fs.readFileSync(a.file));
    if (crypto.createHash("sha256").update(bytes).digest("hex") !== a.sha256)
      throw Error("Archive digest mismatch");
    archiveCount++;
  }
  if (log.fields["archive-count"] !== archiveCount)
    throw Error("Archive log count mismatch");
  const byId = (table) => new Map(data[table].map((r) => [r.id, r]));
  const classes = byId("live-classes"),
    schedules = byId("wef-schedules.schedules");
  let scheduleLinks = 0,
    entryLinks = 0;
  for (const row of data["live-classes"])
    for (const id of row.fields.schedules || []) {
      const s = schedules.get(id)?.fields,
        c = row.fields;
      if (
        !s ||
        Number(s["show-id"]) !== c["show-id"] ||
        s.date !== c.date ||
        Number(s["class-id"]) !== c["class-id"]
      )
        throw Error("Invalid schedule link");
      scheduleLinks++;
    }
  for (const row of data["live-entries"]) {
    const f = row.fields,
      c = classes.get(f.class?.[0])?.fields;
    if (!c || c["show-id"] !== f["show-id"] || c.date !== f.date)
      throw Error("Invalid class link");
    const expected = [f["show-id"], f.date, f["entry-id"], c["class-id"]].join(
      "|",
    );
    if (expected !== f["entry-key"])
      throw Error("Participation identity mismatch");
    const raw = JSON.parse(f["source-json"]);
    if (
      Number(raw.live.id) !== f["trip-id"] ||
      Number(raw.identity.trip_id) !== f["trip-id"] ||
      Number(raw.identity.entry_id) !== f["entry-id"]
    )
      throw Error("Trip mapping mismatch");
    entryLinks += (f.entry || []).length; // Retained links only; Entries owns qualification.
  }
  const result = {
    at: new Date().toISOString(),
    run: receipt.run_id,
    started: receipt.started,
    completed: receipt.completed,
    scope: receipt.scope,
    counts: receipt.counts,
    archiveCount,
    scheduleLinks,
    entryLinks,
    warnings: receipt.warnings,
    keys: Object.fromEntries(
      Object.entries(keys).map(([table, key]) => [
        table,
        data[table].map((r) => ({ id: r.id, key: r.fields[key] })),
      ]),
    ),
  };
  fs.writeFileSync(
    path.join(__dirname, "evidence", "normal-run-" + receipt.run_id + ".json"),
    JSON.stringify(result, null, 2),
  );
  console.log(JSON.stringify({ ...result, keys: undefined }, null, 2));
  return result;
}
if (require.main === module)
  verify().catch((e) => {
    console.error(e.message);
    process.exitCode = 1;
  });
module.exports = { verify };
