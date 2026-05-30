const assert = require("assert");
const fs = require("fs");
const path = require("path");

const source = fs.readFileSync(path.resolve(__dirname, "..", "trips_tagger.js"), "utf8");

assert.ok(
  source.includes('APP_CONTEXT_SOURCE || "heartbeat"'),
  "trips_tagger should use heartbeat context by default instead of pinging /ring first"
);

assert.ok(
  /async function fetchAppContextFromHeartbeat\s*\(/.test(source),
  "trips_tagger should be able to build app context from the latest heartbeat record"
);

assert.ok(
  /app_context_source:\s*appCtx\.source/.test(source),
  "trips_tagger run output should expose where app context came from"
);

assert.ok(
  source.includes('WATCH_VIEW || "heartbeat"'),
  "trips_tagger should default to the heartbeat view and rely on scope filtering"
);

assert.ok(
  !/mode === "NIGHT"[\s\S]{0,140}shiftSqlDateText/.test(source),
  "trips_tagger ring fallback must not derive app_sql_date by NIGHT +1"
);

assert.ok(
  source.includes("if (class_id === null)") && source.includes("classEndpoint = null;"),
  "trips_tagger should not ping malformed class detail endpoints when class_id is null"
);

console.log("trips_tagger_heartbeat_context tests passed");
