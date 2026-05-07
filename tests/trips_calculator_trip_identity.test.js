const assert = require("assert");
const fs = require("fs");
const path = require("path");

const source = fs.readFileSync(path.resolve(__dirname, "..", "trips_calculator.js"), "utf8");

assert.ok(
  /CLASS_NUMBER:\s*process\.env\.FIELD_CLASS_NUMBER\s*\|\|\s*"class_number"/.test(source),
  "calculator must read watch_trips.class_number"
);

assert.ok(
  /function\s+buildTripIdentityKey\s*\(/.test(source),
  "calculator must build a fallback trip identity"
);

assert.ok(
  /return\s+`people:\$\{classNumber\}:\$\{entryNumber\}`/.test(source),
  "fallback trip identity must use class_number + entry_number"
);

assert.ok(
  /if\s*\(!values\.trip_identity_key\)\s*skipReasons\.push\("missing_trip_identity"\)/.test(source),
  "calculator eligibility must use trip_identity_key, not only entryxclasses_uuid"
);

assert.ok(
  !/if\s*\(!values\.entryxclasses_uuid\)\s*skipReasons\.push\("missing_entryxclasses_uuid"\)/.test(source),
  "missing entryxclasses_uuid alone must not hard-skip calculator rows"
);

console.log("trips_calculator_trip_identity tests passed");
