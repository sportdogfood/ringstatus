const assert = require("assert");
const fs = require("fs");
const path = require("path");

const source = fs.readFileSync(path.resolve(__dirname, "..", "heartbeat_patterns.js"), "utf8");

assert.ok(
  /HEARTBEAT_ISA_FIELD[\s\S]+HEARTBEAT_ISB_FIELD[\s\S]+HEARTBEAT_ISC_FIELD[\s\S]+HEARTBEAT_ISD_FIELD/.test(source),
  "heartbeat_patterns.js must define all A/B/C/D slot fields"
);

assert.ok(
  /function\s+slotFromFields\s*\(/.test(source),
  "heartbeat_patterns.js must detect the current source slot"
);

assert.ok(
  /function\s+nextSlotFromSource\s*\(/.test(source),
  "heartbeat_patterns.js must rotate from the source slot"
);

for (const transition of [
  /sourceSlot\s*===\s*"A"[\s\S]+return\s+"B"/,
  /sourceSlot\s*===\s*"B"[\s\S]+return\s+"C"/,
  /sourceSlot\s*===\s*"C"[\s\S]+return\s+"D"/,
]) {
  assert.ok(transition.test(source), "heartbeat_patterns.js must rotate A -> B -> C -> D");
}

for (const fieldName of [
  "HEARTBEAT_ISA_FIELD",
  "HEARTBEAT_ISB_FIELD",
  "HEARTBEAT_ISC_FIELD",
  "HEARTBEAT_ISD_FIELD",
]) {
  assert.ok(
    new RegExp(`fieldsToRead[\\s\\S]+${fieldName}`).test(source),
    `heartbeat_patterns.js must read ${fieldName}`
  );

  assert.ok(
    new RegExp(`\\[${fieldName}\\]\\s*:\\s*slot\\s*===`).test(source),
    `heartbeat_patterns.js must write ${fieldName} from the target slot`
  );
}

for (const outputName of ["target_isA", "target_isB", "target_isC", "target_isD"]) {
  assert.ok(
    source.includes(outputName),
    `heartbeat_patterns.js must log ${outputName}`
  );
}

console.log("heartbeat_patterns_slots tests passed");
