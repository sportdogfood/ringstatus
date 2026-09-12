const assert = require("assert");
const fs = require("fs");
const path = require("path");

const scriptPath = path.resolve(__dirname, "..", "sgl_browser_enrichment.js");
const script = fs.readFileSync(scriptPath, "utf8");

assert.ok(
  script.includes('method: "POST"') &&
    /async function logError[\s\S]+airtableCreate\(OOG_BASE, ERR_TABLE/.test(script),
  "automation_errs rows must be created with Airtable POST rather than update-only PATCH"
);

assert.ok(
  /async function airtableWrite[\s\S]+method: "PATCH"/.test(script),
  "watch table enrichment must continue updating existing Airtable records with PATCH"
);

assert.ok(
  script.includes("if (!normalized) return null") && script.includes("matches[0].order <= 0"),
  "blank or non-positive OOG values must remain blank rather than becoming zero"
);

console.log("sgl_browser_enrichment tests passed");
