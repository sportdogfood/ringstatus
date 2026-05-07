const assert = require("assert");
const fs = require("fs");
const path = require("path");

const workerPaths = [
  path.resolve(__dirname, "..", "lib", "cloudflare", "ringstatus-proxy", "worker.js"),
  path.resolve(__dirname, "..", "utils", "workers", "ringstatus-proxy", "worker.js"),
  path.resolve(__dirname, "..", "utils", "workers", "ringstatus-proxy", "worker2.js"),
];

for (const workerPath of workerPaths) {
  const worker = fs.readFileSync(workerPath, "utf8");

  assert.ok(
    worker.includes('const isDataFile = p.endsWith(".json") || p.endsWith(".ndjson");'),
    `${path.basename(workerPath)} must identify published JSON/NDJSON data files separately from static assets`
  );

  assert.ok(
    worker.includes('freshUrl.searchParams.set("_cb", String(Date.now()))'),
    `${path.basename(workerPath)} must add an upstream cache buster for published data reads`
  );

  assert.ok(
    /isDataFile[\s\S]+\{ cacheTtl: 0, cacheEverything: false \}/.test(worker),
    `${path.basename(workerPath)} must bypass Cloudflare fetch cache for published data reads`
  );

  assert.ok(
    worker.includes('h.set("Cache-Control", "no-store, no-cache, max-age=0")') &&
      worker.includes('h.set("Pragma", "no-cache")'),
    `${path.basename(workerPath)} must tell clients and intermediaries not to cache published data responses`
  );
}

console.log("ringstatus_proxy_cache tests passed");
