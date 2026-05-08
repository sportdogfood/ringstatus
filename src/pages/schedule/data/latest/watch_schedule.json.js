import fs from "node:fs/promises";
import path from "node:path";

const root = process.cwd();

const candidates = [
  path.join(root, "docs", "schedule", "data", "latest", "watch_schedule.json"),
  path.join(root, "..", "ringstatus-data", "docs", "schedule", "data", "latest", "watch_schedule.json"),
  path.join(root, "..", "ringstatus-data", "docs", "schedules", "master.json"),
  path.join(root, "..", "ringstatus-data", "docs", "schedule", "data", "latest", "schedule.json"),
];

async function readJson(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  return JSON.parse(text);
}

function rowsFrom(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.rows)) return payload.rows;
  if (Array.isArray(payload?.schedule)) return payload.schedule;
  if (Array.isArray(payload?.records)) return payload.records;
  return [];
}

export async function GET() {
  const failures = [];

  for (const filePath of candidates) {
    try {
      const payload = await readJson(filePath);
      const rows = rowsFrom(payload);
      if (!rows.length) {
        failures.push({ file: filePath, reason: "empty_rows" });
        continue;
      }

      return new Response(JSON.stringify({
        ok: true,
        source: path.relative(root, filePath),
        generated_at: new Date().toISOString(),
        rows,
      }), {
        headers: { "content-type": "application/json; charset=utf-8" },
      });
    } catch (error) {
      failures.push({ file: filePath, reason: error?.code || error?.message || "read_failed" });
    }
  }

  return new Response(JSON.stringify({
    ok: false,
    source: null,
    generated_at: new Date().toISOString(),
    rows: [],
    errors: failures,
  }), {
    status: 200,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}
