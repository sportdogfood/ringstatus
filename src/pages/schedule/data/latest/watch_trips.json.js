import fs from "node:fs/promises";
import path from "node:path";

const root = process.cwd();

const directCandidates = [
  path.join(root, "docs", "schedule", "data", "latest", "watch_trips.json"),
  path.join(root, "..", "ringstatus-data", "docs", "schedule", "data", "latest", "watch_trips.json"),
  path.join(root, "..", "ringstatus-data", "docs", "schedule", "data", "latest", "trips.json"),
];

async function readJson(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  return JSON.parse(text);
}

function rowsFrom(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.rows)) return payload.rows;
  if (Array.isArray(payload?.trips)) return payload.trips;
  if (Array.isArray(payload?.records)) return payload.records;
  return [];
}

async function latestScheduleSid() {
  const candidates = [
    path.join(root, "..", "ringstatus-data", "docs", "schedules", "master.json"),
    path.join(root, "..", "ringstatus-data", "docs", "schedule", "data", "latest", "schedule.json"),
  ];

  for (const filePath of candidates) {
    try {
      const rows = rowsFrom(await readJson(filePath));
      const sid = rows[0]?.sid || rows[0]?.show_id;
      if (sid) return String(sid);
    } catch {
      // Keep trying lower-priority staged files.
    }
  }

  return null;
}

async function discoverTripFiles() {
  const docsRoot = path.join(root, "..", "ringstatus-data", "docs");
  const entries = await fs.readdir(docsRoot, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    files.push(path.join(docsRoot, entry.name, "schedules", "trips.json"));
  }

  return files;
}

export async function GET() {
  const failures = [];

  for (const filePath of directCandidates) {
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

  try {
    const sid = await latestScheduleSid();
    const files = await discoverTripFiles();
    const discovered = [];

    for (const filePath of files) {
      try {
        const rows = rowsFrom(await readJson(filePath));
        const matchingRows = sid ? rows.filter((row) => String(row?.sid || row?.show_id || "") === sid) : rows;
        if (matchingRows.length) {
          discovered.push({ filePath, rows: matchingRows });
        }
      } catch (error) {
        failures.push({ file: filePath, reason: error?.code || error?.message || "read_failed" });
      }
    }

    if (discovered.length) {
      const rows = discovered.flatMap((item) => item.rows);
      return new Response(JSON.stringify({
        ok: true,
        source: discovered.map((item) => path.relative(root, item.filePath)).join(";"),
        generated_at: new Date().toISOString(),
        rows,
      }), {
        headers: { "content-type": "application/json; charset=utf-8" },
      });
    }
  } catch (error) {
    failures.push({ file: "ringstatus-data/docs/*/schedules/trips.json", reason: error?.code || error?.message || "discover_failed" });
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
