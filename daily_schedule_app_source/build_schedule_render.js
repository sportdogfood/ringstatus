#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const LOCAL_ROOT = __dirname;
const DEFAULT_SOURCE = path.join(LOCAL_ROOT, "samples", "latest_daily_schedule_app_source.json");
const DEFAULT_OUT = path.join(LOCAL_ROOT, "render", "schedule_preview.html");
const DEFAULT_MODEL = path.join(LOCAL_ROOT, "render", "schedule_preview_model.json");

function isBlank(value) {
  return value === undefined || value === null || String(value).trim() === "";
}

function firstValue(...values) {
  for (const value of values) {
    if (Array.isArray(value)) {
      const first = value.find((item) => !isBlank(item));
      if (!isBlank(first)) return first;
    } else if (!isBlank(value)) {
      return value;
    }
  }
  return null;
}

function byFirst(...fieldNames) {
  return (row) => firstValue(...fieldNames.map((field) => row?.[field]));
}

function keyBy(rows, getter) {
  const map = new Map();
  for (const row of rows || []) {
    const key = getter(row);
    if (!isBlank(key) && !map.has(String(key))) map.set(String(key), row);
  }
  return map;
}

function groupBy(rows, getter) {
  const map = new Map();
  for (const row of rows || []) {
    const key = getter(row);
    if (isBlank(key)) continue;
    const text = String(key);
    if (!map.has(text)) map.set(text, []);
    map.get(text).push(row);
  }
  return map;
}

function timeSort(value) {
  const text = String(value || "");
  const match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (!match) return 999999;
  return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3] || 0);
}

function formatTime(value) {
  if (isBlank(value)) return "";
  const text = String(value);
  const match = text.match(/^(\d{1,2}):(\d{2})/);
  if (!match) return text;
  let hour = Number(match[1]);
  const minute = match[2];
  const suffix = hour >= 12 ? "p" : "a";
  if (hour === 0) hour = 12;
  if (hour > 12) hour -= 12;
  return `${hour}:${minute}${suffix}`;
}

function htmlEscape(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function buildScheduleViewModel(payload) {
  const lanes = payload?.lanes || {};
  const rings = lanes.rings || [];
  const classes = lanes.classes || [];
  const starts = lanes.class_start || [];
  const groups = lanes.groups || [];
  const tripGos = lanes.trip_go || [];
  const trips = lanes.trips || [];
  const entries = lanes.entries || [];
  const horses = lanes.horses || [];
  const riders = lanes.riders || [];

  const classByRecord = keyBy(classes, byFirst("schedule_record_id"));
  const groupByRecord = keyBy(groups, byFirst("schedule_record_id"));
  const ringByNumber = keyBy(rings, byFirst("ring_number"));
  const tripsByRecord = keyBy(trips, byFirst("trip_record_id", "trip_instance_key", "trips_key"));
  const entryByInstance = keyBy(entries, byFirst("trip_instance_key", "trips_key"));
  const horseByTrip = keyBy(horses, byFirst("trips_key"));
  const riderByTrip = keyBy(riders, byFirst("trips_key"));
  const tripGosByScheduleRecord = groupBy(tripGos, byFirst("schedule_record_id"));
  const tripGosByScheduleKey = groupBy(tripGos, byFirst("schedule_key"));

  const rows = starts.map((start) => {
    const scheduleRecordId = firstValue(start.schedule_record_id);
    const scheduleKey = firstValue(start.schedule_key);
    const cls = classByRecord.get(String(scheduleRecordId)) || {};
    const group = groupByRecord.get(String(scheduleRecordId)) || {};
    const ringNumber = firstValue(group.ring_number, cls.ring_number, scheduleKey ? String(scheduleKey).split("|")[2] : null);
    const ring = ringByNumber.get(String(ringNumber)) || {};
    const rowTripGos = [
      ...(tripGosByScheduleRecord.get(String(scheduleRecordId)) || []),
      ...(!tripGosByScheduleRecord.has(String(scheduleRecordId)) ? (tripGosByScheduleKey.get(String(scheduleKey)) || []) : []),
    ];

    const rowTrips = rowTripGos.map((go) => {
      const tripKey = firstValue(go.trips_key);
      const trip = tripsByRecord.get(String(firstValue(go.trip_record_id, go.trip_instance_key, go.trips_key))) || {};
      const entry = entryByInstance.get(String(firstValue(go.trip_instance_key, go.trips_key))) || {};
      const horse = horseByTrip.get(String(tripKey)) || {};
      const rider = riderByTrip.get(String(tripKey)) || {};
      return {
        trips_key: tripKey,
        entry_number: firstValue(entry.entry_number, go.entry_number, trip.entry_number),
        rider_name: firstValue(rider.rider_name, trip.rider_name),
        horse: firstValue(horse.horse, horse.horse_name, trip.horse),
        estimated_go_time: firstValue(go.estimated_go_time, trip.estimated_go_time),
        status: firstValue(trip.status),
        order: firstValue(go.actual_order, go.rs_order_of_go, trip.rs_order_of_go),
      };
    }).sort((a, b) => timeSort(a.estimated_go_time) - timeSort(b.estimated_go_time) || String(a.entry_number || "").localeCompare(String(b.entry_number || "")));

    return {
      schedule_record_id: scheduleRecordId,
      schedule_key: scheduleKey,
      schedule_instance_key: firstValue(start.schedule_instance_key, cls.schedule_instance_key),
      ring_number: ringNumber,
      ring_name: firstValue(ring.ringName, ring.ring_nickname, group.ringName, group.ring_nickname, `Ring ${ringNumber}`),
      start_time: firstValue(start.estimated_start_time),
      class_number: firstValue(cls.class_number),
      class_name: firstValue(cls.class_name),
      class_type: firstValue(cls.class_type),
      sequence_type: firstValue(cls.schedule_sequencetype),
      group_name: firstValue(group.group_name),
      completed_trips: firstValue(group.completed_trips),
      total_trips: firstValue(group.total_trips),
      status: firstValue(group.status),
      trips: rowTrips,
    };
  }).sort((a, b) => {
    const ringCompare = Number(a.ring_number || 0) - Number(b.ring_number || 0);
    if (ringCompare) return ringCompare;
    const timeCompare = timeSort(a.start_time) - timeSort(b.start_time);
    if (timeCompare) return timeCompare;
    return String(a.class_number || "").localeCompare(String(b.class_number || ""));
  });

  const rowsByRing = groupBy(rows, byFirst("ring_number"));
  const ringModels = [...rowsByRing.entries()].map(([ringNumber, ringRows]) => {
    const ring = ringByNumber.get(String(ringNumber)) || {};
    return {
      ring_number: ringNumber,
      ring_name: firstValue(ring.ringName, ring.ring_nickname, `Ring ${ringNumber}`),
      rows: ringRows,
    };
  }).sort((a, b) => Number(a.ring_number || 0) - Number(b.ring_number || 0));

  return {
    generated_at: payload?.meta?.generated_at || new Date().toISOString(),
    summary: {
      rings: ringModels.length,
      rows: rows.length,
      trips: tripGos.length,
    },
    rings: ringModels,
  };
}

function progressText(row) {
  if (isBlank(row.completed_trips) && isBlank(row.total_trips)) return "";
  if (isBlank(row.completed_trips)) return `${row.total_trips} trips`;
  if (isBlank(row.total_trips)) return `${row.completed_trips} completed`;
  return `${row.completed_trips}/${row.total_trips}`;
}

function renderTrip(trip) {
  return `
    <li class="trip-row">
      <span class="trip-entry">#${htmlEscape(trip.entry_number || "")}</span>
      <span class="trip-main">${htmlEscape(trip.rider_name || "Rider TBD")}</span>
      <span class="trip-horse">${htmlEscape(trip.horse || "")}</span>
      <span class="trip-time">${htmlEscape(formatTime(trip.estimated_go_time))}</span>
    </li>`;
}

function renderScheduleRow(row) {
  const meta = [row.class_type, row.sequence_type, row.group_name].filter((value) => !isBlank(value)).join(" | ");
  const progress = progressText(row);
  return `
    <article class="schedule-row" data-schedule-instance="${htmlEscape(row.schedule_instance_key)}">
      <div class="time-cell">${htmlEscape(formatTime(row.start_time))}</div>
      <div class="class-cell">
        <div class="class-line">
          <span class="class-number">${htmlEscape(row.class_number || "")}</span>
          <span class="class-name">${htmlEscape(row.class_name || "Unnamed class")}</span>
        </div>
        <div class="class-meta">${htmlEscape(meta)}</div>
        ${row.trips.length ? `<ul class="trip-list">${row.trips.map(renderTrip).join("")}</ul>` : ""}
      </div>
      <div class="progress-cell">${htmlEscape(progress)}</div>
    </article>`;
}

function renderScheduleHtml(model) {
  const ringSections = model.rings.map((ring) => `
    <section class="ring-section">
      <header class="ring-header">
        <h2>${htmlEscape(ring.ring_name || `Ring ${ring.ring_number}`)}</h2>
        <span>${ring.rows.length} classes</span>
      </header>
      <div class="schedule-table">
        ${ring.rows.map(renderScheduleRow).join("")}
      </div>
    </section>`).join("");

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RingStatus Schedule Preview</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #16181d;
      --muted: #68707d;
      --border: #d9dee7;
      --accent: #176b5d;
      --accent-soft: #e6f3ef;
      --shadow: 0 1px 2px rgba(20, 24, 31, .06);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-size: 15px;
      line-height: 1.4;
    }
    .page {
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px 18px 44px;
    }
    .topbar {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin-bottom: 20px;
    }
    h1 {
      margin: 0 0 4px;
      font-size: 28px;
      line-height: 1.1;
      letter-spacing: 0;
    }
    .subtle { color: var(--muted); font-size: 13px; }
    .summary {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .summary span {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 6px 9px;
      box-shadow: var(--shadow);
      color: var(--muted);
      font-size: 13px;
    }
    .ring-section {
      margin-top: 16px;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .ring-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--border);
      background: #fbfcfd;
    }
    .ring-header h2 {
      margin: 0;
      font-size: 17px;
      line-height: 1.2;
      letter-spacing: 0;
    }
    .ring-header span {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    .schedule-row {
      display: grid;
      grid-template-columns: 86px minmax(0, 1fr) 86px;
      gap: 14px;
      padding: 12px 16px;
      border-bottom: 1px solid var(--border);
      align-items: start;
    }
    .schedule-row:last-child { border-bottom: 0; }
    .time-cell {
      color: var(--accent);
      font-weight: 700;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .class-line {
      display: flex;
      gap: 8px;
      min-width: 0;
      align-items: baseline;
    }
    .class-number {
      flex: 0 0 auto;
      color: var(--muted);
      font-weight: 700;
      font-variant-numeric: tabular-nums;
    }
    .class-name {
      min-width: 0;
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .class-meta {
      margin-top: 2px;
      color: var(--muted);
      font-size: 13px;
      overflow-wrap: anywhere;
    }
    .progress-cell {
      justify-self: end;
      min-width: 52px;
      text-align: center;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
      padding: 4px 8px;
      font-variant-numeric: tabular-nums;
    }
    .progress-cell:empty { display: none; }
    .trip-list {
      list-style: none;
      padding: 0;
      margin: 10px 0 0;
      border-top: 1px solid #edf0f4;
    }
    .trip-row {
      display: grid;
      grid-template-columns: 58px minmax(120px, 1fr) minmax(100px, 1fr) 70px;
      gap: 8px;
      padding: 7px 0;
      color: var(--muted);
      font-size: 13px;
      border-bottom: 1px solid #f0f2f5;
      align-items: center;
    }
    .trip-row:last-child { border-bottom: 0; }
    .trip-entry, .trip-time {
      font-variant-numeric: tabular-nums;
      color: var(--text);
      font-weight: 650;
      white-space: nowrap;
    }
    .trip-main, .trip-horse { overflow-wrap: anywhere; }
    @media (max-width: 720px) {
      .topbar { display: block; }
      .summary { justify-content: flex-start; margin-top: 12px; }
      .schedule-row {
        grid-template-columns: 64px minmax(0, 1fr);
      }
      .progress-cell {
        grid-column: 2;
        justify-self: start;
        margin-top: 4px;
      }
      .trip-row {
        grid-template-columns: 48px minmax(0, 1fr) 56px;
      }
      .trip-horse { grid-column: 2 / 4; }
    }
  </style>
</head>
<body>
  <main class="page">
    <div class="topbar">
      <div>
        <h1>Schedule Preview</h1>
        <div class="subtle">Generated from latest local source data at ${htmlEscape(model.generated_at)}</div>
      </div>
      <div class="summary">
        <span>${model.summary.rings} rings</span>
        <span>${model.summary.rows} classes</span>
        <span>${model.summary.trips} trips</span>
      </div>
    </div>
    ${ringSections}
  </main>
</body>
</html>`;
}

function parseArgs(argv) {
  const args = { source: DEFAULT_SOURCE, out: DEFAULT_OUT, model: DEFAULT_MODEL };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--source") args.source = path.resolve(argv[++i]);
    else if (arg === "--out") args.out = path.resolve(argv[++i]);
    else if (arg === "--model") args.model = path.resolve(argv[++i]);
    else if (arg === "--help") args.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log("Usage: node daily_schedule_app_source/build_schedule_render.js");
    return;
  }
  const payload = JSON.parse(fs.readFileSync(args.source, "utf8"));
  const model = buildScheduleViewModel(payload);
  const html = renderScheduleHtml(model);
  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  fs.writeFileSync(args.out, html, "utf8");
  fs.writeFileSync(args.model, JSON.stringify(model, null, 2) + "\n", "utf8");
  console.log(JSON.stringify({ ok: true, out: args.out, model: args.model, summary: model.summary }, null, 2));
}

if (require.main === module) {
  try {
    main();
  } catch (err) {
    console.error(err && err.stack ? err.stack : String(err));
    process.exitCode = 1;
  }
}

module.exports = {
  buildScheduleViewModel,
  renderScheduleHtml,
};
