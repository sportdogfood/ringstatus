const fs = require("fs");
const path = require("path");

const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

const TABLES = {
  focusShow: "focus_show",
  updateSchedule: "update_schedule",
  classOog: "class_oog",
  classHide: "class_hide"
};

function clean(value) {
  return String(value ?? "").trim();
}

function first(value) {
  return Array.isArray(value) ? clean(value[0]) : clean(value);
}

function numberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function csvCell(value) {
  const s = clean(value);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function minuteOfDay(value) {
  const raw = clean(value).toUpperCase().replace(/\s+/g, "");
  if (!raw || raw === "CHECKTIME") return null;
  let m = raw.match(/^(\d{1,2}):(\d{2})(A|P|AM|PM)$/);
  if (!m) m = raw.match(/^(\d{1,2})(\d{2})(A|P|AM|PM)$/);
  if (!m) return null;
  let hour = Number(m[1]);
  const minute = Number(m[2]);
  const meridian = m[3][0];
  if (meridian === "P" && hour !== 12) hour += 12;
  if (meridian === "A" && hour === 12) hour = 0;
  return hour * 60 + minute;
}

function normName(value) {
  return clean(value).toLowerCase().replace(/\s+/g, " ");
}

async function airtableFetch(url) {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    }
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function listRecords(tableName, params = {}) {
  const records = [];
  let offset = "";
  do {
    const qs = new URLSearchParams({ pageSize: "100", ...params });
    if (offset) qs.set("offset", offset);
    const result = await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}?${qs}`);
    records.push(...(result.records || []));
    offset = result.offset || "";
  } while (offset);
  return records;
}

function formulaForFocus(showNo, focusDay) {
  return `AND({show_no}=${Number(showNo)},IS_SAME({focus_day},'${focusDay}','day'))`;
}

function rowFromUpdate(record) {
  const f = record.fields || {};
  const display = clean(f.display_time || f.display_time2 || f.time_text || f.time);
  const classOrderValues = Array.isArray(f["class_order (from class_oog)"])
    ? f["class_order (from class_oog)"].map(numberOrNull).filter((v) => v !== null)
    : [];
  return {
    id: record.id,
    show_no: clean(f.show_no),
    focus_day: clean(f.focus_day).slice(0, 10),
    ring_day_no: clean(f.days),
    ring_no: clean(f.ring_no),
    ring_name: clean(f.ring_name || first(f.ring_names)),
    display_time: display,
    minute: minuteOfDay(display),
    class_no: clean(f.class_no),
    class_number: clean(f.class_number),
    class_name: clean(f.class_name || f.event_name),
    entry_count: clean(f.entry_count),
    hide: f.hide === true ? "1" : "",
    conflict: f.conflict === true ? "1" : "",
    class_hide_linked: Array.isArray(f.class_hide) ? String(f.class_hide.length) : "",
    class_order_min: classOrderValues.length ? Math.min(...classOrderValues) : "",
    class_order_values: classOrderValues.join("|")
  };
}

function rowFromOog(record) {
  const f = record.fields || {};
  const activeValues = Array.isArray(f["active (from trainers)"]) ? f["active (from trainers)"] : [];
  const active = activeValues.some((value) => value === true || clean(value) === "1" || clean(value).toLowerCase() === "true");
  return {
    show_no: clean(f.show_no),
    focus_day: clean(f.focus_day).slice(0, 10),
    ring_day_no: clean(f.days),
    ring_no: clean(f.ring_no),
    class_no: clean(f.class_no),
    class_order: numberOrNull(f.class_order),
    entry_no: clean(f.entry_no),
    entry_order: numberOrNull(f.entry_order),
    horse: clean(first(f["horse_display (from horses)"]) || f.horse),
    trainer: clean(first(f["trainer_display (from trainers)"]) || f.trainer),
    active
  };
}

function clusterRows(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = [row.show_no, row.focus_day, row.ring_day_no, row.ring_no].join("|");
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  const clusters = [];
  let clusterIndex = 0;
  for (const [baseKey, groupRows] of groups.entries()) {
    const sorted = [...groupRows].sort((a, b) => {
      const am = a.minute ?? 99999;
      const bm = b.minute ?? 99999;
      if (am !== bm) return am - bm;
      const ao = numberOrNull(a.class_order_min) ?? 99999;
      const bo = numberOrNull(b.class_order_min) ?? 99999;
      if (ao !== bo) return ao - bo;
      return (numberOrNull(a.class_number) ?? 99999) - (numberOrNull(b.class_number) ?? 99999);
    });

    let current = [];
    for (const row of sorted) {
      const prev = current[current.length - 1];
      const nearTime = prev && row.minute !== null && prev.minute !== null && Math.abs(row.minute - prev.minute) <= 2;
      const sameClassName = prev && normName(row.class_name) && normName(row.class_name) === normName(prev.class_name);
      if (!prev || nearTime || sameClassName) {
        current.push(row);
      } else {
        if (current.length > 1) clusters.push({ id: `${baseKey}|cluster_${++clusterIndex}`, rows: current });
        current = [row];
      }
    }
    if (current.length > 1) clusters.push({ id: `${baseKey}|cluster_${++clusterIndex}`, rows: current });
  }
  return clusters;
}

function renderHtml(title, rows) {
  const body = rows.map((row) => `<tr>
    <td>${row.cluster_id}</td><td>${row.reason}</td><td>${row.rank}</td>
    <td>${row.show_no}</td><td>${row.focus_day}</td><td>${row.ring_day_no}</td><td>${row.ring_no}</td><td>${row.ring_name}</td>
    <td>${row.display_time}</td><td>${row.class_order_min}</td><td>${row.class_no}</td><td>${row.class_number}</td>
    <td>${row.class_name}</td><td>${row.entry_count}</td><td>${row.active_entries}</td><td>${row.active_horses}</td>
    <td>${row.hide}</td><td>${row.conflict}</td><td>${row.class_hide_linked}</td>
  </tr>`).join("\n");
  return `<!doctype html><html><head><meta charset="utf-8"><title>${title}</title>
  <style>
    body{font-family:Arial,sans-serif;margin:24px;color:#111}
    h1{font-size:22px;margin:0 0 6px}
    .meta{font-size:13px;color:#555;margin:0 0 16px}
    table{border-collapse:collapse;width:100%;font-size:12px}
    th,td{border:1px solid #d8d8d8;padding:5px 7px;vertical-align:top}
    th{position:sticky;top:0;background:#f1f3f4;text-align:left}
    tr:nth-child(even){background:#fafafa}
  </style></head><body><h1>${title}</h1><div class="meta">Audit only. No hide rule applied.</div>
  <table><thead><tr>
    <th>cluster</th><th>reason</th><th>rank</th><th>show</th><th>focus</th><th>ring_day</th><th>ring_no</th><th>ring</th>
    <th>time</th><th>class_order</th><th>class_no</th><th>#</th><th>class</th><th>entries</th><th>active_entries</th><th>active_horses</th>
    <th>hide</th><th>conflict</th><th>class_hide</th>
  </tr></thead><tbody>${body}</tbody></table></body></html>`;
}

async function main() {
  const focusRecords = await listRecords(TABLES.focusShow, { filterByFormula: "{show_no}>0" });
  const focus = focusRecords[0]?.fields || {};
  const showNo = clean(process.argv.find((arg) => arg.startsWith("--show-no="))?.split("=")[1] || focus.show_no);
  const focusDay = clean(process.argv.find((arg) => arg.startsWith("--focus-day="))?.split("=")[1] || focus.focus_day).slice(0, 10);
  if (!showNo || !focusDay) throw new Error("Missing show_no/focus_day");

  const filterByFormula = formulaForFocus(showNo, focusDay);
  const [updateRecords, oogRecords] = await Promise.all([
    listRecords(TABLES.updateSchedule, { filterByFormula }),
    listRecords(TABLES.classOog, { filterByFormula })
  ]);

  const updates = updateRecords.map(rowFromUpdate).filter((row) => row.class_no);
  const oogRows = oogRecords.map(rowFromOog);
  const oogByClass = new Map();
  for (const row of oogRows) {
    if (!oogByClass.has(row.class_no)) oogByClass.set(row.class_no, []);
    oogByClass.get(row.class_no).push(row);
  }

  const clusters = clusterRows(updates);
  const auditRows = [];
  for (const cluster of clusters) {
    const ordered = [...cluster.rows].sort((a, b) => {
      const ao = numberOrNull(a.class_order_min) ?? 99999;
      const bo = numberOrNull(b.class_order_min) ?? 99999;
      if (ao !== bo) return ao - bo;
      return (numberOrNull(a.class_number) ?? 99999) - (numberOrNull(b.class_number) ?? 99999);
    });
    const times = new Set(ordered.map((row) => row.display_time).filter(Boolean));
    const reason = times.size === 1 ? "same_ring_day_time" : "same_ring_day_near_time";
    ordered.forEach((row, index) => {
      const oog = oogByClass.get(row.class_no) || [];
      const active = oog.filter((entry) => entry.active);
      auditRows.push({
        cluster_id: cluster.id,
        reason,
        rank: index + 1,
        ...row,
        active_entries: active.length,
        active_horses: [...new Set(active.map((entry) => entry.horse).filter(Boolean))].join(", ")
      });
    });
  }

  const outDir = path.join(__dirname, "reports");
  fs.mkdirSync(outDir, { recursive: true });
  const stem = `${showNo}-${focusDay}-display-conflict-audit`;
  const headers = ["cluster_id","reason","rank","show_no","focus_day","ring_day_no","ring_no","ring_name","display_time","class_order_min","class_order_values","class_no","class_number","class_name","entry_count","active_entries","active_horses","hide","conflict","class_hide_linked"];
  const csv = [headers.join(","), ...auditRows.map((row) => headers.map((h) => csvCell(row[h])).join(","))].join("\n");
  fs.writeFileSync(path.join(outDir, `${stem}.csv`), csv);
  fs.writeFileSync(path.join(outDir, `${stem}.html`), renderHtml(`${showNo} ${focusDay} Display Conflict Audit`, auditRows));
  console.log(JSON.stringify({
    ok: true,
    show_no: showNo,
    focus_day: focusDay,
    update_schedule_rows: updates.length,
    class_oog_rows: oogRows.length,
    clusters: clusters.length,
    audit_rows: auditRows.length,
    csv: path.join(outDir, `${stem}.csv`),
    html: path.join(outDir, `${stem}.html`)
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
