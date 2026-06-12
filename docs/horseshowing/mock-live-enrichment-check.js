const DEFAULT_BASE =
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

function arg(name, fallback) {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function clean(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function intOrNull(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function classNumberFromLabel(value) {
  const match = clean(value).match(/^(\d+)\s*[-)]/);
  return match ? match[1] : "";
}

function classNameFromLabel(value) {
  return clean(value).replace(/^\d+\s*[-)]\s*/, "");
}

function liveKey(row) {
  return [
    clean(row.show_no ?? row.show_id),
    clean(row.ring_no ?? row.ring_number),
    clean(row.ring_day_no),
    clean(row.class_no)
  ].join("|");
}

function orderFallbackKey(row) {
  return [
    clean(row.show_no ?? row.show_id),
    clean(row.ring_no ?? row.ring_number),
    clean(row.ring_day_no),
    clean(row.class_number || classNumberFromLabel(row.class || row.class_name)),
    clean(row.class_name || classNameFromLabel(row.class)).toLowerCase()
  ].join("|");
}

function applyMockLive(scheduleRows, ringsRows, ordersRows) {
  const ringsByKey = new Map(ringsRows.filter((row) => row.class_no).map((row) => [liveKey(row), row]));
  const ordersByKey = new Map(ordersRows.map((row) => [orderFallbackKey(row), row]));

  return scheduleRows.map((row) => {
    const ringLive = ringsByKey.get(liveKey(row));
    const orderLive = ordersByKey.get(orderFallbackKey(row));
    const live = ringLive || orderLive;
    if (!live) return row;
    return {
      ...row,
      n_gone: intOrNull(live.n_gone),
      n_to_go: intOrNull(live.n_to_go),
      entry_count: intOrNull(live.total) ?? row.entry_count,
      current_entry_no: clean(live.entry).match(/#(\d+)/)?.[1] || row.current_entry_no || "",
      current_horse: clean(live.entry).replace(/^#\d+,?\s*/, "").replace(/<br>.*$/i, "") || row.current_horse || "",
      elapsed_seconds: intOrNull(live.elapsed) ?? row.elapsed_seconds,
      live_source: ringLive ? "mock_get_rings.php" : "mock_get_orders.php"
    };
  });
}

function estimateGoSeconds(classStartTime, entryOrder, nGone, elapsedSeconds) {
  const paceSeconds = nGone > 6 && elapsedSeconds > 0
    ? Math.max(30, Math.round(elapsedSeconds / nGone))
    : 120;
  return {
    paceSeconds,
    offsetSeconds: Math.max(0, (entryOrder - 1) * paceSeconds)
  };
}

async function main() {
  const showNo = arg("--show-no", "14906");
  const focusDay = arg("--focus-day", "2026-06-11");
  const baseUrl = arg("--base-url", DEFAULT_BASE);
  const url = `${baseUrl}?action=schedule-json&show_no=${encodeURIComponent(showNo)}&focus_day=${encodeURIComponent(focusDay)}`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`schedule-json HTTP ${response.status}: ${await response.text()}`);
  const scheduleRows = await response.json();
  if (!Array.isArray(scheduleRows) || scheduleRows.length === 0) throw new Error("schedule-json returned no rows");

  const target = scheduleRows.find((row) => row.class_no && row.ring_day_no && row.ring_number) || scheduleRows[0];
  const mockRings = [{
    show_no: showNo,
    ring_no: target.ring_number,
    ring_day_no: target.ring_day_no,
    class_no: target.class_no,
    ring: target.ring_name,
    day: focusDay,
    class: `${target.class_number}) ${target.class_name}`,
    entry: "#9999, Mock Horse<br>In ring at 9:10am",
    total: "30",
    n_to_go: "18",
    n_gone: "12",
    time: "9:10am",
    elapsed: 900
  }];
  const mockOrders = [{
    show_no: showNo,
    ring_no: target.ring_number,
    ring_day_no: target.ring_day_no,
    ring: target.ring_name,
    day: focusDay,
    class: `${target.class_number}) ${target.class_name}`,
    entry: "#8888, Mock Order Horse<br>In ring at 9:12am",
    total: "30",
    n_to_go: "17",
    n_gone: "13",
    time: "9:12am",
    elapsed: 975
  }];

  const ringsOnly = applyMockLive(scheduleRows, mockRings, []);
  const ordersOnly = applyMockLive(scheduleRows, [], mockOrders);
  const enrichedTarget = ringsOnly.find((row) => clean(row.class_no) === clean(target.class_no));
  const orderTarget = ordersOnly.find((row) => clean(row.class_no) === clean(target.class_no));
  const activeRows = ringsOnly.filter((row) => Array.isArray(row.trainer_rollups) && row.trainer_rollups.length > 0);
  const timing = estimateGoSeconds(target.class_start_time, 10, enrichedTarget.n_gone || 0, enrichedTarget.elapsed_seconds || 0);

  const checks = {
    schedule_rows: scheduleRows.length,
    active_rows: activeRows.length,
    mock_target_class_no: target.class_no,
    live_source: enrichedTarget.live_source,
    n_gone: enrichedTarget.n_gone,
    n_to_go: enrichedTarget.n_to_go,
    current_entry_no: enrichedTarget.current_entry_no,
    order_live_source: orderTarget.live_source,
    order_n_gone: orderTarget.n_gone,
    order_current_entry_no: orderTarget.current_entry_no,
    pace_seconds: timing.paceSeconds,
    entry_10_offset_seconds: timing.offsetSeconds,
    pass: enrichedTarget.live_source === "mock_get_rings.php" &&
      enrichedTarget.n_gone === 12 &&
      enrichedTarget.n_to_go === 18 &&
      enrichedTarget.current_entry_no === "9999" &&
      orderTarget.live_source === "mock_get_orders.php" &&
      orderTarget.n_gone === 13 &&
      orderTarget.current_entry_no === "8888" &&
      timing.paceSeconds === 75
  };

  console.log(JSON.stringify(checks, null, 2));
  if (!checks.pass) process.exit(1);
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
