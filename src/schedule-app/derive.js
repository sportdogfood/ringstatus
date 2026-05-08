import {
  bucketStatus,
  byNumberThenLabel,
  compact,
  firstValue,
  formatCount,
  labelOrUnknown,
  normalizeKey,
  numberValue,
  uniqueBy,
  yes,
} from "./utils.js";

function visibleRow(row) {
  return row?.fields && typeof row.fields === "object" ? row.fields : row;
}

function ringKeyFor(row) {
  const ringNumber = firstValue(row, ["ring_number", "ringNumber", "ring_no", "ring"]);
  const ringId = firstValue(row, ["ring_id", "ringId"]);
  const ringName = firstValue(row, ["ringName", "ring_name", "ring_nickname"]);
  return normalizeKey(ringNumber || ringId || ringName || "unmatched-ring") || "unmatched-ring";
}

function classLabelFor(row) {
  return compact([
    firstValue(row, ["class_number", "classNumber"]),
    firstValue(row, ["class_name", "className", "group_name", "group_display"]),
  ]).join(" - ");
}

function scheduleMatchKeys(row) {
  return compact([
    row.classGroupId && `class-group:${row.classGroupId}`,
    row.classId && `class:${row.classId}`,
    row.ringKey && row.classNumber && `ring-class-number:${row.ringKey}:${normalizeKey(row.classNumber)}`,
    row.ringKey && row.classLabel && `ring-class-label:${row.ringKey}:${normalizeKey(row.classLabel)}`,
    row.classTextKey && `class-text:${row.classTextKey}`,
  ]);
}

function tripMatchKeys(row) {
  return compact([
    row.classGroupId && `class-group:${row.classGroupId}`,
    row.classId && `class:${row.classId}`,
    row.ringKey && row.classNumber && `ring-class-number:${row.ringKey}:${normalizeKey(row.classNumber)}`,
    row.ringKey && row.classLabel && `ring-class-label:${row.ringKey}:${normalizeKey(row.classLabel)}`,
    row.classTextKey && `class-text:${row.classTextKey}`,
  ]);
}

function normalizeScheduleRow(source, index) {
  const row = visibleRow(source);
  const ringNumber = firstValue(row, ["ring_number", "ringNumber", "ring_no", "ring"]);
  const ringName = firstValue(row, ["ringName", "ring_name", "ring_nickname"]) || (ringNumber ? `Ring ${ringNumber}` : null);
  const groupId = firstValue(row, ["class_group_id", "group_id", "class_groupxclasses_id"]);
  const classGroupId = firstValue(row, ["class_groupxclasses_id", "class_group_id", "group_id"]);
  const classId = firstValue(row, ["class_id", "entryxclasses_uuid"]);
  const classNumber = firstValue(row, ["class_number", "classNumber"]);
  const className = firstValue(row, ["class_name", "className"]);
  const groupLabel = firstValue(row, ["group_display", "group_name", "group_label"]) || className || "unknown group";
  const classLabel = classLabelFor(row) || groupLabel || "unknown class";
  const ringKey = ringKeyFor(row);
  const groupKey = normalizeKey(groupId || `${ringKey}:${groupLabel}`) || `group:${index}`;
  const classKey = normalizeKey(classId || classGroupId || `${ringKey}:${classLabel}`) || `class:${index}`;
  const completedTrips = numberValue(firstValue(row, ["completed_trips", "completedTrips", "completed"]));
  const remainingTrips = numberValue(firstValue(row, ["remaining_trips", "remainingTrips", "remaining"]));
  const totalTrips = numberValue(firstValue(row, ["total_trips", "totalTrips", "rollup_trips", "rollup_entries"]));
  const statusRaw = firstValue(row, ["status", "latestStatus", "latest_status", "scope_status"]);
  const estimatedStart = firstValue(row, ["estimated_start_time", "estimatedStart", "latestStart", "start_display", "start_display_short"]);
  const normalized = {
    id: `schedule:${classKey || groupKey || index}`,
    source,
    index,
    showId: firstValue(row, ["show_id", "sid", "app_show_id", "app_show_idv2"]),
    showDate: firstValue(row, ["show_date", "dt", "app_sql_date", "app_sql_datev2", "scheduled_date"]),
    showLabel: firstValue(row, ["show_days_report_title", "show_days_display_date", "show_name"]),
    ringNumber,
    ringName,
    ringLabel: ringName || (ringNumber ? `Ring ${ringNumber}` : "unknown ring"),
    ringKey,
    groupId,
    groupLabel,
    groupKey,
    classGroupId,
    classId,
    classNumber,
    className,
    classLabel,
    classKey,
    classTextKey: normalizeKey(compact([classNumber, className, groupLabel]).join(" ")),
    estimatedStart,
    startDisplay: firstValue(row, ["start_display", "start_display_short", "latestStart", "estimated_start_time"]),
    statusRaw,
    completedTrips,
    remainingTrips,
    totalTrips,
    matchingTrips: [],
    matchingTripCount: 0,
    hasFollowedTrips: false,
    sortValue: numberValue(firstValue(row, ["time_sort", "class_group_sequence", "schedule_sequence", "ring_number"])),
  };
  normalized.statusBucket = bucketStatus(statusRaw, normalized);
  return normalized;
}

function normalizeTripRow(source, index) {
  const row = visibleRow(source);
  const ringNumber = firstValue(row, ["ring_number", "ringNumber", "ring_no", "ring"]);
  const ringName = firstValue(row, ["ringName", "ring_name", "ring_nickname"]) || (ringNumber ? `Ring ${ringNumber}` : null);
  const classGroupId = firstValue(row, ["class_groupxclasses_id", "class_group_id", "group_id"]);
  const classId = firstValue(row, ["class_id", "entryxclasses_uuid"]);
  const classNumber = firstValue(row, ["class_number", "classNumber"]);
  const className = firstValue(row, ["class_name", "className"]);
  const groupLabel = firstValue(row, ["group_display", "group_name", "group_label"]) || className || "unknown group";
  const classLabel = classLabelFor(row) || groupLabel || "unknown class";
  const ringKey = ringKeyFor(row);
  const horse = firstValue(row, ["horseName", "horse_name", "horse", "barnName", "sched_display", "teamName"]);
  const rider = firstValue(row, ["riderName", "rider_name", "rider", "groomName"]);
  const tripId = firstValue(row, ["trip_id", "tripId", "entryxclasses_uuid", "entry_id"]);
  const latestGO = firstValue(row, ["latestGO", "latest_go", "estimatedGO", "estimated_go_time", "rs_go_time"]);
  const statusRaw = firstValue(row, ["status", "latestStatus", "latest_status"]);
  const normalized = {
    id: `trip:${tripId || `${ringKey}:${classGroupId || classId || classNumber || index}:${normalizeKey(horse || rider || index)}`}`,
    source,
    index,
    showId: firstValue(row, ["show_id", "sid", "app_show_id", "app_show_idv2"]),
    showDate: firstValue(row, ["show_date", "dt", "schedule_show_datev2", "scheduled_date"]),
    ringNumber,
    ringName,
    ringLabel: ringName || (ringNumber ? `Ring ${ringNumber}` : "unknown ring"),
    ringKey,
    groupId: firstValue(row, ["class_group_id", "group_id"]),
    groupLabel,
    groupKey: normalizeKey(firstValue(row, ["class_group_id", "group_id"]) || `${ringKey}:${groupLabel}`),
    classGroupId,
    classId,
    classNumber,
    className,
    classLabel,
    classKey: normalizeKey(classId || classGroupId || `${ringKey}:${classLabel}`),
    classTextKey: normalizeKey(compact([classNumber, className, groupLabel]).join(" ")),
    horse,
    horseKey: normalizeKey(horse),
    rider,
    riderKey: normalizeKey(rider),
    entryNumber: firstValue(row, ["entryNumber", "entry_number", "backNumber", "back_number"]),
    latestGO,
    oog: firstValue(row, ["runningOOG", "lastOOG", "oog", "rs_order_of_go"]),
    placing: firstValue(row, ["latestPlacing", "lastPlace", "placing", "place"]),
    score: firstValue(row, ["lastScore", "score", "score1"]),
    latestStatus: statusRaw,
    completedTrips: numberValue(firstValue(row, ["completed_trips", "completedTrips"])),
    remainingTrips: numberValue(firstValue(row, ["remaining_trips", "remainingTrips"])),
    secondsTill: numberValue(firstValue(row, ["secondsTill", "rs_min_till_go"])),
    isFollowed: yes(firstValue(row, ["is_target", "followed", "is_followed", "active"])) || Boolean(horse || rider || tripId),
    matchingScheduleRow: null,
  };
  normalized.statusBucket = bucketStatus(statusRaw, normalized);
  return normalized;
}

function addToMultiMap(map, key, value) {
  if (!key) return;
  if (!map.has(key)) map.set(key, []);
  map.get(key).push(value);
}

function createEntities(scheduleRows, tripRows) {
  const ringMap = new Map();
  const groupMap = new Map();
  const classMap = new Map();
  const horseMap = new Map();
  const riderMap = new Map();

  for (const row of scheduleRows) {
    if (!ringMap.has(row.ringKey)) {
      ringMap.set(row.ringKey, {
        id: `ring:${row.ringKey}`,
        key: row.ringKey,
        label: row.ringLabel,
        ringNumber: row.ringNumber,
        scheduleRows: [],
        tripRows: [],
      });
    }
    ringMap.get(row.ringKey).scheduleRows.push(row);

    if (!groupMap.has(row.groupKey)) {
      groupMap.set(row.groupKey, {
        id: `group:${row.groupKey}`,
        key: row.groupKey,
        label: row.groupLabel,
        ringLabel: row.ringLabel,
        scheduleRows: [],
        tripRows: [],
      });
    }
    groupMap.get(row.groupKey).scheduleRows.push(row);

    if (!classMap.has(row.classKey)) {
      classMap.set(row.classKey, {
        id: `class:${row.classKey}`,
        key: row.classKey,
        label: row.classLabel,
        ringLabel: row.ringLabel,
        scheduleRows: [],
        tripRows: [],
        totalTrips: 0,
      });
    }
    classMap.get(row.classKey).scheduleRows.push(row);
    classMap.get(row.classKey).totalTrips += row.totalTrips || 0;
  }

  for (const trip of tripRows) {
    if (ringMap.has(trip.ringKey)) ringMap.get(trip.ringKey).tripRows.push(trip);
    if (groupMap.has(trip.groupKey)) groupMap.get(trip.groupKey).tripRows.push(trip);
    if (classMap.has(trip.classKey)) classMap.get(trip.classKey).tripRows.push(trip);

    if (trip.horseKey && !horseMap.has(trip.horseKey)) {
      horseMap.set(trip.horseKey, { id: `horse:${trip.horseKey}`, key: trip.horseKey, label: trip.horse, tripRows: [] });
    }
    if (trip.horseKey) horseMap.get(trip.horseKey).tripRows.push(trip);

    if (trip.riderKey && !riderMap.has(trip.riderKey)) {
      riderMap.set(trip.riderKey, { id: `rider:${trip.riderKey}`, key: trip.riderKey, label: trip.rider, tripRows: [] });
    }
    if (trip.riderKey) riderMap.get(trip.riderKey).tripRows.push(trip);
  }

  const rings = [...ringMap.values()].sort(byNumberThenLabel);
  const groups = [...groupMap.values()].sort(byNumberThenLabel);
  const classes = [...classMap.values()].sort(byNumberThenLabel);
  const horses = [...horseMap.values()].sort(byNumberThenLabel);
  const riders = [...riderMap.values()].sort(byNumberThenLabel);

  for (const ring of rings) {
    ring.matchingTripCount = ring.tripRows.length;
    ring.followedTripCount = ring.tripRows.filter((trip) => trip.isFollowed).length;
  }

  for (const item of [...groups, ...classes]) {
    item.matchingTripCount = item.tripRows.length;
    item.followedTripCount = item.tripRows.filter((trip) => trip.isFollowed).length;
  }

  return { rings, groups, classes, horses, riders };
}

function createThreads(scheduleRows, tripRows, rings, classes, meta) {
  const threads = [];
  const followedTrips = tripRows.filter((trip) => trip.isFollowed);
  const shortSource = (source) => {
    if (!source) return "unknown source";
    const parts = String(source).split(";").filter(Boolean);
    if (parts.length > 1) return `${parts.length} staged trip files`;
    return parts[0].split(/[\\/]/).filter(Boolean).slice(-2).join("/") || parts[0];
  };

  threads.push({
    id: "thread:data-refresh",
    type: "data-refresh",
    title: "Data refreshed",
    text: `Schedule ${shortSource(meta.scheduleSource)}; trips ${shortSource(meta.tripsSource)}.`,
    statusBucket: "unknown",
    relatedRows: [],
  });

  for (const ring of rings.filter((item) => item.scheduleRows.length).slice(0, 8)) {
    threads.push({
      id: `thread:ring:${ring.key}`,
      type: "ring-summary",
      title: `${ring.label} has ${formatCount(ring.scheduleRows.length, "schedule row")}`,
      text: `${formatCount(ring.followedTripCount, "followed trip")} found in this ring.`,
      statusBucket: ring.tripRows.some((trip) => trip.statusBucket === "live") ? "live" : "upcoming",
      relatedRows: [...ring.scheduleRows.slice(0, 8), ...ring.tripRows.slice(0, 8)],
    });
  }

  for (const trip of followedTrips.slice(0, 10)) {
    const horse = labelOrUnknown(trip.horse);
    threads.push({
      id: `thread:trip:${trip.id}`,
      type: trip.statusBucket === "completed" ? "completed" : "followed-trip",
      title: `${horse} appears in ${labelOrUnknown(trip.classLabel)}`,
      text: `${labelOrUnknown(trip.ringLabel)}${trip.latestGO ? ` at ${trip.latestGO}` : ""}.`,
      statusBucket: trip.statusBucket,
      relatedRows: compact([trip.matchingScheduleRow, trip]),
    });
  }

  for (const item of classes.filter((klass) => klass.totalTrips || klass.followedTripCount).slice(0, 8)) {
    threads.push({
      id: `thread:class:${item.key}`,
      type: "class-summary",
      title: `${item.label} has ${formatCount(item.totalTrips || item.matchingTripCount, "trip")}`,
      text: `${formatCount(item.followedTripCount, "followed overlay")} tied to this class.`,
      statusBucket: item.followedTripCount ? "upcoming" : "unknown",
      relatedRows: [...item.scheduleRows.slice(0, 4), ...item.tripRows.slice(0, 6)],
    });
  }

  for (const row of scheduleRows.filter((item) => item.matchingTripCount === 0).slice(0, 6)) {
    threads.push({
      id: `thread:missing:${row.id}`,
      type: "missing-data",
      title: `${row.classLabel} has no trip overlay`,
      text: `${row.ringLabel} is present in the schedule map without followed trip detail.`,
      statusBucket: "unknown",
      relatedRows: [row],
    });
  }

  return threads;
}

function createSummaryStats(scheduleRows, tripRows, rings, groups, classes, horses, riders, threads, meta) {
  const followedTrips = tripRows.filter((trip) => trip.isFollowed);
  const followedUpcoming = followedTrips.filter((trip) => trip.statusBucket === "upcoming" || trip.statusBucket === "live");
  const followedCompleted = followedTrips.filter((trip) => trip.statusBucket === "completed");
  const nextFollowedTrip = [...followedUpcoming].sort((a, b) => {
    const aSeconds = a.secondsTill ?? Number.MAX_SAFE_INTEGER;
    const bSeconds = b.secondsTill ?? Number.MAX_SAFE_INTEGER;
    return aSeconds - bSeconds;
  })[0] || followedTrips[0] || null;
  const mostActiveRing = [...rings].sort((a, b) => b.followedTripCount - a.followedTripCount || b.scheduleRows.length - a.scheduleRows.length)[0] || null;

  return {
    totalScheduleRows: scheduleRows.length,
    totalTripRows: tripRows.length,
    ringCount: rings.length,
    groupCount: groups.length,
    classCount: classes.length,
    followedHorseCount: horses.length,
    followedRiderCount: riders.length,
    liveCount: scheduleRows.filter((row) => row.statusBucket === "live").length,
    upcomingCount: scheduleRows.filter((row) => row.statusBucket === "upcoming").length,
    completedCount: scheduleRows.filter((row) => row.statusBucket === "completed").length,
    followedUpcomingCount: followedUpcoming.length,
    followedCompletedCount: followedCompleted.length,
    lastGeneratedAt: meta.lastGeneratedAt || null,
    lastFetchedAt: meta.lastFetchedAt || null,
    nextFollowedTrip,
    mostActiveRing,
    navCounts: {
      summary: followedTrips.length,
      lite: followedUpcoming.length,
      full: scheduleRows.length || groups.length,
      threads: threads.length,
    },
  };
}

export function deriveAppModel(rawScheduleRows = [], rawTripRows = [], meta = {}) {
  const scheduleRows = rawScheduleRows.map(normalizeScheduleRow);
  const tripRows = rawTripRows.map(normalizeTripRow);
  const lookup = new Map();

  for (const row of scheduleRows) {
    for (const key of scheduleMatchKeys(row)) addToMultiMap(lookup, key, row);
  }

  for (const trip of tripRows) {
    const match = tripMatchKeys(trip).map((key) => lookup.get(key)?.[0]).find(Boolean) || null;
    trip.matchingScheduleRow = match;
    if (match) {
      trip.ringKey = match.ringKey || trip.ringKey;
      trip.groupKey = match.groupKey || trip.groupKey;
      trip.classKey = match.classKey || trip.classKey;
      match.matchingTrips.push(trip);
    }
  }

  for (const row of scheduleRows) {
    row.matchingTrips = uniqueBy(row.matchingTrips, (trip) => trip.id);
    row.matchingTripCount = row.matchingTrips.length;
    row.hasFollowedTrips = row.matchingTrips.some((trip) => trip.isFollowed);
  }

  const sortedScheduleRows = scheduleRows.sort((a, b) => (
    byNumberThenLabel({ ...a, sortValue: a.ringNumber }, { ...b, sortValue: b.ringNumber }) ||
    byNumberThenLabel(a, b)
  ));
  const sortedTripRows = tripRows.sort((a, b) => (a.secondsTill ?? Number.MAX_SAFE_INTEGER) - (b.secondsTill ?? Number.MAX_SAFE_INTEGER));
  const { rings, groups, classes, horses, riders } = createEntities(sortedScheduleRows, sortedTripRows);
  const threads = createThreads(sortedScheduleRows, sortedTripRows, rings, classes, meta);
  const summaryStats = createSummaryStats(sortedScheduleRows, sortedTripRows, rings, groups, classes, horses, riders, threads, meta);

  return {
    scheduleRows: sortedScheduleRows,
    tripRows: sortedTripRows,
    rings,
    groups,
    classes,
    horses,
    riders,
    threads,
    summaryStats,
  };
}
