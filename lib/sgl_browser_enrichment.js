function text(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function selectScrapeRequest(rows, focusDay, requestField) {
  const requested = rows.filter((row) => row?.fields?.[requestField] === true);
  const focus = text(focusDay);
  const matchingFocus = focus
    ? requested.filter((row) => text(row?.fields?.focus_day) === focus)
    : [];
  return matchingFocus[0] || requested[0] || null;
}

function selectScheduleScrapeRequest(rows, focusDay) {
  return selectScrapeRequest(rows, focusDay, "schedule_scrape_now");
}

function selectTripsScrapeRequest(rows, focusDay) {
  return selectScrapeRequest(rows, focusDay, "trips_scrape_oog_now");
}

function groupRecordsByUrl(records, getUrl) {
  const groups = new Map();
  const missing = [];
  for (const record of records) {
    const url = text(getUrl(record));
    if (!url) {
      missing.push(record);
      continue;
    }
    if (!groups.has(url)) groups.set(url, []);
    groups.get(url).push(record);
  }
  return { groups, missing };
}

function isUnderSaddleRecord(record) {
  const fields = record?.fields || {};
  const sequenceType = text(fields.schedule_sequencetype).toLowerCase();
  const orderSet = text(fields.order_set).toLowerCase();
  return sequenceType.includes("under saddle")
    || sequenceType.includes("flat")
    || orderSet === "class is under saddle";
}

function countOrderSetStatuses(records) {
  const counts = { order_not_yet_set: 0, order_is_set: 0, class_is_under_saddle: 0, err: 0, blank_or_other: 0 };
  for (const record of records) {
    const status = text(record?.fields?.order_set).toLowerCase();
    if (status === "order not yet set") counts.order_not_yet_set += 1;
    else if (status === "order is set") counts.order_is_set += 1;
    else if (status === "class is under saddle") counts.class_is_under_saddle += 1;
    else if (status === "err") counts.err += 1;
    else counts.blank_or_other += 1;
  }
  return counts;
}

module.exports = { countOrderSetStatuses, groupRecordsByUrl, isUnderSaddleRecord, selectScheduleScrapeRequest, selectTripsScrapeRequest };
