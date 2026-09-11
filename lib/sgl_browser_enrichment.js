function text(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function selectScheduleScrapeRequest(rows, focusDay) {
  const requested = rows.filter((row) => row?.fields?.schedule_scrape_now === true);
  const focus = text(focusDay);
  const matchingFocus = focus
    ? requested.filter((row) => text(row?.fields?.focus_day) === focus)
    : [];
  return matchingFocus[0] || requested[0] || null;
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

module.exports = { groupRecordsByUrl, selectScheduleScrapeRequest };
