export function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

export function normalizeKey(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

export function compact(values) {
  return values.filter((value) => value !== undefined && value !== null && String(value).trim() !== "");
}

export function firstValue(source, keys) {
  const row = source?.fields && typeof source.fields === "object" ? source.fields : source;
  for (const key of keys) {
    const value = row?.[key];
    if (Array.isArray(value) && value.length) return value[0];
    if (value !== undefined && value !== null && String(value).trim() !== "") return value;
  }
  return null;
}

export function numberValue(value) {
  if (value === undefined || value === null || value === "") return null;
  const numeric = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(numeric) ? numeric : null;
}

export function yes(value) {
  if (value === true) return true;
  const text = normalizeKey(value);
  return text === "true" || text === "1" || text === "yes" || text === "y";
}

export function labelOrUnknown(value) {
  const text = String(value ?? "").trim();
  return text || "unknown";
}

export function formatCount(value, singular, plural = `${singular}s`) {
  const count = Number(value) || 0;
  return `${count} ${count === 1 ? singular : plural}`;
}

export function byNumberThenLabel(a, b) {
  const an = numberValue(a.sortValue ?? a.ringNumber ?? a.classNumber);
  const bn = numberValue(b.sortValue ?? b.ringNumber ?? b.classNumber);
  if (an !== null && bn !== null && an !== bn) return an - bn;
  if (an !== null && bn === null) return -1;
  if (an === null && bn !== null) return 1;
  return String(a.label || a.name || "").localeCompare(String(b.label || b.name || ""));
}

export function bucketStatus(rawStatus, row = {}) {
  const completedTrips = numberValue(row.completedTrips ?? row.completed_trips ?? row.completed);
  const remainingTrips = numberValue(row.remainingTrips ?? row.remaining_trips ?? row.remaining);
  const estimated = row.estimatedStart || row.startDisplay || row.latestStart || row.estimatedGO || row.latestGO;
  const status = normalizeKey(rawStatus || row.status || row.latestStatus || row.latest_status);

  if (completedTrips !== null && completedTrips > 0 && remainingTrips === 0) return "completed";
  if (/complete|completed|done|result|placed|score|finished/.test(status)) return "completed";
  if (/live|current|running|now|active|in progress/.test(status)) return "live";
  if (estimated) return "upcoming";
  return "unknown";
}

export function uniqueBy(items, getKey) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = getKey(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}
