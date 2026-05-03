function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function numOrNull(value) {
  if (isBlank(value)) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function strOrNull(value) {
  if (isBlank(value)) return null;
  return String(value).trim();
}

function buildClassDetailEndpoint({
  baseUrl,
  classId,
  showId,
  customerId = 15,
  classGroupId = null,
}) {
  if (isBlank(baseUrl) || isBlank(classId) || isBlank(showId)) return null;

  const url = new URL(`/classes/${encodeURIComponent(classId)}`, String(baseUrl).replace(/\/+$/, ""));
  url.searchParams.set("show_id", String(showId));
  url.searchParams.set("customer_id", String(customerId));
  if (!isBlank(classGroupId)) url.searchParams.set("cgid", String(classGroupId));
  return url.toString();
}

function normalizeClassEndpointWithCgid(rawEndpoint, classGroupId) {
  const raw = strOrNull(rawEndpoint);
  if (!raw) return null;

  try {
    const url = new URL(raw);
    if (!isBlank(classGroupId) && !url.searchParams.has("cgid")) {
      url.searchParams.set("cgid", String(classGroupId));
    }
    return url.toString();
  } catch {
    if (isBlank(classGroupId) || /[?&]cgid=/i.test(raw)) {
      return raw;
    }
    const separator = raw.includes("?") ? "&" : "?";
    return `${raw}${separator}cgid=${encodeURIComponent(classGroupId)}`;
  }
}

function tripUuid(trip) {
  return strOrNull(
    trip?.entryxclasses_uuid ??
    trip?.entryxclassesUUID ??
    trip?.entryxclasses_id ??
    trip?.entry_x_classes_uuid
  );
}

function classTrips(payload) {
  const related = payload?.class_related_data && typeof payload.class_related_data === "object"
    ? payload.class_related_data
    : null;
  if (Array.isArray(related?.trips)) return related.trips;
  if (Array.isArray(payload?.trips)) return payload.trips;
  return [];
}

function findClassTrip(payload, { entryxclassesUuid } = {}) {
  const wanted = strOrNull(entryxclassesUuid);
  if (!wanted) return null;
  return classTrips(payload).find((trip) => tripUuid(trip) === wanted) || null;
}

function classGroupOrderEntries(payload) {
  if (Array.isArray(payload?.class_group_order_of_go?.entries)) {
    return payload.class_group_order_of_go.entries;
  }
  if (Array.isArray(payload?.classGroupOrderOfGo?.entries)) {
    return payload.classGroupOrderOfGo.entries;
  }
  return [];
}

function findClassGroupOrderEntry(payload, { entryId, classId } = {}) {
  const wantedEntryId = numOrNull(entryId);
  const wantedClassId = numOrNull(classId);
  if (wantedEntryId === null || wantedClassId === null) return null;

  return classGroupOrderEntries(payload).find((entry) =>
    numOrNull(entry?.entry_id ?? entry?.entryId) === wantedEntryId &&
    numOrNull(entry?.class_id ?? entry?.classId) === wantedClassId
  ) || null;
}

module.exports = {
  buildClassDetailEndpoint,
  classGroupOrderEntries,
  classTrips,
  findClassGroupOrderEntry,
  findClassTrip,
  normalizeClassEndpointWithCgid,
};
