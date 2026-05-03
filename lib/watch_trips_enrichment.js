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

function buildClassSignupGroupEndpoint({
  baseUrl,
  classGroupId,
  entryId = null,
  showId,
  customerId = 15,
}) {
  if (isBlank(baseUrl) || isBlank(classGroupId) || isBlank(showId)) return null;

  const url = new URL(`/classsignup/${encodeURIComponent(classGroupId)}`, String(baseUrl).replace(/\/+$/, ""));
  if (!isBlank(entryId)) url.searchParams.set("eid", String(entryId));
  url.searchParams.set("show_id", String(showId));
  url.searchParams.set("customer_id", String(customerId));
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

function rowEntryId(row) {
  return numOrNull(row?.entry_id ?? row?.entryId);
}

function rowClassId(row) {
  return numOrNull(row?.class_id ?? row?.classId);
}

function rowClassNumber(row) {
  return numOrNull(row?.class_number ?? row?.classNumber);
}

function rowEntryNumber(row) {
  return numOrNull(
    row?.number ??
    row?.entry_number ??
    row?.entryNumber ??
    row?.entry_no ??
    row?.entryNo ??
    row?.h_eid
  );
}

function classMatches(row, wantedClassId) {
  if (wantedClassId === null) return true;
  const actualClassId = rowClassId(row);
  return actualClassId === null || actualClassId === wantedClassId;
}

function classTrips(payload) {
  const related = payload?.class_related_data && typeof payload.class_related_data === "object"
    ? payload.class_related_data
    : null;
  if (Array.isArray(related?.trips)) return related.trips;
  if (Array.isArray(payload?.trips)) return payload.trips;
  return [];
}

function findClassTrip(payload, { entryxclassesUuid, entryId, classId, entryNumber } = {}) {
  const wanted = strOrNull(entryxclassesUuid);
  const wantedEntryId = numOrNull(entryId);
  const wantedClassId = numOrNull(classId);
  const wantedEntryNumber = numOrNull(entryNumber);
  const trips = classTrips(payload);

  if (wanted) {
    const match = trips.find((trip) => tripUuid(trip) === wanted);
    if (match) return match;
  }

  if (wantedEntryId !== null) {
    const match = trips.find((trip) =>
      rowEntryId(trip) === wantedEntryId &&
      classMatches(trip, wantedClassId)
    );
    if (match) return match;
  }

  if (wantedEntryNumber !== null) {
    const match = trips.find((trip) =>
      rowEntryNumber(trip) === wantedEntryNumber &&
      classMatches(trip, wantedClassId)
    );
    if (match) return match;
  }

  return null;
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

function classSignupEntries(payload) {
  if (Array.isArray(payload?.entry_x_classes)) {
    return payload.entry_x_classes;
  }
  if (Array.isArray(payload?.entryXClasses)) {
    return payload.entryXClasses;
  }
  return [];
}

function findClassGroupOrderEntry(payload, { entryxclassesUuid, entryId, classId, entryNumber } = {}) {
  const wanted = strOrNull(entryxclassesUuid);
  const wantedEntryId = numOrNull(entryId);
  const wantedClassId = numOrNull(classId);
  const wantedEntryNumber = numOrNull(entryNumber);
  const entries = classGroupOrderEntries(payload);

  if (wanted) {
    const match = entries.find((entry) =>
      tripUuid(entry) === wanted &&
      classMatches(entry, wantedClassId)
    );
    if (match) return match;
  }

  if (wantedEntryId !== null && wantedClassId !== null) {
    const match = entries.find((entry) =>
      rowEntryId(entry) === wantedEntryId &&
      rowClassId(entry) === wantedClassId
    );
    if (match) return match;
  }

  if (wantedEntryNumber !== null && wantedClassId !== null) {
    const match = entries.find((entry) =>
      rowEntryNumber(entry) === wantedEntryNumber &&
      rowClassId(entry) === wantedClassId
    );
    if (match) return match;
  }

  return null;
}

function findClassSignupEntry(payload, { entryId, entryNumber, classNumber, classId } = {}) {
  const wantedEntryId = numOrNull(entryId);
  const wantedEntryNumber = numOrNull(entryNumber);
  const wantedClassNumber = numOrNull(classNumber);
  const wantedClassId = numOrNull(classId);
  const entries = classSignupEntries(payload);

  if (wantedEntryId !== null && wantedClassNumber !== null) {
    const match = entries.find((entry) =>
      rowEntryId(entry) === wantedEntryId &&
      rowClassNumber(entry) === wantedClassNumber
    );
    if (match) return match;
  }

  if (wantedEntryId !== null && wantedClassId !== null) {
    const match = entries.find((entry) =>
      rowEntryId(entry) === wantedEntryId &&
      rowClassId(entry) === wantedClassId
    );
    if (match) return match;
  }

  if (wantedEntryNumber !== null && wantedClassNumber !== null) {
    const match = entries.find((entry) =>
      rowEntryNumber(entry) === wantedEntryNumber &&
      rowClassNumber(entry) === wantedClassNumber
    );
    if (match) return match;
  }

  if (wantedEntryNumber !== null && wantedClassId !== null) {
    const match = entries.find((entry) =>
      rowEntryNumber(entry) === wantedEntryNumber &&
      rowClassId(entry) === wantedClassId
    );
    if (match) return match;
  }

  if (wantedEntryId !== null) {
    const matches = entries.filter((entry) => rowEntryId(entry) === wantedEntryId);
    if (matches.length === 1) return matches[0];
  }

  if (wantedEntryNumber !== null) {
    const matches = entries.filter((entry) => rowEntryNumber(entry) === wantedEntryNumber);
    if (matches.length === 1) return matches[0];
  }

  return null;
}

module.exports = {
  buildClassDetailEndpoint,
  buildClassSignupGroupEndpoint,
  classGroupOrderEntries,
  classSignupEntries,
  classTrips,
  findClassGroupOrderEntry,
  findClassSignupEntry,
  findClassTrip,
  normalizeClassEndpointWithCgid,
};
