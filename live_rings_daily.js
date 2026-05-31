const crypto = require("crypto");

const TABLE_LIVE_RINGS = process.env.TABLE_LIVE_RINGS || "live_rings";
const LIVE_SCORE_WIDGET_URL = process.env.LIVE_SCORE_WIDGET_URL ||
  "https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget";

function isBlank(value) {
  return value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "") ||
    String(value).trim().toLowerCase() === "null" ||
    String(value).trim().toLowerCase() === "nan";
}

function firstValue(value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      if (!isBlank(item)) return item;
    }
    return undefined;
  }
  return value;
}

function strOrNull(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  return String(picked).trim();
}

function numOrNull(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  const num = Number(picked);
  return Number.isFinite(num) ? num : null;
}

function toIsoDateOnly(value) {
  const picked = firstValue(value);
  if (isBlank(picked)) return null;
  if (picked instanceof Date) return picked.toISOString().slice(0, 10);
  const text = String(picked).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return text.slice(0, 10);
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString().slice(0, 10) : null;
}

function buildRingKey({ customer_id, customerId, show_id, showId, focus_day, focusDay, ring_number, ringNumber }) {
  const customer = numOrNull(customer_id ?? customerId);
  const show = numOrNull(show_id ?? showId);
  const focus = toIsoDateOnly(focus_day ?? focusDay);
  const ring = numOrNull(ring_number ?? ringNumber);
  if (customer === null || show === null || !focus || ring === null) return "";
  return [customer, show, focus, ring].join("|");
}

function stableHash(value) {
  return crypto.createHash("sha1").update(JSON.stringify(value ?? null)).digest("hex");
}

function linkOne(recordId) {
  return recordId ? [recordId] : undefined;
}

function liveGroupLinkKey(showId, focusDay, ringNumber, classGroupId) {
  if (showId === null || !focusDay || ringNumber === null || classGroupId === null) return "";
  return [showId, focusDay, ringNumber, classGroupId].join("|");
}

function pickRingItem(ringData, bucketName) {
  const rows = Array.isArray(ringData?.[bucketName]) ? ringData[bucketName] : [];
  return rows[0] || null;
}

function ringStateFromBuckets(liveItem, nextItem, completedRows) {
  if (liveItem) return "live";
  if (nextItem) return "upcoming";
  if (Array.isArray(completedRows) && completedRows.length) return "completed";
  return "idle";
}

function progressText(item) {
  const gone = numOrNull(item?.gone);
  const total = numOrNull(item?.total);
  if (gone === null || total === null) return null;
  return `${gone}/${total}`;
}

function normalizeLiveRingSnapshots(payload, context = {}) {
  const rows = [];
  const shows = Array.isArray(payload) ? payload : [];
  const customerId = numOrNull(context.customer_id ?? context.customerId);
  const focusDay = toIsoDateOnly(context.focus_day ?? context.focusDay);
  const asOf = strOrNull(context.as_of ?? context.asOf) || new Date().toISOString();
  const liveGroupLinks = context.liveGroupLinks instanceof Map ? context.liveGroupLinks : new Map();

  for (const show of shows) {
    const showId = numOrNull(show?.show_id) ?? numOrNull(context.show_id ?? context.showId);
    if (showId === null || !focusDay) continue;
    const liveData = show?.live_data && typeof show.live_data === "object" ? show.live_data : {};

    for (const [ringNumberText, ringData] of Object.entries(liveData)) {
      const ringNumber = numOrNull(ringData?.ring_number) ?? numOrNull(ringNumberText);
      if (ringNumber === null) continue;

      const liveItem = pickRingItem(ringData, "livenow");
      const nextItem = pickRingItem(ringData, "upcoming");
      const completedRows = Array.isArray(ringData?.completed) ? ringData.completed : [];
      const liveClassGroupId = numOrNull(liveItem?.class_group_id);
      const nextClassGroupId = numOrNull(nextItem?.class_group_id);
      const liveLink = liveGroupLinks.get(liveGroupLinkKey(showId, focusDay, ringNumber, liveClassGroupId));
      const nextLink = liveGroupLinks.get(liveGroupLinkKey(showId, focusDay, ringNumber, nextClassGroupId));
      const ringId = numOrNull(liveItem?.ring_id) ?? numOrNull(nextItem?.ring_id) ?? numOrNull(ringData?.ring_id);
      const ringName = strOrNull(ringData?.name) || strOrNull(liveItem?.ring) || strOrNull(nextItem?.ring);
      const ringKey = buildRingKey({
        customer_id: customerId,
        show_id: showId,
        focus_day: focusDay,
        ring_number: ringNumber,
      });

      const fields = {
        ring_key: ringKey,
        response_ready: true,
        is_latest: true,
        show: linkOne(context.show_record_id ?? context.showRecordId),
        show_id: showId,
        focus_day: focusDay,
        ring_number: ringNumber,
        ring_id: ringId,
        ring_name: ringName,
        is_current_scope: true,
        dropped_at: null,
        as_of: asOf,
        last_seen_at: asOf,
        payload_hash: stableHash(ringData),
        ring_query_key: ringKey,
        live_group: linkOne(liveLink),
        live_class_group_id: liveClassGroupId,
        live_status: strOrNull(liveItem?.status),
        live_start_time: strOrNull(liveItem?.estimated_start_time),
        live_gone: numOrNull(liveItem?.gone),
        live_total: numOrNull(liveItem?.total),
        live_progress: progressText(liveItem),
        next_group: linkOne(nextLink),
        next_class_group_id: nextClassGroupId,
        next_start_time: strOrNull(nextItem?.estimated_start_time),
        ring_state: ringStateFromBuckets(liveItem, nextItem, completedRows),
      };

      for (const [key, value] of Object.entries(fields)) {
        if (value === undefined || value === null) delete fields[key];
      }
      fields.dropped_at = null;

      rows.push({
        key: ringKey,
        fields,
        snapshot_hash: stableHash({ live: liveItem, next: nextItem, completed_count: completedRows.length }),
      });
    }
  }

  return rows;
}

module.exports = {
  LIVE_SCORE_WIDGET_URL,
  TABLE_LIVE_RINGS,
  buildRingKey,
  normalizeLiveRingSnapshots,
};
