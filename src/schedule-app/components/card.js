import { compact, escapeHtml, formatCount, labelOrUnknown } from "../utils.js";

function chips(values) {
  return compact(values).map((value) => `<span class="row-tag">${escapeHtml(value)}</span>`).join("");
}

function detailAttrs(type, id, source = "card") {
  return `data-detail-type="${escapeHtml(type)}" data-detail-id="${escapeHtml(id)}" data-detail-source="${escapeHtml(source)}"`;
}

export function aggregateCard({ id, label, value, hint, rows = [] }) {
  return `
    <button class="schedule-card schedule-card--metric" type="button" data-card-role="summary-card" ${detailAttrs("aggregate-detail", id, "summary-aggregate")}>
      <span class="schedule-card__kicker">${escapeHtml(label)}</span>
      <strong class="schedule-card__metric">${escapeHtml(value)}</strong>
      <span class="schedule-card__hint">${escapeHtml(hint || formatCount(rows.length, "row"))}</span>
    </button>
  `;
}

export function actionCard({ title, subtitle, chips: chipValues = [], detailType, detailId, source, role = "summary-card" }) {
  return `
    <button class="schedule-card schedule-card--action" type="button" data-card-role="${escapeHtml(role)}" ${detailAttrs(detailType, detailId, source)}>
      <span class="schedule-card__title">${escapeHtml(title)}</span>
      <span class="schedule-card__subtitle">${escapeHtml(subtitle)}</span>
      <span class="schedule-card__chips">${chips(chipValues)}</span>
    </button>
  `;
}

export function ringCard(ring, source = "ring-card") {
  const liveCount = ring.scheduleRows.filter((row) => row.statusBucket === "live").length;
  return `
    <button class="schedule-card schedule-card--ring" type="button" data-card-role="ring-card" ${detailAttrs("ring-detail", ring.id, source)}>
      <span class="schedule-card__kicker">${escapeHtml(ring.label)}</span>
      <span class="schedule-card__title">${escapeHtml(formatCount(ring.scheduleRows.length, "schedule row"))}</span>
      <span class="schedule-card__subtitle">${escapeHtml(formatCount(ring.followedTripCount, "followed trip"))}</span>
      <span class="schedule-card__chips">${chips([liveCount ? `${liveCount} live` : null, ring.followedTripCount ? "overlay" : "map only"])}</span>
    </button>
  `;
}

export function scheduleCard(row, source = "schedule-card") {
  return `
    <button class="schedule-card schedule-card--schedule ${row.hasFollowedTrips ? "is-active" : ""}" type="button" data-card-role="class-card" ${detailAttrs("class-detail", row.id, source)}>
      <span class="schedule-card__kicker">${escapeHtml(row.ringLabel)}</span>
      <span class="schedule-card__title">${escapeHtml(labelOrUnknown(row.classLabel))}</span>
      <span class="schedule-card__subtitle">${escapeHtml(compact([row.startDisplay || row.estimatedStart, row.groupLabel]).join(" | ") || "time unknown")}</span>
      <span class="schedule-card__chips">
        ${chips([
          row.statusBucket,
          row.totalTrips !== null ? `${row.totalTrips} trips` : null,
          row.matchingTripCount ? `${row.matchingTripCount} followed` : "no overlay",
          row.remainingTrips !== null ? `${row.remainingTrips} remaining` : null,
        ])}
      </span>
    </button>
  `;
}

export function tripCard(trip, source = "trip-card") {
  return `
    <button class="schedule-card schedule-card--trip" type="button" data-card-role="trip-card" ${detailAttrs("trip-detail", trip.id, source)}>
      <span class="schedule-card__kicker">${escapeHtml(labelOrUnknown(trip.ringLabel))}</span>
      <span class="schedule-card__title">${escapeHtml(labelOrUnknown(trip.horse))}</span>
      <span class="schedule-card__subtitle">${escapeHtml(compact([trip.rider, trip.classLabel]).join(" | ") || "trip detail")}</span>
      <span class="schedule-card__chips">
        ${chips([
          trip.statusBucket,
          trip.latestGO ? `GO ${trip.latestGO}` : null,
          trip.oog ? `OOG ${trip.oog}` : null,
          trip.placing ? `Place ${trip.placing}` : null,
          trip.score ? `Score ${trip.score}` : null,
        ])}
      </span>
    </button>
  `;
}

export function threadCard(thread, source = "thread-card") {
  return `
    <button class="schedule-card schedule-card--thread" type="button" data-card-role="thread-card" ${detailAttrs("thread-detail", thread.id, source)}>
      <span class="schedule-card__kicker">${escapeHtml(thread.type)}</span>
      <span class="schedule-card__title">${escapeHtml(thread.title)}</span>
      <span class="schedule-card__subtitle">${escapeHtml(thread.text)}</span>
      <span class="schedule-card__chips">${chips([thread.statusBucket, thread.relatedRows?.length ? `${thread.relatedRows.length} related` : null])}</span>
    </button>
  `;
}
