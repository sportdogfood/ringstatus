import { compact, escapeHtml, labelOrUnknown } from "../utils.js";

function chipList(values) {
  return compact(values).map((value) => `<span class="row-tag">${escapeHtml(value)}</span>`).join("");
}

function detailRow(label, value) {
  if (value === undefined || value === null || String(value).trim() === "") return "";
  return `
    <div class="schedule-detail-row">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}

function relatedRow(row) {
  if (!row) return "";
  const isTrip = String(row.id || "").startsWith("trip:");
  const title = isTrip ? labelOrUnknown(row.horse) : labelOrUnknown(row.classLabel || row.label);
  const subtitle = compact([row.ringLabel, row.classLabel && isTrip ? row.classLabel : row.groupLabel, row.latestGO]).join(" | ");
  return `
    <div class="row schedule-related-row">
      <span class="row-title">${escapeHtml(title)}</span>
      <span class="row-tag">${escapeHtml(subtitle || row.statusBucket || "related")}</span>
    </div>
  `;
}

function titleFor(detail) {
  const payload = detail.payload || {};
  if (detail.type === "aggregate-detail") return payload.label || "Aggregate";
  if (detail.type === "ring-detail") return payload.label || payload.ringLabel || "Ring";
  if (detail.type === "group-detail") return payload.label || payload.groupLabel || "Group";
  if (detail.type === "class-detail") return payload.classLabel || payload.label || "Class";
  if (detail.type === "trip-detail") return payload.horse || "Trip";
  if (detail.type === "horse-detail") return payload.label || "Horse";
  if (detail.type === "rider-detail") return payload.label || "Rider";
  if (detail.type === "thread-detail") return payload.title || "Thread";
  return "Detail";
}

function subtitleFor(detail) {
  const payload = detail.payload || {};
  if (detail.type === "aggregate-detail") return payload.hint || `${payload.rows?.length || 0} related rows`;
  if (detail.type === "thread-detail") return payload.text || payload.type || "";
  return compact([
    payload.ringLabel || payload.label,
    payload.groupLabel,
    payload.classLabel,
    payload.rider,
  ]).join(" | ");
}

function rowsFor(detail) {
  const payload = detail.payload || {};
  if (detail.type === "aggregate-detail") {
    return [
      detailRow("Value", payload.value),
      detailRow("Rows", payload.rows?.length ?? detail.relatedRows?.length),
      detailRow("Source", detail.source),
    ].join("");
  }

  if (detail.type === "trip-detail") {
    return [
      detailRow("Horse", payload.horse),
      detailRow("Rider", payload.rider),
      detailRow("Entry", payload.entryNumber),
      detailRow("Ring", payload.ringLabel),
      detailRow("Class", payload.classLabel),
      detailRow("GO", payload.latestGO),
      detailRow("OOG", payload.oog),
      detailRow("Placing", payload.placing),
      detailRow("Score", payload.score),
      detailRow("Status", payload.statusBucket),
    ].join("");
  }

  if (detail.type === "thread-detail") {
    return [
      detailRow("Type", payload.type),
      detailRow("Status", payload.statusBucket),
      detailRow("Related", detail.relatedRows?.length || 0),
    ].join("");
  }

  return [
    detailRow("Ring", payload.ringLabel || payload.label),
    detailRow("Group", payload.groupLabel),
    detailRow("Class", payload.classLabel),
    detailRow("Time", payload.startDisplay || payload.estimatedStart),
    detailRow("Status", payload.statusBucket),
    detailRow("Total trips", payload.totalTrips),
    detailRow("Followed trips", payload.matchingTripCount || payload.followedTripCount),
    detailRow("Completed", payload.completedTrips),
    detailRow("Remaining", payload.remainingTrips),
  ].join("");
}

export function renderFlyup(detail) {
  if (!detail) return "";

  const payload = detail.payload || {};
  const related = detail.relatedRows || payload.rows || [];

  return `
    <div class="schedule-flyup-layer" data-action="close-detail">
      <section class="schedule-flyup" role="dialog" aria-modal="true" aria-labelledby="schedule-flyup-title" data-stop-close>
        <div class="schedule-flyup__handle" aria-hidden="true"></div>
        <div class="schedule-flyup__head">
          <div>
            <h2 id="schedule-flyup-title">${escapeHtml(titleFor(detail))}</h2>
            <p>${escapeHtml(subtitleFor(detail))}</p>
          </div>
          <button class="header-back schedule-flyup__close" type="button" data-action="close-detail">Close</button>
        </div>
        <div class="schedule-card__chips">
          ${chipList([detail.type, payload.statusBucket, payload.type, detail.source])}
        </div>
        <div class="schedule-detail-rows">
          ${rowsFor(detail)}
        </div>
        <div class="section-title">Related</div>
        <div class="schedule-related-list">
          ${related.length ? related.slice(0, 12).map(relatedRow).join("") : relatedRow({ label: "No related rows", statusBucket: "empty" })}
        </div>
      </section>
    </div>
  `;
}
