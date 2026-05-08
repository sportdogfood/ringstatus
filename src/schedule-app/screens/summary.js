import { actionCard, aggregateCard, ringCard, scheduleCard, threadCard, tripCard } from "../components/card.js";
import { errorCard } from "../components/empty.js";
import { compact, escapeHtml, labelOrUnknown } from "../utils.js";

function readinessStrip(state) {
  const row = state.derived.scheduleRows[0] || {};
  const stats = state.derived.summaryStats;
  const updated = state.meta.lastFetchedAt ? new Date(state.meta.lastFetchedAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }) : "unknown";
  return `
    <div class="schedule-readiness">
      <span>${escapeHtml(row.showLabel || row.showDate || "Daily schedule")}</span>
      <strong>${escapeHtml(updated)}</strong>
      <span>${escapeHtml(stats.totalScheduleRows ? "data ready" : "empty")}</span>
    </div>
  `;
}

function mattersNow(state) {
  const stats = state.derived.summaryStats;
  const next = stats.nextFollowedTrip;
  if (next) {
    return tripCard(next, "summary-now");
  }

  if (stats.mostActiveRing) {
    return ringCard(stats.mostActiveRing, "summary-now");
  }

  return actionCard({
    title: stats.totalScheduleRows ? "Schedule loaded" : "No schedule rows found",
    subtitle: stats.totalScheduleRows ? `${stats.totalScheduleRows} schedule rows are ready.` : "The staged schedule source returned no rows.",
    chips: compact([stats.totalTripRows ? `${stats.totalTripRows} active trips` : null, stats.ringCount ? `${stats.ringCount} rings` : null]),
    detailType: "aggregate-detail",
    detailId: "agg:scheduleRows",
    source: "summary-now",
  });
}

function aggregateGrid(derived) {
  const stats = derived.summaryStats;
  const aggregates = [
    { id: "agg:scheduleRows", label: "Schedule rows", value: stats.totalScheduleRows, hint: "full map", rows: derived.scheduleRows },
    { id: "agg:tripRows", label: "Active trips", value: stats.totalTripRows, hint: "overlay", rows: derived.tripRows },
    { id: "agg:rings", label: "Rings", value: stats.ringCount, hint: "rings today", rows: derived.rings },
    { id: "agg:live", label: "Live", value: stats.liveCount, hint: "current rows", rows: derived.scheduleRows.filter((row) => row.statusBucket === "live") },
    { id: "agg:upcoming", label: "Upcoming", value: stats.upcomingCount, hint: "not complete", rows: derived.scheduleRows.filter((row) => row.statusBucket === "upcoming") },
    { id: "agg:completed", label: "Completed", value: stats.completedCount, hint: "done rows", rows: derived.scheduleRows.filter((row) => row.statusBucket === "completed") },
  ];

  return `<div class="schedule-aggregate-grid">${aggregates.map(aggregateCard).join("")}</div>`;
}

export function renderSummary(state) {
  const derived = state.derived;
  const topRings = [...derived.rings].sort((a, b) => b.followedTripCount - a.followedTripCount || b.scheduleRows.length - a.scheduleRows.length).slice(0, 3);
  const topSchedule = derived.scheduleRows.filter((row) => row.hasFollowedTrips).slice(0, 3);
  const cards = [...topRings.map((ring) => ringCard(ring, "summary-ring")), ...topSchedule.map((row) => scheduleCard(row, "summary-class"))].slice(0, 5);
  const threadPreview = derived.threads.slice(0, 3);

  return `
    <div class="list-column schedule-screen schedule-screen--summary">
      ${state.error ? errorCard(state.error) : ""}
      ${readinessStrip(state)}
      <div class="section-title">What matters now</div>
      ${mattersNow(state)}
      ${aggregateGrid(derived)}
      <div class="section-title">Relevant rings and classes</div>
      ${cards.length ? cards.join("") : actionCard({
        title: "Schedule loaded",
        subtitle: `${labelOrUnknown(derived.scheduleRows[0]?.showLabel)} has ${derived.scheduleRows.length} rows.`,
        chips: ["map ready"],
        detailType: "aggregate-detail",
        detailId: "agg:scheduleRows",
        source: "summary-empty-relevance",
      })}
      <div class="section-title">Recent threads</div>
      ${threadPreview.map((thread) => threadCard(thread, "summary-thread")).join("")}
    </div>
  `;
}
