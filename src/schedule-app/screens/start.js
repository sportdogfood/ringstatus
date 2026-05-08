import { escapeHtml } from "../utils.js";

export function renderStart({ meta, derived }) {
  const stats = derived?.summaryStats;
  const date = derived?.scheduleRows?.[0]?.showLabel || derived?.scheduleRows?.[0]?.showDate || "schedule pending";
  const loaded = stats?.totalScheduleRows ? "Ready" : "Waiting";

  return `
    <div class="list-column schedule-screen schedule-screen--start">
      <button class="row row--tap row--active" type="button" data-action="start-session">
        <span class="row-title">Start Session</span>
        <span class="row-tag row-tag--boolean row-tag--positive">${escapeHtml(loaded)}</span>
      </button>
      <div class="row">
        <span class="row-title">${escapeHtml(date)}</span>
        <span class="row-tag">${escapeHtml(stats?.totalScheduleRows ? `${stats.totalScheduleRows} rows` : "0 rows")}</span>
      </div>
      <div class="row">
        <span class="row-title">Last updated</span>
        <span class="row-tag">${escapeHtml(meta.lastFetchedAt ? new Date(meta.lastFetchedAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }) : "unknown")}</span>
      </div>
    </div>
  `;
}
