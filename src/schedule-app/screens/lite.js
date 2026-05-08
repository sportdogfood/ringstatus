import { tripCard } from "../components/card.js";
import { emptyCard } from "../components/empty.js";
import { renderGlobalToggles, renderPeakFilters, renderSearch } from "../components/filters.js";
import { normalizeKey } from "../utils.js";

function applyFilters(rows, state) {
  const global = state.filters.global;
  const screenFilter = state.filters.lite;
  const query = normalizeKey(state.search.lite);

  return rows
    .filter((trip) => trip.isFollowed)
    .filter((trip) => global.scope === "full" || trip.statusBucket !== "completed")
    .filter((trip) => global.status === "all" || trip.statusBucket === "live")
    .filter((trip) => screenFilter === "all" || trip.statusBucket === screenFilter)
    .filter((trip) => {
      if (!query) return true;
      return normalizeKey([trip.horse, trip.rider, trip.classLabel, trip.ringLabel].join(" ")).includes(query);
    });
}

export function renderLite(state) {
  const rows = applyFilters(state.derived.tripRows, state);
  const filters = [
    { value: "all", label: "All" },
    { value: "upcoming", label: "Upcoming" },
    { value: "live", label: "Live" },
    { value: "completed", label: "Completed" },
    { value: "unknown", label: "Unknown" },
  ];

  return `
    <div class="list-column schedule-screen schedule-screen--lite">
      ${renderGlobalToggles(state.filters)}
      ${renderSearch("lite", state.search.lite, "Search horse, rider, class, ring")}
      ${renderPeakFilters("lite", state.filters.lite, filters)}
      <div class="schedule-card-stack">
        ${rows.length ? rows.map((trip) => tripCard(trip, "lite")).join("") : emptyCard("No followed trips found for the current filters.")}
      </div>
    </div>
  `;
}
