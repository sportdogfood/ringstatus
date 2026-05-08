import { threadCard } from "../components/card.js";
import { emptyCard } from "../components/empty.js";
import { renderGlobalToggles, renderPeakFilters, renderSearch } from "../components/filters.js";
import { normalizeKey } from "../utils.js";

const TYPES = [
  { value: "all", label: "All" },
  { value: "followed-trip", label: "Followed" },
  { value: "ring-summary", label: "Rings" },
  { value: "class-summary", label: "Classes" },
  { value: "data-refresh", label: "Refresh" },
  { value: "missing-data", label: "Missing" },
  { value: "completed", label: "Completed" },
  { value: "upcoming", label: "Upcoming" },
];

function applyFilters(rows, state) {
  const query = normalizeKey(state.search.threads);
  const type = state.filters.threads;
  const global = state.filters.global;

  return rows
    .filter((thread) => type === "all" || thread.type === type || thread.statusBucket === type)
    .filter((thread) => global.status === "all" || thread.statusBucket === "live")
    .filter((thread) => global.scope === "full" || thread.type !== "missing-data")
    .filter((thread) => {
      if (!query) return true;
      const related = (thread.relatedRows || []).map((row) => [row.horse, row.rider, row.ringLabel, row.classLabel, row.statusBucket].join(" ")).join(" ");
      return normalizeKey(`${thread.title} ${thread.text} ${thread.type} ${related}`).includes(query);
    });
}

export function renderThreads(state) {
  const rows = applyFilters(state.derived.threads, state);

  return `
    <div class="list-column schedule-screen schedule-screen--threads">
      ${renderGlobalToggles(state.filters)}
      ${renderSearch("threads", state.search.threads, "Search horse, rider, ring, class, status")}
      ${renderPeakFilters("threads", state.filters.threads, TYPES)}
      <div class="schedule-card-stack">
        ${rows.length ? rows.map((thread) => threadCard(thread, "threads")).join("") : emptyCard("No thread items found for the current filters.")}
      </div>
    </div>
  `;
}
