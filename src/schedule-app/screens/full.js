import { scheduleCard } from "../components/card.js";
import { emptyCard } from "../components/empty.js";
import { renderGlobalToggles, renderPeakFilters, renderSearch } from "../components/filters.js";
import { escapeHtml, normalizeKey } from "../utils.js";

function applyFilters(rows, state) {
  const global = state.filters.global;
  const screenFilter = state.filters.full;
  const query = normalizeKey(state.search.full);

  return rows
    .filter((row) => global.scope === "full" || row.statusBucket !== "completed" || row.hasFollowedTrips)
    .filter((row) => global.status === "all" || row.statusBucket === "live")
    .filter((row) => {
      if (screenFilter === "all") return true;
      if (screenFilter.startsWith("ring:")) return row.ringKey === screenFilter.slice(5);
      if (screenFilter.startsWith("group:")) return row.groupKey === screenFilter.slice(6);
      return true;
    })
    .filter((row) => {
      if (!query) return true;
      return normalizeKey([row.ringLabel, row.groupLabel, row.classLabel, row.classNumber].join(" ")).includes(query);
    });
}

function groupedByRing(rows) {
  const groups = new Map();
  for (const row of rows) {
    if (!groups.has(row.ringKey)) groups.set(row.ringKey, { label: row.ringLabel, rows: [] });
    groups.get(row.ringKey).rows.push(row);
  }
  return [...groups.values()];
}

export function renderFull(state) {
  const rows = applyFilters(state.derived.scheduleRows, state);
  const peakFilters = [
    { value: "all", label: "All" },
    ...state.derived.rings.slice(0, 10).map((ring) => ({ value: `ring:${ring.key}`, label: ring.label })),
  ];

  const groups = groupedByRing(rows);

  return `
    <div class="list-column schedule-screen schedule-screen--full">
      ${renderGlobalToggles(state.filters)}
      ${renderSearch("full", state.search.full, "Search ring, group, class")}
      ${renderPeakFilters("full", state.filters.full, peakFilters)}
      ${groups.length ? groups.map((group) => `
        <section class="schedule-group-block">
          <div class="section-title">${escapeHtml(group.label)}</div>
          ${group.rows.map((row) => scheduleCard(row, "full")).join("")}
        </section>
      `).join("") : emptyCard("No schedule rows found.")}
    </div>
  `;
}
