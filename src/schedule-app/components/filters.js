import { escapeHtml } from "../utils.js";

function toggleButton(kind, value, label, activeValue) {
  const active = activeValue === value;
  return `
    <button class="chip ${active ? "is-active" : ""}" type="button" data-toggle="${escapeHtml(kind)}" data-value="${escapeHtml(value)}" aria-pressed="${active ? "true" : "false"}">
      ${escapeHtml(label)}
    </button>
  `;
}

export function renderGlobalToggles(filters) {
  return `
    <div class="schedule-toggle-row" aria-label="Global filters">
      <div class="chip-strip schedule-chip-strip">
        ${toggleButton("scope", "active", "ACTIVE", filters.global.scope)}
        ${toggleButton("scope", "full", "FULL", filters.global.scope)}
      </div>
      <div class="chip-strip schedule-chip-strip">
        ${toggleButton("status", "live", "LIVE", filters.global.status)}
        ${toggleButton("status", "all", "ALL", filters.global.status)}
      </div>
    </div>
  `;
}

export function renderSearch(screen, value, placeholder) {
  return `
    <input
      class="schedule-search"
      type="search"
      value="${escapeHtml(value || "")}"
      placeholder="${escapeHtml(placeholder)}"
      data-search-screen="${escapeHtml(screen)}"
      autocomplete="off"
    />
  `;
}

export function renderPeakFilters(screen, activeValue, filters) {
  const chips = filters.map((filter) => `
    <button class="chip ${activeValue === filter.value ? "is-active" : ""}" type="button" data-filter-screen="${escapeHtml(screen)}" data-filter-value="${escapeHtml(filter.value)}" aria-pressed="${activeValue === filter.value ? "true" : "false"}">
      ${escapeHtml(filter.label)}
    </button>
  `).join("");

  return `<div class="chip-strip schedule-peak-filters">${chips}</div>`;
}
