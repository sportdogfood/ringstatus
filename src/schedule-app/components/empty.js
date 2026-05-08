import { escapeHtml } from "../utils.js";

export function emptyCard(message, tone = "empty") {
  return `
    <div class="schedule-card schedule-card--${escapeHtml(tone)}" data-card-role="${escapeHtml(tone)}-card">
      <div class="schedule-card__title">${escapeHtml(message)}</div>
    </div>
  `;
}

export function loadingCard() {
  return emptyCard("Loading schedule data...", "loading");
}

export function errorCard(message) {
  return emptyCard(message || "Could not refresh data. Showing last known data if available.", "error");
}
