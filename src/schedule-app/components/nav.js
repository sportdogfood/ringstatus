import { escapeHtml } from "../utils.js";

const NAV = [
  { screen: "start", label: "Start" },
  { screen: "summary", label: "Summary" },
  { screen: "lite", label: "Lite" },
  { screen: "full", label: "Full" },
  { screen: "threads", label: "Threads" },
];

export function renderNav(activeScreen, counts = {}) {
  const buttons = NAV.map((item) => {
    const active = activeScreen === item.screen;
    const count = item.screen === "start" ? "" : ` ${counts[item.screen] ?? 0}`;
    return `
      <button class="nav-btn ${active ? "is-active" : ""}" type="button" data-screen="${escapeHtml(item.screen)}" aria-pressed="${active ? "true" : "false"}">
        ${escapeHtml(item.label)}${escapeHtml(count)}
      </button>
    `;
  }).join("");

  return `<div class="nav-strip">${buttons}</div>`;
}
