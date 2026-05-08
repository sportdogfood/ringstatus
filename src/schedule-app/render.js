import { renderFlyup } from "./components/flyup.js";
import { renderNav } from "./components/nav.js";
import { loadingCard } from "./components/empty.js";
import { renderFull } from "./screens/full.js";
import { renderLite } from "./screens/lite.js";
import { renderStart } from "./screens/start.js";
import { renderSummary } from "./screens/summary.js";
import { renderThreads } from "./screens/threads.js";
import { state, setDetail, setGlobalFilter, setScreen, setScreenFilter, setSearch } from "./state.js";

const titles = {
  start: "RingStatus",
  summary: "Summary",
  lite: "Lite",
  full: "Full",
  threads: "Threads",
};

function screenHtml() {
  if (state.loading || !state.derived) {
    return `<div class="list-column">${loadingCard()}</div>`;
  }

  switch (state.screen) {
    case "summary":
      return renderSummary(state);
    case "lite":
      return renderLite(state);
    case "full":
      return renderFull(state);
    case "threads":
      return renderThreads(state);
    case "start":
    default:
      return renderStart(state);
  }
}

function aggregatePayload(id) {
  const derived = state.derived;
  const stats = derived.summaryStats;
  const map = {
    "agg:scheduleRows": { label: "Schedule rows", value: stats.totalScheduleRows, hint: "Full schedule scaffold", rows: derived.scheduleRows },
    "agg:tripRows": { label: "Active trips", value: stats.totalTripRows, hint: "Followed trip overlay", rows: derived.tripRows },
    "agg:rings": { label: "Rings", value: stats.ringCount, hint: "Rings in the map", rows: derived.rings },
    "agg:live": { label: "Live", value: stats.liveCount, hint: "Live schedule rows", rows: derived.scheduleRows.filter((row) => row.statusBucket === "live") },
    "agg:upcoming": { label: "Upcoming", value: stats.upcomingCount, hint: "Upcoming schedule rows", rows: derived.scheduleRows.filter((row) => row.statusBucket === "upcoming") },
    "agg:completed": { label: "Completed", value: stats.completedCount, hint: "Completed schedule rows", rows: derived.scheduleRows.filter((row) => row.statusBucket === "completed") },
  };
  return map[id] || { label: "Aggregate", value: 0, hint: "No matching rows", rows: [] };
}

function resolveDetail(type, id, source) {
  const derived = state.derived;
  if (!derived) return null;

  let payload = null;
  let relatedRows = [];

  if (type === "aggregate-detail") {
    payload = aggregatePayload(id);
    relatedRows = payload.rows || [];
  } else if (type === "ring-detail") {
    payload = derived.rings.find((ring) => ring.id === id || ring.key === id);
    relatedRows = payload ? [...payload.scheduleRows, ...payload.tripRows] : [];
  } else if (type === "group-detail") {
    payload = derived.groups.find((group) => group.id === id || group.key === id);
    relatedRows = payload ? [...payload.scheduleRows, ...payload.tripRows] : [];
  } else if (type === "class-detail") {
    payload = derived.scheduleRows.find((row) => row.id === id) || derived.classes.find((klass) => klass.id === id || klass.key === id);
    relatedRows = payload?.scheduleRows ? [...payload.scheduleRows, ...payload.tripRows] : [payload, ...(payload?.matchingTrips || [])].filter(Boolean);
  } else if (type === "trip-detail") {
    payload = derived.tripRows.find((trip) => trip.id === id);
    relatedRows = [payload?.matchingScheduleRow, payload].filter(Boolean);
  } else if (type === "horse-detail") {
    payload = derived.horses.find((horse) => horse.id === id || horse.key === id);
    relatedRows = payload?.tripRows || [];
  } else if (type === "rider-detail") {
    payload = derived.riders.find((rider) => rider.id === id || rider.key === id);
    relatedRows = payload?.tripRows || [];
  } else if (type === "thread-detail") {
    payload = derived.threads.find((thread) => thread.id === id);
    relatedRows = payload?.relatedRows || [];
  }

  if (!payload) return null;
  return {
    type,
    id,
    source,
    payload,
    relatedRows,
    openedAt: new Date().toISOString(),
  };
}

export function renderApp() {
  const main = document.getElementById("rs-app-main");
  const nav = document.getElementById("rs-app-nav");
  const flyup = document.getElementById("rs-flyup-root");
  const title = document.getElementById("rs-header-title");
  const back = document.getElementById("rs-header-back");
  const action = document.getElementById("rs-header-action");
  const focusedSearch = document.activeElement?.dataset?.searchScreen;
  const selectionStart = document.activeElement?.selectionStart ?? null;

  if (!main || !nav || !flyup || !title || !back || !action) return;

  title.textContent = titles[state.screen] || "RingStatus";
  back.classList.toggle("is-invisible", state.screen === "start");
  action.classList.add("is-invisible");

  main.innerHTML = screenHtml();
  nav.innerHTML = renderNav(state.screen, state.derived?.summaryStats?.navCounts || {});
  flyup.innerHTML = renderFlyup(state.detail);

  if (focusedSearch) {
    const input = document.querySelector(`[data-search-screen="${focusedSearch}"]`);
    if (input) {
      input.focus();
      if (selectionStart !== null) input.setSelectionRange(selectionStart, selectionStart);
    }
  }
}

export function bindEvents() {
  document.addEventListener("click", (event) => {
    const target = event.target;
    const overlay = target.closest(".schedule-flyup-layer");
    const close = target.closest('[data-action="close-detail"]');
    if (close) {
      if (overlay && target.closest("[data-stop-close]") && close === overlay) return;
      setDetail(null);
      renderApp();
      return;
    }

    const start = target.closest('[data-action="start-session"]');
    if (start) {
      setScreen("summary");
      renderApp();
      return;
    }

    const back = target.closest("#rs-header-back");
    if (back) {
      setScreen("start");
      renderApp();
      return;
    }

    const nav = target.closest("[data-screen]");
    if (nav) {
      setScreen(nav.dataset.screen);
      renderApp();
      return;
    }

    const toggle = target.closest("[data-toggle]");
    if (toggle) {
      setGlobalFilter(toggle.dataset.toggle, toggle.dataset.value);
      renderApp();
      return;
    }

    const peak = target.closest("[data-filter-screen]");
    if (peak) {
      setScreenFilter(peak.dataset.filterScreen, peak.dataset.filterValue);
      renderApp();
      return;
    }

    const detail = target.closest("[data-detail-type]");
    if (detail) {
      const nextDetail = resolveDetail(detail.dataset.detailType, detail.dataset.detailId, detail.dataset.detailSource);
      if (nextDetail) {
        setDetail(nextDetail);
        renderApp();
      }
    }
  });

  document.addEventListener("input", (event) => {
    const input = event.target.closest("[data-search-screen]");
    if (!input) return;
    setSearch(input.dataset.searchScreen, input.value);
    renderApp();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && state.detail) {
      setDetail(null);
      renderApp();
    }
  });
}
