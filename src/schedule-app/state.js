export const SCREENS = ["start", "summary", "lite", "full", "threads"];

const STORAGE_KEY = "ringstatus.schedule.prototype.state.v1";

export const state = {
  screen: "start",
  loaded: { schedule: false, trips: false },
  loading: true,
  error: null,
  raw: { schedule: [], trips: [] },
  meta: {
    scheduleSource: null,
    tripsSource: null,
    lastFetchedAt: null,
    lastGeneratedAt: null,
    usedCache: false,
  },
  derived: null,
  filters: {
    global: { scope: "active", status: "all" },
    lite: "all",
    full: "all",
    threads: "all",
  },
  search: {
    lite: "",
    full: "",
    threads: "",
  },
  detail: null,
};

export function hydrateState() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
    if (SCREENS.includes(saved.screen)) state.screen = saved.screen;
    state.filters = {
      ...state.filters,
      ...(saved.filters || {}),
      global: { ...state.filters.global, ...(saved.filters?.global || {}) },
    };
    state.search = { ...state.search, ...(saved.search || {}) };
  } catch (error) {
    console.warn("schedule_state_hydrate_failed", error);
  }
}

export function persistState() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      screen: state.screen,
      filters: state.filters,
      search: state.search,
    }));
  } catch (error) {
    console.warn("schedule_state_persist_failed", error);
  }
}

export function setScreen(screen) {
  if (!SCREENS.includes(screen)) return;
  state.screen = screen;
  state.detail = null;
  persistState();
}

export function setGlobalFilter(kind, value) {
  if (!["scope", "status"].includes(kind)) return;
  state.filters.global[kind] = value;
  persistState();
}

export function setScreenFilter(screen, value) {
  if (!["lite", "full", "threads"].includes(screen)) return;
  state.filters[screen] = value || "all";
  persistState();
}

export function setSearch(screen, value) {
  if (!["lite", "full", "threads"].includes(screen)) return;
  state.search[screen] = value || "";
  persistState();
}

export function setDetail(detail) {
  state.detail = detail;
}
