import { loadScheduleData } from "./data.js";
import { deriveAppModel } from "./derive.js";
import { bindEvents, renderApp } from "./render.js";
import { hydrateState, state } from "./state.js";

async function boot() {
  hydrateState();
  bindEvents();
  renderApp();

  const result = await loadScheduleData();
  state.raw.schedule = result.schedule || [];
  state.raw.trips = result.trips || [];
  state.loaded.schedule = state.raw.schedule.length > 0;
  state.loaded.trips = state.raw.trips.length > 0;
  state.meta = { ...state.meta, ...(result.meta || {}) };
  state.error = result.error || null;
  state.derived = deriveAppModel(state.raw.schedule, state.raw.trips, state.meta);
  state.loading = false;
  renderApp();
}

boot().catch((error) => {
  console.error("schedule_app_boot_failed", error);
  state.loading = false;
  state.error = "Could not refresh data. Showing last known data if available.";
  state.derived = deriveAppModel([], [], state.meta);
  renderApp();
});
