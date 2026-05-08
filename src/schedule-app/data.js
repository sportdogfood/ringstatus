const CACHE_KEY = "ringstatus.schedule.prototype.lastGood.v1";

const SCHEDULE_URL = "/schedule/data/latest/watch_schedule.json";
const TRIPS_URL = "/schedule/data/latest/watch_trips.json";

function rowsFrom(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.rows)) return payload.rows;
  if (Array.isArray(payload?.schedule)) return payload.schedule;
  if (Array.isArray(payload?.trips)) return payload.trips;
  if (Array.isArray(payload?.records)) return payload.records;
  return [];
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`${url} ${response.status}`);
  return response.json();
}

function readCache() {
  try {
    return JSON.parse(localStorage.getItem(CACHE_KEY) || "null");
  } catch (error) {
    console.warn("schedule_cache_read_failed", error);
    return null;
  }
}

function writeCache(payload) {
  try {
    localStorage.setItem(CACHE_KEY, JSON.stringify(payload));
  } catch (error) {
    console.warn("schedule_cache_write_failed", error);
  }
}

export async function loadScheduleData() {
  const fetchedAt = new Date().toISOString();

  try {
    const [schedulePayload, tripsPayload] = await Promise.all([
      fetchJson(SCHEDULE_URL),
      fetchJson(TRIPS_URL),
    ]);

    const scheduleRows = rowsFrom(schedulePayload);
    const tripRows = rowsFrom(tripsPayload);
    const data = {
      schedule: scheduleRows,
      trips: tripRows,
      meta: {
        scheduleSource: schedulePayload?.source || SCHEDULE_URL,
        tripsSource: tripsPayload?.source || TRIPS_URL,
        lastFetchedAt: fetchedAt,
        lastGeneratedAt: schedulePayload?.generated_at || tripsPayload?.generated_at || fetchedAt,
        usedCache: false,
      },
    };

    if (scheduleRows.length || tripRows.length) writeCache(data);
    return { ok: true, ...data };
  } catch (error) {
    console.warn("schedule_data_fetch_failed", error);
    const cached = readCache();
    if (cached?.schedule || cached?.trips) {
      return {
        ok: false,
        schedule: cached.schedule || [],
        trips: cached.trips || [],
        meta: {
          ...(cached.meta || {}),
          lastFetchedAt: fetchedAt,
          usedCache: true,
        },
        error: "Could not refresh data. Showing last known data if available.",
      };
    }

    return {
      ok: false,
      schedule: [],
      trips: [],
      meta: { lastFetchedAt: fetchedAt, usedCache: false },
      error: "Could not refresh data. Showing last known data if available.",
    };
  }
}
