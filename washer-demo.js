/**
 * washer-demo.js
 * - Runs for LOOP_SECONDS, ticks every TICK_SECONDS.
 * - Each tick:
 *    1) get server_now_epoch (SHOWTIME_URL -> epoch_ms) or Date.now()
 *    2) list Airtable records from a bucket view (HOT)
 *    3) identify "due + unlocked"
 *    4) log what would churn; optionally lock + reschedule if DO_WRITE=1
 *
 * Airtable "list records" supports filtering by view via `?view=...` :contentReference[oaicite:2]{index=2}
 */

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const BASE_ID          = process.env.AIRTABLE_BASE_ID || "";
const TABLE            = process.env.AIRTABLE_TABLE || "";
const VIEW_HOT         = process.env.AIRTABLE_VIEW_HOT || "";
const SHOWTIME_URL     = process.env.SHOWTIME_URL || "";

const FIELD_NEXT_DUE   = process.env.FIELD_NEXT_DUE || "next_due_epoch";
const FIELD_LOCK_UNTIL = process.env.FIELD_LOCK_UNTIL || "lock_until_epoch";
const FIELD_BUCKET     = process.env.FIELD_BUCKET || "bucket";

const DO_WRITE         = (process.env.DO_WRITE || "0") === "1";
const LOOP_SECONDS     = Number(process.env.LOOP_SECONDS || "300");
const TICK_SECONDS     = Number(process.env.TICK_SECONDS || "5");

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function pickEpochMsFromJson(j) {
  if (j == null) return null;
  if (typeof j === "number") return j;
  const keys = ["epoch_ms", "server_epoch_ms", "now_ms", "time_ms", "server_now_ms"];
  for (const k of keys) {
    if (typeof j[k] === "number") return j[k];
    if (typeof j[k] === "string" && /^\d+$/.test(j[k])) return Number(j[k]);
  }
  return null;
}

async function getServerNowMs() {
  if (!SHOWTIME_URL) return Date.now();
  try {
    const res = await fetch(SHOWTIME_URL, { method: "GET" });
    const txt = await res.text();
    // handle JSON or raw number
    try {
      const j = JSON.parse(txt);
      const ms = pickEpochMsFromJson(j);
      return ms ?? Date.now();
    } catch {
      if (/^\d+$/.test(txt.trim())) return Number(txt.trim());
      return Date.now();
    }
  } catch {
    return Date.now();
  }
}

async function airtableListByView(viewName) {
  if (!AIRTABLE_TOKEN || !BASE_ID || !TABLE || !viewName) return [];
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}`);
    url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);

    const res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` }
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Airtable list failed (${res.status}): ${body}`);
    }

    const json = await res.json();
    out.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }

  return out;
}

async function airtablePatch(recordId, fields) {
  const url = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ fields })
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Airtable patch failed (${res.status}): ${body}`);
  }
  return res.json();
}

function n(v) {
  const x = Number(v);
  return Number.isFinite(x) ? x : null;
}

(async () => {
  const start = Date.now();
  const endAt = start + LOOP_SECONDS * 1000;
  let tick = 0;

  console.log(`washer-demo start | loop=${LOOP_SECONDS}s tick=${TICK_SECONDS}s do_write=${DO_WRITE}`);

  while (Date.now() < endAt) {
    tick++;

    const nowMs = await getServerNowMs();
    const nowEpoch = Math.floor(nowMs / 1000);

    let records = [];
    try {
      records = await airtableListByView(VIEW_HOT);
    } catch (e) {
      console.log(`[t${tick}] now=${nowEpoch} | Airtable read error: ${e.message}`);
      await sleep(TICK_SECONDS * 1000);
      continue;
    }

    const due = [];
    for (const r of records) {
      const f = r.fields || {};
      const nextDue = n(f[FIELD_NEXT_DUE]);
      const lockUntil = n(f[FIELD_LOCK_UNTIL]) ?? 0;

      if (nextDue != null && nextDue <= nowEpoch && lockUntil <= nowEpoch) {
        due.push({ id: r.id, nextDue, lockUntil, bucket: f[FIELD_BUCKET] });
      }
    }

    console.log(
      `[t${tick}] now=${nowEpoch} | hot=${records.length} due=${due.length}`
    );

    // Demonstrate churn behavior
    for (const d of due.slice(0, 10)) {
      console.log(`  CHURN -> ${d.id} bucket=${d.bucket ?? "?"} next_due=${d.nextDue} lock_until=${d.lockUntil}`);

      if (DO_WRITE) {
        const newLockUntil = nowEpoch + 60;     // lock 60s
        const newNextDue   = nowEpoch + 20;     // demo: reschedule soon so you can watch repeats
        await airtablePatch(d.id, {
          [FIELD_LOCK_UNTIL]: newLockUntil,
          [FIELD_NEXT_DUE]: newNextDue
        });
      }
    }

    await sleep(TICK_SECONDS * 1000);
  }

  console.log(`washer-demo end`);
})();
