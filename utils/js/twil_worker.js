// ringstatus-sms — worker.js (FULL DROP)
//
// PURPOSE
// - Parse inbound Twilio SMS
// - Map to exactly one ring or return guidance
// - Use payload clock to determine DAY/NIGHT
// - Fetch target ring payload directly from upstream
// - Build front-facing SMS response
// - POST a structured payload to Airtable webhook
// - Return only TwiML to Twilio
//
// REQUIRED env:
// - SGL_TOKEN
// - SGL_COOKIE
//
// OPTIONAL env:
// - CUSTOMER_ID (default 15)
// - SHOW_ID
// - SHOW_DATE_OVERRIDE
// - DEBUG_SMS="1"
// - UNKNOWN_REPLY

const EMPTY_RING_ID_FOR_CLOCK = 51;
const DAY_END_MIN = 17 * 60; // 5:00 PM hard switch

const AIRTABLE_WEBHOOK =
  "https://hooks.airtable.com/workflows/v1/genericWebhook/apptdhhNzduxm5gjn/wfleDEPvPZQjrHJ8E/wtrTvF3XlVXnMSMbT";

const UNMAPPED_REPLY =
  `We could not process your ring #. Try "Ring 10" or "Grand". If you are still having trouble or need support, email win@ringstatus.com.`;

const MULTI_RING_REPLY =
  `We could not process your ring #. Please send one ring at a time, like "Ring 9" or "Ring 10". If you are still having trouble or need support, email win@ringstatus.com.`;

export default {
  async fetch(request, env) {
    if (request.method !== "POST") return twimlMessage("");

    const raw = await request.text();
    const form = new URLSearchParams(raw);

    const inboundBody = (form.get("Body") || "").trim();
    const twowayId = (form.get("MessageSid") || "").trim();
    const phone = (form.get("From") || "").trim();

    const UNKNOWN =
      (env.UNKNOWN_REPLY || "").trim() ||
      UNMAPPED_REPLY;

    const debug = String(env.DEBUG_SMS || "").trim() === "1";
    const dbg = [];
    const addDbg = (s) => debug && dbg.push(String(s));
    const withDbg = (txt) => (!debug ? txt : `${txt}\nDBG ${dbg.join(" | ")}`.slice(0, 1400));

    try {
      addDbg(`body="${inboundBody}"`);

      const ringDecision = decideRingStrict(inboundBody);

      if (ringDecision.status === "none") {
        const msg = UNMAPPED_REPLY;
        await postToAirtable({
          twoway_id: twowayId,
          body: inboundBody,
          phone,
          title: msg,
          first: "",
          next: "",
        });
        return twimlMessage(withDbg(msg));
      }

      if (ringDecision.status === "multiple") {
        const msg = MULTI_RING_REPLY;
        await postToAirtable({
          twoway_id: twowayId,
          body: inboundBody,
          phone,
          title: msg,
          first: "",
          next: "",
        });
        return twimlMessage(withDbg(msg));
      }

      const decision = ringDecision.ring;
      addDbg(`ring#${decision.ring_number}->id${decision.ring_id}`);

      const upstreamBase = "https://sglapi.wellingtoninternational.com";
      const commonHeaders = buildUpstreamHeaders(env);

      // 1) CLOCK PING
      const clockUrl =
        `${upstreamBase}/ring/${encodeURIComponent(EMPTY_RING_ID_FOR_CLOCK)}` +
        `?customer_id=${encodeURIComponent(String(env.CUSTOMER_ID || "15").trim())}` +
        (String(env.SHOW_ID || "").trim()
          ? `&show_id=${encodeURIComponent(String(env.SHOW_ID).trim())}`
          : "");

      addDbg(`clockUrl=${clockUrl}`);

      const clockResp = await fetchTextWithTimeout(clockUrl, 6500, commonHeaders);
      addDbg(`clockStatus=${clockResp.status}`);

      const clockPayload = tryParseJson(clockResp.text);
      addDbg(`clockJson=${clockPayload ? "yes" : "no"}`);

      const clockTz = clockPayload?.time_zone_date_time || null;
      const clockSqlDate =
        String(env.SHOW_DATE_OVERRIDE || "").trim() ||
        String(clockTz?.sql_date || "").trim() ||
        todayInTimeZone("America/New_York");

      const clockTimeStr = String(clockTz?.time || "").trim();
      const clockTimeMin = parseTimeToMinutes(clockTimeStr);
      const mode = clockTimeMin != null && clockTimeMin >= DAY_END_MIN ? "NIGHT" : "DAY";

      let targetShowDate = extractISODate(inboundBody) || clockSqlDate;
      if (!extractISODate(inboundBody) && !String(env.SHOW_DATE_OVERRIDE || "").trim()) {
        if (mode === "NIGHT") targetShowDate = addDaysSql(clockSqlDate, 1) || clockSqlDate;
      }

      addDbg(`clockSqlDate=${clockSqlDate}`);
      addDbg(`clockTime=${clockTimeStr || "?"}`);
      addDbg(`mode=${mode}`);
      addDbg(`targetShowDate=${targetShowDate}`);

      // 2) TARGET FETCH
      const targetUrl =
        `${upstreamBase}/ring/${encodeURIComponent(decision.ring_id)}` +
        `?show_date=${encodeURIComponent(targetShowDate)}` +
        `&date=${encodeURIComponent(targetShowDate)}` +
        `&customer_id=${encodeURIComponent(String(env.CUSTOMER_ID || "15").trim())}` +
        (String(env.SHOW_ID || "").trim()
          ? `&show_id=${encodeURIComponent(String(env.SHOW_ID).trim())}`
          : "");

      addDbg(`targetUrl=${targetUrl}`);

      const targetResp = await fetchTextWithTimeout(targetUrl, 6500, commonHeaders);
      addDbg(`targetStatus=${targetResp.status}`);

      const payload = tryParseJson(targetResp.text);
      addDbg(`targetJson=${payload ? "yes" : "no"}`);

      const ringLabel = `Ring ${decision.ring_number}`;

      if (!payload || typeof payload !== "object") {
        const title = `${ringLabel} — as of ${nowShort("America/New_York")}`;
        const msg = `${title}\nunavailable right now. Try again shortly.`;

        await postToAirtable({
          twoway_id: twowayId,
          body: inboundBody,
          phone,
          title,
          first: "unavailable right now. Try again shortly.",
          next: "",
        });

        return twimlMessage(withDbg(msg));
      }

      const ringName = String(payload?.ring?.ring_name || ringLabel).trim() || ringLabel;
      const tz = payload?.time_zone_date_time || {};
      const asOf = formatAsOfFromPayloadTz(tz) || nowShort("America/New_York");
      const title = `${ringName} — as of ${asOf}`;

      const groupsRaw = Array.isArray(payload?.class_groups) ? payload.class_groups : [];
      const groups = groupsRaw
        .filter((g) => Number(g?.cancelled ?? 0) === 0)
        .sort((a, b) => Number(a?.group_sequence ?? 0) - Number(b?.group_sequence ?? 0));

      addDbg(`groups=${groups.length}`);

      if (!groups.length) {
        const first = "no ring data posted yet.";

        await postToAirtable({
          twoway_id: twowayId,
          body: inboundBody,
          phone,
          title,
          first,
          next: "",
        });

        return twimlMessage(withDbg(`${title}\n${first}`));
      }

      const anchorClock = String(tz?.time || "").trim() || null;
      const anchorMs = parseAnyClockToMsSameDay(anchorClock);
      const runningIndex = groups.findIndex((g) => isGroupInProgress(g, anchorMs));

      addDbg(`anchorClock=${anchorClock || "?"}`);
      addDbg(`runningIndex=${runningIndex}`);

      let built;

      if (runningIndex >= 0 && mode === "DAY") {
        built = buildInProgressReply({
          groups,
          runningIndex,
          ringName,
          asOf,
          anchorMs,
          debugAdd: addDbg,
        });
      } else {
        built = buildPreStartReply({
          groups,
          ringName,
          asOf,
          anchorMs,
        });
      }

      await postToAirtable({
        twoway_id: twowayId,
        body: inboundBody,
        phone,
        title: built.title,
        first: built.first,
        next: built.next,
      });

      return twimlMessage(withDbg(built.fullText));
    } catch {
      const msg = UNKNOWN;
      return twimlMessage(withDbg(msg));
    }
  },
};

function buildUpstreamHeaders(env) {
  return {
    Accept: "application/json",
    "User-Agent": "Mozilla/5.0",
    Authorization: `Bearer ${env.SGL_TOKEN}`,
    "sgl-request-origin": "SGL-API",
    Cookie: env.SGL_COOKIE || "",
    Origin: "https://www.wellingtoninternational.com",
    Referer: "https://www.wellingtoninternational.com/",
  };
}

function twimlMessage(message) {
  const safe = xmlEscape(String(message || ""));
  return new Response(
    `<?xml version="1.0" encoding="UTF-8"?><Response>${safe ? `<Message>${safe}</Message>` : ""}</Response>`,
    { status: 200, headers: { "Content-Type": "text/xml; charset=utf-8" } }
  );
}

function xmlEscape(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

async function fetchTextWithTimeout(url, ms, headers) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    const resp = await fetch(url, {
      signal: ac.signal,
      headers,
      cf: { cacheTtl: 0, cacheEverything: false },
    });
    const text = await resp.text().catch(() => "");
    return { ok: resp.ok, status: resp.status, text };
  } catch (e) {
    const isAbort = String(e?.name || "").toLowerCase().includes("abort");
    return { ok: false, status: 0, text: isAbort ? "timeout" : String(e?.message || e) };
  } finally {
    clearTimeout(t);
  }
}

function tryParseJson(text) {
  const s = String(text || "").trim();
  if (!s) return null;
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

async function postToAirtable(payload) {
  try {
    await fetch(AIRTABLE_WEBHOOK, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        twoway_id: String(payload.twoway_id || ""),
        body: String(payload.body || ""),
        phone: String(payload.phone || ""),
        title: String(payload.title || ""),
        first: String(payload.first || ""),
        next: String(payload.next || ""),
      }),
    });
  } catch {}
}

function nowShort(tz) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  })
    .format(new Date())
    .replace(" AM", "A")
    .replace(" PM", "P");
}

function todayInTimeZone(tz) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());

  const y = parts.find((p) => p.type === "year")?.value || "0000";
  const m = parts.find((p) => p.type === "month")?.value || "01";
  const d = parts.find((p) => p.type === "day")?.value || "01";
  return `${y}-${m}-${d}`;
}

function parseTimeToMinutes(t) {
  const ms = parseAnyClockToMsSameDay(t);
  return ms == null ? null : Math.floor(ms / 60000);
}

function addDaysSql(sqlDate, days) {
  const d = new Date(`${sqlDate}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function formatAsOfFromPayloadTz(tz) {
  const time = String(tz?.time || "").trim();
  return formatDisplayTime(time) || "";
}

const RINGS = [
  { ring_number: 1, ring_id: 51, aliases: ["intl", "international", "wellington international"] },
  { ring_number: 2, ring_id: 9, aliases: ["grand", "mische"] },
  { ring_number: 3, ring_id: 37, aliases: ["rost"] },
  { ring_number: 4, ring_id: 52, aliases: ["mogavero"] },
  { ring_number: 5, ring_id: 49, aliases: ["denemethy"] },
  { ring_number: 6, ring_id: 25, aliases: ["ring 6", "r6", "6"] },
  { ring_number: 7, ring_id: 56, aliases: ["ring 7", "r7", "7"] },
  { ring_number: 8, ring_id: 57, aliases: ["ring 8", "r8", "8"] },
  { ring_number: 9, ring_id: 58, aliases: ["ring 9", "r9", "9"] },
  { ring_number: 10, ring_id: 53, aliases: ["ring 10", "r10", "10"] },
  { ring_number: 11, ring_id: 22, aliases: ["ring 11", "r11", "11"] },
  { ring_number: 12, ring_id: 30, aliases: ["ring 12", "r12", "12"] },
  { ring_number: 13, ring_id: 38, aliases: ["south", "south ring", "pista sur"] },
  { ring_number: 14, ring_id: 2, aliases: ["30 ring", "ring 30", "pista 30"] },
  { ring_number: 15, ring_id: 10, aliases: ["derby", "derby field", "ring 15", "r15", "15"] },
];

function decideRingStrict(input) {
  const norm = normalize(input);
  const matches = [];

  for (const r of RINGS) {
    for (const a of r.aliases) {
      const an = normalize(a);
      if (an && norm.includes(an)) {
        matches.push(r);
        break;
      }
    }
  }

  const digitMatches = extractAllRingDigits(norm);
  for (const n of digitMatches) {
    const found = RINGS.find((r) => r.ring_number === n);
    if (found) matches.push(found);
  }

  const unique = dedupeRings(matches);

  if (unique.length === 0) return { status: "none", ring: null };
  if (unique.length > 1) return { status: "multiple", ring: null };
  return { status: "one", ring: unique[0] };
}

function dedupeRings(arr) {
  const seen = new Set();
  const out = [];
  for (const r of arr) {
    const k = `${r.ring_number}:${r.ring_id}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  return out;
}

function extractAllRingDigits(norm) {
  const out = [];
  const rx = /(?:^|\b)(?:ring|r)\s*(\d{1,2})(?:\b|$)|(?:^|\b)(\d{1,2})(?:\b|$)/gi;
  let m;
  while ((m = rx.exec(String(norm || ""))) !== null) {
    const raw = m[1] || m[2];
    const n = Number(raw);
    if (Number.isFinite(n) && n >= 1 && n <= 15) out.push(n);
  }
  return [...new Set(out)];
}

function extractISODate(s) {
  const m = String(s || "").match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  return m ? m[1] : null;
}

function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isGroupInProgress(g, anchorMs) {
  const total = Number(g?.total_trips ?? 0);
  const gone = Number(g?.completed_trips ?? 0);
  const act = String(g?.actual_start_time || "").trim();
  const actMs = parseAnyClockToMsSameDay(act);

  if (!act || act === "00:00:00") return false;
  if (!(gone > 0 && gone < total)) return false;

  if (anchorMs == null || actMs == null) return true;
  return actMs <= anchorMs;
}

function buildPreStartReply({ groups, ringName, asOf, anchorMs }) {
  const firstGroup = groups[0] || null;
  const nextGroup = groups[1] || null;
  const title = `${ringName} — as of ${asOf}`;

  let first = "";
  let next = "";

  if (firstGroup) {
    const total = Number(firstGroup?.total_trips ?? 0);
    const gone = Number(firstGroup?.completed_trips ?? 0);
    const left = Math.max(0, total - gone);
    const start = formatDisplayTime(pickDisplayStartTime(firstGroup));
    const endsMin = deriveMinutesToEndFromAnchor(firstGroup, anchorMs);
    const endsTxt = formatDurationMinutes(endsMin);

    first =
      `First: ${cleanName(firstGroup?.group_name || "Unknown class")}.\n` +
      `${start ? `Start: ${start} | ` : ""}Trips: ${total} | Gone: ${gone} | Left: ${left}` +
      (endsTxt ? ` | Ends: ${endsTxt}` : "");
  }

  if (nextGroup) {
    const total = Number(nextGroup?.total_trips ?? 0);
    const gone = Number(nextGroup?.completed_trips ?? 0);
    const left = Math.max(0, total - gone);
    const start = formatDisplayTime(pickDisplayStartTime(nextGroup));
    const tillMin = deriveMinutesToStartFromAnchor(nextGroup, anchorMs);
    const tillTxt = formatDurationMinutes(tillMin);

    next =
      `Next: ${cleanName(nextGroup?.group_name || "Unknown class")}.\n` +
      `${start ? `Start: ${start} | ` : ""}Trips: ${total} | Gone: ${gone} | Left: ${left}` +
      (tillTxt ? ` | Till: ${tillTxt}` : "");
  }

  const fullText = [title, first, next].filter(Boolean).join("\n\n");
  return { title, first, next, fullText };
}

function buildInProgressReply({ groups, runningIndex, ringName, asOf, anchorMs, debugAdd }) {
  const cur = groups[runningIndex];
  const nextGroup =
    groups.slice(runningIndex + 1).find((g) => Number(g?.total_trips ?? 0) > Number(g?.completed_trips ?? 0)) || null;

  const curTotal = Number(cur?.total_trips ?? 0);
  const curGone = Number(cur?.completed_trips ?? 0);
  const curLeft = Math.max(0, curTotal - curGone);

  const perTripSchedMs = derivePerTripSchedMs(cur);
  const perTripLiveMs = derivePerTripLiveMs(cur, anchorMs);

  if (debugAdd) {
    debugAdd(`schedSec=${perTripSchedMs ? Math.round(perTripSchedMs / 1000) : "x"}`);
    debugAdd(`liveSec=${perTripLiveMs ? Math.round(perTripLiveMs / 1000) : "x"}`);
  }

  const chosenCurMs = choosePerTripMs(perTripLiveMs, perTripSchedMs);
  const endsMin = chosenCurMs ? Math.max(0, Math.round((curLeft * chosenCurMs) / 60000)) : null;
  const tillMin = nextGroup
    ? deriveMinutesToStartFromSchedule(cur, nextGroup, anchorMs, chosenCurMs)
    : null;

  const endsTxt = formatDurationMinutes(endsMin);
  const tillTxt = formatDurationMinutes(tillMin);
  const curStart = formatDisplayTime(pickDisplayStartTime(cur));
  const title = `${ringName} — as of ${asOf}`;

  const first =
    `Now: ${cleanName(cur?.group_name || "Current class")}.\n` +
    `${curStart ? `Start: ${curStart} | ` : ""}Trips: ${curTotal} | Gone: ${curGone} | Left: ${curLeft}` +
    (endsTxt ? ` | Ends: ${endsTxt}` : "");

  let next = "";
  if (nextGroup) {
    const nextTotal = Number(nextGroup?.total_trips ?? 0);
    const nextGone = Number(nextGroup?.completed_trips ?? 0);
    const nextLeft = Math.max(0, nextTotal - nextGone);
    const nextStart = formatDisplayTime(pickDisplayStartTime(nextGroup));

    next =
      `Next: ${cleanName(nextGroup?.group_name || "Next class")}.\n` +
      `${nextStart ? `Start: ${nextStart} | ` : ""}Trips: ${nextTotal} | Gone: ${nextGone} | Left: ${nextLeft}` +
      (tillTxt ? ` | Till: ${tillTxt}` : "");
  }

  const fullText = [title, first, next].filter(Boolean).join("\n\n");
  return { title, first, next, fullText };
}

function cleanName(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function pickDisplayStartTime(group) {
  const actual = String(group?.actual_start_time || "").trim();
  if (actual && actual !== "00:00:00") return actual;

  const estimated = String(group?.estimated_start_time || "").trim();
  if (estimated && estimated !== "00:00:00") return estimated;

  const fallback = String(group?.start_time_default || "").trim();
  if (fallback && fallback !== "00:00:00") return fallback;

  return "";
}

function formatDisplayTime(value) {
  const ms = parseAnyClockToMsSameDay(value);
  if (ms == null) return "";

  const totalSeconds = Math.floor(ms / 1000);
  let hh = Math.floor(totalSeconds / 3600);
  const mm = Math.floor((totalSeconds % 3600) / 60);

  const ap = hh >= 12 ? "P" : "A";
  hh = hh % 12;
  if (hh === 0) hh = 12;

  return `${hh}:${String(mm).padStart(2, "0")}${ap}`;
}

function formatDurationMinutes(mins) {
  if (mins == null || !Number.isFinite(mins)) return "";
  const whole = Math.max(0, Math.round(mins));

  if (whole < 60) return `${whole}m`;

  const hrs = Math.floor(whole / 60);
  const rem = whole % 60;

  if (rem === 0) return `${hrs}hr`;
  return `${hrs}hr ${rem}m`;
}

function derivePerTripSchedMs(group) {
  const estStart = parseAnyClockToMsSameDay(group?.estimated_start_time);
  const estEnd = parseAnyClockToMsSameDay(group?.estimated_end_time);
  const total = Number(group?.total_trips ?? 0);

  if (estStart == null || estEnd == null || total <= 0) return null;

  const diff = estEnd - estStart;
  if (!(diff > 0)) return null;

  const ms = diff / total;
  return ms > 150000 ? ms : null;
}

function derivePerTripLiveMs(group, anchorMs) {
  const actStart = parseAnyClockToMsSameDay(group?.actual_start_time);
  const gone = Number(group?.completed_trips ?? 0);

  if (actStart == null || anchorMs == null || gone <= 0) return null;

  const diff = anchorMs - actStart;
  if (!(diff > 0)) return null;

  const ms = diff / gone;
  return ms > 150000 ? ms : null;
}

function choosePerTripMs(...vals) {
  for (const v of vals) {
    if (typeof v === "number" && Number.isFinite(v) && v > 0) return v;
  }
  return null;
}

function deriveMinutesToEndFromAnchor(group, anchorMs) {
  const endMs = parseAnyClockToMsSameDay(group?.estimated_end_time);
  if (endMs == null || anchorMs == null) return null;
  return Math.max(0, Math.round((endMs - anchorMs) / 60000));
}

function deriveMinutesToStartFromAnchor(group, anchorMs) {
  const startMs = parseAnyClockToMsSameDay(pickDisplayStartTime(group));
  if (startMs == null || anchorMs == null) return null;
  return Math.max(0, Math.round((startMs - anchorMs) / 60000));
}

function deriveMinutesToStartFromSchedule(cur, nextGroup, anchorMs, currentPerTripMs) {
  if (!nextGroup) return null;

  const nextStartMs = parseAnyClockToMsSameDay(pickDisplayStartTime(nextGroup));
  if (nextStartMs != null && anchorMs != null) {
    return Math.max(0, Math.round((nextStartMs - anchorMs) / 60000));
  }

  if (currentPerTripMs != null) {
    const curTotal = Number(cur?.total_trips ?? 0);
    const curGone = Number(cur?.completed_trips ?? 0);
    const curLeft = Math.max(0, curTotal - curGone);
    return Math.max(0, Math.round((curLeft * currentPerTripMs) / 60000));
  }

  return null;
}

function parseAnyClockToMsSameDay(value) {
  const s = String(value || "").trim().toUpperCase();
  if (!s) return null;

  let m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const hh = Number(m[1]);
    const mm = Number(m[2]);
    const ss = Number(m[3] || 0);
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59 && ss >= 0 && ss <= 59) {
      return ((hh * 60 + mm) * 60 + ss) * 1000;
    }
  }

  m = s.match(/^(\d{1,2}):(\d{2})\s*([AP])M?$/);
  if (m) {
    let hh = Number(m[1]);
    const mm = Number(m[2]);
    const ap = m[3];

    if (!(hh >= 1 && hh <= 12 && mm >= 0 && mm <= 59)) return null;

    if (hh === 12) hh = 0;
    if (ap === "P") hh += 12;

    return ((hh * 60 + mm) * 60) * 1000;
  }

  return null;
}
