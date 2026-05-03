// ringstatus-sms — worker.js (FULL DROP)
//
// PURPOSE
// - Parse inbound Twilio SMS
// - Expeditor: try strict ring recognition first
// - If exactly one ring is recognized, run the existing ring lane unchanged
// - If no ring is recognized, try rider/horse lookup through ringstatus-proxy
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
// - WATCH_TRIPS_URL
// - RIDER_LOOKUP_URL
// - HORSE_LOOKUP_URL

const DAY_END_MIN = 17 * 60; // 5:00 PM hard switch
const WORKER_VERSION = "ringstatus-sms-six-rings-2026-04-28";
const RING_REPLY_EXAMPLES = `"Derby" or "Hunter 1"`;

const AIRTABLE_WEBHOOK =
  "https://hooks.airtable.com/workflows/v1/genericWebhook/apptdhhNzduxm5gjn/wfleDEPvPZQjrHJ8E/wtrTvF3XlVXnMSMbT";

const DEFAULT_WATCH_TRIPS_URL =
  "https://ringstatus-proxy.gombcg.workers.dev/docs/9999/schedules/trips.json";

const DEFAULT_RIDER_LOOKUP_URL =
  "https://ringstatus-proxy.gombcg.workers.dev/rider";

const DEFAULT_HORSE_LOOKUP_URL =
  "https://ringstatus-proxy.gombcg.workers.dev/horse";

const FALLBACK_WATCH_TRIPS_URL =
  "https://raw.githubusercontent.com/sportdogfood/ringstatus-data/main/docs/9999/schedules/trips.json";

const UNMAPPED_REPLY =
  `We could not process your ring #. Try ${RING_REPLY_EXAMPLES}. If you are still having trouble or need support, email win@ringstatus.com.`;

const MULTI_RING_REPLY =
  `We could not process your ring #. Please send one ring at a time, like ${RING_REPLY_EXAMPLES}. If you are still having trouble or need support, email win@ringstatus.com.`;

const RIDER_LOOKUP_UNAVAILABLE_REPLY =
  `We found the rider lookup feed, but it does not include rider names right now. Try a ring like ${RING_REPLY_EXAMPLES}, or try the rider again shortly.`;

const RIDER_LOOKUP_ERROR_REPLY =
  `Rider lookup is temporarily unavailable. Try the rider again shortly, or send a ring like ${RING_REPLY_EXAMPLES}.`;

const SHOW_RIDER_LOOKUP_FAILURE_DETAIL = true;

export default {
  async fetch(request, env) {
    if (request.method === "GET") return healthResponse(request, env);
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
      logWorkerEvent("sms_inbound", {
        version: WORKER_VERSION,
        body: inboundBody,
        from_last4: phone.slice(-4),
      });

      addDbg(`version=${WORKER_VERSION}`);
      addDbg(`body="${inboundBody}"`);

      const ringDecision = decideRingStrict(inboundBody);
      logWorkerEvent("sms_ring_decision", {
        status: ringDecision.status,
        ring_number: ringDecision.ring?.ring_number ?? null,
        ring_id: ringDecision.ring?.ring_id ?? null,
      });

      if (ringDecision.status === "none") {
        addDbg(`lane=rider-fallback`);

        const riderResult = await tryRiderLane({
          inboundBody,
          twowayId,
          phone,
          env,
          addDbg,
          withDbg,
        });

        if (riderResult) return riderResult;

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
      addDbg(`lane=ring`);
      addDbg(`ring#${decision.ring_number}->id${decision.ring_id}`);

      const upstreamBase = "https://sglapi.wellingtoninternational.com";
      const commonHeaders = buildUpstreamHeaders(env);

      // 1) CLOCK PING
      const clockUrl =
        `${upstreamBase}/ring/${encodeURIComponent(decision.ring_id)}` +
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

      const ringLabel = decision.sms_label || `Ring ${decision.ring_number}`;

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

      const ringName = String(ringLabel || payload?.ring?.ring_name || "").trim() || `Ring ${decision.ring_number}`;
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
    } catch (e) {
      logWorkerEvent("sms_error", {
        version: WORKER_VERSION,
        message: String(e?.message || e).slice(0, 240),
      });
      addDbg(`error=${String(e?.message || e).slice(0, 160)}`);
      const msg = UNKNOWN;
      return twimlMessage(withDbg(msg));
    }
  },
};

async function tryRiderLane({ inboundBody, twowayId, phone, env, addDbg, withDbg }) {
  const intent = parseLookupIntent(inboundBody);
  if (!intent.shouldLookup) return null;

  addDbg(`lookupQuery="${intent.query}"`);
  addDbg(`lookupLane=${intent.lane || "auto"}`);
  addDbg(`lookupMode=${intent.mode || "next"}`);
  if (intent.classQuery) addDbg(`lookupClass=${intent.classQuery}`);

  const lanes = intent.lane ? [intent.lane] : ["rider", "horse"];
  let proxyFailed = false;
  let failedLookupUrl = "";
  let failedLookupReason = "";

  for (const lane of lanes) {
    const baseUrl = lane === "horse"
      ? String(env.HORSE_LOOKUP_URL || "").trim() || DEFAULT_HORSE_LOOKUP_URL
      : String(env.RIDER_LOOKUP_URL || "").trim() || DEFAULT_RIDER_LOOKUP_URL;
    const lookupUrl = buildLookupUrl(baseUrl, intent);

    addDbg(`${lane}LookupUrl=${lookupUrl}`);
    logWorkerEvent("lookup_proxy_start", {
      version: WORKER_VERSION,
      lane,
      query: intent.query,
      mode: intent.mode,
      class_query: intent.classQuery,
      lookup_url: lookupUrl,
    });

    const resp = await fetchTextWithTimeout(
      lookupUrl,
      6500,
      {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0",
      }
    );

    addDbg(`${lane}LookupStatus=${resp.status}`);
    addDbg(`${lane}LookupLen=${String(resp.text || "").length}`);

    if (!resp.ok) {
      proxyFailed = true;
      failedLookupUrl = lookupUrl;
      failedLookupReason = `${lane} lookup status ${resp.status}`;
      logWorkerEvent("lookup_proxy_fetch", {
        lane,
        status: resp.status,
        ok: resp.ok,
        body_head: debugPreview(resp.text),
      });
      continue;
    }

    const payload = tryParseJson(resp.text);
    addDbg(`${lane}LookupJson=${payload ? "yes" : "no"}`);
    addDbg(`${lane}LookupShape=${describeJsonShape(payload)}`);

    if (!payload || payload.ok !== true) {
      proxyFailed = true;
      failedLookupUrl = lookupUrl;
      failedLookupReason = payload ? `${lane} lookup ok=false` : `${lane} lookup bad JSON`;
      logWorkerEvent("lookup_proxy_json", {
        lane,
        parsed: Boolean(payload),
        ok: payload?.ok ?? null,
        body_head: payload ? "" : debugPreview(resp.text),
      });
      continue;
    }

    const matches = Number(payload.matches ?? 0);
    const recordCount = Array.isArray(payload.records) ? payload.records.length : 0;
    addDbg(`${lane}LookupMatches=${matches}`);

    if (!payload.selected && recordCount <= 0) continue;
    if (matches <= 0 && recordCount <= 0) continue;

    const built = buildLookupReply(payload, lane, intent.query);

    await postToAirtable({
      twoway_id: twowayId,
      body: inboundBody,
      phone,
      title: built.title,
      first: built.first,
      next: built.next,
    });

    logWorkerEvent("lookup_proxy_match", {
      lane,
      matches,
      selected_id: payload.selected?.id || "",
      reply_label: payload.selected?.replyLabel || "",
      trip_category: payload.selected?.tripCategory || "",
      record_count: recordCount,
    });

    return twimlMessage(withDbg(built.fullText));
  }

  if (proxyFailed) {
    addDbg(`lookupProxyFallback=watch-trips`);
    const legacyResult = await tryLegacyRiderLane({
      inboundBody,
      lookupBody: intent.query,
      twowayId,
      phone,
      env,
      addDbg,
      withDbg,
    });

    if (legacyResult) return legacyResult;

    return postAndReturnMessage({
      twowayId,
      inboundBody,
      phone,
      msg: buildRiderLookupFailureReply(failedLookupReason || "proxy lookup failed", failedLookupUrl),
      withDbg,
    });
  }

  const msg = `We could not find a current trip for "${cleanName(intent.query)}". Try the rider or horse name exactly as listed, or send a ring like ${RING_REPLY_EXAMPLES}.`;
  return postAndReturnMessage({
    twowayId,
    inboundBody,
    phone,
    msg,
    withDbg,
  });
}

async function tryLegacyRiderLane({ inboundBody, lookupBody, twowayId, phone, env, addDbg, withDbg }) {
  const bodyForLookup = cleanName(lookupBody || inboundBody);
  const needle = normalize(bodyForLookup);
  if (!needle) return null;

  const isRiderQuery = looksLikeRiderQuery(bodyForLookup);
  addDbg(`riderNeedle=${needle}`);
  addDbg(`isRiderQuery=${isRiderQuery ? "yes" : "no"}`);

  const watchTripsUrl = String(env.WATCH_TRIPS_URL || "").trim() || DEFAULT_WATCH_TRIPS_URL;
  let watchTripsFetchUrl = addCacheBust(watchTripsUrl);
  addDbg(`watchTripsUrl=${watchTripsUrl}`);
  addDbg(`watchTripsFetchUrl=${watchTripsFetchUrl}`);
  logWorkerEvent("rider_lookup_start", {
    version: WORKER_VERSION,
    needle,
    is_rider_query: isRiderQuery,
    watch_trips_url: watchTripsUrl,
    watch_trips_fetch_url: watchTripsFetchUrl,
  });

  let resp = await fetchTextWithTimeout(
    watchTripsFetchUrl,
    6500,
    {
      Accept: "application/json",
      "User-Agent": "Mozilla/5.0",
    }
  );

  addDbg(`watchTripsStatus=${resp.status}`);
  addDbg(`watchTripsLen=${String(resp.text || "").length}`);
  logWorkerEvent("rider_lookup_fetch", {
    url: watchTripsFetchUrl,
    status: resp.status,
    ok: resp.ok,
    body_len: String(resp.text || "").length,
    body_head: !resp.ok ? debugPreview(resp.text) : "",
  });

  let failedFetchUrl = watchTripsFetchUrl;
  let failedFetchReason = `feed status ${resp.status}`;

  if (!resp.ok && normalizeUrlForCompare(watchTripsUrl) !== normalizeUrlForCompare(FALLBACK_WATCH_TRIPS_URL)) {
    const fallbackUrl = addCacheBust(FALLBACK_WATCH_TRIPS_URL);
    addDbg(`watchTripsFallbackUrl=${fallbackUrl}`);
    logWorkerEvent("rider_lookup_fallback_start", {
      from_url: watchTripsFetchUrl,
      fallback_url: fallbackUrl,
      previous_status: resp.status,
    });

    const fallbackResp = await fetchTextWithTimeout(
      fallbackUrl,
      6500,
      {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0",
      }
    );

    logWorkerEvent("rider_lookup_fallback_fetch", {
      url: fallbackUrl,
      status: fallbackResp.status,
      ok: fallbackResp.ok,
      body_len: String(fallbackResp.text || "").length,
      body_head: !fallbackResp.ok ? debugPreview(fallbackResp.text) : "",
    });

    if (fallbackResp.ok) {
      watchTripsFetchUrl = fallbackUrl;
      resp = fallbackResp;
      addDbg(`watchTripsFallbackStatus=${resp.status}`);
    } else {
      addDbg(`watchTripsFallbackStatus=${fallbackResp.status}`);
      failedFetchUrl = `${watchTripsFetchUrl}; fallback ${fallbackUrl}`;
      failedFetchReason = `feed status ${resp.status}; fallback status ${fallbackResp.status}`;
    }
  }

  if (!resp.ok) {
    addDbg(`riderLookupFail=feed-status-${resp.status}`);
    addDbg(`watchTripsHead=${debugPreview(resp.text)}`);
    if (!isRiderQuery) return null;
    return postAndReturnMessage({
      twowayId,
      inboundBody,
      phone,
      msg: buildRiderLookupFailureReply(failedFetchReason, failedFetchUrl),
      withDbg,
    });
  }

  const payload = tryParseJson(resp.text);
  addDbg(`watchTripsJson=${payload ? "yes" : "no"}`);
  addDbg(`watchTripsShape=${describeJsonShape(payload)}`);
  logWorkerEvent("rider_lookup_json", {
    parsed: Boolean(payload),
    shape: describeJsonShape(payload),
    body_head: payload ? "" : debugPreview(resp.text),
  });

  if (!payload) {
    addDbg(`riderLookupFail=bad-json`);
    addDbg(`watchTripsHead=${debugPreview(resp.text)}`);
    if (!isRiderQuery) return null;
    return postAndReturnMessage({
      twowayId,
      inboundBody,
      phone,
      msg: buildRiderLookupFailureReply("bad JSON", watchTripsFetchUrl),
      withDbg,
    });
  }

  const records = Array.isArray(payload) ? payload : Array.isArray(payload?.records) ? payload.records : [];
  addDbg(`watchTripsRecords=${records.length}`);

  if (records[0] && typeof records[0] === "object") {
    addDbg(`recordKeys=${Object.keys(records[0]).slice(0, 10).join(",")}`);
  }

  if (!records.length) {
    if (!isRiderQuery) return null;
    return postAndReturnMessage({
      twowayId,
      inboundBody,
      phone,
      msg: RIDER_LOOKUP_UNAVAILABLE_REPLY,
      withDbg,
    });
  }

  const riderNameValues = records.flatMap((r) => getRiderSearchValues(r));
  addDbg(`riderNameValues=${riderNameValues.length}`);
  addDbg(`sampleRiders=${sampleValues(riderNameValues, 5).join(",") || "none"}`);

  const matches = records.filter((r) =>
    getRiderSearchValues(r).some((name) => normalize(name) === needle)
  );
  addDbg(`riderMatches=${matches.length}`);
  logWorkerEvent("rider_lookup_match", {
    records: records.length,
    rider_name_values: riderNameValues.length,
    sample_riders: sampleValues(riderNameValues, 5),
    matches: matches.length,
  });

  if (!matches.length) {
    if (!isRiderQuery) return null;

    const msg = riderNameValues.length
      ? `We could not find a current trip for "${cleanName(bodyForLookup)}". Try the rider name exactly as listed, or send a ring like ${RING_REPLY_EXAMPLES}.`
      : RIDER_LOOKUP_UNAVAILABLE_REPLY;

    return postAndReturnMessage({
      twowayId,
      inboundBody,
      phone,
      msg,
      withDbg,
    });
  }

  const picked = pickBestRiderRecord(matches);
  const built = buildLegacyRiderReply(picked);

  await postToAirtable({
    twoway_id: twowayId,
    body: inboundBody,
    phone,
    title: built.title,
    first: built.first,
    next: built.next,
  });

  return twimlMessage(withDbg(built.fullText));
}

async function postAndReturnMessage({ twowayId, inboundBody, phone, msg, withDbg }) {
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

function healthResponse(request, env) {
  const url = new URL(request.url);
  const watchTripsUrl = String(env.WATCH_TRIPS_URL || "").trim() || DEFAULT_WATCH_TRIPS_URL;
  const riderLookupUrl = String(env.RIDER_LOOKUP_URL || "").trim() || DEFAULT_RIDER_LOOKUP_URL;
  const horseLookupUrl = String(env.HORSE_LOOKUP_URL || "").trim() || DEFAULT_HORSE_LOOKUP_URL;
  const body = {
    ok: true,
    service: "ringstatus-sms",
    version: WORKER_VERSION,
    path: url.pathname,
    rider_lookup_url: riderLookupUrl,
    horse_lookup_url: horseLookupUrl,
    watch_trips_url: watchTripsUrl,
    fallback_watch_trips_url: FALLBACK_WATCH_TRIPS_URL,
  };

  return new Response(JSON.stringify(body, null, 2), {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function buildRiderLookupFailureReply(reason, url) {
  if (!SHOW_RIDER_LOOKUP_FAILURE_DETAIL) return RIDER_LOOKUP_ERROR_REPLY;

  const safeReason = cleanName(reason || "unknown");
  const safeUrl = String(url || "").trim();
  return `${RIDER_LOOKUP_ERROR_REPLY} Diagnostic: ${safeReason}${safeUrl ? ` at ${safeUrl}` : ""}.`;
}

function parseLookupIntent(input) {
  const raw = cleanName(input);
  const norm = normalize(raw);
  const empty = { shouldLookup: false, lane: "", query: "", mode: "next", classQuery: "" };
  if (!norm) return empty;
  if (/\b(ring|today|tomorrow|yesterday|help|status|schedule)\b/.test(norm)) {
    return empty;
  }

  const parts = raw.split(/\s+/).filter(Boolean);
  let lane = "";
  let query = raw;
  const first = normalize(parts[0] || "");

  if (["horse", "h"].includes(first)) {
    lane = "horse";
    query = parts.slice(1).join(" ");
  } else if (["rider", "r"].includes(first)) {
    lane = "rider";
    query = parts.slice(1).join(" ");
  }

  let mode = "next";
  let classQuery = "";
  const queryParts = cleanName(query).split(/\s+/).filter(Boolean);
  if (queryParts.length > 1) {
    const last = normalize(queryParts[queryParts.length - 1]);
    if (/^\d{1,4}$/.test(last)) {
      mode = "class";
      classQuery = last;
      queryParts.pop();
    } else if (["all", "al"].includes(last)) {
      mode = "all";
      queryParts.pop();
    } else if (["next", "ne"].includes(last)) {
      mode = "next";
      queryParts.pop();
    } else if (["result", "results", "res", "re"].includes(last)) {
      mode = "result";
      queryParts.pop();
    }
  }

  query = cleanName(queryParts.join(" "));
  const queryNorm = normalize(query);
  if (!queryNorm || /^\d+$/.test(queryNorm)) return empty;
  if (!/^[a-z0-9]+(?: [a-z0-9]+){0,5}$/.test(queryNorm)) return empty;

  const tokens = queryNorm.split(" ");
  const last = tokens[tokens.length - 1] || "";
  if (tokens.length === 1 && ["all", "al", "next", "ne", "result", "results", "res", "re"].includes(last)) {
    return empty;
  }

  return { shouldLookup: true, lane, query, mode, classQuery };
}

function buildLookupUrl(baseUrl, intent) {
  const input = String(baseUrl || "").trim();
  const query = String(intent?.query || "").trim();
  const mode = String(intent?.mode || "next").trim();
  const classQuery = String(intent?.classQuery || "").trim();
  try {
    const u = new URL(input);
    u.searchParams.set("q", query);
    if (mode && mode !== "next") u.searchParams.set("mode", mode);
    if (classQuery) u.searchParams.set("class", classQuery);
    if (mode === "all") u.searchParams.set("limit", "10");
    u.searchParams.set("_sms_ts", String(Date.now()));
    return u.toString();
  } catch {
    const params = new URLSearchParams({ q: query, _sms_ts: String(Date.now()) });
    if (mode && mode !== "next") params.set("mode", mode);
    if (classQuery) params.set("class", classQuery);
    if (mode === "all") params.set("limit", "10");
    const sep = input.includes("?") ? "&" : "?";
    return `${input}${sep}${params.toString()}`;
  }
}

function buildLookupReply(payload, lane, fallbackName) {
  if (payload?.query?.mode === "all") return buildLookupAllReply(payload, lane, fallbackName);

  const record = payload?.selected || {};
  const subject = cleanName(
    lane === "horse"
      ? firstText(record?.horseName, fallbackName, "Horse")
      : firstText(record?.riderName, fallbackName, "Rider")
  );
  const title = `${subject} - as of ${nowShort("America/New_York")}`;
  const { first, next } = buildLookupRecordParts(record, lane, false);

  const fullText = [title, first, next].filter(Boolean).join("\n");
  return { title, first, next, fullText };
}

function buildLookupAllReply(payload, lane, fallbackName) {
  const records = Array.isArray(payload?.records) && payload.records.length
    ? payload.records
    : payload?.selected
      ? [payload.selected]
      : [];
  const firstRecord = records[0] || payload?.selected || {};
  const subject = cleanName(
    lane === "horse"
      ? firstText(firstRecord?.horseName, fallbackName, "Horse")
      : firstText(firstRecord?.riderName, fallbackName, "Rider")
  );
  const title = `${subject} - as of ${nowShort("America/New_York")}`;
  const currentRecord = records.find((record) => isUnderwayRecord(record)) || null;
  const nextRecord = records.find((record) =>
    record !== currentRecord &&
    !isClassCompletedRecord(record) &&
    !isUnderwayRecord(record)
  ) || null;
  const sections = [];

  if (currentRecord) {
    const { first, next } = buildLookupRecordParts(currentRecord, lane, {
      label: "Now",
      supplemental: "go-when-gone-in",
    });
    sections.push([first, next].filter(Boolean).join("\n"));
  }

  if (nextRecord) {
    const { first, next } = buildLookupRecordParts(nextRecord, lane, {
      label: "Next",
      supplemental: "go-when-gone-in",
    });
    sections.push([first, next].filter(Boolean).join("\n"));
  }

  if (!sections.length && records[0]) {
    const { first, next } = buildLookupRecordParts(records[0], lane, {
      label: isClassCompletedRecord(records[0]) ? "Completed" : "Next",
      supplemental: "go-when-gone-in",
    });
    sections.push([first, next].filter(Boolean).join("\n"));
  }

  const first = sections[0] || "No current classes found.";
  const next = sections.slice(1).join("\n\n");
  const fullText = [title, ...sections].filter(Boolean).join("\n\n");

  return { title, first, next, fullText };
}

function buildLookupRecordParts(record, lane, options) {
  const opts = typeof options === "object" && options !== null
    ? options
    : { compact: Boolean(options) };
  const label = firstText(opts.label) || lookupReplyLabel(record, opts.compact ? "Class" : "Next");
  const className = cleanName(firstText(record?.className, record?.class_name, record?.group_name, "Class"));
  const classNumber = firstText(record?.classNumber, record?.class_number);
  const statsParts = buildClassStatsParts(record);
  const contextParts = buildLookupContextParts(record, lane);
  const supplemental = buildLookupSupplementalLine(record, opts.supplemental || "go-and-results");

  const first =
    `${label}: ${className}${classNumber ? ` (${classNumber})` : ""}` +
    (contextParts.length ? `\n${contextParts.join(" | ")}` : "") +
    (statsParts.length ? `\n${statsParts.join(" | ")}` : "");

  const next = supplemental;

  return { first, next };
}

function buildClassStatsParts(record) {
  const total = numberOrNull(record?.totalTrips, record?.total_trips);
  const gone = numberOrNull(record?.completedTrips, record?.completed_trips);
  const left = total == null || gone == null ? null : Math.max(0, total - gone);
  const minutesTillStart = numberOrNull(record?.minutesTillStart, record?.rs_mins_till_start);
  const minutesToEnd = numberOrNull(record?.minutesToEnd, record?.rs_mins_till_end);
  const tillTxt = formatLookupMinutes(minutesTillStart, record?.tripCategory);
  const endsTxt = formatLookupMinutes(minutesToEnd, record?.tripCategory);
  const start = formatDisplayTime(firstText(record?.startTimeDisplay, record?.startTime, record?.latestStart));
  const parts = [];
  const completed = isClassCompletedRecord(record);
  const underway = isUnderwayRecord(record);

  if (completed) return parts;

  if (start) parts.push(`Start: ${start}`);
  if (total != null) parts.push(`Trips: ${total}`);
  if (gone != null) parts.push(`Gone: ${gone}`);
  if (left != null) parts.push(`Left: ${left}`);
  if (!completed && underway && endsTxt) parts.push(`Ends: ${endsTxt}`);
  if (!completed && !underway && tillTxt) parts.push(`Till: ${tillTxt}`);

  return parts;
}

function buildLookupContextParts(record, lane) {
  const contextParts = [];
  if (record?.ringName) contextParts.push(cleanName(record.ringName));
  if (lane === "rider" && record?.horseName) contextParts.push(cleanName(record.horseName));
  if (lane === "horse" && record?.riderName) contextParts.push(cleanName(record.riderName));
  return contextParts;
}

function buildLookupSupplementalLine(record, mode) {
  const goLine = buildGoParts(record).join(" | ");
  const resultLine = buildResultParts(record).join(" | ");

  if (mode === "go-when-gone-in") {
    return isGoneInRecord(record) ? goLine : resultLine;
  }

  if (isClassCompletedRecord(record)) return resultLine;
  return [goLine, resultLine].filter(Boolean).join("\n");
}

function buildGoParts(record) {
  const order = firstText(record?.rs_order_of_go, record?.orderOfGo, record?.runningOrderOfGo, record?.rs_running_order_of_go);
  const go = formatDisplayTime(firstText(record?.rs_go_time, record?.goTimeDisplay, record?.goTime, record?.latestGO, record?.estimatedGO));
  const minutesToGo = numberOrNull(record?.rs_min_till_go, record?.minutesToGo);
  const minutesTxt = formatLookupMinutes(minutesToGo, record?.tripCategory);
  const parts = [];

  if (go) parts.push(`Go time: ${go}`);
  if (order) parts.push(`Order: ${order}`);
  if (minutesTxt) parts.push(`Till Go: ${minutesTxt}`);

  return parts;
}

function buildResultParts(record) {
  const resultParts = [];
  const time = resultTimeText(record);
  const score = resultScoreText(record);
  const place = resultPlaceText(record);

  if (score) resultParts.push(`Last Score: ${score}`);
  if (time) resultParts.push(`Time: ${time}`);
  if (place) resultParts.push(`Place: ${place}`);
  return resultParts;
}

function lookupReplyLabel(record, fallback) {
  const explicit = cleanName(firstText(record?.replyLabel));
  const explicitNorm = normalize(explicit);

  if (isUnderwayRecord(record)) return "Now";
  if (isClassCompletedRecord(record)) return "Completed";
  if (explicitNorm === "completed" || explicitNorm === "now") return fallback;
  return cleanName(firstText(explicit, fallback));
}

function hasDisplayResult(record) {
  return Boolean(record?.hasResult || resultTimeText(record) || resultScoreText(record) || resultPlaceText(record));
}

function resultTimeText(record) {
  return firstText(
    record?.lastTime,
    record?.last_time,
    record?.time_one,
    record?.time1,
    record?.time_1,
    record?.time,
    record?.time_two,
    record?.time2,
    record?.time_2
  );
}

function resultScoreText(record) {
  return firstText(
    record?.lastScore,
    record?.last_score,
    record?.score,
    record?.score1,
    record?.score_one,
    record?.score_1,
    record?.score2,
    record?.score_2,
    record?.score3,
    record?.score_3
  );
}

function resultPlaceText(record) {
  const place = firstText(
    record?.lastPlacing,
    record?.last_placing,
    record?.lastPlace,
    record?.last_place,
    record?.latestPlacing,
    record?.latest_placing,
    record?.placing,
    record?.place
  );
  const n = Number(place);

  if (Number.isFinite(n) && (n <= 0 || n >= 99999)) return "";
  return place;
}

function isClassCompletedRecord(record) {
  const status = normalize(firstText(record?.statusCategory, record?.status, record?.latestStatus, record?.rs_status));
  return status === "completed";
}

function isUnderwayRecord(record) {
  const status = normalize(firstText(record?.statusCategory, record?.tripCategory, record?.status, record?.latestStatus, record?.rs_status));
  return status === "underway" || status === "in progress";
}

function isGoneInRecord(record) {
  return shouldDisplayFlag(firstText(record?.goneIn, record?.gone_in, record?.rs_gone_in));
}

function shouldDisplayFlag(value) {
  const v = normalize(value);
  return Boolean(v && !["0", "false", "no", "n"].includes(v));
}

function numberOrNull(...values) {
  for (const value of values) {
    if (value == null || value === "") continue;
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function formatLookupMinutes(minutes, tripCategory) {
  if (minutes == null || !Number.isFinite(minutes)) return "";
  if (minutes < 0 && String(tripCategory || "") === "Completed") return "";
  return formatDurationMinutes(Math.max(0, minutes));
}

function getRiderSearchValues(record) {
  return [
    record?.riderName,
    record?.rider_name,
    record?.["Rider Name"],
    record?.teamName,
    record?.team_name,
  ].filter((v) => String(v || "").trim());
}

function looksLikeRiderQuery(input) {
  const norm = normalize(input);
  if (!norm || /\d/.test(norm)) return false;
  if (/\b(ring|today|tomorrow|yesterday|help|status|schedule)\b/.test(norm)) return false;
  return /^[a-z]+(?: [a-z]+){0,3}$/.test(norm);
}

function pickBestRiderRecord(records) {
  const statusRank = (s) => {
    const v = normalize(s);
    if (v === "not started" || v === "upcoming") return 0;
    if (v === "underway" || v === "in progress") return 1;
    if (v === "completed") return 2;
    return 9;
  };

  const toNum = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 999999;
  };

  return [...records].sort((a, b) => {
    const sr = statusRank(a?.latestStatus) - statusRank(b?.latestStatus);
    if (sr !== 0) return sr;

    const tr = toNum(a?.time_sort) - toNum(b?.time_sort);
    if (tr !== 0) return tr;

    const gr = parseAnyClockToMsSameDay(a?.latestGO) ?? 999999999;
    const hr = parseAnyClockToMsSameDay(b?.latestGO) ?? 999999999;
    if (gr !== hr) return gr - hr;

    return 0;
  })[0];
}

function buildLegacyRiderReply(record) {
  const rider = cleanName(firstText(record?.riderName, record?.rider_name, record?.teamName, "Rider"));
  const ringText = formatRiderRingText(record);
  const horse = cleanName(firstText(record?.horseName, record?.horse_name, record?.barnName, record?.barn_name));
  const className = cleanName(firstText(record?.className, record?.class_name, record?.groupName, record?.group_name, "Class"));
  const classNumber = firstText(record?.classNumber, record?.class_number);
  const label = legacyReplyLabel(record);
  const title = `${rider} - as of ${nowShort("America/New_York")}`;
  const completed = isClassCompletedRecord(record);

  const total = numberOrNull(record?.totalTrips, record?.total_trips);
  const gone = numberOrNull(record?.completedTrips, record?.completed_trips);
  const left = total == null || gone == null ? null : Math.max(0, total - gone);
  const minutesTillStart = numberOrNull(record?.minutesTillStart, record?.rs_mins_till_start);
  const classLengthMinutes = numberOrNull(record?.classLengthMinutes, record?.rs_length);
  const minutesToEnd =
    numberOrNull(record?.minutesToEnd, record?.rs_mins_till_end) ??
    (minutesTillStart != null && classLengthMinutes != null ? minutesTillStart + classLengthMinutes : null);
  const endsTxt = formatLookupMinutes(minutesToEnd, label === "Completed" ? "Completed" : "");
  const start = formatDisplayTime(firstText(
    record?.startTimeDisplay,
    record?.latestStart,
    record?.startTime,
    record?.estimated_start_time
  ));

  const firstParts = [];
  if (!completed) {
    if (start) firstParts.push(`Start: ${start}`);
    if (total != null) firstParts.push(`Trips: ${total}`);
    if (gone != null) firstParts.push(`Gone: ${gone}`);
    if (left != null) firstParts.push(`Left: ${left}`);
    if (endsTxt) firstParts.push(`Ends: ${endsTxt}`);
  }

  const contextParts = [];
  if (ringText) contextParts.push(ringText);
  if (horse) contextParts.push(horse);
  if (!completed && isGoneInRecord(record)) contextParts.push("Gone in: Y");

  const first =
    `${label}: ${className}${classNumber ? ` (${classNumber})` : ""}` +
    (contextParts.length ? `\n${contextParts.join(" | ")}` : "") +
    (firstParts.length ? `\n${firstParts.join(" | ")}` : "");

  const go = formatDisplayTime(firstText(
    record?.goTimeDisplay,
    record?.latestGO,
    record?.goTime,
    record?.estimatedGO
  ));
  const order = firstText(
    record?.orderOfGo,
    record?.rs_order_of_go,
    record?.runningOrderOfGo,
    record?.rs_running_order_of_go
  );
  const goneIn = firstText(record?.goneIn, record?.gone_in, record?.rs_gone_in);
  const minutes = numberOrNull(record?.minutesToGo, record?.rs_min_till_go, record?.ringWalk);
  const minutesTxt = formatLookupMinutes(minutes, label === "Completed" ? "Completed" : "");
  const goneFlag = shouldDisplayFlag(goneIn);

  const goParts = [];
  if (!isClassCompletedRecord(record)) {
    if (goneFlag) {
      // Gone-in is displayed on the context line for rider lookups.
    } else {
      if (order && go) goParts.push(`Go Time: ${go}`);
      if (order) goParts.push(`Order: ${order}`);
      if (order && minutesTxt) goParts.push(`Till Go: ${minutesTxt}`);
    }
  }

  const resultParts = buildResultParts(record);

  const next = [
    goParts.join(" | "),
    resultParts.length ? `Last: ${resultParts.join(" | ")}` : "",
  ].filter(Boolean).join("\n");

  const fullText = [title, first, next].filter(Boolean).join("\n");
  return { title, first, next, fullText };
}

function legacyReplyLabel(record) {
  if (isUnderwayRecord(record)) return "Now";
  if (isClassCompletedRecord(record)) return "Completed";
  return "Next";
}

function buildRiderReply(record) {
  const rider = cleanName(firstText(record?.riderName, record?.rider_name, record?.teamName, "Rider"));
  const ringText = formatRiderRingText(record);
  const horse = cleanName(firstText(record?.horseName, record?.barnName, "Horse"));
  const className = cleanName(firstText(record?.class_name, record?.group_name, "Class"));
  const classNumber = firstText(record?.class_number);
  const backNumber = firstText(record?.backNumber, record?.entryNumber);
  const scheduled = firstText(record?.latestStart, formatDisplayTime(record?.estimated_start_time));
  const latestGo = firstText(record?.latestGO, record?.estimatedGO);
  const ringWalk = firstText(record?.ringWalk);

  const parts = [
    `${rider} — ${ringText}. ${horse}. ${className}${classNumber ? ` (${classNumber})` : ""}`,
  ];

  if (backNumber) parts.push(`Back #${backNumber}`);
  if (scheduled) parts.push(`Scheduled ${scheduled}`);
  if (latestGo) parts.push(`Latest go ${latestGo}`);
  if (ringWalk && ringWalk !== "0") parts.push(`Ring walk ${ringWalk} min`);

  return `${parts.join(". ")}.`;
}

function formatRiderRingText(record) {
  const ringName = firstText(record?.ringName, record?.ring_name);
  if (ringName) return cleanName(ringName);

  const ringNumber = firstText(record?.ring_number);
  if (!ringNumber) return "Ring";

  const mapped = RINGS.find((r) => String(r.ring_number) === String(ringNumber));
  return mapped?.sms_label || `Ring ${ringNumber}`;
}

function firstText(...values) {
  for (const value of values) {
    if (value == null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function describeJsonShape(value) {
  if (Array.isArray(value)) return `array:${value.length}`;
  if (value && typeof value === "object") return `object:${Object.keys(value).slice(0, 8).join(",")}`;
  return value == null ? "null" : typeof value;
}

function debugPreview(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 160);
}

function sampleValues(values, limit) {
  const out = [];
  const seen = new Set();
  for (const value of values) {
    const text = cleanName(value);
    const key = normalize(text);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(text);
    if (out.length >= limit) break;
  }
  return out;
}

function addCacheBust(url) {
  const input = String(url || "").trim();
  if (!input) return input;

  try {
    const u = new URL(input);
    u.searchParams.set("_sms_ts", String(Date.now()));
    return u.toString();
  } catch {
    const sep = input.includes("?") ? "&" : "?";
    return `${input}${sep}_sms_ts=${Date.now()}`;
  }
}

function normalizeUrlForCompare(url) {
  try {
    const u = new URL(String(url || "").trim());
    u.search = "";
    u.hash = "";
    return u.toString();
  } catch {
    return String(url || "").trim();
  }
}

function logWorkerEvent(event, fields = {}) {
  try {
    console.log(JSON.stringify({
      event,
      ...fields,
    }));
  } catch {}
}

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
  { ring_number: 1, sms_label: "Derby", ring_id: 10, aliases: ["derby", "derby field"] },
  { ring_number: 2, sms_label: "Annex", ring_id: 3, aliases: ["annex"] },
  { ring_number: 3, sms_label: "Covered", ring_id: 44, aliases: ["covered"] },
  { ring_number: 4, sms_label: "Village", ring_id: 13, aliases: ["village"] },
  { ring_number: 5, sms_label: "Hunter 1", ring_id: 48, aliases: ["hunter 1"] },
  { ring_number: 6, sms_label: "Hunter 2", ring_id: 45, aliases: ["hunter 2"] },
];

function decideRingStrict(input) {
  const norm = normalize(input);
  const matches = [];

  for (const r of RINGS) {
    for (const a of r.aliases) {
      const an = normalize(a);
      if (aliasMatchesInput(norm, an)) {
        matches.push(r);
        break;
      }
    }
  }

  const extractedAliases = extractRingAliases(norm);
  for (const alias of extractedAliases) {
    const found = findRingByAlias(alias);
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

function aliasMatchesInput(normInput, normAlias) {
  const input = String(normInput || "").trim();
  const alias = String(normAlias || "").trim();
  if (!input || !alias) return false;

  const escapedAlias = escapeRegex(alias);
  const rx = new RegExp(`(?:^| )${escapedAlias}(?: |$)`);
  return rx.test(input);
}

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function findRingByAlias(normAlias) {
  return RINGS.find((r) =>
    r.aliases.some((alias) => normalize(alias) === normAlias)
  ) || null;
}

function extractRingAliases(norm) {
  const input = String(norm || "").trim();
  const out = [];
  const add = (alias) => {
    if (findRingByAlias(alias)) out.push(alias);
  };

  let m;
  const ringRx = /\bring\s*(\d{1,2})\b/gi;
  while ((m = ringRx.exec(input)) !== null) {
    add(`ring ${Number(m[1])}`);
  }

  const shortRx = /\br\s*(\d{1,2})\b/gi;
  while ((m = shortRx.exec(input)) !== null) {
    add(`r${Number(m[1])}`);
  }

  if (/^\d{1,2}$/.test(input)) {
    add(String(Number(input)));
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
