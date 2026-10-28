"use strict";
const integer = (v, label) => {
  const n = Number(v);
  if (v == null || v === "" || !Number.isSafeInteger(n) || n <= 0)
    throw Error("Invalid " + label);
  return n;
};
const number = (v) =>
  v == null || v === "" ? null : Number.isFinite(Number(v)) ? Number(v) : null;
const flag = (v) => [true, 1, "1", "true"].includes(v);
const text = (v) => (v == null ? "" : String(v));
const day = (v) =>
  /^\d{4}-\d{2}-\d{2}/.test(text(v)) ? text(v).slice(0, 10) : null;
const key = (...v) => v.join("|");
function localDay(now) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(now));
}
function scope(shows, now) {
  const current = shows.filter(
    (r) =>
      Number(r.fields.customer_id) === 15 && r.fields.is_default === true,
  );
  if (current.length !== 1)
    throw Error("Expected one default wef-shows record");
  const s = current[0].fields;
  const date = day(s.selected_date);
  const age = Date.parse(now) - Date.parse(s.last_checked);
  if (s['show-key'] !== `15|${s.show_id}` || !Number.isFinite(age) || age < -60000 || age > 30 * 60000)
    throw Error('Invalid or stale wef-shows default');
  if (
    !date ||
    date < s.start_date ||
    date > s.end_date ||
    !text(s.available_dates).split(/,\s*/).includes(date)
  )
    throw Error("Invalid authority show/date");
  return {
    show: integer(s.show_id, "show ID"),
    customer: integer(s.customer_id, "customer ID"),
    date,
    authority: current[0].id,
    checked_at: s.last_checked,
    active: date === localDay(now),
  };
}
function unique(rows, field) {
  const seen = new Set();
  for (const r of rows) {
    const v = r[field];
    if (!v || seen.has(v)) throw Error("Duplicate or blank " + field);
    seen.add(v);
  }
  return rows;
}
function common(s, now, raw) {
  return {
    "show-id": s.show,
    "customer-id": s.customer,
    date: s.date,
    "last-seen": now,
    "source-updated-at": text(raw.curr_updated_at),
    "source-json": JSON.stringify(raw),
  };
}
function groups(payload, s, now) {
  if (!Array.isArray(payload)) throw Error("Unreadable groups list");
  const selected = payload.filter((x) => Number(x.show_id) === s.show);
  if (selected.length > 1) throw Error("Duplicate source show");
  if (!selected.length) {
    if (payload.length) throw Error("Live source show mismatch");
    return { rings: [], groups: [], classes: [] };
  }
  if (!Array.isArray(selected[0].json_data))
    throw Error("Missing explicit groups array");
  const rows = selected[0].json_data;
  if (rows.some((g) => !day(g.day))) throw Error("Group date missing");
  if (rows.length && !rows.some((g) => day(g.day) === s.date))
    throw Error("Live source day mismatch");
  const rings = new Map(),
    classes = new Map(),
    out = [];
  for (const g of rows.filter((g) => day(g.day) === s.date)) {
    const rid = integer(g.ring_id, "ring ID"),
      gid = integer(g.class_group_id, "group ID"),
      rk = key(s.show, s.date, rid),
      gk = key(s.show, s.date, gid);
    if (
      !Array.isArray(g.classes) ||
      !Array.isArray(g.classNumbers) ||
      !Array.isArray(g.classNames) ||
      g.classes.length !== g.classNames.length ||
      g.classes.length !== g.classNumbers.length
    )
      throw Error("Incomplete class identities");
    const ring = {
      ...common(s, now, g),
      "ring-key": rk,
      "ring-id": rid,
      "ring-number": integer(g.ring_number, "ring number"),
      "ring-name": text(g.ring),
    };
    if (rings.has(rk) && rings.get(rk)["ring-number"] !== ring["ring-number"])
      throw Error("Conflicting ring identity");
    rings.set(rk, ring);
    const group = {
      ...common(s, now, g),
      "group-key": gk,
      "class-group-id": gid,
      "group-name": text(g.group_name),
      "group-sequence": number(g.group_sequence),
      status: text(g.status),
      "is-live": flag(g.is_live),
      "has-json": flag(g.has_JSON),
      "estimated-start-time": text(g.estimated_start_time),
      _ring: rk,
      _classes: [],
    };
    for (const n of [
      "total",
      "gone",
      "jo_total",
      "jo_gone",
      "r2_total",
      "r2_gone",
    ])
      group[n.replaceAll("_", "-")] = number(g[n]);
    for (let i = 0; i < g.classes.length; i++) {
      const cid = integer(g.classes[i], "class ID"),
        ck = key(s.show, s.date, cid);
      group._classes.push(ck);
      const prior = classes.get(ck);
      if (prior && prior._ring !== rk)
        throw Error("Class appears in conflicting rings");
      if (prior) {
        prior._groups.push(gk);
        prior["detail-available"] ||= flag(g.has_JSON);
      } else
        classes.set(ck, {
          ...common(s, now, g),
          "class-key": ck,
          "class-id": cid,
          "class-number": number(g.classNumbers[i]),
          "class-name": text(g.classNames[i]),
          "detail-available": flag(g.has_JSON),
          _groups: [gk],
          _ring: rk,
        });
    }
    out.push(group);
  }
  return {
    rings: [...rings.values()],
    groups: unique(out, "group-key"),
    classes: [...classes.values()],
  };
}
function ringState(payload, s, plan) {
  if (!Array.isArray(payload)) throw Error("Invalid ring widget");
  const shows = payload.filter((x) => Number(x.show_id) === s.show);
  if (shows.length !== 1 && plan.rings.length)
    throw Error("Widget show mismatch");
  if (!shows.length) return;
  const live = shows[0].live_data;
  if (!live || typeof live !== "object" || Array.isArray(live))
    throw Error("Missing ring data");
  for (const ring of plan.rings) {
    const raw = live[String(ring["ring-number"])];
    if (!raw) throw Error("Widget ring missing");
    for (const [source, dest] of [
      ["livenow", "_current"],
      ["upcoming", "_next"],
      ["completed", "_completed"],
    ]) {
      if (raw[source] !== undefined && !Array.isArray(raw[source]))
        throw Error("Invalid widget ring groups");
      ring[dest] = (raw[source] || [])
        .filter((g) => day(g.day) === s.date)
        .map((g) => {
          if (Number(g.ring_id) !== ring["ring-id"])
            throw Error("Widget ring ID mismatch");
          const k = key(
            s.show,
            s.date,
            integer(g.class_group_id, "widget group"),
          );
          if (!plan.groups.some((x) => x["group-key"] === k))
            throw Error("Widget group absent from group response");
          return k;
        });
    }
    ring["source-json"] = JSON.stringify(raw);
  }
}
function classDetail(payload, decoded, s, c, now) {
  const cid = c["class-id"];
  if (
    Number(payload?.ID) !== cid ||
    !Array.isArray(payload.rows) ||
    Number(payload.recs) !== payload.rows.length
  )
    throw Error("Incomplete class detail");
  if (
    Number(decoded?.class?.class_id) !== cid ||
    Number(decoded?.class?.show_id) !== s.show ||
    day(decoded.class_related_data?.date) !== s.date ||
    !Array.isArray(decoded.trips)
  )
    throw Error("Decoded class scope mismatch");
  const trips = new Map();
  for (const t of decoded.trips) {
    const id = integer(t.trip_id, "decoded trip");
    if (trips.has(id)) throw Error("Duplicate decoded trip");
    trips.set(id, t);
  }
  const rows = payload.rows.map((r) => {
    if (Number(r.class_id) !== cid) throw Error("Live row class mismatch");
    const tid = integer(r.id, "live trip"),
      t = trips.get(tid);
    if (!t) { const error=Error("No decoded identity for trip " + tid);error.code="LIVE_IDENTITY_SNAPSHOT_MISMATCH";error.trip_id=tid;throw error; }
    const eid = integer(t.entry_id, "entry ID");
    return {
      ...common(s, now, { live: r, identity: t }),
      "source-updated-at": text(payload.curr_updated_at),
      "live-entry-key": key(s.show, s.date, cid, tid),
      "trip-id": tid,
      "entry-id": eid,
      "entry-number": number(r.ENo),
      "horse-id": number(t.horse_id),
      "horse-name": text(r.Hor),
      "rider-id": number(t.rider_id),
      "rider-name": text(r.Rid),
      "trainer-id": number(t.trainer_id),
      "trainer-name": text(r.Tra || t.trainer),
      "entry-key": key(s.show, s.date, eid, cid),
      "order-of-go": number(r.OOG),
      "actual-order": number(r.Actual_OOG),
      scratch: flag(r.Scr),
      gone: number(r.Gone),
      position: number(r.ActualPos ?? r.Pos),
      _class: c["class-key"],
    };
  });
  const count = (v) => text(v).split(",").filter(Boolean).length;
  return {
    entries: unique(rows, "live-entry-key"),
    fields: {
      "source-json": JSON.stringify(
        Object.fromEntries(
          Object.entries(payload).filter(([k]) => k !== "rows"),
        ),
      ),
      "source-updated-at": text(payload.curr_updated_at),
      total: payload.rows.length,
      gone: count(payload.gone_trips),
      "jo-gone": count(payload.jo_gone_trips),
      "r2-gone": count(payload.r2_gone_trips),
      "current-trip-id": number(payload.curr),
    },
  };
}
function reference(rows, fields, label) {
  const index = new Map();
  for (const r of rows) {
    const k = fields.map((f) => r.fields[f]).join("|");
    if (index.has(k)) throw Error("Ambiguous " + label + " " + k);
    index.set(k, r.id);
  }
  return index;
}
module.exports = {
  integer,
  number,
  flag,
  day,
  key,
  localDay,
  scope,
  unique,
  common,
  groups,
  ringState,
  classDetail,
  reference,
};
