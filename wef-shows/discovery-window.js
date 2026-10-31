"use strict";

const timezone = 'America/New_York';
const eastern = new Intl.DateTimeFormat('en-US', {
  timeZone: timezone, weekday: 'short', hour: '2-digit', hourCycle: 'h23'
});

function discoveryWindow(now) {
  const instant = new Date(now);
  if (!Number.isFinite(instant.getTime())) throw Error('Invalid discovery clock');
  const parts = Object.fromEntries(eastern.formatToParts(instant).map(p => [p.type, p.value]));
  const showDay = Number(parts.hour) >= 16 && Number(parts.hour) < 20;
  return { timezone, show: showDay && parts.weekday === 'Sun', showDay };
}

function canonicalShow(rows) {
  const defaults = rows.filter(r => r.fields.is_default === true && Number(r.fields.customer_id) === 15);
  if (defaults.length !== 1) throw Error('Exactly one retained canonical show required outside Sunday discovery');
  const row = defaults[0], f = row.fields;
  if (!Number.isSafeInteger(f.show_id) || f.show_id <= 0 || f['show-key'] !== `15|${f.show_id}`)
    throw Error('Invalid retained canonical show identity');
  require('./show-days').dates(row);
  const {isoDate} = require('../wef-schedules/collect');
  const day = isoDate(f.selected_date);
  if (day < f.start_date || day > f.end_date || !Number.isFinite(Date.parse(f.last_checked)))
    throw Error('Invalid retained canonical show date/evidence time');
  return row;
}

// The known show is selected from canonical authority, never from a fabricated ring response.
async function discoverShowDay({show, fetchJson}) {
  const {isoDate} = require('../wef-schedules/collect');
  const sid = show.fields.show_id;
  const endpoint = require('../wef-schedules/collector-core').buildScheduleEmptyEndpoint(sid, 15);
  const payload = await fetchJson(endpoint), meta = payload.show || {};
  if (Number(meta.show_id) !== sid || (payload.show_id != null && Number(payload.show_id) !== sid))
    throw Error('Default schedule disagrees with retained canonical show');
  const start = isoDate(meta.start_date || payload.start_date);
  const end = isoDate(meta.end_date || payload.end_date);
  const selected = isoDate(payload.show_date ?? payload.showDate);
  if (start > end || selected < start || selected > end) throw Error('Invalid source-selected show day');
  return {payload, show: {fields: {show_id: sid, show_name: meta.show_name || payload.show_name || '',
    start_date: start, end_date: end, sgl_default_day: selected}}};
}

module.exports = {discoveryWindow, canonicalShow, discoverShowDay};
