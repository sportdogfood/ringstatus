"use strict";
function statusValue(payload) {
  if (typeof payload === 'boolean') return payload;
  if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
    for (const key of ['status', 'is_live', 'live', 'payload']) {
      if (typeof payload[key] === 'boolean') return payload[key];
    }
  }
  throw Error('Unreadable live status: expected explicit boolean');
}
async function readStatus(scope, request = globalThis.fetch) {
  const url = `https://sglapi.wellingtoninternational.com/homepage/getLiveClassStatus?customer_id=${scope.customer}&datetimestamp=${Date.now()}`;
  const response = await request(url, {
    headers: { Accept: 'application/json', 'Cache-Control': 'no-cache' },
    signal: AbortSignal.timeout(30000),
  });
  if (!response.ok) throw Error(`Live status HTTP ${response.status}`);
  return statusValue(await response.json());
}
function allCompleted(groups) {
  return groups.length > 0 && groups.every(g => String(g.status).trim().toLowerCase() === 'completed');
}
module.exports = { statusValue, readStatus, allCompleted };
