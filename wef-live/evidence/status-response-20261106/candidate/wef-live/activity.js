"use strict";
function statusValue(payload) {
  if (typeof payload === 'boolean') return payload;
  if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
    for (const key of ['status', 'is_live', 'live', 'payload']) {
      if (typeof payload[key] === 'boolean') return payload[key];
    }
  }
  const shape=payload&&typeof payload==='object'?(Array.isArray(payload)?'array':'object keys ['+Object.keys(payload).join(',')+']'):typeof payload;
  throw Error('Unreadable live status: expected explicit boolean; received '+shape);
}
async function readStatus(scope, request = globalThis.fetch) {
  const url = `https://sglapi.wellingtoninternational.com/homepage/getLiveClassStatus?customer_id=${scope.customer}&datetimestamp=${Date.now()}`;
  const target=new URL(url);
  const response = await request(url, {
    headers: {
      Accept: 'application/json, text/plain, */*', 'Cache-Control': 'no-cache',
      'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1',
      Pragma: 'no-cache', Expires: '0', Origin: 'https://www.wellingtoninternational.com',
      Referer: 'https://www.wellingtoninternational.com/', 'sgl-request-origin': 'SGL-API',
      authority: target.host, method: 'GET', path: target.pathname+target.search, scheme: 'https',
      'accept-encoding': 'gzip, deflate, br, zstd', 'accept-language': 'en-US,en;q=0.9', priority: 'u=1, i',
      'sec-ch-ua': '"Not-A.Brand";v="99", "Microsoft Edge";v="157", "Chromium";v="157"',
      'sec-ch-ua-mobile': '?1', 'sec-ch-ua-platform': '"iOS"',
      'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-site',
    },
    signal: AbortSignal.timeout(30000),
  });
  if (!response.ok) throw Error(`Live status HTTP ${response.status}`);
  return statusValue(await response.json());
}
function allCompleted(groups) {
  return groups.length > 0 && groups.every(g => String(g.status).trim().toLowerCase() === 'completed');
}
module.exports = { statusValue, readStatus, allCompleted };
