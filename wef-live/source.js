"use strict";
const ROOT =
  "https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/";
const endpoints = {
  groups: ROOT + "ListAjax?from_wp_api=true",
  widget:
    "https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget",
  detail: (s, id) => ROOT + `getLiveClassData?show_id=${s.show}&cid=${id}&t=${Date.now()}`,
  decoded: (s, id) =>
    `https://wef-schedules-decoded.gombcg.workers.dev/classes/${id}?show_id=${s.show}&customer_id=${s.customer}&cgid=-1`,
};
async function get(url, decoded = false) {
 const target = new URL(url);
 const direct = target.origin === 'https://sgl.wellingtoninternational.com' && [
  '/iphonev2/index.php/esp/liveclassv2/ListAjax',
  '/iphone.php/esp/webservice/LiveScoreWidget',
  '/iphonev2/index.php/esp/liveclassv2/getLiveClassData',
 ].includes(target.pathname);
 const request = direct ? async target => {
  const response = await fetch(target, {
   headers: { Accept: 'application/json', 'Cache-Control': 'no-cache', 'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1' },
   signal: AbortSignal.timeout(30000),
  });
  return { response, text: await response.text() };
 } : undefined;
 return require('../lib/wef-source-read').readSource(url,{decoded,...(request ? {request} : {})});
}
module.exports = { endpoints, get };
