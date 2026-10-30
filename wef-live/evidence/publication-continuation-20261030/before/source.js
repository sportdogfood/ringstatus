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
 return require('../lib/wef-source-read').readSource(url,{decoded});
}
module.exports = { endpoints, get };
