"use strict";
const ROOT =
  "https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/";
const endpoints = {
  groups: ROOT + "ListAjax?from_wp_api=true",
  widget:
    "https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget",
  detail: (s, id) => ROOT + `getLiveClassData?show_id=${s.show}&cid=${id}`,
  decoded: (s, id) =>
    `https://wef-schedules-decoded.gombcg.workers.dev/classes/${id}?show_id=${s.show}&customer_id=${s.customer}&cgid=-1`,
};
async function get(url, decoded = false) {
  const r = await fetch(url, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(30000),
  });
  if (!r.ok) throw Error("Source HTTP " + r.status);
  if (decoded && r.headers.get("x-rs-decoded") !== "1")
    throw Error("Decoding not confirmed");
  let data;
  try {
    data = await r.json();
  } catch {
    throw Error("Unreadable source JSON");
  }
  if (!data || typeof data !== "object" || data._t)
    throw Error("Invalid source payload");
  return data;
}
module.exports = { endpoints, get };
