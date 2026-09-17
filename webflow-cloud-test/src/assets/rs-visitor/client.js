(function () {
  var allowedPaths = new Set(["/", "/lainey", "/lm", "/ld", "/lh", "/lc"]);
  var pagePath = window.location.pathname;
  if (!allowedPaths.has(pagePath) || window.__rsVisitorEventSent) return;
  window.__rsVisitorEventSent = true;

  try {
    var query = new URLSearchParams(window.location.search);
    var eventUid = typeof crypto.randomUUID === "function"
      ? crypto.randomUUID()
      : "visitor_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2);
    var payload = {
      event_uid: eventUid,
      page_path: pagePath,
      referrer: document.referrer || "",
      client_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "",
      viewport_width: window.innerWidth,
      utm_source: query.get("utm_source") || "",
      utm_medium: query.get("utm_medium") || "",
      utm_campaign: query.get("utm_campaign") || ""
    };

    fetch("https://ringstatus.com/test/rs-visitor/event", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      keepalive: true,
      credentials: "omit"
    }).catch(function () {});
  } catch (_) {}
})();
