(() => {
  const endpoints = {
    "/wec-mobile": "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-mobile-embed-html",
    "/wec-print": "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-print-embed-html"
  };
  const endpoint = endpoints[location.pathname];
  if (!endpoint) return;

  const load = async () => {
    try {
      const response = await fetch(endpoint + "&_=" + Date.now(), { cache: "no-store" });
      if (!response.ok) throw new Error("WEC load failed " + response.status);
      const html = await response.text();
      const doc = new DOMParser().parseFromString(html, "text/html");
      document.querySelectorAll("[data-rs-wec-loader]").forEach((node) => node.remove());
      doc.querySelectorAll("style,link[rel='stylesheet']").forEach((node) => {
        const clone = node.cloneNode(true);
        clone.setAttribute("data-rs-wec-loader", "1");
        document.head.appendChild(clone);
      });
      document.body.innerHTML = doc.body.innerHTML;
      doc.querySelectorAll("script").forEach((script) => {
        if (script.textContent.trim()) new Function(script.textContent)();
      });
    } catch (error) {
      document.body.innerHTML = '<div style="padding:12px;font:14px Arial,sans-serif;color:#111;">WEC failed to load: ' + String(error.message || error) + '</div>';
    }
  };

  load();
})();
