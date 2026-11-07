# Confirmed CF-Worker header trigger

CONFIRMED: Adding CF-Worker: gombcg.workers.dev alone to the otherwise-working local SGL schedule request changes populated 7-ring JSON to HTTP200 literal {}. Removing only that header restores 7 rings. Same token, cookie, URL, local connection code and other headers.

Evidence: cf-worker-header-isolation.json. Wire echo used placeholder credentials only. Local A/B/A source requests used the same fresh token and existing cookies. No decoder or tunnel. No production changes or collectors.

The SGL-side rule implementation is not visible. Header is a proven sufficient trigger; this does not establish it is the only condition affecting Cloudflare traffic.

Cloudflare documents automatic CF-Worker injection: https://developers.cloudflare.com/fundamentals/reference/http-headers/#cf-worker
