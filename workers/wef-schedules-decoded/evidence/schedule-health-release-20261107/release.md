# Schedule health and logging — 7 November 2026

Approved scope: keep show-time clock-only; default-show schedule health; three fixed header profiles, one renewal and repeat after exhaustion; health, renewal and warning records. Existing hourly owner-chat monitoring handles warning submission. No collector, infrastructure, dependency or connection changes.

44 focused checks passed. Local and existing Cloudflare preview returned seven rings with each profile. Corrected show-day preview links current show rec8GtJJ7eSjY3Nd3 and day rec1mryBzjt5wl1TI. Real token metadata saves/readbacks passed for preview, fallback and runner.

Worker version 4ac8cf13-9283-47ce-b8b6-ddb749cfa4d3 is deployed. Existing relay loaded token logging; installed runner worker.js was not replaced. After the five-minute queue propagation window, live `/health` returned 200/ok=true, current default show 200000077, seven rings, runner lane, no errors. Airtable health readback confirms show rec8GtJJ7eSjY3Nd3 and show-day rec1mryBzjt5wl1TI. Normal live schedule returned seven rings via runner; class 200031233 returned eight trips via existing fallback. See live-acceptance.json.

Synthetic exhaustion/recovery was tested offline, not manufactured against production. Pending warning submission is handled by the existing monitoring chat; no claim of owner-read acknowledgment.

Remaining unrelated open work: primary class-detail issue, paused class-detail work and independent source-clock freshness qualification. No changes to those.

Normal show-time 18:36 Eastern: Recorded, clock=true, clockVersion=0.1.44, errors=[]; clock module hash unchanged during this release. Legacy tasks remain disabled. Warning-log contains no fabricated outage or recovery rows; transition/delivery-state behavior was simulated offline, and the existing hourly monitor is configured to submit actual pending notices.
