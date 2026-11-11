# Actual schedule/class header fallback — 8 November 2026

Existing shared-worker fallback branch changed only for schedule and class-ID requests. Primary runner, existing Cloudflare transport, one-second global queue, agent URLs and schedules preserved. Health uses its separate existing sequence without nested attempts. No runner restart or collector invocation.

47 focused checks passed; real Cloudflare preview for Fast date 2026-11-08 returned seven schedule rings and class 200031205 with 27 trips, both through header2. Code review packet verifier PASS. Worker version 5367f7f7-73ca-4009-9341-1c4665609c41 deployed at 09:31 Eastern; allow existing five-minute Durable Object rollout. First normal scheduled collection started 09:33:16 Eastern and completed 09:39:00: ok=true, 1,898 records saved, zero errors. Dated schedule/class acquisition and class/entry/trip/result saves are present. Second normal collection started 09:39:16 and remains in progress. See normal-run-1.json.

Protected hashes in protected.json prove no changes to pacing, decoder, fallback transport, Fast reader/collector, show-time or Wrangler configuration. Other owners' working changes were not included.

## Final normal scheduled acceptance

Two normal runs completed with ok=true, 1,898 saves each, zero errors: 09:33:16–09:39:00 and 09:39:17–09:45:01 Eastern. Both include schedule acquisition and class-detail/entry/trip/result acquisition and saves. Evidence: normal-run-1.json, normal-run-2.json, normal-run-log-readback.json and acceptance.json. No manual collectors. Fast separately confirmed its reader/collector hashes unchanged and no duplicate recovery.
