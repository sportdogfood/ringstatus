# Independent schedule JSON tasks

Days 0–4 are separate Windows tasks with separate scripts and outputs. None imports or starts another or waits for Fast, saving or linking.

| Task | Script | Eastern daytime | Eastern overnight |
|---|---|---|---|
| wef-schedule-json-day-0 | day0.mjs | 07:00–18:50, every 10 minutes | 19:00–06:00, every hour |
| wef-schedule-json-day-1 | day1.mjs | 07:00–18:40, every 20 minutes | 19:00, 21:00, 23:00, 01:00, 03:00, 05:00 |
| wef-schedule-json-day-2 | day2.mjs | 07:00–18:30, every 30 minutes | 19:00, 22:00, 01:00, 04:00 |
| wef-schedule-json-day-3 | day3.mjs | 07:00–18:20, every 40 minutes | 19:00, 23:00, 03:00 |
| wef-schedule-json-day-4 | day4.mjs | 07:00–18:40, every 50 minutes | 19:00, 00:00, 05:00 |

Each reads the existing default show and its actual day-priority date from synced WEF Shows records in WEF Data, then requests the decoder `/schedule` once with `User-Agent: Mozilla/5.0`. Decoder recovery remains internal to that request. No class requests, collector calls, Airtable writes, saving/linking triggers or script retries.

Full successful JSON is saved to `wef-data/runtime/schedule-json/dayN/{customer}/{show}/{date}.json`. Each task writes its own `last-run.json` with the actual date, URL, result, time and saved path. Failure leaves the previous successful JSON intact. A missing actual show-day is recorded as Skipped. Zero rings is accepted.

The private existing `runtime/collector.env` supplies Airtable read access. No credentials are copied or changed. Existing collectors and their schedules are unchanged. The PC must be on and the existing Windows owner account logged in, matching the existing runner host.

Offline verification: `node --test wef-data/schedule-json/schedule-json.test.mjs`. This uses mocked network responses and temporary output only; it does not run a collector or contact SGL/Airtable.
