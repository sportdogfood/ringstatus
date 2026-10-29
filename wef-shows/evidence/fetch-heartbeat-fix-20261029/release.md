# Shows fetch and heartbeat correction — 2026-10-29

Status: PASS — deployed and verified through two normal scheduled runs.

Owner approval: investigate/fix Shows source fetch and heartbeat failures, preserve scope v1.2/count replay/existing workflows, test and verify two normal runs. No manual runs or record repairs.

## Evidence and smallest change

- Prior failed normal run e9d04fdf-a169-4536-8138-3a4ea322acd2 recorded default-ring `fetch failed`, then heartbeat PATCH 422. The old handler retained only error.message; its underlying transport cause cannot be recovered from that receipt. Subsequent untouched normal runs succeeded, including 17:44 UTC. That supports an intermittent failure, not a permanently broken endpoint or decoding route.
- Failure heartbeat sent selected-date as an empty string despite the saved schema identifying a date field. Corrected to null when no selected date exists. A schema-aware fixture rejects the original empty string and verifies the corrected failed heartbeat, error linkage, and unchanged canonical show.
- Added Shows-local bounded recovery to its existing Node source GET callback, following the existing Schedules GET recovery pattern. Maximum three attempts, 30 seconds per request, 500/1000ms waits, same URL and decoded proxy. Only recognized transient network/timeout errors and HTTP 429/5xx recover. Exhaustion stops and records safe nested error codes and endpoint path. No alternate hosts, direct-provider fallback, workflow retries or mutation retries.
- HTTP authentication/not-found failures, certificate failures, invalid JSON and missing decode proof do not retry. Existing validation, discovery helper, two logical source reads, archive protection and count replay remain unchanged.

## Validation

- 25 staged collector/source tests and 20 unchanged module tests passed.
- 45 deployed tests passed. These use fixtures; no production collector or source endpoint was manually run.
- Initial staging harness could not resolve the not-yet-deployed source module; corrected only the local harness require mapping before tests. No business workflow was invoked by that harness failure.
- Deployed four files at 17:46:08 UTC (1:46:08 p.m. Eastern): collector.js, source-fetch.js and their two test files. Existing scheduled task was idle with no collector lock. Established in-place scheduled deployment route reused.
- 19 existing top-level module files, including scope v1.2, count replay and configuration, hash-verified unchanged. Scheduled task definition hash unchanged. No schema changes, record repairs, manual runs, task changes or downstream changes.

## Acceptance and limits

Two post-deployment normal scheduled runs passed with Airtable run/source/heartbeat readback and canonical show-day/count replay checks. Failure-path behavior is fixture-proven; neither normal run required a recovered retry or produced a source failure. No production failure was manufactured. These successful runs prove normal operation, not the lost cause of the earlier network failure or guaranteed future source availability. Prior sources/errors release failure evidence remains historical and unaltered; this release provides the subsequent bounded correction and acceptance.

First normal run: c64f3c9a-7981-474a-9ab1-be43e661ce33, 17:49:02.266–17:49:41.117 UTC (1:49 p.m. Eastern). Task result 0. Airtable audit PASS at 17:50:03.122Z; run record recqrrufucejihMGH Success, three source links, zero error rows, heartbeat recCpv3iPc1R8eAqX current Success. Independent retained count replay and four canonical native show/day link checks PASS at 17:50:13.745Z. Evidence in audit-, days-, receipt- and task- files with that run ID. Older Data counts remain Stale/Unknown; this is not a fresh Data acceptance claim.

Second normal run: ebee16e8-5df0-4a04-ae88-48b65e68c867, 17:54:05.680–17:54:49.488 UTC (1:54 p.m. Eastern). Task result 0. Airtable audit PASS at 17:55:11.923Z; run record recUwZXnpGTRqRXQa Success, same three unique source records and heartbeat, no errors. Independent retained count replay and the same four canonical show-day IDs/native links PASS at 17:55:22.549Z. Default show 200000076 and source-selected 2026-10-29 remain consistent.

Scope v1.2, count replay and existing workflows are preserved. Discovery cadence policy and local archive handoff remain separately open; neither was changed or resolved by this repair. No manual collector runs or record repairs.
