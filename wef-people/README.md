# wef-people

Version 4 — November 1, 2026. Owner chat 01a19727-9a70-7283-a865-01dd38aacb9d; Records coordinator 01a169bc-e58a-7de0-94c6-5d3931fcf745.

Latest direct owner corrections: **disregard owners** and **leave horses unlinked**. Both are excluded from completeness criteria. No source decision remains pending. No external/Data relationship reads are authorized. The horses field stays empty; missing horse evidence is not reported as a workflow problem.

## Authorized boundary and baseline

November 1 creation handoff authorizes this new derived People module, five private tables, a four-hour local runner and a five-day Codex review. Existing target is **wef-seaon-people**, `app6uHgUYCCKTVlJE`; preserve its name. No prior People implementation exists in this checkout. Repository HEAD at intake: `97aba86850ecf40d32aabc701ee96196555ab230`. Existing dirty sibling files are excluded. No dependency upgrades, upstream writes, deletion, repair, manual join runs or sibling cadence changes.

Reuse existing `wef-schedules/tests/wef_data_airtable.py` paginated transport, `wef-data/attempt-log.js` and `.ps1` resilient logs, and Season's hidden Windows task route. SeasonStore is restricted to four Season bases, so it must not be modified to admit People. A module-local projection/upsert is the minimal new behavior. Existing Season implementation is reference evidence, not proof this module works.

## Tables and evidence

Full field IDs/types are in `schema.json`. Inputs: riders `tblGjqYudtbGD3dsY`, trainers `tblnA6yeQtZhKYkQ5`, horses `tblbRctpu5tpvJYdg`, shows `tblXZGPbcoz4gbfwq`, days `tblyANazuTky0Yy31`. Rings and meta are untouched. Outputs: people `tbl1bRy6ftpQKIoX2`, run-logs `tblkszE4vwlZggf7k`, err-logs `tblCf5Td8wMfJMeyJ`, heartbeat `tblrbrB0TvUDFg65U`, code-review `tblpBEZbKMWIvX7ud`.

All five requested tables created through Airtable MCP. Requested native local links automatically create Airtable inverse link fields on shared tables; no original shared field or record was edited. Never use cross-base record IDs in local links.

No w-owners exists; owners are now excluded by the owner. Read-only source inspection confirms Data (`appdl16HJ5ETm8N69`) entries `tblcKPbT4DPbFPu5b` and trips `tblt0GvQ39bHIu930` have existing relationships. They are not shared inputs here and are not runtime inputs. No new feed or invented relationship.

## Join contract

Read every available shared role row and all show/day keys. Role IDs remain distinct namespaces (`rider:<key>`, `trainer:<key>`); source code does not establish cross-role identity equivalence. Names never merge people. Duplicate or invalid role keys retain separate source-record identities and unresolved status. Preserve original source observation time. Resolve comma-separated membership keys exactly; duplicate/missing keys log issues and withhold only that link. Co-attendance never associates a horse. Horses remain unassigned until evidence is available. Existing valid links and records are retained; no deletion. Repeated unchanged inputs must not create or modify people records.

## Runtime, acceptance and stops

First natural run **verified**: scheduled 07:54:44 Eastern November 1, joined/read back 919 people (647 riders, 272 trainers), zero duplicates, zero horse links, zero withheld people. Task exit 0; remote log delivery succeeded. Independent full connector readback matched all expected identity/name/source-clock/native-membership values. Evidence: `evidence/first-natural-run.json`, `natural-readback.json`, `natural-logs-readback.json`. Airtable run `recjRKCLt9dsbbimJ`, heartbeat `recgujbVoWMUnSZhj`, 26 error records. Status **Partial** honestly records 851 issues: 545 memberships reference absent October 23–25 day keys, and 306 role records share numeric IDs across unverified namespaces (153 pairs kept separate). Every issue retains its source record in grouped errors. No last-success timestamp is claimed while status is Partial.

Next natural run 11:54:44 Eastern, then every four hours. Real repeat-run acceptance is **PENDING**; offline repeat/idempotence passed. No manual run or retry was performed. Read the next natural result and compare native People IDs/keys/links against the first receipt; any actual workflow failure stops the affected path and needs fresh applicable authorization. Missing historical days are an input gap, not permission to create/repair shared records. Cross-role equal IDs are not permission to merge.

Windows task `wef-people-join` is registered and enabled every four hours, using the existing Season account and hidden VBS → PowerShell → Node → existing Python route. Only ordinary scheduled triggers execute the join. Task XML is saved in evidence/scheduled-task.xml. Every normal invocation has durable local attempt/error fallback, Airtable run/error/heartbeat delivery, changed-record readback and a persistent stop after actual workflow failure. Partial data coverage is distinct from workflow failure; unresolved day memberships or identity issues mean Partial, never full success. No stop clearing or failed-run edits without fresh authorization.

Review cadence: ACTIVE automation `wef-people-five-day-code-review`, every five days at 10:00 America/New_York, this chat. Actual dated reports only in code-review. A schedule is not execution evidence. Self-review and targeted offline checks are saved in evidence; no independent reviewer claimed. First natural scheduled execution/readback is verified; later repeat-run acceptance and the recurring code-review execution remain unverified.

Instructions READ: supplied global rules and root AGENTS; canonical Instruction Loader, Rebalance, Code Review, Context Compact; installed operating package manifest **0.2.5** and corresponding skills. No RingStatus workflow mode selected. Private checkpoints belong under ringstatus-data/.git/context-recovery/01a19727-9a70-7283-a865-01dd38aacb9d, never business tables.
