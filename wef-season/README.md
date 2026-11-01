# wef-season — retained entities and horse associations

**README version: 1.1.1**  
**Updated: November 1, 2026, 12:37 p.m. America/New_York (UTC−05:00)**

## Scope and ownership

Season adds, upserts and deduplicates existing trainers, riders, horses and rings from wef-data into the four existing Season bases. It preserves keys, native/source IDs, records, source timestamps and supported show/show-day memberships. Horse associations and rings-helper are included. People management remains cancelled.

Data acquisition is independently owned by wef-data-fast-collector, chat01a19273-bdf8-71b2-8503-cc8079efc129. Records coordinates; Shows owns canonical show/day context. Season does not collect source data, edit siblings, archive, clean up Data or create new business identities. Codex is not the runner.

| Destination | Base | Entity table |
| --- | --- | --- |
| Trainers | appzZsd3UqMRR2CNc | tbl1vRzTPX4UOoW8u |
| Riders | appkWkm9rFLrkUvaD | tblQxTdBoM6hWEZzX |
| Horses | appETrjio9Yj9Pv17 | tbl8uYnqzysdiBqiu |
| Rings | appxMk9wfbBHEvC5e | tblzQ33F0whmvcPjp |

## Workflow and cadence

The existing Windows task **wef-season-collector runs every hour (PT1H)** through run.ps1 → entry.js → season.py. It reads current Data business records from appdl16HJ5ETm8N69 and uses the existing native upsert/readback code. It leaves unchanged records alone. Directory links establish show registration; single trip roles supply supported day memberships. Existing historical memberships remain additive.

No completed-day sequence, receipt/cache revision, canonical freshness/fence or heartbeat checkpoint is required to run entity upserts. Existing key/identity safety, uncertain-write reconciliation, overlap protection, failure boundaries and heartbeat/run/error logging remain. No manual collector substitutes are allowed. Preserve the existing task definition and approved cadence; do not create another timer.

## Horse associations and conflicts

The approved release added two multiline-text fields in horses:

- rider-associations (fldtUh5WtRHYsDjq6)
- trainer-associations (fldSo6L3GZj5joi40)

Each JSON map uses the unchanged rider/trainer key with shows, show-days and sources arrays containing existing keys. Multiple riders and different trainers across trips/shows are valid. No bindings table, copied people tables, new keys or new syncs were introduced.

The new conflicts table tblZlDewVNlXmPRqS in the horse base uses source-key, source-type, horse-key, details and status. Ambiguous/changed assignments on the same source are logged there; only the conflicting assignment is withheld and other work continues. Missing roles are not guessed. Repeat conflicts reuse their existing source record rather than creating duplicates. This release changed the approved horse schema; it did not change existing keys/IDs or Data.

## Deployed files and verification

- season.py SHA256: c44a6fd07b5fe66c7858038cbc0b15efc7d3ba3dc5a1978753ea71ae8a208311
- horse_associations.py SHA256: 7732f106d7bb98139d297876a2a5e42eeacd3e24fca880e8a850c96f641c45c1
- schema.json SHA256: b314638055c3dea3c02e80a652ef787ef3cead2862f76af6cd4c50df1f4b1944

Independent review and 21 targeted checks passed before deployment on November1 at08:37Eastern. This is separate from scheduled proof:

- **08:51 run:** all four bases succeeded; 897horse association records populated. Native audit found 295horses with312rider associations (17multi-rider horses),897trainer associations and6logged conflicts. All existing records, IDs, keys, memberships and nicknames were preserved.
- **09:51 repeat:** all four succeeded; 896horse association records unchanged,one updated only with new trip evidence. Rider/trainer pair totals stayed312/897; the same6conflict IDs/contents remained, with no duplicates or lost prior associations/records.
- **Latest inspected run,11:51:15:** all four succeeded, completing11:52:22Eastern, task exit0. Associations:0added,896unchanged,1updated;6conflicts. This is saved scheduled evidence, not a fresh full native audit.

[Release and initial native proof](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/horse-associations-20261101/final-verification.json>) · [09:51 scheduled receipt](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-season/runtime/logging/wef-season-horses/run-logs/0272036b-b8ce-4d9a-8b38-d7a5a6c64f20.json>) · [Preserved latest scheduled result](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/readme-refresh-20261101/latest-scheduled-result.json>).

## Existing logs, boundaries and guidance

Runtime/last-run.json is the latest outcome; runtime/logging/wef-season-{entity}/run-logs contains per-attempt evidence. Each base retains heartbeat,run-logs and err-logs. Association conflicts are business records, not a reason to stop unrelated updates. New actual workflow/verification failures remain subject to owner edit/retry/resumption rules; preserving those rules does not authorize a repair or task change.

Last fully loaded coordination guidance is Operating Skills0.2.8; [complete read proof](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/coordination-rules-20261101/coordinators/records/recipients/wef-season/load-receipt.json>). Loading is not a business run, review, compaction or guarantee. Status answers begin Yes/No; ifNo, continue approved work or state the exact blocker.

This is a documentation refresh only. Runtime code/schema/task/records are unchanged by it. The former README, including superseded Data ownership, five-minute cadence and completed-day instructions, is preserved as [historical reference](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/readme-refresh-20261101/module-before-1.1.1.md>), not current operating instructions. No new implementation is pending from this documentation update.
