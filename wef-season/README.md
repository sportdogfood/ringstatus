# wef-season — retained entities and horse associations

**README version: 1.1.4**  
**Updated: November 2, 2026, 2:58 p.m. America/New_York (UTC−05:00)**

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

### Verified ring recovery — November 2, 2026

The owner-approved Season-only repair omits unavailable ring membership fields while retaining supported ring facts. Existing native links are preserved; no show/day relationship is invented. This removes the missing-link failure without changing Data, the shared store, helpers, schema, keys or hourly task. Independent review and 26 offline checks passed; only season.py was deployed at 2:34:52 p.m. Eastern. The original ring stop was preserved as evidence and released under the owner's repair/recovery approval.

The normal 2:51 p.m. hourly run finished at 2:52:32 p.m.; all four bases succeeded. Native readback verified at 2:54:36 p.m. that all 32 key-only ring rows were filled using their same native IDs, all 88 ring keys/records and prior ring fields/memberships were preserved, and all 14 helpers and owner nicknames remained unchanged. No new stop or manual collector run occurred. Missing upstream show/day memberships remain withheld; this recovery does not claim complete new-show membership. Earlier stopped-state paragraphs below describe the pre-repair observation, not the current ring outcome. [Scheduled recovery and native preservation](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/ring-missing-links-20261102/final-verification.json>).

### Post-deployment Code Review and known complaint

Independent post-deployment Code Review passed on November 2: no actionable product-code defect found in the approved five-line repair. The reviewer compared actual source and deployed hashes, inspected the saved normal hourly run and native before/after records, and reused the unchanged 26 passing tests without rerunning them. [Review report](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/ring-missing-links-20261102/post-deployment-review/independent-review.md>) · [Current review packet](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/ring-missing-links-20261102/post-deployment-review/review-receipt.json>). The original pre-deployment packet remains historical: its task document was later amended; the fresh frozen post-deployment packet validates the current evidence.

[EPIC FAIL complaint — rings stopped and new-show retention incomplete](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recEkiFNgkO29DWap) remains Open. The ring-processing repair is Verified, not blanket closure of the complaint. Rings first stopped November 2 at 1:52:37 a.m. Eastern; documented coordinator awareness began at 8:36:35 a.m. Recovery/native preservation was verified at 2:54:36 p.m., after 13 hours 1 minute of outage (781 whole minutes). Prior stop/awareness measurements and owner-reported communication concerns remain in the complaint history.

Remaining limits: missing upstream show/day memberships are withheld; complete new-show membership is unverified. One natural hourly recovery run is verified; no second natural hourly repeat is claimed. Historical complaints are not automatically closed by this repair. No Data changes, manual collectors or additional runtime changes were made for this review/documentation update.

### New-show alerts from Records

On a new-show alert from wef-records-coordinator, promptly acknowledge the canonical show/day context and check existing scheduled Season results. Treat the alert as notice that outgoing Data may soon be removed: prioritize retaining available trainers, riders, horses and rings through the existing hourly Season workflow. Add, upsert and deduplicate using existing keys and supported show/show-day links, preserving prior records, memberships, associations and ring nicknames. Do not wait for a completed-day sequence or introduce a new readiness gate.

The alert does not itself run Season, change its hourly cadence, authorize a manual collector or bypass a failure stop. Report any affected base and unverified retention promptly; receipt of the alert is not proof that transfer finished. Season does not control Data deletion or certify that Data can safely delete records.

Latest received context: **15|200000077 — 2026 ESP November (#7129), planning day 0 November 5, 2026**. Planning day 0 is not calendar TODAY. Supported ring facts are retained after the verified recovery above; unsupported show/day memberships remain withheld. The pre-repair ring stop is preserved in [failure evidence](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/ring-missing-links-20261102/ring-stop-before.json>).

The existing Windows task **wef-season-collector runs every hour (PT1H)** through run.ps1 → entry.js → season.py. It reads current Data business records from appdl16HJ5ETm8N69 and uses the existing native upsert/readback code. It leaves unchanged records alone. Directory links establish show registration; single trip roles supply supported day memberships. Existing historical memberships remain additive.

No completed-day sequence, receipt/cache revision, canonical freshness/fence or heartbeat checkpoint is required to run entity upserts. Existing key/identity safety, uncertain-write reconciliation, overlap protection, failure boundaries and heartbeat/run/error logging remain. No manual collector substitutes are allowed. Preserve the existing task definition and approved cadence; do not create another timer.

## Horse associations and conflicts

The approved release added two multiline-text fields in horses:

- rider-associations (fldtUh5WtRHYsDjq6)
- trainer-associations (fldSo6L3GZj5joi40)

Each JSON map uses the unchanged rider/trainer key with shows, show-days and sources arrays containing existing keys. Multiple riders and different trainers across trips/shows are valid. No bindings table, copied people tables, new keys or new syncs were introduced.

The new conflicts table tblZlDewVNlXmPRqS in the horse base uses source-key, source-type, horse-key, details and status. Ambiguous/changed assignments on the same source are logged there; only the conflicting assignment is withheld and other work continues. Missing roles are not guessed. Repeat conflicts reuse their existing source record rather than creating duplicates. This release changed the approved horse schema; it did not change existing keys/IDs or Data.

## Deployed files and verification

- season.py SHA256: e1f931f7c71f7bf46d7f66fdd6c4a2a9447a29b988c05e9d3b17fe15aa0cde1f
- horse_associations.py SHA256: 7732f106d7bb98139d297876a2a5e42eeacd3e24fca880e8a850c96f641c45c1
- schema.json SHA256: b314638055c3dea3c02e80a652ef787ef3cead2862f76af6cd4c50df1f4b1944

Independent review and 21 targeted checks passed before deployment on November1 at08:37Eastern. This is separate from scheduled proof:

- **08:51 run:** all four bases succeeded; 897horse association records populated. Native audit found 295horses with312rider associations (17multi-rider horses),897trainer associations and6logged conflicts. All existing records, IDs, keys, memberships and nicknames were preserved.
- **09:51 repeat:** all four succeeded; 896horse association records unchanged,one updated only with new trip evidence. Rider/trainer pair totals stayed312/897; the same6conflict IDs/contents remained, with no duplicates or lost prior associations/records.
- **Historical November 1 run,11:51:15:** all four succeeded, completing11:52:22Eastern, task exit0. Associations:0added,896unchanged,1updated;6conflicts. This is saved scheduled evidence, not a fresh full native audit.

[Release and initial native proof](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/horse-associations-20261101/final-verification.json>) · [09:51 scheduled receipt](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-season/runtime/logging/wef-season-horses/run-logs/0272036b-b8ce-4d9a-8b38-d7a5a6c64f20.json>) · [Preserved latest scheduled result](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/readme-refresh-20261101/latest-scheduled-result.json>).

## Existing logs, boundaries and guidance

Runtime/last-run.json is the latest outcome; runtime/logging/wef-season-{entity}/run-logs contains per-attempt evidence. Each base retains heartbeat,run-logs and err-logs. Association conflicts are business records, not a reason to stop unrelated updates. New actual workflow/verification failures remain subject to owner edit/retry/resumption rules; preserving those rules does not authorize a repair or task change.

Historical November 1 coordination guidance was Operating Skills0.2.8; [complete read proof](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/coordination-rules-20261101/coordinators/records/recipients/wef-season/load-receipt.json>). Loading is not a business run, review, compaction or guarantee. Status answers begin Yes/No; ifNo, continue approved work or state the exact blocker.

This is a documentation refresh only. Runtime code/schema/task/records are unchanged by it. The former README, including superseded Data ownership, five-minute cadence and completed-day instructions, is preserved as [historical reference](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/readme-refresh-20261101/module-before-1.1.1.md>), not current operating instructions. No new implementation is pending from this documentation update.
