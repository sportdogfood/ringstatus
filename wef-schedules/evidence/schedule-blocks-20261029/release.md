# Schooling and break schedule release - October 29, 2026

Owner approved inclusion of schooling sessions and breaks and corrected empty-day reporting. Existing class links/workflows and normal cadence were protected; no manual runs or record repairs.

## Result: PASS

- 139 offline tests passed; five new regressions failed before implementation and passed after it.
- Saved-source replay preserved all 471 existing class rows/keys exactly.
- Source block keys use block:<class_group_id> under the unchanged show/date stage-key. Class IDs/numbers remain blank. Existing type/group/time/trip columns display source details; no schema changes.
- True empty-day wording now covers classes, schooling sessions and breaks. Schooling-only dates do not warn as empty.
- October 29: 24 schooling blocks, 2 breaks, 0 competition classes, and 0 source-reported entries/trips. Open-schooling attendance is unmeasured. The top-level source count 26 is not an attendance count.
- 33 blocks added across the four show dates; 504 schedules total. All 471 prior class record IDs and show/ring links preserved. All 504 IDs stable on repeat. Normal October 30 source changes updated two records in the second run without replacing IDs.
- Existing archive-before-delete, failed-fetch/destructive-empty protections, manual override guards and task XML remain unchanged. Eight first-run and six second-run archive files were decompressed and matched their logged hashes.

| Scheduled trigger (Eastern) | Terminal completion (Eastern) | Run ID | Result |
| --- | --- | --- | --- |
| 08:30 | 08:31:07.116 | bcdf4cbd-572e-47ee-bf06-b0f0753fbd35 | Success |
| 08:35 | 08:36:04.950 | 9d6f6acb-c49e-4d0a-8d95-7d6bc3ef5dc6 | Success |

Both terminal Airtable logs and latest heartbeat were read back; zero errors, four complete dates and zero failed dates. Wrappers ended 08:31:13 and 08:36:10 with exit 0. October 29 reused the dataset on the second run with no adds/changes/drops. Existing CWF normal source datasets were read only and show zero October 29 entries for all configured trainer/user feeds.

Evidence is in this directory: deployment.json, offline-tests.json, offline-source-comparison.json, review.diff, normal-run-1.json, normal-run-2.json, archives-run-1.json, archives-run-2.json, wrapper-exits.json, task-before.xml, task-after.xml, today-entry-counts.md, schedules-before.json and schedules-latest.json. Private snapshots are local evidence, not product data repair.

Editable source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules

Deployment: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules

Preserved limitations: no new participant/horse attendance collection for open schooling was introduced, and no unobserved future run or show rollover is claimed.
