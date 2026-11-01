# Day-0 membership change — November 1, 2026

Owner approved editing after the day-0-only workflow: 25 source groups / 43 group-class appearances, including the two repeated class IDs; no status gates, no deletion, preserve links/logs, stages 8/9 held. Existing three-minute 05:00–19:00 Eastern task unchanged. No manual source/collector runs or repairs.

Verified Data rule: classes/schedules Active when present, Absent after one complete missing observation, Dropped after the next, Active on return. Failed/incomplete inventory does not advance absence. Source competition status is separate. Proof: wef-data/data_runner.py missing_rows and Data chat 01a19273-bdf8-71b2-8503-cc8079efc129 owner-approved states, normal 12:09/12:12 runs. Data's extra Active — no entries is not added to Live under this approval.

Baseline: current wef-live installed 21 JS files frozen in baseline.json. Other collectors, key identities/native links, dependency versions, task, publication/cleanup stops protected.

Necessary storage decision: Live has no record-status fields and no per-group class-appearance table; current live-classes are unique class details with existing entry/schedule links. Do not rekey or duplicate those details. Proposed smallest schema prerequisite: record-status on live-groups, plus live-class-appearances containing appearance-key, show-id, date, group-id, class-id, record-status, last-seen and optional existing group/class links. Approval for this schema prerequisite must precede creating it or integrating writes. Existing details remain 41 unique; appearance ledger contains 43 current records. No migration/deletion or one-off repair.

Independent authorized preparation: frozen candidate without live-status/Completed/detail-availability collection gates; day selection uses canonical Shows day-priority 0; pure recurrence transition and 25/43 fixture tests. This candidate is NOT deployed and schema-dependent wiring is NOT implemented. Normal scheduled verification waits for approved complete integration and release review.

## Schema and delivery approval received
Owner explicitly approved record-status and live-class-appearances, preserving records/links, day0 only, stages8/9held, no gates/manualruns/repairs; test, deploy and verify two normal scheduled runs. Supersedes pending schema decision above. Implementation integrates pure rule into existing Groups lane; no rekey or duplicate class details.
