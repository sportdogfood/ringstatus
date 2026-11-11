# WEF Shows Cleanup Contract

Version 1.5. Updated November 11, 2026. Implementation approved and deployed; normal eligibility checks verified.

## Authority and scope

The owner supplied the full common policy through rs-skills on November 11, 2026. That exact policy controls Shows base `appCaSmL819KK7UFJ`. The example table supplies the inventory/latest-outcome/history layout. Existing Shows code is dated implementation evidence and cannot create policy exceptions.

## Fixed common policy

1. When a checked logging table contains 1,000 or more total records, delete its oldest eligible records until 500 total records remain. The deletion quantity is the actual starting count minus 500, not a fixed batch of 500. For example, if the check finds 1,400 records, delete 900 eligible records to leave 500. Preserve active/pending records, reference tables and cleanup history. If too few records are eligible, preserve protected records and report the actual remaining count and reason. Do not stop at an arbitrary batch size.
2. At **7 p.m. America/New_York**, clear completed eligible logs.
3. On a **new show**, clear eligible owned show records and logs.
4. Protect **active/pending records, reference tables and cleanup history**.
5. **No archives.** Cleanup creates no new archives. Existing historical archive files are outside this cleanup assignment.

A show-day or priority change alone is not a new-show event. Preserve existing approvals and stops. The owner approved implementation, tests and verification through normal scheduled runs on November 11. Manual runs remain prohibited.

## Approved Shows table mapping

| Shows table | New-show cleanup | Daily 7 p.m. clear | 1,000-to-500 cleanup |
| --- | --- | --- | --- |
| shows | Eligible previous-show owned records; current default protected | Excluded | Excluded |
| show-days | Eligible previous-show owned days; current-show days protected | Excluded | Excluded |
| heartbeat | Eligible obsolete previous-show logs; active/current heartbeat protected | Completed eligible historical logs only | Included if checked and threshold reached; active/current record protected |
| run-logs | Eligible previous-show logs | Completed eligible logs | Oldest eligible logs |
| err-logs | Eligible previous-show error logs | Completed eligible logs | Oldest eligible logs |

Shows' `err-logs` maps to the example's `errs-log`. Error records are not permanently exempt merely because they are errors. Do not add a separate resolved-error approval requirement. Protect active/pending records and their current work under the common rule.

Retain `sources`, `code-reviews`, `meta`, `readme`, `cleanup` and `cleanup-example` as reference, administrative or control data. Retain `decoder-readme`, `decode-change-log`, `health-table`, `token-refresh-log`, `warning-log`, `decoder-run-log` and `decoder-fail-alerts` as other-owner reference/evidence data. Their location in Shows does not establish Shows cleanup ownership.

`show-times` stores source-time observations linked to shows/days. Its cleanup ownership and reference classification remain unconfirmed; identify its owner before adding it to Shows cleanup selection. The `cleanup-logs` table is permanent history, excluded from every cleanup rule. The owner approved this five-table mapping on November 11. Mapping must preserve applicable native-link and downstream contracts without inventing a different retention rule.

## Cleanup inventory and evidence

The existing [cleanup table](https://airtable.com/appCaSmL819KK7UFJ/tblv8o1Wu9rWX29s0) was prepared and read back November 11 at approximately 8:12 a.m. Eastern. It has 19 inventory rows and 14 fields. Table counts are snapshots observed around 8:11 a.m. Eastern. Existing run-log and error-log row IDs were preserved. Selections and action-result fields were blank at verification; setup did not execute or enable cleanup.

`records` means the actual table count at action start, never the threshold or target. Latest action results belong in cleanup. Permanent cleanup-logs must record trigger/event ID, run ID, start/completion time, before/after counts, actual deletions, protected count, outcome and errors. No new archive evidence is generated. Existing archive-related fields and historical evidence remain, without being populated with invented new archive results.

Reference layout was verified in [cleanup-example control](https://airtable.com/appCaSmL819KK7UFJ/tblEX3gB6HvfOWqkp/recVbeq3FKNfvWYb5) and [permanent history](https://airtable.com/appCaSmL819KK7UFJ/tblEX3gB6HvfOWqkp/recMOVxzPcQsNuqiD). Sample selections, names, run IDs and results are not copied. Cleanup history is protected under all three triggers.

## Dated runtime gap — inspected November 10–11

Current [collector.js](collector.js) lines 16–23 archives one eligible old run-log before slot reuse at a soft 1,000-row threshold. Current [airtable.js](airtable.js) lacks cleanup-table access and DELETE permission. This path does not implement the common daily clear, new-show deletion, 1,000-to-500 deletion or permanent cleanup history.

The approved cleanup release adapts that old retention path to all three triggers and the no-new-archives cleanup rule. Existing non-cleanup source/discovery archives remain outside this cleanup change. Do not remove historical archive files or treat archive verification as a prerequisite for the proposed cleanup.

## Implementation route and remaining work

[CLEANUP-PROPOSAL.md](CLEANUP-PROPOSAL.md) v1.3 records the approved mapping and route. The available existing route is `wef-shows-collector` through run-hidden.vbs, run.ps1 and entry.js. Its normal five-minute invocations will observe a verified show-ID change, perform one daily clear at the first eligible normal invocation at or after 7 p.m. Eastern, and check the log threshold. Activation and normal-run proof are recorded separately in the implementation evidence. Large actions pause only after confirmed batches before the existing task deadline and continue toward the same target on later normal wakes. Failed or uncertain deletions never resume automatically.

The implementation adds scoped cleanup access/deletions, permanent cleanup-logs, event/daily duplicate protection and uncertain-action reconciliation. No separate timer or manual execution is proposed. Confirm show-times ownership separately. The fixed policy has no remaining archive or 7 p.m. decision.

## Acceptance and approval

Owner approval for the table mapping and implementation was received November 11. Review the approved delta and test all three triggers, threshold versus retained count, protections, no-new-archives behavior, permanent history and repeat/uncertain execution.

Verify through genuine normal workflow events. Read back event/run identity, exact affected/protected records, before/after counts, outcomes and latest/permanent history. Reconcile concurrent writes, preserve active/pending/reference/history records and applicable links, and leave unobserved cases unverified. Preserve existing failed-workflow stops. No fabricated events, manual runs or record repairs.

Current acceptance includes the prepared inventory, created permanent cleanup-logs table `tbl7jjcBsKr8fVQOl`, five verified selected targets and 59 passing offline checks. Reviewed code activation was verified at 1:39 p.m. Eastern on November 11. Normal scheduled checks at 1:49 and 1:54 p.m. Eastern completed successfully with matching Airtable run-logs and heartbeat updates. Four show records and 13 show-days retained their IDs, links and priorities. No deletion was due; actual daily-clear, threshold and new-show deletion remain unverified until genuine events. Evidence is in `.git/ringstatus-control/wef-shows-cleanup-20261111/evidence/`.
