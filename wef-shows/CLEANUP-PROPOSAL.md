# WEF Shows Cleanup Implementation Proposal

Version 1.3. Updated November 11, 2026. Implementation approved and deployed; normal eligibility checks verified.

## Fixed policy supplied by the owner

The full owner policy delivered through rs-skills controls this proposal. [CLEANUP-CONTRACT.md](CLEANUP-CONTRACT.md) v1.4 is the current contract. This document replaces the earlier two-rule proposal and its unresolved archive/daily-clear language.

1. When a checked logging table contains 1,000 or more total records, delete its oldest eligible records until 500 total records remain. The deletion quantity is the actual starting count minus 500, not a fixed batch of 500. For example, if the check finds 1,400 records, delete 900 eligible records to leave 500. Preserve active/pending records, reference tables and cleanup history. If too few records are eligible, preserve protected records and report the actual remaining count and reason. Do not stop at an arbitrary batch size.
2. At **7 p.m. America/New_York**, clear completed eligible logs.
3. On a **new show**, clear eligible owned show records and logs.
4. Protect **active/pending records, reference tables and cleanup history**.
5. **No archives.** Cleanup creates no new archives. Existing historical archive files are outside this cleanup assignment.

These are fixed common rules. Shows' old archive-and-slot-reuse code must be adapted to them after approval. It does not justify a different cleanup policy. No archive-before-delete prerequisite or separate resolved-error approval gate is proposed.

## Shows table mapping

Propose checking `shows`, `show-days`, `heartbeat`, `run-logs` and `err-logs` in Shows' existing cleanup inventory.

- New show clears eligible previous-show owned records and logs in those tables while protecting the active/current show/day context and active/pending records.
- Daily 7 p.m. clear applies to completed eligible `heartbeat`, `run-logs` and `err-logs`.
- Threshold cleanup applies only to those three checked logging tables. At 1,400 total rows, delete 900 eligible records to leave 500. Report the actual remaining count and reason if protections prevent reaching the target.
- Errors participate as eligible logs under the same protections. The existing single current heartbeat remains protected.
- Retain sources, code reviews, meta, readme, cleanup and cleanup-example as reference/administrative/control data. Retain the decoder-owned documentation, health, token-refresh, warning, decoder-run and decoder-failure evidence tables. Permanent cleanup-logs is excluded from every rule.
- Confirm `show-times` ownership and reference classification before including it. This is a table-ownership question, not permission to vary the fixed rules.

The full table mapping and exact reference records are in the contract. Preserve applicable native-link/downstream contracts when implementing that mapping. Do not add other policy exceptions or prerequisites.

## Existing inventory and implementation changes required

The existing cleanup table `tblv8o1Wu9rWX29s0` already has 19 inventory rows, purposes, dated count snapshots and 14 fields. Runtime selections and results remain blank pending implementation approval. `cleanup.records` is the actual count at action start.

After approval:

1. Adapt the current archive-and-slot-reuse retention step to the three fixed cleanup triggers, with no new cleanup archives. Preserve historical archives and existing non-cleanup discovery/source archives.
2. Add narrowly scoped access to the approved cleanup targets and cleanup table. Add permanent cleanup-logs with action/event/run identity, trigger, timestamps, before/after counts, actual deleted/protected counts, outcomes and errors. Preserve that history.
3. Use the existing registered Shows runner route. Propose handling verified new-show publication, once-per-Eastern-date daily clear at the first normal invocation at or after 7 p.m., and log-threshold checks through existing normal invocations. No new task, timer or manual cleanup.
4. Record action intent and reconcile actual outcomes before further writes after uncertainty. Avoid duplicate daily/new-show actions. Preserve applicable existing stops and failure handling.
5. Update latest results in cleanup and append actual history to cleanup-logs. Leave archive fields unused for new cleanup actions. Do not invent execution proof from table setup or reminder delivery.

These are implementation gaps. The current code does not yet provide this common cleanup behavior. Shows owns the adaptation; the registered runner executes it after approval. The weekday reminder itself does not execute cleanup.

## Verification required

Review the exact approved change. Offline checks must cover all three triggers, 999/1,000/1,200 row boundaries, oldest-first deletion, 500 total remaining versus actual removals, protected active/pending/reference/history records, no new archives, uncertain writes and repeat-event/daily duplicate protection.

Verify through genuine normal workflow events. Record and read back exact affected/protected IDs, before/after counts, event/run identity, trigger/timestamps, outcomes/errors, cleanup latest results and permanent history. Account for concurrent inserts and surviving links. Do not manufacture rows/events, manually invoke cleanup, repair records or clear a failed-workflow stop to obtain proof. Naturally absent cases remain unverified.

## Owner approval received

The owner approved mapping the five Shows-owned tables to the full common policy and implementation through the existing scheduled runner, including permanent cleanup history, tests and normal-run verification, on November 11. Confirm show-times ownership before any selection of that table. The reviewed code was activated at 1:39 p.m. Eastern, with no task or schedule change. Large cleanups continue confirmed batches across normal wakes to respect the existing task deadline. Normal scheduled checks at 1:49 and 1:54 p.m. Eastern are verified. Actual deletion events remain unverified until genuine triggers.
