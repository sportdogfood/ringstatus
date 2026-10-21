# WEF schedules heartbeat — 20 October 2026

Owner approved adding heartbeat to wef-schedules, testing/deployment and two
normal scheduled runs. No manual runs or record repairs. Existing workflows,
source selection, schedule/day collection, archive-before-delete and task
cadence remain protected.

## Baseline and delta

Working source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules.
Deployed task package: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules.
Source collect.js and run-logs.js matched exact pre-edit deployed hashes.
Repository HEAD at startup: 1e357b5315479c2fbebaf031b5a7da2e4a630698.
Existing uncommitted/untracked releases were preserved; they are the operational
baseline, not replaced with HEAD. Evidence: wef-schedules/evidence/heartbeat-20261020/.

Added heartbeat table tblj077efYbhlznOJ in appUq5Qz9kARqFija. Metadata-only
setup wrote no business records. The normal collector creates the first status
row. One stable heartbeat-key=schedules row updates after the terminal run-log
is saved and read back. It carries checked-at, run-id, started, selected-date,
show-id, result, rows, days-complete, days-failed, archive-count, summary, errors.

Only the collector entry point and run-log completion gain the status hook;
heartbeat.js implements the small projection. The shared existing collection,
transport, discovery, normalization, retention and archive modules are untouched.
The hook copies verified run-log facts, including Warning and Failed outcomes.
Source-selected date comes from the existing validated show/ring result.
An early discovery failure clears scope instead of presenting stale show data.

Creation POST is not retried; uncertain responses are reconciled by stable key
and full readback. Ambiguous status rows fail without repair. Status write errors
are recorded on the owning run-log and fail the process. If the collector never
starts, or run-log storage itself fails, the heartbeat can remain stale: inspect
checked-at against the existing five-minute cadence. No new trigger or monitor.

## Verification

102 deployed offline schedules tests PASS. 18 focused tests also PASS against
the existing source worktree. Tests cover repeated same-row update, source date,
failed/early run scope, blank fields, warning propagation, duplicate protection,
lost creation response, corrupt readback and status-write failure in run-logs.
Existing full-suite collection/archive guards passed. No live test data created.

Deployment completed between ordinary 14:00 and 14:05 triggers, after the
unchanged 14:00 run finished successfully. Source and deployment changes match.
First ordinary 14:05:01 task start exited 0, run
6867ae7a-6a76-44b3-a3ee-c006269cbf52 completed 14:05:57 Eastern.
Heartbeat recrPBYzI2s6XpW0O matches the terminal run-log: Warning, 435 rows,
four completed days, source-selected October 22. The Warning is the existing
empty-October-22 source response; no failed days or errors. Four archive paths
and SHA256 hashes verified. All 435 schedule IDs/values, 24 rings and six static
helpers match the pre-deployment baseline. Second ordinary 14:10:01 task start exited 0, run
94059842-a96a-42e6-887b-5aabe6039704 completed 14:10:45 Eastern. It updated
the same heartbeat record, preserved 435 schedules, 24 rings, six helpers and
four collection record IDs/datasets, and added exactly one new run-log. Eight
archive events were read back and SHA256 verified across both normal runs.
No earlier run-log changed. Heartbeat content matches each owning terminal log.

The first local repeat-comparison assertion incorrectly assumed collection-id
stays fixed. Existing current-store.js creates a new attempt ID on every normal
run while updating the same per-day Airtable record. The comparison was corrected
to verify retained record IDs, scope, dataset IDs, fingerprints and counts, plus
expected rotating attempt IDs. Both collector runs succeeded; no collector code
or live data was changed in response to that verification-only assumption.

Acceptance PASS. All existing table schemas were preserved; only heartbeat was
added. Task definitions, tenant code and protected schedules modules are unchanged.
Exact final runtime hashes match deployment. Failure/uncertain-write behavior
remains fixture-proven; no production failures or duplicates were manufactured.
No manual collector/source calls.
