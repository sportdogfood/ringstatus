# Shows count replay recovery — 2026-10-30

Owner-approved scope: fix count-replay timeout, restore show-days publication,
preserve count replay, archives and workflows; verify two normal scheduled runs.
No manual collectors, source calls or record repairs.

## Finding and change

The saved stop records `Count replay timed out` at 2026-10-29T23:20:38.852Z.
Subsequent discovery continued but the day publisher correctly stayed stopped.
The original 45-second timeout did not reproduce on retained-input diagnostics.
Profiling did identify duplicate decoding of the same frozen observations during
chain verification and count verification. The Shows bridge now reuses exact-byte
decoded observations within one isolated invocation. Every reference, hash, chain,
identity and count validation still executes. No accepted counts or freshness are
cached across runs. The bounded replay budget is 90 seconds; the existing task
limit and five-minute cadence are unchanged.

The latest retained chain also contains 45 archive-backed references. Shows now
reads their existing local Data archive objects, enforcing show-specific paths,
compressed length/hash, payload hash and the original downstream replay checks.
It does not publish, fetch, repair or delete archives. Missing/corrupt evidence
fails closed. No Data, Season, schedules or tenant code was changed.

Deployment was in the established scheduled-task checkout while the task was idle.
The owner-authorized publication stop was removed only after offline checks; its
original contents remain in `before/show-days-stopped.json`.

## Evidence

- `full-benchmark.json`: original full-chain replay 9.989 seconds, optimized
  7.005 seconds, exact equal outputs for four days. Diagnostic only.
- `staged-full-proof.json`: all 45 archived references match Data's retained
  evidence; full replay succeeds. Diagnostic only.
- `staged-tests.tap` and `deployed-tests.tap`: 48 tests passed, including altered
  evidence rejection, archive boundaries, preserved publication and stop behavior.
- `deployment.json`: deployment time and files.
- `preservation.json`: task definition unchanged; only existing `day-source.js`
  and `day-replay.py` changed, plus new read-only `day-archive.js` and tests.
- `normal-*.json`: GET-only readback of actual scheduled receipts, heartbeat,
  run logs, source links, all four day records and archive hashes.

## Verification boundary

First normal run: 2026-10-30 08:39:02–08:39:25 Eastern, Success,
`d3bdc4ea-64da-43e9-aea1-2c776e748149`.
Second normal run: 2026-10-30 08:44:04–08:45:51 Eastern, Success,
`e899a0c3-b9a0-4ad4-90a4-51028448b408`. This exercised count replay and
published all four accepted day summaries. The unchanged `verify-days.js`
independently replayed their exact Data revision and confirmed equality.
Third observed normal run (second exercising count replay): 2026-10-30
08:49:04–08:50:01 Eastern, Success,
`b2fbcfba-e35b-4f66-8b61-fbbe6b177fe1`. Four counts published again with matching
native IDs, links and digests; independent `verify-days.js` readback passed.
Both actual replay runs have task exit 0, successful heartbeat/run logs, zero
error records and four verified archive files each. The 08:39 run is not counted
toward the two-replay acceptance.

At 08:39, Shows selected October 30 while Data's accepted authority selected
October 29. That run published Unknown while preserving accepted summaries,
counts and links; the context guard returned before replay.

Data subsequently accepted October 30 in revision
`adaptive-2026-10-30T12:38:17.247751+00:00`. The 08:44 run consumed that revision
normally and published counts. Source observations are older than five minutes,
so all four days correctly remain Stale. October 30 has 178 unique entries and
212 proven trips within competition-class-participation coverage. No stale count
is a fresh zero-readiness signal, and schooling/classless coverage is still unproved.

## Release lock and handoff

**COMPLETE / LOCKED**, 2026-10-30. Independent review found no actionable defects;
see [code-review.md](code-review.md). The exact deployed Shows runtime hashes are
recorded in [deployed-manifest.json](deployed-manifest.json). Scope remains v1.4.
No task, cadence, Data, Season, Schedules, Entries, Live or tenant changes were made.
No manual collector/provider run or Airtable record repair was performed.

Records can independently confirm the two actual replay-run receipts above and
the matching `../show-days-build-20261029/normal-<run-id>.json` replay readbacks.
This file records the handoff evidence; it does not claim Records has acknowledged
it. No further implementation work is authorized by this release. New changes
require applicable owner authorization. Existing failure stops remain in force.
