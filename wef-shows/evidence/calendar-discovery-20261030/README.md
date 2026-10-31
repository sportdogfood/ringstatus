# Calendar selection and weekly discovery - October 30, 2026

Status: DEPLOYED October 30 at 19:03:32 Eastern; normal scheduled acceptance in progress. All 16 target hashes verified. Owner approved calendar day selection and weekly three-hour discovery; source timestamps, counts, archives, five-minute task and existing failure stops remain protected.

## Intended behavior

- First normal five-minute wake at/after 19:00 America/New_York advances selected_date to today's date + one calendar day only if within the show range and later than the retained selection. Repeated wakes cannot advance it again. Before 19:00, and when tomorrow is outside the range, retain selection.
- Sunday 22:00 Eastern starts a new-show cycle. The existing ring/default-schedule discovery pair repeats every three elapsed hours from the attempt until show ID changes. Completion stops this cycle until the following Sunday. An unfinished cycle continues past week boundaries; restarts retain baseline and next attempt. Initial weekday activation waits for the next Sunday, rather than backfilling last Sunday's cycle.
- Source-reported date and source last_checked remain separate from selected_date and authority_checked_at. Consumers validate the latter with their existing 30-minute threshold; original source time must still be valid and non-future. All unrelated freshness and TODAY safeguards remain.
- Every normal wake revalidates canonical identity, raw source archive/hash and selection evidence before updating authority_checked_at. Failed discovery does not refresh authority; its persisted next-attempt checkpoint prevents five-minute source retries.
- Count publication remains stopped by its pre-existing failure file. This release does not repair/retry it.

## Actual work and evidence

Isolated candidate: ../../staged/calendar-discovery-20261030. Frozen file paths and hashes: deployment-manifest.json. Before files preserve the deployed baseline; prepared-release contains the candidate. Deployment used existing idle-boundary route; no task changes, manual source/collector calls or business-record repairs.

Four additive Shows schema fields created and read back: source_selected_date, selection_basis, selection_at, authority_checked_at (schema.json). They are populated only by normal deployed collector runs. Data's exact field-ID mapping is included in the candidate.

42 offline Shows/authority/integration tests pass (tests-final.tap). One added test initially expected Timing purge permission for tomorrow; the implementation correctly withheld purge as not-TODAY. The test expectation was corrected, preserving the guard (tests.tap records the original fixture expectation). Original 41 tests initially passed (tests-initial.tap). Review identified source timestamp validation needing preservation; candidate retains it with explicit tests. Live/Entries/Timing actual reader functions and Data production mapping are tested offline; this is not normal-run proof.

## Accepted downstream decision

Owner explicitly selected Keep Live and Timing on TODAY. Live derives its working date from America/New_York today within canonical show dates; Shows selected_date remains independently validated. Timing keeps the untouched canonical selected-date in the Data pointer/fence, while reading synced groups and calculating ring/class answers for TODAY. The existing Timing purge guard still refuses future-day retirement. Live carries canonical selected_date separately so retirement retains its original future-selection protection while observations use TODAY. Both active-show and upcoming-show outgoing-TODAY protections have direct retirement tests.

Candidate expanded to 16 files. New discovery-policy.js is installed before companion readers; old discovery-window.js stays available for the prior collector until the final Shows switch. The new TODAY integration test executes the real Timing function twice with tomorrow's canonical fence and today's inventories/classes; no manual production runs. All 47 offline tests pass (tests-today-final.tap); 16 unchanged Class adapter regression tests also pass (class-adapter-tests.tap). Independent frozen re-review PASS: independent-today-final-review.md; 47 independently executed tests and 30 baseline/candidate cleanup eligibility comparisons passed.

## Remaining verification

Independent candidate review and deployment passed. Normal scheduled readback is in progress. Sunday and three-hour behavior cannot be verified by a manual run. No extra observer or timer is introduced.

## Normal-run result and stop

19:04 run441b82ca-47aa-4358-9ee4-f37ba4883af1 advanced selected_date to October31 and retained source dateOctober30/source last_checked22:59:14.976Z; zero discoveryrequests. Airtable record/heartbeat/log readback and archive hashes PASS (readback-1.json and normal-441b82ca-47aa-4358-9ee4-f37ba4883af1.json). FullrunPartial remains the existing count stop.

19:09 scheduled wake exited1: Schedule collector already running. Existing collector.lock ownerPID7100 was observed as live node process started19:08:45. Evidence normal-1909-lock-failure.txt. Repeat proof is BLOCKED; no lock removal, manualrun, retry or patch. No claim is made about who launched that process. Downstream coordination remains handed to Timing/Records.
