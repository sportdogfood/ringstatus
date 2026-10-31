# Independent count-publication resume review

Reviewer: /root/shows_review. Applied Code Review 0.2.2. Exact review time,
installed candidate/dependency hashes and comparison snapshot are in
review-receipt.json and review-hash-manifest.json. No runtime edits or rollout
actions were performed. This reviews reuse of the already installed saved-day
implementation, not a newly authored timeout patch.

## Findings and decision

No actionable blocker found in the inspected activation and saved-day count path.
Code review PASS. Removing only the identified October 30 count-publication stop
at an idle boundary is technically supported under the parent's supplied new
human repair authorization, after preserving its exact bytes and checking it has
not changed. This report grants no authorization itself. Do not clear other stops,
locks or task state; do not copy an older staged implementation over current work.

## Actual integration inspected

collector.js retains the existing stop gate and calls publishDays through the
audited reader. Failure recreates/preserves that gate. The independently approved
priority phase remains separate; the existing created-day hook only labels newly
created canonical rows. No additional runtime patch is indicated by this review.

day-source.js selects saved-day mode when Data's day-heads directory exists and no
explicit legacy head is requested. day-replay.py calls saved_day_summaries rather
than accepted_summaries/full predecessor replay. Missing saved heads return no
summary; they do not fall back to old full-chain collection. The old 90-second
subprocess timeout remains, and fixture performance does not prove real runtime
will stay below it.

Data's save_day writes a content-addressed receipt before atomically moving its
day pointer, and guards sequence regression/conflicting same-sequence saves.
finish checks current canonical context and retained units before saving Complete
state. The consumer checks receipt path containment/hash/format, show/day,
Complete/pending/day-work state and show-range authority. It verifies compressed
evidence length/hash, decoded payload hashes and reference provenance, then
recomputes totals from dated inventory and class units. No provider or archive
publisher is invoked by this reader. It does not traverse predecessors.

Only selected date through +3 within canonical dates is eligible. The original
source accepted-as-of and saved completed timestamp are retained. Publication
still validates identity/coverage, stages Unknown, checks native links/projection,
then accepts the envelope and reads it back. Five-minute source age remains Stale;
replaying does not make old evidence fresh. Unknown inventory is not zero.

Saved-day omissions preserve existing rows and totals without writes; absent
canonical rows may be established Unknown by existing publisher behavior. Thus
October 29/30 may remain retained rather than refreshed. Retained row UI status is
not newly verified freshness. An all-skipped run is not proof counts were restored.

## Independently executed offline checks

- 17 source/publisher/replay tests PASS against exact copied current Shows files
  in review-isolated, with no Data runtime directory. Python receives only frozen
  legacy fixture inputs; no real saved-day counts were executed.
- saved-fixture-test.py PASS independently: temporary saved receipt and compressed
  fixture evidence reproduce expected totals/source age, repeat identically and
  reject modified receipt/evidence. This is the supplied fixture test executed by
  this reviewer, not an assertion based on the writer's output.
- review-integration.cjs PASS: actual saved reader dispatch avoids legacy API
  reads; actual publisher preserves omitted existing rows byte-for-byte; five-minute
  status remains Stale. APIs are mocks.

Exact commands, environment/runtime, actual times, exit codes and separate output
streams are in review-command-log.json. Tool evidence: ac7597 (17 tests), 2b4a06
(saved fixture); integration output is saved separately. Source/code inspection
is distinct from those independent executions.

## Acceptance still required

Two normal scheduled runs must actually publish eligible saved-day counts with
native IDs/links and source timestamps read back, no replay timeout, heartbeat and
run-log success, and no new stop. Preserve the old stop as evidence. Live timings,
production receipt contents/counts, concurrent publication behavior and the two
normal runs are UNVERIFIED by this offline review. Stop the affected path on any
new failure; do not substitute manual runs or repairs. Source freshness and retained
past-day coverage must be reported separately from successful publication.

Review COMPLETE/PASS is bounded to this frozen installed path. The receipt
verifier checks evidence integrity only, not production readiness.
