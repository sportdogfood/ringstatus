# Independent calendar-discovery candidate review — 2026-10-30

Reviewer: Shows independent review agent. Read-only implementation review under
the Code Review skill, Shows AGENTS.md and WEF common review standard. Only this
report and offline test/hash logs were written. No collector, provider call,
production edit, record repair, task change or stop clearing was performed.

## Findings and decision gates

**P1 CONFIRMED — deployment decision required: next-day selection stops TODAY consumers.**
Candidate `wef-shows/discovery-window.js`, `calendarSelection`, advances
`selected_date` to tomorrow at the first scheduled wake after 19:00 Eastern when
tomorrow is in the show range. Existing `wef-live/collector.js:93` takes its
non-active branch and does not collect Live when that date differs from TODAY;
`wef-timing/data-runner.js:90` defers with `Selected show date is not today`.
Thus this candidate introduces an evening Live/Timing pause until midnight,
including while today's competition may continue. Parent independently identified
the same conflict. Smallest next action is the owner's explicit choice: accept
that pause or approve independent TODAY selection for those consumers. Do not
remove their safety guards or deploy this candidate on an inferred decision.

**P2 CONFIRMED, corrected in candidate — source-time validation bypass.**
The initial `authority-age.js/showAuthorityReason` accepted missing, malformed
and far-future `last_checked` whenever `authority_checked_at` was fresh. Actual
Live, Entries and Timing selectors accepted all three in the offline reproduction
saved as `review-source-time-reproduction.log`. Data's validator had the same
structural bypass. The writer subsequently restored finite/future source-time
validation in both JS and Python, independently of the intentionally separate
authority freshness clock. Inspection and the final 42-test run confirm the
candidate correction. Original source age may be old; invalid/future source time
must not become valid through a heartbeat. No reviewer implementation edits.

## Reviewed behavior

- Sunday 22:00 Eastern cycle and durable three-hour next-attempt checkpoint;
  unchanged-show attempts continue, changed-show completion defers to next Sunday.
- Calendar selection uses tomorrow only within the inclusive show range; repeats
  retain selection and do not roll it backward. Source-selected date/time remain
  separate from calendar selection and authority validation time.
- Eastern calendar conversion and weekly boundaries account for DST. Persisting
  the attempted slot before source access prevents five-minute retry polling.
- Archived payload hash/path and canonical metadata checks remain; calendar
  evidence is checked by the Schedules reader. Native show keys/IDs and existing
  count stop remain protected. New timestamp mapping reaches the Data validator.
- Live, Entries, Schedules and active Timing authority checks use the new
  validation clock while unrelated Data/count/Live observation limits remain.
  Timing supporting Shows references carry the authority clock separately from
  original `observed_at`. Retired Timing paths remain outside the delta.

No additional actionable code defect was identified in this candidate.

## Tests and release boundary

Independently executed the isolated Shows test suite: **42 passed, 0 failed**.
Exact command, Node/Python runtime, start/end time, duration and exit code are in
`independent-review-tests.json`; full TAP output is in
`independent-review-tests.tap`. These tests use fixtures/mock IO, not business runs.
The source-time reproduction above records the earlier failing candidate, not the
corrected candidate's current behavior.

`independent-review-hashes.json` identifies the reviewed runtime candidate files.
This is a moving candidate review, **not final frozen-release acceptance**. Match
hashes before reusing it; review any later material delta.

Deployment remains **BLOCKED** by the explicit TODAY-consumer decision. New field
schema/mapping readback, final manifest, deployment checks and normal scheduled
acceptance remain separate gates. A Friday fixture cannot prove an actual Sunday
cycle, retry or rollover. The existing count-publication stop is neither fixed nor
authorized for retry by this review.

## Frozen-candidate reconciliation

Subsequently reviewed the frozen 14-file `deployment-manifest.json` release.
Every prepared-release file and corresponding isolated test-tree file exactly
matches the manifest hash; `independent-frozen-hashes.json` records the comparison.
The normalized source start/end date comparison preserves the existing `isoDate`
contract, including production timestamps rather than date-only fixtures. The
source-time guard correction is present in both JS and Python. Data's new
authority-field transport mapping is included in the frozen source and tests.

Independently reran the frozen isolated Shows suite: **42/42 PASS**, exit 0.
Command/runtime/timestamps are in `independent-frozen-tests.json`, with complete
output in `independent-frozen-tests.tap`. This supersedes the moving-candidate
qualification above for these exact 14 hashes only.

Code review has no additional actionable implementation finding after the P2
correction. **Deployment remains BLOCKED by the P1 TODAY-consumer policy decision**;
passing tests do not authorize accepting the 19:00-to-midnight pause. No runtime
deployment or scheduled acceptance was performed by this reviewer. Preserve the
existing count stop and other module failures independently.
