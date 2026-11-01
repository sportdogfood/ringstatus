# Independent Shows scope-reduction review

Reviewer: /root/shows_review. Applied local Code Review skill and its approved
change-boundary check. Exact UTC time and frozen before/candidate/dependency hashes
are recorded in review-receipt.json and review-hash-manifest.json.

## Findings and bounded decision

No actionable defect found in the reviewed counting-removal delta. Candidate PASS
for shows/dates/priorities only. Automatic Records alert delivery remains BLOCKED
and is not implemented by this release. This is not completion of that alert
requirement; do not label the whole owner request complete.

## Actual delta and scope boundary

Inspected task.md and actual before/staged diffs: two runtime files and three
associated test files. Collector no longer imports/invokes the direct counter or
Data count reader, no longer reads/writes the old count stop, and calls publishDates
instead of publishDays. Existing historical count helpers remain inert/available
for evidence inspection. No package/lockfile/runtime upgrade, new schema, endpoint,
service, observer, transport, outbox, automation, Data/Season/downstream edit or
opportunistic cleanup is part of the five-file release manifest. Such expansions
are excluded rather than inferred from the alert request.

publishDates reuses dates() validation, refuses duplicate keys or mismatched
existing date/native-show links before any creation, creates only missing keys,
show links and dates, then checks their readback. Existing rows receive no update
from this function, so old totals, accepted envelopes, count statuses and source
timestamps remain untouched. Failed date publication is logged as Partial; this
candidate intentionally does not repurpose the historical count-stop file for
date failures. Existing discovery/calendar and separate priority behavior are
preserved, including labeling newly created dates. The count stop remains evidence
on disk and cannot prevent current date publication. No deletion or record repair.

Reviewed Records README lines 43-58: transition alerts are required, but no installed
unattended event-to-chat ingress exists. An active-agent authorized handoff is
documented, including a prior submission, but does not prove automatic delivery.
The candidate adds no alert framework and makes no claim that transport works.

## Independent execution

24/24 targeted offline tests PASS, exit 0, using the frozen staged tree. Exact
command, cwd, runtime, start/end timestamps and separate stdout/stderr are in
review-command-log.json; raw streams are review-tests.stdout/review-tests.stderr.
Tool provenance: 2404d5. This is the reviewer's execution, not the writer's result.

Integration tests forbid global fetch and count/Data callbacks, execute repeat
collector functions with in-memory Airtable mocks, retain old stop bytes/count
fields, exercise evening selection/priority, create missing canonical dates without
count fields, preserve native IDs on repeat, and reject duplicate/wrong links.
Discovery, logging, priority and transport fixture regressions also pass. No real
collector, endpoint, provider or business-data operation was executed.

## Remaining acceptance

Counting-removal code review COMPLETE/PASS; offline tests PASS. Deployment and
normal scheduled-run readback are UNVERIFIED by this reviewer. Verify the existing
schedule reports only show/date/priority work and logs/heartbeat, makes no count
requests, and leaves count-stop bytes/history unchanged. Alert delivery remains a
separate explicit blocker; no additional implementation is approved by this review.
Mechanical receipt PASS establishes packet integrity, not operational acceptance.
