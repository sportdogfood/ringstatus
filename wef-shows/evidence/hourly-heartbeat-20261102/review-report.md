# Independent hourly heartbeat gate review

Reviewer: /root/shows_review. Local Code Review skill applied with approval-boundary
and recurring-delivery checks. Exact UTC time, candidate/baseline/dependency hashes
are in the review receipt and hash manifest.

Approval provenance: parent supplied the human request for hourly heartbeat when
TODAY is outside show dates and five-minute heartbeat/run-logs inside, followed by
the exact approval "approved to edit and test". These excerpts were supplied in
the review handoff, not independently retrieved from chat history. The accepted
proposal preserves discovery and 19:00 selection. No deployment or companion
freshness change was approved in that supplied history.

Code review COMPLETE; candidate FAIL. The 28/28 targeted offline tests pass but
omit the existing downstream authority-freshness contract. This approval covers
editing and testing only. Deployment remains unapproved and must not proceed.

P1 CONFIRMED: run-cadence.js final elapsed-hour return and entry.js early return
skip the entire collector, including its five-minute authority_checked_at refresh.
Existing authority-age.js showAuthorityReason expires that clock after 30 minutes;
Schedules show-source.js, Entries model.js and Live model.js invoke this guard.
Outside show dates, a normal successful 11:04 Eastern run is skipped at 11:35 while
those readers reject its authority as stale. This produces recurring downstream
unavailability during each hourly gap. The parent identified the compatibility
risk; reviewer independently traced current source and reproduced it using actual
gate/authority helper: gateSkips:true, downstreamReason:stale_observation at31min
(tool 3b7cd0; review-authority.cjs/output/log). No production call was used.

Smallest supported direction: retain five-minute canonical validation and suppress
only the requested heartbeat/run-log publication outside show dates, or obtain
explicit approval before changing companion freshness contracts. No correction
was made by reviewer. Passing gate tests cannot authorize this contract change.

Inspected actual entry diff, new run-cadence helper/test and README/scope changes
against saved baseline and owner requirement excerpts. Only the existing entry
gate plus one pure helper is added. No dependency/runtime upgrade, task/scheduler,
checkpoint, source call, count revival, schema change or downstream edit is in the
release. Current collector, discovery, priority/status behavior and protected
runtime stops remain unchanged.

Gate uses actual calendar TODAY from existing America/New_York policy, inclusive
start/end dates. Inside the range every normal task wake runs. Outside, a valid
successful completed cached report permits skipping until one elapsed hour from
its start, regardless of planning selected_date. Sunday discovery/three-hour due
attempts and the first evening decision bypass the skip. Missing, malformed,
failed, future or inconsistent cache falls through to existing validation/logging;
thus degraded cache can produce more frequent runs rather than hide failures.
Elapsed-time comparison handles the repeated DST hour. No source age is renewed.

The entry acquires/releases the existing lock even on skips. Skips neither invoke
collector nor overwrite last-run nor write Airtable heartbeat/run-logs. The existing
PowerShell wrapper still appends START/END for each five-minute task wake; this is
documented and distinct from Airtable run-logs. Due work retains established report,
exit-code and error behavior. No lock/stop is cleared by the candidate or reviewer.

Independently executed targeted cadence, day-priority, direct-integration and shows
tests in isolated stage: 28 PASS, exit 0 (tool 7d4118). Actual entry function uses
mock dependencies and temporary state only. Cases cover hourly outside/after-show,
inclusive dates/Eastern midnight, Sunday/three-hour and evening exceptions, DST
fallback, failed/invalid cache, skip without report write, existing lock release,
and failed collector report evidence. The 19 prior module tests remain unchanged.
Exact argv/cwd/runtime/times/stdout/stderr are in review-command-log.json with raw
review-tests.stdout/review-tests.stderr. No production collector/provider/business
execution occurred and no implementation edits were made by reviewer.

After the confirmed compatibility defect is resolved and separately reviewed,
required future proof after separate deployment authorization: normal outside-show
wakes skip without new Airtable logs/timestamp changes until due, hourly execution
updates them, inside-range execution remains five-minute and scheduled exceptions
remain intact. Offline checks establish branch behavior, not live cadence. Packet
verifier PASS is evidence integrity only. No additional watcher, timer or wait
workflow is proposed by this review.

The writer subsequently preserved the rejected entry/helper/test in rejected-collector-skip and changed the staging tree. This receipt covers those preserved original bytes only. Execution logs retain their actual then-staged paths. The counterexample is historical evidence executed before that change; no later design is approved by this report.
