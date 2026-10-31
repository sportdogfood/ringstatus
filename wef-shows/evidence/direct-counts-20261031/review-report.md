# Independent direct-counts review

Reviewer: /root/shows_review. Applied Code Review 0.2.2. Approved scope: Shows
counts through the existing decoded proxy for every canonical show-day, storing
totals/logs only; Data unchanged; normal scheduled proof only. Exact UTC review
time, seven frozen candidate hashes, four existing-file baselines and inspected
dependencies are in review-receipt.json and review-hash-manifest.json.

## Findings

No unresolved actionable finding in the final frozen candidate.

P2 CONFIRMED and corrected before release: initially valid inventory followed by
class-detail failure produced an incomplete summary but publishDays returned
ok=true. Collector audit.capture did not change report.ok, allowing heartbeat and
run-log Success despite incomplete source checks. Initial reviewer reproduction
returned sourceIncomplete:true, publisherOk:true, status:Unknown, errors:[] (tool
baec65). That original reproduction output is recovered from tool history; no
execution times were reconstructed. The final publisher now withholds every
missing/non-complete direct day, preserves old totals, marks latest attempt Failed,
continues other days and returns ok=false. The existing collector then writes
Partial and its durable count stop. Independent final tests verify this correction.

## Source and integration review

Reviewed actual diffs of collector, audit, source-fetch and show-days against
before/, plus new direct-counts and its two tests. Existing Data endpoint/validation
contracts were inspected as references only. Production default no longer calls
day-source or Data; historical utility and prior count policy remain available.
Only the existing decoded Worker origin and dated schedule/classes routes are
used. Decode header, JSON shape, dates/show/ring/class identity, explicit pagination
indicators and total_entry_trips/length are checked. No invented pagination route.

Entry IDs are deduplicated per day; trip UUID or trip ID identities are deduplicated
per day. Conflicting owners across classes withhold the day. Scratch status is not
excluded; only current roster membership counts. Empty explicit inventory/rosters
can certify zero, while missing identities/inventory are not zero. All canonical
days, including past dates, are visited. Final inventory comparison withholds a
changed class/ring set. This is sequential source observation, not an atomic
snapshot of every roster; late roster changes await a later normal check.

Four in-flight reads maximum; source attempts use an 18-second timeout clipped to
the remaining 150-second budget from collector start. Existing bounded retries
are preserved. Publication/log writes occur after source work; this budget is not
a proof of completing the whole task inside four minutes. request_count counts
logical reads, not each transport retry. The size check rejects oversized text
after response.text() has buffered it; it is not a streaming memory ceiling.

Response bodies, names and membership sets stay in memory. Returned summaries
contain aggregate counts, policy/coverage, source timestamps and hash attestation;
diagnostics contain date/class identifiers and safe validation/transport messages.
No raw bodies are passed to archive or entity persistence. Existing discovery
archives remain unchanged, distinct from direct-count responses. The original
source observation time is retained for age classification; verification time
does not renew it. Native show-day links/keys and historical accepted-envelope
validation remain intact. Failed days retain accepted totals but publication is
Unknown and latest attempt Failed, making the withholding explicit.

## Independent offline execution

Initial targeted suite: 22/22 PASS; initial manifest retained separately. After
the confirmed defect correction: final 23/23 PASS. Commands were restricted to
direct-counts, direct-integration, show-days and source-fetch test files in the
isolated stage. Tests call actual functions with in-memory HTTP/Airtable fixtures;
they never invoke the production collector or real endpoint.

Final suite includes two collector fixture invocations, 42 mocked source reads,
no Data/entity table reads and no private names in stored values; separate failing
collector fixture verifies old totals preserved, six safe days published, Partial
heartbeat/run-log, err-logs and a temporary durable stop. Reviewer-owned
review-partial.cjs independently reproduces the original defect and now asserts
failed publication without writing null/partial totals for a class failure.

Exact argv/cwd/runtime/times/exit/stdout/stderr are in review-command-log.json;
final raw outputs are review-final-targeted.stdout and review-partial.stdout.
Tool provenance: db3d6a initial suite, 035490 final suite. No full suite, provider
call, production collector, Data write, rollout action or implementation edit by
the reviewer. Test scripts and report are evidence-only writes.

## Disposition and remaining proof

Review COMPLETE, candidate PASS, offline tests PASS. Prepared runtime only:
deployment and two normal scheduled runs UNVERIFIED by this review. Required
acceptance is actual direct-source checks for canonical days, accepted totals and
native link readback, source/run/error logs and heartbeat, and no new failure stop
within the unchanged task limit. Source freshness, broad/classless coverage and
provider availability remain separate from execution success. On failure, retain
the existing stop; no manual retry or record repair is authorized by this report.

Receipt verifier PASS establishes packet integrity only, not scheduled proof.
