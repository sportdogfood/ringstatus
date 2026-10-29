# Tenant-independent Entries preparation

Date: 29 October 2026
Baseline commit: 03987bb58984e501252091acf97dad6915ff8365
Status: Narrow matcher implemented and tested locally; not deployed.

## Latest authorized implementation

Latest owner boundary: CWF is outside all WEF workflow dependencies and acceptance gates. Historical CWF reader-transition requirements below no longer gate the independent WEF design. Removing residual coupling from the old running collector/task is separate from this bounded local matcher implementation; no stop, deletion or production switch was performed.

The owner approved the narrow matcher, keeping CWF out and continuing supported links while flagging uncertain group links. participation-linker.js now implements this distinction and the staged collector uses it. Unique class/date schedule association can continue independently of group eligibility. The strict group rule requires full class-roster equality, not subset membership. No tenant reads or optional downstream dependencies were added.

Verification: 46 Node offline checks pass (15 matcher checks, five prepared-function checks, and existing regressions), plus four retained-input Python checks. See matcher-test-results.txt and ../../MATCHER.md. The implementation has no source/network/write operations. The recurring collector, CWF and schedules were not changed by this task. Earlier preparation notes below are historical where they conflict with this update.

Owner authorized removal of CWF from the Entries stage and continued work. The module links schedules, rings, groups, classes, horses, riders and trainers. Messages, results and waze are developing optional connections. Tenant selection belongs downstream.

## Verified locally

- Eleven pure linking tests pass with no tenant/trainer-slot/private-follow inputs.
- Four retained-input tests pass using the Data owner's frozen fixture and existing revision_chain and retained_units validators.
- Stable operational participation keys and local schedule record links are preserved in the candidate.
- Complete empty class membership succeeds; malformed input fails. Ambiguous groups remain unresolved.
- Actual trainer IDs are separated from the legacy trainer-feed association field.
- No production source calls, manual collector runs, record repairs, deployments or Airtable changes were performed.

## Exact remaining work

- Confirm source participation to operational schedule eligibility with Schedules/Live. Even a unique show/class/date/ring match requires separate proven schedule eligibility. The candidate rejects contextual-only matches.
- Finish the production retained-input reader and destination persistence only against that accepted mapping; preserve archive-before-write, failed-scope history and original observation times.
- Coordinate the old CWF consumer contract transition before switching the writer. Its entries-consumer.js currently requires heartbeat-key CWF and CWF|show|date|trainer-feed|pid collection keys. It treats every nonblank trainer-ids row as a team export. Do not fill that field with all-show actual trainers.
- Confirm native ring/group/entity and optional downstream connection field mappings; the candidate currently produces schedule links and source entity identities, not a deployed entity-table migration.
- Deploy through the established route, then verify two normal Entries runs. CWF task completion is outside Entries acceptance; boundary compatibility is still required.

Records coordinator received candidate location, test results and exact consumer dependency. Data owner confirmed frozen-input validation. Schedules/Live subsequently supplied the construction and matching references below; they did not establish attributable participation eligibility.

## Confirmed schedule contract

- wef-schedules/normalizer.js, buildScheduleMachineKey: use class_group_id plus class_number, joined by underscore, when both exist; otherwise class_groupxclasses_id, then class_id. Explicit source breaks may use block:class_group_id.
- wef-schedules/current-store.js, columns: stage-key is show-id|YYYY-MM-DD|machine-key. It retains the source row in row-json; ring is ring_number, not ring_id.
- Match supported customer/show/date/ring-number/class/group/occurrence evidence against operational fields and row-json, then copy the actual stage-key and resolve the local schedule record. Do not reconstruct or substitute Data's different schedule key.
- Key construction does not establish which entry/trip belongs to a group or repeated occurrence. A contextual singleton alone remains insufficient. No production eligibility adapter has been proved; unresolved candidate links remain unresolved.
- CWF reader/privacy compatibility remains a separate deployment prerequisite: the existing reader and export use legacy feed associations and CWF-specific readiness records. All-show actual trainers must not replace those associations, broaden team exports, or expose personal follows. No CWF changes are authorized by this technical handoff.

## Live owner handoff: evidence boundaries

Received 29 October 2026; read-only technical handoff, not deployment authorization.

- Existing matcher: wef-live/model.js groups() validates explicit identities and rejects conflicting class rings. wef-live/collector.js schedules() requires one active show/date/class schedule with the explicit group-key prefix; it does not independently compare ring identity.
- Existing reader: wef-live/participation.js validates producer-supplied schedule keys, entry/class/horse/rider identity, observation freshness, dated schedule health and source/sync parity. Its ring check compares schedule copies, not the live ring. It does not derive trip-to-group eligibility.
- Owner-reported normal-run evidence: wef-live/evidence/participation-link-release.md and participation-run-1.json / participation-run-2.json. October 24 runs d66f6c2e-6dd4-4ea8-b93d-ffb5699e840e and 5257eecc-cbfe-4cb6-93bc-e37e3856b472 validated two current participation links and preserved six prior links. Overall runs were Partial because of a separate historical class 200030429 identity mismatch. This handoff is not a fresh independent verification of those files.
- Fixture-only evidence: wef-live/test/participation.test.js and the multiple-live-groups test in test/collector.test.js demonstrate reader support, not natural participation-specific multi-group eligibility.
- Still unproved: the new Data-to-Entries operational schedule matcher and participation-specific group/source-occurrence eligibility. Existing Live proof cannot be substituted for these checks. No group fanout, invented keys or reintroduced tenant dependencies.

## Artifacts

- before/: source baseline copies and hashes for comparison.
- staged/model.js: pure tenant-free scope and class relationship normalization.
- staged/collector.js: pure linking orchestration; no production IO adapter.
- staged/retained_input.py: inert completed-revision class snapshot projection with canonical fence and provenance.
- staged/linking.test.js and staged/test_retained_input.py: offline checks.

The running code remains the earlier tenant-dependent collector. Do not represent this preparation as completed removal or live proof.
