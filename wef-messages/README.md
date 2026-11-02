# WEF Messages: offline decision contract

**Documentation version: 1.2 — updated 2 November 2026.** Offline reminder
calculation added; existing event policy `wef-messages-v1` and dependencies retained.

## Offline 60/30-minute reminder proof

Owner approval on 2 November covers offline files, saved-observation tests,
changing estimates and duplicate prevention. Baseline repository HEAD at start:
`8c8a5ffe1b48f2be4c07cfe88066adc9ef75c982`. Only this directory's four files
are edited. Other owners' existing changes are outside this assignment.

`planClassReminders({observation, prior, now})` calculates pending check times
and returns offline candidates, never send eligibility. It has no scheduler,
source reader, Airtable writer or SMS integration. Both 60 and 30 minutes are
enabled for this fixed offline proof; preference selection is a later adapter.

The observation supplies `scope_key` (class/date plus audience or preference
scope), `show_date` (ISO), `utc_offset_minutes` (explicit, date-specific),
`observed_at` (ISO instant), `start_time` (24-hour HH:mm:ss clock duration),
and `status` (source text). These are local input names, not new Airtable fields
or an asserted producer contract. For the November 1 fixture, -300 is the
America/New_York date conversion context; it was not captured alongside each
archive by the show-clock endpoint. A future adapter must supply verified clock
context. No host clock or implicit timezone is used.

- First observation baselines any elapsed thresholds without catch-up.
- Next check is estimate minus 60 minutes, then minus 30 minutes. A distant
  class needs no intermediate individual countdown checks. Existing source
  updates may replace the estimate before the pending check.
- Estimate changes move only unconsumed checks. A due check reserves that
  milestone once in returned `nextState`. Persist/reuse that state between calls.
  Candidate IDs depend on scope/date/lead, not estimate revision or clock tick.
- If both milestones cross together, return only the nearer 30-minute candidate
  and consume both. A consumed 60 never prevents an unconsumed 30.
- Underway/Completed cancels pending work and stays stopped. At/past the current
  estimate, no stale reminder is returned. Old/future observations, changed data
  at an identical observation time, backward clocks and mismatched prior scopes
  withhold. State hashes detect alteration, not authenticate state.
- Outside 08:00:00–15:00:00 pauses checks for review; retain the observation and
  consumed milestones. No fallback to default_time. Recovery baselines elapsed
  milestones; this deliberately avoids a catch-up caused by an unverified period.

`fixtures/alert-policy.json.saved_class_observations` contains six minimal
projections from saved decoded class 200030779, show 200000076, November 1.
Each carries its archive path and SHA-256 of the decompressed source. Actual
source fields: `decoded.class_related_data.date`, `.estimated_time`, `.status`;
observation timestamps come from saved lane receipt archives. The scope/audience
string and UTC offset are explicitly supplied offline context. Synthetic cases
in tests separately exercise exact boundaries, later estimates and replay.

| Saved estimate | Next 60-minute check | Next 30-minute check |
| --- | --- | --- |
| 13:00:00 | 12:00:00 | 12:30:00 |
| 12:45:00 | 11:45:00 | 12:15:00 |
| 12:20:00 | 11:20:00 | 11:50:00 |
| 12:00:00 | 11:00:00 | 11:30:00 |

All table times are show-local. The 23:59 saved estimate is reviewed separately.
These prove countdown arithmetic and state behavior using saved estimates,
not estimate accuracy, fresh source suitability, delivery or scheduled operation.
Candidates still require the existing consent/provenance/expiry gates before any
future delivery. This helper intentionally does not join into `evaluateAlert`;
runtime integration remains prohibited. A future caller must serialize state
reservations; concurrent writes and missing committed state are unverified.

Offline acceptance command: `node --test wef-messages/alert-policy.test.js`.
The initial tests showed the missing calculation and consumed-slot defect;
after implementation all 38 tests pass, including the original 25. Root
package.json defines no project test command. No dependency upgrades or package
changes.

November 2 verification: all six projections matched the decompressed archive
SHA-256, exact source fields and matching saved receipt timestamps. Syntax and
scoped whitespace checks passed. Independent reviewer /root/reminder_review
reran all 38 tests and passed 32 separate in-memory assertions; no reviewer edits.

**Reminder delta acceptance: FAIL.** Independent review found P2: the new helper
uses shared time()/Date.parse validation, which normalizes impossible calendar
timestamps. Reproduction observation: scope_key a, show_date 2026-03-02,
utc_offset_minutes 0, observed_at 2026-02-30T07:00:00Z, start_time 13:00:00,
status Not Started. At now 2026-03-02T07:00:00Z it returns baseline; reusing
that state at 2026-03-02T12:00:00Z returns candidate. Expected: withhold.
No code correction made after failed verification. Pending fresh owner
APPROVED TO EDIT: strict calendar validation for the helper's instant inputs
and prior timestamps in alert-policy.js, regression tests in alert-policy.test.js,
then offline checks and independent review. Broader evaluateAlert behavior and
its other baseline findings remain outside that correction.
No Airtable writes, runtime integration, schedules or SMS occurred.

## Coordinator responsibilities

The wef-messages-coordinator independently owns validated Timing-event consumption,
alert decisions, eligible recipient selection using current Tenants permissions and
consent, templates, notification threads, duplicate prevention, two-way SMS sender
verification/request handling/replies, delivery attempts/retries/provider outcomes,
safe history, and Messages code/tests/review/workflow acceptance within approvals.

Timing owns ring/class/entry calculations and condition contracts. Records owns
source identity/freshness. Tenants owns canonical permissions/preferences. Recognize
owns identity and OTP, unchanged. Astro externally enforces exact WEF grants;
service authentication grants no tenant rights. Publish and the confirmed
application owner own schedule endpoints. Messages does not take over those roles.

## Current acceptance and stop

The broader first offline implementation remains **NOT ACCEPTED**. Writer and
independent supplied-suite runs passed25/25; the independent reviewer ran10
additional probes,5PASS/5FAIL, confirming four defects:

- P1: nested confirmation/reference material can leak private data into snapshots.
- P2: a consumed earlier reminder can suppress a fresh nearer reminder — corrected
  in the November 2 reminder delta, with a sequential regression test.
- P2: missing timezone silently falls back to the host timezone.
- P2: evaluation time changes material hashes for timing-change notices.

Evidence: C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-related-to-sms/outputs/offline-implementation/review/report.md,
execution-log.json, review-receipt.json and review-verification.json in that same
directory. Packet integrity PASS is not candidate acceptance. The report applies
to the earlier frozen candidate; this documentation-only revision changes README
bytes. The November 2 delta changes the module, tests and fixture as described above.

The November 2 approval covers the reminder delta and its consumed-slot correction.
The other three review findings remain outside that delta and unresolved; no
whole-policy acceptance is claimed. No Airtable writes, runtime
integration, task/schedule changes, deployment/publication or SMS are authorized.
No collector or sender runs; preserve unrelated owners' changes and existing stops.

## Approved first-step scope

Approved scope: four new files only (alert-policy.js, alert-policy.test.js,
fixtures/alert-policy.json, README.md). No runtime adapter, source reads, Airtable
writes, sender, task, schedule, deployment or SMS. No package changes. All fixtures
were synthetic in the original step; saved projections are now also included.
This directory did not exist at the original approved baseline.

## Interface

CommonJS evaluateAlert({event, recipient, prior, now}) returns decision, reasons,
snapshot and nextState. It is synchronous and pure; node:crypto is its only runtime
dependency. now is mandatory ISO-8601 with timezone. No clock/environment/network,
filesystem or provider is consulted. The proposed normalized input is NOT an
existing producer API or current verified permission adapter.

event is version 1 / policy wef-messages-v1 with an immutable id, active/initial/
recovery mode, data_live source lane, available/partial/unavailable status,
customer/show/date/ring/group/class/occurrence/base-phase identity, explicit target
flags, original observation/detection/expiry/end times, per-required-value proof,
already-produced conditions and exact scoped input_refs. Personal conditions also
require entry/trip identity. Times must describe today's show-local date.

Each provenance reference names one of wef-data, wef-live, wef-ring-pace,
wef-class-calculator, wef-entry-calculator and supplies base/table/key/revision/hash,
normalized applicable scope and original observation/expiry. Consumers must supply
all actually required dependencies; this pure function cannot discover omitted
source evidence or authenticate supplied assertions. Upstream adapters, phase
semantics, exact event commit/ACK and serving-time suitability require acceptance.

recipient carries canonical person/represented person, same-person/same-tenant
Enabled membership, exact allowed scope, current authority assertion with expiry,
Granted/Active/Production, enabled=true, revoked=false, provider_suppressed=false,
preference_revision and explicit options. No missing option is enabled by default.
These are offline assertions, not a replacement for Tenants or Recognize. Identity
does not grant access. No raw phone or private body is returned.

prior is the exact previously committed nextState, or absent on first observation.
It contains a scope, monotonically increasing revision, seen immutable event hashes,
consumed milestone slots, notifications and last-change time, with head_hash over
canonical JSON excluding head_hash. The hash detects alteration; it is NOT a
signature, access check or proof Airtable committed anything. Store/read back state
in a later authorized Airtable snapshot adapter. This module has no persistent
memory. A future adapter owns predecessor/atomic overlap and uncertain-write gates.

## Policy and modeled outcomes

- Class/trip first/second reminders require supplied producer crossings from >lead
  to <=lead while target is future; saved minute values override suggested30/15.
  Personal go requires start/phase/order/eligibility/anchor proof.
- rider_oog10 uses explicit positive integer entries count (suggested10), never
  minutes or unknown-as-zero. The nearer enabled slot wins if both crossed;
  equal slots and simultaneous class/personal notices combine for this exact scope.
  All superseded/combined slots are reserved to prevent later duplicate reminders.
- Early/late class changes require >=5-minute absolute target movement, two distinct
  ordered observations confirming direction (latest target equals event target),
  confirmations under10minutes and >=10minutes since previous eligible change.
  Ordinary passage of countdown is not a new condition.
- Started choices require an explicit actual transition, never estimated elapsed
  time. No completion/conflict/unspecified alert is supported.
- First observation, explicit recovery or changed preference revision baselines;
  no catch-up notices. Eligibility reserves a modeled logical notification; it
  does not mean send, submission or delivery. Template-only changes do not resend.
- Freshness: Data/class/entry refs strictly under30minutes; Live and explicitly
  observed-interval pace refs under10. Event expiry is earliest dependency/event
  expiry, condition end, detection+120seconds; current recipient authority can
  shorten it. Future source clocks reject; equality is expired. Missing fields,
  stale/unavailable inputs, phase mismatch or invalid prior fail closed. Partial
  is usable only with required proof. Fallback pace alone is no personal-go proof.
- Scratch/gone/retraction/supersession and revoked/suppressed/ineligible recipients
  cannot produce eligible output. submitted/delivered/unknown/send-intent prior
  notification states cannot create a duplicate attempt. Exactly one retry may be
  modeled after definite nonacceptance with attempt_count=1, >=60seconds since
  attempt, still current/eligible/unexpired. Unknown is never auto-retried. Future
  integration supplies actual provider outcomes; this module never sends or marks
  delivered and does not implement a provider outcome writer.

The following safety properties describe the intended contract; the privacy and
timing-change hash findings above remain unresolved. Input objects are not mutated.
Invalid JSON objects (accessors, functions, cycles,
exotic prototypes) reject before being read. Extra event/recipient fields are not
copied into snapshots. Stable JSON hashes ignore evaluation time and template
extras. Changed material cannot reuse a seen event ID. Replay returns noop with no
snapshot. Prior state is trusted committed history validated structurally/by hash;
the future persistence layer must fence its revision against current durable head.

## Tables and snapshot

wef-messages appXjWPaCthAYs0l7: run-logs tblWhGshVZuUlKHbO; heartbeat
tblXeSnesC31P8P5S; errs-log tbl0q9iniaffVoXAa; code-reviews tbl8vNGsjwQDWPi9c;
alert-snapshots tbleBBQpNdyrxNOmr. meta preserved. Only the last table is modeled;
no writer or records are created by this module. No new table, SQLite or scope cap.

snapshot has the existing23 fields: snapshot-key, scope-key, source-event-id,
alert-kind, source-lane, show-id, date, ring-number, group-id, class-id, entry-id,
trip-key, phase, availability, decision, evaluated-at, observed-at, valid-until,
policy-version, reason-codes-json, input-refs-json, payload-json, content-hash.
IDs are integers (nonapplicable entry/trip null); remaining scalar fields text and
three JSON fields long text. A later writer must map null to unpopulated cells.
The intended payload preserves opaque recipient/tenant IDs, preference revision, exact
conditions, logical notification/attempt IDs, predecessor state hash/revision and
recoverable nextState. Conditions and provenance must be recursively allowlisted;
the current candidate's nested privacy defects remain uncorrected. Phones,
credentials, OTP/private links, arbitrary bodies and raw caller payload are excluded
by the required contract, not proven absent on every path by the supplied tests.

Allowed emitted decisions: baseline, eligible, suppressed, withheld, expired, noop.
noop has no snapshot. Accepted/send-intent/submitted/delivered/failed/unknown and
superseded snapshots belong to later intake/delivery integration, not fabricated
offline outcomes. Actual condition before/after and source IDs remain evidence;
Messages validates assertions but does not calculate pace, go, start or new Timing
conditions. Recipient-specific identity excludes role labels so duplicate roles
cannot create duplicates within the same supplied person/tenant/participation.

## Verification and next gate

Run node --test wef-messages/alert-policy.test.js from repository root. Tests use
synthetic normalized input, explicit clocks and independently specified expected
decisions. They cover invalid scope/provenance, units/slots/combining, baseline,
age/expiry, consent/isolation, replay/restart, hash/chain corruption, modeled retry,
actual transitions, change confirmation/spacing, privacy and no input mutation.

Before integration: independent code review of these exact frozen files with own
checks and durable report/log/receipt; accepted upstream event/ACK and Tenants
assertion adapters; authenticated sender/unknown-outcome reconciliation and
durable singleton/readback/uncertain-write proof. Existing stronger failed-runner
stops remain. No live integration, scheduled proof, provider delivery, full feed,
retention or phase validity is established by these offline tests. Timing/Records/
Tenants/Recognize/Publish responsibilities and approvals remain unchanged.
