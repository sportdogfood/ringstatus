# WEF Messages: offline decision contract

Approved scope: four new files only (alert-policy.js, alert-policy.test.js,
fixtures/alert-policy.json, README.md). No runtime adapter, source reads, Airtable
writes, sender, task, schedule, deployment or SMS. No package changes. All fixtures
are synthetic. This directory did not exist at the approved baseline.

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

Input objects are not mutated. Invalid JSON objects (accessors, functions, cycles,
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
payload safely preserves opaque recipient/tenant IDs, preference revision, exact
conditions, logical notification/attempt IDs, predecessor state hash/revision and
recoverable nextState. Conditions and provenance are allowlisted; no phones,
credentials, OTP/private links, arbitrary bodies or raw caller payload.

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
