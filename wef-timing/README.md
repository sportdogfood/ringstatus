# Ring pace

Owner-approved first Timing build, 29 October 2026. This folder is the deployed
package; Windows task `wef-timing` invokes run-hidden.vbs -> run.ps1 -> entry.js
once per minute while the configured interactive user is logged in. No agent is
on the execution path. Ignore overlapping starts; no manual collection or replay.

Reads only Schedules shows/rings/schedules/show-collection, Live group collections,
and their existing local hash-verified archives. Does not invoke either collector.
Data is a separate future input adapter, not a fallback or authority selection.
Entries, legacy calculators, Messages, Results and Publish are not dependencies.

Writes only wef-engine appam4opFT4NeL4mn:
- timing-answers tblHnV5iEJ13DOsBF: immutable changed-input/evidence/value snapshots.
- engine-checkpoints tblepakByOH4G4ruV: latest runner state and interval baselines.

Schedule pace is a positive same-day estimated window divided by a positive trip
count, initially restricted to singleton class groups. It is labeled partial,
schedule-derived, never observed operation. Multiple-class aggregation, schooling,
breaks, zero counts and invalid windows produce explicit unavailable reasons.

Observed interval pace is elapsed observation minutes divided by newly completed
main-phase trips. Both endpoints, counts, membership and source evidence are
recorded. Requires matching source/customer/show/date/ring/group/class roster,
stable total/membership and positive progress within ten minutes. No actual start
anchor is invented. Interval throughput includes holds/pauses. A new observation
with no progress has no new measured pace. Identical evidence rereads retain a
still-valid prior measurement; count resets/changes/gaps reset the baseline.

Show/schedule age limit 30 minutes; Live ten minutes. Equality is expired. Future
source observations are rejected. Host time is checked against Airtable's HTTPS
Date header within five seconds; Windows Time service is not changed. Schedule
time-only differences are not normalized actual timestamps or serving promises.

Snapshots are as-of records, not a current-answer API or renewable serving lease.
New input evidence creates a new snapshot even if its numeric pace is unchanged.
Each tick admits up to 80 groups in a rotating cursor; deferred coverage is explicit.
Snapshot membership is verified against dated collection dataset/fingerprint and
frozen archives before arithmetic. Read changing producer scopes on the next tick.
Engine write readback precedes baseline/checkpoint advancement; no uniqueness or
multi-record transaction is assumed. There is no committed downstream publication.

50-second hard deadline; admission stops early enough to reserve write/readback
time. Process lock plus IgnoreNew prevents overlap. Hard failure self-disables this
new task only and requires owner-authorized inspection/resume. Never automatically
repair records, remove stale locks or retry a failed business run. Local runtime
contains only the private env, process lock and diagnostic receipts/logs; Engine
is the canonical checkpoint. Snapshot cap 4000 still stops safely if the retained
current/future scope reaches capacity; same-day history compaction is not approved.

Owner-approved rollover purge reads Schedules only for authority. Requires a fresh
unique default show, selected working day equal to today's New York day, and a
complete fingerprint/archive-verified collection for that day. Two distinct normal
invocations confirm the context. Merely discovering a future day never deletes.
Only this writer's Engine ring-pace snapshots for confirmed previous default shows
and dates before the working day qualify. Unrelated/upcoming shows, other customers,
other answer kinds/lanes and all current/future days are preserved. Confirmed day
cutoffs persist so temporary source failures cannot recreate retired snapshots.
Baseline state never crosses dates/shows. No producer data or other base is deleted.

Deletes are bounded to forty records per invocation, ten exact IDs per batch.
Fresh authority is fenced again before each batch. Engine holds the intent before
DELETE; exact unique response IDs and subsequent absence readback are required.
Interrupted intents reconcile before new deletion; surviving/conflicting IDs stop
for explicit owner resolution. No blind retry or automatic repair. Latest purge
intent/receipt and counts remain visible in engine-checkpoints and runner receipts.

Run offline tests with `node --test wef-timing/pace.test.js wef-timing/runner.test.js wef-timing/purge.test.js`.
preflight.js is GET-only source/archive/access diagnosis, not workflow proof.
deploy.ps1 registers the next future trigger; it never starts a task manually.
Normal scheduled runs plus Airtable readback are required for acceptance.
