# Source error correction — 23 October 2026

Baseline: local checkout HEAD 2c3cd2912 plus the existing uncommitted collectors.
The deployed decoding Worker matched its local source. No Worker edits or
Cloudflare deployment were needed.

Observed diagnostic: a decoded class request returned the Worker's generic
502 after 25,068 ms, matching its 25-second fetch timeout. A people request
and public live detail request returned HTTP 200 in under one second.
Diagnostic GETs were read-only, never collector runs or operational proof.

The archived class 200030319 pair disagreed: public live contained trip
200398019, but decoded trips no longer did. An earlier successful archived
pair contained its exact entry ID 200247097. Current public detail also removed
that trip. This is evidence of differently timed snapshots, not a failed ID
decryption. No historical ID was injected into current production records.

Correction: entries and live share a small HTTP reader that retries only one
502/503/504 or specified timeout/reset, then fails. Authentication, malformed
JSON and unconfirmed decoding never retry. Live uses the website's timestamp
parameter for fresh detail and refreshes both snapshots once if an exact trip
identity is missing. Both pairs are archived. Persistent mismatch fails closed.

Forty offline and deployed tests pass. The prior live halt receipt is retained
here. Live was re-enabled on its original five-minute trigger. Entries remains
on its original trigger. No manual collectors or Airtable record repairs.
Normal-run acceptance: entries first run passed at 13:04 Eastern. Live
failed its normal 13:02 run on persistent HTTP 502 for class 200030347 after
the one allowed retry. Further live starts are disabled. No additional source
patch or retry was made after this failed gate. The second independent entries
check remains pending.


Final acceptance at 13:11 Eastern: entries completed normal runs at 13:04 and
13:09, with seven and nine entries respectively. Both had no source errors and
passed record/link/archive checks. The first full verification passed. The
second full verification was blocked by concurrent changes to
wef-tenants/airtable.js, collector.js and settings.js at 13:07, outside this
task's deployment file list. Those changes were left untouched; expected hashes
were not changed to hide the drift.
Live's 13:02 normal run ended Failed with persistent class 200030347 HTTP 502;
no missing-trip identity error remained and 68 archives verified. Live is disabled.
No production patch or manual retry followed that failed gate. Overall work is
incomplete: persistent 502, concurrent protected drift, and two live runs remain.
Full evidence: wef-live/evidence/source-fix/release-status.json.
