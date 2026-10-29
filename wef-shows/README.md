# WEF default-show authority

## Locked operating plan and responsibilities — 24 October 2026

This is the agreed scope for the wef-shows chat. Changes to this boundary require
explicit owner approval. This is a responsibility baseline, not a filesystem lock.

1. Independently discover Wellington's default show every five minutes.
2. Validate and publish show identity, show dates and the source-selected date.
3. Archive source responses and previous show state; record every run and archive.
4. Supply read-only authority to consumers. Never start their collectors.
5. On discovery failure, retain the last verified show and report the failure.
   A missing default is a failure, not a valid empty collection.

| Owner | Responsibility / Airtable base |
| --- | --- |
| wef-shows | Sole writer of canonical show context in `appCaSmL819KK7UFJ`; owns discovery, its archives, run-logs and heartbeat. Holds cross-consumer source-repair history. |
| wef-schedules | Reads show authority; owns schedule/class/group/ring/date collection and local links in `appUq5Qz9kARqFija`. |
| wef-entries | Reads show authority; owns tenant-derived participation and the CWF consumer migration in `appSxF4pYMBaxpTZd`, including its approved CWF boundary. |
| wef-live | Reads show authority; owns today's live observations, trip identities and local reference links in `appgzosUcVqA4vZdf`. |
| wef-records-coordinator | Records handoffs, dependencies and owner acceptance; coordinates any shared-source changes before implementation. |

The handoff key is `show-key = customer_id|show_id`; consumers match source IDs,
not Airtable record IDs across bases. Show metadata has no private tenant
selections, user follows or permissions. `selected_date` is source-selected,
not automatically TODAY. Missing, ambiguous, inconsistent or older-than-30-minute
authority must not become an invented show or trigger another collector.

Downstream collectors, CWF helpers, schemas, tasks and acceptance remain with
their respective owners. This chat hands off exact source IDs, affected runs,
retained evidence and unresolved questions. It does not independently edit
downstream or overlapping shared-source code. The approved CWF base is
`appxKPBtdl2Rdq7CS`; its consumer/helper ownership belongs to the entries owner.

Current plan: preserve the proven discovery implementation and five-minute
cadence. No additional build is needed for this scope. Any future change must
preserve the published keys and links, pass relevant offline checks, and be
verified through two normal scheduled runs. No manual collector runs or record
repairs. Host migration must explicitly resolve the local archive dependency.

## Current operational evidence — checked 24 October, 11:21 a.m. Eastern

- Default: `2026 ESP Fall III (#261901)`, show `200000075`, October 22–25.
- Source-selected date: October 24; one default record, `rec4En9UWd6lkT1Oh`.
- Latest twelve scheduled runs, 10:24 through 11:19, all `Success` in Airtable.
- Latest run: 11:19:03–11:19:09, `8290957b-6acf-43c5-9527-cb18f50ff5a1`.
- Previous run: 11:14:01–11:14:08, `456df403-84b7-4172-8ec9-f0183767c99e`.
- Heartbeat matches the latest completion. All six compressed archives from
  those two runs were read and their SHA256 values verified against run-logs.
- Task enabled/Ready, exit result 0; next scheduled start observed: 11:24.

Evidence was read from Airtable `shows`, `run-logs`, `heartbeat`, the task state,
`runtime/last-run.json`, and the archive paths recorded in the two run logs.
This check made no source requests, collector runs, repairs or runtime changes.
It establishes wef-shows health at the stated time, not downstream readiness.

`wef-shows` owns default-show discovery. It reuses the proven schedules
`discoverFocus` helper, with its own state, transport runtime and task. The helper
remains exported for compatibility and offline fixtures; schedules' normal entry
point reads the verified central show instead of invoking that helper.

Discovery makes the same two requests through the unchanged decoded Worker:

- `/ring?customer_id=15` discovers the source default show ID.
- `/schedule?date=00/00/00&show_id={discovered-id}&customer_id=15` supplies the
  default schedule date, show dates, facility and full default schedule payload.

The independent hidden Windows task `wef-shows-collector` runs every five
minutes, one minute before schedules. It does not start downstream jobs. The
computer must be awake and the current Windows user signed in.

## Ownership

`shows` in base `appCaSmL819KK7UFJ` owns show-key, show_id, customer_id,
facility_id, show_name, start_date, end_date, selected_date, available_dates,
is_default and last_checked. The stable key is `customer|show-id`. Prior shows
remain in the directory; rollover changes the default flag after archiving.
`run-logs` records every attempt, error and archive. `heartbeat` holds the latest
completed discovery result. Existing meta remains untouched.

The complete decoded default response is retained in a compressed, hash-verified
local archive, using the existing archive pattern. The show
record publishes `default-payload-file` and `default-payload-sha256`. Schedules
reads that exact response for its established normalizer/fallback comparison;
it does not reconstruct a partial payload or request default discovery again.
This file handoff is local to this machine, not a hosted download or cross-host
sharing mechanism. A host migration must preserve the archive or explicitly
replace this handoff.

Schedules rejects missing, ambiguous, more-than-30-minute-old or inconsistent
canonical data before collecting. Archive paths are restricted to this runtime's
history folder and compressed contents must match the published SHA256. Failure
does not trigger a manual discovery fallback or erase prior schedules.

The existing schedules `shows` records and their IDs remain in place as linked
local mirrors. Source fields come from wef-shows; archive fields remain owned by
schedules. The mirror's last_checked preserves the canonical discovery time.
Entries and live now read canonical wef-shows directly. CWF consumes the
entries-owned synced participation through its approved migration. Those later
consumer changes are owned and verified by their respective chats; the initial
release evidence below retains its original, narrower scope.

## Scope and evidence

Source-selected date is not automatically TODAY or proof of a live show.
Schedules continues collecting every retained show day, with existing protections
and show-specific manual settings. Discovery has its own cadence and does not
depend on a completed schedules run.

`configure.js` writes destination schema only. `entry.js` is the scheduled
business entry point; do not invoke it manually for proof. `verify.js` reads
existing receipts, records and archives only. Release evidence is in `evidence`.
Private credentials, source responses and operational state are ignored under
`runtime/`. Deployment acceptance requires two normal scheduled consumer runs.

## Release acceptance — PASS, 23 October 2026

Normal producer runs at 10:34 and 10:39 Eastern populated and refreshed the same
canonical show record, rec4En9UWd6lkT1Oh. Normal schedules starts at 10:35 and
10:40 consumed those verified records and completed all four dates, with zero
failed days. The existing empty October 22 warning was preserved. An initial
read-only verification predicate expected only Success; the established schedules
Warning label was checked against zero failed days and only empty_show_day
warnings. No production correction or rerun was needed.

All 449 existing schedule IDs and their show/ring links, all 24 ring IDs and
the existing local show record recpjdzMqBApZ8KPs were preserved. Both schemas,
all three existing collector task definitions and 12 protected code files were
unchanged. Offline checks: 124 schedules, 65 CWF and 10 handoff tests passed.
Normal run logs/heartbeats and compressed archive SHA256 values were read back.
No manual collector runs or record repairs. Exact receipts and checks:
`evidence/release-verification.json`.


## Canonical show-days publication � 29 October 2026

The approved addition publishes canonical `show-days` after existing default-show
discovery, using local native links to `shows`. It reads only Data runs/collections;
source acquisition, Data schemas, downstream inputs and all task cadences stay as-is.
`day-schema.js` is the focused schema configuration function; `configure.js` retains
its original discovery schema role. No manual collector execution is part of deployment.

`day-source.js` reads a completed Data head and exact predecessor/collection references,
uses a hash-and-record-identity checked local cache, and invokes the pure Season
`day_counts.accepted_summaries` validator through `day-replay.py`. The subprocess
has no Airtable credentials or network operations. It uses the existing Data Python
runtime; that deployed module and its replay helper are dependencies of this release.
The terminal head is read again after replay. A normal canonical-context change
before Data catches up produces Unknown days, not invented counts.

`accepted-summary` is authoritative; display columns are projections. It retains
scope, counts, original observation time, upstream revision/digest and exact summary
part locators. Detailed membership stays in the original Data evidence. A reader
must verify the local digest, native show/date scope, read the summary before and
after projections, and require stable revision/digest/status plus matching projections.
Never read just a displayed zero or publication status as a readiness decision.
Unknown observations may have null source time and null counts. The five-minute
age is measured from source as-of, never verification or publication time. Verified
competition-class coverage is not whole-day attendance or a Live suppression gate.

Day publication failures preserve the previous accepted summary, log Partial with
separate discovery success, and create `runtime/show-days-stopped.json`. Later
normal wakes continue original discovery but do not retry the failed day publisher.
Removing this stop or fixing a failed scheduled path requires separate owner approval.
Incomplete/unavailable coverage is a valid Unknown outcome, not an execution failure.
A failed staged projection is nonconsumable until approved recovery; the original
accepted body remains the preserved historical tuple.

`verify-days.js` is GET-only normal-run verification: it compares each accepted body
with an independent replay of its exact producer revision, checks display/native links
and saved run evidence, and writes a local verification receipt. It never invokes a
collector. Two normal producer/consumer pairs are required for deployment acceptance.
