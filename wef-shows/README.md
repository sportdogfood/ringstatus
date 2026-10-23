# WEF default-show authority

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
CWF and entries continue reading their existing references, without code,
schema, task or consumer changes. No sync conversion, record repair or link
migration is included.

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
