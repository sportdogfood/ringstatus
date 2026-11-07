# Show-time and health responsibility

Owner approved creation of this JS and the cadence on November 6, 2026. No collector, discovery-policy, proxy or relay changes belong to this delta. This script is separate from WEF Shows' collector and only writes the two owner-created tables.

## What it does

- Reads the canonical `is_default=true`, customer 15 show from WEF Shows, without modifying shows or show-days. This is saved authority, not independent proof of today's default.
- Reads version.json HTTP Date, stores original header and parsed timestamp in `show-times`, and links the matching show and actual source-clock calendar day. It does not use tomorrow's planning day as today's date. Outside the show's day range, the show-day link stays empty.
- Airtable's existing `eastern-time` formula converts with America/New_York. Cache Age greater than zero is Stale; absent caching evidence is Unverified, never a fabricated freshness guarantee.
- Checks the existing shared `/health` once per due slot. Requires HTTP 200 and ok=true; accepts the source's actual ring count.
- Updates two latest-state health records, clock and decoder, retaining first observed outage and recording recovery. No automatic email, chat notification or new service is installed.
- Proxy outage does not suppress an available clock. Invalid JSON/missing Date, HTTP errors and save failures are failures. No automatic retry or catch-up burst.

## Cadence

America/New_York, including daylight saving changes. One-minute scheduler wake; JS only makes source calls in these slots:

- 07:00 through 18:57 every three minutes: 240 checks.
- 19:00, 22:00, 01:00, 04:00: four overnight checks.

244 checks per normal calendar day **per endpoint**, 488 requests total for clock plus health. Each health check additionally invokes the existing schedule path. On the fall-back day, repeated 01:00 is the same reserved wall-clock slot and is not replayed. A missed slot is not backfilled.

## Scheduled entry

Working directory: `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus`.

Use the existing Node executable and existing Shows credential file:

```
"C:\Program Files\nodejs\node.exe" --env-file="C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\wef-shows\runtime\collector.env" "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\workers\wef-schedules-decoded\show-time-health.mjs" --scheduled
```

Credential used: existing WEF_SHOWS_AIRTABLE_TOKEN. Never store token values in evidence or pass them on the command line. Existing PAT access to the newly created tables must be verified at the first authorized scheduled write; the connector's schema access does not prove the PAT's access.

Owner explicitly approved activation in this chat. Installed task name: `wef-decoder-show-time-health`, first normal wake November 6, 2026 at 16:22 Eastern. It repeats every minute; JS gates source calls to the approved slots. Hidden PowerShell wrapper `run-show-time-health.ps1` captures stdout/stderr to `wef-data/runtime/show-time-health/check-YYYY-MM-DD.log`. No manual Start-ScheduledTask or manual live execution was used. Task uses the existing Shows interactive principal, IgnoreNew for overlaps, three-minute execution limit and no automatic restart. It requires that user's logged-in Windows session, like the existing Shows task. State and concurrent-run lock belong only under `wef-data/runtime/show-time-health/`. Duplicate wakes in a slot do not re-request sources. No source calls occur when importing the module or running offline tests. Scheduled data proof remains pending until the first due wake.

Tables: base appCaSmL819KK7UFJ; show-times tblBJ8DwRklrmDNb5; health-table tblcYgKFwKtILSdU8. show-times holds latest observation per show/source-date; health holds latest status per endpoint rather than unbounded polling rows.

## Shows verification

Read-only verification November 6: existing Shows discovery remains Sunday 22:00 Eastern with three-hour retries until show changes. Last successful source check was November 2 at 01:04 Eastern; next planned source check November 8 at 22:00 Eastern. Its normal five-minute runs validate saved authority. Canonical show is 200000077; planning day November 6; saved source-selected date November 5. The proposed browser sid/date discovery is not yet implemented and needs fresh-session and repeat proof. This script does not silently change that policy or call saved context fresh discovery.

## Acceptance and remaining action

Eleven offline tests passed, covering cadence boundaries and summer/winter offset, default-show ambiguity, no credential forwarding, malformed/missing source fields, independent clock versus proxy failures, actual ring count, outage/recovery state, link/date handling, deduplication, Airtable pagination and readback. Activation approval and task configuration readback are complete. No manual collectors were run.

First due normal wake, November 6 at 16:24 Eastern: monitoring completed and independently read-back health records linked to show rec8GtJJ7eSjY3Nd3 and day recWgNiGlwpQGA14e. Clock request returned HTTP 403; no clock observation was saved. Proxy health returned HTTP 503, ok=false, rings=0, schedule_status=502. Health rows: clock recQSWkx9F6PwLWKB, decoder recBHZPxfVwugzO6s. Task exited zero because both outages were successfully recorded; this does not mean either source was healthy.

Clock acquisition acceptance is FAIL for that wake. Current clock request has Accept only and does not include the previously successful browser User-Agent. Adding that exact header is proposed, not proven to resolve the 403. Per owner AGENTS.md, separate approval to edit after this failed verification is pending. No immediate retry, task restart, source payload substitute or automatic patch was made. Repeated normal scheduled outage reporting remains separate from a manual retry/repair.

At 16:31 Eastern the owner explicitly approved the clock-header edit. Added only the previously tested iPhone Safari User-Agent to the clock request; the proxy health request stays unchanged. Extended offline assertions verify the exact header, no bearer/cookie transmission and no health-request header change. All eleven tests passed. Verification awaits the next normal due wake at 16:33 Eastern; no manual source request or task start was performed.

Normal 16:33 wake verified: clock HTTP 200, Date Fri, 06 Nov 2026 21:33:12 GMT, Eastern formula 2026-11-06 16:33:12. Saved show-time record recpDvbnrFOOTay0B linked to canonical show rec8GtJJ7eSjY3Nd3 and source-calendar day recWgNiGlwpQGA14e. Existing clock health row recovered to Healthy, HTTP 200, with recovered-at 2026-11-06T21:33:05.031Z. Decoder health remained Outage with HTTP 503/schedule_status 502; its original outage-started-at was preserved. Source cache freshness remains Unverified rather than inferred from a successful HTTP response. Repeat scheduled write acceptance remains pending.

Repeat normal 16:36 wake PASS: HTTP 200; Date advanced to Fri, 06 Nov 2026 21:36:12 GMT; Eastern formula 16:36:12, exactly three minutes after previous sample. Same show-time record recpDvbnrFOOTay0B updated, total one show-time row; two health rows retained with no duplicates. Clock stayed Healthy and kept the original recovery timestamp. Decoder stayed Outage and kept original outage-started-at. Independent connector readback confirmed both show/day links and updated checked-at 2026-11-06T21:36:05.792Z. Eleven offline checks plus two normal scheduled clock writes now verified. Overnight cadence and daylight saving behavior have offline proof only. No collectors, manual source retry, proxy edits or relay restart were performed.
