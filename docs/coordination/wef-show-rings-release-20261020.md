# WEF show and daily ring tables — 20 October 2026

Owner authorization: build show-details, rings and rings-helper in the independent wef-schedules base, use the source-selected date and agreed links, preserve existing workflows, test/deploy and verify normal scheduled runs only. No manual business executions or record repairs.

## Baseline and scope

Source: C:\Users\gombc\.codex\worktrees\wef-schedule-collector\ringstatus\wef-schedules (checkout HEAD f5e895b6d3af5b87d1d2f0d8524061a5e7f171f9; private package is untracked, so exact baseline hashes are controlling). Deployment: C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\wef-schedules. All top-level private JavaScript source/deployment hashes matched before work. Baseline is the completed duplicate-key release: 80 tests and two normal 09:45/09:50 runs; 435 schedule records. Preserve its archive-before-delete logic in current-store.js unchanged.

Release evidence: wef-schedules/runtime/show-rings-release-20261020. baseline-code.json, schedules-before.json, protected-before.json, focus-before.json and four task XML snapshots record the starting state. No shared runner scripts, schedules, runtime credentials or deployment tasks are changed.

## Agreed behavior

Normal discovery still starts with /ring?customer_id=15, then the default schedule (date=00/00/00). selected_date is the validated source default show_date, not the local calendar date or legacy focus_day. Source-selected date missing/invalid/outside the show range fails the new stage; it never substitutes today. The original collector retains all show days and its existing date-priority behavior.

show-details (tblBq3gXC0TYBCmMZ): one stable customer/show key; show/customer/facility IDs, name, start/end dates, selected_date, available_dates, is_default and last_checked. Prior shows retained and unselected on rollover.

rings-helper (tblgFqVUAx7QlbnK8): one facility-scoped static identity; ring ID, number and name support lookup within customer/facility. Trim/case normalization applies to matching; conflicting or ambiguous identities fail before writes. Missing identity details are filled from source; existing identity values and owner notes/additional fields are preserved. No static operational facts are invented.

rings (tbliUwvXYdMhvJ3Fm): one stable customer/show/date/ring-number key. Uses all already-fetched dated schedule responses, includes rings without class rows and marks the source-selected date. Links to show-details and rings-helper. Prior daily rings are retained.

schedules receives only two new linked-record fields: show-details and rings. Every active row matches show ID + schedule date + ring number. Retained dropped rows can link to a verified historical daily ring; first adoption with neither source nor historical ring leaves the new ring link blank and reports the count. Existing schedule values, IDs, keys, row-json and archive protections remain intact.

The stage runs only after all existing schedule days and their existing finalization succeed. It makes no extra SGL requests. Writes are read back; new-record POSTs are single attempt with stable-key read-only reconciliation after a lost response. No deletion in this addition. Failures propagate to the existing run log. Success summary includes selected date, daily/selected ring counts, helper count and linked schedule count.

## Predeployment checks

97 offline tests PASS, including repeat identity stability, three-way helper matching/facility isolation, conflicting source identifiers, blank helper fill/static field preservation, no local-date fallback, lost-response reconciliation, unresolved-create stop, rollover, dropped historical ring retention, readback failures and scheduled-entry error logging/lock release. Dropped-ring regressions observed failing before the implementation correction.

Saved-source offline fixture proof PASS: one show, 24 daily rings across October 22–25, six selected October 22 rings, six static helpers and 435 schedule links; repeat creates none, all original schedule IDs/values preserved. This proof uses saved payloads in memory only, with network forbidden; it is not workflow acceptance.

## Live acceptance — PASS

Final code deployed at 11:33:41 Eastern between triggers. Normal trigger 11:35:01 completed at 11:36:14, exit 0; normal trigger 11:40:01 completed at 11:40:50, exit 0. No manual business execution or record repair.

First log: recdFSbrp8NRET2iC / e0311fc6-f5b0-4af1-8441-3afe17507d07. Second log: recRoY49kjHKXsxBP / a108aa1f-7476-4488-854c-af2ad92a939c. Both show 435 rows, four complete dates, no errors and only the existing empty-October-22 warning. Each summary documents 24 daily rings, six selected October 22 rings, six static helpers and 435 schedule links. Four archives per run, eight actual archive files/hash entries verified.

first-verification.json and second-verification.json independently read back every schedule/show/ring/helper link and all original schedule values/IDs. repeat-comparison.json confirms identical ring/helper/schedule records across both runs; show details changed only last_checked. One show, 24 daily rings, six helpers, zero duplicate keys and no repeated record creation. The October 22 ring inventory exists even though that day currently has no collected class rows.

final-boundaries.json confirms 16 protected source files, six unchanged preexisting private modules (including current-store.js) and all four collector/heartbeat/epoch/pipeline task definitions unchanged. The new stage uses no extra SGL requests and adds no trigger. Full offline suite 97 PASS; independent final review no material findings. Lost-response, rollback/date-change and dropped-ring gap branches are fixture-proven, not manufactured in production.
