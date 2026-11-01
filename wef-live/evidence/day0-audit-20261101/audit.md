# Day-0 audit — wef-live

Date: November 1, 2026. Show: **200000076**. Canonical day: **2026-11-01, priority 0**.

Read-only audit of normal scheduled-run evidence and Airtable readbacks. No manual collection, provider calls, repairs, code, task or Airtable changes. Evidence snapshots were gathered from independently running producers; they are not one atomic transaction.

## Main finding: collection runs, but class coverage is incomplete

My earlier description of “77 missing classes” was incorrect. It compared retained Data records with current Live membership without reconciling their source times.

| Inventory | Distinct classes |
| --- | ---: |
| Retained Data day-0 class records | 118 |
| Present in latest saved schedule snapshot | 52 |
| Of those, listed by Live feeds and collected | 41 |
| Of those, absent from Live feeds | 11 |
| Retained Data classes outside latest schedule snapshot | 66 |

Data stores 139 day-0 schedule rows. The latest shared schedule source timestamp, **2026-11-01T15:48:26.601Z**, covers 54 occurrences / 52 distinct classes. The other 85 rows have older October 31 source times. These 66 class records are retained outside that latest snapshot; this does not prove they were canceled or should be deleted.

**Evidence limit:** Data's current fast collector does not preserve the complete original schedule response body. The 52-class inventory is reconstructed from saved row clocks and the producer implementation. Complete upstream inventory certification remains unproven.

Live's saved ListAjax and LiveScoreWidget responses both list **25 groups / 41 classes**. No parser rejection or hard 41-class cap was found. The Class lane discovers classes solely from ListAjax: this explains why the 11 schedule classes receive no current Live class-detail collection. Wellington's internal reason for excluding them is unknown.

### The populated omission

**200030717 — SFHJA Junior/Adult Hunter Seat Medal (Flat Phase)**, group **200028385**, ring **3 / Rost Arena**:

- Saved class source shows **4 trips, all completed**, results verified.
- Trip IDs: **200399174, 200399921, 200399892, 200399581**.
- Never listed in the 55 distinct saved day-0 ListAjax responses inspected; absent from the inspected Widget response too.
- It is a flat / under-saddle class with “No Score, No Jog” metadata. That is observed evidence, not proof of a general exclusion rule.

The other 10 omitted classes have zero schedule trip totals. Four nevertheless contain **7 scratched trip records** in saved class sources, so they must not all be described as empty. See [all 11 omitted classes](missing-11.md).

## Collection and identity checks

Fixed normal Class run **606d32d3-4472-41bb-be9c-b5ddcf6dd26c**, started **15:51:03.333Z / 10:51:03 Eastern**:

- 41 live/decoded response pairs; archive hashes verified.
- 148 live rows and 145 decoded rows; no invalid show/date/class scope found.
- Native saved group/ring values and 145 decoded entry identities matched the inspected sources.
- Three unresolved identities were logged individually; collection continued:

| Class | Trip |
| --- | --- |
| 200030808 | 200399341 |
| 200030811 | 200399094 |
| 200030762 | 200399914 |

Stored day-0 rows: **5 rings, 27 groups, 43 classes, 191 entries**. Current feeds cover 25 groups / 41 classes; two classes/groups are retained after disappearing from source membership. The fixed Class snapshot covers 145 entries; 46 stored entries are outside that snapshot. They are not automatically invalid. Cleanup is on hold.

## Links

- No broken local ring/group/class references or entry key/class/trip identity mismatches across the 191 stored entries.
- Current 41 classes' ring/group associations match saved source.
- Both multi-group classes, **200030701 and 200030830**, retain group links to **200028410 and 200028411**.
- All 42 referenced synced Schedule targets inspected are present, correctly scoped and not dropped.
- **Gap:** class **200030830** lacks its Schedule-occurrence link for group **200028411**. This is a preserved link gap, not evidence of a new successful match.
- No reverse Entries dependency is used. This audit does not certify every retained historical external link.

## Diff reporting is incomplete

Decoded class-level business fields are archived but are not included in the native business-field/diff mapping. For example, class **200030826** changed `results_verified` from 0 in the 10:27 source to 1 in the 10:42 source; the class diff mapping cannot report that change. `actual_time` is also omitted from that mapping.

Therefore, “all collected business fields have diffs” is **not satisfied**. Existing field diffs cover the mapped subsets, not every business field in the full decoded payload.

## Cadence, timings and logs

- **23 normal invocations per lane**, 09:48–10:54 Eastern: no stage failures or logging failures in those 69 receipts.
- The first Class invocation logged missing shared group references during cold start; subsequent populated collections continued.
- Fixed 10:51 readback confirms three heartbeat stamps, three run-header logs and three individual error records. Earlier accepted 09:51/09:54 populated runs and diff readbacks remain separate release evidence.
- Every lane measures collection and save separately. Lane completion/core totals exclude final logging. The measured 10:27 full task wrapper took **11.29 seconds**, including final logging; that does not establish capacity for 118 classes.
- All lanes start together in one scheduled process, every three minutes, 05:00–19:00 Eastern. They save independently, but a whole-process hang/overrun could affect all lanes' next invocation. Evening stop has not been naturally observed in this audit.
- **Heartbeat visibility gap:** new heartbeat records have blank show/date fields; run-logs has context. Day-filtered heartbeat views may hide those stamps.
- Native `last-seen` advances only on business changes. Successful unchanged polls are recorded in local lane coverage, so native `last-seen` is not the last successful collection time.
- Immediate coordinator-chat alerts are not installed: owner accepted errs-log-only failures for this release.

## Canonical context and held work

wef-shows identifies show **200000076**, November 1, priority **0**. Its retained entry totals are from October 31 and publication is Unknown / latest attempt Not active; they are not verified November 1 readiness totals. Live currently does not gate collection on them.

**Stages 8 (cross-base publication) and 9 (archiving/cleanup, including 24-hour log retention) remain on hold and absent from the new execution path.** This release does not prove current wef-data publication or recurring cleanup. Old code/catalogs remain preserved.

Raw live-status boolean and HTTP response cache headers are not saved, limiting retrospective source-status/cache proof. Deployed proxy inspection found caching disabled, but this cannot prove Wellington's internal freshness.

## Acceptance

| Area | Result |
| --- | --- |
| Canonical day/show alignment | PASS for inspected snapshot |
| Normal cadence and continuing after record errors | PASS for audited interval |
| Saved source scope, identities and local links | PASS for resolved records inspected |
| Full latest-schedule class coverage | INCOMPLETE: 11 absent, including one populated class |
| Complete original upstream schedule inventory | UNPROVEN: full response not retained |
| All business-field diffs | FAIL: decoded class-level fields omitted |
| Schedule occurrence links | INCOMPLETE: one missing occurrence |
| Complete identity recovery | INCOMPLETE: three unresolved trips |
| Publication and cleanup | ON HOLD, not executed by new lanes |

No fixes or operational changes were made. Per-class accounting is available as [CSV](all-118-class-accounting.csv) and [full JSON](all-118-class-accounting.json). Machine summary: [summary.json](summary.json).
