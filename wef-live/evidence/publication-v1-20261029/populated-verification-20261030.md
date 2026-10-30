# Populated Live verification — 30 October 2026

**PARTIAL, not release acceptance.** Two naturally populated scheduled invocations completed with exit 0; heartbeat and run-logs both report Partial. No manual collection, source requests, task changes, repairs or runtime edits.

| Normal run (Eastern) | Schedules verified in-run | Data verified in-run | Other outcome |
|---|---:|---:|---|
| 08:23:13–08:24:53 | 6 rings + 28 schedules | 5 schedules + 32 trips | One additional Data trip confirmed by 08:26 reconciliation |
| 08:26:12–08:27:52 | 4 rings + 28 schedules | 29 trips | One ring intent unresolved in selected evidence; 208 Data plans deferred |

Both collected 6 rings, 17 groups, 27 classes and 175 entries. 26 successful class-detail datasets per run were independently revalidated from saved source; class 200030828 remained unresolved for trip 200396076. No Data rings/groups/classes were newly patched during these selected invocations.

132 immediate verified writes plus one later reconciliation. Every audited verified before/readback comparison preserved non-overlay fields; current GET readback retained all affected records, keys and native links. Exact or newer observation envelopes were found. Source observations were 32.429–78.058 seconds old at intent; no stale-time renewal was used. Deployed hashes match. New source receipts with unchanged values advanced freshness; zero explicit no-op counts in these two runs, so naturally repeated identical-receipt no-op proof is not established.

## Exact records and values

[Full bounded report](populated-verification-20261030.json) contains all 134 intents: lane/table/record ID/key, complete exact JSON field value, compact component values, original observed timestamps, source archive references, intent/outcome/reconciliation file paths, protected-field comparison and current destination readback. Only live-observation-json is the publication write. Older components retained inside an envelope are distinguished by their original observedAt.

Example: Schedules record recptl4rwVaVWM15j, key 200000076|2026-10-30|200028192_770: group Underway/isLive=true; class total=40, gone 12→13, joGone 11→12, r2Gone=0, currentTripId 200399283→200399317.

Example: Data trip recCC0Fy3iyCeGlgg, key 15|200000076|2026-10-30|1|200030775|200247590|EDE31ADA47DA42F2B518D8BB546014E2: tripId=200399400, entryId=200247590, horseId=147815, riderId=24454, trainerId=24454, orderOfGo=9, actualOrder=9, scratch="0", gone=1, position="11", identityDifferences={}.

## Remaining blockers and omissions

- Class 200030671: multiple source groups; occurrence counters withheld in both lanes.
- Data occurrence ambiguity: 200030805, 200030786, 200030818, 200030797, 200030815.
- No exact Data trip match: 200399527, 200399531, 200399548, 200399546, 200399547, 200399545.
- Publication lane budget exhaustion; 208 Data plans explicitly deferred in second run. The first run reports deferred=0 despite stopping after an uncertain write; do not interpret that as all targets processed.
- Class 200030828 / trip 200396076: missing decoded identity; other classes continued.
- Known separate detail flag case mismatch (has_JSON vs has_json) remains uncorrected.
- 15 and 16 error-log rows match the selected Partial runs. No logging failures.

Surveyed 165 populated saved receipts, 00:05–08:26 Eastern, with no fully successful pair. Natural activity is now present; acceptance is blocked by incomplete publication/matching, not absence of activity. No fix is authorized by this verification.

Schedules and Data remain independent acquisition owners. Entries is untouched and linking-only.
