# Rollover labeling and retirement scope — 29 October 2026

Owner authorized wef-live rollover labeling and same-date old-show cleanup, preserving links and archive-before-delete; other collectors untouched. No manual production runs or record repairs.

Implementation: `rollover.js` labels records outside the validated canonical show/date as historical before upcoming/empty/inactive skips. `scope-status` text field added to the four live observation tables; fresh observations write current. Previously current participation labels become historical without changing native links or source observation timestamps. Changed rows are archived first. Repeated historical labeling is a no-op. Ambiguous/stale authority blocks labeling. This extends the earlier empty-response preservation behavior with explicitly authorized historical labels.

Cleanup: collector selection and Airtable deletion guard share canRetire. Prior dates remain eligible; same-date rows with a valid different show ID are now eligible. Current-show/current-date and future-date rows remain protected. Existing successful populated collection gates and exact archive-content/hash verification remain required. Empty runs do not delete data. Existing task executes this checkout, still PT3M.

Offline: 49 tests passed. Includes label/link preservation, repeat no-op, invalid authority/archive failure, same-date old-show retirement, active/future protection and adapter rejection of invalid archive hashes. No live endpoints invoked by tests.

Natural run d0db13e4-9987-4935-b080-6825a909c778: 08:26:14–08:27:16 Eastern. Success in heartbeat recQ2c8SPzOxsm7gQ and run-log recWKYwoij9PQAncP; no errs-log rows or logging errors. Four verified rollover-before archives contained 5 rings, 31 groups, 56 classes and 305 entries. All 397 now historical; two formerly current participation labels changed to historical. Existing unmatched/unresolved participation labels retained with historical scope.

Sorted record-ID and field SHA256 comparisons (excluding only scope-status and participation status/reason/checked-at) matched before/after:
- live-rings: 8e17253682fd37bf88e07e9573178604f1d9bc8b771ec7db52b743df22327126
- live-groups: a70419523e2ea7e03bf009d3c3cd8eef5f960627824c2a2895f06ed053d575c7
- live-classes: 7df02d2ed1cf89859234dcaebdc712646132db91f493f3c152a094cc995f11aa
- live-entries: bf31fd979f86bbfcfc8a5111859960c32c5aa7d35916861ed7ff161aa92a069c

Actual same-date old-show deletion remains unproven naturally: no such records or populated new-show live feed were available. Do not seed rows or trigger a run to manufacture proof. Populated new-show links, sync-lag acceptance and multi-group proof remain separate open gates.

Repeat natural run bb35e0f3-92f0-4151-8c70-f8773814ffe7: 08:29:13–08:29:33 Eastern. Heartbeat receswo4m5PQfXjSn and run-log recl5UpLE3HfDD7d9 Success, no errs-log rows, no collection/logging errors, zero repeat rollover archives. All 397 historical rows and all four protected-content hashes unchanged. No business rows deleted in either run.
