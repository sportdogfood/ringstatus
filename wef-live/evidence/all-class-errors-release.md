# All class errors: October 24, 2026

Baseline: identity mismatches already logged as unresolved/Partial; normal runs
10:57 and 11:07 verified continuation. Other class errors still made the run Failed.

Approved delta: ordinary per-class fetch and validation errors now use collection
status error, remain logged in errs-log, and yield Partial rather than globally
Failed when the rest of the run completes. Identity mismatches retain unresolved.
Both source promises settle before archiving: successful halves and error messages
are preserved. Archive and audit-write failures remain operational failures.
Affected class/entry records and reciprocal group memberships remain unchanged.
Any class error/unresolved identity still blocks retirement. No inferred empty data.
Existing cadence, show authority, activity skips, decoder and other modules unchanged.

Changed code: collector.js, entry.js, test/collector.test.js, test/entry.test.js.
32 offline tests passed. Independent read-only reviewer found a reciprocal group
link gap; corrected and covered by a changed-group test. Re-review reported no
remaining blocking findings. Reviewer did not execute tests or production requests.

Deployed source hashes at 11:16:31 ET:
collector.js 65D86A285E586927A1FBE8AC09706D479B2EC4E8C064B10538F2428E6F18A673
entry.js 3C845EBE8FC5069E41DDBCA654379EE1CA0A5F7A16E30C3D390D44F66CDF7693

Verification blocked: first post-change run c06ff203-e64b-402e-88ae-6588a65f5b84
(11:17:13-11:21:40 ET) had two unresolved identities, then timed out during audit
logging after dataset-class-200030363 was archived. Run-logs and heartbeat show
Failed; errs-log contains the timeout and both unresolved messages. No ordinary
non-identity class fetch error occurred in this run, so that behavior remains
offline-tested only. All 91 existing class rows and 501 existing entry rows,
including links, were unchanged on readback against the run's before snapshots.
Evidence: all-class-errors-failed-verification.json. Two accepted normal runs
were not achieved. No corrective patch/retry after this failed verification.
The owner-authorized scheduler continued automatically at 11:22; its state was
not changed. No manual runs, endpoint diagnostics, forced failures or repairs.
