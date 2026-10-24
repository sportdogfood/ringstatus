# Persistent identity mismatch - October 24, 2026

Owner approved class-level unresolved handling, with no manual collector runs or record repairs.

Changes confined to wef-live/collector.js, entry.js and their tests. The existing
identity validator and one paired refresh are unchanged. Both source attempts
remain archived. A persistent LIVE_IDENTITY_SNAPSHOT_MISMATCH is recorded in
report.unresolved, collections.status=unresolved and errs-log. Run-logs and
heartbeat report Partial when no other failure occurs; operational failure is
still Failed. Existing class rows and their links are retained unchanged, as are
existing entries. Other classes continue normal collection. Retirement remains
blocked while any class identity is unresolved. No historical identity fallback
or fabricated binder record is introduced.

Existing collection counts/fingerprint are retained on unresolved updates, not
overwritten with zero. Status and the latest source archive explain why these are
not a fresh successful observation. A later valid source pair restores complete.

26 offline tests pass, covering continuation of another class, preservation of
class/entry links, no retirement, error logging and recovery on a valid pair.
Two initial test runs retained a superseded expectation that mismatches globally
fail; that expectation was updated to the newly approved unresolved behavior.

Normal scheduled run ef0750be-f8ef-4a17-986b-3af89d43bec9, October 24
10:57:13-11:03:19 ET, verified. Airtable run-logs and heartbeat show Partial;
classes 200030333, 200030429 and 200030330 are unresolved in collections and
each has an errs-log message. Other classes supplied 267 live entries.
All three affected class rows and 28 existing affected entry rows (including
their links) exactly match the run's archived before snapshots. Both paired
source attempts remain archived per class; zero retirement archives occurred.
Readback evidence: unresolved-normal-run.json. Five-minute task configuration
unchanged; no manual trigger or record repair. The earlier run started before
deployment and retained its historical failed outcome.
