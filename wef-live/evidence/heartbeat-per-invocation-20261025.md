# Heartbeat per invocation - 25 October 2026

Owner approved a new heartbeat row at every invocation, with startup stamp and terminal update to that same row. Added run-id, started, completed (text timestamps) to heartbeat. Unique heartbeat-key is the run UUID, also shared with run-logs. The old wef-live singleton row remains unchanged. No manual runs or record repairs.

43 offline tests passed, including two distinct invocations with Running visible before source work, terminal update without a duplicate row, and heartbeat outage isolation.

Normal scheduled run db4e3db8-6432-4d0c-b4f4-4878100cd84a:
- Airtable record recYTsUBnO0yXry27 observed Running, started/checked-at 2026-10-25T14:20:12.814Z (10:20:12 ET).
- The same record was read back with completed/checked-at 2026-10-25T14:21:05.026Z (10:21:05 ET), result Partial.
- Collected 5 rings, 30 groups, 55 classes, 278 trips. Existing class identity issues 200030349 and 200030331 remained unresolved; collection continued.

The three-minute schedule and hidden launcher were retained. Startup stamps do not make Airtable infallible: failed log writes are recorded as logging errors without stopping collection. A process that dies after startup can leave a Running row for diagnosis.
