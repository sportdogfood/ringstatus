# Approved attempt/error logging delta

Only schedules logging boundary, run.ps1, run-logs, new error-log schema, related tests/docs. Preserve all business source collection/normalizer/store/ring/archive behavior, IDs/links/manual helpers and task XML/cadence. Normal launches use attempt-entry.js; direct collect.js is not invoked for proof. No source/collector/manual test runs or historical record repairs.

Every wrapper start/end is durably evidenced locally before env/Node; native logging begins before collector imports/lock. Separate deterministic error events. Remote-unavailable new telemetry is replayed only by later scheduled processes, bounded ten per invocation, exact-evidence ACK, all retained receipt files, original timestamps. Unknown outcomes stay unknown. Shared logging serialization protects bounded archive/reuse; dead lock fails closed. No new task/OS logging switch.

Schema error-log fields exported by error-logs.js ERROR_FIELDS: error-id, run-id, stage, recorded-at, started, date, show-id, message, run-log-url. No linked-record schema options or business table fields changed. Table creation/readback precedes deployment; healthy scheduled runs need not create artificial error records.

Acceptance: affected/full offline suite and independent review; schema fields/types readback; exact deploy hash manifest and unchanged task XML; two normal completed wrapper attempts with same IDs/start times as run-logs, terminal heartbeat/counts and no duplicate logging; first receipt ACKed by second normal process; all existing schedule IDs/keys/show/ring links preserved. Failure behavior fixture-proven only unless naturally observed. Actual scheduled path failure stops per AGENTS, no unapproved patch/retry.
