# Class status — approved scope, 7 November 2026

Owner approved creating class-status with the 35 proposed fields, fixed prediction pace 2.5746 minutes/trip, offline testing with yesterday (6 November) saved inputs, Code Review, and a cleanup script/contract. No deployment, new schedule, manual production run, source-table repair or Records alerts.

Baseline: existing class-timing.js and class-adapter.js; existing pure Ring workflow pattern; existing cleanup eligibility patterns. Preserve existing production Class answers and runner. Other-owner changes already present in api.js, entry-data.js, ring-cleanup-plan.js and ring-cleanup.js are excluded.

Deliver new snapshot/cleanup modules without connecting them to the production runner. Reuse existing localInstant for Eastern schedule conversion. URL links avoid modifying synced tables with reciprocal link fields. Current shared Ring table contains a diagnostic record only. Measured Ring pace is informational; predictions use the fixed default. Existing Class timing implementation is not replaced or modified.

Offline source: yesterday-inputs.json contains read-only saved Airtable rows with original clocks. This is retrospective input evaluation, not a reconstructed historical inventory and not proof of live scheduled delivery. No provider endpoint is invoked. Unknown actual start remains unknown.

Cleanup: 7 p.m. Eastern clears eligible completed logs; three-hour check at 1,000+ rows removes oldest 500 eligible per logging table; default-show ID transition clears eligible old-show owned records. Active/current, future, unresolved, pending, synced and reference/catalog records remain protected. No independent day-0 cleanup. New class-status retention remains protected pending an approved history window; never erase useful Class snapshot history by treating it as run logs.

Table created: appeqxxJyPGS1iqdm/tblxhS1xWpfHKTd6l. 35 names/types independently read back. Table is empty until a separately authorized writer route is deployed. No existing runs table; run-logs contains runs.
