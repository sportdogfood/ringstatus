# Owner-approved status error retry — November6,2026

Explicit owner approval: failed live-status checks every3minutes; confirmedfalse remains hourly; test/deploy/verify four direct feeds through normal scheduled runs. No manual runs or other changes.

Baseline includes already-deployed exact User-Agent on activity.js/source.js, all25runtime JS,sharedreader and task. Delta is only live-cadence.js: unavailable status interval60->3, and interpret previous unavailable-status checkpoint at original checked_at+3minutes, aligned to scheduled minute. Normal runner updates its own checkpoint; no manual state edit/forcedtask. Valid false/date/window and all other behavior stay identical. Keep URLs/query/other headers/proxy/sharedreader/schema/task/cadence definitions/credentials unchanged.

Existing3minute task invokes corrected due calculation. Confirm all four feeds by accepted normal savedsource receipts,request labels,actual source data and Airtable readback; do not substitute diagnosticsource probes. No observer,newtask or manual collector/repair. Known other holds/findings unchanged. Scope is this exact correction plus finishing the already-approved User-Agent normal verification.
