# Delete-only correction — November 2, 2026

Owner explicitly approved correction and redeployment in the current chat. Supersedes prior archive-before-delete for cleanup. Delete only: no new cleanup archive files. Existing historical archives/catalogs remain untouched.

New show: delete nonmatching-show records in checked owned tables. New Eastern day only: inspect heartbeat/run-logs/errs-log and delete oldest 500 completed eligible records when total >=1000. Same show/day: no cleanup inventory. Preserve active/current rows and exact pre-delete/readback checks. No repair, manual collector or observer.

Also deploy already-approved cadence: hourly actual status checks/logs 24h, three-minute details only when live=true and canonical priority-0 equals today, 05:00–19:00 Eastern. Existing hidden task acts as quiet local three-minute dispatcher. Stage8 remains held; old24h retention stays disabled. Other collectors untouched.

Delta: cleanup.js, airtable.js retireCleanup only, lanes.js, new live-cadence.js, existing task duration. No schema/dependency upgrade. Two preexisting cleanup-reporting P2 findings remain separate; no opportunistic fixes. Rollback baseline retained; never overwrite intervening edits or retry failed business execution without authority.
