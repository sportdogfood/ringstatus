# Correct the cleanup contract — November 2, 2026

Owner correction: Live ignored the agreed cleanup triggers and distributed the old implementation to Data/Ring; owner directed action. Current task corrects the implementation to the already-settled rules and corrects those handoffs. No new design, task, dependency, schema or provider call.

Contract: new canonical show -> archive/remove nonmatching-show records in checked owned tables; new America/New_York calendar day -> check only heartbeat/run-logs/errs-log, at1000+ archive/delete oldest500 completed eligible records; same show/day -> no log table inventory, archive, deletion or Airtable cleanup action. Original archive verification, exact native snapshots/IDs, active-record protection, catalogs, stops and normal-run boundaries remain. If both show and day change, run both actions once in their existing order. Below-threshold new-day checks are recorded Skipped using existing fields.

Smallest code delta: cleanup.js only. Store last handled Eastern day alongside show in the existing local checkpoint. For legacy checkpoints derive day from original completed timestamp, preserve its clocks/IDs, and add day only through the recurring runner. Same-day checks return before opening an action stop or reading cleanup/log tables. Checkpoint new show/day only after successful actions. No record repair or forced source/day change.

Data/Ring were directly notified that old cleanup.js hash292d8486c531982bdabafb19df491e2542f42967ba559964dd6ef42263f903a2 does not meet this contract. Corrected source/evidence will supersede that source handoff; no new live acceptance is claimed until deployed and observed.

Protect the separately prepared, not-deployed live-status/date cadence change. This cleanup correction does not deploy it, change the task/cadence or fix the two conditional reporting P2s. Do not claim the old989-row acceptance or cadence71-test proof validates these new trigger rules. No manual collection or cleanup invocation against real APIs is permitted.
