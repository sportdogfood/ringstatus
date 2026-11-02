# wef-live — locked baseline

**Locked November 2, 2026. Version 2026.11.02.13.**

Owner-requested code and workflow freeze. Further code, schema, cadence, cleanup-policy or deployment changes require explicit owner approval. This is a documented change boundary, not a file permission or Airtable edit lock.

The existing `wef-live-collector` scheduled task and normal collection/logging/cleanup continue. Read-only checks and existing scheduled code reviews remain permitted. No new observer, task or enforcement infrastructure is introduced.

Protected baseline: release2026.11.02.12, base `appgzosUcVqA4vZdf`,24 runtime JS hashes and actual task XML in [lock evidence](evidence/lock-20261102/baseline.json).

- Actual checks hourly around the clock when inactive, date-mismatched or outside the collection window. True/today detail collection every3minutes05:00–19:00Eastern. Existing hidden local dispatcher unchanged.
- Cleanup deletes only: new-show removal of nonmatching-show records; new Eastern day alone checks heartbeat/run-logs/errs-log at1000+ and deletes oldest500 completed eligible records. Same show/day does no cleanup inventory. No new cleanup archives or deleted-field backups.
- Preserve current/active records, exact delete checks, source identities, links and logs. Stage8 publication and old24h retention remain disabled. Existing archives/catalogs remain untouched.

Locking is not full operational acceptance. First1:00p.m. hourly check verified without errors; second hourly check due2:00p.m. remains unverified at lock time. Natural new-show deletion, new-day threshold deletion and populated collection under the new cadence remain unverified. Two known conditional cleanup-reporting P2 findings remain in [release review](evidence/delete-only-20261102/review.md).

No runtime code, task settings, Airtable data/schema, record permissions or file permissions changed to establish this lock.
