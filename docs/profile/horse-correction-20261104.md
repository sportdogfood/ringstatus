# Approved horse corrections — November 4

Owner approved edit and deploy after the read-only review of b7f2e36f4. Exact delta: unbind native Workers fetch; merge horse package without replacing newer Profile/Team state; retry requested-but-failed horse package at the existing refresh; restore existing Owner Add/Edit controls and their actor-scoped browser storage. No Airtable horse writes, schema, styles, dependencies, Recognize, Schedule, or refresh interval changes.

Baseline: b7f2e36f4. Preserved unrelated untracked evidence and Schedule changes. Corrective source: three application files plus two targeted test files. Existing local horse edits remain local overlays; existing save notice explicitly says saved in this browser. Airtable source snapshot remains separate.

Validation: 45 targeted tests PASS (including actual Workers native fetch, stale membership reply, failed-load refresh, existing saves/identity/refresh/UI contracts). Build PASS. See horse-correction-tests.txt and horse-correction-build.txt. No live success claimed at this checkpoint.

Deployment: existing origin/work/inputs-crm-20261006, existing Cloud environment 110f06dd-c1ea-4839-98af-d829cbe77941, mount /test. Rollback: revert only this correction commit; no migration or record repair. Live verification next: nine horse IDs, cached reload, natural eight-minute refresh.
