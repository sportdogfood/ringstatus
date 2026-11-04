# Approved refresh and history correction

Baseline: 092b919fe, including the already deployed Schedule changes, which remain untouched. Owner approved fixing the eight-minute refresh and session-history 503, testing and deploying through the existing Cloud branch. Protected: Recognize identity/access rules, styling, pending edits, operation receipts, all unrelated code. Horses remain on hold.

Refresh uses the existing authenticated Profile GET views, conditional responses for unchanged packages, and the existing device snapshot/pending-edit merge. No new database, credentials, schema or scheduler service. Investigate the history exporter against its existing stored receipts before correcting the failure; do not repair production records manually.

## Proven history failure and correction

Diagnostic release 0c0f4f3b2 succeeded through the existing branch. A normal browser load returned history_export_pending / session_identity_conflict. Queued event b5e38fe9-6b4f-43a7-8d7e-c64695ed005b is already confirmed in Footprints record recVpO0urhiJcjd93 (created November 2), session 7c3604a0-eb68-4411-9b00-966e437fb551. Its person/device links differ from summary recrNJykRMlko8b4J, which must not be rebound. No production records were manually changed.

The exporter now acknowledges the already-confirmed immutable event while recording the summary conflict in its existing durable receipt. Other failures remain pending. It returns the old session UID so only a matching browser tab can renew its history session; credentials and identity are untouched. New credential operations create a fresh history session ID, retained across retry of that exact operation. This prevents reuse across new sign-ins. Existing closed-session handling is retained.

Checks: 39 Profile/service/adapter/refresh tests passed; added pending-refresh test passed with the 11 service tests. Five focused history/credential tests and 12 session tests passed. Production build passed. Expanded old native suites: 29 passed / 18 failed, identically reproduced from untouched 092b919fe in .proof-baseline-history. Those legacy SMS/invitation fixture failures are not regressions from this correction and were not modified.

Rollback: targeted reverts of the two correction commits through the existing Cloud branch, preserving Schedule and prior desktop release. No schema migration or record rollback.
