# Approved refresh and history correction

Baseline: 092b919fe, including the already deployed Schedule changes, which remain untouched. Owner approved fixing the eight-minute refresh and session-history 503, testing and deploying through the existing Cloud branch. Protected: Recognize identity/access rules, styling, pending edits, operation receipts, all unrelated code. Horses remain on hold.

Refresh uses the existing authenticated Profile GET views, conditional responses for unchanged packages, and the existing device snapshot/pending-edit merge. No new database, credentials, schema or scheduler service. Investigate the history exporter against its existing stored receipts before correcting the failure; do not repair production records manually.
