# Cleanup release — November 2, 2026

Deployed 09:26:18 Eastern through the unchanged wef-live-collector task. Three existing files changed (lanes.js, airtable.js, schema.js), new cleanup.js; 19 other original JavaScript files preserved. Additive cleanup-logs schema and inventory updates only. Other collectors/user edits untouched.

49 offline PASS; author review PASS, mechanical review packet PASS. Normal 09:27 and 09:33 scheduled invocations verified, all six lane IDs in acceptance.json. First action archived/deleted 989 records with immutable hashes, original native link snapshots, durable catalog readback and exact absence readback. Eight action rows remain unchanged on repeat; retained new-show logs/stamps preserved. Second cleanup did no work, proving same-show idempotence.

Bulk reconciliation: 192 live entries, 43 class appearances, 43 classes, 27 groups, 5 rings, 378 completed heartbeat records with no show-id, 111 old-show run logs and 190 old-show errors. Completed blank-show heartbeat rows meet the owner's not-new-show rule; current invocation stamps were protected. No active record, current-show business record, reference/catalog or cleanup-log was a deletion target.

The initial 207.402s cleanup crossed one tick; repeat cleanup 2.504s. Quick 1000/500 and later canonical-change branches have offline proof only, not forced operational proof. Native reciprocal links to deleted rows disappear; original values are in archives. No rollback was needed; frozen baseline remains for an owner-authorized rollback. Stages 8 and legacy 24-hour retention remain off.
