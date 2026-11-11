# Stage 1 inventory review

Reviewed 2026-11-11 against the owner's complete endpoint-inventory request and later request to include existing implementations.

Result: PASS for static inventory and Airtable delivery. Runtime endpoint evaluation is outside this stage.

The review traced saved bundle -> request AST -> canonical route -> catalog metadata -> existing Airtable identities -> submitted field IDs -> full saved readback. Actual current table schema was inspected before writes. Select labels used existing choices. Queries and methods are grouped under one route, and exact source expressions remain available. Existing browser observations were retained without promoting them to anonymous/runner proof.

Two pre-delivery extraction defects were corrected in the analysis artifacts: Fetch request options were separated from JSON body fields, and read-only POST examples were separated from mutation classification. Session/account actions reached through GET were marked outside general collection. These changes affect inventory artifacts only.

Inspected existing implementation facts include Fast's shared `/schedule` and `/classes/{id}` readers, Live's current direct legacy readers plus supported proxy routes, Shows' browser discovery, separate clock-only show-time and current runner/fallback token handling. The catalog distinguishes these source facts from fresh deployment or normal scheduled proof. No broader negative claim is made where the inspected readers contain no implementation.

Executed offline integrity check: `node docs/coordination/evidence/sgl-stage1-20261111/verify-inventory.cjs` from the approved RingStatus checkout, Node v22.22.2, exit 0. Output saved in `offline-verification.json` at 2026-11-11T14:57:53.845Z. It verifies exact source hash/slices, 229 unique route keys, 38 preserved record identities, required metadata, exclusions and credential-literal checks.

Connected Airtable readback at 2026-11-11 14:57:15 UTC found 229 unique rows and zero differences across 4,590 submitted cells. Record references and the actual comparison result are saved in `airtable-verification.json`. The connector's rejected oversized request is recorded in README; subsequent writes were split by payload size and did not add endpoint behavior.

Approval-boundary review: changes are limited to the requested endpoint inventory table and this investigation's documentation/evidence. No production code, deployment, credentials, cadence, collectors, unrelated tables or source business records were changed. Known pending class-detail and show-time-link issues were not repaired.

Limits: source methods/parameters establish client behavior only; dynamic runtime keys and server-requiredness remain unverified. Existing implementation mappings come from inspected source, not fresh scheduled execution. No endpoint was newly tested in Stage 1.
