# Single-table trips correction — November3,2026

Live author review, not independent review. Owner rejected the three separate result/message/comment tables following the approved build. Treat this as correction of that same approved scope; no unrelated architecture or new gates.

Baseline25runtime JS and task frozen. Delta only live-trips.js projects source result facts directly onto trip; lanes.js removes child-result reads/writes; schema.js removes three child-table definitions/links and adds five direct result fields; cleanup.js removes those three child targets while retaining live-trips.21other JS, source endpoints, keys, class/ring/group/schedule/legacy links, changed-only saving, cadence/task, logs/active/current protections remain unchanged.

Results are placing, numeric position, results-verified, class-status and full results-json. Raw trip source-json still preserves all original result facts. Zero/false, nonnumeric placing and verification state retained. No result-finality claims. Messages/comments use trip-key downstream; no new local tables or sending/authoring implementation.

85offline checks PASS, including direct result creation/change and stable trip/class links, source scope, empty/error isolation, UUID identity, class-source fallback, repeated changed-only saves, legacy collection, scheduler and cleanup protections. Current installed code continues until replacement. Schema direct-field creation was GET-read back; three newly created child tables GET-confirmed empty before preparation. Physical deletion must occur after code no longer references them and another immediate empty read, then schema GET-readback. Only associated new cleanup configuration rows removed; not business record repairs.

No new actionable defect found in approved delta. Known two conditional cleanup-reporting P2s unchanged. Code/offline review PASS; deployment/schema removal/normal scheduled proof remain separately recorded. Source live=false; empty idle runs cannot establish populated collection proof. No provider/manual collector/repair/new schedule/observer.
