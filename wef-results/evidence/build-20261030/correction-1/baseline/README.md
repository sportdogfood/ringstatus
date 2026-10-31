# WEF Results

Owner: wef-results-coordinator. Approved build: code, five Airtable tables, offline checks and review. **No task activation, manual business run, source endpoint request, or source-base write.**

## Destination

Base `appK9IX59jFwUkB2u`. Actual IDs/fields are saved in `schema.json` and were read back after creation.

| Table | ID | Meaning |
|---|---|---|
| run-logs | tblXVlQ8VGETl8RLB | One invocation, start/terminal status and counts |
| heartbeat | tbld5k6m2wiKjy3pY | Latest invocation and last full success; no freshness renewal on failure |
| errs-log | tbllVvYbHdcAKmFzG | Scoped errors, each linked by scalar run ID |
| code-reviews | tbl0p1Me0zVdz1pzF | Administrative release review evidence |
| snapshot | tblraM6Oo9pxUxKzV | One current/last-known result set per exact Data class key |

`meta` is preserved. No separate domain queues/checkpoints/revision tables. Results alone writes these tables. Airtable is the only durable business store; the entrypoint's temporary overlap lock is not a checkpoint or archive. No index or business archive added. Prior results remain in the snapshot until a verified replacement; upstream Data owns its original evidence/history. Results does not claim a separate immutable version archive.

## Inputs and readiness

- Read-only Data `appdl16HJ5ETm8N69/classes/tbl0IFf067w4wUBj5`: exact class key, source-json, source last-success, fingerprint and run references.
- Read-only Live `appgzosUcVqA4vZdf/live-classes/tblNaM9ehOJFsrveV`: advisory counts, phase counts and original last-seen. Live never upgrades verification or renews Data source age.
- Canonical authority is read directly from Shows `appCaSmL819KK7UFJ/shows/tblJA5CUon5s1sWib`, using its existing authority-age helper. Calendar selection is not automatically TODAY. No collectors are imported or invoked.

Source scope must match customer/show/date/ring/class. Accept only exact `Completed`, explicit results_verified true/1, show_results true/1, a complete parsed nonempty class trips array with unique trip IDs, participation UUIDs and source result values. Re-read the Data row before acceptance; changed acquisition evidence is withheld. Group-wide counts need not equal class-only trip count; jump-off/R2 and source values are preserved without inference. Missing values, zeros, scratches and source sentinels stay distinct. No automatic no_place, withdrawal, score aggregation or invented official placing.

`finality=source-verified` records the upstream flag, not permanent official finality. Exact trip IDs and UUIDs are retained, along with entry-key `show|date|entry|class` and live-entry-key `show|date|class|trip`. Phase configuration is preserved. Material values/phase changes increment revision-number once; ordering, estimated times and repeated identical evidence do not. Source provenance includes exact record/key, timestamp, source hash and upstream run references.

## Consumption and failures

Use `reader.readSnapshot` with explicit source/check age limits, canonical show/customer and latest heartbeat. It requires a matching completed Success/Partial run, ready check-status, current timestamps and a matching content hash. A stored ready flag alone is not authorization or perpetual freshness. Consumer authentication/tenant access remains with serving/Messages; this module sends nothing and creates no timing events.

Waiting/empty/stale/unavailable checks preserve accepted results-json/content-hash. Absent classes become unavailable; old-show snapshots become historical, never deleted. Duplicate identities fail closed. HTTP failures do not trigger blind write retries; uncertain writes get read-only reconciliation. Snapshot writes use individual atomic records and readback; class groups are not one transaction. A failed run remains visible; no automatic repair or source invocation. Consumers must use source age/heartbeat to reject interrupted or old runs.

Airtable long-text maximum is enforced without truncation: an oversized class payload fails that run and is not accepted. No arbitrary class, row, attempt or historical correction cutoff is introduced. No rows are deleted. Observed request volume/performance and maximum-size acceptance remain scheduled verification items.

## Prepared execution — inactive

Existing host convention: `run-hidden.vbs` -> `run.ps1` -> Node 22 `entry.js`, private `runtime/collector.env`. Launchers are prepared only; no scheduled task was registered, enabled or started. No review automation/cadence is created. Source collector tasks and archives remain unchanged.

Activation requires separate owner approval and a private environment file with `WEF_RESULTS_AIRTABLE_TOKEN`, `WEF_RESULTS_RUNNER_ENABLED=1`, `WEF_RESULTS_MAX_SOURCE_AGE_MINUTES`, and `WEF_RESULTS_MAX_AUTHORITY_AGE_MINUTES`. Both age limits must be positive explicit policy; no historical default is silently adopted. Shows' own authority validation also applies. Credentials need read access to the three source bases and write access only to Results. No credential copied or printed during build.

Offline checks: `node --test test/*.test.js`. Build evidence and review are saved under `evidence/build-20261030/`. Live population, normal repeated scheduled runs and downstream runtime acceptance remain unverified until separately approved activation. Snapshot/run/heartbeat/error business rows remain empty during this build; the administrative review entry is separate.

## Operating guidance

Use canonical Instruction Loader, Context Compact, Code Review and Rebalancer from the installed RingStatus Operating Skills package when applicable. Canonical review: `C:/Users/gombc/.agents/skills/code-review/SKILL.md`; canonical continuity: `C:/Users/gombc/.codex/skills/context-compact/SKILL.md`. Preserve the owner's global/runner instructions and `../docs/coordination/wef-owner-operating-instructions.md`. A README does not auto-load skills, schedule anything or approve activation. Snapshot/history requirements supersede the historical five-domain-table design.

Next authorized action: finish offline checks, schema readback and review; retain task inactive. Activation and its exact recurring policy remain a separate owner action.
