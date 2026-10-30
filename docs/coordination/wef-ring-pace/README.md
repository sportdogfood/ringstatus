# wef-ring-pace owner

## Controlling ownership update — October 30 specialist consolidation

Read [Specialist consolidation](SPECIALIST-CONSOLIDATION-20261030.md) first. Ring-pace now owns **ring conditions and input validation**, absorbing the state/condition and validity/freshness specialist evidence. Timing coordinates common policy and Records/Messages handoffs. Class, entry, conflict, Results and message delivery remain with their owners. The source chat IDs, exact evidence paths, superseded proposals, pending decisions and explicit stops are retained in that handoff. This documentation update activates no evaluator or monitor and archives no chats.

The older 11:07 snapshot below is historical where it conflicts with later verified work: current calculations are **today-only Data + Live**; ring answers go to `appYOykKIy1kAquhP/tblhIjSjeGs5td23c`, class answers to `appeqxxJyPGS1iqdm/tblmoBFySYW2TkDHf`. The current launcher reports failure without automatically disabling the normal schedule; pending-write/deletion uncertainty still blocks work. Existing task route remains `wef-timing-data` → hidden VBS → PowerShell → `entry-data.js` → `data-runner.js`. No older Schedules/Engine lane, SQLite plan or one-minute cadence is revived.

The [common freshness/completion contract](../wef-freshness-completion-contract-20261030.md) controls the approved Live-first overlapping-field semantics; desired corrections are not claimed deployed by this README. Current retention remains gated. The latest reconciled retention candidate at `C:/Users/gombc/Documents/Codex/staging/wef-timing-retention-reconciled-20261030/REVIEW-HANDOFF.md` stopped at 151/159 offline passes with eight failures. Fresh approval is required before its correction/rerun; documentation consolidation does not clear that stop. Historical proposed five-minute operational monitoring is rejected and must not be revived. Existing independently approved review schedules are unchanged.

Updated October 30, 2026, 11:07 Eastern. This is owner documentation, not implementation or deployment approval. Read [workspace instructions](WORKSPACE-INSTRUCTIONS.md) on resume.

## Identity, hierarchy and scope

Owner chat: `01a16f18-1cfe-7663-85a5-71dc2bbe3576` (wef-ring-pace). Actual workspace: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`. This dedicated owner folder does not change shared root instructions or ownership of shared `wef-timing` code.

Human owner sets scope and approvals → wef-timing-coordinator `01a169bc-0bd6-77d3-b7b4-283a07061e90` coordinates contracts, integration, independent review and scheduled acceptance → ring-pace owns pace/delay interpretation only. Codex inspects and implements specifically approved repeatable changes; registered runners execute business workflows. A chat agent is not a collector or runner.

Implemented responsibility: schedule-window estimated pace and compatible observed interval throughput, explicit availability/reasons, exact scope/provenance, immutable answer revisions and interval baselines. Exclusive planned responsibility also includes supported ring current/next/completed interpretation, projected group finish, drift and downstream delay. Those additional outputs are NOT established as implemented. Class owner owns occurrence duration/start/end and its fallback; entry owner owns entry go-time/order; state/conflict/freshness owners retain their separate contracts. Results, messaging, publisher and Astro serving are separately owned. Shared runner integration assignments do not transfer their calculation ownership.

## Sources of truth and consumers

| Producer / destination | Exact facts and keys |
| --- | --- |
| Shows `appCaSmL819KK7UFJ` / shows `tblJA5CUon5s1sWib` | Canonical unique default customer/show and selected date: `show-key=customer_id|show_id`, `customer_id`, `show_id`, `is_default`, `start_date`, `end_date`, `selected_date`, `last_checked`. Selected date is not automatically today. |
| Data `appdl16HJ5ETm8N69`, retained local handoff | Pinned `wef-data/runtime/handoff/head-<customer>-<show>.json`; exact run/head/hash and authority fence. Verified complete dated inventories carry customer/show/date/ring/group, `stage-key`, `class-id`, `start-time`, `end-time`, `total-trips`, type, original observation and archive/collection references. No live acquisition by Timing, Schedules fallback or SQLite. |
| Live `appgzosUcVqA4vZdf` | `collections` with successful dated completion, row count, fingerprint and archive-file; archived groups with customer/show/date/ring/group, class roster, total/gone, last-seen, status and source-json. Validated class evidence is consumed by the class owner. Main/JO/R2 are distinct; do not relabel counts or infer actual starts from earliest trips. |
| Ring destination `appYOykKIy1kAquhP` | Base name wef-ring-pace, formerly wef-timing. `timing-answers` `tblhIjSjeGs5td23c`; `engine-checkpoints` `tblfWyTqeyvIvhAjb`. Shared current runner also writes class-timing here; no production destination transfer to class base is established. |

Shows/Data freshness must be under 30 minutes; Live under 10 minutes. Equality expires, future timestamps are rejected. Replay/new publication does not renew the original observation. Data/Live archives prove inputs, not archival of Timing outputs. Missing/stale/partial/unavailable facts must not become zero, fresh readiness or completed operation.

Ring `scope-key` is `data_live|show|date|ring|group|base`; customer identity remains in payload and validated context (not an extra invented scope segment). Runner `answer-key` is scope + run ID + first 16 content-hash characters. Consecutive unchanged content is a no-op; returning A→B→A changes get new revisions. Preserve `answer-kind=ring-pace`, source-lane, show-id/date/ring-number/group-id/phase, availability, pace-method, pace-minutes-per-trip, evaluated-at, observed-at, reason-codes-json, input-refs-json, payload-json and content-hash.

Schedule pace requires one supported class member, positive same-day window/count; multi-class aggregation, breaks/schooling and invalid evidence stay unavailable. Observed pace is elapsed observation minutes / newly completed trips with matching roster, totals and membership; preserve both endpoints. It includes pauses and is not individual trip duration or an invented actual start/end. Compatible observed pace is preferred to schedule estimate. Class timing consumes verified stored pace and owns its labeled three-minute fallback.

Snapshots are immutable as-of evidence, not a serving lease, current-answer API or committed downstream publication. An incorrect pace/identity/freshness claim can contaminate class timing and later entry/conflict/notification decisions. Ring answer saves do not prove Messages, Results or publication handoffs. The manifest/head publication protocol and those integrations remain unverified/unimplemented under current scope.

## Actual workflow and cadence

Read-only task inspection at this update: `wef-timing-data` was Running, every **3 minutes**, `IgnoreNew`, Windows task limit **175 seconds**. Existing route: `wef-timing/run-hidden-data.vbs` → `run-data.ps1` → `entry-data.js` → `data-runner.js`, directly in this repository. Internal deadline 160 seconds, process hard limit 165 seconds. No agent is in this execution path. The old `wef-timing` Schedules/Engine lane is historical and must not be revived.

Each eligible invocation validates Shows and pinned Data, reads verified Live evidence, evaluates every eligible group/class without processing caps, writes changed snapshots, independently reads exact written ID→answer-key associations and fields, then persists checkpoint. It protects pending deletion intents and source changes. Existing calculation scope includes eligible future days; changing to today-only remains a separate decision. Producers collect independently; Timing does not launch them.

Latest saved receipt inspected: `wef-timing/runtime/data/last-run.json`, run `ae7858f9-3f54-47c3-8072-de0c47243f91`, **October 30, 11:04:01–11:04:31 Eastern**. It reports ok=true, 202 groups and 372 classes evaluated, all unavailable/unchanged, zero writes/deletes and `archive_required`. This is a saved receipt, not new independent Airtable acceptance. The 11:07 invocation was running when inspected; its result was not claimed.

Current source has fixed `archiveReady=false`; there is **no active Timing archive cadence**. Every-three-minute checks reporting archive_required are not archival. Logs are `runtime/data/runner.log` and `last-run.json`; `runner.lock` protects overlap. Canonical checkpoint contains baselines, source proof, outcome and any purge intent. Hard failure stops the task through the existing launcher; no blind retry, alternate run or lock removal.

Source checkout HEAD inspected: `ddb39509198b766883848ff156fef65548c7f3e1`. SHA-256 at inspection: data-runner.js `C01DC35E7BA585487EF6843B834AA1FA9442B2D5D2CECABCB8645E996590B867`; pace.js `B81A95C94A1CD18C428978ED89C5273C5035C40685CB0A3490A20994C154BB2D`; api.js `339D39A13364BDD0BEDCCCBC50DF0230B6536D473A391A6C151126967E2A54F0`. These are inspected source hashes, not proof of a running process's loaded version.

## Retention policy and explicit stop

Required policy for both ring and class bases: **24 hours minimum age**, full-record compressed archival with exact IDs/fields/links, verified decompression/hash/one-to-one coverage, durable base-local catalog write/readback before deletion. Protect current/future show-day and operational dates for every show, ambiguous identities, active checkpoints, unresolved evidence and synced Live tables. Preserve exact intents, deletion response and absence proof; uncertain outcomes stop. No archive/index disposal is authorized.

Owned ring operational tables: run-logs `tblpWjnTv7S0CBKLV`, heartbeat `tblOeNxsfjCawlEmu`, errs-log `tbliHowblBdSL51ov`, code-reviews `tblZ9hOKkfGMAPp8v`. Catalog `tbl1qyii3z32hBiq9` exists from prior independent schema readback. Table existence is not writers, review execution or archival implementation. Active/mutable checkpoints, unresolved errors and reviews cannot be deleted merely because they are old. Meta and Live synced references are outside cleanup.

**STOPPED:** two-base retention package `C:/Users/gombc/Documents/Codex/staging/wef-ring-pace-retention-20261030`. Earlier 144 offline passes are dated evidence superseded for readiness by independent failed starvation verification: repeated cleanup passes can revisit the same records indefinitely and miss later records or the class base. Coordinator reported this failure and requested fresh **APPROVED TO EDIT**. Do not patch, deploy, rerun failed verification or resume it under documentation/review authorization. The earlier duplicate-record deletion comparison was fixed in staging; that does not resolve starvation. Staging README/manifests are not release approval.

Class base `appeqxxJyPGS1iqdm` and its catalog `tblLepghPma7hV2Hw` exist; its separate retention pass remains stopped staging, not wired production. Existing class answers still go to ring base. Runtime class GET access was coordinator-reported; class catalog write/delete permissions and two-base archive/deletion/scheduled proof remain gaps. Do not invent a class producer or migrate outputs.

## Review, evidence and next action

Before this setup, local automation inspection found no ring-pace/timing-specific review. Owner-authorized review heartbeat **wef-ring-pace-monday-code-review** is now ACTIVE and readback verified, targeting this exact chat, **Mondays 11:00 America/New_York**, maximum **20 minutes**. First scheduled opportunity: November 2, 2026. Configuration lives at `C:/Users/gombc/.codex/automations/wef-ring-pace-monday-code-review/automation.toml`. Host/app availability is required; no execution has yet been proved.

The heartbeat is strictly read-only: saved evidence/source/task inspection, no tests on stopped work, no file/Airtable writes, no runs, fixes, schema/task/deployment changes, agents or cross-chat messages. Quiet on unchanged/non-actionable state. It does not replace independent review by a different reviewer before release. For own authored code, label owner review accurately; ask coordinator for independent review under the accepted assignment, never claim the writer reviewed itself independently.

Periodic Airtable review-record identity is **not established**; no record was created under this documentation task. Preserve existing code-reviews table. Review results live in this chat unless separately authorized otherwise. Keep findings separate from offline tests, deployment, scheduled acceptance, source freshness and archive integrity. Reuse independent findings only after matching actual reviewed source/tests/contracts; findings never authorize fixes.

Immediate authorized outcome: maintain these owner documents and the bounded review heartbeat; send evidence-based receipt to coordinator. Retention next action is an owner decision on fresh APPROVED TO EDIT, followed by scoped correction, independent review and separately authorized safe deployment/normal scheduled proof. No business changes are authorized by this README.
