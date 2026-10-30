# wef-class-calculator owner

October 30 ownership update: class condition detection and class input validation are now absorbed into this owner. Timing coordinates shared policy and Messages/Results contracts. Read the [specialist evidence handoff](specialist-handoff-20261030.md) for source chat/turn IDs, preserved evidence, unresolved policies and explicit stops. The older status below is a dated snapshot; shell-only output, task state, retention versions and separate condition ownership must not be treated as current facts. This update makes no runtime changes and archives no chats.

Evidence checked October 30, 2026, approximately 11:07 America/New_York. This is the class owner's documentation, not a new runner/module deployment. Workspace: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`, branch `main`; inspected HEAD `ddb39509198b766883848ff156fef65548c7f3e1`. Recheck current source and task state before relying on this dated snapshot.

## Responsibility and hierarchy

Owner chat: `wef-class-calculator`, `01a16f18-25b9-7222-9b53-b64496ec84d9`. Coordinator: `wef-timing-coordinator`, `01a169bc-0bd6-77d3-b7b4-283a07061e90`. Human owner sets scope and approvals; Timing coordinator reconciles shared contracts and delivery. This owner covers group/class occurrence timing, estimated duration/end and completion evidence, class input adaptation and validation. Ring-pace owns pace/delay and the shared runner; entry timing, condition transitions, conflicts, shared freshness, Results and publishing remain separately owned.

The existing implementation is `wef-timing/class-timing.js`, `class-adapter.js`, `class-live-source.js`, `class-worker.js` and their tests. Shared `data-runner.js`, `api.js`, retention and task integration are dependencies. The ring-pace owner is the sole writer for the current shared retention release. A review finding does not authorize this chat to patch those files.

Read [owner workspace guidance](AGENTS.md), [Timing coordination](../wef-timing-coordinator/README.md), [owner operating instructions](../wef-owner-operating-instructions.md) and [review standard](../wef-code-review-standard.md). Resolve stale status paragraphs against current evidence and the latest owner instruction; do not revive the historical Schedules/Engine lane in `wef-timing/README.md`.

## Physical base ownership versus active output

`wef-class-calculator` is `appeqxxJyPGS1iqdm`. Verified owned schema:

| Table | ID | Primary field |
| --- | --- | --- |
| timing-answers | tblmoBFySYW2TkDHf | answer-key |
| engine-checkpoints | tblJzYflUc6STJRI2 | checkpoint-key |
| run-logs | tblyI9N4ehBLoRI2S | run-id |
| heartbeat | tblPkpcdX1gYWYgMA | heartbeat-key |
| errs-log | tblQXKCLO4rxw9WK8 | error-key |
| code-reviews | tblPUqiquhOTxac93 | routine |
| archive-catalog | tblLepghPma7hV2Hw | archive-key |

`meta` (`tblwBvigcmqRlSYUb`) is preserved and excluded from retention. The six-table field/type inventory is in the dated [schema receipt](../wef-timing-coordinator/class-base-schema-retention.md); its earlier undecided-retention/index statements are superseded by the later 24-hour authorization and live catalog schema, not by verified runtime implementation. Catalog fields: `archive-key`, `source-table`, `archive-file`, `content-hash`, `record-count` (integer), `scope-json`, `verified-at`, `cleanup-status`, `cleanup-at`, `policy-version`.

Live reads at this documentation check returned zero records in class `timing-answers`, `engine-checkpoints`, `archive-catalog` and `code-reviews`. Earlier same-day reads found all seven tables empty. There is no verified separate class producer, runner, operational writer or recurring archive process.

Current shared code writes both `ring-pace` and `class-timing` to ring-pace base `appYOykKIy1kAquhP`, answer table `tblhIjSjeGs5td23c`, checkpoint table `tblfWyTqeyvIvhAjb`. Its checkpoint key is `ring-pace-data-runner`. Creating the class shell did not migrate output or consumers. No consumer may switch to the empty class base merely because its schema exists.

## Source authority and answer contract

| Producer | Authority and required evidence | Class use |
| --- | --- | --- |
| Shows, appCaSmL819KK7UFJ / shows | Unique default; show-key, customer_id, show_id, start_date, end_date, selected_date, last_checked | Exact show/day authority; no fallback to another show |
| Data, appdl16HJ5ETm8N69 | Pinned immutable handoff; run-key, state-sha256, inventories, inventory_complete, original observed_at, refs and hashes; group identity, exact stage-key/class-id | Class occurrence/membership, estimated schedule and class-specific counts |
| Live, appgzosUcVqA4vZdf | Verified collections, live-classes, live-groups, native group links, frozen archives, original observation times | Observed progress/start/completion only when identity, scope and evidence validate |
| Ring pace | Compatible group identity/roster, pace answer key, content hash, input refs and freshness | Estimated throughput; otherwise labeled three-minute fallback |

Shows/Data observations must be under 30 minutes; Live under 10 minutes. Preserve original source time and valid_until. Replaying evidence does not renew freshness. Missing, stale, failed, ambiguous and verified zero are distinct. Do not invent identities, positional occurrences, starts or completion.

Identity fields are `customer`, `show`, `date`, `ring`, `group`, `class`, `occurrence`, `phase`; current phase is `base`. Scope is the JSON tuple `[source_lane,"class-timing",customer,show,date,ring,group,class,occurrence,phase]`. The worker's persisted answer key is `scope-key|runId|first16(content-hash)`. Unchanged content reuses its snapshot; A-to-B-to-A creates a new revision. Class identity remains in scope/payload, not an invented top-level class-id column.

Answer fields: `answer-key`, `scope-key`, `answer-kind`, `source-lane`, `show-id`, `date`, `ring-number`, `group-id`, `phase`, `availability`, `pace-method`, `pace-minutes-per-trip`, `evaluated-at`, `observed-at`, `reason-codes-json`, `input-refs-json`, `payload-json`, `content-hash`. Preserve exact hash/provenance semantics in `class-timing.js`: evaluation/countdown time and unrelated producer head are excluded from material hashing; consumed immutable evidence and completion transitions remain material.

Payload distinguishes start_kind/source, class counts/completion scope, pace/method/source answer, duration, estimated_end_at and actual_completed_at. Equal positive counts plus end evidence may support an advisory completion_candidate; known phase-only evidence suppresses it. Actual whole-class completion requires explicit validated all_phases_complete evidence. Estimates are not actuals or renewable serving guarantees. Downstream entry/conflict/condition/publishing consumers must honor identity, provenance, age and availability; no live integration with those consumers is established here.

## Actual cadence and dated operation

Inspected Windows task `wef-timing-data`: three-minute interval, IgnoreNew, 175-second task limit; observed Running at inspection. Existing route is repository `wef-timing/run-hidden-data.vbs -> run-data.ps1 -> entry-data.js`. Entry uses a 160-second working deadline, 165-second process limit and `runtime/data/runner.lock`. No class-specific task was found. Historical `wef-timing` is Disabled; its one-minute cadence is not the Data lane.

Saved normal-run receipt `ae7858f9-3f54-47c3-8072-de0c47243f91`, October 30 15:04:01.815Z to 15:04:31.088Z: ok=true; 202 groups and 372 classes evaluated, all unavailable, unchanged; no new snapshots or deletion; purge_status=archive_required. Source: `wef-timing/runtime/data/last-run.json` (mutable; the quoted ID/time identifies this observation). This is saved runner evidence, not a new independent Airtable acceptance test or fresh-source PASS. Current source retains the archive-required deletion gate. No manual invocation was performed for documentation.

Calculation follows existing Data/Live producers; this agent does not collect. Evaluate every eligible group/class before cleanup, without admission caps. Shared logs/checkpoint/receipt are the current operational evidence, not the class shell's empty run-logs/heartbeat. The launcher disables the task on failure. Uncertain writes/deletes, pending intents and locks require the existing owner-resolution process; never remove locks, repair records, retry or run alternate endpoints to manufacture proof.

## Retention: approved requirement, staged implementation, missing proof

The owner approved a 24-hour minimum and verified archive-before-delete for ring-pace and class-calculator. Current/future show-day records for every show, synced Live tables, incomplete runs, active controls, unresolved dependencies and pending intents remain protected. Preserve complete records/IDs/fields/links, compressed archive and content hash; independently read back archive and durable same-base catalog before exact unchanged-record recheck, intent, deletion and absence verification. Unknown age/identity/coverage blocks affected deletion. Controls/errors/reviews are archive-only in the reviewed proposal; meta/catalog are not cleanup targets.

Two-base implementation was reviewed under `C:/Users/gombc/Documents/Codex/staging/wef-ring-pace-retention-20261030`; staging is not deployment. Proposed class retention runs after existing calculations under the same scheduled runner, with separate base/table allowlists, class intent state, same-base catalog and base-specific archive root. It does not publish class answers into the shell. There is no verified class archival cadence; the proposed opportunity is after each successful eligible three-minute shared run, subject to authority/budget. No archive deletion period is inferred.

Dated independent review in this chat: 52 offline tests passed, nine release hashes matched, duplicate-readback defect fixed. A P2 repeat-progress/starvation issue was reproduced: protected records are revisited from the beginning until budget expires, potentially preventing later tables/class-base work. This describes the reviewed staged version; inspect current source/hash and subsequent review evidence before calling it resolved. Further gaps: full-volume integration of actual two-base retention, class-answer transport deletion, partial/interrupted deletion, deadline reserve, runtime credential access and scheduled archive/catalog proof. Existing acceptance verifies answers but not both retention catalogs. These are not permission to resume implementation.

## Independent periodic review

Automation inventory checked 23 local configurations; none matched class-calculator, this chat ID or wef-timing. Created and read back ACTIVE heartbeat `wef-class-calculator-monday-code-review`, name `WEF class-calculator Monday code review`, targeting this owner chat `01a16f18-25b9-7222-9b53-b64496ec84d9`. Schedule: Mondays 11:30 America/New_York, 20-minute bound; first calendar occurrence November 2, 2026. Host timezone was verified as Eastern Standard Time. App automation view and `C:/Users/gombc/.codex/automations/wef-class-calculator-monday-code-review/automation.toml` confirmed identity, target, schedule and status. Existing Data/Live/Season/Shows reviews remain separate and unchanged. This is configuration proof only, not proof the first review ran.

Review changes against the exact proven baseline, upstream contracts, actual source, downstream consumers and saved operational evidence. An author cannot substitute their own test summary for independent review; shared retention remains ring-pace-written and class-owner-reviewed. If this owner authors a future change, obtain a separate reviewer through authorized coordination. Use severity/file/trigger/consequence/smallest correction; separate code/offline/deployed/scheduled/source-freshness gates. Reuse prior review only after verifying unchanged source/tests/contracts and label the reuse.

Class code-reviews table has no record; this documentation request does not authorize Airtable review-record writes. Review outcomes remain in this chat. First scheduled execution is unverified; configuration never proves execution. Known next work is the sole writer's handling of the staged finding followed by independent review and authorized scheduled evidence, not automatic activation by this README.

## Instruction-loading receipt

Actually read for this update: `C:/Users/gombc/.agents/skills/instruction-loader/SKILL.md`, `C:/Users/gombc/.agents/skills/code-review/SKILL.md`, `C:/Users/gombc/.agents/skills/rebalance/SKILL.md`, and `C:/Users/gombc/.codex/skills/context-compact/SKILL.md`. Applied to scope/source inspection, independent-evidence distinctions, updated task-state reconciliation and exact-session telemetry. Coordinator supplied rs-skills verification that RingStatus Operating Skills 0.2.0 remains current; its plugin entrypoints were not additionally read or installed by this update. No claim of plugin loading follows from that supplied verification. The owner-specific 65% threshold and existing stops control.
