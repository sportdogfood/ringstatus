# Owner operating instructions for WEF agents

Owner-supplied October 30, 2026. These instructions preserve applicable platform/global/repository precedence, existing approvals/stops and each module's agreed scope. Reading or receiving this document does not authorize code changes, failed-run retries, manual collection, record repairs, migrations, deployment or publication.

## Required owner instruction block

Use $instruction-loader: read and apply Instruction Loader.

Follow all applicable instructions and the agreed scope throughout the task. Recheck them when resuming, changing direction, or handing off.

Own the authorized outcome end to end:

- Look downstream before acting: identify dependencies, integration, deployment or delivery, downstream consumers, and acceptance criteria.
- Inspect and reuse existing working implementations and established deployment routes. Preserve proven behavior.
- Make routine decisions within scope and continue until the accepted outcome is verified. Partial progress, a completed substep, or an unavailable preferred tool is not a reason to stop.
- Deliver a maintainable, repeatable solution that handles future runs—not a one-off fix.
- Finish all independent authorized preparation and checks before asking for missing input or permission. Never ask again for permission already granted.
- Stop only for completion, an explicit stop, or a demonstrated blocker affecting the next action. State the exact blocker and ask for the smallest necessary decision or permission. Continue unaffected authorized work.
- Preserve approval, runner, deployment, publication, and time/budget boundaries. Persistence does not authorize bypassing them.

Keep updates brief and factual: result, evidence, and exact next action. No long explanations, self-diagnosis, vague blockers, or offers to continue instead of doing the work. Never claim completion beyond verified evidence.

Read and apply these skills:

- $context-compact: Preserve the current objective, decisions, approvals, evidence, blockers, and exact next action before compaction. Never claim compaction ran without evidence.
- $code-review: Review the actual changes against requirements, proven behavior, and downstream effects. Report actionable findings and verification gaps.
- $rebalance: Reconcile the work with current instructions, owner corrections, evidence, and scope; then continue toward the authorized end-to-end outcome.

Keep responses brief. Preserve explicit stops and approval boundaries. Partial progress is not completion or a reason to stop.

## Current responsibility boundaries

The owner sets scope/approvals. Records coordinates contracts, evidence and handoffs; it does not own another module's acquisition or act as the runner. Each module owns only its accepted implementation and publication contract. Local wef-season-coordinator retains both Data acquisition and separately Season retention; the temporary Data fork handoff was reversed. A future split is deferred to the owner after stability.

- Shows owns canonical show identity/discovery/default selection and canonical show-day publication. Data is the producer of accepted count evidence; Shows verifies that evidence. Counts retain coverage, source-age and Unknown/Stale/verified-zero distinctions.
- Data owns source acquisition, accepted schedule/entry/trip/entity facts, exact existing identities, original source/evidence/archive hashes and verified revisions. Historical retained rows are not automatically current membership; current-occurrence delivery remains a migration design gap.
- Live owns independent TODAY observations, validated observed progress and its explicitly approved one-field publication overlays. It does not own acquisition fields/native links in Schedules/Data or prepare/enrich Entries. Recorded publication defects and Partial acceptance remain open until separately approved corrections are verified.
- Season owns retaining accepted Data history/entities/memberships, add/upsert/dedup/indexing, local native links and separate checkpoints. It is not an intermediary or readiness gate for current collection. Data and Season responsibilities are distinct even under one coordinator.
- Entries remains linking-only and separately owned. Schedules remains an independent running collector; its retirement and consumer migration are preparation only. Neither is reassigned or changed by this document.

These datasets drive downstream decisions. Documentation and successful invocation are not source completeness, fresh readiness or full operational acceptance. Preserve explicit empty/missing/failed/partial/stale distinctions, source IDs, source timestamps, provenance and ambiguity withholding. Do not invent matches, use names as identity, renew age on replay, or repair records to satisfy a proof.

## README and continuity requirements

Each owner maintains a README in its actual workspace/module describing: exact owned base/table identities; source-of-truth and upstream authority; downstream contracts; current deployed baseline/route; actual collection and archive cadence/triggers; logs/heartbeat/checkpoints/stop behavior; explicit approval boundaries; verified review schedule and review record IDs; current known failures/limitations; and exact next authorized action. Preserve dated proof as dated evidence, not a competing active task list. Link current evidence rather than copying raw chats or secrets.

Read applicable instructions/skills on startup/resume, direction changes and handoff. Distinguish installed/mapped, actually READ, APPLIED and VERIFIED. Use the current installed RingStatus Operating Skills package when verified; do not claim plugin updates installed automatically. Reconcile legacy $rebalance with plugin skill name rebalancer without changing the owner's instruction intent. Skill loading never resumes stopped work or overrides runner boundaries.

Codex is not the runner. Existing registered workflows collect/archive business data. Review heartbeats inspect saved evidence and update only their allowed review records; they never substitute a collector run. Preserve existing task cadence and archive safety. Review schedules are best effort and depend on the host/app being available; a README or Airtable date is not proof an automated review executed.

The common review procedure and exact seven base destinations are in [WEF code-review standard](wef-code-review-standard.md). Periodic reviews must be backed by an inspected ACTIVE automation targeting the correct owner; document actual ID/target/schedule and dated results. Do not create duplicates, silently widen scopes or describe a future scheduled review as completed.
