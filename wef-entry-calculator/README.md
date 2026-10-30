# wef-entry-calculator

Entry owner chat: `01a16f18-32b3-7900-8bbc-e8de123ca3b8`. Timing coordinator: `01a169bc-0bd6-77d3-b7b4-283a07061e90`. Ring owns shared-runner integration; Class independently reviews. Entry also owns the consolidated planning context for horse/rider timing conflicts, entry conditions and validation; [specialist handoff](SPECIALIST-HANDOFF.md) preserves those sources and unresolved contracts.

## Current status — 30 October 2026

Human **APPROVED TO EDIT** and requested code-review in coordinator turn `01a18e50-5cba-7db2-b4cd-04d6b2e7a51d`. The malformed-group correction is frozen and independently reviewed PASS. Raw class/group/stage multiplicity is retained; only safe nonexcluded groups reach class adaptation. Scoped malformed/duplicate issues do not poison unrelated work or persist entry_failed.

- **23/23 Node checks PASS**, including actual entry stage, checkpoint/log readbacks, both input orders and repeat runs: [raw report](evidence/node-malformed-group-20261030.txt).
- Unchanged Python retained-projection fixtures **4/4 PASS** using `wef-data/.venv/Scripts/python.exe`: [raw report](evidence/python-project-runtime-20261030.txt).
- [Frozen source hashes](evidence/malformed-group-hashes-20261030.json) and [writer plus independent review](evidence/malformed-group-review-20261030.md).
- Ring is staging full shared-runner regressions using the actual entry stage, followed by combined independent review. **No entry deployment or normal scheduled acceptance yet.**
- Production phase/sequence/start-anchor evidence is unproved. Answers preserve `phase_unproven`; a synthetic phase-proof test is not evidence of usable production predictions.
- Retention remains disabled behind shared verified archive release. Tables and candidate code do not establish an active archival service.

The earlier error-message test failure, system Python timezone setup failure and combined malformed-group review failure are retained in evidence. The first two were resolved; the last has this approved narrow correction but still needs full combined acceptance. New failed verification stops its affected path under the owner's rule.

## Base and ownership

Base `appyoLTcNmLrgpHcg`; exact field IDs and created/read-back schema in [schema.json](schema.json). Existing meta preserved.

| Table | ID |
| --- | --- |
| timing-answers | tblg0E317kTVI71Wn |
| engine-checkpoints | tblubkHgnhLE7N6Kh |
| run-logs | tblT0CVaBcvW4oybM |
| heartbeat | tblBAp64wopJCzjlZ |
| errs-log | tblGgFWqJ8ibpHkPd |
| code-reviews | tblKzdte0XfjALsXS |
| archive-catalog | tbligPSRSbgFeERyZ |

Tables were created and independently listed. No business answers, operational review records or recurring review automation were created by this owner task. No active periodic review ID is claimed.

## Source and implementation

Data authority is its verified immutable retained handoff, base `appdl16HJ5ETm8N69`; Shows `appCaSmL819KK7UFJ` supplies canonical context. Live `appgzosUcVqA4vZdf` supplies independently validated current observations/frozen archives. Ring answers use `appYOykKIy1kAquhP/tblhIjSjeGs5td23c`; Class answers use `appeqxxJyPGS1iqdm/tblmoBFySYW2TkDHf`.

`data-input.py` extends the existing read-only timing projection with latest accepted TODAY trip payloads and exact immutable references. It retains original source times and pointer fencing. No collector/endpoints or mutable-table membership assumptions.

`worker.js` counts the complete raw inventory before excluding unsafe groups, validates exact occurrence/roster identities, overlays compatible Live values first and retains qualified Data fallback. `class-proof.js` verifies saved payload material, provenance and current recomputed class hash. No inferred phase or progress anchor.

`calculator.js` handles eligibility/order/workload and a conditional explicit phase-proof estimate. Countdown alone does not revise material snapshots. `transport.js` limits GET/PATCH to owned tables, follows pagination and batches ten records without scope caps. No deletion API.

`stage.js` plus `operations.js` implement preflight, shared and entry-local durable intent, exact answer ID/key/field readback, own checkpoint/run-log/heartbeat/error records and blocking failure state. Written counters advance only after readback. `retention.js` creates protected 24-hour candidate manifests only; it cannot delete.

## Shared integration contract

Existing route: `wef-timing-data -> run-hidden-data.vbs -> run-data.ps1 -> entry-data.js -> data-runner.run`, every three minutes. TODAY in America/New_York only. Preserve existing160-second worker/165-second process budgets and8MiB projection buffer. No caps, SQLite, Schedules lane, legacy changes or manual production runs.

Exports from `stage.js`:

- `preflight({api,state})` before any ring/class/entry business writes.
- `stage({api,state,save,markStage,runId,now,clock,data,liveClasses,classRows,paceRows,answers,readArchive,excludedGroups})`.

Pass original unfiltered Data and the shared validator's original-object `excludedGroups` Set. Pass independently GET-read-back class rows merged with prior rows, verified `afterPace`, and existing verified Live evidence/archive reader. Never substitute attempted class write fields.

Shared save persists intent before entry PATCH; entry also mirrors its checkpoint with independent readback. Entry issues/counters remain separate from ring/class skips. Ring fences day/deadline/pinned pointer before each answer batch and Shows authority before the first. Uncertain writes stay blocked for owner review.

Shared staging: `C:/Users/gombc/Documents/Codex/staging/wef-timing-entry-integration-20261030`. That directory's current release manifest and independent review control deployment; an older combined162-test result did not cover the real malformed-group interaction. Ring is replacing that coverage with actual-stage regressions.

## Remaining acceptance

1. Full shared real-stage malformed/duplicate continuation in both orders and repeated runs, then independent combined code-review.
2. Coordinator-controlled idle reconciliation, exact frozen payload deployment and normal scheduled first/repeat-run proof across all three bases. Earlier orphan-lock state is dated evidence; recheck before acting.
3. Full retained-workload capacity and projection size under the existing budgets; no synthetic-test timing claim of production capacity.
4. Proven phase/sequence/anchor semantics before usable go-time predictions; validated finish bounds before conflict findings. No new phase inference is authorized.
5. Verified archive/index release before any cleanup, protecting current/future/sync records and uncertain intents. No active retention claim.
6. Results/Messages/serving-time expiry contracts and any condition thresholds remain separate acceptance work, not implied by table creation.

Existing shared dirty edits were preserved. Initial repository baseline was `ddb39509198b766883848ff156fef65548c7f3e1`; exact current entry versions are in the frozen manifest. Latest narrow source changes were only worker.js and calculator.test.js. No task/cadence/collector changes by Entry.
