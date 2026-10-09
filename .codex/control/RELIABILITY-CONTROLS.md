# RingStatus reliability controls — October 7, 2026

Scope: project-local completion evidence and instruction continuity. No business runner, deployment, Airtable/CRM data or global instruction changes.

The existing startup, user-prompt and completion hooks are extended. Their definitions and trust hashes are unchanged. The separate write-blocking hook is not enabled by this change. It needs its own qualification; this package does not claim to enforce every tool action.

## What is checked mechanically

For a bound task, completion requires a versioned task/session identity, unchanged recorded acceptance, one PASS result per requirement, matching proof level (local/live/document), and existing evidence files whose SHA-256 matches the receipt. Every new user prompt invalidates the old receipt. A resumed session reuses its own record. Other sessions have separate records.

The first unsupported completion asks the agent to correct the result. A repeated failure stops with an explicit NOT VERIFIED warning; it never silently certifies completion. Accurate PARTIAL, FAIL and BLOCKED reports can end a turn without pretending the task is complete. Malformed evidence reports a control error.

## Agent operation — no owner bookkeeping

The assistant prepares the record from the complete user request and applicable instructions. The owner is not asked to write JSON or manage receipts. Read-only discussion alone does not require enrollment.

Use `node .codex/hooks/task-control.mjs bind SESSION CONTRACT.json` before implementation/verification work. The contract has version=2, task_id, session_id, source_request (literal request or exact source reference), objective, target, and requirements: [{id,text,proof_level}]. Preserve each independent requested outcome; do not replace an outcome with an easier component test.

`status SESSION` returns exact paths, contract hash and current prompt revision. `receipt SESSION RECEIPT.json` records task_id, session_id, contract_hash, prompt_revision and results: [{id,status,proof_level,evidence:[{path,sha256}]}]. Paths may reference appropriate local test logs, live readbacks or documents. No secrets in evidence.

Only an explicit completion report for the retained task starts `Result: PASS`, and only when that full request is satisfied. In that report, otherwise use `Result: PARTIAL`, `Result: FAIL` or `Result: BLOCKED`, followed by the remaining requirement. Ordinary answers, acknowledgments, clarification, research, handoffs and reports about a separate request do not require a result label or an older task's status. A retained contract preserves unfinished work; it does not make every later reply a completion report. The validator does not decide which work the user authorized.

October 7 correction: the prompt context now distinguishes ordinary replies from explicit completion reports. The Stop hook checks session-bound evidence only when a leading `Result:` disposition is present. Ordinary replies do not consume receipts and are never recorded as verified completion. Explicit PASS still requires unchanged scope and current evidence for every requirement. The shared write-contract completion gate also requires an explicit disposition and applies only to its owner session when an owner is specified. Legacy ownerless contracts retain their explicit-completion gate. A session-bound task always checks its own evidence regardless of the shared contract's owner. This does not semantically detect an unlabeled false completion claim; that remains an instruction-following limitation, not a reason to label every answer PARTIAL.

`amend SESSION CONTRACT.json` needs the current newer source_prompt_revision and a change_reason grounded in that user instruction. It archives the old contract, invalidates the old receipt and preserves an audit event. Amend does not authorize a scope change. Never amend solely to obtain PASS.

Records live in Git's private directory under `ringstatus-control/<session hash>/`. They are not shared across chats or committed. No raw prompt text is stored by the prompt hook; it records a digest and revision. Records and evidence have no automatic retention/deletion schedule.

## Limits — do not report these as solved

### Complaint-based checks added October 7

Full source snapshot: `complaint-source-20261007.json`; exact-record coverage: `complaint-coverage-20261007.json`; readable index: `COMPLAINT-COVERAGE.md`. All 82 fetched records are retained, including original complaints and recorded solutions. Coverage is not correction. The source is a dated snapshot, not a live synchronization.

A requirement can include `proof: {kind, target, source_version, checks, minimum_runs}`. Kind and exact target are required; checks is a nonempty list of unique named acceptance checks. The evidence file is an observation JSON record with the same kind/target/version, `checks: [{id,status}]`, and `artifacts: [{path,sha256}]`. Artifact paths resolve from the repository root. Each required check must be PASS; extra unresolved checks also fail. Missing/changed artifacts fail. Scheduled observations additionally need `trigger: "schedule"`; a minimum_runs criterion counts distinct run_ids. Native-runtime, visual, interaction and handoff kinds cannot be replaced by mock tests, saved styles or prepared prompts. A visual contract names each required viewport and fidelity check explicitly; a handoff contract names actual delivery and acceptance.

This validates submitted records. It does not measure a screen, execute an interaction, authenticate a run ID, prove source truth, or prevent a misleadingly labeled record. Existing contracts are not silently rewritten. The assistant owns accurate contract preparation and evidence collection; the owner does not maintain this format.

`continuation_guard: true` adds a check to explicit PARTIAL/FAIL/BLOCKED reports for that bound task. Its current receipt must account for all requirements. PENDING or a nonempty next_action requests continuation; BLOCKED/FAIL needs `blocker: {kind, detail, evidence:[{path,sha256}]}`. Kinds are access, dependency, owner_decision, authorization, runner_failure or capability. Completed requirements still need their valid proof. A receipt-level `stop_reason` with kind user_stop, authorization or runner_failure, detail and intact evidence takes precedence: a failed approved runner path never causes an alternate run. This check never executes work. One continuation attempt is allowed; repeated failure stops unverified. An unlabeled ordinary answer remains unaffected. Recorded blockers can still be dishonest or incomplete; no semantic guarantee is claimed.

Compatibility: new typed-proof and continuation checks apply only to explicitly configured contracts. This session is enrolled; other chats are not silently enrolled. Hook definitions, trust settings and AGENTS.md are unchanged. PreToolUse remains disabled. This change adds no resident process, schedule, new hook or production runner.

Tests: `tests/codex-complaint-controls.test.mjs` replays complaint-shaped good/bad records in disposable repositories. These are local control regressions, not real autonomous agent deliveries. The separate legacy regression suite remains applicable.

Change-specific rollback: `C:/Users/gombc/Documents/Codex/complaint-controls-20261007/rollback.mjs`. Run with `--check` to inspect, or `--apply` to restore only this change after hash checks. It preserves edits already present before this change. This rollback supersedes the older rollback below for the complaint-based extension only.

This verifies bookkeeping and file integrity, not evidence truth or semantic completeness. An omitted initial requirement, misleading evidence, an inappropriate amendment, an unenrolled task, or ignored instructions can still escape these checks. Agents retain filesystem access; these are not tamper-proof controls. PARTIAL/BLOCKED reports are not mechanically checked for contradictions elsewhere in the text. Initial task enrollment and correct interpretation still require the assistant to follow instructions.

OpenAI documents hooks as a guardrail with incomplete tool-path coverage. Stop blocks create continuation prompts; they are not a general output-rewriting or correctness mechanism. Hook failures can have platform-specific handling. Activation must be verified in the actual client. Sources: https://learn.chatgpt.com/docs/hooks (read October 7, 2026).

The code and tests are a RingStatus-specific implementation, not an OpenAI guarantee of consistent model behavior. Passing regression cases does not establish reliability across all future tasks. The original Barn/agent trial remains unfinished at the full requested scope.

## Unwind

The change-specific backup, installed-file hashes, activation state, rollback utility and test evidence are at:
`C:/Users/gombc/Documents/Codex/reliability-controls-20261007/`

Emergency disable: `node "C:/Users/gombc/Documents/Codex/reliability-controls-20261007/rollback.mjs" --disable-only`

Restore this change: `node "C:/Users/gombc/Documents/Codex/reliability-controls-20261007/rollback.mjs" --apply`

The restore first checks all managed file hashes. If later edits exist, it stops instead of overwriting them; disable-only remains available. It restores only this change's files and three enable flags. It does not reset the repository, rewrite other settings, delete evidence, or revert business data. Newly created session logs are retained for diagnosis. A rollback does not retroactively alter an already running turn; verify the next turn's hook state before relying on it.
