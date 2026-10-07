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

A bound task's final starts `Result: PASS` only when the full request is satisfied. Otherwise use `Result: PARTIAL`, `Result: FAIL` or `Result: BLOCKED`, followed by the remaining requirement. The validator does not decide which work the user authorized.

`amend SESSION CONTRACT.json` needs the current newer source_prompt_revision and a change_reason grounded in that user instruction. It archives the old contract, invalidates the old receipt and preserves an audit event. Amend does not authorize a scope change. Never amend solely to obtain PASS.

Records live in Git's private directory under `ringstatus-control/<session hash>/`. They are not shared across chats or committed. No raw prompt text is stored by the prompt hook; it records a digest and revision. Records and evidence have no automatic retention/deletion schedule.

## Limits — do not report these as solved

This verifies bookkeeping and file integrity, not evidence truth or semantic completeness. An omitted initial requirement, misleading evidence, an inappropriate amendment, an unenrolled task, or ignored instructions can still escape these checks. Agents retain filesystem access; these are not tamper-proof controls. PARTIAL/BLOCKED reports are not mechanically checked for contradictions elsewhere in the text. Initial task enrollment and correct interpretation still require the assistant to follow instructions.

OpenAI documents hooks as a guardrail with incomplete tool-path coverage. Stop blocks create continuation prompts; they are not a general output-rewriting or correctness mechanism. Hook failures can have platform-specific handling. Activation must be verified in the actual client. Sources: https://learn.chatgpt.com/docs/hooks (read October 7, 2026).

The code and tests are a RingStatus-specific implementation, not an OpenAI guarantee of consistent model behavior. Passing regression cases does not establish reliability across all future tasks. The original Barn/agent trial remains unfinished at the full requested scope.

## Unwind

The change-specific backup, installed-file hashes, activation state, rollback utility and test evidence are at:
`C:/Users/gombc/Documents/Codex/reliability-controls-20261007/`

Emergency disable: `node "C:/Users/gombc/Documents/Codex/reliability-controls-20261007/rollback.mjs" --disable-only`

Restore this change: `node "C:/Users/gombc/Documents/Codex/reliability-controls-20261007/rollback.mjs" --apply`

The restore first checks all managed file hashes. If later edits exist, it stops instead of overwriting them; disable-only remains available. It restores only this change's files and three enable flags. It does not reset the repository, rewrite other settings, delete evidence, or revert business data. Newly created session logs are retained for diagnosis. A rollback does not retroactively alter an already running turn; verify the next turn's hook state before relying on it.
