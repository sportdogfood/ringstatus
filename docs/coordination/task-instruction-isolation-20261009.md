# Task instruction isolation — revision321

## Current owner restriction — revision326, October 9, 2026

The owner states that all four hooks failed and were never approved for use. Earlier installation records describe what the agent did; they must not be treated as owner approval. All four hooks for this checkout are now configured disabled in C:/Users/gombc/.codex/config.toml: SessionStart, UserPromptSubmit, Stop and PreToolUse. Three enabled entries were changed to false; PreToolUse was already false. Do not re-enable any of them until a full review and explicit owner approval. The previous ownership-filter fix does not qualify the hooks for use. Preserve source and historical evidence for review. This is a configuration readback, not proof of in-memory desktop reload or of another checkout's hook state. Previously injected instructions remain in existing chat history. The earlier sections below describe historical states, not current permission.

Owner reports an unrelated new runner referring to Feed/SMS/Barn tasks and requests that ambiguous task details not accompany general instructions.

Root cause inspected: .codex/hooks/user_prompt_submit.mjs unconditionally appended the repository's .codex/control/task-contract.json on every prompt. Root and global AGENTS.md do not contain those task details. The contract belongs to session01a11835-8cca-78e3-8342-c03a13d6ba93, not every runner in this repository.

Authorized correction: only append that existing contract when a nonempty current session_id exactly matches its owner_session_id. An unowned contract is not broadcast. General policy and session-specific evidence context remain unchanged. Preserve the contract and its full acceptance for its owner. No AGENTS.md, business code, Airtable data, hook registration, stop guard or tool-authorization rules changed.

Acceptance maps to existing C002/C003 scope and C025 evidence complaints: no unrelated task details in another runner's prompt; owning runner retains its full contract; missing/partial identity must not match; current general policy and evidence context remain; saved contract bytes remain intact.

Verification: new tests/codex-prompt-scope.test.mjs first reproduced the leak (unrelated runner received another task). After correction, six ownership/missing-identity cases passed. Session startup isolation test passed. Missing session identity retains the existing block behavior; the test was corrected to expect that existing behavior rather than changing it.

The older codex-control-hooks.test.mjs failed at line84 expecting ordinary text "done" to be blocked by the Stop hook. It exercises unchanged pre_tool_use/stop_guard code, not the changed prompt hook; its expectation conflicts with the existing rule that only explicit Result reports invoke completion checks. This failure is retained, not fixed or counted as passing within this scoped change.

Runtime limit: these are executable hook tests in isolated repositories, not evidence that an already-open conversation has forgotten previously injected text. The change affects future prompt-hook output in this checkout. A new actual runner has not been created to validate desktop activation; existing transcript content and other checkout copies are unchanged.

Rollback: reverse only the ownership-filter change in user_prompt_submit.mjs after checking subsequent edits; this restores the previous broad injection. The original task contract has not been deleted or reduced.

## Activation history and current state — revision324

Owner explicitly asked which hooks were started and why. This investigation changes no hook enablement.

The October 7 installation/reactivation record is timestamped 2026-10-07T13:58:30.534Z (9:58 AM America/New_York). It records local Codex controls, not business runners. This is evidence of that installation/reactivation, not the first-ever creation date.

| Hook | Current configured state, checked October 9 | Recorded purpose |
| --- | --- | --- |
| SessionStart | Enabled | Restore session-bound task and evidence context on startup/resume. |
| UserPromptSubmit | Enabled | Refresh instruction/revision context when a user message arrives. This was the source of the unrelated task-contract injection corrected above. |
| Stop | Enabled | Check evidence for explicit completion reports, intended to prevent unsupported PASS claims. It cannot establish semantic correctness by itself. |
| PreToolUse | Disabled | Intended scope/write-permission check; not enabled by this reliability installation. |

The saved source request was: “Implement and document where we can unwind quickly if this creates unintended consequence further downstream. Priority: consistent behavior before completing the original agent task.” The saved objective was reversible reliability controls. These records explain the implementation rationale; they do not establish that the owner separately requested each named hook.

Crucial rollback detail: RESULT.md says the original files and three disabled settings were restored during a rollback test, and then “The updated files and enabled settings were then restored.” reactivation-result.txt confirms three enabled hooks and PreToolUse disabled. Therefore the rollback test did not leave the three hooks off. Current user config still has those three enabled. No recent instruction-isolation repair enabled or disabled hooks.

The four scheduled monitors are separate from these hooks. The inspected automation configurations were PAUSED: onboarding-consistency-audit-supervision, onboarding-continuation-supervision, recognize-one-off-task-supervision, and usage-remaining-20-and-10-alerts. Their paused state does not disable project hooks.

Remaining limitations: previously injected text remains in existing conversations; the retained reliability task can still appear in this chat; another checkout can have different instructions. These are not resolved by the ownership filter. No wider reliability or drift-prevention PASS is claimed.

Evidence locations:

- C:/Users/gombc/Documents/Codex/reliability-controls-20261007/installation-evidence.json
- C:/Users/gombc/Documents/Codex/reliability-controls-20261007/reactivation-result.txt
- C:/Users/gombc/Documents/Codex/reliability-controls-20261007/RESULT.md
- C:/Users/gombc/Documents/Codex/reliability-controls-20261007/current-task.json
- C:/Users/gombc/.codex/config.toml — hooks.state entries for this checkout
- C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.codex/hooks.json
- C:/Users/gombc/.codex/automations/<monitor-id>/automation.toml — separate scheduled monitor settings
