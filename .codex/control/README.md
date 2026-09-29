# RingStatus Codex-native control proof

This branch-only harness uses Codex's documented project-local hooks.

## What is mechanically enforced

- Default repository writes are blocked.
- `apply_patch` is blocked unless the task contract is `write_approved` and every target path is allowed.
- Mutating shell commands are blocked unless explicitly allowed.
- Write-capable MCP tools are blocked unless explicitly allowed.
- Write tasks cannot finalize while required verification evidence is missing.
- Every user prompt receives the stable RingStatus control policy and current task contract as developer context.

## What is not claimed

- The hook cannot perfectly understand semantic scope from natural language.
- It does not mechanically solve the "helpful final sentence" problem.
- It does not prove a user's correction is technically correct.
- Codex documentation states some specialized tool paths may bypass normal tool hooks, and hook failures/timeouts can fail open. This proof must therefore be tested in the actual Codex environment before relying on it.

## Activation

Codex loads `<repo>/.codex/hooks.json` only when the project hook definition is trusted. Use `/hooks` in Codex to review the exact hook definitions.

The default `task-contract.json` is deliberately read-only.
