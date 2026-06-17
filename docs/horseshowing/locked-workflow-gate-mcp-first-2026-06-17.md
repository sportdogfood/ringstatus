# LOCKED WORKFLOW GATE - MCP FIRST

Date: 2026-06-17

## Purpose

This contract governs WEC/Horseshowing workflow audit, repair, and buildout. The goal is a repeatable end-to-end workflow, not isolated fixes, one-off shell output, or documentation-only proof.

## Core Rule

When the user says:

```text
LOCKED WORKFLOW GATE - MCP FIRST
```

Codex must stop treating the workflow as open-ended and must operate from inspected evidence, existing docs, existing source, MCP/CLI output, Airtable rows, Catalyst rows, logs, and live render checks.

## Authority

The user is the fastest source of workflow intent, schema approval, and operational decision-making.

Codex must not reinvent process, schema, cadence, keys, or source-of-truth paths without explicit approval.

## Required Behavior

- Do not reinvent existing structure.
- Do not assume intent.
- Do not reconstruct from memory.
- Do not treat a documented/proven workflow as unknown.
- Do not make quick plausible patches.
- Do not build one-off shells as completed workflow.
- Do not silently change direction because of an observation or suggestion.
- Do not agree automatically when the evidence does not support the change.
- Do not proceed past a failed stage without fixing the repeatable workflow path or proving a blocker.

## Evidence Order

Use the strongest available evidence before answering or changing code:

1. Existing workflow docs/contracts
2. Existing source files
3. Catalyst MCP / Catalyst CLI
4. Airtable MCP / Airtable CLI
5. Catalyst rows
6. Airtable rows
7. Runner output
8. wec-logs / wec-alerts
9. Live API payloads
10. Live Webflow/mobile/print render

## Known Tooling

- Catalyst org: `700800454`
- Catalyst project: `horseshowing | 5614000000393031`
- Catalyst CLI: `zcatalyst-cli@1.26.1`, binary `catalyst`
- Airtable MCP CLI: `@airtable/mcp-cli@0.2.5`
- Debugging skill: `superpowers:systematic-debugging`

## Decision Handling

Codex must classify user input before acting:

- `constraint`: must be followed
- `observation`: must be tested against evidence
- `suggestion`: may inform solution, but does not redirect workflow alone
- `decision`: approved change from user

If a suggestion conflicts with the current workflow, Codex must prove the conflict with source/live evidence and ask for approval before changing direction.

## Stage Completion Rule

A stage is not done when its immediate output looks correct.

A stage is done only when it prepares the next stage.

Each stage must finish with:

- source rows accounted for
- target rows accounted for
- keys confirmed
- required links populated
- stale/removed behavior defined
- logs written
- audit check added or confirmed
- next-stage input proven usable

## PASS / FAIL / OPEN

- `OPEN`: work is still running; continue.
- `PASS`: exact workflow stage is verified clean with evidence.
- `FAIL`: no solution was found after exhausting the available workflow path, with proven blocker.

No "mostly," "partial," or "local only" completion.

## Communication Rule

- Keep responses operational and concise.
- Do not re-explain failure.
- Do not patronize.
- Do not bury the user in repeated narrative.
- Report what was inspected, what changed, what passed, or what is still blocking.

## Catalyst, PowerShell, and Airtable Roles

Catalyst is the workflow system.

PowerShell is only a local operator tool to inspect, deploy, trigger, or verify Catalyst. It must not become the workflow, transform data, or be presented as the solution.

Airtable is mostly a mirror/support surface. It is temporary visibility and convenience, not the core workflow engine.

The Airtable exceptions are:

- `focus_show` / `focus_day`: manual control lever
- `update_schedule_staging`: manual review/lock lever
- helpers like `horses`, `trainers`, `rings`, and `class_hide`: operator-managed helper controls

Everything else should be Catalyst-owned or Catalyst-produced, then mirrored into Airtable for visibility.

If PowerShell is used, it should only be for:

- running Catalyst CLI
- calling a Catalyst function endpoint
- checking Airtable/Catalyst output
- running the audit gate
- reading source/log files

It must not be used to create a one-off workflow, reshape data outside Catalyst, or bypass the repeatable function path.

## Summary

We are not starting over. We are enforcing the existing WEC workflow, using MCP/CLI/source/log/live evidence, and making each stage produce a clean handoff for the next stage. The user owns workflow decisions. Codex owns execution, verification, and preventing repeatable workflow drift.
