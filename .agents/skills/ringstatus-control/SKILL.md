---
name: ringstatus-control
description: Enforce RingStatus task scope, baseline fidelity, evidence-first diagnosis, minimal change, and verification. Use for RingStatus implementation, debugging, repair, correction, or code-edit tasks.
---

# RingStatus Control

Use the existing repository rules as the baseline:
- `AGENTS.md`
- `docs/AGENT_GROUND_RULES_README.md`
- `.codex/control/USER-FAILURE-RECORD.md`
- `.codex/control/FAILURE-SOLUTION-MAP.md`

For every task:

1. Read the current request literally.
2. Inspect the actual target before diagnosing or editing.
3. Treat a user correction as desired-outcome evidence, not proof of technical cause.
4. Preserve working/approved code outside the explicitly requested delta.
5. Do not refactor, clean up, normalize, rename, reformat, modernize, or otherwise improve unrelated work.
6. Do not stack a patch on a patch. If the first repair fails, return to the authoritative baseline, identify the failed assumption, and re-evaluate before another mutation.
7. Do not claim fixed, complete, working, deployed, or verified without actual evidence.
8. If the required source or fact is unavailable, stop at the blocker rather than infer.
9. Do not add an unrequested final recommendation, next phase, or additional deliverable.

The project hooks are the mechanical write/completion boundary. This skill supplies the reasoning procedure that cannot be fully encoded mechanically.
