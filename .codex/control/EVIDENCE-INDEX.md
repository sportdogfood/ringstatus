# RingStatus Agent-Control Evidence Index

Status: preservation index. This file does not replace or condense the source records.

The control work must preserve both solved and unsolved failure evidence. A missing published mechanism is recorded as **NO PUBLISHED SOLUTION FOUND** or **PARTIAL SOLUTION**; the requirement is not deleted.

## Canonical preserved evidence

- `.codex/control/USER-FAILURE-RECORD.md` — preserved user failure statements and explicit requirements.
- `.codex/control/history/AGENT-HIERARCHY-REVIEW-2026-09-29.txt` — preserved hierarchy-review evidence.
- `.codex/control/history/CURRENT-SESSION-EVIDENCE-2026-09-29.md` — preserved current-session evidence.
- `.codex/control/history/FULL-SESSION-REVIEW-2026-09-29.txt` — complete preserved session review supplied for this control effort.
- `.codex/control/history/FAILURE-24-2026-09-29.txt` — preserved additional failure entry.
- `docs/horseshowing/chatgpt-codex-operating-package-2026-07-12.md` — historical same-thread failure record and earlier control design. Status remains REVIEW REQUIRED; it is evidence, not automatically active policy.
- `docs/AGENT_GROUND_RULES_README.md` — existing RingStatus operating rules.

## Classification sidecars

- `.codex/control/FAILURE-SOLUTION-MAP.md` — published-procedure/mechanism classification for the initial failure set.
- `.codex/control/ENTRY-STATUS-SIDECAR.md` — per-entry/per-class status for the preserved hierarchy review, July failure classes, and current-session evidence.

These sidecars may classify evidence as:
- **SOLUTION FOUND**
- **PARTIAL SOLUTION**
- **NO PUBLISHED MECHANICAL SOLUTION FOUND**
- evidence-only when a statement is a consequence/reaction rather than a distinct control requirement.

They do not replace the source evidence.

## Preservation rule

Do not remove a requirement because:
- no published mechanism exists;
- the mechanism is only partial;
- a newer control package exists;
- a newer assistant interpretation seems simpler;
- a behavior cannot be fully mechanically enforced.

Preserve the source record and change only its sidecar status when later evidence justifies that change.
