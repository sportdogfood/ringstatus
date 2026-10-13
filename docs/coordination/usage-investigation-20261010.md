# Codex usage investigation — 10 October 2026

## Measured evidence

Account snapshot: Pro; weekly Codex window 74% used, 26% remaining; two reset credits available. Account-wide figures do not attribute consumption to this chat or validate the exact percentage spent yesterday.

Local session: C:/Users/gombc/.codex/sessions/2026/10/07/rollout-2026-10-07T07-49-23-01a11632-62fd-73b1-997e-8be4119185de.jsonl

Cumulative token-counter difference from 2026-10-10T11:38:01.499Z to 15:38:13.971Z (07:38–11:38 America/New_York):
- Input: 77,561,145, including 76,156,032 cached; uncached remainder 1,405,113.
- Output: 146,575; separately reported reasoning-output counter: 7,161 (do not add this subset to output again).
- 443 token-count telemetry events, not 443 user messages or a verified request count.
- 132 turn-context entries in window, all gpt-6-astra / medium. Service tier missing; speed mode unverified.
- Entire session: 44 compaction records and 67 user heartbeat messages at inspection. These are historical counts, not proof monitors remain active.

Counters count repeated processing, not unique written content or chargeable dollars. Child sessions and other account activity were not separately attributed. This is not a complete billing reconciliation. No claim that every historical message is replayed on every request.

## Findings

This recent burn occurred at Medium, so High cannot alone explain it. Large repeatedly processed context, a higher-cost model, tool results, repeated corrections and coordination all contribute. The transcript demonstrates repeated full schema/prompt output and avoidable rechecking. Individual percentage attribution is unavailable. Reported quality problems include missed execution and stale status; no controlled evidence establishes Medium caused them or High would prevent them.

Official Standard credit rates per million tokens: Astra input 250/cached 25/output 1250; GPT-6.1 Sol input 50/cached 2.5/output 250. Credit rates do not directly determine included subscription percentage. Fast consumes included usage at 2.5x Standard; current speed was not verified.

## Recommended next operating comparison — not settings changed

Use a fresh narrowly scoped task with a verified compact handoff rather than importing this entire dialogue. Compare GPT-6.1 Sol Medium at Standard speed on one bounded real task, retaining identical acceptance and readback checks. Escalate a specific difficult defect to Astra only when needed. Measure usage delta and accepted result, including correction count; do not infer quality from effort labels. Keep routine status/metadata work small and avoid full schemas, histories and long prompts when exact fields suffice. Use agents for distinct work, not repeated discovery; no additional agents or schedules created by this investigation. Review recurring checks separately before altering them.

## Official sources inspected

- https://learn.chatgpt.com/docs/pricing — context/tool/caching drivers, subscription sharing, speed multipliers and credit rate card.
- https://learn.chatgpt.com/docs/model-selection — task-specific model selection.
- https://developers.openai.com/api/docs/guides/reasoning — nonvisible reasoning uses tokens; API billing is not a Pro-plan percentage calculator.
- https://developers.openai.com/api/docs/guides/latest-model — compare settings on representative work; routine follow-ups need not use the highest effort.

No model settings, automations, application code, billing preferences or account resets changed.
