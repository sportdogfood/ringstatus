# Actual setup and continuation repair — 14 October 2026

Scope: skills-and-agents continuity only. No business/runtime changes, new hooks, model changes, automatic compaction or operational restarts. Owner authorized audit, repair and a fresh-agent demonstration without replaying this thread.

## Observed setup

| Check | Actual evidence | Consequence / correction |
| --- | --- | --- |
| Project identity | `list_projects` returned local project `7073fdbc-f525-4251-aedc-63fc27eaf755`, RingStatus checkout, `isGitRepository:true` | This is a local repository project. Do not claim ChatGPT Project memory supplies its other Codex chats. |
| Instruction chain | Global `C:/Users/gombc/.codex/AGENTS.md` 5,427 bytes; repository `AGENTS.md` 4,301 bytes; no global/root override or docs/coordination nested instruction found | Combined 9,728 bytes is below documented default cap; no size-cap explanation found for missing business context. These generic instructions do not contain the whole business model. |
| Local instruction settings | No `project_doc_max_bytes` or fallback-filename override in inspected config | No cap/config edit warranted. |
| Custom loaders | Instruction Loader and Handoff files exist; each declares `allow_implicit_invocation:false` | Use explicit invocation in the startup prompt; listing file paths or catalog entries is not invocation. Do not enable them globally or load all skills. |
| Old hooks | Four RingStatus hook state entries all `enabled:false` | Leave disabled; no dependency on them introduced. |
| Earlier successor | Continuity's last observed turn read the full packet plus broad supporting sources and produced an adoption receipt | Read/adoption evidence exists, but it is not proof a genuinely fresh agent continues the broad role. |
| Handoff growth | Recorded inputs: new 188,629/258,400; compact 139,467/258,400; continuity 159,328/475,000. Handoff turns grew by roughly 40,212, 23,839 and 33,035 respectively | Full reload is costly. Existing context and denominators differ; no causal inference about model quality or Pro charges. |
| Startup scope | Full reference section 1 assigned adoption; section 10 specified a receipt; older packet contained inventory exercise | Replace the executable startup, not merely add another promise of broad continuity. |
| Knowledge storage | Detailed contracts, decisions, proof commits and source pointers already exist | Keep them; use selected headings/records instead of reading them all at startup. |

## Applied repair

`rs-skills-start.md` is the single current entrypoint. It preserves the broad role, authorization, ownership and actionable current task; routes questions to bounded existing sources; invokes the required loader; and eliminates adoption/inventory as a permanent next action. Existing full packet is retained as a topic reference, not a mandatory full read. Old handoffs receive a superseding pointer. No domain logic or established decision is rewritten.

No new global AGENTS.md task content is needed: that would risk the same unrelated-task leakage the owner previously rejected. The explicit continuation prompt supplies the scoped entrypoint. New tasks remain free of this role unless assigned it.

## Official support and applicability

- [Projects and chats](https://learn.chatgpt.com/docs/projects): primary local folder governs Codex instruction/skill discovery; shared files and separate conversation histories are different. Applies to this verified local project; not evidence of automatic full chat transfer.
- [AGENTS.md discovery](https://learn.chatgpt.com/docs/agent-configuration/agents-md): global/root discovery, override precedence and default 32 KiB document cap. Applied to actual paths/config above; no override or size repair needed.
- [Skills and prompt context](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra): short routing and progressive disclosure reduce unnecessary skill context. Applied as selected-topic loading; not a promise of perfect recall.
- [Persistent execution plans](https://developers.openai.com/cookbook/articles/codex_exec_plans): saved task context should let a reader continue without the original conversation. Applied as explicit state, source and next action, not a new planning framework.

## Acceptance fixed before the fresh test

The fresh test uses no inherited conversation (`fork_turns:none`), no source transcripts, no previous adoption receipts and no supplied answer key. The source retains this rubric for independent grading; the receiver gets only the repaired entrypoint and real owner questions.

1. Reconstruct relevant company/domain boundaries and tiny-business constraints without owner re-education.
2. Apply season membership, additional-trainer horses versus riders, groom assignments and personal favorites correctly.
3. Keep recognition separate from membership and preserve current launchpad/desktop-code behavior.
4. Preserve all Messages lanes, Publish delivery ownership and native Webflow boundary.
5. Handle TODAY/tomorrow, manual focus, empty timing and working WEF proof without inventing a new architecture or executing it.
6. Inspect current Agents task/complaint through connector; distinguish live record from dated release proof and turn it into a usable next-work artifact.
7. Carry permissions, stopped workers and reminder ownership; continue independent work if one lookup is unavailable.
8. Demonstrate targeted retrieval, not just self-certify reads. Source checks tools/read ledger and output; report measured telemetry if available, never invent savings.

Record actual results after testing in `rs-skills-continuation-proof-20261014.md`. A specific test result establishes the covered behaviors, not guaranteed future recall. The source remains available until the owner accepts the successor.

Completed: see [actual test evidence](rs-skills-continuation-proof-20261014.md). The first cold receiver exposed excessive retrieval output, which was corrected before a second cold receiver and topic-change test. Current task next-action field and existing successor entrypoints were updated; receipt-only adoption was not accepted as the proof.
