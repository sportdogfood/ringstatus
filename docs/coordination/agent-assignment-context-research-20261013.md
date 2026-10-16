# Agent assignments, coordination and context — OpenAI research

Researched 13 October 2026. Status: research complete; operating changes proposed, not activated.

Purpose: define how RingStatus selects models, loads instructions and skills, equips coordinators, measures work, and starts or hands off chats without repeating the previous drift and token waste. This document does not authorize implementation, new agents, model changes, monitors or business operations.

## What the documentation establishes

| Requested topic | Support found | What is established | Remaining local responsibility |
| --- | --- | --- | --- |
| Model and reasoning level | Yes | OpenAI provides task-based starting guidance and recommends comparing identical inputs at different settings. | Verify actual settings and evaluate against RingStatus acceptance; no setting guarantees success. |
| Instructions and skills | Yes | Instructions have discovery rules; skills use metadata first and full instructions when selected. | Confirm the correct workspace, exact sources, relevant full reads and usable tools. A registry row alone proves none of these. |
| Coordinators and subagents | Yes | Codex supports focused subagents, inherited configuration and agent-specific configuration. | Confirm the tools and permissions exposed in the actual session; assign distinct responsibilities and verify results. |
| Time, tokens and checks | Yes, with limits | Goals provide persistent objectives and budget-related stopping; usage instrumentation exists, with product-specific differences. | Prompt deadlines and scheduled reminders are not demonstrated hard wall-clock enforcement. Desktop account quota is not task attribution. |
| Long chats and splitting | Yes, with limits | Codex compacts context; projects support distinct chats; independent work can run separately. | No universal message-count or time threshold for starting a new chat was found in the reviewed sources. |

## 1. Model selection is a task decision

OpenAI positions Luna for scoped, frequent tasks; Sol for complex work where cost matters; and Astra for demanding analysis. Its model-selection page specifically suggests Sol Medium for complex technical work and coordinated deliverables, and recommends retaining the lightest setting that meets the quality bar after comparing the same inputs. These are starting points, not guarantees. [OpenAI model selection](https://learn.chatgpt.com/docs/model-selection)

The models reference says higher effort can take longer and consume more tokens. Its starting guidance includes the client default for Sol and Light for Astra; Light in the app corresponds to `low` in configuration. Effort labels do not establish equal performance across different models. Verify the available model and effort together, rather than translating a historical setting by name alone. [OpenAI models](https://learn.chatgpt.com/docs/models)

Proposed RingStatus selection:

| Assignment | Starting candidate, subject to owner selection | Escalation reason |
| --- | --- | --- |
| Straightforward record inventory or comparison | Current efficient default; evaluate Luna for repeated narrow work | Missed facts or inconsistent structured output on a fixed sample |
| Coordinator resolving dependencies, scope and acceptance | GPT-6.1 Sol Medium | Specific unresolved reasoning problem, not the coordinator title itself |
| Scoped implementation against proven code | GPT-6.1 Sol Medium; retain a lighter setting when it meets the same checks | Demonstrated difficulty with the authorized delta |
| Difficult architectural or contradictory-evidence review | Compare Sol at higher effort with Astra Light/Medium on the same bounded problem | Better accepted results justify the additional usage |

This is a proposal, not a change to current defaults. Do not silently increase effort, switch models or retry indefinitely. A coordinator doing a simple status read does not need an architectural reasoning setting merely because it is a coordinator.

A minimal comparison uses the same source snapshot, prompt, tools, limits and acceptance criteria. Record completion quality, regressions, unsupported claims, owner interventions, elapsed time, available token telemetry and retries. Prefer successful task completion per unit of work over the lowest single-response cost. Do not repeatedly rerun production workflows for evaluation.

No reviewed source proves that Astra High caused our historical failures or that Medium caused poorer behavior. OpenAI does directly describe a relevant mechanism: accumulated broad skills and excessive mandatory reading can bloat context; overly restrictive review boundaries can make Astra stop early; completion must explicitly include the required running and verification steps. That supports reviewing our instructions, not blaming a model setting without controlled evidence. Owner safety and authorization boundaries remain controlling. [Rethinking skills and prompts for GPT-6 Astra](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra)

## 2. Make the agent aware of the right skills without loading everything

OpenAI documents progressive disclosure: skill names/descriptions are initially provided, then full `SKILL.md` instructions are read when selected. The initial Codex skill list has a context budget; large catalogs can produce shortened descriptions or omitted entries. Clear, narrowly applicable descriptions and explicit invocation help selection. A skill may also declare dependencies in `agents/openai.yaml`. [Build skills](https://learn.chatgpt.com/docs/build-skills)

Codex discovers global and repository instruction files in order, with closer instructions taking precedence. Its default combined project-document cap is 32 KiB. Workspace location and overrides therefore matter; a handoff cannot assume that mentioning a file makes it part of the automatic instruction chain. [AGENTS.md discovery](https://learn.chatgpt.com/docs/agent-configuration/agents-md)

Proposed assignment practice:

1. Identify the actual lane, target, source baseline and smallest authorized outcome. A lane is our business scope label, not an OpenAI runtime object.
2. Specify the necessary instruction sources and a short skill map: required now, conditional later, and irrelevant. Preserve explicit-only activation rules.
3. Give exact accessible paths or plugin skill names. Read each required skill's full instructions before its dependent action; load referenced sections when relevant.
4. Confirm required connectors/tools are callable for the assigned operation. Reading instructions cannot grant access or install a missing capability.
5. Record separate states: mapped, available, read, applied and outcome verified. Acknowledgment alone proves only acknowledgment.

The existing inventory found 127 `skills-included` records, including eight without a resolved standalone source and three duplicate-source groups. Those findings must be resolved before using those records as reliable automatic handoff inputs. See [skills inventory](skills-inventory-20261013.md). This research does not load that entire inventory into an agent.

Keep durable shared rules short. Keep Recognize, Tenants and other domain detail in their respective source documents/skills, linked from a task-specific handoff. Do not add every operational detail to root `AGENTS.md`.

## 3. Give coordinators capabilities, not just a title

OpenAI's Codex documentation supports custom agents with a name, description and instructions, with optional model, effort, MCP and skill settings. Inheritance and overrides must be considered together. Narrow jobs and suitable tool access are recommended. [Codex subagents](https://learn.chatgpt.com/docs/agent-configuration/subagents)

OpenAI also warns that subagents add token usage and are less useful for tightly ordered work or frequent changes to shared state. Its API multi-agent guidance supports parallel independent workstreams, but API configuration is not automatically a desktop control. [Multi-agent guidance](https://developers.openai.com/api/docs/guides/responses-multi-agent)

Current session capability evidence, from the exposed tool contracts:

| Need | Available here | Important limit |
| --- | --- | --- |
| Delegate a finite subtask | Collaboration spawn, message, follow-up, list, wait and interrupt tools | Current instructions require explicit delegation authorization; no agents were spawned for this research. |
| Inspect an existing independent chat | Read-chat and compact wait tools | Reading status does not resume work. |
| Continue an independent chat | Send-message tool | Requires explicit user authorization to message that chat. |
| Create a new independent chat | Create-chat tool | Requires an explicit new-chat request. |
| Verify artifacts and update the registry | Local file tools and Airtable connector | Scope and acceptance still apply; a saved status is not execution proof. |

This tool surface currently allows four concurrent agents including the parent. Full-history subagent forks inherit the parent model/effort; an authorized override requires the fresh-context mechanism exposed by the spawn tool. Generic TOML documentation must not be assumed to override this session's tool contract.

Proposed coordinator responsibilities: preserve the owner's target and proven baseline; assign nonoverlapping work; know who may write each resource; inspect evidence against acceptance; retain remaining gaps; and continue independent authorized work without sending the owner questions already answered by the handoff. Use one writer per shared change and targeted independent review when warranted. A coordinator need not duplicate every worker connector if it can inspect the resulting evidence reliably.

Use a subagent for a bounded parent-owned investigation, review or independent component. Use a separate specialist chat for an ongoing lane with its own durable checkpoint and owner interaction. Neither choice automatically transfers schedules, access, acceptance responsibility or lifecycle ownership. Do not create a hierarchy merely to keep agents busy.

## 4. Assign limits and measure work accurately

OpenAI Goals supply a persistent thread objective with a measurable finish line, verification surface and constraints. The documented lifecycle includes continuation while active and within budget, and stopping on completion, interruption, budget or a genuine blocker. Budget exhaustion is not success. Goals do not widen permissions. [Using Goals in Codex](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex)

Current tool evidence: `create_goal` accepts an explicit token budget; `get_goal` reports status, usage and remaining budget. Creation requires an explicit goal request, and a token budget requires an explicit requested amount. No goal was created here. Do not assume the budget aggregates every independent chat or descendant without verifying its accounting scope.

| Control | Meaning for our assignments |
| --- | --- |
| Outcome and acceptance | Name the exact behavior/artifact to prove; local checks and live workflow acceptance stay distinct. |
| Start, reserve and deadline | Record actual UTC timestamps and preserve them through handoffs; reserve is for verification and saving. |
| Token allowance | Record its scope and source of measurement. Use a supported goal budget only when explicitly authorized. |
| Checkpoints | Check after meaningful milestones, failures, drift and before release/handoff; use the existing 30-minute active-work check. |
| No-progress condition | Review the proven baseline and changed evidence. Do not turn repeated failing attempts into automatic permission to edit. |
| Stop/handoff | Save exact evidence, remaining work and smallest necessary human action. Never reset elapsed time or budgets by renaming the task. |

A deadline written in a prompt is an instruction, not a demonstrated forced termination mechanism. Our periodic monitor is also best effort. Hard cancellation requires a runtime control with confirmed scope and behavior; this research does not introduce a watchdog or new infrastructure.

OpenAI's Agents API usage documentation counts inputs, cached inputs, outputs and reasoning, including delegated work and retries. Cached tokens are part of input totals; reasoning is part of output totals. Counts may be delayed or unavailable. Cached input still has a cost, so a high cache-hit percentage alone does not establish an inexpensive completed task. That documentation describes API accounting, not a conversion formula for this Pro subscription. [Observability and usage](https://developers.openai.com/api/docs/guides/agents-api/observability)

For this desktop setup, retain available per-chat telemetry and account quota separately. At assignment boundaries, record counter baselines and compare later counters; do not sum cumulative snapshots or duplicated fork history. Keep parent and child ownership clear before aggregating. Missing telemetry is unknown. Diff lines such as `+14,684` measure changed source lines, not token usage.

## 5. Chat targets, compaction and fresh starts

OpenAI recommends keeping related work together, separating independent tasks and avoiding competing writers. Each chat retains its own context and goal. [Long-running work](https://learn.chatgpt.com/docs/long-running-work)

Projects provide shared source organization, while chats keep separate transcripts. The primary local folder affects automatic instruction, skill and configuration discovery; merely attaching another folder does not make its instructions automatically active. [Projects and chats](https://learn.chatgpt.com/docs/projects)

Codex automatically compacts long conversations into a smaller representation. OpenAI explains that model calls carry context and that caching/compaction manage this cost; changing model or tools can affect cache reuse. Compaction supports continuity, but does not justify unlimited unrelated history. [The Codex agent loop](https://openai.com/index/unrolling-the-codex-agent-loop/)

Proposed decision rule:

| Situation | Preferred treatment |
| --- | --- |
| Same bounded objective, useful current context and accepted progress | Continue in the same chat. |
| Independent task with a distinct source/write boundary | Separate task or authorized subagent; pass only its required context. |
| New outcome, accumulated unrelated lanes, repeated rediscovery or large context with little accepted progress | Prepare a compact handoff and use a fresh chat when requested. |
| Need an alternative direction retaining the full conversation | Fork; do not call this context reduction. The current fork tool retains history. |
| Need a quick explanation while another task works | Use a separate question/chat when useful; avoid interrupting or importing the entire task history. |

No universal OpenAI threshold of a specific message count, elapsed duration or token count for a new chat was found. Our existing 100,000-input-token / 30-minute / account-usage checks are local review triggers, not official limits or automatic stops. See [context-check procedure](context-check-procedure.md).

Observed in this research: the local helper reported 146,224 input tokens for its latest recorded model request, triggering the existing large-context review. This is not the cumulative usage of this research, and the interval since yesterday's checkpoint includes idle time. A compact continuation target is included below; no chat was created or forked.

## 6. Reminders should supervise a defined assignment

OpenAI distinguishes scheduled work that returns to an existing chat from independent scheduled runs. Local scheduled work requires the computer, application and project to remain available. It recommends testing the prompt and inspecting initial runs; skills can define the reusable action. [Scheduled tasks](https://learn.chatgpt.com/docs/automations)

Proposed RingStatus practice: use the existing assignment's owner, target IDs, latest cursor, deadline and completion conditions. Read only changed evidence. Notify only material progress, completion, failure or a necessary owner action. Stop obsolete reminders. A handoff must explicitly identify who owns each existing reminder and whether it remains necessary; creation of a new chat must not produce duplicate supervision. A reminder firing is not proof of work or deadline enforcement. A recurring status loop should not substitute for a scoped execution goal.

## Smallest practical assignment record

Use existing Agents tasks/handoffs and linked skills/connectors records, not new overlapping tables. The following is a proposed compact record body:

```text
Target: one outcome, lane, exact chat/agent and owner
Baseline: proven source path/version and current compatibility gap
Authority: allowed changes, protected resources, release/manual boundaries
Configuration: requested model/effort; observed model/effort or unknown
Instructions: applicable sources; required skills with exact paths; conditional skills
Tools: required operations and verified capability/access
Acceptance: exact checks and where their evidence will be recorded
Limits: start, verification reserve, deadline, token allowance/scope if authorized
Progress: latest verified result, remaining work, next executable action
Handoff: artifact links, worker state and schedule ownership
```

The startup check should be one compact internal record, not an interview or a new approval ceremony. Missing information already available in the source must be retrieved. Ask only for a necessary decision or inaccessible input, while continuing independent work.

## Proposed changes to existing guidance

1. Extend the existing handoff/planning guidance with the assignment fields above and a verified model/effort distinction. Preserve current owner boundaries and proven-source rules.
2. Resolve the inventory's ambiguous and duplicate skill references; shorten routing descriptions where needed; map necessary skills and tool dependencies without loading every skill. Update Instruction Loader and Request Router only within their existing scope.
3. Clarify rs-coordinator's distinction between bounded subagents and independent chats, actual management tools, single-writer ownership, and evidence-based closure. Do not introduce resident coordinators or a mandatory multi-level hierarchy.
4. Extend the existing context-check record with task counter baselines and accounting scope where telemetry supports them. Keep current review triggers; do not present them as hard enforcement or create another monitor.
5. Validate the resulting assignment on the next authorized small real task: actual settings, exact skill reads, connector access, one accepted outcome, elapsed time, usage evidence and clean resume packet. Documentation alone cannot close the associated behavioral complaints.

## Resume target

Research is complete. The next distinct task is to review and, if authorized, implement the five proposed guidance changes above using the existing custom skills and context procedure. Start from this file, the skills inventory and the 13 October five-skill update. Do not reread this conversation or resume Webflow, Recognize, Tenants or engine work to perform that task. No runtime configuration, skill source, agent, goal or schedule was changed by this research.
