# RingStatus workflow — purpose, status, standards, and downstream view

Status snapshot: October 1, 2026. This is an overview and requirements record, not an application deployment or proof of unattended operation.

## Purpose and ownership

Provide three explicitly selected ways to work with ChatGPT/Codex: factual advice, project planning, and implementation through verified handoff. The project owner controls scope, authorized changes, and acceptance; technical advice must come from inspected evidence, authoritative documentation, or established implementation patterns.

Reduce repeated mistakes without requiring the owner to act as researcher, debugger, or routine QA. Instructions can support consistent behavior; they cannot guarantee correctness or prevent platform interruption.

## Choose one mode

| Selection | Purpose | Result and boundary |
|---|---|---|
| Mode 1: factual advice | Establish supported facts and decision-useful advice | Read-only research/inspection; no implementation or external changes |
| Mode 2: advice + plan | Turn advice into a scoped new-project plan | Objective, target, dependencies, acceptance criteria, unresolved facts, and handoff; stop before code |
| Mode 3: plan + build | Implement the authorized plan | Code, actual tests, independent review, evidence, and run/handoff instructions |
| No selection | Continue normal operation | Do not automatically apply this workflow |

Invoke in Codex with `$ringstatus-workflow mode 1`, `mode 2`, or `mode 3`. An explicitly equivalent selection is accepted by the instructions. Selection applies to the current task; moving from advice to planning or from planning to building requires an explicit choice. If the skill is invoked without a mode, ask which mode rather than defaulting to code.

Cycled synthetic stages, SMS-in/out, and monitors are products created through mode 3. They are not extra chat modes, and selecting mode 3 alone does not authorize building every product.

## Current placement and actual status

The skill is saved in `sportdogfood/ringstatus` under `.agents/skills/ringstatus-workflow/`, with `SKILL.md`, `agents/openai.yaml`, and two references: `build-cadence.md` and `support-and-limits.md`. A personal copy is also saved in the user's skills directory; the original disposable prototype remains separate.

| Item | Verified state |
|---|---|
| Repository installation | PASS: four added workflow files were fetched back and matched the prepared contents |
| Invocation policy | PASS: `allow_implicit_invocation: false` is present in the saved repository metadata |
| Instruction consistency | PASS: independent static review found no supported boundary contradiction |
| Mode 2 trial | PASS: a separate agent returned a chat-only task-list plan and stopped before implementation |
| User's actual Codex client | NOT VERIFIED: skill discovery, selection, and activation were not exercised there |
| Latest lightweight modes 1–2 refinement | PENDING: requirement recorded below; not applied to the installed instructions |
| Root `AGENTS.md` | Unchanged by this workflow installation; current inspected file contains Webflow MCP guidance |
| Recurring execution, SMS, monitors | Not implemented/tested as part of these prototypes |

Documentation for repository skill discovery and explicit invocation: [OpenAI Build skills](https://learn.chatgpt.com/docs/build-skills). Saved source: [RingStatus workflow](https://github.com/sportdogfood/ringstatus/tree/main/.agents/skills/ringstatus-workflow).

## Keep modes 1–2 light and editable

Required refinement: one accountable lead, concise instructions, and bounded research; use an extra investigator only when a specific independent question warrants it. Do not run a five-controller build process for advice or planning. Keep the mandatory grounding/rebalancing check brief and read-only where the mode is read-only.

Keep workflow-specific behavior in the explicitly invoked skill. Keep `AGENTS.md` small and use it only for approved rules intended to apply whenever work occurs in that project or directory. A root `AGENTS.md` rule also affects ordinary operation: do not put opt-in workflow procedures there and silently make them universal.

When a recurring failure is observed: inspect the failure and controlling evidence, propose one precise rule at the narrowest applicable scope, obtain authorization for the instruction edit, check for conflicting instructions, and verify the revised behavior on a bounded example. Do not accumulate duplicate rules or claim a rule eliminates a failure without testing it. Keep the cadence fixed unless the owner explicitly changes it.

Official guidance supports small, scoped `AGENTS.md` files and incorporating recurring review feedback: [OpenAI Customization](https://learn.chatgpt.com/docs/customization/overview). This overview records the refinement; it does not edit the skill or `AGENTS.md`.

## Required code and communication standards

These are owner requirements for future work, not claims that all existing RingStatus code has been audited for compliance.

- Use clean, consistent code that fits the inspected system and its upstream/downstream contracts.
- Deliver complete, implementation-ready affected code blocks/files when code is the deliverable; do not make the owner reconstruct a solution from fragments.
- Never layer a one-off patch on a failed patch to satisfy the current answer. Return to the authoritative baseline, establish the cause, and correct the authorized behavior within the existing design.
- Never use CSS `!important` in newly delivered changes. Resolve the responsible cascade, selector, or component structure within scope; do not silently rewrite unrelated styling.
- Never reinvent a supported solution. Use established tools, documented APIs, and existing approved patterns; verify their compatibility with the actual target.
- Never optimize, refactor, redesign, rename, or improve approved work without an explicit request. A clean deliverable is not permission for a cleanup campaign.
- Never silently assume missing facts, technical causes, capabilities, approvals, or expected behavior. Research or inspect them; report what remains unknown.
- Inspect consumers, producers, persisted data, events, outputs, and existing tests before changing a contract. Every change has downstream consequences.
- Stop the same repair path after two failed fixes; report the remaining blocker without inventing a third workaround or changing the objective.
- Verify the actual result before claiming success; distinguish local, deployed, scheduled, and client checks.
- Keep routine replies to two sentences, with a compact evidence table only when useful. Provide no failure monologue, teaching, or unrequested alternatives.

## Mode 3: development cadence and agent responsibilities

One lead owns the plan, task scope, coordination, and final answer. Ground/rebalance at startup, phase boundaries, handoffs, enhancements, resumption or compaction, observed conflict/drift, milestone loops, and completion. Recheck the objective, target, ownership, approved files, acceptance criteria, evidence, and unresolved items. This is an instruction checkpoint, not an automatic context-length detector.

| Responsibility | Output | Agent boundary |
|---|---|---|
| Planning | Scoped plan and acceptance criteria | Bounded source investigation when useful; no implementation |
| Implementation | Complete authorized code change | One code writer at a time; exact approved files |
| Testing | Actual behavior/regression evidence | Independent executor when available; isolated test data |
| Review | Findings against exact diff and plan | Reviewer must not be the implementation author |
| Evidence and handoff | Reconciled criterion status and approved version | Lead consolidates all results and unresolved items |

These are five responsibility lanes, not a requirement for five continuously running agents. Add independent agents for useful investigation, testing, or review; dependent phases remain sequential. Parallel agents consume additional tokens and shared edits can conflict, so keep file ownership explicit and parallel work independent. Each controller has at most one assigned worker; workers do not delegate further. Record reused identities and prior roles.

Pass applicable project instructions, current task/plan, exact target, allowed actions, evidence requirements, and stop conditions to each agent. Wait for results and inspect evidence. Use native capabilities; do not invent a custom agent framework or enable hooks/config changes without a separate request. Code testing and independent review remain required; irrelevant optional product lanes may be disabled in the plan.

Primary support: [OpenAI Subagents](https://learn.chatgpt.com/docs/agent-configuration/subagents) and [long-horizon task guidance](https://developers.openai.com/blog/run-long-horizon-tasks-with-codex). The exact five-lane cadence and two-fix limit are project policies, not OpenAI reliability guarantees.

## Full downstream view — planned, not deployed

The development workflow produces an approved code version, run contract, tests, and operational handoff. A scheduler or event source executes that version; it does not re-plan or rewrite the application every cycle. Operational monitors observe execution; bounded agents may investigate evidence or review findings, while code changes return through mode 3.

| Downstream product | Purpose | Operating path | Current proof |
|---|---|---|---|
| Cycled synthetic stages | Exercise recurring stage execution before live integration | Scheduler → approved stage code → output and audit | One manual synthetic slot tested; repeated cycles not tested |
| SMS-out alerts | Send an approved notification when a defined condition occurs | Approved event/output → messaging API → delivery callback/log | Not built or tested |
| SMS-in/out | Receive an inbound message and produce an authorized response | Provider webhook → validated handler → response/outbound API → logs | Not built or tested |
| Monitors | Check failures, stale data, missing runs, and operational evidence | Bounded scheduled check → run/source evidence → criterion status → authorized notification | Not built or tested |
| Agent investigation | Explain a supported operational finding using actual evidence | Monitor finding → lead → narrowly assigned investigator/tester → consolidated result | Delegation exercised in development; recurring operational use not tested |

Catalyst documents recurring job submission and job execution/status; it is a supported candidate, not a newly deployed environment: [Cron](https://docs.catalyst.zoho.com/en/job-scheduling/help/cron/introduction/) and [Jobs](https://docs.catalyst.zoho.com/en/job-scheduling/help/job/introduction/). Twilio documents inbound webhooks and delivery callbacks; it is a capability example, not a provider selection: [Messaging webhooks](https://www.twilio.com/docs/usage/webhooks/messaging-webhooks).

OpenAI documents recurring scheduled tasks and native subagents: [Scheduled tasks](https://learn.chatgpt.com/docs/automations). Scheduling a prompt does not prove access to this repository, runtime, integrations, or cancellation controls; verify the chosen execution surface before enabling recurrence.

### One WEF slot: stage descriptions

| Stage | Purpose |
|---|---|
| Heartbeat/control | Decide whether the approved slot runs and apply its pause/control state |
| Schedule acquisition | Obtain schedule input from the chosen source |
| Normalization | Validate and normalize schedule identities |
| Classes and entries | Associate entries with known classes |
| Live state | Establish current counts and class state |
| Timing | Compute only estimates supported by known inputs |
| Results | Carry validated result data; optional in the synthetic demonstration |
| Outputs | Produce the approved output contract |
| Audit | Record completed, disabled, or failed stages |

Keep SMS and monitors separately scoped so changes to them do not silently change stage behavior. Connect products through inspected, approved input/output contracts. Any later RingStatus integration must inspect its real collection, storage, publication, and message consumers; no live compatibility is established by the synthetic fixture.

### Recurring monitoring: required handoff and boundaries

Before enabling a recurring run, specify its code version, host, credentials/access, cadence, maximum duration, overlap policy, input/output contract, audit fields, pause/stop behavior, and failure response. Unknown values remain unresolved; no timing, vendor, or retry count is invented here.

A monitor needs defined evidence sources and criteria: for example, last successful run, stage outcome, expected freshness, and delivery status where applicable. Assign one accountable lead per monitoring task; add bounded read-only investigators or test executors only when useful. Do not keep forty agents resident, or treat indefinite chat activity as a scheduler.

Monitoring must not automatically edit source code, repeatedly repair a failing integration, broaden scope, or publish changes. A supported defect returns to an authorized mode 3 build with tests/review and a new approved version. Any automated recovery action must be specifically planned, authorized, and verified first.

## What the earlier tests actually established

| Evidence | Established behavior | Limit |
|---|---|---|
| 10/10 task CLI tests | Initial persistent add/list/complete application, including actual CLI processes | Application tests, not ten agent-reliability evaluations |
| 18/18 task CLI tests | Original ten plus eight enhancement tests for reopen, priority, and filters | Separate disposable project; not RingStatus deployment |
| 19 CLI subprocess checks | Additional coordinator verification of valid flows and byte-preserving errors | Does not prove scheduled operation |
| Agent/controller exercise | Actual bounded handoffs and review; roles reused when allocation was rejected | No proof five controllers beat fewer agents; phase-two audit reused its own testing actors |
| Fixture cleanup | Persistent post-run absence remained BLOCKED/unproven | Two remedies did not establish lasting absence |
| 8/8 synthetic slot tests | Nine stages, pause before acquisition, optional Results, invalid input/source failure, source preservation, actual CLI behavior | One manual run; no real WEF collection, recurring schedule, production publishing, persistence across cycles, or SMS |
| Cancellation exercise | Individual agent interruption and lead-owned command cancellation worked | Parent interruption left a worker running; worker-owned command cancellation was blocked |

Recorded reports: `codex-full-prototype-results.md`, `wef-slot-prototype/RESULTS.md`, and `codex-cancellation-results.md` in the evaluation workspace. These reports were inspected for this overview; the historical tests were not rerun during documentation work. The synthetic slot did not receive independent agent code review.

## Freeze, cancellation, and platform limits

The owner reports two apparent freezes. No diagnostic connection/session logs establishing their cause have been obtained; the cause is unknown. An unanswered turn, a frozen display, and active background work cannot be equated without run-state evidence.

The tests ran in a ChatGPT Work Mode cloud workspace on Linux/Node, not a registered Codex Cloud environment. No Codex Cloud environment or production recurring service was established by these exercises.

Observed parent interruption did not automatically stop its worker. Actual user-side Stop, disconnect/reconnect behavior, and independent cancellation while disconnected remain untested. The official subagent documentation says the web activity sidebar is read-only and does not provide individual stop/steer controls; this is a UI limitation, not evidence of the reported freeze's cause.

While the lead has control, it can stop new assignments, enumerate agents and command sessions, interrupt/cancel each individually, inspect outcomes, and record CANCELLED or UNKNOWN. That procedure is not an independent kill switch when the lead or connection is unavailable. Do not call this unattended-safe until cancellation is verified on the actual surface.

## Remaining verification before operational use

1. Apply and verify the authorized lightweight mode 1–2 refinement, keeping opt-in procedures out of always-applied project instructions.
2. Verify discovery, explicit activation, and nonactivation in the actual Codex client.
3. Build one recurring synthetic-slot product through mode 3 on an explicitly selected host; prove repeated cycles, failure behavior, stop/pause, and downstream output.
4. Build and verify SMS and monitors only within their separately approved scopes, using actual integration evidence.
5. Verify user-side Stop and disconnect recovery before describing recurring agent work as unattended-safe.

This README records direction and known evidence. It does not apply pending instruction changes, configure agents, create schedules, send messages, deploy application code, or establish a platform cancellation mechanism.
