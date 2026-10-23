# WEF Records Coordination Plan

**Goal:** Coordinate the four existing WEF chats, preserve approved work, prevent overlapping edits, and report evidence-backed progress and owner decisions.

**Architecture:** This chat is wef-records-coordinator. Existing chats remain independent user-owned chats; they cannot be converted into native child agents. Use thread reads, messages and bounded waits to coordinate them. Native subagents may perform bounded independent reviews, without replacing existing implementations or duplicating active writers.

**Tools:** Codex thread read/message/wait tools; native subagents when needed. No additional plugin, service or production runner is needed.

**Spec:** Owner request in this coordinator chat, supplied global runner instructions and repository AGENTS.md. This is a coordination plan, not authorization for application edits or selection of an opt-in workflow mode.

## Sources and authorization

- READ: repository AGENTS.md; RingStatus Operating Skills 0.1.0 request-router, instruction-loader, rebalancer, handoff and shared task record; Superpowers 7.0.1 using-superpowers, writing-plans and dispatching-parallel-agents; Plugin Management 0.1.0 guidance.
- SUPPLIED: owner global runner boundaries and earlier four-chat status requests.
- Owner appointed this coordinator to manage the chats. Coordination messages may request status, preserve scope, assign ownership within existing authorization and prevent conflicting work. They do not grant new edit or runtime authority.
- Fourth chat interpreted as wef-live; the latest request repeated wef-shows.
- Existing checkout is on main with substantial ongoing changes. Preserve all of them; no broad commit, rollback or cleanup.

## Chat registry and ownership

| Chat | Thread ID | Responsibility |
|---|---|---|
| wef-shows | 01a16976-f0a6-72a1-a75f-75de7f19a815 | Canonical show discovery and currently authorized cross-consumer source repair |
| wef-schedules | 01a14082-b22f-73b1-bba7-16781892a55d | Dated schedules, rings and preserved local show links |
| wef-entries | 01a1691d-3bd7-7eb2-af17-b9ba39a7501e | Entry collection and approved CWF consumer migration |
| wef-live | 01a16902-240a-7280-8661-90b0d8d7325a | TODAY live collection and schedule/entry links |

## Monitoring and management steps

- [ ] Refresh all four chats at activation. Record latest user authorization, active work, changed paths, latest scheduled proof, task enabled state, failures and next action. Chat reports are reported evidence until underlying artifacts are inspected.
- [ ] Send each chat one concise coordination message requesting these facts and asking it to retain its current approved scope. Do not restart settled discovery or duplicate implementation.
- [ ] Resolve the current shows/live overlap first: wef-shows retains its existing source-repair assignment; wef-live reports/reviews that outcome and does not independently edit the same source path. Obtain acknowledgement before assigning further overlapping edits.
- [ ] Keep one current task record in this coordinator conversation. Track PASS / FAIL / PARTIAL / BLOCKED per acceptance criterion, with run time, evidence location, owner and next action. Keep known failures visible alongside progress.
- [ ] During active coordination, wait on up to four chats in one bounded call, with saved cursors and waits no longer than 60 seconds. Read additional history only when a change needs explanation. Do not repeatedly announce unchanged state.
- [ ] Report meaningful completion, a newly failed gate, conflicting edits or an owner decision. Quiet unchanged work needs no repeated update. An idle chat is not proof of business success, and an active chat is not proof of runner health.
- [ ] Before completion, independently inspect the acceptance artifacts and downstream links applicable to the change. Local tests alone cannot establish scheduled workflow proof.
- [ ] At handoff, preserve exact paths, source versions when known, approvals, protected changes, failures, run evidence and next action. Follow the repository context checkpoint procedure at applicable boundaries.

## Immediate work queue

The following is the last read chat evidence, not a fresh production check:

1. **wef-shows:** discovery and schedules consumption passed two normal runs. Direct entries/live consumers were connected. Source repair subsequently left live failing on HTTP 502 for class 200030347; entries' second post-fix run was still pending. Obtain the final outcome and artifacts.
2. **wef-schedules:** two normal consumer runs passed with 449 schedule links and 24 ring records preserved. Empty October 22 warning remains. No new implementation assignment.
3. **wef-entries:** CWF read-only team sync and helper links are created; scheduled validation is pending. Duplicate CWF collection remains active until required proof permits the approved switch. Do not retire the old table or broaden migration scope.
4. **wef-live:** matcher link correction passed, but two successful normal runs remain unverified. Task disabled after persistent source 502. No re-enable, substitute run or additional patch after failure without fresh required owner authorization.

## Using native subagents

- Existing chats are coordinated workers, not native subagents. A native subagent cannot inherit a chat's identity or automatically acquire its history/access.
- Prefer the existing domain chat for its implementation. Give a temporary native reviewer a self-contained packet: outcome, exact source paths/version, approved delta, protected paths, acceptance criteria, evidence paths and stop conditions.
- At most three native children can run beside this coordinator. There is no need to create four persistent agents.
- Allow parallel read-only reviews of independent evidence. Use one writer at a time for overlapping application files, shared helpers, schemas, task settings and shared coordination documents.
- Require a return containing criterion-specific results, evidence, changed files (if authorized), blockers and next action. Independent review is required for applicable RingStatus mode 3 work; no mode is selected by this plan.
- A worker must not pass new production instructions to another worker. Route cross-domain decisions through this coordinator and preserve owner authorization.

## Runner and failure rules

- Codex is not the runner. Normal scheduled workflow evidence is required unless the owner explicitly accepts manual proof.
- No manual/direct substitute execution, record repairs, repeated attempts to change state or publication.
- On an approved workflow failure, stop that path and preserve evidence; continue independent authorized work. Further patches require the separately required APPROVED TO EDIT authorization.
- Never re-enable a disabled failed task merely to monitor it. Coordination authority does not expand production authority.

## Acceptance and activation

The plan is ready when all four identities, ownership, evidence requirements, overlap rules and escalation boundaries are explicit. Activation is demonstrated by fresh reads and acknowledgement of ownership messages, not by this document alone.

This turn creates the plan only. No coordination messages, subagents, automation or production changes have been started. Active-turn monitoring ends when this turn ends. Recurring monitoring would require a separately configured heartbeat with a clear interval and quiet-on-unchanged behavior; this plan does not create one or promise unattended monitoring.
