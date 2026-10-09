# Common delivery stages: prototype → Webflow → Astro → verified release

Saved 8 October 2026 at the owner's request. Reusable procedure for Recognize, barn-input, barn-onboard, user-optin and later assigned interfaces. This defines stages; it does not start implementation or authorize publishing. Each project retains its own logic, designated storage, permissions and acceptance tests.

## Stages and exit evidence

### Before entering any stage: preserve existing proof
Owner rule, revision242: use the existing working implementation or documented proof of concept; never reinvent or replace it unless the owner explicitly requests replacement. Inspect and record its path/commit, demonstrated behavior, dependencies, compatibility gaps and smallest authorized delta in the existing task record before planning or dispatch. Enter at the first genuinely missing stage; do not recreate completed stages. A changed Airtable base, schema, host or native Webflow presentation does not authorize recreating the underlying logic. Rebalance immediately when the owner points back to working code. Never patch over a failed patch; inspect the divergence and root cause before any separately authorized correction, preserving runner stop rules. Verify the requested change and preserved baseline behavior against the actual authorized target.

Known references, inspected in revisions240–241 (historical source, not fresh live proof): WEC Packing backend/routes and frontend at `647ea24f8`; its comprehensive overview first appears at `84a06827f`, not the earlier snapshot. Recognize member/device/session flow is preserved at `f012c3077` under `webflow-cloud-test/src/{assets,lib,pages}/rs-recognition*` and `webflow/rs-recognition`. Its July recovery code queues `send_member_link`; the external delivery automation and current SMS-only policy require their own verified mapping. Preserve these references without restoring old base access or old login policy.

| Stage | Work | Required exit evidence |
|---|---|---|
| 1. Fix the prototype reference | Finish the requested prototype decisions and identify the exact saved version, accessible source, assets, visible states, themes and responsive behavior. | Named source version and agreed visual/behavior inventory; unresolved decisions listed. Public deployment is not assumed to equal the latest saved version. |
| 2. Reproduce in native Webflow | Build the authorized native draft from that reference. Prefer a clean draft when authorized and earlier experiments are unsuitable; preserve old pages. Keep functional implementation separate from styling. | Editable native elements; rendered source/target comparisons at matched viewports, required themes and visible states; documented visual acceptance. Saved styles alone do not pass. |
| 3. Map the interface and data | Map each native display/input/control to its response field, action, designated Airtable field and log event. Confirm effective route, base/table IDs and success/error meaning. | Complete mapping, validated schema and explicit storage destination. No legacy fallback or invented IDs. |
| 4. Wire and verify the complete test path | Bind existing native elements to Astro behavior and Airtable storage/logging. Test from the browser through the actual authorized test runtime and back to the displayed result. | Passing interaction, persistence, retry/failure, authorization, log and visual-regression evidence. Mock/direct-endpoint checks cannot substitute for the browser path. |
| 5. Owner publishes; verify release | Present the exact ready page and passed pre-publish checks. The owner publishes the Webflow draft. Astro deployment is separately authorized and coordinated with that page. | Confirmed deployed versions and passing critical browser → Astro → Airtable → log → display checks on the released path. Any test requiring publication is identified beforehand. |
| 6. Lock the completed baseline | Preserve page/source/code versions, mapping, storage identities, evidence, remaining non-required backlog and rollback instructions together. | Every required gate passed; no hidden required work. Later changes use an explicit delta and affected regression tests. A lock is a documented baseline, not technical immutability. |

## Responsibilities that stay constant

- **Webflow owns presentation:** native markup, layout, fonts, colors, responsive styling and styled states.
- **Astro owns behavior and server data access:** validation, permitted actions and JSON responses. A small page-specific adapter populates designated existing elements and selects predefined states. It must not inject a replacement styled interface.
- **Airtable owns designated data and logs:** exact base/table/field identities are pinned for each project; browser code never receives Airtable credentials. An old wiring reference is historical evidence, not permission to access that base.
- Token libraries are referenced during visual/mapping work where applicable. They do not silently override the accepted prototype or authorize redesign. Data-driven token selection follows the accepted mapping.

## How each project uses this plan

### Terminology: one-off task agent

Owner-defined term, 8 October 2026: **one-off task agent** (machine-readable label: `one-off-task-agent`). An agent assigned exactly one defined outcome, using the authorized connections and all relevant skills needed for that task. Prefer an existing task-specific skill when available; verify access and load applicable instructions rather than assuming a named connection or skill is usable. Skills provide reusable methods, not additional scope or permissions.

- The assignment names its outcome, exact targets, relevant connections/skills, completion checks, limits and protected work.
- Necessary guidance, clarification, correction and verification continue within that same assignment. Waiting for publication or a dependency does not create a new task or mean completion. A task may contain multiple necessary steps; “one-off” does not mean one turn.
- Complete only when the assigned acceptance checks have current evidence. A budget stop, blocker, saved draft or prepared handoff remains accurately recorded as unfinished when required work remains.
- Once marked complete on that evidence, the agent's assignment ends. No follow-on work, unrelated improvements, recurring responsibilities or automatic next assignment. A new distinct task gets a new one-off task agent.
- Preserve the finished record and artifacts for reference. No automatic deletion or archiving is implied. Reusable skills and connections remain available for later agents; they do not make this agent a permanent service owner.

This term describes task ownership and lifecycle, not a new runtime or technical enforcement mechanism. Coordinators can provide necessary guidance without expanding the task or resetting its limits.

### Terminology: one-off task coordinator

**One-off task coordinator** (`one-off-task-coordinator`) owns preparation, delegation and completion review for one assigned outcome. The one-off task agent performs the implementation. This is a reusable role definition; it does not create a new chat, skill, supervisor hierarchy or automatic monitor.

The coordinator must:

1. Resolve the task's current instructions, source/target identities and completion gates.
2. Locate and read applicable skills, preferring an existing task-specific skill. Record exact accessible skill paths/resource IDs and necessary authorized connections in the task record. Verify access; a list of names is not loaded knowledge. Use the current skills catalog and existing Handoff procedure rather than duplicating whole skill libraries into prompts.
3. Prepare a self-contained handoff with scope, protected work, first action, source versions, exact data destination, tests, budget, rollback and known blockers. Keep useful task-specific skills reusable across agents.
4. Recommend an available model and supported reasoning effort suited to the task, noting the quality/cost tradeoff without claiming an untested optimum. For Codex create_thread, current tool rules require an explicit user request for a specific model before passing a model override; otherwise use the configured default. Record requested/default settings separately from settings actually verified. Apply permitted effort settings through the tool, not merely text in the prompt. A general role definition does not establish a specific model selection.
5. Dispatch only within owner authorization; verify delivery and the agent's first concrete action. Inspect progress through supported status tools, provide necessary same-task guidance, preserve stop/correction limits and prevent overlapping write ownership. Any recurring supervision needs an explicitly configured schedule; this title alone does not keep the coordinator running.
6. Review current evidence for the whole assigned outcome, retain unresolved gates honestly, and close the assignment after verified completion. Do not automatically assign the next project. A new outcome uses a new assignment/agent; preserve artifacts for reference.

Handoff fields therefore include: task, connections, applicable skill locations, source/target, model/effort policy, acceptance, limits, evidence location and stop condition. Unknown access or unsupported model settings are explicit constraints, not facts to invent.

### Coordinator tracking lifecycle

Owner addition, 8 October 2026: the coordinator remains accountable after handoff. Create or reuse one **active-agent record** in the existing agents-base `active-threads` table, assign coordination ownership to the coordinator's own chat identity, and record the distinct worker identity. Use existing fields; if no dedicated owner field exists, put explicit coordinator and worker IDs in thread-overview. Do not confuse this agents registry with the task's application data base.

Before dispatch, or immediately when registering an already-running assignment, record scope, connections/skills, targets, evidence location, deadline, last check and next action. Verify delivery and actual first action. Configure an authorized recurring check; do not claim that a prompt alone keeps checking. Deduplicate monitors by worker ID.

Each check reads current activity and evidence. Update the active-agent record; a running tool is activity, not completion. Provide bounded same-task guidance only when useful. Do not repeatedly nudge an active agent, bypass a blocker/owner decision, or restart exhausted work. Pause polling when a required owner action or durable blocker is reached, preserving the open task until the dependency changes.

Create **failure entries** in `rs-agents-complaints-lib`, linked to the active record, for observed failures. Include time, affected task/action, evidence, impact, correction and verification status. Deduplicate the same incident; distinguish a failed tool call from a failed whole task. Never mark a proposed correction resolved until its result is checked.

On verified completion, update the same record with completion evidence/time, active=false and closed=true, then stop its monitor. A budget stop/blocker is not completion: retain closed=false and the actual unfinished state. Preserve the history; no automatic next assignment. Existing fields active/closed describe assignment lifecycle, while thread-overview records running, waiting, blocked or stopped state.

Current application and exact field IDs: [Recognize supervision](recognize-supervision.md). Scheduled execution is local and does not establish a hard timeout or guaranteed unattended reliability.

Maintain one project task record with: current stage, owner authorization, exact source and target IDs, designated base, protected work, acceptance matrix, evidence, elapsed budget, next executable action or specific blocker, and rollback. Link detailed evidence rather than duplicating it. Start at the earliest unpassed required gate; do not rebuild accepted unchanged work merely to replay the stages.

One one-off task agent owns one bounded deliverable. Use a fresh one-off task agent for the next distinct deliverable and transfer the compact record, verified work and remaining gates. A visual task must not drift into JavaScript repair; an integration task must not redesign the interface. Completing one stage never means the whole project is complete. Existing owner stops and budgets survive a handoff.

Retain the [existing cost/correction limits](../webflow-native-rebuild-cost-limits.md): 60-minute page budget including waits unless explicitly changed, final 15 minutes reserved for verification/checkpoint, at most two correction/retest cycles per defect, 20 minutes aggregate corrections, and one supported recovery within five minutes per incident. Reconcile actual accumulated time and remaining authorization before dispatch; splitting work into stages or chats does not reset limits. These are operating limits, not an enforced spending cap or guarantee that every project fits in one hour. Preserve unfinished gates when a real limit/blocker is reached; never rename them complete.

For native prototype handoffs, use the existing [Webflow prompt procedure](<C:/Users/gombc/.agents/skills/handoff/references/ringstatus-webflow-prompt.md>) through the [Handoff skill](<C:/Users/gombc/.agents/skills/handoff/SKILL.md>). Supply six concrete blocks: outcome/first action, source/target, method, acceptance, limits and checkpoint. Verify actual delivery and first action. These local paths require access on this host; embed essential instructions when the receiver lacks access.

## Current application

- [Recognize integration and completion plan](recognize-native-astro-plan.md) is the specific plan for stages 3–6, retaining the unresolved visual gates from stage 2. Designated data/log base: **`app9kOZdIaGyKk5uG` only**. Integration is in progress; current authorization and evidence are in [the Recognize task](recognize-wiring-task.md).
- [Schedule v24 new-draft task](schedule-v24-new-draft.md) is an existing stage-2 assignment. This common procedure does not change its scope or restart its clock.
- After Recognize, the owner's revision170 queue is **barn-inputs → barn-onboarding → barn-optins**, followed by Feed. These are separate UI/data-action integration assignments using Webflow presentation and Astro mapping. Exact pages, mappings and behaviors must be established for each; do not assume identical workflows or start them now. Earlier barn-input/barn-onboard/user-optin labels are historical; do not rename technical resources without verifying their identity.

The detailed prototype-to-Webflow procedure already exists. This document connects that procedure to the saved integration/release plan so future tasks can use one sequence without relying on the long conversation.

### Owner priorities revision170 — 8 October 2026

Finish Recognize first. Next: barn-inputs, barn-onboarding, barn-optins; then Feed. The owner expects Feed to be straightforward, but its remaining integration effort is not yet verified. Feed's owner-ready draft is preserved and does not establish data wiring completion. Schedule and the consistency audit remain OPEN with their unfinished requirements retained. Keeping them open does not resume their stopped workers or the owner-stopped visual audit. This queue update dispatches no new task and changes no existing page, data or release boundary.

### Owner transition revision136 — presentation and wiring ownership
Owner takes remaining manual Webflow visual corrections and directs progression to the data/action integration stage. Webflow owns all native styling; Astro maps data and permitted actions to designated native elements. No style-injecting/replacement interface is permitted. Existing visual defects remain recorded for manual acceptance, while independent integration preparation continues. Recognize is first under recognize-wiring-task.md; barn-input, barn-onboard and user-optin remain separate successors. No publication/deployment or access-policy decision is implied by this transition.
