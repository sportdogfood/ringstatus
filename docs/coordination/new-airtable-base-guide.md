# Create or normalize an Airtable base

## Requirement-to-action reconciliation

For every accepted owner requirement, record the exact target, action, readback evidence and unresolved dependency in existing tasks. Before calling a base a model, verify each requirement against the physical base and current records, not merely its documentation or central registry. Separately check schema, relationships, operating content and runtime dependencies. Counts and plans are not acceptance evidence. Known exceptions must remain visible in the readiness decision until resolved. Do not create another tracking table.

This corrects the execution failure recorded in Agents complaint recdhCDN3p5dUr3j2; prevention is not yet proven.

Reusable small-business setup guide — 10 October 2026.

Use the agreed eight operating tables to make each base understandable on its own. Recognize is undergoing correction and is NOT a validated base template. Keep the setup practical: a base can be useful while some work remains manual or unfinished. This guide does not authorize moving live workflows, publishing, deleting records or creating agents.

## 1. Decide what belongs here

Write a short purpose, what this base owns, what stays elsewhere, and who is responsible. Reuse an existing base if it already owns the work. Do not duplicate shared people, horses or other identities simply to support another service; reference their established identifiers.

Identify existing working tables/code before creating replacements. A copied table is not automatically connected to its application.

## 2. Create the base and record its address

For an existing base, skip creation. Read its current schema, records, relationships and recorded application dependencies before changing its structure. Do not copy Recognize or Tenants business tables wholesale.

Create the base in the intended Airtable workspace using the available supported Airtable interface or connector. Use its agreed project/system name. If the agent cannot create it, the owner can create the empty base and supply its link; continue from there.

Verify the resulting base name, base ID and access. Record it in the central Agents base's `bases` register (formerly `agents-bases`), reusing the correct entry if one exists. Preserve owner-maintained project assignments. Use actual base IDs and links, not guessed names or placeholders presented as completed connections.

Set `bases.Resume here` to the direct URL of the owning base's maintained system checkpoint, not the base homepage or this guide. This is the former `Start here` field, renamed in place; do not create a duplicate. Keep that record URL stable and update its content each session. Agents, Recognize and Tenants have established checkpoint links; leave other links blank until a real checkpoint is verified.

Reconcile `base-lanes` against the current owning-base schema and agreed purpose. Reuse existing lane records, name retained lanes accurately, add missing current business lanes, and include exact table IDs/URLs and known limits in purpose. Link every lane to the correct `bases` record. Mark confirmed structure/purpose `Locked`; leave provisional lanes `Expected`. Preserve obsolete or duplicate entries with `___` and blank status, explaining their replacement without deleting history. A registry rename does not authorize renaming a runtime table. Shared references do not establish global master ownership.

Read back the base ID, Resume here target, project links and complete lane set. Locked means structure is agreed, not that deployment, permissions, automation or live testing is complete. Application health and remaining tasks stay in the owning system checkpoint.

## 3. Create the eight supporting tables

Use these exact names. Reuse matching tables rather than creating duplicates. `Name` is the primary text field; long notes use long text, statuses use single select, and dates use date/time. Leave unknown dates and optional fields blank.

| Table | Purpose | Fields |
| --- | --- | --- |
| system | Base context and starting point | Name, Updated date, Purpose/scope, Message, Status, Owner, Connections, Skills, References, Related tables, Code locations, Overview/instructions, Health, Last checked, Open issues |
| tasks | Work still to do and its completion evidence | Name, Created date, Scope, Message, Status, Acceptance criteria, Next action, Evidence, Due date, System, Assigned agent |
| agents | Responsibility and where the agent's work lives | Name, Purpose/scope, Message, Status, Thread link, Last activity date, System, Assigned tasks |
| fails | Observed problems and verified corrections | Name, Reported date, Message, Status, Evidence, Cause, Correction, Verified date, System, Task, Agent |
| activity | One note per work session | Name, Date, Scope, Message, Status, Evidence, Next step, System, Task, Agent |
| development | Proposals and working examples to reuse | Name, Updated date, Purpose/scope, Message, Status, Existing working reference, Proposed change, Test evidence, System, Related task |
| workflow | Ordered expected behavior versus actual implementation | Name, Step, Updated date, Purpose/scope, Trigger, Pages, Expected behavior, Message, Status, Evidence, System, Task |
| changes | History of meaningful applied changes | Name, Date, Purpose/scope, Message, Status, Affected tables/files, Version/reference, Evidence, System, Task, Agent |

`Step` is a whole number. `Thread link` is a URL. `Related tables`, connections, skills and source references can be plain text with exact names, IDs, paths and links. Do not add more tables simply to hold these references. Pages may remain blank for a system without pages.

### Link records without duplicating relationships

- Each of tasks, agents, fails, activity, development, workflow and changes links to `system`.
- `tasks.Assigned agent` links to `agents`; its reverse relationship is `agents.Assigned tasks`. This includes completed history, so task status identifies current work.
- `fails`, `activity` and `changes` each link to the relevant task and agent.
- `development.Related task` and `workflow.Task` link to `tasks`.
- Use automatically created reverse links; do not create a second relationship for the same purpose.
- In the central Agents base, link each activity note through `activity.Bases` to every base it covers. Use the automatically created reverse relationship so the note is visible from each base record. State which bases were changed and which are only planned next work.

### Status choices

| Table/field | Choices |
| --- | --- |
| system.Status | Draft, Active, Retired |
| system.Health | Not checked, Needs work, Blocked, Healthy |
| tasks.Status | Planned, In progress, Blocked, Ready for review, Done, Cancelled |
| agents.Status | Available, Working, Blocked, Stopped, Complete |
| fails.Status | Open, Investigating, Correction ready, Verified resolved, Closed without resolution |
| activity.Status | In progress, Checkpoint, Blocked, Complete |
| development.Status | Proposed, Approved, In progress, Testing, Complete, Deferred |
| workflow.Status | Source confirmed; live unverified, Partial, Not implemented, Designation pending, Live verified |
| changes.Status | Applied; unverified, Verified, Deployed; unverified, Live verified, Reverted |

System Status describes the record's lifecycle, not whether the application works. Start Health at Not checked unless an actual review supports another state. Healthy requires current evidence against the agreed workflow, including any explicitly accepted human steps. Do not invent completion or failure records to populate empty tables.

## 4. Add two system records

### The system's own record

Name it for the system. Include purpose and ownership, exact approved connection/skill references, code locations, related business tables, and an overview of the expected behavior. Record the known running version separately from a local working copy. Store references to credentials, never their values.

Put this short summary in Message:

> START HERE
>
> Last review and evidence type: documentation, source, local test or live test.
>
> State and health.
>
> What works and what remains unverified.
>
> Links to open tasks and development proposals.
>
> Who is responsible; what is actually running versus merely planned.
>
> Next action and latest activity note.

Link operating records to this system record.

### Direct document directory

In `system.References`, provide direct references to the current overview Markdown, resume checkpoint, workflow table, rules where relevant, tasks, development, code/source locations and supporting documents. Use absolute local paths and exact Airtable record/table URLs. Store the full expected overview and workflow in Airtable too, so people without the local checkout can read them. Label older plans, snapshots and proof as historical and state what later decisions supersede them. A file location is not proof of a deployed version or access for another agent.

### How to use this base

Create one reference record explaining the eight table purposes, business tables, working order and session-close rule. Link its References to the system's own record. Do not link work assignments to the guide record.

Working order: read START HERE → workflow and latest activity → choose an existing scoped task → confirm responsibility → perform authorized work → record relevant evidence/changes/problems → leave the next step.

`development` is optional for routine work. Not every task needs every table or field filled.

## 5. Add only necessary business tables

Identify the minimum data the application needs. Reuse proven schemas and preserve stable identity keys. Add only the fields and links required by the agreed behavior.

For copies or migrations, preserve the originals until a separate cutover is authorized and verified. Check record counts, values and relationships. Explicitly identify calculated values copied as snapshots, links that cannot be preserved, and automations/code that still point to the source. Check name-based dependencies before renaming tables.

Use `___` only for identified pending-deprecation tables when renaming is safe. It does not mean unused or safe to delete. Record unsafe-to-rename cases in their descriptions and tasks instead.

### Lessons from normalizing Tenants

- Review each legacy table individually: retain, rename in place, consolidate with evidence, or preserve as `___`. Delete nothing merely because it looks old or was not used in a recent test. Preserve table IDs and links when renaming.
- Consolidate identities only with existing explicit relationships or other reliable evidence; never merge people by name alone. Preserve source provenance and flag conflicting identities. Descriptive roles or imported memberships do not grant access.
- Distinguish a shared entity reference from ownership of the global master. Tenant-specific membership, assignments, personal follows, consent and options are separate concerns; another base must not recreate those masters to support its own service.
- Keep old application destinations until cutover is separately authorized and verified. A new name or copied table does not rewire Astro, Webflow, callbacks or automations.
- Input logs, confirmed target records and event history serve different purposes. A submission being received is not evidence that its final target was saved. A preference save is not delivery proof. Preserve revoked consent.
- Rules record agreed behavior; development records undecided implementation; tasks record executable work. Do not mark proposals or a human checklist as enforced runtime behavior. Domain-specific tables such as favorites or onboarding-stages are not mandatory boilerplate for other bases.
- Where device caching/dirty state applies, document expected cadence, confirmation, offline retry and conflict behavior; keep undecided intervals explicit. Do not transplant one system's UI requirements into another without a real consumer need.

## 6. Record the real work and workflow

Add the known workflow steps and open tasks with plain-language names. For each relevant page, service or manual action, record what starts it and what should happen next. Record current behavior separately from intended behavior.

Before an application change, identify the last working version and how to return to it. For an accepted manual fallback, state who acts, what they do, where the result is recorded and what allows the next step to continue. Do not bypass access controls or use manual execution as claimed automation proof.

Coordinator grouping is a separate proposal. A shared data dependency does not automatically mean two systems need the same coordinator. An agents record does not launch a worker or establish monitoring.

## 7. Verify the setup and try one small task

Read back the table names, fields, relationships and starting records. Verify that START HERE links to the actual open work. Then use one real, small task to check that someone can start, finish or explicitly hand off the work and leave a useful session note.

Keep two conclusions separate: the base is ready for recordkeeping; the application is verified operational. A useful base can accurately show unfinished application work.

## Session-close rule

Before ending a work session, update affected task/agent statuses and relevant proposal, failure and change records. Write or update that session's activity note without erasing prior evidence. Refresh START HERE, Health and Last checked from actual evidence. State what stopped, who owns the next action and what remains unresolved.

Use the existing structure. Do not add tables or fields without a concrete need and owner direction.

Add a concise central Agents activity note linked to the affected bases, include exact evidence and the next agent's first action, and refresh the latest-activity link in Agents' checkpoint. Record meaningful changes locally and globally as relevant without duplicating business tasks. User-facing completion replies should state the result plainly and end without an ambiguous extra caveat.

## Required content check before leaving a base

If review finds a correctable mismatch within authorized scope, say "Fixing now", correct it and read back the result in the same task. Do not stop at a "partial" or "not fully" explanation that requires the owner to request the obvious correction. A genuine access or authorization blocker must still be stated accurately and briefly; never claim completion without evidence.

For a normalized base, reconcile all current physical tables, including operating tables, with table-backed lane references. Preserve separate historical registry entries as legacy. Each reference must name the actual table and URL; a registry-only ___ prefix must not imply the physical table was renamed.

Read back the content, not only the table structure. Confirm START HERE identifies the last completed action and evidence date, actual health, unresolved gaps, the exact first next task and its acceptance, responsibility (or explicitly unassigned), all open tasks, developing work and the latest session note. Connections must identify known bases, pages, services and source locations, with verified, historical and unresolved mappings distinguished. Skills must have exact available paths and explain which task uses them; listing a skill does not prove it was loaded.

Give each unresolved connection or implementation gap an actionable existing task. A generic instruction to review connections is insufficient. Documentation readiness is separate from application completion. Apply this check as each base is stabilized; do not claim every base has been audited. Record the actual stop point and precise next action without creating new tables, hooks or agents.

## Reference examples — not validated templates

- [Agents base register](https://airtable.com/appZahVgD156cMAe3/tbltaHwEyBfcuOHIm)
- [Tenants RESUME HERE](https://airtable.com/app7BXcw801n19QKF/tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y) — formalized business lanes; application health remains Needs work.

- [Recognize base](https://airtable.com/app9kOZdIaGyKk5uG)
- [Recognize START HERE](https://airtable.com/app9kOZdIaGyKk5uG/tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe)
- [How to use Recognize](https://airtable.com/app9kOZdIaGyKk5uG/tblTlw48kcRoEX6yf/recgkEzzza9jzTIdR)

This guide reflects the expanded eight-table model. The earlier `base-operations-model-20261010.json` is the initial six-table snapshot and must not be treated as the complete current template. Verify the live model before copying; copy structure and relevant instructions, not Recognize's people, failures, completion claims or system-specific assignments.

## Starting procedure for any base

For active-session context and usage checks, follow `docs/coordination/context-check-procedure.md`; keep a compact resume packet and never import a long dialogue as the operating record.

1. Read its Agents registry entry, current schema, records and latest checkpoint. For a new base, establish the agreed purpose and minimum data first; for an existing base, inventory what is already present.
2. Identify existing working code, automations, connections and source versions. Record each table's writers, readers and downstream consumers. Keep inspection separate from executing the business workflow.
3. Define what this base owns, what it references elsewhere and what remains undecided. Preserve established identities and working paths; normalization does not authorize moving a live integration.
4. Reuse equivalent operating tables and minimum necessary business fields. Review each legacy table and duplicate individually, retain provenance and check dependencies before renaming. Do not copy another base's domain-specific schema as boilerplate.
5. Document the expected workflow alongside the actual implementation and evidence. Distinguish intake, processing and confirmed outcomes; document applicable retry, failure and human-handoff behavior without inventing new mechanisms.
6. Fill the overview, document directory, state, health, last stop point, open tasks, development and one exact first next task. Identify responsibility or explicitly leave it unassigned. Keep proposals and unverified behavior clearly labeled.
7. Read back contents and links, set the base's Resume here URL, reconcile its base-lanes, and add an Agents activity note linked to all affected bases. Lock confirmed structure only; application acceptance requires its own evidence.

Base-specific starting instructions belong in that base's task, system or linked activity records—not in this reusable guide.

Reuse the saved coordination proof only if a coordinator/worker assignment is requested: [Agents proof reference](https://airtable.com/appZahVgD156cMAe3/tblblIlvppDTFdpor/recibtAD67D2ycwdH). It does not authorize starting expired tests, new agents or monitors.


## Observed recurrence and claim checks — 10 October 2026

The existing reconciliation instruction was present but was not consistently executed. Adding prose alone does not prove prevention. Apply these checks within existing task/handoff records; no new hooks, tables or approval gates.

- Before claiming an overhaul complete: compare every accepted decision with current table IDs, names, links and operating records. Distinguish inventory correctness, structural completion and runtime acceptance. Each claim must name the acceptance it actually satisfies.
- Before claiming a worker active: dispatch first, record the returned worker ID and execution location, obtain acknowledged progress, and inspect actual current state. A saved prompt is Prepared. An ended worker is not Working.
- On topic changes: retain unfinished assigned obligations and their next executable action in existing tasks. Do not silently replace an unresolved Recognize assignment with a different Tenants assignment.
- When challenged: inspect the specific disputed item and decision history before agreeing, denying, editing or assigning a cause. Field counts do not establish wrong table names. Do not fabricate a failure explanation to agree with the owner.
- Close a recurring failure only after its correction is observed in subsequent work. Record timestamp/timezone and actual model/effort when known; otherwise record unknown. Do not infer causation from effort changes, context length or token use.

Evidence: Agents complaints rec8jIV2bAgsr8FkZ and recdhCDN3p5dUr3j2; Tenants fail recO5G1sYDCA8u6KA. Actual Recognize dispatch is now /root/recognize_completion, registry recvIfHlAJKjTmbBE. This establishes one real dispatch, not sustained prevention or completed Recognize implementation.

## Table names
Use one clear word where practical; otherwise lowercase words separated by hyphens. Preserve the agreed ___ prefix on deprecated tables, followed by the same lowercase/hyphen rule. Keep stable table IDs and existing meaning; do not rename fields as part of table naming. Check name-based consumers before renaming, and update the owning index to the exact physical name.
