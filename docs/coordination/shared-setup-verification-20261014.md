# Shared setup verification — 14 October 2026

## Outcome and scope

Completed the authorized shared-guidance/source-ownership correction. This is verification of guidance, record navigation and bounded agent adoption, not a declaration that Recognize, Tenants, Messages, Publish or Engines is complete.

No product implementation, live business writes, runner execution, deployment, publishing, SMS, hooks, new monitors or model changes were performed. Existing agents received guidance-only read assignments and stopped after their receipts. Existing product stops remain controlling.

## Changes actually made

- Defined authoritative sources in `domain-coordinator-structure-20261013.md`: Company Home for grounding/navigation; AGENTS for generic rules; maintained domain contracts and exact proven Git source for accepted behavior; owning system/tasks for current state and next action; skills for procedure; activity/changes for evidence.
- Corrected the skills-role entry to `rs-skills-start.md` and prevented an inventory backlog from replacing the owner's current full-role assignment.
- Updated five existing domain entrypoints: Recognize, Tenants, Messages, Publish and WEF Engines. Historical dispatch and monitor instructions do not restart stopped work.
- Updated Instruction Loader, Handoff, rs-coordinator, Recognize and Tenants custom skills; updated the coordinator context reference and maintenance workflow. Corrected a stale frontend-agent-proof reference.
- Updated Agents Start Here, four existing domain system records and Company Home with current resume/source references. Preserved product health and approval boundaries.
- Added Engines' actual resume link to the existing Agents trace task because Engines has no system table. Corrected that task's next-action reference to include the October 13 recurring-task/manual-preservation findings alongside the October 11 trace. Discovery remains stopped.
- Updated the five existing skills-included revision records. No new inventory or duplicate complaint was created.

## Verification

| Check | Evidence/result |
|---|---|
| Modified source set | 15 source files differ from preserved before-images; exact SHA-256 before/after values saved |
| Local links | 14 links checked; none missing |
| Skills | Five frontmatters and shared-map references checked successfully |
| Standard skill validator | Not rerun: prior check established missing PyYAML; direct checks used without dependency installation |
| Agents/Company Home | Exact readback comparisons passed for Agents Start Here, four domain system updates, Engines resume link and next action, and all four edited Company Home blocks |
| Skill registry | Five existing source revisions updated and independently read back |
| Existing agents | Seven completed scoped guidance-only reads; parent inspected read commands, successful outputs and receipts |
| Fresh reader | New subagent received no full-chat history; recovered actual Messages/Publish proposals, checkpoints, allowed scope and Engines resume source through bounded source/connector reads |
| Product boundaries | Inspected reload turns contained local read/hash commands, not product execution; fresh-reader assignment was read-only except its evidence artifact |

The fresh reader found Publish proposal `recH1SolPEAYk7AkD`, Messages proposal `recAvn2x1x77OSoB5`, and their existing planned tasks. It preserved Proposed/Deferred/Planned distinctions and did not invent a current Blocked status or live success. It correctly separated Messages feed processing from Publish display, Recognize identity and Tenants permissions. This demonstrates scoped retrieval and reasoning without owner reeducation; it does not guarantee every future handoff.

## Reload receipts

| Recipient | Existing chat | Completed turn |
|---|---|---|
| Recognize | 01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5 | 01a13bc9-6fd0-7083-b347-335a44e4123b |
| Tenants | 01a12690-0e9d-71f0-bb85-000d02307e8d | 01a13bc9-77c4-7d92-ac72-110e0966c3af |
| Messages | 01a12690-e713-7fa1-8787-87b45eceb1a3 | 01a13bc9-daec-7b62-8af6-6cc3f2e614dc |
| Publish | 01a12778-d699-7c92-baa7-81f46ddb1b64 | 01a13bcb-422c-7121-8777-6a15fb41b63b |
| Coordinator | 01a126ee-2608-7623-b5db-55d350e59cd0 | 01a13bcc-9aa2-7221-935b-7a8153168564 |
| Webflow | 01a120ec-8fef-7322-8ca4-6cfc1ed17701 | 01a13bcd-6810-7f71-ad21-e2e34f42f123 |
| Skills and agents continuity | 01a13599-5856-7523-b416-9a16fd03aa20 | 01a13bcd-aa90-7f90-8a9f-38dc102334ce |

No Engines chat recipient was invented. The existing trace checkpoint was updated without starting discovery.

## Agents records

- Task: https://airtable.com/appZahVgD156cMAe3/tbluT1yhP1FJMb8Cm/reckrLZ1WRfUTawWG
- Change: https://airtable.com/appZahVgD156cMAe3/tblilqxgvWvQOS83B/recqSDxqAnBVuvLM9
- Company Home: https://chatgpt.com/space/page_6aca71a8c7d881918e6687969a6aad48

Existing wider handoff complaint remains open. This task does not close operational acceptance or the separate inventory task.

## Exact evidence locations

Under `.git/ringstatus-control/`:
- `shared-setup-20261014-before.json`: full source before-images, including pre-existing edits.
- `shared-setup-20261014-registry-before.json`: prior remote fields/Company Home.
- `shared-setup-20261014-hashes.json`: all 15 source hashes.
- `shared-setup-20261014-validation.json`: direct static validation.
- `shared-setup-20261014-reload-evidence.json`: seven inspected reload turns and checkpoint readback results.
- `shared-setup-receiver-20261014.md`: independent fresh-reader findings and source ledger.
- `shared-setup-20261014-final-readback.json`: final registry/task/change/activity readback comparisons.

Evidence in .git is local, not automatically available to another host. A successor must verify access rather than assume a path transfers it.

## Resume

Use `docs/coordination/rs-skills-start.md` for the full Skills and Agents role, then the current owner request and relevant owning checkpoint. This setup task is complete; it does not assign inventory or resume any product task. Existing operational gaps remain in their respective task records. No new architecture or recurring enforcement was introduced.

