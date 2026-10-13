# RingStatus coordinator handoff

Prepared October 10, 2026, 17:39 UTC / 1:39 PM Eastern. Snapshot, not a claim of future state.

## Purpose and first action

SUPERSEDING OWNER ROLE CHANGE: chat `01a11632-62fd-73b1-997e-8be4119185de` now exclusively maintains custom skills. Its operational three-specialist monitor is PAUSED; specialist work is not cancelled. No successor monitoring transfer has occurred. See `docs/coordination/custom-skills-maintenance-owner.md`. Earlier statements assigning active operational supervision to this chat are historical.

Coordinator entrypoint: `C:/Users/gombc/.codex/skills/rs-coordinator/SKILL.md`. Owner requested this skill to oversee Recognize, Tenants and Messages using existing Request Router, Instruction Loader, Boundaries, Planning, Code Review, Rebalance and Handoff skills plus context/capability checks. Existing combined monitor now covers all three, retaining five-minute progress and fifteen-minute usage/task checks; no additional monitor or chat created.

Latest supervision change, owner-requested October 10: domain skills now exist at `C:/Users/gombc/.codex/skills/recognize/SKILL.md` and `C:/Users/gombc/.codex/skills/tenants/SKILL.md`. Existing `recognize-and-tenants-final-timebox` has been repurposed as ACTIVE five-minute progress monitoring with fifteen-minute task/usage checks under `docs/coordination/specialist-skill-supervision-20261010.md`. This supersedes older paused-monitor snapshots, but does not renew worker budgets or authorize implementation/releases. Original coordinator retains ownership; do not duplicate it.

### Superseding checkpoint — 10 October 2026, 18:03 UTC

The earlier ACTIVE monitor and worker snapshots below are historical. The final-timebox monitor was paused after both workers reached their assignment boundaries. Owner has since separately APPROVED TO EDIT the Recognize Sessions lookup-failure/retry defect; dispatched to rs-recognize and active start verified at 18:01:46 UTC, with a 20-minute limit from receipt. Deployment/publication is not included. Tenants was dispatched to finish its own read-only concrete release review within 10 minutes, rather than requiring the owner to do technical review. Messages finished its explicitly reference-only retrofit/map; runtime implementation remains unassigned, and this coordinator owns preparing that assignment. No new monitor was created. Use current worker evidence for subsequent state.

Accountability correction: assignment completion is not product completion; generic "release review" is not an actionable owner handoff. See specialist-shared-contract-20261010.md, Completion ownership clarification. Locked business behavior, proven baselines, acceptance and AGENTS authorization rules remain unchanged.

Continue coordinating the existing Recognize, Tenants and Messages specialists toward their actual agreed outcomes. Do not create replacement workers or reconstruct the long conversation. Read current AGENTS.md and this packet; take one compact wait_threads snapshot per specialist. Read only changed evidence needed to choose an action. Report the next concrete executable step, then perform authorized work. Do not stop after reporting routine status.

This original coordinator remains open at the owner's request until the owner is satisfied with the successor's behavior. Original chat: 01a11632-62fd-73b1-997e-8be4119185de. The successor initially reviews and prepares next actions; avoid issuing duplicate worker instructions while the original owns timed supervision. Explicitly record which coordinator owns a dispatch before sending it. Reading a handoff does not transfer permissions or tools.

## Operating behavior

- Be concise, direct and practical. No ambiguous outro, repeated explanations, promises or technical choices pushed back to the owner. Fix discrepancies within existing authority rather than merely describing them.
- Finish authorized independent work; a blocked action does not stop unrelated work. Do not re-ask already settled design choices. Distinguish a real owner business decision from routine implementation judgment.
- Preserve supplied working code. Inspect exact baseline and smallest compatibility delta; no replacement or speculative infrastructure. On failed verification, follow AGENTS.md: no further correction without applicable separate APPROVED TO EDIT. Do not layer guesses over failures.
- Local tests, documentation, registry updates and completed subagents do not prove a live end-to-end workflow. Never report completion without actual acceptance evidence.
- Respect current shared-file ownership, previous failed paths, owner changes and protected scopes. No deployment of a checkout containing unfinished sibling changes.
- Load only relevant skills. Do not implicitly activate ringstatus-control or add brainstorming approval gates to already-approved behavior. Existing specialist packets list applicable skills and proven sources.
- Use compact snapshots and stable record IDs. Do not print full histories or schemas. Follow docs/coordination/context-check-procedure.md at task boundaries and after 30 active minutes; no new hooks or automatic monitors just for context checks.

## Existing specialists and latest evidence

| Chat | ID | Snapshot and next step |
| --- | --- | --- |
| rs-recognize | 01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5 | Active saving release evidence. Isolated artifact passed focused checks and build, excluded unfinished Tenants files. NEW reviewer finding: Airtable lookup failure can leave Sessions claim stuck, preventing silent recognition in that tab after recovery. Release blocked; inspect exact finding and baseline, no self-authorized correction loop. |
| rs-tenants | 01a12690-0e9d-71f0-bb85-000d02307e8d | Active on explicitly approved completion corrections and isolated artifact verification. Must preserve actual barns/riders load/edit/add/save/readback/reload acceptance, plus locked persistence checks. Automation is an OFF draft; live completion unverified. |
| rs-messages | 01a12690-e713-7fa1-8787-87b45eceb1a3 | Idle, scoped base retrofit complete: 11 tables/index and five deliverables read back. Runtime not complete. Next optional task is read-only Twilio routing/callback/suppression verification; engine separation deferred, code reference-only. |

Read the specialist's current owner messages for newest approvals. Prior broad approvals are not evidence of authorization for every later correction, live test or release. Original coordinator is explicitly authorized to message all three. A successor should obtain direct user authorization through its startup prompt before cross-chat messages; do not assume this document itself grants it.

## Active timebox and monitor ownership

Owner window: 17:33:03–18:03:03 UTC on October 10, 2026. Reserve begins 17:53:03 UTC. Eastern: reserve 1:53 PM; cutoff 2:03 PM. Preserve any earlier stricter limit; no automatic extensions. If this packet is opened later, do not restart expired work.

ACTIVE heartbeat `recognize-and-tenants-final-timebox` belongs to original coordinator, every three minutes. At reserve it sends at most one save/verify reminder per running worker; at cutoff one stop-and-save instruction per running worker, records gaps and pauses itself. No restart of idle work. Best-effort checks, not guaranteed termination.

State: .git/ringstatus-control/finalization-timebox-20261010.json. Read current cursors/dedup flags before action. Do not create another monitor or move/disable this one until an explicit supervision transfer. Earlier usage/Recognize/onboarding monitors were previously paused; recheck only if changing automation ownership. Keeping this chat open does not require continuous polling.

## Sources and scope

Approved repo: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus.

Read the relevant packet, not all three by default:
- docs/coordination/specialist-shared-contract-20261010.md
- docs/coordination/recognize-specialist-handoff-20261010.md
- docs/coordination/tenants-specialist-handoff-20261010.md
- docs/coordination/messages-specialist-handoff-20261010.md

Existing shared Cloud source: C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test. Baseline 18e6e993aeaff6fd53168dac4d6f1b7b46baf195. Use actual specialist release manifests for new isolated artifact locations; never assume the shared checkout is releasable.

Recognize: native Webflow presentation, Astro data/actions; public pages silent without forced login, unknown gated browser verifies by code in that same browser, known browser silently reaches /rs-member. Cookie target365days. Identity/session workflow separate from onboarding/profile. Only approved SMS test number16318752160; test authorization still must apply to the specific action.

Tenants: tenant=barn; people consolidates staff/owners/contacts/grooms/exercisers, horses/riders remain shared entities. Membership independent of weekly entries. Primary trainer horses+riders; additional trainer horses only. Favorites separate; groom assignment limits and explicit consent preserved. Astro input logs -> Airtable automation -> designated records is already agreed. Shared IndexedDB persistent optimistic dirty-state behavior is locked in docs/coordination/shared-data-behavior-contract.md. No repeated processor/design approval questions. Real live person/barn membership must be evidenced, never guessed.

Messages: preserve current two-way Worker and native Recognize OTP flow. Current preference writer still uses Recognize until Tenants cutover verified. Deferred technical/business policies belong in development, not blockers to completed base retrofit. Evidence: docs/coordination/messages-retrofit-acceptance-20261010.md and messages-source-inventory-20261010.md.

## Airtable navigation

- Agents appZahVgD156cMAe3; handoffs tbl5GR2kL4BlUbfJx; agents tblmfGCqj3XZiXDBV; bases tbltaHwEyBfcuOHIm; activity tblITeJfxlIhlDvGk.
- Handoff records: Recognize recGaodxumKRecL9i; Tenants rec6kFNbb87Iggd6S; Messages recnfd1H9rHLMO5C9.
- Recognize app9kOZdIaGyKk5uG; system tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe; tasks tbltfO97Vy1StVVkO.
- Tenants app7BXcw801n19QKF; system tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y; tasks tblXxPquz9jtdQM3n.
- Messages apphwDLkVCp8ut2Hd; system tblycxFymN5gaPKlT/recjGDLwQ5U0VliiE.

Preserve correct domain/task ownership. Record actual dispatch, evidence and unfinished gates. Read back mutations. No new tables just for this transition.

## Company Home work to retain

Company entry point established: https://chatgpt.com/space/page_6aca71a8c7d881918e6687969a6aad48 is linked prominently from the RingStatus Space entrance and Agents system/Start Here rec9chkIJwhTU7O4H. Use it for company orientation and relevant grounding; do not load every linked source for every task. Domain bases retain operational authority. This supersedes the example-only navigation status below, not existing factual approval boundaries.

Example Page: https://chatgpt.com/space/page_6aca71a8c7d881918e6687969a6aad48 in existing RingStatus Space. Company -> brand/voice -> products -> systems -> execution. This is a navigation example, not an approved migration of all documentation. Existing company and branding Pages must be reused.

Grounding: https://chatgpt.com/space/page_efc476f296ec8191870b9c9ce19df26e and https://chatgpt.com/space/page_e21d3f79e9a881919e957a26575f9dd6. Company context starts above technical systems. Airtable holds structured operational records; repository holds code/versioned technical contracts; Pages provide readable orientation and links, not duplicate competing truth.

## Successor first response

After actual snapshots, state in a few sentences: what can be done next, what specific real dependency prevents a live finish, and who currently owns timed supervision. Then continue authorized work. Do not recreate the system, restart expired tasks or claim behavior will be identical merely because this packet exists.
