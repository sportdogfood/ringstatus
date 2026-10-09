# Webflow native rebuild: cost and correction limits

Effective October 8, 2026. Owner instruction: document the expensive repair cycle and implement time and correction limits without reducing output quality.

## Lesson and evidence

The reviewed turn `01a11915-69ee-7ff2-af4d-8858f2a2ee5c` in runner `01a11835-8cca-78e3-8342-c03a13d6ba93` took 2h 39m 37s. It completed Feed verification and saved substantial SMS Alerts work; Schedule remained unstarted. Feed corrections included inherited code forcing width/margin, scroll structure, inline token wrapping and numeral styling. Connection failures also occurred. This was not three hours spent solely waiting for Bridge.

The execution record contains 266 model response usage records and input context growing from 130,803 to 518,446 tokens. Output usage was 225,777 tokens, including 170,107 reasoning tokens. These figures are usage telemetry, not a dollar invoice. Detailed breakdown: `C:/Users/gombc/.codex/visualizations/2026/10/07/01a11632-62fd-73b1-997e-8be4119185de/webflow-runner-efficiency-review.md`.

The owner had identified earlier drafts as unsuccessful attempts. The assignment nevertheless constrained the runner to those existing drafts. That made inherited structure and scripts a material cost risk. A clean draft could have avoided some inherited problems, but no comparative clean-build test establishes its speed or total cost. A new draft can still inherit site-wide scripts and styles.

Latest inspected state: the runner reached SMS Alerts Preview, but Save opened Webflow's built-in form dialog instead of prototype validation. Inspection then stopped at the dialog. This is an unresolved behavior failure, not permission for a speculative correction.

## Mandatory task limits

These are conservative operating budgets selected for the owner's request, not measured completion estimates. All limits are elapsed wall time, including tool waits and verification. The earliest limit wins.

| Limit | Rule |
| --- | --- |
| Existing-draft triage | At most 10 minutes before further construction. Inspect current structure, existing scripts, shared dependencies and conflicts. Record reuse-versus-clean-draft rationale. |
| Page work | At most 60 minutes per page from the next owner-authorized execution. Reserve the final 15 minutes for verification/checkpoint; do not start fresh construction in that reserve. |
| Corrections | At most two edit-and-retest cycles per defect, and at most 20 minutes correcting defects in aggregate per page. Count native element, style, interaction and behavior-code changes alike. |
| Access recovery | At most one supported recovery attempt and five minutes total per access incident. Stop dependent work if access remains unavailable. Do not repeatedly rediscover or retry the same connection. |
| Reset prevention | Preserve elapsed time, defect IDs and attempt counts in the checkpoint. A new chat, compaction, resume, defect rename or successor runner cannot reset the budget. Only an explicit owner extension can do so. |

Existing elapsed history stays recorded. The new 60-minute budget applies prospectively to remaining work; documenting/adopting this policy does not itself launch or authorize work. Existing page-specific prototype edit authorization remains valid. The separate APPROVED TO EDIT rule in root AGENTS applies to failed cadence/workflow/scheduled/production-data verification; it is not an additional gate for correcting this browser-local prototype within its approved page scope. Repository code, site-wide scripts and business workflows remain outside scope.

Before each mutation or retry, check remaining time and the affected defect's attempt count. A correction cycle is a coherent change for one evidenced defect followed by its targeted retest. Multiple tool calls in that coherent change are one cycle; unrelated defects cannot be hidden inside the same cycle. A second cycle requires new causal evidence or a corrected root-cause diagnosis. Do not stack compensating patches. Stop at the limit with saved work, exact remaining gates, observed cause, budget consumed and the specific decision needed. Never lower acceptance, report acceptance merely because a budget expired, or silently grant another time block. Preserve the complete unfinished task.

## Clean-draft decision

For future prototype recreations, prefer a clean unpublished native draft when the owner reports an old draft is a failed experiment or triage confirms conflicting inherited structure/scripts. Verify site-wide inheritance before treating it as clean. Keep the original intact and record source/target IDs. Native editability, source fidelity and all required viewport/theme/behavior tests still apply.

Current authorization permits only the three existing Feed, SMS Alerts and Schedule drafts. It explicitly excludes new pages. This policy does not override that restriction or authorize deleting/replacing a page. If a clean draft is the better route, stop repair at the decision gate and obtain the necessary page-scope change instead of spending the budget on salvaging the old draft. Verified Feed must not be rebuilt to follow this policy.

## Efficiency without reducing quality

Owner correction October 8: one bounded task end to end per runner; use a fresh runner for the next distinct task. Future handoffs separate native visual reproduction plus its rendered verification from JavaScript behavior plus its functional/regression verification. Preserve existing working code and record uncompleted functional requirements; accepting a visual deliverable does not complete the full page. Scope partition does not reset cumulative page budgets. The existing runner has been asked for a read-only full checkpoint and told not to advance to Schedule; no new runner or replacement draft is launched by this policy change.

- Inspect source hierarchy, tokens, responsive transitions, interactions and target script conflicts before construction. Fix the underlying cause within approved scope.
- Batch coherent native operations. Read back partial results and apply only missing changes.
- Return compact tool summaries; read exact subtrees and styles rather than repeatedly loading entire pages.
- Reuse valid evidence for unchanged work; rerun affected checks and retain the complete final acceptance requirements.
- Save a compact page checkpoint with source version, exact IDs, protected changes, cumulative elapsed time, defect counts, evidence and unresolved gates before any handoff.
- Preserve no-publish, no-production-data and no-unrelated-code boundaries.

## Application and enforcement boundary

Checkpoint review completed in runner turn `01a11b9f-b7b6-7542-9095-38d58df513f3`: Feed retains recorded visual/behavior PASS, not a fresh recheck. SMS Alerts has native construction and its page-local adapter saved, but no completed rendered fidelity acceptance, and Save currently fails prototype validation in Preview. Schedule is unstarted in this assignment. The latest recovery made zero edits. Its budget checkpoint incorrectly stores 14,455.2855555 seconds; the recorded UTC timestamps span 55.278265 seconds. Both the parent calculation and runner review confirmed the discrepancy. The erroneous checkpoint has not been overwritten by this review; reconcile it before using it as a future budget source. The one-recovery-attempt limit was genuinely reached independently of that duration error.

Current path: preserve all drafts and saved behavior; current runner stops at this checkpoint and does not advance to Schedule. Once responsive inspection is available within an authorized recovery allowance, the next bounded deliverable is SMS Alerts native visual reproduction and verification, with no JavaScript repair mixed into it. Reuse working controls to expose visual states; inability to reach a required state is an explicit functional dependency, never a visual pass. A separate fresh runner handles functional correction/testing against the preserved visual baseline. Keep all existing functional gates in the backlog and do not call the full page complete until both deliverables pass. No new runner was launched by the checkpoint review.

Prompt preparation is now wired into the installed Handoff skill at `C:/Users/gombc/.agents/skills/handoff/SKILL.md`, which requires `references/ringstatus-webflow-prompt.md` for RingStatus native Webflow prototype handoffs. That procedure defines the pre-dispatch clean/existing decision, six-block prompt, concrete next action, full acceptance, cumulative budgets and post-run measurements. It preserves the skill's explicit-only invocation policy; this is not automatic loading in every chat. Rollback of this integration: remove only the RingStatus Webflow section and its reference after checking for later edits. The pre-change entrypoint is saved at `C:/Users/gombc/.codex/visualizations/2026/10/07/01a11632-62fd-73b1-997e-8be4119185de/handoff-skill-before-webflow-template-20261008.md`.

Apply this document to the current runner through an explicit task instruction and obtain its acknowledgment of these exact limits before subsequent work. The current assignment remains Feed → SMS Alerts → Schedule; Recognize and Barn setup are protected, not additional assigned builds.

Delivery verified October 8: runner `01a11835-8cca-78e3-8342-c03a13d6ba93` read the file and acknowledged all four limits, cumulative checkpoint accounting, owner-only extensions, protected Feed and existing-page restrictions in completed turn `01a11b8d-4104-7153-8d66-3f3869937606`. It reported no conflict and did not resume implementation. Subsequent execution compliance has not yet been tested.

Subsequent correction: the adoption-only hold ended with that turn. On the owner's report that the runner was not resuming after Bridge recovery, it was instructed to resume current SMS Alerts inspection and authorized prototype correction/verification under these limits. The earlier message's extra APPROVED TO EDIT gate was a mistaken application of the business-workflow rule, not a new owner restriction. No new page or broader edit scope was granted.

These are task operating instructions. The current disabled PreToolUse prototype and completion-only Stop hook do not enforce a stopwatch, count correction cycles or prevent every write. No new hook, resident process, runtime repair or automatic monitor is installed by this change. Acknowledgment establishes receipt, not proven compliance. Do not describe these limits as a mechanical spending cap.

Rollback: an owner instruction can revise or withdraw this policy prospectively. Preserve prior incident evidence and approved artifacts. Changing limits does not grant page creation, code editing, publication or production access.

Complaint mapping: rendered fidelity (`recFC8bFpQCGsxCxB`), native interaction proof (`recRW0nqJFLyQ04mt`), source tokens (`recAkDVrWZZv0ufDD`) and preserved full scope (`recNOlUMUFI4qZ8p8`) remain required. The new owner-imposed budget stop supersedes continued spending at a limit; it does not mark the unfinished work complete or claim these complaints solved.
