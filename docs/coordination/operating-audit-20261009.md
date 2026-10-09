# RingStatus operating setup audit and improvement plan

Revision242 update, 9 October 2026: the owner explicitly requested installation of the proven-baseline reuse rule before the broader plan. That narrow rule is now installed in project AGENTS.md, common-delivery-stages.md, COMPLAINT-COVERAGE.md and local planning/coding-instructions/handoff skills plus the handoff task record. It prohibits reinvention/replacement without explicit owner request and patching over a failed patch; it requires exact baseline inspection, protected behavior and authorized delta before dependent work. The original audit below remains historical. All other skill amendments remain proposed; application work is unchanged, cloud/package copies are not assumed synchronized, and behavioral compliance is unverified. Existing C004/Recognize baseline/coordinator complaints remain open. Decision record: rec4eYaftDe3nftD7. Backup/readback: .git/ringstatus-control/proven-baseline-rule-20261009-{before,verification}.json.
Date: 2026-10-09, America/New_York. Request revision239.
Owner/coordinator: 01a11632-62fd-73b1-997e-8be4119185de.
Scope: research, documentation, complaint classification and agents-base bookkeeping. No application repairs, deployments, new agents, schedule activation or skill rewrites.

## Finding
The project/repository connection is correct. The operating setup is only partly implemented: durable records, skills, agents and monitors exist, but current ownership, skill versions, actual execution and acceptance were not consistently reconciled. More hierarchy is not a demonstrated solution. The immediate plan repairs those gaps using existing components and qualifies one bounded delivery before scaling.

90 existing complaints were retrieved with no continuation cursor and individually mapped below: 46 have an official documented method, 41 have partial guidance, and 3 have no direct OpenAI solution found. None is marked resolved merely because documentation exists. Two newly observed audit failures (registry freshness and skill-source conflicts) are recorded separately. Historical claims of repairs remain distinguished from fresh verification.

## Project identity and effective folder use
The app reports local project ringstatus, ID7073fdbc-f525-4251-aedc-63fc27eaf755, at C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus, isGitRepository=true. Git independently returns the same root and origin https://github.com/sportdogfood/ringstatus.git. Thus it is an app project pointing to the repository, not a disconnected ChatGPT-only folder. If a different similarly named project is intended, its identity remains unverified; no second ringstatus project appeared in the project listing.

Use this primary folder for shared source, scoped project instructions and the existing docs/coordination records. Start distinct outcome chats within it. A project does not automatically give every chat all sibling transcripts or local files to a cloud runtime. Verify each receiving checkout/access. Keep exact deployed-source identity separate from this checkout and worktrees. The deployed Recognize correction used C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus; that is not a new authoritative business-document home. OpenAI supports separate outcome chats and durable checked-in guidance. [P]

Proposed compact entry point: the existing docs/coordination/common-delivery-stages.md should link the current assignment list, source/deployment map and evidence. Keep long logs under task evidence paths and fetch them only when needed. Keep reusable RingStatus skills versioned in an approved repository location; personal/cloud copies need explicit version/distribution mapping. Do not relocate or rewrite them during this audit. Files under .git are machine-local evidence, not automatically versioned, cloud-accessible or transferred by a link.

## Why delivery was difficult
These are evidence-backed operating failures, not a diagnosis of model intentions.
- Readiness was inferred from prototype history and green local test counts. The actual recovery destination was still /test/onboarding; the missing native invitation handoff surfaced during the owner's phone test. Revision232 corrected it, and revision234 records provisional owner acceptance, not full closure.
- Integration dependencies were discovered late: canonical person/phone mapping, real Airtable Twilio behavior, deployed source compatibility, atomic claims, secrets and actual runtime. The full path should have been mapped and one representative journey exercised before broad implementation.
- Visual recreation, inherited-draft repair, JavaScript behavior and backend work became entangled. The handoff reference now separates them, but that procedure was not consistently reflected in delivery.
- Access failures and permission wording sometimes blocked unrelated executable work. Conversely, timers and prompts were treated as though they guaranteed continuation. Neither confidence nor a monitor configuration establishes progress.
- Registry entries became histories rather than concise current control records. Six legacy active-threads rows still have October5–6 modification dates. Recognize was updated October8 at22:24:31 Eastern and audit at16:08:02, so not every row was older than24hours; neither captured the latest complete disposition at this audit.
- Supplied skills, actual local skill content and Airtable summaries diverge. Loading all third-party skills would add conflicts and context rather than solve this.
- Coordination and repeated broad inspection added overhead. This audit itself initially requested excessive Airtable output and hit one PowerShell formatting error; stored results were reused and the command corrected once. These are logged as continued examples of the existing cost/execution complaint, not evidence that the pattern is solved.
- Astra High use is documented for the coordinator; the coding worker also used Sol Low. No controlled evidence assigns the failures or the reported35% usage to a single model. The comparison plan below is required before asserting a best setting.

Sources: current complaint records; recognize-wiring-task.md; recognize-failures-model-effort-review-20261008.md; common-delivery-stages.md; current worker snapshots; actual skill files.

## Airtable records: purpose and changes
Base appZahVgD156cMAe3.
- active-threads: one identifiable assignment/conversation with owner, exact worker/chat ID, state, next action and evidence. It is not the runtime. An active checkbox is not proof a worker is running. Preserve historical ownership; refresh known task dispositions and register missing current assignments.
- rs-agents-complaints-lib: incident, accountability, supporting source, proposed/applied correction and closure evidence. Existing complaint-status and support-doc-found fields had no choices and no populated values in this90-row snapshot. Populate both; add only remediation-state to distinguish Proposed, Implemented-unverified and Verified. Existing solution/source fields hold specific changes, checks and report location. Never equate Method found with fixed.
- rs-decisions: one significant decision or change per record, with date, reason/boundary, outcome and evidence; not a task list, heartbeat transcript or completion certificate. A choice may be Recorded or Planned before it is Implemented. The table has19 records before this audit; create an audit/plan record linking this report and the complaint matrix. Do not label planned skill changes Implemented.

Snapshot before writes: .git/ringstatus-control/operating-audit-20261009-before.json. This holds full original records for comparison and scoped recovery. Reverse only the audit's new field values/notes after comparing subsequent edits; never overwrite newer user changes wholesale.

## Current task disposition and remaining headwind
| Assignment | Evidence / disposition | What still needs acceptance |
| --- | --- | --- |
| Recognize | Old worker stopped; later coordinator deployment5684b4178 and owner's successful corrected recovery are retained. Provisionally accepted for now. | Owner corrections, native Continue-to-Barn, correlated device/session and return visit, retained profile/device/access and deployed recovery/retry/replay cases. |
| Barn Inputs | Internal worker completed scoped source preparation and reported203 regression plus11 lifecycle/auth checks. This is worker-reported local evidence. | Reconcile with combined deployed version and prove actual native read/write/error/log path; do not rebuild already applied source. |
| Barn Onboarding | Internal worker completed native mapping/adapter and reported24 local checks. | Published mobile/desktop interactions, save/reload and actual storage/logs; approved native styling remains owner-controlled. |
| Barn Opt-ins | Internal worker reports33 subscription and203 regression checks, local independent Workers/restart. | Grant notice, engine mappings, parameters/timezone, native mounting and deployed browser/storage proof remain in its report. |
| Feed | Owner said draft ready; prior visual evidence retained. | Do not infer data integration complete from ready draft; check exact remaining mapping/behavior before resuming. |
| Schedule v24 — new native Webflow draft | Fresh snapshot: notLoaded, latest run completed with30 saved visual fixtures; responsive/control details unverified due to target browser access. | CSS/visual gates, control behavior and separately scoped Astro data mapping/full-system proof. |
| Onboarding drafts — final consistency audit | Fresh snapshot: stopped by owner; designs unaccepted. | Retained cross-draft buttons/type/tokens/spacing/padding/margins and viewports/themes, especially Feed/SMS Alerts/Barn setup v24; include Recognize/Schedule. |
| RingStatus coordination — current projects… | Fresh snapshot inactive; latest statement was an old Schedule status read. | It has not become the accepted successor coordinator merely because a handoff exists. |
| Coordinator A — three-worker heartbeat… / Coordinator B — SMS Alerts heartbeat… | Fresh snapshots: simulated token released, observers paused, results frozen. | Proof-trial results do not establish production coordination. Retain upper-agent proposal for later collision-only discussion; do not activate it. |
| Three attached internal Barn subagents | Completed runs according to collaboration tool. | Run completion is separate from full application acceptance; reuse reports and exact manifests. |

The six named external worker snapshots were read without messaging/resuming them. Broader legacy rs-* owners have not been newly executed or taken over. Their historical active flags remain explicitly unverified instead of falsely treated as current runtime state.

## Skills to amend — proposed changes, not installed fixes
Local custom skill root: C:/Users/gombc/.agents/skills/. Eight of ten skills2 names have matching local SKILL.md files there. ringstatus-context and ringstatus-capabilities are absent at that path; cloud skill links remain in skills2. This does not prove they are absent from every plugin or unavailable on another surface. Resolve their real package/version before dispatch.

| Source | Observed gap | Narrow amendment and verification |
| --- | --- | --- |
| request-router/SKILL.md | Local file preserves opt-in modes and recommends route; skills2 cloud summary says terminate router ownership and no workflow modes. | Pick canonical local/cloud versions and distinguish route-only transfer from continuing coordinator ownership. Test one handoff without owner relay. |
| instruction-loader/SKILL.md | Local says applicable sources only; skills2 summary requires full bundled globals/complaint safeguards. Actual catalogue descriptions are heavily truncated. | One task-sized source/skill manifest: path/version, READ/SUPPLIED/MISSING, target access and conflicts. Never reload every complaint on each run. |
| rebalance/SKILL.md | Scope preservation exists, but canonical current-record location and registry update are not concrete. | Update one current state block at phase changes; preserve full backlog/history by reference. Test recovery after compaction without restarting completed work. |
| handoff/SKILL.md + references/task-record.md | Generic template lacks explicit scheduler IDs, parent/worker distinction, saved cursors and transfer verification. | Add compact optional delegation block with ownership, monitor state, budget and acceptance; verify receiver first action and old monitor disposition. No automatic transfer claims. |
| handoff/references/ringstatus-webflow-prompt.md | Useful60min/page,15min reserve,two corrections and one5min access recovery already exist. Not shown consistently effective. | Reuse these limits, replace contradictory instructions, and keep visuals separate from functional work. Do not add another duplicate budget system. |
| planning/SKILL.md | End-to-end trace is present, but dispatch did not establish reachable final proof early. | Require one small path from native input to final destination/storage/log, plus dependency ordering and available validation surface before wide implementation. |
| coding-instructions/SKILL.md | Baseline/review rules exist; local check counts still dominated status. | Bind each check to target runtime and user outcome; prefer existing implementation/contracts. Independent review only under applicable authorization; preserve runner boundary. |
| code-review/SKILL.md | Read-only review exists, but review findings and release acceptance were conflated in delivery. | Check final user journey and deployed-version parity as separate requirements; retain unresolved gates, with no automatic permission to edit. |
| boundaries/SKILL.md | Correct principle of blocking only dependent actions exists. | Supply exact allowed/protected resource list at dispatch and a specific owner decision only when necessary; test one real partial-access scenario. |
| ringstatus-context / ringstatus-capabilities | Registered cloud skills not found at expected user-local paths. | Locate authoritative package and map runtime source; do not install guessed replacements or assert loaded. |
| .agents/skills/ringstatus-control/SKILL.md | Says project hooks are mechanical write/completion boundary, but PreToolUse is disabled and completion checks cannot prove semantic truth. Seven broad reference links also encourage excess loading. | Correct enforcement claim; route references by task. Keep actual evidence limits and runner rules. No hook activation. |
| .agents/skills/ringstatus-workflow/SKILL.md | Opt-in mode rules are legitimate but can become an unintended startup gate when imported indiscriminately. | Keep opt-in only and avoid making ordinary authorized work select a mode. Existing one-lead rule does not prohibit independent nonoverlapping coordinators. |

No SKILL.md file, AGENTS.md or hook was modified by this research task. skills2 registration annotations identify recommendations, not implementation.

## Third-party skills: load by task, not all at startup
OpenAI describes selective skill loading; broad/conflicting skills can waste context. [S,R] The following are an inspected task-fit recommendation, not OpenAI endorsement of a third-party package.
| Task | Required or conditional skill bundle |
| --- | --- |
| Coordinator research/registry | openai-docs; Airtable overview plus filters only when querying complex filters; applicable custom planning/handoff sources explicitly named. |
| Native Webflow styling | webflow-mcp:designer-tools and production webflow_guide_tool; computer-use and frontend-testing-debugging for actual rendered comparison; saved prototype source and token reference only as applicable. |
| Astro/data integration | custom coding-instructions; relevant Webflow Cloud state-detection guidance; systematic-debugging for an actual failure; verification-before-completion at completion; Airtable mapping guidance. |
| Cloudflare runtime/storage | workers-best-practices; wrangler for actual CLI/runtime work; durable-objects only when that architecture is actually selected. D1 is not a reason to load an unrelated Durable Objects recipe. |
| SMS | Existing Airtable-native Twilio action and its current supported configuration first. Twilio messaging/security/reliability skill only for the actual changed operation; no two-way Worker substitution. |
| CSS audit | frontend-testing-debugging plus computer-use, exact page/viewport/theme matrix and source tokens. Audit assignment does not authorize repairs. |
| Skill revisions | skill-creator for the authorized skill edit; then a bounded practical trial, not installing every available package. |

Inspected files include C:/Users/gombc/.agents/skills/webflow-mcp-designer-tools/SKILL.md, webflow-cli-cloud/SKILL.md, and installed frontend-testing-debugging, systematic-debugging and verification-before-completion package files.
Resolve conflicts explicitly: Designer skill requests site discovery but supplied repository rule says reuse the verified site ID; repository rule wins. Cloud skill initialization is inappropriate for the existing configured app. Verification skill asks fresh commands on every status; do not rerun unchanged expensive tests just to quote retained evidence, and never rerun a failed production path against the owner's restriction. Scope claims to prior evidence or run the affected authorized verification. Do not edit vendor package cache to hide these conflicts.
Candidate Cloudflare/Twilio skills are available in the skill catalog but were not loaded as execution instructions in this audit; their exact applicable source must be read by that future task.

## Subagents versus separate chats and handoff
Use an internal subagent for a bounded part of the same deliverable, especially read-only investigation/review. Parent collects results and retains acceptance. One writer owns each overlapping file/page; independent writers need isolated scopes/checkouts and one integration owner. More agents cost additional tokens. [M]

Use a separate project chat for a distinct outcome, longer-lived specialist or explicit new task request. It is not automatically a controllable child; record its ID, ownership, permissions and evidence. The current app exposes read/wait tools and human-authorized messaging, so user relay is not the required default. Do not spawn new chats merely to manufacture activity. [P; current app tool contracts]

For the future coordinator handoff: save current task map and deployed-source identifiers; verify receiver access; preserve exact authorized work and unsettled gates; reconcile scheduler target_thread_id, active rows, budget and failure links; verify receiver's first concrete action; leave only one owner/monitor for each assignment. A new chat or fork does not transfer timers, files, credentials or subagent handles automatically. Use reports from completed internal agents; create successors only when the next task is actually authorized. No new hierarchy or upper controller is required for the present completion work.

## Scheduled heartbeat practice
All four screenshot schedules are configured PAUSED and target this chat. Their recorded cadences are15min for usage and3min for each task monitor. This audit changes none. A configured timer is neither an execution receipt nor a hard deadline.

Use one task monitor only while that task has actionable unfinished work; observe snapshots using saved cursors, do not nudge running workers, record actual state in active-threads and stop on owner pause/completion/real dependency. Preserve approved deadlines and no automatic budget reset. A reminder only provides another model turn; it does not enforce resource ownership or correctness. [H]

Keep monitoring prompts short and refer to a small accessible state file/skill. Use a same-chat schedule when its context is needed. Use a standalone scheduled task for independent read-only checks such as usage if the owner chooses to change the current arrangement; that would need explicit creation/update, not an improvised replacement. Test one scheduled run and its stop behavior before relying on recurrence. Proposed cadence should match expected progress rather than blindly polling every3minutes. No claimed savings until measured. Avoid overlap among the Recognize, audit and continuation monitors. During handoff pause the old owner, retarget/recreate only the required schedule, verify exact state, then resume once.

## Ordered improvement plan
These are planned changes; this audit does not implement them or resume paused application work.
1. **Registry repair now (this task):** classify all90 complaints, add explicit remediation state, refresh known current assignments, register this coordinator/missing bounded assignments, link this report from rs-decisions. Verify every written field by readback; preserve original history.
2. **Canonical skill reconciliation:** settle local/cloud sources for the ten skills2 entries. Narrowly revise the identified conflicting/custom instructions and mirror version/path summaries. Do not copy an entire operating manual into every task.
3. **One compact project/task entry point:** update existing common-delivery-stages links rather than create another control system. Separate scope, current state, acceptance, source/deployment and history.
4. **Prepare the next exact completion assignment:** preserve Recognize provisional acceptance; enumerate only remaining agreed corrections/evidence. For Barn Inputs/Onboarding/Opt-ins reuse applied work and manifests. Keep Feed/Schedule/CSS audit open. Identify validation access and final destination before implementation; no new discovery of already settled architecture.
5. **Dispatch with minimum skills and one owner:** specify exact loaded skill paths/version, existing source, permitted delta, runtime acceptance, model/effort, budget, first action and genuine stop conditions. Separate visual and backend tasks but retain full page acceptance.
6. **Qualify coordination once:** on the next owner-approved bounded task, observe receiver start, one useful checkpoint, actual result and registry update. If a scheduled monitor is needed, verify one natural execution and pause behavior. Do not require a new two-coordinator/upper-agent build to finish the applications.
7. **Model/effort comparison:** use Sol at the available default as the OpenAI starting recommendation; our bounded implementation candidate remains Sol Medium. Astra Light/low is an official starting point, not automatically High. Reserve higher effort for demonstrated hard reasoning. Compare equal scope and acceptance using actual input/cached/output usage, elapsed time, correction count and accepted outcome. No automatic model changes or fabricated savings. [B]
8. **Close on evidence, not count:** actual native user journey, storage/log readback and retained failure/visual gates, plus protected regression. Owner provisional acceptance is recorded separately. Mark only the tested defect/assignment closed; repeated operating reliability remains unverified until exercised.

Success of this plan means fewer owner interventions and a complete agreed result within the same recorded budget. Finding documentation and adding fields do not establish that success.

## Source register
Official pages opened October9,2026. Markdown endpoints returned errors; successful HTML pages were used. These are methods, not guarantees or proof of RingStatus runtime.
- [P] https://learn.chatgpt.com/docs/projects
- [S] https://learn.chatgpt.com/docs/build-skills
- [A] https://learn.chatgpt.com/docs/agent-configuration/agents-md
- [M] https://learn.chatgpt.com/docs/agent-configuration/subagents
- [H] https://learn.chatgpt.com/docs/automations?surface=app
- [B] https://learn.chatgpt.com/guides/best-practices
- [R] https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra
- [E] https://developers.openai.com/api/docs/guides/evaluation-best-practices
- [K] https://learn.chatgpt.com/docs/hooks

Local sources: the eight existing user-local skills named above; handoff references/task-record.md and ringstatus-webflow-prompt.md; repository ringstatus-control and opt-in ringstatus-workflow; .codex/control/RELIABILITY-CONTROLS.md; current coordination task/report files; current Airtable schema and90 complaints/9 active rows/19 decisions/10 skills; current project/Git identity and six worker snapshots. Full before-data is stored in the private snapshot. No old snapshot is represented as a fresh application test.

## Complaint-by-complaint evidence map
Support labels mean: Method found = official method addresses the class of failure; Partial guidance = useful process guidance without an exact target fix; No direct solution found = no OpenAI-specific resolution established. All90 existing incidents remain Open pending their individual closure evidence. Proposed refers to a future correction; Implemented-unverified preserves recorded component repairs without asserting the entire complaint resolved.

<a id="rec0W3tGvkL0uGMbo"></a>
### C051 — rec0W3tGvkL0uGMbo
- Complaint: Directs user to a fresh-session skill before verifying discovery and access
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Select only applicable skills, verify exact installed source and actual read; reconcile local versus cloud skill variants.
- Skill/procedure: instruction-loader; request-router; ringstatus-capabilities.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver demonstrates access and uses the required workflow; a registry entry or available skill is insufficient.
- Sources: https://learn.chatgpt.com/docs/build-skills ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="rec1nyeqTi0vT1jKK"></a>
### C011 — rec1nyeqTi0vT1jKK
- Complaint: Architecture is designed before the business goal
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Establish business outcome and actual user journey before choosing architecture; inspect authoritative inputs and downstream destination.
- Skill/procedure: planning; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: A compact plan names the input, storage, final displayed result and evidence for each boundary.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/projects

<a id="rec2CLGZIpBkbDO5h"></a>
### C039 — rec2CLGZIpBkbDO5h
- Complaint: Time tokens and cost are wasted through drift
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep current state in one small task record and fetch only relevant evidence; avoid full-history replay and overlapping check loops.
- Skill/procedure: rebalance; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Fresh context identifies the next authorized action from the task record without reconstructing the entire chat.
- Sources: https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra ; https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/build-skills

<a id="rec2VE2SkgUyYIIzX"></a>
### C037 — rec2VE2SkgUyYIIzX
- Complaint: Technical decisions are shifted to the user
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

<a id="rec4POV3YMurKOKnt"></a>
### C043 — rec4POV3YMurKOKnt
- Complaint: One-time proof substitutes for repeatable cadence-owned behavior
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="rec5822ZHBywJTwY0"></a>
### 2026-10-07 Recognize — premature stopping at intermediate results — rec5822ZHBywJTwY0
- Complaint: CONFIRMED — The controller repeatedly treated diagnosis, deployment, a test result, or a status response as a stopping point while the authorized Recognize outcome remained unfinished. The owner had to restart execution with repeated prompts. Apologies and self-diagnosis substituted for corrective action. Ending a response ended active work; no background controller was continuing the task. This repeats the pattern recorded in C046 and C061. Impact: owner supervision, interrupted delivery, and reported wasted time and tokens; no measured total is claimed.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Define the entire done-when boundary and executable next action; continue authorized work, preserving real stop conditions.
- Skill/procedure: handoff; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Observed continuation completes the bounded outcome or reaches a specific dependency; instructions alone do not guarantee persistence.
- Sources: https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra ; https://learn.chatgpt.com/guides/best-practices

<a id="rec5GdTzC7zY2ouDS"></a>
### C002 — rec5GdTzC7zY2ouDS
- Complaint: Unsolicited final improvements
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="rec5m2H6fhoU5bHS2"></a>
### C061 — rec5m2H6fhoU5bHS2
- Complaint: CONFIRMED — rs-inputs-agent returned another PARTIAL status and a future test sequence instead of advancing the unfinished Recognize/User Inputs end-to-end objective. The owner had repeatedly required this thread to own execution and verification without repeated prompting, and reported returning to only a 41-second status response. That response reread schema/counts and a previous test log but performed no new implementation or end-to-end test. It concluded that the owner must resume implementation, adding another activation step. Earlier scoped work had been completed; the failure is stalled delivery of the remaining user journey, not that no work was ever done.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Define the entire done-when boundary and executable next action; continue authorized work, preserving real stop conditions.
- Skill/procedure: handoff; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Observed continuation completes the bounded outcome or reaches a specific dependency; instructions alone do not guarantee persistence.
- Sources: https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra ; https://learn.chatgpt.com/guides/best-practices

<a id="rec6awkmFIDPORYQa"></a>
### C052 — rec6awkmFIDPORYQa
- Complaint: Assumes a generated script was downloaded to the stated local path
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="rec7YOefo8nWFwd1N"></a>
### C016 — rec7YOefo8nWFwd1N
- Complaint: Code is repeatedly patched after premature delivery
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Reproduce the affected failure, trace its cause and make one scoped correction; preserve baseline and bounded correction budget.
- Skill/procedure: coding-instructions; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Previously failing behavior passes and protected behavior remains unchanged; obey the owner's stop-after-workflow-failure rule.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="rec8Y2gLqfxGcg9EJ"></a>
### C038 — rec8Y2gLqfxGcg9EJ
- Complaint: Conversation expands and repeatedly changes direction
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep current state in one small task record and fetch only relevant evidence; avoid full-history replay and overlapping check loops.
- Skill/procedure: rebalance; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Fresh context identifies the next authorized action from the task record without reconstructing the entire chat.
- Sources: https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra ; https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/build-skills

<a id="rec9wthLiiLrWFBw8"></a>
### 2026-10-07 Recognize — owner became the integration and QC operator — rec9wthLiiLrWFBw8
- Complaint: CONFIRMED — The owner repeatedly carried PowerShell results and screenshots between steps and had to prompt the controller to connect the evidence and proceed. Some live participation was necessary because agent browser access was restricted; repeated coordination and supervision were not justified by that restriction. Owner supplied invalid_invitation response at 2026-10-07T00:42:35Z and later profile, Barn setup, and recognized-after-reload screenshots. This verified a user-assisted journey, not autonomous browser QA. Related patterns: C037 and C056.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

<a id="recAOsJJHjpj9SSIL"></a>
### C014 — recAOsJJHjpj9SSIL
- Complaint: Current best practices are not researched
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Select only applicable skills, verify exact installed source and actual read; reconcile local versus cloud skill variants.
- Skill/procedure: instruction-loader; request-router; ringstatus-capabilities.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver demonstrates access and uses the required workflow; a registry entry or available skill is insufficient.
- Sources: https://learn.chatgpt.com/docs/build-skills ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="recAkDVrWZZv0ufDD"></a>
### 2026-10-06 — RS Section Scale Test: Source overlay typography replaced with invented sizing — recAkDVrWZZv0ufDD
- Complaint: FAIL — In the full-image overlay reproduction, I introduced draft heading/logo rules using clamp(2rem,6vw,6rem) instead of establishing fidelity to the original source typography. This substituted an unverified sizing choice in a task that required recreation using existing specifications. The draft overlay was not visually verified.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recBIlefd7cgndklX"></a>
### 2026-10-06 — RS Section Scale Test: Two days and over 15 hours without accepted delivery — recBIlefd7cgndklX
- Complaint: FAIL — The owner reports two days and over 15 total hours spent, with every requested task failing the required delivery standard. I acknowledged failure to deliver faithful reproduction and verified visual quality across viewports. Existing drafts and limited saved-state or desktop checks did not satisfy the complete requested outcome. The duration is owner-reported, not independently measured.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recCJk4xslZWil5Bp"></a>
### 2026-10-06 — RS Section Scale Test: Required repeated owner supervision to enforce visual requirements — recCJk4xslZWil5Bp
- Complaint: FAIL — The owner repeatedly had to restate VISUAL, identify mismatches, request correction, ask whether rebalance was loaded, and insist that viewport quality was the whole point. I did not consistently apply the already stated reproduction and visual-verification requirements without that supervision.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recCYMCEcyO6c48UO"></a>
### Recognize — incomplete recovery delivery and excessive SMS table design — recCYMCEcyO6c48UO
- Complaint: Owner requested a complete, tested Recognize flow with minimum required fields. At revisions180–185 the delivered preparation still lacked executable SMS recovery and automatic send history. Coordinator created rs_sms_requests with 11 explicit fields plus the reciprocal event link and rs_sms_events with 8 fields, without first mapping the actual sending automation; person_uid was plain text rather than a person link and there was no phone lookup. Owner had to add the person link/phone lookup and configure/test the SMS connection. This violated the requested minimum-field approach and shifted integration work back to the owner.

Observed evidence: initial schema/readback in revision180-sms-tables.json; revision183 live schema has owner-added person_uid link and phone lookup fld5u5Z5XnC92hW1m. Automation wfl6oiGMgUPmEkUuS was deployed with recordCreated trigger and one Twilio action sending literal body-test, with no request-status update or footprint action. SMS events table was empty. Isolated source rs-recognition-native.js line36 still returned sms_recovery_unconfigured, inside authenticated access, so the prepared source did not implement lost-session recovery. The coordinator had also proposed using the old purpose-specific two-way Worker before owner correction and investigation of existing Airtable-native SMS.

Reported 191 functional checks, 2 Workers checks and build success were local preparation evidence, not real page-to-SMS-to-redemption-to-log proof. Three known durability counterexamples remained failing acceptance; they are already recorded under rechz3ofexwVWukPt and are not new defects here. Owner reports twice believing Recognize was complete/tested and alleges a prior false PASS; those exact historical completion statements have not been independently retrieved in this incident, so intentional deception is not established by this record.

Responsibility: coordinator01a11632-62fd-73b1-997e-8be4119185de owns premature table delivery, excessive/unjustified fields, omitted person/phone relation, integration guidance and acceptance tracking. Worker01a11cc4-39cd-7eb2-872f-89ef41193275 owns the prepared Recognize implementation; its observed latest checkpoint explicitly said PARTIAL. Do not attribute coordinator-created tables to the worker.

Revision228–229 NEW FAILED LIVE GATE: owner confirms SMS arrived and link opened, then reports page frozen and not mobile; identifies /test/onboarding. Source inspection confirms SMS generator uses RS_INPUTS_ONBOARDING_URL and requires pathname ending /onboarding; configured route is old Astro/React page /test/onboarding. Its AccessGate consumes invitation and renders existing children, with no redirect to native /rs-recognize. Native Recognize client currently does not consume invitation fragments, so a URL-only replacement is insufficient. Desktop public-route inspection shows Preview device:iPhone and Astro access UI; phone freeze cause is not independently established. Actual SMS receipt and cleared invitation hash remain valid component evidence, not completed native recovery. Complaint remains OPEN. Required correction: reuse existing invitation API in a bounded native handoff, preserve presentation/auth callers, and verify mobile native restored access plus Continue-to-Barn. No code/style/deployment change or repeat SMS made in this review.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Implemented-unverified**.
- Method: Check the exact deployed source and runtime early; preserve native presentation and test persistence, races and recovery where they execute.
- Skill/procedure: coding-instructions; systematic-debugging; workers-best-practices.
- Modified/applied evidence: Existing SMS person link/lookup and native invitation correction deployed at 5684b4178; owner provisional recovery acceptance. Minimum-schema and full acceptance remain open.
- Closure: Actual deployed path and correlated storage/log evidence satisfy the retained failure cases, not just mocked assertions.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recCaXALRmfQDaVRf"></a>
### C053 — recCaXALRmfQDaVRf
- Complaint: Treats prompting guidance or related reports as a proven behavioral fix
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance. No official promise of deterministic compliance; evaluate behavior before closure.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recCguZM95UWVU4gW"></a>
### 2026-10-07 Recognize — scoped access blocker stopped independent work — recCguZM95UWVU4gW
- Complaint: CONFIRMED — A real browser access restriction was generalized into a broader stopping point. That restriction limited browser verification, but did not itself prevent Airtable inspection, preparation of a test invitation, code inspection, or automated checks. Independent work resumed only after further owner prompts. The failure was failure to scope the blocker and advance other work, not fabrication of the restriction. Related recurring pattern: C062; that record concerns a separate incident.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Verify exact target, execution surface and accessible evidence; scope a blocker to the dependent action.
- Skill/procedure: instruction-loader; boundaries.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Actual supported tool operation establishes access; independent authorized work proceeds without inventing access.
- Sources: https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/agent-configuration/subagents

<a id="recCyrCQV9g297x4q"></a>
### C046 — recCyrCQV9g297x4q
- Complaint: Acknowledges the request and stops although authorized work remains
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Define the entire done-when boundary and executable next action; continue authorized work, preserving real stop conditions.
- Skill/procedure: handoff; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Observed continuation completes the bounded outcome or reaches a specific dependency; instructions alone do not guarantee persistence.
- Sources: https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra ; https://learn.chatgpt.com/guides/best-practices

<a id="recE2UlzNJC0D43JO"></a>
### C055 — recE2UlzNJC0D43JO
- Complaint: Agent reacts to the latest message by changing process or architecture without preserving the established objective or evaluating downstream consequences end to end.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recERLTu49CYpmv4I"></a>
### C003 — recERLTu49CYpmv4I
- Complaint: Working code changed unnecessarily
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recEc9qZZ4Qq3AWZm"></a>
### C031 — recEc9qZZ4Qq3AWZm
- Complaint: Correction triggers more diagnosis instead of repair
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Reproduce the affected failure, trace its cause and make one scoped correction; preserve baseline and bounded correction budget.
- Skill/procedure: coding-instructions; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Previously failing behavior passes and protected behavior remains unchanged; obey the owner's stop-after-workflow-failure rule.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recF9gKylDrK2nGuO"></a>
### C025 — recF9gKylDrK2nGuO
- Complaint: User becomes the QA and debugger
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

<a id="recFC8bFpQCGsxCxB"></a>
### 2026-10-06 — RS Section Scale Test: Failed visual audit across viewports and intermediate widths — recFC8bFpQCGsxCxB
- Complaint: FAIL — The central task was to audit the entire test page visually so it displayed the correct layout and scaled appropriately at each viewport, including intermediate widths. I did not complete that audit. Saved styles, text and element readbacks did not verify wrapping, clipping, overflow, alignment, spacing, image cropping or typography scaling. Capture failures blocked some checks; they do not erase the known implementation failures.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recG00XrXm7vmnnjF"></a>
### Onboarding audit — isolated preview login blocks visual checks — recG00XrXm7vmnnjF
- Complaint: Audit worker cannot complete target viewport/keyboard/interaction checks in isolated preview because Webflow requires login. Coordinator inspected saved screenshot and confirmed login screen. Worker reports existing Designer session protected from takeover. Headless native reads succeeded for eight onboarding pages; these do not establish rendered consistency.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Verify exact target, execution surface and accessible evidence; scope a blocker to the dependent action.
- Skill/procedure: instruction-loader; boundaries.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Actual supported tool operation establishes access; independent authorized work proceeds without inventing access.
- Sources: https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/agent-configuration/subagents

<a id="recH2jZKYlmN6RHvC"></a>
### 2026-10-06 — rs-cloudflare-agent: unnecessary closing detail creates ambiguity — recH2jZKYlmN6RHvC
- Complaint: User asked: "confirm can see and make edits to all workers". The response listed seven Workers and appended: "Editing is supported by the connector, but write access remains UNVERIFIED until an authorized edit succeeds; no Workers were changed." Combining general connector capability with unverified account write access made the practical answer unclear and required the user to ask again. When the user asked why unnecessary information was added, the next response merely restated the access limitation rather than addressing the communication complaint. The user identifies this as adding more information than needed and a "make better on the way out" improvement already prohibited by the loaded instructions. Impact: avoidable clarification, friction and user supervision.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Answer the requested result directly, omit self-diagnosis and unsolicited improvements, and execute authorized next steps.
- Skill/procedure: request-router; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Short response matches requested shape without omitted work or an unnecessary owner restart.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="recHEIQki0AUG2kM8"></a>
### C013 — recHEIQki0AUG2kM8
- Complaint: Business documents are not reviewed
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Establish business outcome and actual user journey before choosing architecture; inspect authoritative inputs and downstream destination.
- Skill/procedure: planning; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: A compact plan names the input, storage, final displayed result and evidence for each boundary.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/projects

<a id="recINFZbBMDKdkRwZ"></a>
### C004 — recINFZbBMDKdkRwZ
- Complaint: Patch stacked on a failed patch
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Reproduce the affected failure, trace its cause and make one scoped correction; preserve baseline and bounded correction budget.
- Skill/procedure: coding-instructions; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Previously failing behavior passes and protected behavior remains unchanged; obey the owner's stop-after-workflow-failure rule.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recIdE7NSxoJfponq"></a>
### C040 — recIdE7NSxoJfponq
- Complaint: class_start_times remains unresolved downstream
- Support: **No direct solution found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use the approved recurring runner and observe its actual outputs; scheduled configuration or manual probes do not prove recurrence.
- Skill/procedure: planning; coding-instructions.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Required natural run history and business outcome exist; no alternate workflow or record repair substitutes. OpenAI documentation cannot establish or repair this specific engine branch/data defect; requires scoped source and runner evidence.
- Sources: https://learn.chatgpt.com/docs/automations?surface=app ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recNOlUMUFI4qZ8p8"></a>
### 2026-10-07 Recognize — incomplete acceptance tracking and working baseline — recNOlUMUFI4qZ8p8
- Complaint: CONFIRMED — The controller focused on the immediate error without maintaining a complete acceptance record for the working Recognize model and the owner's approved requirements. Return access, expiry behavior, recovery, concurrency, and automated verification remained unresolved or insufficiently reconciled. The owner reported Recognize had been working when handed over and objected to recreating established paths. Important distinction: invited/approved access had been owner-approved; this complaint does not label that policy unauthorized. Eight-hour session expiry is an implementation behavior requiring reconciliation with intended use.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Establish business outcome and actual user journey before choosing architecture; inspect authoritative inputs and downstream destination.
- Skill/procedure: planning; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: A compact plan names the input, storage, final displayed result and evidence for each boundary.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/projects

<a id="recNiiTVWAqeIJTvn"></a>
### C030 — recNiiTVWAqeIJTvn
- Complaint: Behavioral promises replace demonstrated change
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance. No official promise of deterministic compliance; evaluate behavior before closure.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recNjdK8d8N9yB9n8"></a>
### C008 — recNjdK8d8N9yB9n8
- Complaint: Advice uses partial context
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Establish business outcome and actual user journey before choosing architecture; inspect authoritative inputs and downstream destination.
- Skill/procedure: planning; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: A compact plan names the input, storage, final displayed result and evidence for each boundary.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/projects

<a id="recOf0tbsnDHfngb0"></a>
### Recognize — binding writes failed with existing special settings — recOf0tbsnDHfngb0
- Complaint: Worker reports two native binding writes failed when payload included existing special settings; fresh readback showed they had not applied. Coordinator observed this report, not the original failed tool responses.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Implemented-unverified**.
- Method: Read the actual connector schema and current vendor guide; verify each response and paginate metadata before declaring absence.
- Skill/procedure: webflow-mcp:designer-tools; instruction-loader.
- Modified/applied evidence: Later new-only binding writes/readback recorded; ongoing prevention unverified.
- Closure: Exact supported call succeeds and readback matches; OpenAI guidance does not fix vendor transport or schema defects.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recP1AsbdN6GQxxsr"></a>
### C041 — recP1AsbdN6GQxxsr
- Complaint: Cadence exits before the required branch
- Support: **No direct solution found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use the approved recurring runner and observe its actual outputs; scheduled configuration or manual probes do not prove recurrence.
- Skill/procedure: planning; coding-instructions.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Required natural run history and business outcome exist; no alternate workflow or record repair substitutes. OpenAI documentation cannot establish or repair this specific engine branch/data defect; requires scoped source and runner evidence.
- Sources: https://learn.chatgpt.com/docs/automations?surface=app ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recPB0GXJBGWAw5tY"></a>
### 2026-10-06 — RS Section Scale Test: Did not research current responsive styling practices — recPB0GXJBGWAw5tY
- Complaint: FAIL — The owner required current research into best practices for styling and scaling to each viewport. I acknowledged that the requested current research was not completed before implementation and audit decisions.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recPjrNowVUigGoyZ"></a>
### C019 — recPjrNowVUigGoyZ
- Complaint: Partial check is labeled PASS
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recQ3VG0pHSEJOc9S"></a>
### C044 — recQ3VG0pHSEJOc9S
- Complaint: Gathering and summarizing state replaces workflow stabilization
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use the approved recurring runner and observe its actual outputs; scheduled configuration or manual probes do not prove recurrence.
- Skill/procedure: planning; coding-instructions.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Required natural run history and business outcome exist; no alternate workflow or record repair substitutes.
- Sources: https://learn.chatgpt.com/docs/automations?surface=app ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recQvSc0vJcG3gaNp"></a>
### C049 — recQvSc0vJcG3gaNp
- Complaint: Installs personal skills without presenting final contents for review
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Select only applicable skills, verify exact installed source and actual read; reconcile local versus cloud skill variants.
- Skill/procedure: instruction-loader; request-router; ringstatus-capabilities.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver demonstrates access and uses the required workflow; a registry entry or available skill is insufficient. Inspect and present skill content within existing authority; this audit does not install or rewrite personal skills.
- Sources: https://learn.chatgpt.com/docs/build-skills ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="recRW0nqJFLyQ04mt"></a>
### 2026-10-07 Recognize — mocked tests missed native runtime incompatibility — recRW0nqJFLyQ04mt
- Complaint: CONFIRMED — Storage adapters used fetch redirect mode 'error', which failed in the deployed Cloudflare runtime. Earlier mocked tests expected that unsupported setting, so they could not establish native compatibility. Owner observed HTTP 503 storage_unavailable, including x-rs-storage-diagnostic: transport_error;reason=TypeError and request ID 6fdb3f75-0bc3-41a9-8033-62781bd7b033. Passing local/mock checks or a deployment did not justify complete workflow readiness. Related patterns: C007, C017, and C020.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Implemented-unverified**.
- Method: Check the exact deployed source and runtime early; preserve native presentation and test persistence, races and recovery where they execute.
- Skill/procedure: coding-instructions; systematic-debugging; workers-best-practices.
- Modified/applied evidence: Historical native runtime correction recorded in incident; no renewed complete-path proof in this audit.
- Closure: Actual deployed path and correlated storage/log evidence satisfy the retained failure cases, not just mocked assertions.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recS177wNbApfZZs3"></a>
### Recognize — deployed-source compatibility failures — recS177wNbApfZZs3
- Complaint: After the saved recognition delta was applied to an isolated checkout of deployed source8df5bfbb422fd7c3873c5d09cf447456bf8917b8, local compatibility checks failed. Baseline161/161 passed before patch. Coordinator read revision136-compat-diagnostic.txt:37/42 pass,5fail: profile update, phone login, audit-write failure rejection, retirement audit-retry rejection, split-base binding assertion. This is local synthetic test evidence, not a failed live workflow.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Implemented-unverified**.
- Method: Check the exact deployed source and runtime early; preserve native presentation and test persistence, races and recovery where they execute.
- Skill/procedure: coding-instructions; systematic-debugging; workers-best-practices.
- Modified/applied evidence: Deployed-source compatibility changes and later release recorded; whole workflow still not fully accepted.
- Closure: Actual deployed path and correlated storage/log evidence satisfy the retained failure cases, not just mocked assertions.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recSNJxGTkSmCIeEO"></a>
### C048 — recSNJxGTkSmCIeEO
- Complaint: Uses a rejected temporary worktree as the persistent instruction target
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Verify exact target, execution surface and accessible evidence; scope a blocker to the dependent action.
- Skill/procedure: instruction-loader; boundaries.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Actual supported tool operation establishes access; independent authorized work proceeds without inventing access.
- Sources: https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/agent-configuration/subagents

<a id="recSew3Xp8STaNYVx"></a>
### 2026-10-06 — RS Section Scale Test: Copied tabs and media without runtime verification — recSew3Xp8STaNYVx
- Complaint: FAIL — The requested section with tabs initially produced incorrect native tab scaffolding. It was replaced with source-based embed markup, but click behavior was not verified. Later portfolio poster/video assignments were confirmed only as saved attributes; playback, replay, reduced-motion and visibility behavior were not verified in the target. I failed to establish the requested recreated sections behaved correctly.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recTFVO1rlQAOiWJt"></a>
### C033 — recTFVO1rlQAOiWJt
- Complaint: Unstated requirements are not discovered
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Establish business outcome and actual user journey before choosing architecture; inspect authoritative inputs and downstream destination.
- Skill/procedure: planning; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: A compact plan names the input, storage, final displayed result and evidence for each boundary.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/projects

<a id="recTGdzqwkpgfOm09"></a>
### C050 — recTGdzqwkpgfOm09
- Complaint: Claims inspection is underway without inspected target evidence
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recTw20FrA4HjBkSS"></a>
### 2026-10-06 — RS Section Scale Test: stopped with unresolved visual defects — recTw20FrA4HjBkSS
- Complaint: User requested recreation of the top navigation and bottom footer while restoring browser access. I inserted the sections and checked saved styles, but the footer snapshots visibly showed incorrect social icons, legal-link rendering, and a stacked waiting-list form. After the user emphasized VISUAL, I corrected only the form layout and ended again with a PARTIAL status and access blocker instead of completing the authorized visual work. Source-to-draft visual parity and quality across every configured breakpoint and intermediate widths were not established. I also claimed responsive preservation and recreation more broadly than the rendered evidence supported. The failure is leaving the requested visual outcome unfinished and shifting supervision back to the user.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recU7bfnCl0ycfAms"></a>
### C022 — recU7bfnCl0ycfAms
- Complaint: Narrow task becomes broad cleanup
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recUtW878l1HVKuYm"></a>
### C021 — recUtW878l1HVKuYm
- Complaint: Failed approach is reintroduced
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recV7D4hrMoNHv8fI"></a>
### C034 — recV7D4hrMoNHv8fI
- Complaint: Evidence or unsupported requirements are removed
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recVKbozL7EpXrXU4"></a>
### C009 — recVKbozL7EpXrXU4
- Complaint: Latest message overrides established context
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recXIGTypKgS6xygD"></a>
### C042 — recXIGTypKgS6xygD
- Complaint: No retry occurs after workflow failure
- Support: **No direct solution found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use the approved recurring runner and observe its actual outputs; scheduled configuration or manual probes do not prove recurrence.
- Skill/procedure: planning; coding-instructions.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Required natural run history and business outcome exist; no alternate workflow or record repair substitutes. Complaint requests retry, but current explicit runner policy requires stop after failed workflow and separate edit approval; do not auto-retry.
- Sources: https://learn.chatgpt.com/docs/automations?surface=app ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recXljs0wnWt84W0q"></a>
### C032 — recXljs0wnWt84W0q
- Complaint: Explanation buries the requested result
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Answer the requested result directly, omit self-diagnosis and unsolicited improvements, and execute authorized next steps.
- Skill/procedure: request-router; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Short response matches requested shape without omitted work or an unnecessary owner restart.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="recXobaxZzXckhIM9"></a>
### C015 — recXobaxZzXckhIM9
- Complaint: Code is delivered before workflow testing
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recY0ai6E9r077HG3"></a>
### C059 — recY0ai6E9r077HG3
- Complaint: Agent still leaves the user responsible for manually activating or carrying work into the destination thread after the user explicitly rejected being the messenger.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

<a id="recbRCBDgNoJdvjYg"></a>
### C057 — recbRCBDgNoJdvjYg
- Complaint: Agent overweights the user's latest reply and forms the next recommendation from that local context instead of rebalancing against the full retained objective, prior decisions, constraints, and downstream consequences.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recdqvOuOxpk7vky1"></a>
### RS Section Scale Test — incorrectly blocked the whole viewport audit on Edge access — recdqvOuOxpk7vky1
- Complaint: 2026-10-06, rs-section-scale-test agent. Task: faithfully recreate existing sections from their available source specifications and audit pageId=6ac4feea90381f902652073a across viewports; no original design was requested. I treated unavailable Edge control and slow cloud-browser rendering as a reason to stop the entire audit. I repeatedly reported the browser blocker instead of continuing source/target CSS and breakpoint comparisons and pursuing available DOM/layout measurement routes. I failed to deliver per-viewport PASS/FAIL evidence and made the user identify the nonvisual audit methods. Some saved-style and breakpoint reads had been completed, but they were not developed into the required comprehensive audit. On October 6 I acknowledged that missing Edge access blocked one verification route, not all audit work.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Verify exact target, execution surface and accessible evidence; scope a blocker to the dependent action.
- Skill/procedure: instruction-loader; boundaries.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Actual supported tool operation establishes access; independent authorized work proceeds without inventing access.
- Sources: https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/agent-configuration/subagents

<a id="receEALRAU6GAdylS"></a>
### C028 — receEALRAU6GAdylS
- Complaint: Agent answers before checking
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="receQSwUUrQ6Y1SqU"></a>
### Recognize — incomplete variable pagination caused false missing-config claim — receQSwUUrQ6Y1SqU
- Complaint: Worker interim commentary/checkpoint treated first variable-metadata page as complete, incorrectly indicating configuration absent. Worker subsequently corrected this after reading second page. No evidence of environment writes.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Implemented-unverified**.
- Method: Read the actual connector schema and current vendor guide; verify each response and paginate metadata before declaring absence.
- Skill/procedure: webflow-mcp:designer-tools; instruction-loader.
- Modified/applied evidence: Second-page metadata read corrected the specific missing-configuration claim; prevention across future work unverified.
- Closure: Exact supported call succeeds and readback matches; OpenAI guidance does not fix vendor transport or schema defects.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recfjznf3Rh6GVknq"></a>
### C029 — recfjznf3Rh6GVknq
- Complaint: Response contains long self-diagnosis
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Answer the requested result directly, omit self-diagnosis and unsolicited improvements, and execute authorized next steps.
- Skill/procedure: request-router; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Short response matches requested shape without omitted work or an unnecessary owner restart.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="rechuElgtqVgVyVSa"></a>
### C045 — rechuElgtqVgVyVSa
- Complaint: Blockers are worked around instead of returning a conflict and stopping
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use the approved recurring runner and observe its actual outputs; scheduled configuration or manual probes do not prove recurrence.
- Skill/procedure: planning; coding-instructions.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Required natural run history and business outcome exist; no alternate workflow or record repair substitutes.
- Sources: https://learn.chatgpt.com/docs/automations?surface=app ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="rechz3ofexwVWukPt"></a>
### Recognize wiring — five defects reported by independent review — rechz3ofexwVWukPt
- Complaint: Worker reports independent review found: blank edit PINs still reset; action routes accepted ambiguous devices; retry cache could fill permanently; conflicting aliases could be detected after a profile write; unmatched login dropped its audit retry. Recorded as worker-reported review findings, not independently reproduced by coordinator.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Implemented-unverified**.
- Method: Check the exact deployed source and runtime early; preserve native presentation and test persistence, races and recovery where they execute.
- Skill/procedure: coding-instructions; systematic-debugging; workers-best-practices.
- Modified/applied evidence: D1 atomic-claim repair and local runtime tests recorded at revision205; full deployed qualification remains open.
- Closure: Actual deployed path and correlated storage/log evidence satisfy the retained failure cases, not just mocked assertions.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="reciCjmX9QJ7cOSn5"></a>
### 2026-10-06 — RS Section Scale Test: Did not load every requested available skill — reciCjmX9QJ7cOSn5
- Complaint: FAIL — The owner instructed me to load every available skill. I acknowledged loading instruction-loader, rebalance and frontend-testing-debugging, but did not load every available skill or fulfill the complete instruction. Merely naming loaded skills did not establish their effective application.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Select only applicable skills, verify exact installed source and actual read; reconcile local versus cloud skill variants.
- Skill/procedure: instruction-loader; request-router; ringstatus-capabilities.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver demonstrates access and uses the required workflow; a registry entry or available skill is insufficient. Historical explicit load-all instruction remains an unmet historical requirement; proposed default is narrow loading, not retroactive waiver.
- Sources: https://learn.chatgpt.com/docs/build-skills ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="recj83Pj82vXreoT0"></a>
### C018 — recj83Pj82vXreoT0
- Complaint: Manual probe is called cadence proof
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use the approved recurring runner and observe its actual outputs; scheduled configuration or manual probes do not prove recurrence.
- Skill/procedure: planning; coding-instructions.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Required natural run history and business outcome exist; no alternate workflow or record repair substitutes.
- Sources: https://learn.chatgpt.com/docs/automations?surface=app ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recjI8DAiNY57EQ9i"></a>
### 2026-10-06 — RS Section Scale Test: Top navigation, logo and footer fidelity — recjI8DAiNY57EQ9i
- Complaint: FAIL — The owner required visual recreation of the existing top navigation and bottom footer, then specifically reported an incorrect logo and incorrect rs-test-chrome-aaa-decor-1 styling. Draft adjustments did not establish complete visual parity; social-icon and legal/separator defects remained unresolved in the recorded work.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="reckAbo65bjkXBDnr"></a>
### Coordinator failure — Recognize coding readiness and phase handoff — reckAbo65bjkXBDnr
- Complaint: OPEN. Accountability: coordinator 01a11632-62fd-73b1-997e-8be4119185de, not shifted to the owner or solely to the worker.

Owner correction, revision198: Recognize began as styling replication; coding required a properly prepared separate assignment with the relevant skills and proven implementation references. Owner reports that this transition was not handled and that the agent was left to invent solutions.

Verified coordinator failures: (1) did not verify task-specific debugging and completion-verification skills were loaded before backend implementation; executed skill-read history inspected at revision197 showed Handoff, Webflow Designer/custom-code/Cloud, Git worktrees and Cloudflare reads, but no Systematic Debugging or Verification Before Completion read. (2) did not establish and verify an adequate existing-solution/reference assessment for the restart, duplicate-event and invitation-redemption requirements before progressing implementation and reporting readiness. This does not mean no research occurred: the worker did inspect existing code and documentation. (3) allowed implementation progress/local green counts to obscure three known acceptance failures and missing actual end-to-end recovery proof; this left the owner believing completion was closer than evidence supported. (4) added the missing debugging/verification guidance only after the owner's challenge. A corrective prompt alone does not resolve these failures.

Scope-history accuracy: the inspected worker01a11cc4-39cd-7eb2-872f-89ef41193275 was created on October8 specifically as a dedicated Recognize wiring task, with a coding/data-binding assignment. The available creation record therefore does not support stating that this particular worker was originally styling-only or that no separate wiring worker was created. The original visual task and this later coding assignment must remain distinct. The confirmed failure is the coordinator's inadequate preparation, skill verification, solution selection and truthful readiness tracking at that transition; recording the owner complaint does not establish an unsupported technical history.

Impact: repeated owner intervention, rework and extended execution while full-system acceptance remained unmet. Exact incremental cost has not been measured. Related implementation defects remain in rechz3ofexwVWukPt; excessive SMS schema and missing recovery delivery remain in recCYMCEcyO6c48UO; these incidents are not duplicated or closed here.

Revision227–229 model/cost review: session metadata confirms coordinator predominantly gpt-6-astra/high (308 saved contexts; also4 Astra/medium and2 Sol6.1/low), while the inspected dedicated Recognize wiring worker used gpt-6.1-sol/low (6 contexts). These are context counts, not billable calls. OpenAI Codex credit pricing lists Astra at5x uncached-input/output and10x cached-input per-token credits versus Sol6.1; higher effort generally increases reasoning-token use. Model and effort plausibly contributed to coordinator usage, but no task-level cost attribution or controlled comparison proves that High caused the defects or overengineering. Owner-reported35% daily usage is not independently attributed. Recommendation: Sol6.1 Medium for bounded integration/UI work; Astra High only for a bounded difficult review; Low for proven read-only checks. No settings changed; no benchmark claimed. Scope, readiness, schema and evidence failures remain coordinator responsibilities regardless of model. Full consolidated report: docs/coordination/recognize-failures-model-effort-review-20261008.md.

Revision231 — APPROVAL PREMISE AND LATE DISCOVERY FAILURE. Owner states they approved this implementation path because they were led to believe the prototype was already working and tested, and would not otherwise have approved it. This is retained as the owner's explicit account of the approval premise. The coordinator did not establish evidence for the complete intended native Webflow -> Astro -> designated Airtable -> SMS -> native restored-access/mobile path before relying on prototype success. Local tests, an older Astro prototype and successful SMS delivery were insufficient evidence for that different path. The wrong /test/onboarding landing destination was discovered after live SMS testing and owner intervention (revisions228–229). This is a coordinator readiness/verification failure, not excused by model choice. Impact: avoidable late discovery, repeated troubleshooting and owner time; exact avoidable hours/cost are unmeasured. This correction does not establish intentional deception or that every earlier component test failed. Complaint remains OPEN; no additional code/deployment authorization is inferred.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Select only applicable skills, verify exact installed source and actual read; reconcile local versus cloud skill variants.
- Skill/procedure: instruction-loader; request-router; ringstatus-capabilities.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver demonstrates access and uses the required workflow; a registry entry or available skill is insufficient.
- Sources: https://learn.chatgpt.com/docs/build-skills ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="reclBIIyiWBJYOiKq"></a>
### 2026-10-06 — RS Section Scale Test: Split image block delivered with incorrect geometry — reclBIIyiWBJYOiKq
- Complaint: FAIL — The first rs-split-img-block recreation rendered in a narrow left-side area because the copied grid structure lost its full-width behavior. The owner rejected the result as not close. A later desktop snapshot showed four full rows after repair, but that did not establish fidelity across viewports.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="reclhVMPk1tHJPIf7"></a>
### C005 — reclhVMPk1tHJPIf7
- Complaint: Correction becomes diagnosis and edit
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recmVdAxNJ6StcvIH"></a>
### C056 — recmVdAxNJ6StcvIH
- Complaint: Agent makes the user a transport layer between chats by requiring them to carry context, status, decisions, or outputs from one thread to another.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

<a id="recmb1M3AZKdJFiFx"></a>
### C058 — recmb1M3AZKdJFiFx
- Complaint: Ambiguous and overly long explanations obscure the requested answer.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Answer the requested result directly, omit self-diagnosis and unsolicited improvements, and execute authorized next steps.
- Skill/procedure: request-router; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Short response matches requested shape without omitted work or an unnecessary owner restart.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="recn7C8IIc9I7RYaF"></a>
### C035 — recn7C8IIc9I7RYaF
- Complaint: Wrong Codex mode or execution surface is selected
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Select only applicable skills, verify exact installed source and actual read; reconcile local versus cloud skill variants.
- Skill/procedure: instruction-loader; request-router; ringstatus-capabilities.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver demonstrates access and uses the required workflow; a registry entry or available skill is insufficient.
- Sources: https://learn.chatgpt.com/docs/build-skills ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="recoey3JZR10M9Gja"></a>
### C036 — recoey3JZR10M9Gja
- Complaint: Machine or environment access is assumed
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Verify exact target, execution surface and accessible evidence; scope a blocker to the dependent action.
- Skill/procedure: instruction-loader; boundaries.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Actual supported tool operation establishes access; independent authorized work proceeds without inventing access.
- Sources: https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/agent-configuration/subagents

<a id="recp2LY9DziwUBh6B"></a>
### C054 — recp2LY9DziwUBh6B
- Complaint: Request Router creates a two-chat relay that makes the user manually shuttle context, status, and decisions between the router and execution thread instead of transferring ownership once.
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

<a id="recpDZnuUtrSjhvYE"></a>
### 2026-10-06 — RS Section Scale Test: Failed reproduction despite available specifications — recpDZnuUtrSjhvYE
- Complaint: FAIL — I failed to faithfully recreate existing styles despite access to the original Webflow or ChatGPT Sites setup. The task was reproduction using existing specifications, not creation of a new design. The owner repeatedly reported that results did not match the originals.

Target: unpublished RS Section Scale Test, Webflow page 6ac4feea90381f902652073a. The owner requested recreation of existing work using available source pages, styles, specifications and setup. No original design or new creative work was requested.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recpe5t98m4BEgwCr"></a>
### C010 — recpe5t98m4BEgwCr
- Complaint: Established target is reinterpreted
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recq8NUxYxpj48lWX"></a>
### RS Section Scale Test — failure to deliver viewport quality — recq8NUxYxpj48lWX
- Complaint: FAIL. The core requirement was consistent section design and typography scaling across all Webflow viewports and intermediate widths, preserving existing fonts, cadence and intent with isolated draft classes. I created specimens and checked saved code/structure without establishing rendered viewport quality. I substituted layouts instead of inspecting the original first, missed inherited kicker properties, and repeatedly ended turns with unresolved work. When the owner identified the missing viewport quality, I only acknowledged it instead of delivering the required correction. Saved-style checks did not prove typography, wrapping, spacing, alignment or overflow. Test 04 matching and all-viewport acceptance remain unresolved. No fresh live-page verification was performed for the latest status update.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use exact source version and native target; separate visual reproduction from backend behavior, then compare matched viewports and states.
- Skill/procedure: handoff/references/ringstatus-webflow-prompt.md; frontend-testing-debugging.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Rendered source/target evidence covers typography, spacing, overflow and controls at required widths/themes; saved CSS is not proof.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recrI3FepxDifo5G1"></a>
### 2026-10-07 Recognize — live automation delivered without operational prerequisites — recrI3FepxDifo5G1
- Complaint: CONFIRMED — Automation was delivered before the live runner's prerequisites and execution were demonstrated. Local handler/native-runtime regression checks and a successful GitHub workflow run were real, but the live API runner was not executed against a verified dedicated CI identity and credential configuration. Browser automation was also not implemented. Remote secret or fixture absence was not established; their readiness was UNVERIFIED. This left the owner responsible for live QC despite the request for automatic testing. Related pattern: C043.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Use the approved recurring runner and observe its actual outputs; scheduled configuration or manual probes do not prove recurrence.
- Skill/procedure: planning; coding-instructions.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Required natural run history and business outcome exist; no alternate workflow or record repair substitutes.
- Sources: https://learn.chatgpt.com/docs/automations?surface=app ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recrakbTNbtbcRhuD"></a>
### C012 — recrakbTNbtbcRhuD
- Complaint: Architecture follows the repository instead of the business
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Establish business outcome and actual user journey before choosing architecture; inspect authoritative inputs and downstream destination.
- Skill/procedure: planning; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: A compact plan names the input, storage, final displayed result and evidence for each boundary.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/projects

<a id="recrkTIBCnN1KGQJs"></a>
### C020 — recrkTIBCnN1KGQJs
- Complaint: Fixed or complete is claimed without proof
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recsXpAfalA8i6cVj"></a>
### C006 — recsXpAfalA8i6cVj
- Complaint: Assumptions replace inspected evidence
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recsZNwdoaQULbBBE"></a>
### C060 — recsZNwdoaQULbBBE
- Complaint: CONFIRMED — ringstatus-webflow-md-css-js acknowledged explicitly authorized inventory and individual component-draft work, then ended the turn without executing it. The owner had supplied the three target pages, class/typography reuse philosophy and Home hidden-div exclusion. No target-page inventory or component drafts were produced after that instruction; the owner had to return and restart the agent. This repeated C046 and C030 and imposed supervision despite the stated no-babysitting requirement.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Define the entire done-when boundary and executable next action; continue authorized work, preserving real stop conditions.
- Skill/procedure: handoff; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Observed continuation completes the bounded outcome or reaches a specific dependency; instructions alone do not guarantee persistence.
- Sources: https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra ; https://learn.chatgpt.com/guides/best-practices

<a id="recseY6tnu9OdQMqr"></a>
### C047 — recseY6tnu9OdQMqr
- Complaint: Creates instruction skills before checking every supplied complaint
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Select only applicable skills, verify exact installed source and actual read; reconcile local versus cloud skill variants.
- Skill/procedure: instruction-loader; request-router; ringstatus-capabilities.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver demonstrates access and uses the required workflow; a registry entry or available skill is insufficient.
- Sources: https://learn.chatgpt.com/docs/build-skills ; https://learn.chatgpt.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra

<a id="rectDZnuXUz3iwSzL"></a>
### C001 — rectDZnuXUz3iwSzL
- Complaint: Work beyond requested scope
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="rectoBGI0yPev55JI"></a>
### C023 — rectoBGI0yPev55JI
- Complaint: Unapproved changes affect working areas
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recuZOmvd12sI4bP2"></a>
### C007 — recuZOmvd12sI4bP2
- Complaint: Confidence exceeds available evidence
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recucFvnFwYXH8wXe"></a>
### C024 — recucFvnFwYXH8wXe
- Complaint: User repeatedly corrects scope
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Keep the authorized delta and retained outcome explicit; reject unrelated changes and preserve settled decisions.
- Skill/procedure: boundaries; rebalance.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Compare final changed resources and deliverable against the authorized scope; no unrequested additions.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://learn.chatgpt.com/docs/agent-configuration/agents-md

<a id="recuuYUxMK8kDx3WB"></a>
### C017 — recuuYUxMK8kDx3WB
- Complaint: Local success is called end-to-end success
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Separate local checks, deployed runtime, actual user journey, rendered verification and recurring execution; retain failures.
- Skill/procedure: code-review; coding-instructions; handoff.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Each claimed outcome links the correct proof type and target/version; no test total substitutes for full acceptance.
- Sources: https://developers.openai.com/api/docs/guides/evaluation-best-practices ; https://learn.chatgpt.com/guides/best-practices

<a id="recwbZGnrKJuLkHT6"></a>
### C026 — recwbZGnrKJuLkHT6
- Complaint: User must babysit multiple agents
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

<a id="recyb58Ku08tGI6FT"></a>
### C062 — recyb58Ku08tGI6FT
- Complaint: CONFIRMED — after explicit authorization to execute the end-to-end Webflow plan, the agent stopped with incomplete responsive verification, Webflow recreation and Astro verification. The missing control-browser skill was an actual limit for managed Sites browser QA, but the agent treated that scoped limit as a stopping point for the broader authorized work without establishing that all independent work was exhausted. It had not begun Webflow recreation or Astro contract discovery. Individual component draft entry points were saved, not separately recreated Webflow components; build/worker checks did not verify UX fidelity. The subsequent statement that Stages 1–2 were complete was too broad: element trees/stored styles were inventoried, but the requested typography/spacing/viewport inventory remained unvalidated. Owner again had to ask where work stopped and request failure documentation. Related repeat: C060 and C046.

Explicit stopping reason and instruction failure: the agent incorrectly generalized the unavailable managed Sites browser skill into a blocker for the entire plan and treated saved drafts/build checks as sufficient to end the turn. Instruction Loader, Planning and Rebalance had been loaded, including globals and complaint safeguards requiring continuation of executable authorized work. They did not require the broader stop. The agent failed to apply them, leaving independent Webflow/Astro discovery unperformed. It then answered that this explanation was only partially documented instead of completing the already-authorized complaint record update, again requiring owner supervision.

2026-10-06 correction: the agent initially built one component-gallery page despite the owner requiring one page for each of five tabs. A gallery and reusable component definitions do not satisfy five separate pages. Reason: the agent incorrectly treated individual component sections as the requested page deliverable instead of checking the explicit acceptance criterion.

MAJOR FAIL — October 5, 2026, 23:08–23:10 America/New_York. Owner inspected the Webflow recreation and reported it was nowhere near finished. The agent had access to the identified SITE source, CSS, fixtures and interaction details but did not complete a faithful translation. It imported incomplete native styling/control structures and relied on page-level CSS/JS to reconstruct content, leaving native Webflow class and control parity unfinished. It initially substituted a gallery for the explicitly required five separate pages. Creating the five pages corrected page count only; it did not complete the requested SITE recreation. Exact saved-code readback and JSDOM initialization/state checks establish those limited checks, not visual or interaction fidelity in Webflow. The agent reported those milestones as if sufficient progress toward acceptance without demonstrating the requested match.

Execution and access failure: the agent repeatedly stopped at verification/access limits rather than completing independently executable native translation work. Designer connection was successfully restored and Recognize opened. Visual testing then used cloud Chrome rather than the owner's requested Edge. Browser inventory exposed only cloud Chrome; supplied screenshots confirmed Edge extension installed/enabled but did not make Edge available to this execution session. The cause of that availability mismatch remains UNVERIFIED. Browser authentication was declined, so no sign-in was completed. These limits explain unavailable visual verification, not the incomplete source-to-Webflow implementation. The agent failed to apply loaded continuation/acceptance instructions and required repeated owner supervision. SITE recreation status: FAIL / unfinished. No fix or acceptance is claimed.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Verify exact target, execution surface and accessible evidence; scope a blocker to the dependent action.
- Skill/procedure: instruction-loader; boundaries.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Actual supported tool operation establishes access; independent authorized work proceeds without inventing access.
- Sources: https://learn.chatgpt.com/docs/projects ; https://learn.chatgpt.com/docs/agent-configuration/subagents

<a id="recydcuqDQ6U1zXkp"></a>
### Recognize wiring — unsupported Webflow action calls — recydcuqDQ6U1zXkp
- Complaint: Observed 2026-10-08: worker01a11cc4-39cd-7eb2-872f-89ef41193275 turn01a11cc4-3fec-7213-a607-ac81704d3502 issued failed Webflow calls get_page_custom_code, get_page_script, list_applied_scripts and get_all_styles. Runner reported schema mismatches and left loader/style ownership unverified. This is a tool-action/schema failure; it is not evidence of an authentication or Bridge failure. Local implementation continued. Full task is not marked failed or complete from these calls alone.
- Support: **Partial guidance**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Read the actual connector schema and current vendor guide; verify each response and paginate metadata before declaring absence.
- Skill/procedure: webflow-mcp:designer-tools; instruction-loader.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Exact supported call succeeds and readback matches; OpenAI guidance does not fix vendor transport or schema defects.
- Sources: https://learn.chatgpt.com/guides/best-practices ; https://developers.openai.com/api/docs/guides/evaluation-best-practices

<a id="recyoH5QcZX07Do21"></a>
### C027 — recyoH5QcZX07Do21
- Complaint: Multiple agents multiply unreliable reasoning
- Support: **Method found**. Complaint status: **Open**. Remediation: **Proposed**.
- Method: Assign one owner per outcome and resource; use explicit authorized communication, compact result collection and one monitor per assignment.
- Skill/procedure: handoff; request-router.
- Modified/applied evidence: No new skill/application correction applied by this audit; recommendation only.
- Closure: Receiver starts actual scoped work, returns evidence, and parent records acceptance without owner message relay.
- Sources: https://learn.chatgpt.com/docs/agent-configuration/subagents ; https://learn.chatgpt.com/docs/automations?surface=app

## Audit execution and verified registry readback

The audit reviewed all 90 existing complaints and created two distinct new incidents. Airtable readback verified all requested changes to the original 90 complaint records, nine existing task records, ten skills2 responsibility annotations and the report/plan decision. Seven missing assignments and this coordinator were registered, bringing active-threads to 17 records; this does not mean 17 agents are running. Unchecked Airtable checkboxes are omitted in responses and were interpreted as false.

Across all 92 complaint records: 48 Method found, 41 Partial guidance, 3 No direct solution found. Documentation discovery is not incident resolution. All complaint statuses remain Open; remediation separately identifies Proposed or Implemented-unverified. No incident was marked Verified by this research.

<a id="rec4MkKhkfg18A2Pr"></a>
### 2026-10-09 — coordinator registry freshness and incomplete assignment registration — rec4MkKhkfg18A2Pr

Audit239 found90 complaints with blank support-doc-found and complaint-status, and only9 active-threads rows. Current coordinator, Schedule, coordinator trials/fork and three internal Barn assignments were missing distinct rows. Six legacy rows had October5–6 modification dates. Recognize did have a later update at2026-10-09T02:24:31Z, so the assertion that every row was over24hours old is not supported. Recent local notes and thread progress were not reliably reflected in the operational register. Coordinator accountability:01a11632-62fd-73b1-997e-8be4119185de.

Audit239 implements documentation/registry repair: classify90 complaints, register missing assignments, refresh actual known dispositions and link report. Ongoing timely updates are UNVERIFIED. Proposed closure: on next authorized bounded assignment verify dispatch, meaningful checkpoint/blocker and completion update/readback without owner chasing. Do not generate busywork updates for unchanged polling.

<a id="rec3y5PHaTnNz9wTw"></a>
### 2026-10-09 — skill source drift and contradictory enforcement guidance — rec3y5PHaTnNz9wTw

Audit239 compared local Request Router/Instruction Loader/Handoff/Rebalance with skills2 cloud summaries and found differing mode/ownership/loading statements. ringstatus-context and ringstatus-capabilities were absent at the expected user-local paths, while cloud links exist. Repository ringstatus-control claims mechanical hook write/completion enforcement despite disabled PreToolUse and nonsemantic completion checks. Third-party Designer discovery and verification rerun instructions conflict with narrower project boundaries. This establishes documentation inconsistency, not proof that every past failure was caused by it.

Proposed only: identify authoritative versions; selectively revise custom skill routing, handoff scheduler/owner block and enforcement claim; preserve user authorization and no-runner-substitute rules; verify one fresh bounded task loads and uses the intended sources. No SKILL.md, AGENTS.md or hooks changed in this audit.

### Saved records and artifacts

- Full report and enhancement plan: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/operating-audit-20261009.md
- Report/plan in rs-decisions: https://airtable.com/appZahVgD156cMAe3/tblpGVU87DQfJ9C84/rec9UEZDPF8sG9oo8
- Current coordinator assignment: rec7uEb7giQpiql3d
- New registry incident: rec4MkKhkfg18A2Pr
- New skill consistency incident: rec3y5PHaTnNz9wTw
- Newly registered assignments: recoHK2eNWxglpXBD, recBhyB8eWOJEldIF, recW42xozHRyIt4vX, recyYVrSJJtokrLeD, recozMi4EsR8xrD8p, recbQH68osb7PyjMA, recWdGMXxjoZtCHjk
- Before snapshot: .git/ringstatus-control/operating-audit-20261009-before.json
- Readback receipt: .git/ringstatus-control/operating-audit-20261009-verification.json

The snapshot preserves prior cell values for scoped recovery. Restore only the fields written by this audit after checking for later owner edits; do not blindly restore the whole base. New records and remediation-state are additive; review dependencies before removing them. No existing record or table was deleted. Runtime, application source, skills, hooks, schedules and deployment were not changed. The enhancement plan remains Planned until its individual implementation and qualification gates are met.
