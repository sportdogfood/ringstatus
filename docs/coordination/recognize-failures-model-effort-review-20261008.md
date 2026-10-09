# RingStatus delivery failures and model/effort review
Date: October 8, 2026 America/New_York (October 9 UTC). Owner revisions 227–229.
Accountable coordinator: 01a11632-62fd-73b1-997e-8be4119185de.

## Current disposition — revisions233–235

Owner reports the corrected recovery flow worked, with corrections still needed, and accepts it for now to stop further token expenditure. This is provisional owner acceptance, not full completion of the retained matrix. Testing and application edits are stopped. Historical defect descriptions below describe their observed revision; they are not assertions that the deployed destination defect remains present.

Revision232 deployed commit5684b4178b662289a4342146143cb055f0fc591c routes recovery SMS to native /rs-recognize and consumes the invitation through the existing access service. Revision233 submitted exactly one fresh native recovery request to the approved test person/phone. Request rec6QnYwPYqkIdKzu (UID42efce01-0c8c-4abe-b94d-6e6d55bb4668) has requested, attempt_started and unknown footprints. Automation run wfxixUUD309imS0Df succeeded at2026-10-09T03:09:46Z. Native Twilio supplies no provider reference; unknown/native_twilio_no_provider_reference is retained rather than relabeled delivered. Owner subsequently said the flow worked. No separate observation of their phone, final Barn landing or persisted returning-session state is inferred.

### Still needed to mark Recognize finished

1. Identify and resolve the owner's remaining corrections; verify mobile and desktop presentation/controls against the retained widths/themes. Corrections were not specified in the provisional acceptance.
2. Close the actual-path evidence: restored native identity, explicit Continue to /rs-barn-onboarding-setup-v24, matching device/session log readback in Recognize, and recognition after returning/reloading.
3. Complete the remaining applicable profile edit, login, device confirmation/retirement and access-denial checks under the settled invitation-only and SMS-only recovery policies. Do not reopen settled policy or enable public signup.
4. Qualify retained failure/retry gates in the deployed system: expired/reused/revoked invitation, ambiguous or unauthorized access, duplicate/retried requests, restart persistence and failed-log recovery. Local passing tests are evidence of local behavior, not these operational outcomes.
5. Record final protected-page regression and reconcile the full acceptance matrix before closure. No unrelated Barn/Feed/Schedule project completion is required to close Recognize, apart from its designated Barn handoff.

### Additional open work — revision236

- CSS audit: retain the cross-draft consistency audit, especially Feed, SMS Alerts and Barn setup v24; include Recognize and Schedule. Check buttons, typography, token colors/shades, spacing, padding, margins and responsive behavior across the required viewports/themes. Remains open; no audit or styling changes started by this entry.
- Schedule: retain the unfinished Schedule UI, Astro data-only mapping and end-to-end verification as a separate open task. Webflow owns presentation. Current implementation status must be read from its saved checkpoint before resuming; no completion is inferred here.

### Model research notes for the next bounded task

Use the previously researched official sources below as dated references; no new benchmark or research spend was launched for revision235. Provisional starting choice: Sol Medium for scoped implementation, Low for routine read-only checks, Astra High only for a specifically bounded difficult review. Compare the same small representative task with identical source, permissions, tools and acceptance. Record actual model/effort, tokens or credits where exposed, elapsed time, correction cycles, tool waiting/failures and independent acceptance results; set the same fixed time/correction cap before either run. Choose the lowest-cost setting that satisfies the same acceptance, not the largest test count. Do not attribute all account usage to this task or describe 'outsmarting itself' as an established cause. No model setting changed here.

## Finding and recommendation
### Revision231: approval relied on unestablished readiness
The owner states they would not have approved this path had they known the prototype was not established as working and tested for the intended complete flow. The coordinator failed to establish that evidence before progressing. An older Astro prototype, local test counts and successful SMS delivery did not establish native Webflow recovery, mobile usability or the final Barn handoff. Discovering the wrong landing destination only after the owner's live test is a readiness and verification failure, independent of model choice. Existing complaint reckAbo65bjkXBDnr retains this correction and remains open. No renewed implementation authorization is inferred from the complaint.

Use GPT-6.1 Sol at Medium as the starting setting for bounded Webflow/data integration implementation. Keep Astra High for a separately bounded difficult architecture, security or concurrency review. For proven read-only status checks, use Low. These are recommendations, not a benchmark result or a settings change. Lower effort must not reduce the acceptance requirements.

The actual coordinator log contains 308 turn-context entries for gpt-6-astra/high, four for Astra/medium, and two for gpt-6.1-sol/low. The inspected Recognize wiring-worker log contains six gpt-6.1-sol/low entries. Counts are saved context entries, not billable requests, hours or tokens. Therefore “all failures occurred because the coding worker used Astra High” is contradicted by the inspected worker metadata.

OpenAI's current Codex credit table prices Astra at 250 input / 25 cached-input / 1,250 output credits per million tokens, versus Sol 6.1 at 50 / 2.5 / 250. That is 5x uncached-input/output and 10x cached-input credits at equal token counts. It does not mean this task cost exactly 5x, nor does it translate the owner's subscription percentage directly to credits or dollars. Higher reasoning effort generally spends more reasoning tokens; lower effort favors speed and lower token usage. Astra High was a plausible contributor to allowance consumption in the coordinator, not a measured explanation of every percentage point or failure. [Codex pricing](https://learn.chatgpt.com/docs/pricing), [reasoning guidance](https://developers.openai.com/api/docs/guides/reasoning).

OpenAI positions Astra for the most demanding work and Sol 6.1 for a lower-cost balance, and explicitly recommends comparing them on representative tasks. There is no evidence here that High systematically “outsmarted itself,” that Medium would have prevented these defects, or that the model alone caused tool outages. [Model guide](https://developers.openai.com/api/docs/guides/latest-model).

The owner's approximately 35% daily burn is retained as an owner-reported estimate. Account usage is shared across tasks; no per-task cost attribution or equal-scope comparison was performed. Repeated discovery, oversized outputs, rework, long context and supervision turns are observed sources of work, but their individual cost contributions are not measured.

## Failure register and evidence
The existing complaint table is the canonical incident register: agents base appZahVgD156cMAe3, table tblPRAjSK54F8VA8U. Its current name/source index returned 90 records with no next cursor. Earlier full 82-record preservation remains in .codex/control/complaint-source-20261007.json and COMPLAINT-COVERAGE.md; the dated snapshot is not represented as a fresh audit of every historic incident. Existing records are preserved rather than duplicated.

| Failure | Evidence / existing incident | State and required correction |
| --- | --- | --- |
| Coding preparation and phase handoff were inadequately verified | reckAbo65bjkXBDnr; worker creation and skill-read review recorded there | Open. A dedicated wiring worker did exist; the claim that this exact worker was originally styling-only is unsupported. Relevant debugging/verification guidance was supplied late. |
| Local green test totals obscured unmet real-flow acceptance | reckAbo65bjkXBDnr, recCYMCEcyO6c48UO; revision159/185 counterexamples | Open. A test proving a known defect is not passing acceptance. |
| Restart, duplicate-event and invitation-consumption defects | rechz3ofexwVWukPt; revision205 manifests/runtime/regression evidence | Local correction and deployment are recorded; full live retry/concurrency qualification must not be inferred. |
| SMS fields were created before the actual sending integration was mapped | recCYMCEcyO6c48UO, revision180/183 evidence | Open. Owner added canonical person link and phone lookup; minimum-field requirement was missed. |
| Old two-way SMS Worker was proposed for an unsuitable role | recCYMCEcyO6c48UO | Existing Airtable-native Twilio was subsequently used; do not reuse the unrelated Worker. |
| Unsupported Webflow action/schema calls | recydcuqDQ6U1zXkp | Read actual tool schema before mutation; tool availability is not proof of valid arguments. |
| Native bindings collided with existing special settings | recOf0tbsnDHfngb0 | Preserve exact-page/readback evidence; do not call a saved attribute full interaction proof. |
| Incomplete environment pagination produced false missing-config claim | receQSwUUrQ6Y1SqU | Read every relevant page before declaring a dependency absent. |
| Deployed-source compatibility differed from prepared code | recS177wNbApfZZs3 | Verify exact deployed source and runtime rather than substituting a different checkout. |
| Owner became integration and QC operator; repeated intermediate stops | rec9wthLiiLrWFBw8, rec5822ZHBywJTwY0 | Open systemic failure; a continuation prompt or timer alone is not effective prevention. |
| Visual recreation, mobile verification and browser/Bridge delays consumed extended time | Prior Webflow complaints, owner screenshot of 2h39m run and retained messages | Owner-reported duration and observed tool issues; not a measured breakdown of productive work vs waiting. Keep styling replication separate from new backend behavior. |
| Agent crossover proof was confused with ordinary app automation | Owner's retained conversation corrections | Owner-reported scope failure; ordinary website-to-CRM automation does not demonstrate agent-to-agent delegation. |
| Repeated tool/schema/path mistakes and excessive inspection output | Current coordinator tool history, including wrong Windows wildcard paths and overly broad record output | Coordinator execution failure. Narrow source reads and validate argument names first. |
| SMS reaches the old Astro UI instead of the intended native Webflow destination | Revisions228–229; rs-recognition-sms.js:82–83/101, onboarding.astro, AccessGate.tsx; owner reports frozen/nonmobile page | CONFIRMED destination mismatch, user-reported phone freeze. No repair applied in this review. |
| Successful SMS receipt was insufficient proof of restored native access | revision226-live-recovery-proof.json and subsequent owner correction | Delivery and invitation consumption remain separate successes; native restored access/Continue/mobile gates remain failed or unverified. |

## Current destination defect: exact boundary
The SMS generator reads RS_INPUTS_ONBOARDING_URL and requires a path ending in /onboarding. The configured destination was https://ringstatus.webflow.io/test/onboarding. That Astro page mounts the previous React App and its AccessGate. The gate consumes the invitation and displays its children; it does not redirect to /rs-recognize. Browser inspection of the nonprivate public URL showed a “Preview device: iPhone” selector and the Astro access screen.

The native /rs-recognize client does not currently consume an invitation fragment. Merely changing the SMS URL would therefore leave a missing acceptance step. A bounded correction must reuse the existing invitation API, complete the handoff into /rs-recognize, and verify Continue to the designated Barn page. Preserve Webflow styles, the original /recognize proof of concept, and unrelated Inputs/auth callers. The user's report proves the phone outcome failed; it does not establish the technical cause of freezing or mobile sizing.

No repeat SMS, private-link inspection, production repair, deployment or style change was performed for this review. The existing instruction requires separate APPROVED TO EDIT after a failed verification before applying a code correction.

## Practical operating recommendation
- Routine read-only monitoring and exact record lookup: Sol Low, compact observations.
- Native Webflow replication and scoped Astro/Airtable implementation: Sol Medium; one task, exact target, existing source, required viewport/interaction evidence.
- Difficult auth, concurrency or architecture question: bounded Astra High review with a specific decision/output; then return to the implementation setting.
- Preserve the same success criteria at every model setting. Escalate effort for a demonstrated reasoning problem, not transport failures, missing credentials or repeated schema errors.
- Before another implementation task, identify its actual model/effort and successfully load the relevant skills. Verify the destination and first complete user path early, before adding optional behavior.
- No automatic model switch, new hook, new agent or benchmark was launched. Quality savings remain unproven until compared on the same bounded task.

## Evidence locations
All repository-relative paths below refer to C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus:
- docs/coordination/recognize-wiring-task.md — assignment, authorizations, revisions and release history.
- .git/ringstatus-control/recognize-native-wiring/revision205-manifest.json — local repair evidence.
- .git/ringstatus-control/recognize-native-wiring/revision219-release.json — rollout evidence.
- .git/ringstatus-control/recognize-native-wiring/revision226-live-recovery-proof.json — real recovery request, automation and owner receipt; not native end-to-end success.
- .codex/control/COMPLAINT-COVERAGE.md — earlier 82-incident coverage, with unresolved limitations.
- Coordinator session log: C:/Users/gombc/.codex/sessions/2026/10/07/rollout-2026-10-07T07-49-23-01a11632-62fd-73b1-997e-8be4119185de.jsonl.
- Wiring-worker session log: C:/Users/gombc/.codex/sessions/2026/10/08/rollout-2026-10-08T14-26-24-01a11cc4-39cd-7eb2-872f-89ef41193275.jsonl.
- Inspected deployed-source checkout: C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test.
