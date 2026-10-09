# Recognize — PARTIAL checkpoint and resume record

Owner disposition: revision294, 9 October 2026: “lets claim partial and document ALL”.
Status: **PARTIAL — not locked complete.** This is a documentation checkpoint, not permission to resume tests, send SMS, publish, deploy, redesign or start another task.

## Current workflow and boundaries

- Recognize is recognition, separate from onboarding, profile editing, barn membership, authorization and SMS opt-ins.
- A recognized browser silently resolves its device/person, reuses or records the appropriate session, refreshes the 365-day recognition cookie and returns home (`/`, temporary launcher destination).
- An unrecognized browser prompts only on designated gated/profile entry points. `/rs-recognize` is the explicit entry/test page. Global-footer rollout is not demonstrated or completed.
- Desktop unknown-browser login uses a phone-sized native Webflow fly-up. The user enters their phone, receives a six-digit SMS code and enters it in the SAME desktop browser. A phone-opened magic link does not establish that desktop's recognition.
- Use the owner-approved cryptographic random-code implementation and existing Airtable-native Twilio delivery, not Twilio Verify. Code expiry: 10 minutes; maximum five checks; bounded sending. Cookie lifetime: 365 days. These are configured/local-tested behaviors, not proof of elapsed production durability.
- Phone and associated geo/IP observations may support lookup/logging. IP/geo alone must not grant identity or access.
- Webflow owns native markup, styling and states. Astro maps data/actions; no replacement styled template, React shell or injected stylesheet.
- Application data/log destination: Recognize `app9kOZdIaGyKk5uG` only. Original `/recognize`, old-base business data, existing Inputs auth/callers and unrelated two-way SMS Worker remain protected.
- Only authorized test recipient: `16318752160`. Do not send additional messages merely to refresh this checkpoint.
- Reuse proven implementations; no replacement or stacked speculative repair without explicit authorization. An approved workflow failure requires stopping that path; no direct endpoint/record repair substitutes for workflow proof.

## Exact saved/deployed state

| Artifact | Last recorded state |
|---|---|
| Webflow site/page | Site `6982268b7543ac3c80151266`; page `6ac459b135ad0c4c3b253d97`; `https://ringstatus.com/rs-recognize` |
| Astro source | `C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test` |
| Deployed commit | `cc83c1ebb33aea76219ada69aba5294d8c858b32`; parent `8578c8802c7ffb876f32fd35b2466a351c03dc8f` |
| Cloud deployment | `09579c8c-2fce-4b34-a4f4-889112fc4f6f`, success recorded `2026-10-09T14:54:44.172Z` |
| Cloud app/environment | `d7d97751-20e1-4148-a5cf-ee58671c128a` / `110f06dd-c1ea-4839-98af-d829cbe77941`; mount `/test` |
| Native client/API | `/test/rs-recognition/native-client.js`; `/test/rs-inputs/native-recognition` |
| Revision292 draft loader | Module uses current-origin relative URL; baseUrl uses current location origin. Complete saved footer readback matched. Prior footer preserved; only two URL expressions changed. |
| Revision292 native style | `rs-recognize-surface` max-width changed 560px → 393px. Existing bottom fly-up positioning, height, typography and colors retained. Style-write response confirms saved value; no independent rendered proof yet. |
| Publication | Revision292 changes were NOT published by this agent. No later owner publication confirmation is recorded at revision294. Do not infer current live version. |
| Delivery automation | `wfl6oiGMgUPmEkUuS`, existing `sms-recovery` Airtable/Twilio workflow, unchanged in revision290/292 |
| Atomic control storage | Existing D1 binding `RS_RECOGNITION_CONTROL_DB`, existing `recognize_claims` table; no new database/table in random-code change |
| Secrets | Existing automation secret and session secret reused; no values in this record |

Current-origin correction applies to the browser loader. The existing automation's server-side `ringstatus.webflow.io/test/rs-inputs/native-recognition?operation=sms_prepare` URL is a separate working path and was not changed.

## Application tables

All below belong to `app9kOZdIaGyKk5uG`:

| Purpose | Table ID |
|---|---|
| People (`rs_people_test`) | `tbly1PM5iFYqVzKSm` |
| Devices (`rs_devices_test`) | `tblfkRSJAEMzuzApR` |
| Phone aliases | `tblgDWKi0Bb6OcoqS` |
| Recognition sessions/events | `tblWjbASVMIjFLyW8` |
| SMS requests | `tblxC4SYtzYhJNcJS` |
| SMS events | `tblF1Hdqi3yoKTZPV` |

Test person `recV9CvYGaTGz7Wzr`. Table names ending in `_test` are not by themselves proof of isolation. No additional fields/tables or cleanup were performed for this disposition.

## Evidence: what succeeded, and its limits

| Evidence | What it establishes | What it does not establish |
|---|---|---|
| Revision290: 218 regression tests, one actual two-Worker/D1 runtime test, Astro build | Local implementation checks including atomic consumption, wrong/expired/replayed codes, attempts/send limits | Actual desktop SMS completion, published presentation or all historical acceptance |
| Revision290 Cloud deployment success | Recorded source was deployed | User journey passes |
| Actual desktop request and automation `wfxLeS1dqzHHLJ0a5`, 14:55:15–14:55:24Z, success | UI requested code through existing automation; desktop displayed code field/Verify code | Owner received this specific OTP, entered it successfully, redirected and silently returned |
| Revision255 owner confirmation plus matched session `recySpoGfsRMvmWY4` and success `recQmeA5Dwo6lbhV2` | Earlier magic-link/Continue journey reached Barn for test person/device | New desktop OTP journey; current home destination; every device |
| Revision281 owner: “yes returns to home” | Owner-observed same-browser silent return/home gate for that tested browser | Fresh independently correlated person-linked footprint for that revisit |
| Revision292 exact saved footer readback and native style response | Approved loader correction and 393px width saved | Published/live adapter initialization or visual acceptance |

Historical SMS provider outcome remains unknown when native Twilio does not expose a provider reference. Owner receipt and automation success are separate evidence. Do not repair an unknown status into delivered.
Ordinary queue/events and D1 do not store plaintext OTP in the recorded implementation; authenticated automation receives the SMS body and its run history may retain it. Do not claim no plaintext exists anywhere.
Test totals are scoped evidence, not interchangeable units of end-to-end completion. Historical passes do not automatically qualify later versions.

## Failures and unresolved gaps

Existing canonical incidents remain in agents base `appZahVgD156cMAe3`, `rs-agents-complaints-lib` / `tblPRAjSK54F8VA8U`. Preserve their histories and unresolved status.

| Failure/gap | Evidence/reference | Disposition |
|---|---|---|
| Incomplete baseline/acceptance, insufficient reuse of supplied proofs | `recNOlUMUFI4qZ8p8`, C004 `recINFZbBMDKdkRwZ` | Open systemic issue; revised instructions do not prove consistent adherence |
| Coding readiness/phase ownership and skill guidance supplied too late | `reckAbo65bjkXBDnr` | Open; do not conflate styling and backend assignments |
| Mock tests missed deployed runtime incompatibility | `recRW0nqJFLyQ04mt` | Historical scoped repair exists; complete current workflow still unverified |
| SMS fields/integration mapped late; old two-way Worker unsuitable | `recCYMCEcyO6c48UO` | Existing Airtable-native path reused; full current OTP delivery/entry evidence pending |
| Restart, duplicate-event and invitation race failures surfaced late | `rechz3ofexwVWukPt`, revision205 evidence | Local corrections recorded; no blanket live durability claim |
| Wrong Astro onboarding landing/frozen mobile report | revisions228–255 and failure/model review | Earlier destination corrected and owner handoff succeeded; not current desktop proof |
| Recognition depended on input instead of silent returning flow | revisions261–281 | Corrected scope and owner return report retained; full linked revisit evidence unresolved |
| Desktop magic link recognized the phone rather than requesting desktop | revision285 | Replaced by authorized SMS code flow; real desktop completion pending |
| Published footer reverted to cross-origin loader; Alex Morgan demo remained | revision291 DOM: adapter unavailable; revision292 saved footer confirmed old origin | Two URL expressions corrected in saved draft; cause of reversion UNKNOWN; publication/live verification pending |
| Desktop presentation complaint | Owner “desktop a mess”; observed 560px surface; revision292 393px save | New width unverified visually after publication |
| Repeated close/reopen/native state divergence | revision252-live-verification.json | Retained interaction gate; not implicitly closed by saved width |
| Unsupported tool arguments and oversized inspection output recurred | revision292 invalid get_page_custom_code/get_styles query calls; existing `recydcuqDQ6U1zXkp` | Correct supported calls later succeeded; process failure remains recorded |
| Excessive rework, pauses and owner babysitting/cost | failure/model review and owner reports | No proven recurring prevention or measured per-task cost attribution |

Model/effort: owner reported approximately35% account allowance consumed and a reset. Prior inspected metadata showed substantial Astra High coordinator use, while the wiring worker also had Sol Low entries. The hypothesis that reasoning effort increased cost is not proof it caused defects or “outsmarted itself.” Existing dated research/recommendations are in `recognize-failures-model-effort-review-20261008.md`; no fresh research, model change or benchmark performed for this checkpoint.

## Full remaining acceptance (not reduced to publishing)

1. Establish revision292 publication and exact live loader/source. Unknown-browser desktop shows usable393px native fly-up, not sample Alex Morgan state.
2. Actual UI phone request → existing automation → owner receives code → code entered in initiating desktop → home. No direct endpoint substitute.
3. Fresh silent revisit in that desktop, correct365-day cookie behavior, correct person/device/session and footprint linkage; distinguish reused session from fresh event.
4. Current mobile and desktop visual/interaction proof: keyboard/focus, close/reopen, errors/loading, duplicate handlers, long/empty content, white/dark and retained responsive matrix. Original widths375,393,479,480,481,767,768,899,900,991,992,1280,1440,1920 and393×852 remain documented, not silently marked passed.
5. Wrong/expired/reused/concurrent code and bounded retry behavior; invalid/ambiguous/inactive/retired identity, revocation/Not you; no unintended access/credential disclosure. Preserve local versus approved live test boundary.
6. Persistence/restart, duplicate effective actions, logging-failure/unknown-outcome recovery and protected-route/page regression gates remain mapped to evidence; no broad live proof inferred from local tests.
7. Original plan's profile create/edit/add, refreshed persistence, invitation/access and Barn handoff acceptance remain in the history. Owner later separated Recognize from profile/onboarding, deferred invitation focus and selected home; these are explicit scope changes, NOT passed features. Profile/Barn/opt-in acceptance belongs to separate unfinished assignments and must be reconciled before any broad original-plan completion claim.
8. Canonical data/log fields and actual write destinations remain correct in designated base. Existing anonymous home geo logs cannot be combined with older matched records to claim current identity linkage.
9. Only after current applicable evidence is complete may Recognize be locked. Owner acceptance of PARTIAL is not completion, authorization extension or waiver of failure history.

Other work remains separate: Barn Inputs, Barn Onboarding, Barn Opt-ins, Feed data mapping, Schedule and CSS/consistency audit. This checkpoint does not run, close or certify them. User had reported Feed draft ready; that is not data-wiring proof.

## Ownership and restart boundary

Coordinator `01a11632-62fd-73b1-997e-8be4119185de`; historical worker `01a11cc4-39cd-7eb2-872f-89ef41193275`; active-threads record `recXpqNMKrk6b7tg5`. Worker previously idle, old monitor state PAUSED with elapsed deadline. No fresh worker status or automation scheduling check is claimed by this documentation update; no agent/monitor resumed.
Active record remains open (`closed=false`) with PARTIAL disposition. The coordinator's recent direct work must not be attributed to a running historical worker.
Resume only from this checkpoint plus latest owner instruction and actual target state; do not restart discovery or replace architecture. First outstanding dependency is publication/verification of the already-saved revision292 changes. User OTP entry stays private.
The unrelated retained reliability-controls contract remains unchanged and unresolved; this Recognize checkpoint does not certify it.

## Files, historical proofs and reversal

Primary repo: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`.

- `docs/coordination/recognize-wiring-task.md`: full assignment, revision history, releases and source references.
- `docs/coordination/recognize-native-astro-plan.md`: original full acceptance; interpret with explicit later owner corrections above.
- `docs/coordination/recognize-failures-model-effort-review-20261008.md`: failure ledger, dated model research, cost caveats.
- `docs/coordination/operating-audit-20261009.md` and `.codex/control/COMPLAINT-COVERAGE.md`: earlier operating/complaint audit; not newly revalidated.
- `.git/ringstatus-control/recognize-native-wiring/revision290-{tests,runtime,regression,build}.txt`: local proof; regression total218, runtime1.
- Same evidence directory: `revision252-live-verification.json`, `revision274-release.json`, `revision281-owner-return.json` preserve historical live limits.
- Same directory: `revision292-footer-before.html` is exact pre-repair footer. Reversal also restores native surface max-width560px. That footer contains the known broken cross-origin loader; rollback is not recommended success state.
- Deployed revision290 code can be reversed through a separately authorized scoped revert of `cc83c1e...` to parent `8578c88...`, after checking later changes. Never reset unrelated files, D1 data or business records.
- Legacy reference `f012c3077` preserves original rs-recognition member/session code; `647ea24f8` preserves WEC Packing reusable examples. They are historical evidence, potentially stale, not safe-to-deploy replacements.
- Native binding reference page `6a2210d896d9a3dc2eee8fe8` is recorded in existing task history.

No application code, Webflow, automation, schema, secret, deployment, publication or business data changed for revision294. This document preserves known evidence; it does not assert an exhaustive fresh retest.

