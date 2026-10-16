# Recognize / Tenants — three review corrections, 15 October 2026

## Remaining live checks - 15 October, 20:26 UTC

Existing owner phone-format save PASSED visibly: ten-digit input -> Saving -> Saved; canonical phone_e164 retains the same normalized +1 number. Reload and Edit my profile confirmed normalized value. Active status and both owner memberships unchanged. No source change, deployment, static publishing, PIN/invite attempt, or repeat of two-User proof. Existing Barn Retry draft preserved. Evidence: .git/ringstatus-control/recognize-tenants-full-review-20261015/remaining-live-phone-20261015.json.

Still unverified: fresh nonowner phone sign-in/access, role/status transitions, individual consent writes, image/logo writes, API-agent parity. Asked owner for an existing nonowner test identity and registered phone; no new phone or credentials invented. Owner/Barn image URL fields observed empty. This normalization proof does not establish replacement-number synchronization or fresh authentication.

## Latest save-path release - 15 October, 19:34 UTC: scoped live proof passed

One-file commit 0dec58fb39ab35cab5c2a4fd830783dab0257f00 deployed successfully as 4d6e4f84-f8d0-4157-8673-8fc539a10462 at19:31:43.842Z. Owner ten-minute window started19:25:55Z. Product verification completed before deadline. No static publishing.

The existing server bridge now reuses first-validation cached reads, retains fresh validation after reservation, avoids unchanged credential synchronization, and returns the verified edited record after fresh actor/access and target membership/roles/roster checks instead of rebuilding the event snapshot. Independent review caught a missing target-membership check; corrected before deployment and re-reviewed with no remaining blocker. Final build passed; selected regression10passed, focused recovery9passed. Local zero-network-latency profile trace:74 calls/10.724s before,50 calls/8.673s after.

Same Edge tab685191457: retained owner edit Retry returned200/Saved in2.824104s. Edit my profile opened correct owner with zero Inputs requests. Fresh display-name edit from RingStatus Test - verified back to RingStatus Test showed immediate feedback, then200/Saved in14.036384s. Reload and Airtable readback confirmed display-name RingStatus Test, Active status and unchanged two owner memberships. Prior two-User proof preserved and not repeated. Unrelated Barn Retry draft preserved.

Evidence: .git/ringstatus-control/recognize-tenants-full-review-20261015/save-path-proof.json, save-path-live-saved.png, save-path-live-reloaded.png. Application test-evidence/save-path-20261015.log, save-path-recovery-final.log, save-path-count-20261015.log, save-path-build-final-20261015.log. Agents, Recognize and Tenants checkpoints updated and read back.

Scope complete: profile opening and save confirmation, including reload. Not blanket V1 completion. Server confirmation still takes14seconds. Owner-deferred PIN/invite checks and other existing V1/API-agent gaps remain. No automatic restart. Local report write encountered an encoding error after product completion; prior report content below recovered from its recorded full readback, then saved explicitly as UTF-8. No product/data change during report recovery.

## Latest deployment/live check — 15 October, 18:49–18:51 UTC

Owner approved deploying only the tested four-defect package and checking profile opening/save confirmation within ten minutes, no static publishing. Source hashes matched the reviewed manifest. Isolated commit `99652c87faa052c05af9c1a9a3890368f21e6b67` contains only six production files plus its targeted test atop `a6d394e5c189b254c64c8e2c25bdc4fd3e124af3`. Deployment `e5e16bad-2034-4ff7-8337-e9cc2c4fa5af` succeeded at18:49:22.162Z.

Live profile opening PASS: fresh Edge tab685191457 in the existing profile loaded assigned Barn; Edit my profile opened the correct owner, with zero Inputs network requests after clicking (no profile-link write). Existing test owner display-name changed to `RingStatus Test — verified`; immediate visible update/Saving observed.

Live save confirmation FAIL: POST `/test/rs-inputs/record` started at CDP timestamp656261.273349 and received HTTP504 at656281.392789 (20.11944seconds). Response was HTML titled `cdn.webflow.com | 504: Gateway time-out`, server Cloudflare; response CF-Ray `a4b102467ce7e6c4-MIA`, error-page Ray `a4b1024689bde6c4`, date18:50:40Z. Client retained `save_504`, unchanged operation/draft and Retry. No retry was executed.

Connector readback confirms canonical People `rec09Fqo8EzuNXHa1` saved the changed display-name; original name, Active status and both existing owner memberships unchanged. Thus business data applied but HTTP confirmation failed. Exact slow backend phase remains unproven; latest available runtime logs stop17:52Z. This evidence does not retrospectively prove the original incident's cause. Existing two-User proof and records preserved; no static publication, automatic replay, new correction or record repair. Pending edit stays in the same marked Edge tab. Next correction requires the applicable post-failure edit authorization; scope is the measured response timeout and unnecessary post-write work, using WEC returned-record confirmation, not a new saving architecture.

Evidence: `.git/ringstatus-control/recognize-tenants-full-review-20261015/release-four-defects-candidate.json`, `four-defects-live-proof.json`, `four-defects-live-save-504.png`.

## Latest local correction — 15 October, 18:12–18:22 UTC timebox

Owner approved editing/testing the four independent-review defects only. Applied in the existing `recognize-tenants-bindings/ringstatus/webflow-cloud-test` worktree: server-confirmed saves survive local cleanup failure; existing unique owner/Barn profile opens through the existing editor without a write; timestamped interrupted operations recover through exact-JSON fencing and existing readback, with atomic ownership-conditioned claim cleanup; errors retain HTTP status/request ID/content type separately from safe user copy. The source error code remains a string; stale diagnostic details are cleared between attempts. Native client regenerated from its existing build script. Route logging now identifies the actual operation and safe submitted operation ID.

WEC reference actually inspected: `webflow/packing-worksheet/wec-packing.js:897–935`, returned-data confirmation and status-bearing failures. Existing Tenants authorization, identity, consent and duplicate-write checks retained. No replacement implementation, schema, automation, deployment or static publication. Previously passed live two-User proof was not repeated.

Verification: 18 targeted/persistence tests passed; four applicable existing client checks passed. Independent reviewer found a stale-executor cleanup race, which was corrected and independently exercised using the actual save-operation code with SQLite. Final diagnostic metadata refinement was also tested. Evidence in the app's `test-evidence/four-defects-targeted.log`, `four-defects-build.log`, and `four-defects-hashes.json`; new tests `test/tenant-save-four-defects.test.js`.

Limits retained explicitly: pre-update busy operations without timestamps remain fail-closed, because an old executor's cleanup cannot safely be assumed stopped. Device cleanup failure can leave a retained operation after reload; confirmed server success is no longer reported as failed in the current save. Original historical failed-response cause remains unproven. PIN/invite checks remain deferred by owner; API experiment remains separate. A broader old client test failed at its phone-recognition fixture before reaching the save path; it is not a passing regression claim and was not expanded into unrelated fixture work (`four-defects-tests.log`). No live acceptance of this undeployed correction is claimed.

## Independent deployed-save review — 15 October, 18:00 UTC

Read-only reviewer `/root/save_baseline_review` inspected deployed commit `a6d394e5c189b254c64c8e2c25bdc4fd3e124af3` against WEC/HPS. No implementation, deployment, business calls or repeat of the passed two-User proof occurred. Pending owner-deferred PIN/invite checks and the unavailable API experiment do not block reviewing or correcting the native save path.

Confirmed code findings, not attribution of the historical failed response:

1. `native-client.js:277–288`: a validated successful server result is followed by awaited local `device.confirm`. Local storage cleanup failure enters the save-failure path. Separate confirmed server success from local cleanup failure while preserving safe reconciliation.
2. `native-client.js:640–649`, `rs-inputs.js:56–63,94–110`: Edit my profile always calls the writable profile-link path, even when the matching owner is already loaded. It can restage recognition name/email, advance revision and invoke credential/event work merely to open an editor. Use the existing editor for the uniquely matched, authorized owner; link only when actually missing.
3. `rs-tenant-save-operation.js:14,23,48–58`: interrupted execution or failed cleanup can retain busy state indefinitely and reject same-operation Retry before recovery readback. Any correction must distinguish an active operation from an interrupted one and preserve duplicate-write protection.
4. `native-client.js:243–244,288`: HTTP status, content type and request ID are discarded, and retained errors become generic messages. WEC `wec-packing.js:915–917` retains detail/error/HTTP-status fallback. Restore that diagnostic distinction and correlate the existing operation without exposing private data.

Parent verification used the actual TypeScript API bundled in memory with synthetic HTML 502 and 504 responses. Both returned `request_failed`, the same generic message, and no preserved status or request ID. This proves diagnostic loss; it does not establish that either status occurred live.

The bridge also performs fallible relationship/event writes, authorization/readback reconstruction and receipt completion after the primary data write. That can explain applied data with incomplete confirmation in principle; no captured failed response proves it caused the earlier incident. Preserve necessary permission/receipt checks; do not remove them on speculation.

Next correction scope is the confirmed save/editor defects above, followed by focused failure-path checks and independent review. No correction was made during this review. Original incident causation remains unknown because its failed response and matching server evidence were not retained. The passed assigned-Barn/two-User proof stays closed; no credential creation, API-service startup, new architecture or static publication is implied.

## Latest V1 check — 15 October, 1:50–2:00 PM Eastern

V1 REMAINING CHECKS — 2026-10-15 17:50–18:00Z bounded continuation, no new implementation/deployment/static publishing. Prior exact two-User proof preserved and not repeated. New live proof: Edit my profile eventually returned 200 with correct owner, SMS and owner role; owner display-name 'RingStatus Test' saved with visible Saved/HTTP200 and canonical People readback. Existing User 2 display-name 'Release User 2' and tags 'v1-check' saved visibly and persisted in Airtable and the reloaded editor; default user role/Active membership unchanged. User 1 opt-ins GET200 showed none; owner opt-ins GET200 showed existing Revoked consent and separate options, unchanged. Known-browser /about-ringstatus remained public without sign-in or redirect. No API-agent listener on its configured local port 43187; deployed experiment config absent from source copy, so hosted model parity not executed. Owner explicitly requested leaving PIN/invite live checks pending.

OPEN: (1) Original save-confirmation cause unresolved: pre-retry failed response was not captured; latest runtime logs stop 17:31:25Z before the 17:33 save. Successful same-operation Retry does not establish root cause. Current owner-profile attempts also showed generic request_failed before an observed 200; no matching failed response retained, so no causal attribution. Existing client maps non-ok/nonconforming responses without error code to request_failed; GET storage failures and several post-write readbacks can fail, but none is proven as this incident's cause. (2) Live phone/PIN/owner-invite parity: PIN/invite pending by owner decision; phone-only fresh entry, inactive/no-phone, nonowner role restrictions remain local-proof-only on this release. (3) API-agent live parity requires its existing local service/configuration to be available. (4) Live SMS/PIN/profile-image/Barn-logo edits, role/status changes and opt-in save/opt-out by newly recognized individual User are not covered by today's additional display-name/tags/read-isolation checks. Do not turn these into passing gates. Unrelated retained Barn draft and deferred Setup/Add Barn remain untouched. Do not repeat the passed two-User proof.

Evidence: `.git/ringstatus-control/recognize-tenants-full-review-20261015/v1-remaining-checks.json`, `v1-fields-reloaded.png`. No prior passing tests were rerun; no automatic extension.


## Current checkpoint

**Regression closure — 15 October 2026, 17:09 UTC:** the complete previously affected 136-test set now passes, with zero failures, skips or cancellations (`test-evidence/remaining-regressions-final.log`, exit 0). The remaining historical fixture task is closed. The narrower 71-test fixture run also passed. Only tests/support changed during this closure; all 158 source-file hashes still match `corrections-source-after.json`, retaining the prior build and visible-browser evidence. Independent read-only review found no actionable issue in these fixture changes; authorization assertions were preserved. Nothing deployed or published. Next: owner authorization for the scoped application release and hosted acceptance, not another fixture/debugging cycle.

Owner authorized editing and testing the three findings from `recognize-tenants-full-workflow-review-20261015.md`, using existing working code, with no reinvention or publishing. Corrections are saved locally and independently re-reviewed. This is not a deployed acceptance claim.

Source: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test`; HEAD `bf9a93531209212f303d7e8175af5c9fb01ba2bd` plus existing dirty work. Preserve all earlier changes. Seven source files differ from the pre-review manifest; six edited and one new local helper. Exact hashes are in the main repository `.git/ringstatus-control/recognize-tenants-full-review-20261015/corrections-source-after.json` and `corrections-changed-source.json`.

No deployment, static publication, real SMS, schema/automation changes or production-record tests occurred. Existing live two-User proof from release `733587411ad328b5535c4549d9b7a81f9a896389` remains historical and does not verify this newer source.

## Corrections and baseline

1. **Legacy public bypass closed.** `src/pages/rs-recognition/action.js` returns 410 without upstream writes. Current native and API-agent interfaces already use the shared Inputs credential controller. Internal authenticated recognition helpers remain available. `rs-recognition-action.js` accepts explicit PIN only, never phone suffix. Phone-only recognition remains the approved behavior; no mandatory SMS was added.
2. **Existing direct-save recovery corrected.** WEC `webflow/packing-worksheet/wec-packing.js` and HPS `webflow/hps/hps.js` remain the baseline: immediate local display, direct Astro/Airtable save, confirmed record merge, retained same-operation retry. The server reuses the existing Recognize D1 claim/readback pattern from `rs-recognition-session.js`, in `rs-tenant-save-operation.js`; no new table, service, queue or automation. Caught failures before effects release reservations. Observed applied writes are read back by stable identity/returned record ID before retry continues. Same-operation identity and current permissions remain enforced. PIN is excluded from saved operation envelopes. Stable cookie/PIN access remains available during interrupted phone synchronization, while entered phone must match the canonical registered number.
3. **New-User personal opt-ins work without seeds.** `rs-input-subscriptions.js` routes first use to the existing `personalPreferences` direct writer. Empty initial choices do not grant consent. Explicit save/reload uses existing canonical opt-ins and audit. Existing staged records retain their processor. Missing direct audit is recovered through its matching durable reservation and target request identity, excluding staged submissions. No consent is copied from owner to User, and no SMS is activated.

Independent re-review found two follow-up defects in the corrections: a demoted owner's pending self-edit could restore old roles; an absent first-use consent audit could switch Retry to the staged path. Both were corrected and exact regression tests passed. `/root/full_workflow_review` inspected the resulting source again and reported no additional actionable defect in that focused review. The reviewer did not run tests or live calls; parent executed the checks below. The dedicated CLI limitation recorded in the original report remains unchanged; do not describe the CLI as having completed a review.

## Executed proof

Evidence directory is the application checkout's `test-evidence/`.

| Evidence | Actual result |
| --- | --- |
| `review-corrections-profile.log` | Assigned Barn, owner profile/Barn logo, two Users, return/edit/reload, phone/PIN, owner-only invites, disable/reactivate/no-phone, role restrictions and individual opt-in isolation passed through actual application handlers with synthetic storage. |
| `review-corrections-defects-final.log` | 32 checks passed: legacy route closure; existing direct-save regression; zero-effect preflight recovery; four lost applied responses; no duplicate User/membership; partial cross-base phone recovery; demotion denial; first-use opt-ins; repair of failed first-use consent audit. |
| `review-corrections-current-regression.log` | 29 current credential/API-agent policy/access/submission/reservation/profile-link checks passed. Hosted model invocation is not tested. |
| `review-corrections-visible-users.log` | Local Edge, one session: assigned Barn → User 1 → User 2 → return → inline-edit User 1 → save → reload. Immediate display preceded storage confirmation. An actual handler/provider rejection produced Retry; reload preserved the same body/request ID; Retry saved and reload confirmed. Owner profile/Barn logo and separate opt-in display also passed. |
| `review-corrections-visible-alerts.log` | Existing mobile/desktop preferences save, opt-out history, retained retry, actor isolation and unknown-visitor Recognize return passed. |
| `review-corrections-new-user-alerts.log` | Fresh User without preferences/options visibly opened all-off choices, explicitly saved and reloaded their own selections; owner consent unchanged. |
| `review-corrections-browser.log` | Owner activation/role browser check passed. The initial combined run then encountered an outdated alerts identity fixture and was stopped; this file is not a passing suite. Corrected alerts fixture and separate reruns above supersede it. |
| `review-corrections-build-final.log` | Final Astro build exited 0. Existing dynamic-route `getStaticPaths` warning remains. `git diff --check` exited 0. |

Browser tests use current native JavaScript and archived Webflow markup, local SQLite D1 and synthetic Airtable records; external requests are intercepted. They establish visible local behavior, not current hosted CSS, deployment, live schemas, real model output or production latency. Screenshots: `tenants-user-profile-mobile.png`, `tenants-user-profile-desktop.png`, `new-user-optins-mobile.png`.

## Evidence limits and remaining work

- Historical failure retained: `review-corrections-regression.log` returned 81 passed / 55 failed. After the prior source corrections and current fixture alignment, the same full affected 136-test set passes in `remaining-regressions-final.log`. Fixtures now supply canonical owner/membership records through the injected provider; subscription cookie setup uses that provider; preference setup uses the existing direct writer; assigned-Barn tests edit existing records and retain forbidden public-create coverage; closed-mode checks assert rejection before network access. Current User/no-phone/inactive/role/invite negative tests remain. This closes the specific regression task, not every repository test or hosted acceptance.
- Recovery is not universal: an abruptly terminated executor can retain a busy claim; an uncertain create with no observable result remains blocked to avoid duplicates. No automatic lease stealing or blind write replay was introduced. These cases require investigation/reconciliation under separate authorization; ordinary Retry must not be described as guaranteed recovery from every interruption.
- No current hosted native/API-agent/public-footer acceptance occurred. The exact current source requires release authorization before deployment, followed by the agreed hosted journey. Do not rerun the proven basic build as discovery or restart stopped operational agents.
- Follows, show/rider/horse/location/ring expansion and new indexes remain deferred.

## Resume

Read this checkpoint and the saved source manifest first. The three scoped source corrections, listed local journeys and full affected 136-test regression set are verified; preserve them. Do not repeat fixture alignment or restart stopped operational workers. Retain unresolved-interruption limitations. The next owner action is authorization to deploy the scoped Recognize/Tenants changes to the existing /test app and perform hosted native/API-agent acceptance: recognition into assigned Barn, owner/User fields, two Users, return/edit/save/reload, roles and individual opt-ins. Prepare/isolate the scoped release against the current remote baseline using the established release procedure; do not push the whole dirty checkout. No static publishing or automation changes are required by the local corrections, and none are authorized here. Live verification may expose an actual host-specific dependency; do not preemptively invent one.

## Approved deployment and multi-Barn correction — 15 October 2026, 17:30 UTC

The owner authorized deployment and the exact live journey: recognized assigned Barn → add User 1 → add User 2 → return → inline-edit User 1 → save → reload. No static publishing. Release `625ca0ad73322fe1efca8cf61d71645659bfd324` deployed successfully as Webflow deployment `08478b86-5822-4c64-b14b-da8cd3f70855` at 17:17:23Z. This supersedes the release-authorization boundary above.

Live Edge recognition reached the assigned Barn without manual selection. Fresh state then returned `409 ambiguous_tenant_mapping`. The canonical owner has two valid enabled Barn memberships; the prepopulated User reader incorrectly treated the same Person across different Barns as a duplicate. Exact-name Airtable readback proved the first new User was not saved. Its draft `TEST — Release User 1 2026-10-15 1318` remains in the existing Edge tab; preserve it and use its Retry after the corrected release. Do not repair/delete either membership.

Owner separately approved this correction, testing and deployment. The two-file correction scopes User reads to the selected/requested Barn through state, profile-link, save validation, preflight and related lookups. Same-Barn ambiguity detection is retained. Candidate commit `a6d394e5c189b254c64c8e2c25bdc4fd3e124af3` contains only `src/lib/rs-inputs.js` and `src/lib/rs-tenant-barn.js`, atop the deployed release. Independent read-only review passed. `test-evidence/multi-barn-scope-final.log` passes the actual two-Barn owner regression through two Users, edit/reload, unchanged original memberships and duplicate-membership rejection. `multi-barn-build.log` build passed. Broader targeted regression is still running at this checkpoint.

Browser control detached (`Debugger unattached`). Owner has been asked to reconnect the existing Edge Tenants tab; no browser data was cleared. Continue deployment after the remaining targeted tests pass, then use the retained draft and finish the exact live journey. No live completion claim yet.

17:31:46Z — Correction deployment succeeded (`7ecab5a9-84b7-4f20-a0ff-8fdf25bfbbf2`, commit `a6d394e5c189b254c64c8e2c25bdc4fd3e124af3`). All 11 targeted regression checks passed, plus the corrected alternate-Barn test and Astro build. Same-Barn duplicate rejection remains. Browser recovered using a fresh tab in the same Edge Office profile; original tab/drafts untouched. Fresh deployed form loaded the assigned Barn, prepopulated owner and role choices; the old ambiguous_tenant_mapping state failure is no longer observed. Retained User 1 Retry submitted; saved confirmation still pending at this note.

## Live result at cutoff — 17:37 UTC

The correction is deployed and assigned-Barn User loading succeeds. Retrying the retained first User created People `recP6ms1bbo8tlHxK`, membership `recX6XixdALCDEzdc`, source mapping `rec42vrHeHwVTrDKj`, and committed event `recKsa4GUjYZrQ4ez` for entity `rs_d77bd4367c03a62154e8951f380ddfc7` in assigned Barn `rs_bcf9f1335925df5d13ecd85bd8e87fa5`. Connector readback verified these actual effects.

The UI nevertheless returned “The request could not be completed. Your entry is still here; try again.” No second Retry or User 2 submission was performed. The underlying response/confirmation failure is not established: network capture was not active on that submission, browser logs returned none, and retrieved runtime logs contained no post-17:32 entry. Do not invent a cause or treat committed data as a passing visible workflow. The exact live proof remains unfinished.

Preserved draft: `TEST — Release User 1 2026-10-15 1318`. Current Edge Office handoff tab: `685191416`; original tab/drafts preserved. Screenshot: `.git/ringstatus-control/recognize-tenants-full-review-20261015/multi-barn-live-save-confirmation.png`. No static publishing, automation changes, record repair or new unapproved code correction. Correction cycle reached its time boundary; no automatic extension.

Resume only the failed confirmation path with this existing operation, not a replacement User or new architecture. Required next result: confirmed User 1, then User 2, return, inline-edit User 1, save, reload and canonical readback. Separate post-failure edits/retries remain subject to owner authorization. All prior tests/reviews and both original Barn memberships remain preserved.


## Authorized continuation — 17:39–17:47Z: exact live proof passed

LIVE TWO-USER WORKFLOW VERIFIED 2026-10-15 17:47Z. Existing deployed commit a6d394e5c189b254c64c8e2c25bdc4fd3e124af3. In the same Edge session, User 1 confirmation completed through the existing same-operation Retry (HTTP 200, Saved; preserved recP6ms1bbo8tlHxK without duplicate). User 2 added (recNeqLj91eUI2LDB; HTTP 200 and Saved). Navigated home then /rs-recognize; silently returned to assigned Barn without choosing Barn. Returned to Users, inline-edited User 1 name to 'TEST — Release User 1 2026-10-15 1318 edited'; immediate visible update then Saved. Reloaded page; both Users and edited name visible. Airtable readback confirms exactly these two Release User records and both Enabled memberships linked to Barn recsJkP55Xps5nSL8. No new source change/deployment this continuation: existing deployed correction and WEC-style returned-record confirmation/retry succeeded. Prior failed confirmation cause remains unestablished; do not claim a newly diagnosed/fixed cause. No static publishing, manual record repair, or automation change.

This exact assigned-Barn two-User acceptance is passed; do not recreate/retest it without changed evidence. Separate remaining scope: broader owner/user credential, role and individual opt-in live matrix is not established by this proof. Existing unrelated Barn draft still displays Retry and was preserved untouched; setup/Add Barn removal deferred per owner. No product work restarted.

Evidence: `.git/ringstatus-control/recognize-tenants-full-review-20261015/two-users-live-reload.png` and `two-users-live-proof.json`. User 1 membership `recX6XixdALCDEzdc`; User 2 membership `recEH7IvhTpue8tr9`. WEC reference: `webflow/packing-worksheet/wec-packing.js` (`saveInlineEdit`, `postAction`, returned action-record merge). The current native client preserves the requested edit immediately and marks Saved after server confirmation. No additional implementation was necessary to complete this authorized continuation; earlier failed response cause is not proven.

