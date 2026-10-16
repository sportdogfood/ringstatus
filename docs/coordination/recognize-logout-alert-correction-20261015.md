# Logout / alert Barn-binding correction window

Renewed owner authorization received and clock recorded at 2026-10-15 22:54:14 UTC. Hard deadline 23:04:14 UTC (7:04:14 PM America/New_York). Ten minutes including review, deployment and live verification; no extension.

Logout source correction adds a normal visible `Not you? Sign out` link to the Barn surface and routes it through the existing native `retire_device` implementation before silent recognition. It removes the one-time URL flag and preserves pending drafts. Targeted recognition tests passed; local build passed; independent review found no blocking new source regression. Full legacy recognition test file includes four failures around previously removed SMS/profile behavior; it is not reported as a fully passing suite. The initial Barn contract invocation omitted the Cloudflare loader; subsequent targeted invocation used it.

Alert binding is confirmed missing implementation. Live Airtable read identified automation `wflUVrvlg5XZVzcz6` as `deploymentStatus: deployed`, with no differing deployedVersion. Its OFF/undeployed description and script header are stale; they do not prove inactivity. The returned script independently rejects populated Membership/Barn/User context at lines 224, 239 and 245, matching the local processor contract. It is a separate staged writer, not the excluded API-agent. Correcting local source and Webflow Cloud bridge alone would not update the deployed Airtable script. The available update tool only updates its draft; applying requires the Airtable UI Update action. A coordinated existing-script deployment and compatibility/consent-history verification are required. No half-correction or consent-record mutation has been made.

Logout release commit `ca94a11fbe839dec709b493690b6fc899770cda0` was pushed as a scoped four-file child of `fef5ea561305d3b6634260ef4fcef7b24bcb214e`. Deployment `8442826b-c593-459f-9d71-e27d56c30228` is being checked. The full old Barn contract suite was also attempted: 1 pass / 5 failures in its eligibility fixture (`storage_unavailable`); no full-suite pass is claimed. Targeted recognition checks and the new logout test passed. No backend files changed in this logout release.

Live proof and exact release results will be recorded below before cutoff. Protected Users, memberships, pending edits, API-agent and static Webflow publication remain untouched.

Deployment `8442826b-c593-459f-9d71-e27d56c30228` succeeded at 22:59:20.372Z for exact commit `ca94a11fbe839dec709b493690b6fc899770cda0`. Read-only asset check confirmed the deployed Barn bundle contains the sign-out link. Edge visibly rendered that link; clicking could not yet be confirmed because browser interaction timed out. The same observed page href is being followed through normal browser navigation, not a direct API request.

At approximately 23:01:25Z, following the observed sign-out page link in the in-app browser reached the actual native login state with `Enter your phone number to sign in.` The existing test phone 6313757752 was entered into the native `input[name=identifier]` and its native Continue action was clicked. Prototype duplicate controls were not used. This establishes visible logout-to-login navigation; sign-in outcome is recorded separately.

## Live User result

PASS: phone 6313757752 entered through the native form, Continue, automatic assigned-Barn landing. After loading, the Barn was `TEST — Inputs proof 2026-10-09 edited — Tenants verified — live verified 2026-10-15`. The Users panel contained only `TEST — Tenants User 1 2026-10-15 1014 edited`. Edit my profile opened that same User; Roles and User access controls were disabled. A screenshot was captured in the browser tool. No profile values were changed or saved. An Add user affordance remained visible; no claim that every nonowner UI affordance is correct.

Clicking the new sign-out anchor did not give a confirmed navigation (Edge additionally had CDP timeouts). Following its observed page URL normally did reach native login, and the subsequent User login succeeded. Therefore the retirement route and normal credential UI are verified, but the visible anchor-click behavior is NOT marked passed. No direct API call, cookie manipulation or hidden command was used as proof.

The same sign-out page was opened again and owner phone 6318752160 was entered and Continue clicked at approximately 23:02:40Z; resulting owner access is recorded below.

## Live owner result and final gaps

PASS at approximately 23:03:05Z: fresh owner phone 6318752160 → native Continue → automatic assigned-Barn landing. Users panel showed the existing associated Users; Edit my profile opened `Editing RingStatus Test`, with owner selected and Active access. Screenshot showed the existing owner phone +16318752160. No profile value was edited or saved. The browser was left at the owner profile.

Both existing phones are now verified through the real native sign-in UI, with distinct correct User/owner surfaces. The regular User saw only their own User record; the owner saw their existing management list. No SMS/PIN/invite step was used. The passed two-User creation proof was not repeated and no new Users were created.

Final outcome remains PARTIAL: successful deployed retirement route and both phone sign-ins; visible sign-out anchor click not confirmed; personal alert Barn/membership binding not corrected; new live profile-save and alert-persistence checks not performed. Earlier local proactive-save proof remains local evidence, not new live proof. No additional implementation or deployment started near cutoff. Live PIN/invite remains deferred. No static Webflow publication or API-agent changes.

## Own alerts local correction — 15 October 2026, 10:20–10:30 PM ET

Authorization relayed from owner: edit/test own alerts for existing owner rec09Fqo8EzuNXHa1 and existing User recfl24ktSvsf6R0q only. Start02:20:04Z, hard stop02:30:04Z on16 October UTC. No deployment, publishing, automation update/activation, record creation, alert delivery, schema changes or repeated phone/profile/sign-out proofs.

Baseline remains deployed commit da2c3cba20a140cfe8a22c1b5ec1183a85a56ca1. Worktree: C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus. Unrelated dirty files preserved. The existing personal-preference target is actor-derived; owner_uid and target_uid must still equal the authenticated actor. All active recognized Users retain existing own-alert permission, without a new role rule.

Small compatibility correction: accept existing Barn/User/Membership metadata on that owned Person target instead of rejecting it. Direct writes retain existing barn_uid/user_uid; Membership is omitted from the writable payload and preserved by Airtable PATCH. Staged writes already omit those metadata fields. Existing consent/evidence requirements, engine and activation restrictions, Managed by/Selected favorites guards, submission metadata checks, signed admission/completion and audit ownership checks remain. This does not repair or certify correctness of preexisting linkage metadata, grant another account access, or invent consent.

Exact changed files under webflow-cloud-test:
- src/lib/rs-input-preferences.js — preserve existing Barn/User context on direct preference update.
- src/lib/rs-input-subscription-store.js — permit that existing personal context without changing owner/target checks.
- src/lib/rs-tenant-submissions.js — accept context on the actor-owned staged target.
- scripts/tenant-preferences-processor.cjs — matching target compatibility check in existing processor; no deployed script update performed.
- test-support/recognize-tenants-fixture.mjs — optional actor/phone fixture parameters, original defaults preserved.
- test/tenant-own-alert-context-browser.test.js — four actual rendered local cases, two existing-account contexts times two existing writers.

Observed local browser results: OWNER direct PASS, OWNER staged PASS, USER direct PASS, USER staged PASS. Each used actual rendered alert inputs and normal Save with the first POST held: selected state immediately visible, phone/alert controls enabled while Saving. After release, status reached Saved; reload retained phone, SMS setting and selected Class started choice. Both direct and real staged processor source operated against intercepted local Airtable/D1 stand-ins. Existing metadata and historical audit event remained deep-equal. Caller-supplied context and foreign owner writes were rejected. The User fixture used user rather than owner role. All external browser requests were intercepted; account identifiers are context labels in local fixtures, not live sign-in/persistence proof.

Final new rendered suite:4/4 PASS. Existing admission/receipt regression suite:15/15 PASS. Application build PASS. Independent read-only reviewer separately confirmed direct save/replay and staged processor completion preserve context/history, and foreign owner/target/caller-supplied context remain rejected. An initial new direct fixture omitted mandatory consent_source and its two direct cases timed out; corrected only the synthetic fixture's preexisting evidence, then all four passed. No production guard was relaxed to conceal that failure.

LIVE STATUS: not verified in this window. Known deployed code/processor still reject the relevant existing context; no doomed live save or substitute direct mutation was attempted. Prior passed profile/sign-out proofs remain valid and were not repeated. Protected Users recP6ms1bbo8tlHxK and recNeqLj91eUI2LDB, existing records/consent history, API-agent and PIN/invite checks were untouched.

Exact release prerequisite: owner approval to deploy this scoped server correction to the existing Webflow Cloud environment and update/apply the matching existing Airtable preferences processor wflUVrvlg5XZVzcz6, then verify normal own-alert Save/reload for both existing accounts. Local source edits do not update that deployed automation. No static Webflow publication, new automation or infrastructure is required. Until that coordinated deployment and live UI proof, own alerts is locally tested, not live-passed.

Concrete local package: commit 2e75748726490bdd0452ff0da4ab7dd1632e1273, parent da2c3cba20a140cfe8a22c1b5ec1183a85a56ca1. Six files only, 41 insertions/9 deletions. Alternate index used; ordinary index/HEAD and unrelated changes untouched. Commit has NOT been pushed or deployed.

Independent rendered verification detail: reviewer User/direct PASS; reviewer User/staged timed out at its60-second limit, so that independent rendered case is NOT claimed passed. Reviewer's separate real staged-processor assertions passed. Writer's complete four-case suite passed; one additional focused User/staged rerun passed1/1 in17.3seconds (18.3seconds including runner). No source change was made to hide the timeout. This intermittent independent timeout remains a local test limitation; it is not live workflow evidence.

Completion recorded approximately02:26:43Z, inside original02:30:04Z cutoff. No release action performed. The package is locally tested and ready for a separately authorized coordinated deployment and live proof; it is not a claim that live own-alert saves now work.

## Approved coordinated release — 15 October, 10:39:52 PM ET

Hard deadline02:49:52Z16October. Exact commit2e75748726490bdd0452ff0da4ab7dd1632e1273 pushed to existing deployment branch. Webflow Cloud deployment9d30a7e5-6e0f-4858-b92b-52b6c328da9f building.

Existing automation wflUVrvlg5XZVzcz6: fetched full config before update; preserved name, description, node identity, input mapping, output schema and trigger. Replaced only script with exact git-show source from tested commit. Draft save valid, actionact1rKLnC41xtgm1V. Existing authenticated Edge UI displayed unpublished changes and Update. Clicked Update normally. Connector readback: deploymentStatus=deployed, deploymentError=null, no differing deployedVersion, script exactly equals commit source, trigger unchanged. Thus processor application is verified, not merely draft save. No owner Update click is required. Stale OFF description was deliberately preserved; actual deployed status is authoritative.

Server deployment9d30a7e5-6e0f-4858-b92b-52b6c328da9f succeeded02:42:22.568Z for exact commit2e75748726490bdd0452ff0da4ab7dd1632e1273. Owner normal sign-in opened assigned Barn; own RingStatus Test row Opt-ins -> Edit my opt-ins opened native alerts.

Owner original visible choices: SMS off; Class reminder1 On30minutes; Task reminder1 On10:15; remaining alerts Off (Rides-to-go stored10). Temporary Class started On saved via normal button. Immediate Saving preferences, controls enabled; then Saved. Reload showed Class started checked and enabled after load. Restored Class started Off through normal Save; confirmed Saved; final reload comparison pending. SMS master switch, phone and original other choices were not changed. No alert messages sent.

OWNER live PASS: final normal reload after restoration matched the original captured form inputs exactly (JSON equality), including selected choices/time/phone/SMS master. Existing account switch used actual visible sign-out; no repeat standalone sign-out proof. User verification starting.

User live own-alert workflow: existing User signed in normally; assigned Barn Users panel contained only TEST — Tenants User1. Opt-ins showed no saved opt-ins; Edit my opt-ins opened an empty personal form (SMS off, phone blank, all18 choices Off). Temporary Class started On -> normal Save immediately showed Saving preferences with controls enabled -> Saved. Normal reload retained Class started On after loading. Restored Class started Off -> normal Save -> Saved; final reload comparison pending. No phone/master-SMS value was added and no messages were enabled/sent.

Live automation history corroborates the owner's two staged saves: runwfxovrhNU7xTQgxgM02:43:04–02:43:16Z and runwfx5gx5T4AbBslqlm02:43:35–02:43:45Z, both success on deploymentwfdjgkjBXqZAPqmJf. User with no preexisting opt-in used the existing direct writer; no new automation was created.

USER live PASS: final normal reload after restoration exactly matched the captured original form inputs (JSON equality). Final screenshot showed0of18selected, SMS off, blank phone placeholder. Both owner and User completed normal rendered temporary-choice Save -> Saved -> reload-retained choice -> restore -> Saved -> reload-original comparison. Immediate Saving feedback and enabled controls were observed for each. No direct mutation or uncertain-save replay was used; no failed live save was observed.

FINAL02:46:30Z: approved package deployed; matching existing Airtable processor applied and source-verified; both existing accounts' own alert preference save/reload gates PASS. No remaining owner Update click/action. Finished before02:49:52Z deadline. Scope limitation: both accounts retained SMS master OFF; this proves saving/reloading personal choices, not enabling live SMS consent or delivery. Delivery, PIN/invite, API-agent and unrelated UI remain outside this proof. User's initially absent preference record is now a saved all-off preference with normal audit history; the original form choices were restored, not the absence of a record. No records/audit history were deleted or repaired. Prior independent local browser timeout remains recorded above; it does not replace or erase these separate live observations.
