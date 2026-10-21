# rec2 Astro Recognize — 16 October 2026

Owner authorization: APPROVED TO EDIT and test Astro Recognize for clean /rec2; single embed including HTML; existing code/records; existing gated /rs-barn-onboarding-setup-v24; no publishing. Original window 18:49:26–18:59:26 UTC. Renewed approval permits local diagnosis/fixes through passing verification within that same deadline. No deployment, static publishing or worker restart performed.

## Exact implementation boundary

Source: C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test.
Baseline: deployed commit 2e75748726490bdd0452ff0da4ab7dd1632e1273. Shared checkout has unrelated dirty changes; NEVER deploy it wholesale.

- src/assets/rs-recognition/embed.js: HTML/styles and minimal presenter only. Exactly one credential input, Continue and existing retry/sign-out actions. Imports the existing native controller rather than replacing credential handling. No prototype records, SMS controls, profile editor, IX3 or local agent-service requirement.
- src/pages/rs-recognition/embed.js.ts: serves that module under /test/rs-recognition/embed.js. Its relative native-client import uses the existing Astro native-client route.
- src/assets/rs-recognition/native-client.js: optional minimal presenter mode; existing credential dispatch and eligibility; confirmed identity cache for immediate return; cache clearing on negative lookup and sign-out. Existing agent event contract preserved.
- src/lib/rs-recognition-native.js and src/pages/rs-inputs/native-recognition.js: existing Cloudflare execution context keeps session/visit tracking alive with waitUntil without holding recognition response. Eligibility lookup still precedes recognition. Hosts without execution context retain awaited summary fallback. No new database, endpoint credential implementation, table, schema or automation.

No changes to the Tenants interface, existing Users, membership, Barn, consent, API-agent service or operational jobs. Normal browser recognition creates its expected device/session records; no people or Barns were created for the live preview. Protected two-User proof not repeated.

## Embed

<div id="rs-rec2"></div>
<script type="module" src="/test/rs-recognition/embed.js"></script>

This URL requires deployment of this tested Astro source before the owner-pasted embed can load normally. No claim it is already deployed. Static Webflow publishing stays owner-controlled.

## Evidence and limits

1. Local real-browser phone/PIN/owner-invite entry through the single field -> actual archived Tenants page markup/current native client -> actual app handlers -> correct assigned fixture Barn. Each also proves cached return without a recognition POST and sign-out/reload. External Airtable/D1 storage is simulated in these tests; not live PIN/invite proof. Existing agent adapter exercises the PIN case through the same controller. No paid model or local agent service was run.
2. Additional existing agent cookie/blocked-storage regressions passed. Older credential-cadence fixture lacked required tenant membership; corrected its fixture initialization to tenantOwner:true, then passed. No eligibility guard weakened.
3. Tracking test holds actual tracking fetches pending: recognized response succeeds before release, then both tracking records complete through captured waitUntil tasks. No dropped fire-and-forget Promise.
4. Astro build passed, existing dynamic [slug].astro warning retained.
5. Real existing owner account: normal visible phone input in an UNPUBLISHED browser preview of the embed -> existing LIVE credential/recognition/access endpoints -> actual LIVE gated page -> actual assigned test Barn visible and state request200. Secure HttpOnly recognition cookie and local cache observed. Return via /rec2 produced zero additional recognition POSTs. No profile/save/User proof repeated. This proves the new browser presentation against current live backend, NOT deployment of the new module or background logging release.

Live preview artifacts in source app: test-evidence/rec2-live-preview.json and rec2-live-gated-mobile.png. Test sources: test/rec2-embed-browser.test.js, test/rec2-audit-background.test.js, test-support/rec2-live-preview.mjs. Initial harness failures retained in chat: collapsed Set-Cookie response headers and PIN/invite attached to wrong fixture table. Corrected harness uses separate cookie headers and Recognize identity fields; unchanged production guards.

## State and next action

Edit/test only; no release authorization. Next permitted release needs explicit owner approval for the isolated five-file Astro delta, followed by actual deployed /rec2 embed acceptance. Existing agent adapter compatibility is proven locally; actual OpenAI model/service availability is not proven and must not become a recognition dependency. Live PIN/invite remain pending; do not label all Recognize V1 DONE.

## Authorized deployment and actual live acceptance — 16 October 2026

Owner authorized deployment and a ten-minute live check, 19:10:45–19:20:45 UTC. This supersedes the earlier local-only release state above. Isolated five-file commit ac50571f1dd66a11e91ed05cbac8060db5752e3d deployed successfully as Webflow Cloud deployment 40c1a217-ca1f-41cf-84f1-8b361ed90542 at 19:13:19.671Z. No static Webflow publishing, service restart, profile edit or protected two-User proof repetition.

Actual public /rec2 in a fresh Edge browser, without mocks, route overrides or injected UI: exactly one input, no legacy profile/recovery/prototype panels; existing owner phone recognized; assigned real test Barn visibly loaded at /rs-barn-onboarding-setup-v24; secure HttpOnly recognition cookie and confirmed local identity cache observed; returning through /rec2 made zero recognition POSTs and silently reopened the same Barn. Evidence: source app test-evidence/rec2-deployed-proof.json and rec2-deployed-entry.png, rec2-deployed-assigned-barn.png.

Sign-out remains under verification; the first test used a button locator and the second a link role locator, neither found the visible control. These are locator timeouts, not evidence of a server retirement failure. Follow-up uses actual visible text and records separately in rec2-deployed-signout-proof.json. No product correction is authorized by a test locator failure. Preserve all earlier passed work; do not call complete until the actual click and cleared recognition are evidenced.

Final live result at 19:20:32.890Z: sign-out click blocked at phone viewport390x844. Exact visible anchor exists (role=button, href=/rs-recognize?sign_out=1), but .rs-barn-v24-header intercepts pointer events. No retirement request was sent. This establishes a pointer obstruction on the existing gated page, not a retirement API failure. No forced click, injected navigation, product edit or bypass attempted. Recognition/assigned Barn and silent return stay passed. Stopped within original deadline; next correction needs separate owner approval under AGENTS.md after failed verification. Follow-up evidence: test-evidence/rec2-deployed-signout-proof.json, failure screenshot rec2-deployed-failure.png.

## Header overlap correction — actual live sign-out PASS, 16 October 2026

Owner separately authorized ONLY header overlap edit/test/deploy and actual sign-out proof; ten-minute window19:33:21–19:43:21 UTC. Exact cause retained above. Correction moves the existing sign-out anchor from root.prepend into the existing .rs-barn-v24-main in source barn-native-view.mjs and served native-client.js; two changed lines. Existing href and retirement controller unchanged. No other product files released. Mobile browser click test with archived actual markup/CSS passed; independent read-only review found no scoped defect; Astro build passed (existing dynamic route warning retained).

Commit a2a36ecc009c19b1051ac04d7a364a9ac81a06c7, parent ac50571f1dd66a11e91ed05cbac8060db5752e3d, deployed successfully19:36:52.710Z as e8024e90-e171-4cd3-a5a1-07b495252c5d to existing Cloud app/environment. No static Webflow publishing, restart, new feature, User/profile write or repetition of two-User proof.

Actual live fresh Edge390x844, no injected UI, forced click, API substitute, route override or mock: existing owner recognized through actual rec2 and assigned Barn loaded as setup for sign-out; actual visible Not you? Sign out is now in .rs-barn-v24-main at y153; normal click sent existing retire_device action and received200/retired=true; secure recognition cookie absent and confirmed identity cache null; rec2 shows its actual single-input sign-in form; reload remains signed out. No page errors. PASS19:37:36.920Z, within original deadline. Evidence: C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test/test-evidence/rec2-overlap-live-proof.json and rec2-overlap-signed-out.png. Release record R/.git/ringstatus-control/rec2-overlap-20261016/release.json.

This supersedes only the earlier mobile sign-out blocker. Prior deployed rec2 recognition, assigned Barn and zero-POST silent-return proof remains valid, preserved without repeating silent return. Original two-User/profile/alerts proofs preserved. Live PIN/owner-invite remain pending; no assertion that every historical V1 workflow passed. No further work started or authorized here.


## Current verified slim IAM / rec2 sign-out checkpoint — 16 October 2026

VERIFIED — 16 October 2026, rec2 recognition and immediate sign-out.
Current release 861452c5bf8722cbe8df76b5a5efa3a6775d0dc6, deployment a790c063-a25a-461f-acf5-11603256ed03 succeeded 2026-10-16T21:43:11.592Z. Parent slim IAM release 2d4e6fb80134a6fc104b30cf852a0c3bf0e88b84, deployment 2b03a964-5a07-4935-a140-45ddc05b0b25 succeeded21:07:41.006Z.
Live existing-owner phone recognition -> assigned Barn -> silent return passed on private Cloudflare D1. Explicit owner-authenticated IAM snapshot load returned200/refreshed:true (source counts: People136, memberships20, Barns14, identities10; not eligible-user counts). Recognition/sign-out paths make zero Airtable calls; Airtable reads occur only on explicit refresh.
Actual visible Not you? Sign out click now opens /rec2?sign_out=1. DOM click event -> visible single-input form measured674ms while retirement request was held. Browser recognition cache cleared before server response. Reload retained same queued retirement event; retry cleared secure cookie; subsequent reload stayed signed out. Captured IAM replies200, cloudflare-d1, AirtableCalls0. Fresh headless Edge at390x844; not a physical mobile-device test. No forced click, fabricated API response or injected UI.
The initial16.7s assertion is retained: later event tracing measured11000ms of automation wait before the actual click, followed by674ms click-to-form. Do not claim initial gated-page loading/actionability is immediate.
Only the two existing Barn sign-out hrefs changed in this final release. Exact diff review and isolated build passed. Original two-User, both-account profile and own-alert proofs were preserved, not repeated or modified. No static publishing, business-record edits, account reload, API-agent restart or SMS. This note documents already completed proof; it does not restart work.
This current checkpoint supersedes conflicting older SMS/OTP, /rs-recognize entry, no-index and undeployed-source statements below. Current agreed entry is ONE input for registered phone/PIN/owner invite; no SMS/OTP. Owners manage associated Users; active phone-populated Users require enabled membership and active Barn; all active Users may manage their own alerts. Client cache controls presentation; protected data uses server authorization.

### Still open before Recognize is DONE

Recognize is not fully DONE. Preserve completed rec2 recognition/assigned Barn/silent-return/sign-out/reload/retry.
1. Establish the existing IAM refresh path after Tenants identity/phone/PIN/status/membership changes. Owner-only refresh action exists, but no automatic change-to-refresh connection or agreed manual operational handoff is verified.
2. Finish nonblocking Sessions/Footprints export from the new IAM audit queue. New IAM stores events in D1 recognize_claims with exported:false; Airtable export/drain is not connected. Historical Sessions writer proof does not prove the new IAM exports.
3. Remaining live credential/eligibility acceptance: live PIN and owner-invite are explicitly deferred; existing regular-User IAM entry, inactive-account/Barn behavior and physical mobile-device acceptance are not established by the current owner/Edge viewport proof. Relevant local fixture checks passed; do not label those live passes or add extra role matrices.
4. Owner-authorized rollout of the tested embed to designated pages. /rec2 remains staging; no static publication or sitewide footer replacement is performed.
API-agent availability/parity remains separately deferred and must not become a dependency for deterministic recognition. No SMS/OTP, recovery, new fields or unrelated V1 expansion added. Next implementation requires a fresh owner scope/approval; no work is running from this documentation update.

### Exact evidence

C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/rec2-signout-link-20261016/acceptance.json
C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/rec2-signout-link-20261016/actual-click-timing.json
C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/rec2-signout-link-20261016/live-timing-and-retry-trace.json
C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/rec2-signout-link-20261016/live-proof.json (initial failed command-time assertion retained)
C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/slim-iam-release-20261016/live-proof.json (recognition/assigned Barn/silent-return and account load)
C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/recognize-rec2-staging-20261016.md

Documentation-only update requested by the owner after the proof. No product code changes, browser tests, deployment, publishing, account refresh or agent restart were performed for this documentation update.

## Verified live connections — 16 October 2026, 8:41 PM EDT
COMPLETE for this scope: Tenants application save -> automatic IAM refresh, and separate background history -> existing Airtable Footprints/Sessions writers. Deployed exact tested seven-file package e9b1c453f196f2d86fe62254577eab916ff24641, deployment3cfb0a61-c490-4b88-92fc-490de6e92209 (success00:37:56.180Z).
Actual Tenants owner-profile name edit/save was recognized by fresh phone sign-in without invoking refresh manually; original value restored and read back. Recognition and sign-out completed with history HTTP requests held; actual click -> rec2 form985ms. Recognition/sign-out replies report Cloudflare D1 and0Airtablecalls. Releasing history and reloading drained the queue. Airtable readback: FootprintrecUSHSBbczeZiaUf and SessionrecfpfJK6GlRcvDLK share sessionbf18664d-42f3-446e-8c31-3f46664719cd and the same person/device links; session is revoked after sign-out. Protected two Users unchanged, prior proofs preserved; no static publishing.
Authoritative evidence: .git/ringstatus-control/iam-connections-20261016/acceptance.json, history-live-proof.json, airtable-readback.json. Initial invalid click timer retained separately; final document-capture timer passed without product changes. Live PIN/invite remain owner-deferred. This scoped completion does not authorize new page rollout or claim untested broader gates.

## Documentation closure — 16 October 2026
Verified connections recorded and independently read back in Recognize system recB69Oe39xZ0nsAe, Tenants system recJzisHwiPPkKG6y, and existing Agents activity rechdzv5o5nsWCA5C. Agents activity links verified Recognize and Tenants base registry rows. Historical proof text and references preserved; current Open issues/Next step supersede obsolete missing-refresh/export claims.
Remaining V1 only: unproved User fields/owner management and regular-User new-IAM/access/visibility plus inactive eligibility; retained failed-save/retry and physical-phone acceptance; separately approved designated-page embed rollout. PIN/owner-invite, API-agent, setup removal remain deferred; other entity links later. Owner Users-tab passed in latest actual profile proof. No runtime/source edit, restart, re-test or deployment in this documentation closure.

## DONE — Recognize rec2 V1, 16 October2026, 9:20PM EDT
Live one-input PIN and owner invite each recognized existing test owner and opened assigned Barn. Invite reuse rejected invalid_invitation. Credential responses200/cloudflare-d1/AirtableCalls0. Original test credential fields restored/read back; restored IAM snapshot loaded. No source changes/publication/new users/messages or protected record changes.
Evidence: .git/ringstatus-control/recognize-pin-invite-20261016/acceptance.json; pin-assigned-barn.png; invite-assigned-barn.png. Current release e9b1c453f196f2d86fe62254577eab916ff24641. Existing phone/silent return/sign-out/IAM refresh/history passes retained. PIN/invite no longer deferred. No remaining correction in approved rec2 delivery. Stop; do not add Tenants checks or additional-page rollout as new blockers. Physical-device hardware testing was not performed; Edge viewport evidence stays accurately scoped.
