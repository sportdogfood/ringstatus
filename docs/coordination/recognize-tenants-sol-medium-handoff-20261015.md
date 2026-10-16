# Recognize → Tenants: fresh Sol Medium continuation
Prepared 15 October 2026. Source chat: 01a11632-62fd-73b1-997e-8be4119185de.
Intended receiver: GPT-6.1 Sol, Medium. Setting is a recommendation, not a verified model change.

Owner approved sending this final handoff to 01a14153-29b7-7a70-a912-cd9db26540c9 for read-only loading. Delivery does not authorize code changes, live retries, deployment or operational restarts. Section 2 is the controlling V1 workflow. Historical code and findings below are evidence, not conflicting implementation instructions or proof the revised V1 already works.

## 1. Current assignment and authority — read first

Continue the owner's Recognize → Tenants completion context, not skill inventory, a Rider task, a new architecture or a full transcript reconstruction. The intended product result is an existing registered person recognized without any SMS sending or verification flow, receiving their assigned Barn and permitted profile/individual opt-in access, with reliable immediate editing and confirmed persistence.

**Product work is STOPPED. This packet authorizes no edits, patches, live sign-in retries, record repair, deployment, publishing, worker restart or new agent.** The latest owner requested this handoff only after ordering stop. Older APPROVED TO EDIT messages and expired ten-minute windows do not grant a fresh implementation cycle. Leave the API-agent alone, including its service. Do not start or transfer reminders.

The assignment is to finish the COMPLETE V1 workflow in section 2 using the inspected working references, preserving every already-passed gate. It is not an isolated historical-defect repair, inventory exercise or new design. Section 2 defines the outcome; sections 4–7 provide reference/evidence and cannot narrow or expand that outcome. The receiver already loaded this packet and references: do not repeat startup or the protected two-User proof. After implementation is authorized, reconcile the remaining V1 gates against existing evidence, perform the smallest necessary source-grounded corrections and verify those remaining gates through the actual interface. Resolve implementation details internally; do not ask the owner to restate settled requirements. Until that approval, product work stays stopped.

There is no new timer running. Previous cumulative budgets were exceeded; do not reset those facts. On a future authorized task establish one actual wall-clock deadline (the owner's recent correction windows were ten minutes), including source checks, review, deployment and verification if authorized. No automatic extension or chained fallback. At cutoff preserve the result and exact remaining gate.

**Communication:** brief, factual, independent. No reflexive agreement, lengthy self-analysis, vague outro, “almost” as delivery, or unsupported success. Complete executable authorized work rather than ending at a plan/status. A real stop or authorization boundary remains controlling. No code tutorials unless asked. Tiny business; practical Airtable/manual steps are acceptable; no SaaS rebuild.

## 2. Locked workflow and terminology

- Tenant = existing Barn. RingStatus/admin establishes Barn and owner before access. No public signup or self-created Barn.
- Tenant login may use its established phone, populated PIN or owner invitation. Regular Users cannot use invitations: their only sign-in options are their populated SMS/phone number or populated PIN.
- A User is a person belonging to the Barn, not a show Rider. Do not substitute Rider CRUD for User acceptance.
- Every User must have a populated SMS/phone number, active status, and belong to a tenant in good standing (not inactive). Preserve the tenant membership link. Users may sign in only with that phone number OR their four-digit PIN where populated. PIN is not an exception to the populated-phone requirement. Tenant eligibility applies to returning recognized Users too.
- Known eligible phone alone is sufficient under the owner's chosen workflow. There is no SMS sending, SMS verification, OTP or SMS recovery flow anywhere in this recognition workflow. Here SMS/phone means the stored telephone number, not a message. No PIN follow-up or invite after phone recognition.
- Phone input: ten US digits; existing normalization also accepts eleven beginning with 1. Preserve canonical +1 formatting for downstream alerts; this does not introduce SMS into recognition. Never diagnose the supplied prefix as the cause without evidence.
- PIN: four digits, may collide. Use an eligible known device or associated phone to disambiguate; never first match or suffix match.
- Owner invite: seven digits, unique, active/inactive; preserve existing expiry/one-use rules. Live PIN and owner-invite checks are explicitly deferred by owner.
- Specified pages always attempt silent recognition; cookie and localStorage establish device state using the existing eligibility rules. Reuse valid state instead of repeatedly interrupting the visitor with recognition. Public /, /ring-status, /about-ringstatus remain public for both known and unknown visitors.
- On a specified gated page, an eligible recognized visitor automatically sees that page's permitted content. An unrecognized visitor sees the phone/PIN/owner-invite form, with User eligibility restricting Users to phone or populated PIN. Recognition opens the requested gated content with the Barn already assigned; no manual Barn chooser or extra Continue step. /rs-recognize is a staging page; its existing /rs-member landing is not a requirement to divert every gated visit through a launcher.
- /rs-barn-onboarding-setup-v24 is the Tenants page. Existing setup/Add Barn controls conflict with the final concept, but their removal is explicitly deferred. Do not “clean up” those controls in this task.
- “Not you?” retires the current identity/device association through existing code; never clear unrelated drafts as a shortcut.
- Recognition owns identity, device cookie, session/footprint recording. Preserve intended 365-day cookie, valid-session reuse and eligibility/revocation. Device storage accelerates display; it does not override server access. IP/geo is context, not sufficient authorization.
- Tenants owns Barn, People, membership, roles and canonical personal preferences/opt-ins. Identity is not a membership grant.
- The tenant has its own login and prepopulated Users. The tenant can add and edit all its associated Users, change roles, make Users inactive and override their profile details. This authority is limited to its own tenant. Default is the existing user role.
- Once recognized, a User sees only their own profile and options allowed by their role. They can edit their own name, display-name, profile image, SMS/phone and PIN. They cannot use profile editing to change their own role/status or view other Users. Replacing a phone or PIN must update the existing recognition link so future recognition uses the replacement, without creating another person or tenant.
- Barn holds its name/logo. Owner SMS/PIN belong to the owner's personal profile.
- Requested personal/User attributes: name, display-name, SMS, PIN, profile image, roles, tags and link to that person's own opt-ins. Do not create new schema to restate these.
- Both tenant and Users can opt in to alerts. For V1, all active Users may opt in to their own alerts; no role-specific alert permission mapping is required. Existing phone and tenant-good-standing eligibility still applies. Keep each person's opt-ins tied to that person and tenant context. Tenant authority to edit a User does not imply fabricated consent; preserve existing consent/history. Pending consent is not confirmed, without extra prompts or workflow gates.
- Messages owns /rs-barn-onboarding-alerts (page 6ac459b4506821bd349ed047) and its presentation. Tenants owns saved individual options/consent; Recognize supplies identity and return path. No duplicate consent store or competing writer.
- Every form, add, inline edit and opt-in uses proactive saving: Save/Submit immediately updates the device and visible data and ends the edit interaction. The person can continue using the interface while Airtable updates quietly. No page-wide, form-wide or Users-kind lock, blocking handshake, reload, modal or server-response wait to continue. This is a visible interaction requirement, not a claim that the network write completes instantly.
- Keep the latest local changes on the device across navigation/reload and retain them for retry if the network/save fails. Do not silently erase edits or replace a newer local edit with an older response. Background confirmation reconciles saved data; a pending local change must not be falsely described as Airtable-confirmed. Quiet Saving/Saved/Retry feedback is acceptable; do not require the user to watch or acknowledge it. Airtable remains the saved-data authority.
- Use the supplied WEC/HPS working methods at the applicable boundaries. Alternatives are acceptable only with explicit evidence that they work and improve this exact user experience before replacement; no guessed superiority or speculative layers. An endpoint with webhook fallback is a permitted example to evaluate, not a selected design, required component or authorization to build one. No fallback may block interaction waiting for handshakes. Preserve existing retry safety and access checks without making them a user-facing obstacle.
- Phone-first, quick and simple, for fewer than 100 users. Keep desktop within the same simple/mobile-sized experience where applicable. Practical manual administration is acceptable. No commercial-scale platform, new infrastructure or background machinery merely to appear complete.
- V1 is tenant management of its Users/roles/status, own-profile editing and own alert opt-ins for all active Users, supported by recognition. More-follows, user-more-follows, shows, horses, Riders, rings, locations and all other functionality are outside V1. Preserve their existing data; do not broaden this completion.

## 3. Exact workspace, source and connectors

Approved repository R:
C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus

Correction checkout W:
C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus

Application A:
C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test

This checkout has many modified and untracked files. Do not reset, clean, replace it or deploy its whole directory by assumption. HEAD is older than its working content. The historical recognize-deployed-delta worktree in older skills is NOT the current correction source.

Last evidenced deployed commit:
0dec58fb39ab35cab5c2a4fd830783dab0257f00
Deployment: 4d6e4f84-f8d0-4157-8673-8fc539a10462
Deployed: 2026-10-15T19:31:43.842Z.
This is the last verified release, not an assertion no other actor could subsequently change it.

During packet preparation the tracked native-client.js, rs-recognition-native.js and rs-tenant-barn.js compared without differences against that release. rs-recognition-credential.js is UNTRACKED in this checkout's older index: git diff against the release misleadingly reports deletion. The file EXISTS and its actual text was compared with the release Git object and matched (normalized line endings). Inspect actual bytes for untracked files; don't “restore” a nonexistent deletion.

Primary source paths under A:
- src/assets/rs-recognition/native-client.js
- src/pages/rs-inputs/native-recognition.js
- src/lib/rs-recognition-native.js
- src/lib/rs-recognition-credential.js
- src/lib/rs-inputs-recognition.js
- src/lib/rs-inputs-access.js
- src/lib/rs-tenant-recognition-policy.js
- src/assets/rs-barn-onboarding/native-client.js
- src/assets/rs-barn-onboarding/barn-native-adapter.mjs
- src/assets/rs-barn-onboarding/barn-native-view.mjs
- src/components/rs-onboarding/api.ts
- src/assets/rs-inputs/persistence.js
- src/lib/rs-tenant-barn.js
- src/lib/rs-tenant-save-operation.js
- src/lib/rs-inputs.js
- src/pages/rs-inputs/[operation].js
- src/lib/rs-tenant-profile-fields.js
Only inspect affected paths; this list is not authority to edit all of them.

Connections:
- Airtable connector preferred to browser. Agents appZahVgD156cMAe3; Recognize app9kOZdIaGyKk5uG; Tenants app7BXcw801n19QKF.
- Tenants stable table IDs: Barn tbled1saw01Thffs1; People/Users tblyTBgrgdhT3Tcg7; memberships tblFf4aWQYA2RRsWo; roles tblRqRbjmqmNfJtIP; sources tblRswgKjA9dt3PED; events tbluecPFIorOytASP; submissions tblldMTDEC697hPwE; opt-ins tblCX61u6pjSLrP5S. Read schema names when needed; don't rename or infer them.
- Astro application served under /test; Webflow static hosting/front-facing native presentation. Cloudflare proxy/D1 exist. Reuse them; do not introduce another controller, index, database or automation. An alternative transport/fallback requires the section 2 proof and replacement conditions, plus implementation authorization; it is not a preset design.
- Webflow MCP server webflow; site 6982268b7543ac3c80151266. Guide once before actual Webflow operations. No static publishing, OAuth reset or site rediscovery.
- Edge actual visible UI is required for authorized live acceptance. Connector readback alone does not prove visible workflow. Load computer-use skill before control; no assumed inherited browser/session access.
- Twilio infrastructure may exist elsewhere but is outside this recognition workflow. No SMS sends, verification, console reconfiguration or automation changes.
- API-agent is a separately proven experiment, currently excluded. Its local service being down must not block this native workflow.

Read current instructions before dependent work:
C:/Users/gombc/.codex/AGENTS.md
R/AGENTS.md
Applicable instruction chain inside W/A.
The global rules and owner's latest stop override stale dated imperatives. Codex is not the business runner. No alternate endpoint/manual record repair as workflow proof.

## 4. Working WEC/HPS — actual behavior, not slogans

Both reference commits are available as local Git objects. Read them using git show, without checkout/reset. Current files at the same paths may have evolved.

### WEC baseline 647ea24f8

Exact files relative to R or W Git repository:
webflow/packing-worksheet/wec-packing.js
webflow-cloud-test/src/pages/wec-packing/action.js
webflow-cloud-test/src/lib/wec-packing.js
Context: R/docs/wec_packing_comprehensive_overview_2026-06-06.md.

The inline save collects only changed item fields. Its actual core (client lines 579–598):
~~~js
const pendingKey = pendingActionKey("update_item_fields", itemId);
if (state.pendingActions[pendingKey]) return;
const snapshot = snapshotItemInlineState(itemId);
state.pendingActions[pendingKey] = "save";
applyOptimisticItemFieldUpdate(itemId, fields);
clearItemInlineEdit(itemId);
state.saveMessage = "Saving item...";
render();
await postAction({
  action: "update_item_fields",
  itemId,
  effectiveNeeded: item.needed,
  ...actionItemPayload(item),
  fields
}, null, {
  pendingKey,
  message: "Saving item...",
  quietStart: true,
  rollback: () => restoreItemInlineState(itemId, snapshot)
});
~~~
postAction (about lines 897–933) POSTs JSON, requires HTTP success AND result.ok, installs returned state when provided, clears only the matching failed action, reports Saved; on error rolls back and queues the failed payload; finally releases that action/item pending key and renders preserving position.

Failed actions are best-effort localStorage, fingerprinted and bounded (about 1105–1155). Storage is not a required precondition for initial POST. Retry occurs on online, after load, and through Retry (58,79,936–989). WEC session_start is fire-and-forget (101–117), not an authentication proof.

**Important correction:** June WEC actionReport (1844–1893) calls stateReport after inline update. applyItemFieldUpdate (3692–3754) creates a packing ledger entry and event; it is NOT a universal single PATCH. createPackingLedgerEntry is at 3528–3560. mergeReturnedActionRecord (client1048–1065) handles COMMENTS, not every edited item.
Later WEC at 0dec skips returned state for add_quantity/set_pack_state/set_horse_pack_state/set_horse_kit_state, NOT update_item_fields.

### HPS baseline 05854306ba53b0130d8f3f972fc9907554616aec

Exact files:
webflow/hps/hps.js
webflow-cloud-test/src/pages/hps/horses.js

Owner's example tenantId8778; browser config endpoint /test/hps/horses.
Actual saveRecordChange (client649–678) sends:
~~~js
{
  horseRecordId: record.id,
  horseKey: change.horseKey,
  horseName: change.horseName,
  fieldName: change.fieldName,
  oldValue: change.oldValue,
  newValue: change.newValue,
  tenantId,
  source: "hps"
}
~~~
Then requires response.ok AND result.ok and uses:
~~~js
record.fields[change.fieldName] =
  result.updated?.value ?? change.newValue;
~~~
Server112–137 validates, checks active tenant and horse membership, resolves schema, updates horse, creates change log, then returns updated/log. updateHorseRecord307–338 PATCHes the actual horse field and may update linked WEC records. This is the specific returned-value confirmation reference. It is not universally one request and tenantId alone is not access proof.

**Use exactly the relevant proven boundary:** WEC per-item immediate feedback/retained retry; HPS direct update and returned-field confirmation. Do not copy packing-specific ledger semantics into people, weaken access, or call a generic replacement “WEC-based” because it uses fetch. WEC/HPS are editing references, NOT phone authentication implementations. Existing credential code and the owner's rules govern recognition.

Historical WEC rollback and pending-key behavior above describe the reference accurately; they are not instructions to discard the owner's latest local edits or make the interface wait. Section 2's proactive-save contract governs the adaptation. HPS waiting internally for its response is not permission to block the interface. Prove both immediate continued interaction and eventual background persistence, rather than citing either example as blanket UX proof.

The owner's supplied manual WEC session embed POSTed action start-session with timestamp session/device IDs, show14906 and focus2026-06-14 to /test/wec-schedule/edit. It demonstrates that test logging route only, not identity, persistent recognition or Tenants access.

## 5. What passed, what failed and protected state

Evidence E:
R/.git/ringstatus-control/recognize-tenants-full-review-20261015/

| Gate | Evidence and exact limits |
|---|---|
| Assigned Barn → add User1 → add User2 → return → inline-edit User1 → Saved → reload | PASSED in same Edge session at17:47Z; two-users-live-proof.json. Release a6d394e5c189b254c64c8e2c25bdc4fd3e124af3. User1 confirmation used existing same-operation Retry without duplicate; User2 Saved; home→Recognize silently returned to assigned Barn; both visible after reload and canonical readback. DO NOT repeat. |
| Owner Edit my profile, changed display-name, confirmed save/reload | PASSED at19:34Z on0dec; save-path-proof.json. Fresh save14.036384s; retained confirmation retry2.824104s. Immediate display does not prove no interaction lag. |
| Owner phone normalized and reloaded | PASSED20:26Z; remaining-live-phone-20261015.json; owner rec09Fqo8EzuNXHa1, two memberships preserved. |
| Existing separate test User's phone saved and linked | PASSED write/readback20:34Z; nonowner-live-check-20261015.json. This is NOT sign-in proof. |
| Fresh nonowner phone sign-in | FAILED owner test: “no match found”, subsequently SMS verification error. Loaded recovery→SMS binding and runtime sms mode established; earlier no-match/403 exact causes unknown. |
| User own-profile and replacement credentials, isolation from other Users, own alert opt-ins for all active Users, inactive User/tenant denial | NOT proved live; remain required for the revised V1. |
| Tenant management/override of its Users, roles/status, profile-image and remaining V1 fields | NOT established by the above limited proofs. Barn-logo editing is not added as a V1 gate. Use existing supplied records/assets; no invention. |
| Proactive save across forms/add/inline edit/opt-ins | Immediate display was observed on some earlier saves, but continued interaction, local retention and background reconciliation across this entire V1 are not established by those results. |
| PIN and owner invitation live gates | Explicitly deferred. Do not solicit credentials or consume invite. |
| API-agent parity | Excluded by latest owner; no service restart. |

Protected Barn: recsJkP55Xps5nSL8.
Protected passed pair:
User1 recP6ms1bbo8tlHxK / membership recX6XixdALCDEzdc.
User2 recNeqLj91eUI2LDB / membership recEH7IvhTpue8tr9.
Do not create replacements, change them or rerun their proof. Preserve unrelated existing Barn Retry draft and all consent/history/pending receipts.

Separate test User selected by owner:
Tenants Person recfl24ktSvsf6R0q.
Membership recnPR1cvvKp9eqGR, Enabled, Barn above.
Recognize Person recNt8WzPrOhDKgUs.
Tenants entity UID rs_c72e974694fb92e183a7dd387432eb22.
Recognize UID tenant_rs_c72e974694fb92e183a7dd387432eb22.
Phone owner supplied: 16313757752; canonical +16313757752; ten-digit6313757752.
Existing name TEST — Tenants User1 2026-10-15 1014 edited.
Person Active; corresponding Recognize identity Active/input_access approved/access_level member at readback.
Earlier membership role list was empty; reread before diagnosing eligibility. No individual opt-in existed at precheck. **Do not repair roles/records manually to make a test pass.** Empty role observation is not the established cause of no-match.

Old evidence files may retain historical pending descriptions; current failure above supersedes their last pending wording. A later write/readback does not establish fresh sign-in.

## 6. Historical native failure evidence — not a separate assignment

Live read-only Edge inspection17:04–17:11EDT:
- InPrivate /rs-recognize runtime: stage entry, busy false, entryMode sms.
- Actual “Find my profile” DOM control bound to data-rs-native-action=recovery.
- Actual loaded listener matches native-client.js365:
~~~js
else if (command === "login" || command === "recovery") {
  choose(command === "recovery" ? "sms" : "phone");
}
~~~
- phone-login line371:
~~~js
if (entryMode === "sms" || otpPhone) await verifyPhone();
else await submitCredential(values("login").identifier || "", values("login").phone);
~~~
verifyPhone291–315 uses otp_start/otp_check and emits the owner's exact error:
“Verification could not finish. Request a new SMS code.”
Console reported403 at /test/rs-inputs/native-recognition?operation=action; response body and historical click chain unavailable.
Default entryMode is phone (140). submitCredential180–192 uses action credential and does not automatically select SMS on unknown.
Separate retained server path rs-recognition-native.js141–144 falls back from phone_login401 to SMS recovery.

**Established departure:** a visible profile-finding control selects the historical SMS flow instead of the registered-phone path. **Not established:** which historical click selected sms, why the earlier credential lookup was no-match, or precise reason for403. Don't blame phone formatting, Twilio, stale deployment or API-agent without evidence.

### Applying evidence within the full V1 assignment

The observations above identify behavior that conflicts with V1; they do not assign a separate investigation or require the owner to discuss legacy features again. Use the existing source and recorded evidence only where necessary to complete a remaining V1 gate. Unknown historical causes remain unknown until proven; do not make reproducing every old failure a prerequisite for delivery. No additional implementation authority comes from this evidence.

### Once the complete V1 correction/testing scope is expressly authorized

Use existing fixtures and actual visible control bindings; don't substitute internal app.add/openSheet calls for clicks. Verify registered phone takes credential path without otp_start/otp_check, returns correct assigned Barn, enforces active User, populated phone, tenant in good standing (not inactive), membership and role-scoped options, and preserves existing owner silent return behavior. Independent review must cover these actual gates, not only changed lines.

When live testing is authorized, use the existing separate User above in an isolated browser identity, not the protected pair. Prove assigned Barn, own-profile save/reload, no visibility of other Users, role-scoped options, own opt-in for active Users save/reload/opt-out and denial for inactive User or inactive tenant within permission granted. Prove tenant edit/override and role/status management of its associated Users without repeating the passed two-User creation proof. A phone/PIN replacement must stay linked to the same person; PIN live checks remain deferred. Do not conflate canonical write with confirmation. Inspect existing test assets for image fields; do not invent URLs or alter production people. Leave owner-invite checks deferred.

For all affected form/add/inline/opt-in paths, verify that Save/Submit updates the device and visible data immediately and leaves the interface usable before the response returns. Use controlled delayed/failed responses for local testing, retain pending edits across reload and retry without duplicates, and ensure an older response cannot overwrite a newer local edit. Background Airtable readback verifies persistence separately. Do not repeat valid unchanged CRUD proof just to satisfy this different interaction check. No forced production outage or unapproved live manipulation.

For any remaining save problem, compare the affected action with WEC/HPS and the current proactive-save contract before edits. Measure both visible interaction and request phases, not elapsed guesses. Existing journals/pending operations must be accounted for before changing coordination. No guessed parallel function, blanket rewrite or new automation. An alternative requires demonstrated improvement and applicable replacement authorization, not merely the owner's example of a possible webhook fallback. The earlier exact504 cause is still unproven; reduced read counts do not establish it.

Completion can only cover observed gates; deferred PIN/invite and excluded API-agent must stay explicitly outside any release acceptance claim. No full Recognize/Tenants PASS from the two-user CRUD proof.

## 7. Current departures and historical corrections — do not repeat assumptions

The source-verified complaint is Agents complaints recIzrud23on6Rkid (Open).
1. Generic full-draft/nested-dialog editor instead of WEC item-field inline handling.
2. Exact pageID + five forms/tabs/templates required together, allowing unrelated missing markup to abort.
3. Full draft/context/revision/request ID instead of WEC changed fields.
4. Current pendingKinds locks other edits of the same kind; Users form inert. Earlier page-wide lock was corrected.
5. Retained optimistic state and same-target unresolved-request blocking differs from WEC rollback+queue. Persistent dirty state itself was approved.
6. IndexedDB prepareIndependent gates initial POST; WEC best-effort queue does not.
7. Manual draft retry and older12×2s tenant_save_pending polling differ from WEC online/load retry.
8. Legacy Recognize input records merged with canonical Tenants through mapping/events; Horses/Locations still legacy.
9. Committed event JSON, revision and ownerUID needed to reconstruct mapped rows, beyond canonical record.
10. Mandatory D1 recognize_claims save journal/busy lease/step recovery absent from reference save handlers.
11. Repeated preflight, actor/access checks and write readbacks amplify requests. Keep required checks; avoid guessed deletion.
12. Cache clearing and225ms request pacing compound work, not proof of exact504 cause.
13. Duplicate-name/revision/mapping restrictions, including whole canonical-table new-name check beyond Barn.
14. Selective changed-record response resembles HPS; not universal WEC inline response.
15. Setup/AddBarn/chooser remains, removal deferred.
16. Native credential/legacyphone_login/recovery/SMS paths overlap; actual binding defect above.
17. Retained Recognize lookup awaits session log, unlike WEC fire-and-forget log; not proof every silent path uses it.

These are comparisons, not17 newly authorized repairs. Some safeguards serve explicit identity, role, actor-isolation and retry requirements absent from packing. Do not delete them because packing doesn't have them.
Direct Astro→Airtable replaced mandatory automation for new Barn/Rider/User writes. Earlier browser15s cutoff, page-wide lock, Rider opener and device-cleanup-overrides-server-success defects have corrections. Do not report all historical failures as current.

The coordinator's false claims must not propagate:
- “Uses WEC/HPS” wasn't justified by optimism + fetch alone.
- WEC inline does rebuild state; returned-record merger is comments-only.
- WEC is not universally a single PATCH; HPS may also update linked records/log.
- Immediate display ≠ ability to keep interacting.
- Two-user CRUD ≠ authentication/access/individual consent proof.
- More tests or model effort do not prove workflow fidelity.

## 8. Skills and exact access procedure

Startup explicitly invokes Instruction Loader in the copyable prompt. Read its full applicable procedure; headers/catalog entries are not loaded/applied skills.
Required startup:
C:/Users/gombc/.agents/skills/instruction-loader/SKILL.md
C:/Users/gombc/.codex/skills/recognize/SKILL.md
C:/Users/gombc/.codex/skills/tenants/SKILL.md

For source defect diagnosis:
C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/superpowers/6.4.2/skills/systematic-debugging/SKILL.md
For authorized browser acceptance:
C:/Users/gombc/.codex/plugins/cache/openai-bundled/computer-use/26.1007.71326/skills/computer-use/SKILL.md
C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/build-web-apps/0.1.2/skills/frontend-testing-debugging/SKILL.md
For completion:
C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/superpowers/6.4.2/skills/verification-before-completion/SKILL.md
For Airtable:
C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/airtable/7.0.0/skills/airtable-overview/SKILL.md
For independently authorized release:
C:/Users/gombc/.agents/skills/webflow-cli-cloud/SKILL.md
For native saved-code changes, only if actually in scope:
C:/Users/gombc/.agents/skills/webflow-mcp-custom-code-management/SKILL.md
For review use available dedicated code-review capability; discover its actual schema, do not claim review merely from loading a skill.

Load only the skill needed before its operation. Verify installed path/version; if missing locate the current catalog equivalent, never pretend loaded. Do not read every plugin, the entire transcript or all bases.
Recognize/Tenants skills contain older paths and broader historical acceptance (Riders/API-agent). This packet's current scope supersedes them. Their text authorizes no new agents, edits or release.
Repository SHELL_CONTRACT.md applies if UI-shell changes later become authorized; none are requested here.

READ during preparation: Handoff skill, Instruction Loader procedure for handoff design, Recognize/Tenants skills, global instructions, supplied repository instructions, complaints and source/evidence above. This is not a receiver loading receipt. Receiver records READ/APPLIED/VERIFIED separately and only as actually demonstrated.

## 9. Failed-handoff lessons actually carried here

Reviewed Agents complaints inventory112records, then relevant handoff/continuity/preparation complaints and the15-requirement audit in R/docs/coordination/rs-skills-continuation-review-20261014.md. These are observable preparation failures, not a theory that Astra/High or missing Loader alone caused everything.

| Complaint | Actual failure | Concrete protection in this packet |
|---|---|---|
| recJqs9oRcyGDh2Hj | Same-role continuation narrowed to skill-category reconciliation; business context stayed untested prose. Later latest corrections missing; inventory assigned again despite tests. | Sections1–2 assign current product continuity, with locked rules embedded; section6 is historical evidence only. No inventory trial. |
| Same record | Source blamed missing Loader alone although many instructions were present; source prepared dispatch before full task audit. | Explicit Loader invocation plus actual workflow, versions, failures and authority; no claim prompt or reading guarantees judgment. |
| recmPExORDmWA2XHN | Generic skills/connections and structural review substituted for content review; vague closing “review” created exchanges. | Exact paths, table IDs, sources, gates and approval boundary; clear short final. |
| rec8jIV2bAgsr8FkZ | Prepared handoff presented without expected dispatch; task lost across topics. | This task expressly requests preparation/no restart. State prepared-only; no fictional assigned/running worker. |
| reckAbo65bjkXBDnr | Coding phase lacked task-specific skills/reference assessment; local tests obscured live failures; wrong destination discovered late. | Source-level WEC/HPS behavior and limits; the entire V1 is the target; scoped live evidence ledger. |
| rec3y5PHaTnNz9wTw | Local/cloud skill drift and false hook-enforcement claims. | Installed-path checking, current owner authority, no hooks or enforcement claims, stale-path caveat. |
| recCYMCEcyO6c48UO | Excessive recovery schema and incomplete native journey; SMS/local proof mistaken for intended path. | No new schema or SMS flow; known-phone path and exact remaining gates. Historical SMS requirements do not control current flow. |
| recLS59uk1DRZXlE0 / rec3gqVsndMTgtAYt | Automation retained after WEC/HPS direction; original cumulative deadline lost across fix cycles. | Direct-save baseline and real one-deadline rule; no approval-as-clock-reset. |
| recD00TQ93grfmVyB | Tested Riders instead of two Users. | User definition and exact protected User IDs/proof; no Rider acceptance task. |
| recIzrud23on6Rkid | Patch layering, false WEC assertions, failed fresh User recognition. | Sections4–7 preserve actual source comparison, corrected claims and unproven causes; no rewrite permission. |
| recZASoWsfhOkvJh2; recn0fWRYlFTvOsDT; rectkSUsNnGS1I1pZ; C062/recyb58Ku08tGI6FT | Fixable work stopped at explanation/plan; scoped blocker generalized; background progress implied. | Continue executable work only when authorized; preserve stop now; no background implication or technical owner homework. |

No complaint is closed by this document. No new agent has been tested. Handoff usefulness is ultimately demonstrated by the receiver applying these facts to the actual next task without owner re-education, not by another inventory exercise.

## 10. Receiver startup and acceptance of the transfer

Read this document first, then Instruction Loader/current instructions and relevant source excerpts only. Do not restart stopped work.
A concise startup receipt must identify:
- the assignment is the complete section 2 V1 workflow; historical defects are supporting evidence, not a replacement task;
- the two-User proof/records are protected and already passed;
- WEC inline response rebuilds state; HPS confirms returned field;
- V1 tenant manages/overrides its Users; User sees only own profile and role-permitted options; all active Users can opt in to their own alerts;
- every save updates local display immediately and lets the person continue while Airtable updates quietly; no server-wait UX or speculative infrastructure for fewer than100 users;
- User ≠ Rider; API-agent/PIN/invite/follows are excluded/deferred;
- current owner stop means read/preparation only until explicit restart/correction authority.

Then follow the owner's actual next request, not a default backlog item. If asked to implement with approval, proceed within that precise scope through testing/review instead of asking the owner for already-settled rules. If only asked to load, keep product stopped. Missing access blocks only dependent actions; do not invent an alternate production path.

Relevant source documents, retrieve only when needed:
R/docs/coordination/recognize-tenants-review-corrections-20261015.md
R/docs/coordination/tenants-cause-audit-20261015.md
R/docs/coordination/recognize-tenants-access-rules-20261015.md
R/docs/coordination/recognize-tenants-messages-ownership-20261014.md
R/docs/coordination/shared-data-behavior-contract.md
R/docs/coordination/rs-skills-continuation-review-20261014.md
Older tenants-proven-ui-reference-plan-20261014.md automation imperatives are superseded. Dated sources are evidence, not competing current instructions.

### Copyable first message for the fresh chat

Use Instruction Loader at C:/Users/gombc/.agents/skills/instruction-loader/SKILL.md. Read C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/recognize-tenants-sol-medium-handoff-20261015.md and apply section 2 as the current Recognize → Tenants V1 workflow. Use GPT-6.1 Sol Medium as selected in the app. Silent recognition; no SMS sending/OTP; tenant manages all its associated Users; active Users with populated phones and active tenants see only their own profile and role-permitted options, with own alert opt-ins available to all active Users. All forms/adds/inline edits update local data immediately and keep the interface usable while saving quietly to Airtable. Fewer than100 users; proven WEC/HPS methods, no guessed replacement. Do not reload the old transcript, repeat the completed source loading or restart inventory. Preserve the passed two-User proof. Leave the API-agent alone and live PIN/invite checks deferred. Load applicable instructions and verify cited sources; retain the complete V1 assignment and identify only its remaining unproved gates without repeating passed work or asking me to restate the workflow. Product work remains stopped: no edits, patches, live retries, deployments, publishing or restarts until I authorize the correction.

