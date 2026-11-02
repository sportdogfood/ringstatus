# rs-profile-coordinator — ownership handoff

Date: 2 November 2026. Source: **rs-recognize-coordinator**, chat `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`. Handoff type: a distinct Profile coordinator role, not a continuation of Recognize ownership and not an implementation/deployment assignment. Source model/effort is not independently verified; no model change is requested.

Existing recipient: chat `01a12690-0e9d-71f0-bb85-000d02307e8d`, verified title `rs-profile`, idle at preparation. Reuse that existing owner/route rather than creating a duplicate chat. This packet does not rename it; receiving it does not prove acceptance or source access.

## Startup instruction

Use **$instruction-loader** and **$rebalancer** from RingStatus Operating Skills0.2.9 for this handoff. Read their applicable instructions completely, including coordination and approved-change-boundary references; loading instructions does not resume stopped implementation. Use **$code-review** for an explicitly assigned review and **$handoff** for a later authorized transfer. Keep one existing task record.

Act as **rs-profile-coordinator**. Own Profile and its User/Team responsibilities after Recognize hands over identity. Say things simply in short human language. Give detailed explanations only when requested. Keep code small and maintainable. Preserve the supplied working implementation and current design. Do not replace it, invent records/APIs/schema, stack fixes, create duplicate writers or introduce speculative infrastructure.

**First executable action is read-only:** verify access to the exact isolated source below; read this packet, `docs/profile/README.md`, current owner/repository instructions and the listed acceptance evidence. Confirm the deployed/source baseline and exact Profile-to-Recognize call boundary. Reconcile your existing stopped task and return one concise statement of accepted responsibility plus the specific next unresolved action or missing owner decision. Do not rerun passed tests or business workflows for an intake acknowledgment. Do not begin implementation, alter data or deploy from this packet alone.

## Hard ownership cut

| rs-recognize-coordinator | rs-profile-coordinator |
| --- | --- |
| Cookie/IP identity lookup; phone/PIN/recognition-invitation entry | Profile toolbar, User/Profile panels and Team screens |
| Recognition cookies, devices, sign-out and identity session/history | Canonical User reads/edits and Team/membership/role behavior |
| Recognize People/credential authority and D1 identity/index | Consumer authorization, exact User/Team mappings and Profile save/readback |
| Recognize widget UI and routes | Profile widget UI, data adapter and routes |
| Identity handoff and trust validation helper | Validate/consume identity, then own the private application outcome |

Profile must not change the recognizer to obtain Team or WEF access. A denied Profile/Team operation must not clear identity or retire devices. Recognize does not decide Profile permissions, own Team workflows or carry Profile feature backlog. WEF serving and Messages remain separate owners; this packet does not transfer them to Profile.

## Existing identity contract — preserve, do not invent

- Browser `recognitionAdapter.current()` returns `person_uid`, null, or an unavailable error. It does not currently return a new structured trust envelope.
- Cookie `__Host-rs_recognition_device` is validated server-side by existing `remembered(request, env)`. That server principal includes `identity_only` for IP-based recognition. Browser local state is not private-data authority.
- Profile calls `/test/rs-profile/data` with the cookie and expected `X-RS-Actor-Scope`; its server rejects actor changes and identity-only private access. IP-only identity can request the existing phone/PIN confirmation.
- User matches exactly through `recognition_person_uid`. Own Profile does not require Team membership. Team views/actions use actual record links and their own active membership/Team/role checks; never select the first ambiguous match.
- Current public entry: `https://ringstatus.com/recognize`; credential success or **Open profile** navigates to `https://ringstatus.com/profile`. Silent recognition leaves the visitor on the current page. Embedded recognition inside Profile refreshes that consumer without a separate sign-in model.
- Profile currently reads identity first/last names and requests credential changes through existing Recognize records/helpers. Coordinate changes to that interface with Recognize. Do not move fields, duplicate credentials or introduce a parallel identity provider under this ownership cut.

## Exact source and protected dependencies

Approved main checkout: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`. It contains other owners' changes and older application source; do not assume it matches deployment.

Verified implementation checkout: `C:/Users/gombc/.codex/worktrees/recognize-isolated-20261101/ringstatus`, branch `codex/profile-real-records-20261101`. Deployed source `89abf609f816b6c9d10f05d301027c991a33280a`; local pre-cut HEAD `82fc11b68d0f825bc6908ba84176dc5784aa4a6d` adds documentation only. Read source from this isolated path; a link is not proof the receiver has read access. Owner-approved boundary changes in this handoff affect documents/assignments only.

Profile-owned files relative to `webflow-cloud-test/`:

- `src/assets/rs-profile/client.js`, `profile.css`, `recognize.css` — current consumer UI, including its embedded Recognize presentation copy.
- `src/assets/rs-profile/service.js` — browser data/saves/pending state.
- `src/lib/rs-profile.js` — canonical records, consumer permissions, save/readback.
- `src/pages/rs-profile/client.js.ts`, `data.js`, `index.astro` — module/API/standalone entry.
- Current design is published `/profile`; do not substitute old `/rs-recognize`, unrelated `lp-profile`, seeded demo HTML or a redesign.

Protected shared dependencies include `src/assets/rs-recognition/widget.js`, `native-client.js`, recognition UI modules, `src/lib/rs-recognition-iam.js`, `rs-recognition-action.js`, `rs-recognition-iam-connections.js`, D1 claims/save helpers, `rs-tenant-barn.js` and shared Cloud configuration. They are not freely editable merely because Profile imports them. Identify affected callers, agree one writer and coordinate with Recognize/Tenants/Astro as applicable before changing them.

Existing untracked `webflow-cloud-test/.wrangler/state/v3/d1/` is local generated state; leave it out of commits. No application-source edits are included in this ownership cut.

## Canonical data and existing save behavior

Tenants base `app7BXcw801n19QKF`: User/people `tblyTBgrgdhT3Tcg7`, Team/tenants `tbled1saw01Thffs1`, memberships `tblFf4aWQYA2RRsWo`, roles `tblRqRbjmqmNfJtIP`. Recognize base `app9kOZdIaGyKk5uG`, People `tbly1PM5iFYqVzKSm`, retains identity/credentials. Do not add copies or move sources without approval.

The current Profile implementation has real own-User saves and Owner nonowner-membership Status changes, request IDs/revisions, readback, retained uncertain requests, PIN excluded from persisted draft data, account-change generation fences, and per-User local display-hide preferences. This is not proof of live save acceptance. Its localStorage draft implementation must not be misrepresented as completion of a broader IndexedDB/shared-cache plan in older Tenants guidance; any proposed storage change needs its own approved delta.

Owner uses `___` as a deprecation warning. Do not create dependencies on those tables. The older shared Inputs code still depends on six Recognize-base legacy tables, outside the new Profile data route; do not silently remove them or claim they are retired. Private audit: `deprecated-table-dependencies-20261102.json`. This packet does not assign a migration.

Use the existing Tenants system/task records after verifying their live identities/schema: system `tblMKWJw8Q6joKc8U` / `recJzisHwiPPkKG6y`; tasks `tblXxPquz9jtdQM3n`. Keep only useful evidence/status in the existing records. Do not create extra tasks or tables for every event. Recognize's new `code-reviews`/`errs-log` tables and proposed maintenance schedule do not automatically become Profile's schema or activated workflows.

## Deployment and evidence already obtained

Existing Cloud app `d7d97751-20e1-4148-a5cf-ee58671c128a`, environment `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`, D1 binding `RS_RECOGNITION_CONTROL_DB`. Existing GitHub branch `work/inputs-crm-20261006` auto-deploys on push. Commit `89abf609f816b6c9d10f05d301027c991a33280a` succeeded on 2 November at12:11:26 UTC. This dated receipt is not permission for another push/deployment.

The owner published the five demo-code removals. After that, missing Profile script/data returned404 and Profile was blank. The app-only release resolved that blocker. Codex did not publish the website. Do not resurrect the old missing-route blocker as current, or treat it as authority for a rewrite.

Actual browser acceptance, 2 November: existing secure cookie -> published `/profile` -> real existing test User -> active linked Team/six-member roster; reload retained the correct User. API User, Team and membership IDs independently matched Airtable reads. Recognition/session/Profile responses200; unauthenticated Profile401; standalone app entry/assets200. User `recfl24ktSvsf6R0q`, Team `recsJkP55Xps5nSL8`, membership `recnPR1cvvKp9eqGR`. These were real preexisting test records, not embedded sample objects and not proof of the owner's personal identity. No live Profile/Team edits were made during that verification.

Prior16 scoped backend/service tests, final build, bounded independent source review and local fixture browser checks passed. Preserve their scope: local fixtures used simulated records and are not live persistence proof. The current handoff does not certify a new review under the latest Code Review receipt procedure.

Main-checkout private evidence directory: `.git/ringstatus-control/recognize-model-correction-20261101/`:

- `live-acceptance-20261102.json` — actual live read/handoff acceptance and explicit limits.
- `deployed-http-20261102.json` — route/assets200, unauthenticated data401.
- `source-review.md`, `profile-final-tests.log`, `profile-final-build.log` — prior bounded review/offline evidence.
- `published-check-20261102.json` — historical pre-deployment failure, now resolved.

Screenshot: `C:/Users/gombc/.codex/visualizations/2026/10/10/01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5/recognize-profile-team-live-20261102.jpg`.

## Remaining acceptance and unapproved work

Unverified in the latest release: fresh phone/PIN entry, live own-Profile saves/retries, live Owner membership writes, live IP-only private-access confirmation and mobile acceptance. Coordinate Recognize-side checks; Profile owns its own consumer acceptance. Do not call all controls complete because the read flow works.

Unimplemented: User Independent/Preferred storage, Team creation/invitation persistence, inline file uploads, real modified-date sorting. Older proposals in `profile-real-records-20261101.md` are not approved schemas or requirements. Keep the existing design, report exact unconnected controls and prepare only the smallest needed owner decision. No automatic assignment/migration, guessed roles, fake success or new table/provider/framework is authorized.

Existing rs-profile implementation stop remains until a concrete implementation assignment is authorized. No workflow execution, production-record repair, schema change, push/deployment or website publication is granted by this packet. Codex is not the runner; failed approved runner execution stops that path, with no substitute manual proof. Continue independent authorized work, retain real blockers and do not ask again for already granted in-scope permissions.

## Specialists and coordination

Recognize's internal `widget_ui`, `widget_data_staging` and `widget_astro_publish` agents remain with Recognize and are now Recognize-only. They are not transferred or made permanent Profile owners. A Profile coordinator may use bounded UI/data/Astro specialists only under the applicable authorization; one writer per shared file and one coordinated release. Do not create four new chats merely because three capabilities exist.

Recognize contact: chat `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`. Existing Astro contact: `01a14153-29b7-7a70-a912-cd9db26540c9`. Existing WEF Tenants coordinator: `01a169c2-de92-71e1-9857-084838862ae7`. Verify current owner/reservations and actual messaging authority before action. No new monitor, timer or schedule is created or transferred by this handoff.

## Sources and receipt

Primary has READ this packet's source files, current Profile/Recognize docs, live-acceptance evidence, applicable Handoff guidance and current recipient status. Global/repository owner instructions are SUPPLIED and applicable. Receiver must perform its own READ; source paths do not transfer access. Relevant additional sources: `C:/Users/gombc/.codex/AGENTS.md`, main repository `AGENTS.md`, `C:/Users/gombc/.codex/skills/tenants/SKILL.md`, `docs/coordination/shared-data-behavior-contract.md`, `docs/coordination/domain-coordinator-structure-20261013.md`. Current owner instructions in this packet supersede dated Team-gated recognition, legacy designs and old pending-deployment statements.

Handoff acceptance requires an actual recipient readback of source/access, scope cut, protected behavior, evidence limits and next action. Delivery alone is not acceptance. Preserve this prepared packet if the recipient cannot access it; name the exact missing artifact rather than rebuilding the application.
