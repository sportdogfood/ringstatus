# Tenants live page review — 11 October 2026

Owner reported `/rs-barn-onboarding-setup-v24` did not work and was not gated. Review used the Tenants skill, maintained handoff/workflow/shared-data contract, existing deployed source, historical WEC source `647ea24f8`, actual browser and Airtable readback. No application source changes or new deployment were made in this review.

## Actual observed behavior

- Original user tab `685189890` displayed Example Barn, a request-failed message, prototype text and an unsaved Add barn value `df`. That tab and draft were left untouched. The original failed action was not replayed and its exact cause was not inferred solely from the message.
- A separate normal browser tab `685189934` loaded the existing native asset, `/test/rs-inputs/access`, and `/test/rs-inputs/state`, all HTTP 200. It displayed saved test barn `rs_bcf9f1335925df5d13ecd85bd8e87fa5` and test rider `rs_6c1cf915a82a9922a435968969f1b3c7`.
- Through the existing native Barn form, changed the labelled Test barn name from `TEST — Inputs proof 2026-10-09 edited` to that name plus ` — verified 2026-10-11`. Normal `/record` returned 200. Airtable readback of `rec0k7lui5ZPk9Cn2` showed the exact value and revision 3; normal page reload retained it.
- Restored the original test name through the same native UI save. `/record` returned 200; Airtable readback showed the original name and revision 4. No direct endpoint execution, direct business-record repair, grants, new records or SMS were used.
- Existing rider opened in its inline editor with the stored name. Rider mutation/add was not performed in this review.
- Runtime logs confirm successful state/record operations at 17:09–17:10 UTC. Earlier access failure `recognition_lookup_failed` occurred at 16:38:22 UTC, before the previously approved table-address deployment. This does not establish the cause of every error in the original tab.

## Deployment and destination facts

Current source is `785f571c2167c82c214f0e5c81ffcb77208b70e8`, Webflow deployment `94246ddd-cc67-4590-b1c8-52084ce694b7`. Existing loader is `/test/rs-inputs/native-barn.js`, importing `src/assets/rs-barn-onboarding/native-client.js`. Existing backend remains `src/lib/rs-inputs-airtable.js`.

That store explicitly targets Recognize base `app9kOZdIaGyKk5uG`, including Barn table `tblRvTwo3HYPUkZou` (`___rs-input-barns`) and Rider table `tblnd2ToLs7dTzLAM` (`___rs-input-riders`). It is not a canonical Tenants-base write. The live successful test must not be represented as Tenants-base migration completion.

The saved combined release `C:/Users/gombc/.codex/recognize-tenants-combined-20261010/combined-release-manifest.json` contains Tenants preference/persistence/submission work; it does not replace the native Barn store with canonical Tenants Barn/Rider mappings. Deploying that package alone cannot complete this cutover. Its preferences processor activation is a separate recorded boundary.

## Gate distinction

The recognized browser passes the existing server access check; visiting without another visible login in that browser is expected. Source `rs-inputs-access.js` still validates recognition plus existing Inputs permission. Source `bindMainForms.initialize` catches access failures as form feedback; it does not redirect a signed-out visitor to Recognize. A rendered static shell is not proof of unauthorized data access. No signed-out live acceptance is claimed here.

## Delivery failure and exact remaining work

The domain checkpoint still described obsolete table-address failures after those addresses were corrected elsewhere. Prepared preferences work and table organization did not deliver the expected canonical tenant loop. Preserve the proven native editor and old rows; do not replace the implementation or simply switch a guarded base ID.

1. Finish the agreed signed-out entry behavior only on this designated gated page; preserve recognized access and original user drafts.
2. Complete existing Barn/Rider adapter-to-canonical-Tenants mapping, including actual identity/membership and target fields, before any cutover. Prepared preferences code is insufficient evidence.
3. Release only the reviewed delta under the applicable authorization; verify native add/edit/save/reload against the intended target records and then the locked pending/isolation behavior. Do not mark completion from local tests or legacy-base proof.

Full Tenants end-to-end acceptance remains open. This review does not create a new worker or claim anyone is running. Model/effort are not exposed as verified run metadata here; no causal inference about model settings is made.

## Completion preparation — 11 October, 17:23 UTC onward

Owner instruction: finish Tenants. Existing specialist remains idle at cursor `971b8d27-34c6-4c0e-869a-10db55847e23:51`; this chat owns the preparation, with no parallel writer.

Baseline for the correction is the current deployed `785f571c2167c82c214f0e5c81ffcb77208b70e8`, not the mixed historical checkout. Preserve native markup and existing Barn/Rider add/edit controls. Source of the generated native client is `src/assets/rs-barn-onboarding/barn-native-adapter.mjs` plus `native.ts`; regenerate with the existing `build-native.mjs`. Do not hand-edit generated `native-client.js`.

The exact entry correction is in `bindMainForms.initialize`: handle a confirmed authentication-required response through the existing `/rs-recognize` entry, while retaining useful failure feedback for storage/network errors instead of redirecting those into a login loop. Preserve all existing authenticated behavior and unsaved drafts. Recognition currently lands at `/rs-member`; do not invent a return-to protocol or change Recognize to implement this Tenants correction.

The data connection is substantive implementation, not a base-ID substitution. Reuse `rs-inputs.js` and its established UI/API contract. Current `rs-inputs-airtable.js` requires entity_uid/barn_uid/revision/owner_uid/request fields; canonical `tenants` (`tbled1saw01Thffs1`) and `riders` (`tbllUlTlcEMgZiW3h`) do not have that schema. Existing `sources`, `events`, `input-submissions`, `tenant-memberships`, and `tenant-roster` must be assessed as the already-agreed mapping/processing surfaces before adding fields or tables. Keep Recognize identity storage separate and preserve the working legacy path until the intended connection is verified. Do not activate the preferences-only automation as though it implements Barn/Rider processing.

Fresh readback: legacy labelled test Barn `rec0k7lui5ZPk9Cn2` retains its original name, revision 4, owner_uid `this_sms_test`, and entity UID `rs_bcf9f1335925df5d13ecd85bd8e87fa5`. Tenants People has no row matching that recognition UID or owner UID; sources has no mapping for that Barn or the existing test Rider `rs_6c1cf915a82a9922a435968969f1b3c7`. All 13 current tenant-memberships have blank Status. Therefore no enabled canonical test membership has been established by this read. Do not enable a legacy real-person membership, fabricate a mapping, or bulk migrate records to make a test pass. A labelled test association must be explicitly covered in the correction/live-test scope before it is created.

Concrete verification: unauthenticated designated-page entry; recognized authorized stored-data load; one labelled Barn edit/save/readback/reload and Rider add/save/readback/reload in the intended base; preservation of current identity/public-page behavior; agreed pending/retry/conflict/isolation checks for the changed implementation. A failed workflow path stops under AGENTS; do not substitute direct endpoint execution or data repair.

The owner has been asked once for the required post-failure code-edit and tested-deployment approval. Until answered, no application source, business records, grants, automation activation or deployment is changed. Independent source/schema preparation above is complete. Webflow publishing remains owner-controlled. This entry is preparation, not an implementation or completion claim.

## Approved correction release — 11 October 2026, 17:45 UTC

Owner explicitly approved editing and deploying the tested Tenants correction. The approval above is now answered. This chat is the sole code writer; tenants_mapping_review independently reviewed the correction and final claim/status fix.

Release commit: `bf9a93531209212f303d7e8175af5c9fb01ba2bd`, parent current live `785f571c2167c82c214f0e5c81ffcb77208b70e8`. Only twelve listed code/test files changed; no Recognize implementation, credentials, environment variables, canonical schema, native styling or existing business records were modified. Isolated source and full SHA-256 manifest: `.git/ringstatus-control/tenants-correction-20261011/candidate/webflow-cloud-test` and `release-manifest.json`.

The native adapter redirects confirmed signed-out access to `/rs-recognize`. Native Barn/Rider calls opt into the Tenants bridge; other existing callers retain their provider. Existing input validation, record IDs, native editors and shared IndexedDB helper are reused. Astro admits signed submissions; the Airtable processor applies canonical records, exact source mapping, owner membership and roster, and immutable event evidence. Saved feedback requires actual target/event readback. Legacy rows remain intact. No guessed imported-record merge or activation of existing blank memberships occurs. Unmapped existing names require explicit mapping; only an explicit owned test edit or authenticated new record follows the first-copy path.

The local tests cover native legacy client compatibility, canonical intake/automation/readback, unchanged source records, signed-out redirect versus storage errors, forged envelope rejection, duplicate and stale competing operations, lost completion, historical acknowledgement, offline draft persistence and account-switch isolation. Nineteen focused tests pass. Broader regression ran 266 tests: five old CRM fixture failures also reproduced on the unchanged live baseline; two missing hosting fixtures were restored from that exact baseline and subsequently passed. CRM is not the selected runtime and was not modified. Final Astro build passes. These are local results, not live workflow proof.

New automation draft: `wflEeG5BOUO3vbaW0`, **Tenants Barn and Rider saves**, https://airtable.com/app7BXcw801n19QKF/wflEeG5BOUO3vbaW0 . Connector creation validated and exact script readback matched. It is OFF/undeployed until the owner reviews and enables it in Airtable; the connector cannot perform that last action. Existing preferences automation remains unchanged/off. No direct processor execution substitutes for this trigger.

GitHub push started Webflow deployment `502f91fa-f2eb-4eeb-ba5a-4abac322b8bf` for the exact approved commit. Do not trigger a duplicate deployment. Pending: verify deployment success, obtain owner activation, then test through the actual native form and read back Tenants records/reload. Preserve original user tab draft `df`; use separate verification tab. Live acceptance is not yet claimed.

## Deployed readback — 17:47 UTC

Webflow confirms deployment `502f91fa-f2eb-4eeb-ba5a-4abac322b8bf` succeeded at `2026-10-11T17:47:21.423Z` for commit `bf9a93531209212f303d7e8175af5c9fb01ba2bd`. Live `/test/rs-inputs/native-barn.js` is byte-identical to the tested candidate (SHA-256 `7be0236485e34cfe6ebdb8557beaf517efba97b0f9bd766933a4f26c96b1efa9`). Separate existing test tab reloaded and shows the actual labelled Test Barn, existing Rider option and connected records, with preserved native styling. The original user tab/draft was not touched.

Cookie-free HTTP diagnostics to existing `/test/rs-inputs/access` and `/state` both returned 401 `authentication_required`; these are diagnostic reads, not substitutes for a live signed-out browser journey. No record or processor call was made by this diagnostic.

Required owner action was requested once: review and turn ON `Tenants Barn and Rider saves` at the exact saved automation URL. Tool contract only creates an OFF draft. Do not submit a live test while its processor remains OFF, directly execute the processor, or repair data to manufacture success. Tasks `recjNCFtq6dJB7sk3` and `recwzt7S1umtaMyvj` were updated with concrete deployment evidence and next action; broader unfinished scope remains recorded.

## Owner activation and actual live workflow proof — 11 October 2026, 17:49–17:55 UTC

Owner replied **enabled**. Fresh connector readback confirms automation `wflEeG5BOUO3vbaW0` deploymentStatus `deployed`. The native form in separate Edge verification tab `685189934` was the only business-write initiator; no processor/direct-record execution was substituted.

| Actual form operation | Submission | Canonical target | Observed result |
| --- | --- | --- | --- |
| Edit labelled Test Barn | `recE4uCWOoukYOdjI` | tenants `tbled1saw01Thffs1` / `recsJkP55Xps5nSL8` | Applied; native UI confirmed; name survived reload |
| Add labelled Test Rider | `rec2pTAfDO3DXXfp4` | riders `tbllUlTlcEMgZiW3h` / `recrDDyCPnnJDiNlJ` | Applied; native UI said saved; row survived reload |
| Inline edit same Rider | `recusDJTIB6TyiiqO` | same Rider `recrDDyCPnnJDiNlJ` | Applied; native UI said updated; changed name survived a second reload |

Final Barn name: `TEST — Inputs proof 2026-10-09 edited — Tenants verified`. Final Rider name: `TEST — Tenants live rider 2026-10-11 edited`. Rider roster association is `recOa0laLMUe5OYzl`; Barn owner membership is `recXJlBT4bXHWCRkK`. These are explicitly labelled test records, not real-person record repairs. Connector readback confirmed each Applied result and target name. Original legacy Rider remained visible alongside the new canonical Rider; no old record was deleted. Original user tab and its draft remained untouched.

The pending 409 responses during asynchronous processing were followed by successful target confirmation; no failed live workflow was retried or repaired. Browser tooling occasionally returned an incomplete accessibility tree or stale transition; screenshot/DOM readback resolved those observation limitations without additional saves.

Screenshot: `.git/ringstatus-control/tenants-correction-20261011/live-barn-rider-reload.jpg`. Exact release and local-test evidence remain above. This proves the approved live Barn edit and Rider add/edit/save/reload data path into Tenants through its actual automation.

Acceptance boundaries retained: signed-out API denial was verified live and native redirect locally; a genuinely signed-out live browser navigation has not been independently demonstrated. Shared Edge profile remained recognized. Full multi-device/offline conflict/expiry acceptance and other entity workflows are not implied by these three successful writes. No SMS was sent. Existing preferences automation was not enabled. No Webflow styling/publication or Recognize authentication code was changed in this correction.

## Resume checkpoint — 17:58 UTC

Current accepted live result is the Barn/Rider loop above. No worker or monitor is implied active. Application code is released and unchanged since bf9a935. A separate attempted tab-scoped no-cookie browser diagnostic was unavailable: raw CDP method returned unsupported before a test could execute; the extra test tab was closed and user login was not cleared. Do not claim that attempt as signed-out browser proof. The successful verification tab remains available as the deliverable; original unsaved user draft remains untouched.

Current tasks and domain checkpoint were updated and read back through Airtable. Remaining broad acceptance is not silently closed: genuine signed-out browser navigation, additional entity/provider cutover and live resilience/isolation cases. Next independent reviewer should read this current section plus the release manifest and the existing task Acceptance fields; do not restart discovery, re-enable the already enabled Barn/Rider automation, or duplicate the successful test rows. A failed workflow would require the existing separate-edit rule; no business workflow failed in this live verification.
