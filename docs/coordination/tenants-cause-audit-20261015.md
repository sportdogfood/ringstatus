# Tenants cause review — 15 October 2026

Owner: identify why delivery failed, correct instructions, document lessons, and establish a clear resolution path within ten minutes. Review started 08:13:14 America/New_York; deadline 08:23:14. The conditional fallback is fifteen minutes proving one assigned-Barn form, create two Users and return/edit one; only if that fails, thirty minutes for a separate HPS-based function. A clear existing-code correction path means do not automatically start either replacement experiment.

## Evidence and causes

### 1. Rider dialog — two entrypoints, one unmanaged

Current public HTML captured read-only at `.git/ringstatus-control/tenants-cause-audit-20261015/live-page.html`. Two `button.rs-barn-v24-link` elements open `rs-barn-v24-sheet-rider`: a section-level opener and the Horse form relationship-field opener. `barn-native-view.mjs:159–160` binds the relationship-field opener only; the main `.rs-barn-v24-add-button` at line147 invokes the main editor instead. The section-level native opener can display the browser popover without invoking `openSheet`.

`openSheet` lines106–112 alone pushes the active frame. `saveSheet` line115 returns without that frame. `closeSheets` line104 hides only popovers listed in frames, while its click wrapper line66 prevents the native close action. This explains the recorded combination of visible dialog, no save request and Close doing nothing. It is an entrypoint/state mismatch, not evidence that Rider data cannot save. The main form already saved a Rider.

Verification failure: `test/tenants-immediate-browser.test.js` loads October10 HTML and calls `page.evaluate(()=>app.add('riders'))` instead of clicking the actual visible Add Rider button. That bypasses the broken opener. The existing passing test never exercised this failed user action.

Smallest correction: bind the actual section-level Rider opener to the existing `openSheet('riders')` method; retain the same controller and save path. Test that visible opener, Close without save, reopen, save and reload using current markup. Do not replace the form framework or add another sheet mechanism.

### 2. Save latency — measured redundant upstream work

Live evidence `.git/ringstatus-control/tenants-confirmation-20261014/live-acceptance-final.json` records a single successful Rider POST taking18.934seconds. It has no per-upstream-request production timings, so no exact breakdown of those18.934seconds is claimed.

An offline diagnostic reused the existing correction fixture and actual `createTenantRest`/`createTenantBarnBridge`. All storage and identity fetches were intercepted by in-memory fixtures; no external call or business write occurred. A new Rider under an already assigned Barn generated **46 canonical Airtable requests plus five identity reads**, taking **10.722seconds with instant mocked storage**. The adapter's225ms spacing between upstream starts accounts for substantial artificial duration before real network latency. This is a controlled reproduction of request amplification, not a production profile or a promise of a particular optimized duration.

Probe and full request list: `.git/ringstatus-control/tenants-cause-audit-20261015/read-count-probe.mjs`, `read-count-result.json`.

Counts: sources13GET, events5GET, people5GET, memberships5GET, roles5GET, Riders4GET, Barn2GET, roster2GET, old submissions1GET; fourPOSTs create Rider, roster, source mapping and event. Identity reads are separately counted.

Causes in `src/lib/rs-tenant-barn.js`: all cached tables cleared on every fresh read and write (20,22); preflight performed twice (210 and157); actor checked repeatedly; each write immediately read back (113–117); mapped lists reconstruct each record from events and canonical rows (63–72). Staging runs `saveInputRecord` and discards its full returned state (192; shared implementation `rs-inputs.js:106`); final confirmation builds all five collections again (109; `rs-inputs.js:22–25`). Some work protects access/concurrency and cannot simply be deleted; repeated whole-state reconstruction is not required by the owner.

Smallest sequence: remove the discarded staging response construction while preserving validation; measure request counts again; then return the verified changed record and merge it into the existing client state, using HPS/WEC selective response behavior rather than rebuilding every collection for a one-record save. Evaluate narrower cache invalidation only with tests proving fresh permissions, mappings and readbacks still hold. Keep Airtable rate pacing; removing rate protection is not the solution. Preserve stable operation/receipt and pending changes. Do not claim all production latency is solved by extending a timeout.

### 2a. Owner correction: measure user wait separately from server completion

The immediate optimistic row is proved; that alone does not establish fluid interaction. Tenants adapter `run` sets one global `pending` until the response returns (80–86); `add` and `cancel` reject while pending (187–201). View click handlers reject all their bound clicks while `busy` (66), and the saving form is inert (view busy handler). Thus confirmation delay can block starting another edit/add or closing through these handlers even though the new row appears immediately. Native tab behavior must be checked separately; do not claim every page interaction is frozen.

WEC uses per-action/per-record `pendingKey` checks for inline operations, optimistic changes, retained failed actions and returned-record merge. `postAction` (898–932) waits for confirmation but does not itself install the Tenants global controller lock. This source comparison does not prove every WEC control stays enabled; inspect the specific equivalent inline operation. Acceptance must measure click-to-visible change, whether a different row can be opened/edited while the first save is pending, same-row duplicate protection, eventual confirmation and retained retry separately. The corrective path should reproduce the existing WEC action-scoped interaction pattern, not simply make the server faster or call an immediate row insert a fully fluid UI.

### 3. Instructions that sustained drift

Before this review the domain skill correctly said direct save, but its mandatory sources contradicted it:

- `tenants-specialist-handoff-20261010.md` instructed preparing automation and treated the processor choice as already settled.
- `shared-data-behavior-contract.md` final clarification instructed the Tenants API **and automation**.
- `tenants-workflow-overview-20261010.md` normal loop and cadence still required automation.

Those active instructions are corrected in place. Existing preference automation is separate and unchanged. Historical input-log/automation discussion did have owner approval; the failure was retaining that older direction after the later explicit direct-save correction. It would be inaccurate to label every automation, cache or safety check unauthorized. Persistent dirty state, identity/tenant isolation, operation IDs and consent preservation were requested. No record in this review establishes specific authorization for redundant full-state rebuilds or for silently bypassing the visible Add Rider control in acceptance tests. These are implementation/verification choices, not requested business behavior.

The repository's post-failure `APPROVED TO EDIT` requirement contributed repeated owner exchanges. Explicit cycle-wide local approval should have been carried forward within its exact scope; it did not authorize unrelated deployment or subsequent live changes. Do not remove owner controls or invent new permission gates. Generic verification guidance must not cause rerunning unchanged suites when exact passing evidence is already retained.

### 4. Why the work kept expanding

Coordinator accepted isolated main-form/local proof as progress toward a wider page outcome without an exact visible-entrypoint checklist, retained conflicting source instructions, and managed serial correction cycles without enforcing a cumulative owner deadline. These are coordinator and verification failures, not a demonstrated model/effort limitation. Token totals and attribution are not measured here.

Users/Horses/Locations are not proven by Barn/Rider success: current `src/pages/rs-inputs/[operation].js` routes only `barn` and `riders` through the direct Tenants bridge. The other kinds fall through to the legacy store. Their table cutover and actual live behavior remain separate work. Do not use the two-User fallback to claim success if it writes the wrong base.

## Changes actually made by this review

- Corrected the three conflicting documents above.
- Updated `C:/Users/gombc/.codex/skills/tenants/SKILL.md` with actual-visible-control testing, canonical-destination checks, measured latency, retained evidence and cumulative deadlines.
- Updated `C:/Users/gombc/.codex/skills/rs-coordinator/SKILL.md` with cross-document contradiction checks, actual-entrypoint acceptance and no deadline resets.
- Captured current public markup and offline request-count evidence; independent read-only review inspected the deployed source.

No runtime source, business data, automation, deployment or static publication changed in this review. Stopped product workers were not restarted. Documentation correction is not a repaired UI or a closed behavioral complaint.

## Bounded implementation path

1. Wire the one missing Rider opener into the existing controller and test visible open/close/save/reload on current markup.
2. Remove unused full-state staging work and measure the same save fixture; then minimize returned state using established HPS/WEC behavior while preserving access and retry guarantees.
3. Prove one assigned-Barn Users form against the actual Tenants People table before expanding to Horses/Locations. Two distinct test Users, normal reload, edit one, reload/readback. Creating a person must not silently grant login, membership or consent.

No setup/Add Barn changes in this correction. No new framework or automatic restart. Each product correction needs its applicable owner authorization and remaining time; the owner's explicit fallback stages are conditional, not permission to build three alternatives in parallel.


## Authorized correction completed locally — 15 October, 12:36 UTC

Owner approved editing/testing the existing defects. Window12:25:47–12:40:47Z; verification completed before cutoff. No deployment, publication, automation change or live record mutation.

Implemented in `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test`:
- Bound the visible section Rider button to the existing tracked sheet. Save and Close now use the same state as the working relationship opener.
- Saving inhibits the affected entity editor rather than all bound controls; other entity drafts can be opened/edited and survive confirmation. Same-kind save remains guarded because that editor has one retained pending operation. This is not a claim of unlimited concurrent same-entity edits.
- Used WEC/HPS returned-record merging. New native Barn/Rider requests ask for the verified record; other clients retain full-state responses. Preserve fresh access, mapping, roster and target checks, consent, operation IDs and dirty retry.
- Removed discarded staging full-state construction; write cache invalidation is table-specific, while explicit fresh preflight and final target verification remain.
- Conflict reopening refreshes the affected stale data. Independent review caught and corrected stale conflict reopen, broad historical-row merging and unrelated draft clearing before final verification.

Proof: actual current captured page markup, actual native client and Astro handlers, isolated storage fixtures; real visible button clicks, no app.add substitution. Mobile390x844 and desktop1280x900 passed load, Barn edit/save/reload, failed-save retained retry, Rider dialog open/Close/reopen/add/Save, immediate pending row, Barn editing during pending Rider save, preserved unrelated Barn draft, Rider inline edit/Save/reload. This is local browser integration proof, not a deployed browser/Airtable test or visual styling audit.17 direct-boundary tests plus7 client/editor tests pass;2 browser journeys pass. Native bundle consistency and Astro build pass. Independent read-only reviewer found no remaining definite blocker in scoped source.

Measured identical offline fixture:46 canonical requests/10.722s before;40/9.072s after negotiated record response. Five identity reads remain. This does not establish live19-second response timing; do not claim server latency eliminated. User interaction no longer waits globally for that response.

Evidence: `.git/ringstatus-control/tenants-cause-audit-20261015/correction-manifest.json`, `correction.diff`, `read-count-selective-after.json`; worktree `test-evidence/tenants-visible-correction-20261015.tap`, `tenants-correction-build-20261015.log`, mobile/desktop functional screenshots. Deployed baseline44ed49e8c43ec614c37c84ee2c299e0dc71b1f5a unchanged. Approval covers local changes/tests only. Next release work must isolate these9 files from unrelated checkout changes and obtain deployment authorization; live timings/visible acceptance then remain to verify. Users/Horses/Locations canonical cutover is separate and not proved here. Setup/Add Barn remains deferred.


## Approved release and live verification — 2026-10-15 09:18 EDT

Owner approved deploying the tested package and proving live load/add/inline edit/save/reload, with no static Webflow publishing. Window: 13:05:32–13:20:32Z. Exact nine-file manifest deployed as commit `40df86e5fd44c5a32ed56516dfc54d06613715af`, existing release branch `work/inputs-crm-20261006`; Webflow deployment `a234731b-c718-431b-944b-b3106869d5fa` succeeded at13:12:13.337Z. Browser-fetched native bundle SHA256 matches tested manifest exactly. Dirty checkout and unrelated changes were excluded using an isolated index. No static publishing or automation changes.

Actual live IAB session: assigned test Barn loaded; visible Add Rider dialog created labelled Rider; immediate row feedback then Saved; full reload retained it; actual inline edit then Save and another reload retained edited name. Airtable connector independently read back Rider `rece6MfrpafrnU1aE`, one matching record, name `TEST — Live release Rider 2026-10-15 0916 edited`. Existing test Barn `recsJkP55Xps5nSL8` was edited through the visible form, confirmed Saved and retained after reload; connector readback agrees. Network responses200: Rider add16.203s, Rider edit15.819s, Barn edit9.599s. Immediate feedback is verified; server latency remains substantial. No claim that Users/Horses/Locations are verified.

Existing Edge session: Add Rider opens, closes and reopens correctly, but new Save failed before any observed `/record` POST and retained the new entry. Old failed Barn/Rider drafts were visible before the release and remain untouched; no retry, clearing, replay, record repair or source correction occurred. Source `api.ts`/generated client rejects a different body while a persisted operation remains unresolved. This is consistent with the observed old-session blocker; exact stored operation was not inspected because browser IndexedDB read is unsupported. The clean-session success does not close this existing-session failure. Next correction must establish the retained operation outcome and preserve drafts, rather than clear storage or rebuild the save path. Requires post-failure editing approval and a new explicit bounded window.

Machine evidence: `.git/ringstatus-control/tenants-cause-audit-20261015/release-20261015.json`; screenshot `live-dialog-failure.png`. Product workers not restarted. End-to-end acceptance for existing Edge session remains failed.


## Approved pending-record correction — 2026-10-15 09:35 EDT
Owner authorized edit/test/deploy, same Edge add/edit/save/reload, preserve drafts, ten minutes, no static publishing. Deadline13:45:13Z. Compared WEC `failedActions`, payload fingerprint and removal of only confirmed actions against current kind-wide single-pending blocker. Retained existing IndexedDB/store and legacy pending shape. Added opt-in independent pending operations to the same persistence object; original failed operation/request ID remains, independent targets can save, same-target unknown outcomes still cannot be overwritten. Only exact confirmed operation is removed. Existing-record editing and explicit Barn switching clear stale form Barn context. No automatic replay or storage clearing.
Independent review caught dirty-state cleanup when an older retained operation confirms last. Corrected restored-draft tracking; regression tests cover final clean state and preservation of newer unsent edits. Nine client/browser/recovery tests passed; revised two recovery tests passed; final build passed; scoped independent re-review clear. Exact five-file release `11de2ee4622fd81c701cc2bfd71d33e9bf5c51c4` pushed from prior deployed40df86e using separate index. Live proof pending; no completion implied by this entry.


### Same-Edge release outcome at ten-minute cutoff
Release11de2ee deployed successfully13:43:39.895Z. Same Edge tab685191250 reloaded, visibly restored both old failed drafts; no storage was cleared or old save replayed. Selected existing TEST Barn, clicked visible Add Rider, entered `TEST — Edge recovery 2026-10-15 0944`, clicked Save rider & return. Immediate new row appeared while Saving; subsequently Saved. Independent Airtable readback found canonical Rider `rec9nrjlxoQjfu5iF`, created13:45:14Z, with roster linkreclXLAe26a95RWJq. The previously blocked independent save now succeeds in the same Edge session.
Ten-minute deadline13:45:13Z reached while this submitted save was completing. No new mutation started after cutoff; read-only confirmation and checkpoint closeout captured its outcome. Same-session inline edit and final reload remain unperformed; do not claim full requested acceptance. Exact next action is edit this same new record through the visible row, Save, reload and verify same canonical ID/name. No more implementation or deployment needed to attempt these checks; owner time extension required. Evidence recovery-release-20261015.json and edge-recovery-cutoff.png/txt.
