# Profile and Schedule shared data and UI contract

Contract publication: **2026-11-12**. Canonical repository path: docs/profile/README.md on shared main. Profile and wef-schedule must reference the same repository commit, not a worktree copy.

The agreed rules below are preserved from the existing V3 contract. Release labels and evidence in that section are dated November 5, not a claim of current deployment. For November 11 implementation, exact storage, measured behavior and unresolved verification, read [the current runbook](../recognize-profile-runbook/README.md). Requirements are not proof of implementation. No code changes, schema changes or deployment are authorized by this documentation publication.


Updated 5 November 2026. Current Profile V3 GeoIP release: `82d22139` (initial ping `24d1ce17`); navigation release `309ac5f8`; coordinated Schedule return correction: `61b1f899`; launcher correction: `2f893c8a`.

## Scope and current implementation

Profile V3 runs as a widget alongside the public website. Recognize is the gate: signed-out visitors see only the existing Recognize toggle; recognized visitors see the Profile launcher. Public browsing stays available. `/profile` and `/wef` are standalone staging surfaces, not the normal website journey.

Paths below are relative to `webflow-cloud-test/`:

- Current UI and locked shared styling: `public/rs-profile-v3/app.js`, `components.js`, `primitives.css`, `styles.css` and `icons.js`.
- Website overlay and Schedule return handling: `public/rs-profile-v3/embed.js`.
- Existing recognition adapter/controller: `src/assets/rs-recognition/widget.js` and `widget-view.js`.
- Existing Profile device data, load and save adapter: `src/assets/rs-profile/service.js`; UI connection: `public/rs-profile-v3/connection.js`.
- Existing server permissions, records and saves: `src/lib/rs-profile.js`; endpoint `/test/rs-profile/data`.
- Browser refresh cadence: `public/rs-profile-v3/refresh.js`.

The older `src/assets/rs-profile/client.js` implementation and 2 November coordinator handoff are historical context, not the active V3 UI. Do not restore them over V3 or substitute an external prototype.

## Stylesheet rules

These rules apply to Profile V3 and screens consuming its shared components.

1. Use the existing shared `public/rs-profile-v3/primitives.css`, `styles.css`, `fonts.css`, `components.js` and `icons.js`. Do not copy styles into another screen or create screen-specific substitutes for the same component.
2. A shared style correction must flow through the shared source. Preserve the locked font, type scale, colors, button treatment, spacing and component states; do not redesign adjacent elements.
3. Preserve the agreed stack and optional-row behavior. Keep the footer and **Not you?**. Do not restore demo/browser-saved copy or add unapproved footer actions.
4. Desktop remains right aligned. Keep its existing Profile scale and the launcher maximum of393 rendered pixels. Mobile is full bleed; its open launcher sits above the footer. Do not infer new sizing rules from a staging wrapper.
5. Profile stays at the left while destinations scroll independently with end clearance. Active Profile styling must follow open/closed state, including Close and Escape. Preserve all ten existing actions and logout to Recognize.
6. Verify affected shared consumers at desktop and393px before claiming a styling correction complete. Keep unrelated Schedule sizing and behavior protected.
7. Proposed scroll-up reveal and desktop Expand/Restore behavior remain unimplemented until explicitly approved. Documentation does not authorize their implementation.

## Locked shell and launcher

One shared style/component source applies across screens. Preserve the agreed back row, brand, optional Profile/action/tab/search rows, sections and always-present footer. Profile uses picture/edit, name and tags; the Owner tag is inverse. The footer retains **Not you?** and excludes the old demo/browser-saved copy.

- Profile toggles the widget; its active and expanded states follow visibility, including Close and Escape.
- Profile stays at the left outside the horizontally scrolling destination icons. All ten existing actions remain available, with trailing clearance for Logout.
- Desktop launcher is right aligned with a 393px rendered maximum. Existing Profile desktop sizing remains protected.
- Mobile is full bleed. When Profile is open, the launcher sits directly above the footer inside the shell. When closed, the launcher remains available at the bottom.
- Logout immediately removes the private display and restores Recognize.
- Schedule is the separate Astro surface; other launcher destinations use Profile.

Scroll-up reveal behavior and a desktop Expand/Restore control were discussed but are **not implemented**. Do not describe them as delivered.

See [launcher contract and checks](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/launcher-contract-20261105.md).

## Coordinated Profile and Schedule links

| Context | Forward to Schedule | Back to Profile | Target |
| --- | --- | --- | --- |
| Public website overlay | `/test/wef-schedule-v2/schedule.html` | `/test/rs-profile-v3/?website=1` | `_self`, inside the existing overlay |
| Standalone staging | `/wef` | `/profile` | `_top` |

The launcher and Agent Schedule links use the same context selection. Overlay navigation keeps the public website URL unchanged. Back reloads Profile through its existing device-held data; it does not preserve the previous mounted Profile document. No new navigation message contract was added.

`ringstatus-profile-widget.sportdog.chatgpt.site` is obsolete and must not be used as the return destination. Schedule owns its static Back link; Profile owns its launcher/Agent links and the website overlay override. The coordinated correction was agreed with **wef-schedule-app**, chat `01a18df3-c380-7f11-afe6-057e10f86308`.

See [coordinated live checks](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/coordinated-links-20261105.md).

## Data behavior rules — single end-to-end contract

This is the agreed contract for the website's Recognize → Profile → Schedule journey. It replaces the separate descriptions of cookie, cache and loading behavior. Rules describe required behavior; dated verification below describes what has actually been proved. The GeoIP coordinate ping uses the implementation below. Live acceptance is recorded separately from the rules; documentation alone is not completion proof.

### Ownership and existing connections

- **wef-profile-prototype** owns end-to-end integration, Profile and its permitted lists, and coordination of the existing Recognize/Astro boundary.
- **wef-schedule-app** owns Schedule and its data engine. Both use the same established identity and coordinated navigation contract; do not create a second recognition flow, Schedule loader or competing deployment.
- Use the existing Recognize, Astro and Airtable connections and Profile service. Server permissions remain authoritative. A local UID, cookie's presence or GeoIP result alone does not grant access.

### 1. Cookies and Recognize protocol

1. On arrival, check existing device-held identity and Profile state immediately. Do not wait for the network before displaying an eligible returning-device snapshot.
2. In parallel, call the existing silent Recognize flow. The browser sends its protected recognition cookie automatically; Profile JavaScript must not try to read the HttpOnly cookie. The existing adapter supplies the device-token fallback where supported.
3. The existing server validates the device/person and applicable eligibility. Successful recognition renews the existing recognition cookie through the server response. Preserve its existing lifetime, HttpOnly, Secure and SameSite settings; do not introduce a separate cookie-renewal timer or change expiration rules.
4. A valid identity enables Profile and its permitted data. If recognition needs input, show only the existing Recognize toggle/form with its single phone/PIN/invite input. Preserve public browsing; do not navigate visitors to staging pages.
5. Rejected access removes the private cached display. Logout hides it immediately, clears local recognition state and retires the existing server device/session through the established flow. A pending logout must not silently restore identity.
6. Cookie renewal and the eight-minute data refresh are separate responsibilities. Do not describe a data refresh as cookie validation or renewal.

### 2. Device-held data and saves

1. The device holds each person's working copy; Airtable holds confirmed saved records. Keep the last confirmed snapshot, pending edits, save identifiers/revisions and personal view preferences scoped to that person.
2. Before displaying a cached Profile, require the existing valid local identity/device association, matching person snapshot and no pending logout. Show that snapshot with retained pending edits immediately, then revalidate silently. A temporary connection failure is not the same as an explicit access rejection.
3. Before opening a list, check whether its permitted JSON package is already loaded on the device. If present, render it immediately. If absent, use the priority-loading rule below. Do not fetch all packages again merely because a panel opens.
4. Save locally first: retain requested edits, update the screen and close the editor promptly. Use the existing endpoint for confirmation. Only confirmed persistence is **Saved**; local work stays pending until confirmed.
5. Retain pending work through navigation, reload and connection failure. Retry with the existing operation identifiers; serialize changes to the same record. Older responses must not overwrite newer edits, and unrelated actions remain usable.
6. Never persist PIN values in Profile storage. Search text and open-panel state may stay in memory. Changing show/date must not discard Profile data or pending edits; the Profile cache has no separate fixed expiration timer.
7. On logout/account change, remove the previous person's private display immediately. Their pending work remains theirs and must never be displayed or submitted as another person.
8. Preserve server-side Team scope for lists, including horses. Current horse edits are local-only; do not describe them as confirmed Airtable saves.

### 3. GeoIP coordinates

1. After recognition is established, obtain approximate GeoIP latitude/longitude quietly through the existing Astro connection. It must not delay recognition, cached Profile display, list interaction or saving.
2. Reuse existing request-provided geography where available. Any required GeoIP lookup belongs behind the same existing Astro boundary; do not introduce a new browser identity or authentication mechanism.
3. Associate the resulting coordinates with their source and observation time, so approximate GeoIP cannot be confused with browser-provided location. Missing/failed GeoIP must leave the rest of the application usable.
4. GeoIP does not require browser location permission. Precise device location is a separate optional Profile action and requires browser permission; do not request it silently.
5. Do not use coordinates to establish identity, Team access or record ownership. Do not add recurring GeoIP polling, new Airtable fields or a new persistence destination as an implied part of the eight-minute refresh.
6. **Implementation:** after the confirmed minimal Profile response, the service starts one unawaited `GET /test/rs-profile/data?view=geoip` per recognized person/device in the document. The existing cookie and actor header authorize it. Astro uses request-provided coordinates first, then the existing GeoJS provider with a1.5-second timeout. The browser retains only coordinates/source/time, person and device association in `rs_profile_geoip_v1:<person_uid>`; `profileState().geoip` exposes only the matching person/device value. Late account/logout responses are ignored. No raw IP, new Airtable fields, writes, timer or UI prompt is added. Browser-side timeout is2.5 seconds. Optional precise browser location and Airtable coordinate persistence remain outside this implementation.

### 4. Priority and background loading

1. Recognize triggers preparation of the minimum permitted Profile package first. Deliver useful Profile data without waiting for the full roster or other lists.
2. Prepare remaining permitted lists as separate JSON packages quietly, one background package after the next. Coordinate Schedule preparation with its existing owner/engine; do not fold Schedule into a replacement Profile data engine.
3. If the person clicks an unloaded list, that requested package takes priority over background preparation. Avoid duplicate requests for the same package. Resume the interrupted package and remaining queue after the requested package is available.
4. Use already-loaded device data immediately. Keep saves and unrelated controls usable while background work continues. Preserve identity/account-change checks so a delayed response cannot repopulate another person's display.

### 5. Eight-minute changed-data refresh

1. After initial preparation completes, start the existing browser-owned eight-minute data refresh. Keep one refresh sequence; do not overlap runs or create a timer per panel/list.
2. Refresh permitted loaded Profile/Team/list packages through the existing Astro/Airtable path. Compare with the confirmed device snapshot and apply only changed data, preserving newer local and pending edits.
3. A clicked unloaded package retains priority. Refresh work must not block navigation or saving, and signed-out visitors must not refresh private data.
4. If a refresh fails, retain usable device data and pending work; continue through the existing retry/cadence behavior. Do not repair production records or run substitute direct pipeline commands.
5. Work runs while the page is active; do not promise exact wall-clock execution in suspended/closed browsers. Preserve existing page-exit cleanup. A data timer is not a cookie-renewal or GeoIP-polling timer.

### Acceptance and known limits

Verify the full journey through actual website controls: silent recognition or credential input → immediate eligible cached Profile → local list/package checks → clicked-list priority and background resumption → local save/confirmed save distinction → natural eight-minute changed-data refresh → logout back to Recognize. Test delayed/rejected responses and account separation. Preserve the locked stylesheet throughout.

The [four-page live check](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/full-cycle-live-20261105.md) proves its recorded recognition, cache, priority and refresh cases; it did not force an Airtable business-record change and does not prove every later sign-in restarts the timer. Nine Team-scoped horses were verified loading and staying cached. That earlier check does not prove the new GeoIP ping. Its acceptance is recorded in [GeoIP ping evidence](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/geoip-coordinate-ping-20261105.md). Optional browser-location integration and any untested live save cases must not be presented as verified.

## Known errors and closure rules

1. Record observed errors here with their affected behavior, status, source/release and evidence. Distinguish a confirmed product defect from an unverified condition, a tooling failure or an unimplemented proposal.
2. Do not close an error because a change was committed, deployed or passed local tests. Closure requires verification of the affected live behavior and its protected baseline.
3. Keep unresolved errors open. State the exact remaining proof; do not invent a diagnosis or claim success from missing evidence.
4. Update this register when a correction is delivered or a new failure is observed. Preserve the linked evidence and owner boundaries. The register grants no extra edit or deployment authority.

| Item | Status | Evidence / limit |
| --- | --- | --- |
| Cached Profile waiting for recognition; clicked-list priority; eight-minute refresh | Verified corrected in the recorded four-page check | [Live cycle evidence](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/full-cycle-live-20261105.md); this is dated acceptance, not a promise about later releases. |
| Profile active state, mobile launcher placement and overflow | Corrected in `2f893c8a` | [Launcher checks](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/launcher-contract-20261105.md); live DOM checks passed, but live screenshot capture timed out. |
| Obsolete external Profile return link | Verified corrected | Schedule `61b1f899`, Profile `309ac5f8`; [coordinated live checks](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/coordinated-links-20261105.md). |
| A final horse read repeats after the priority-loaded horse package | Observed; not corrected in these releases | [Priority request trace](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/full-cycle-live-20261105.md). No claim that redundant reads have been eliminated. |
| Profile GeoIP coordinate ping | Deployed and live verified in `82d22139` | [GeoIP ping evidence](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/geoip-coordinate-ping-20261105.md). Device metadata only; no Airtable coordinate writes or precise-location UI. |
| GeoIP lookup used unsupported Workers redirect:error mode | Corrected and live verified in `82d22139` | Removed the new unsupported option to match existing GeoIP fetch behavior; compatibility regression added. See [ping evidence](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/geoip-coordinate-ping-20261105.md). |
| Live screenshot capture timeout | Tooling limitation; not a diagnosed product defect | Launcher/navigation were checked through real controls and DOM/URL state. No captured live image proof claimed. |

Schedule Back currently reloads Profile from device data rather than preserving its mounted document. Scroll-up reveal and Expand/Restore are unimplemented proposals. These are recorded limitations/proposals, not verified product failures.

## Verified evidence and limits

- [Four-page live cycle](https://github.com/sportdogfood/ringstatus/blob/16291a95b665d61413d04b02cc04c6dd9f0d032e/docs/profile/full-cycle-live-20261105.md): Home, Recognize, About RingStatus and Ring Status; real Recognize/Profile/Schedule/logout cycle, reload with fresh Profile requests blocked, clicked-list priority/resumption and natural eight-minute refresh. No Airtable business records were modified to force refresh changes.
- Launcher: local desktop1440x1000 and mobile393x852 checks, build and12 relevant automated checks passed. Live DOM checks confirmed full bleed, placement above the footer, active toggle and ten actions. Live screenshot capture timed out; no captured image proof is claimed for this correction.
- Coordinated links: actual live website forward/Back retained the Home URL and restored recognized Profile. Standalone Profile's Schedule click reached `/wef`; Schedule's owner independently verified Back to `/profile` on desktop and393.

Historical 2 November evidence and ownership handoff remain at [the coordinator handoff](../coordination/rs-profile-coordinator-handoff-20261102.md). Their old release status and unimplemented-feature list must not be treated as current V3 acceptance.

## Deployment and rollback

Established source: branch `work/inputs-crm-20261006`, existing Webflow Cloud `/test` deployment. Do not initialize another app or deploy mixed unfinished changes. Preserve other owners' Schedule changes and local evidence.

GeoIP rollback covers `82d22139` and `24d1ce17`; no schema migration or Airtable repair is needed. Rollback only the specific correction commit: `309ac5f8` for Profile Schedule links, `2f893c8a` for launcher behavior, or Schedule-owned `61b1f899` for its static Back link. Preserve subsequent changes and apply the existing approval boundaries. This README grants no additional edit, publication, data-repair or workflow authority.


---

## Historical November 3 ownership guide — retained for reference

The following older guide is preserved without deleting its record. Its ownership assignments, release status and UI implementation references do not supersede the V3 contract above or later owner instructions.

<details>
<summary>November 3 guide</summary>

# RingStatus Profiles — ownership and operating guide

Version: **1.3.0**  
Dated: **3 November 2026 — America/New_York**  
Owner: **rs-profile-coordinator**  
Chat: `01a12690-0e9d-71f0-bb85-000d02307e8d`

This README describes Profile's responsibilities, existing implementation, specialist roles and connections. It is documentation, not authorization to run workflows, change code or records, deploy, publish, or activate agents. Current owner instructions remain controlling. The owner authorized the existing Profile controls, required data additions and app-only deployment on 2 November 2026. Live acceptance is tracked in the evidence below; authorization alone does not establish completion.

## 1. What Profiles does

Profiles is the application that lets an identified User view and manage their own application profile and their permitted Team information. Recognize answers **who is here**. Profile answers **which application records belong to this person, what may they see or change, and was their change saved**.

The coordinator owns the complete Profile outcome: current toolbar and panels, User loading/editing, Team screens, memberships, roles, consumer permissions, save confirmation, downstream compatibility and acceptance evidence. It coordinates the capabilities needed for that outcome without taking over other domains.

An independent User does not need a Team to have a Profile. Team membership is a separate relationship. A Team role is not the same thing as a User level, descriptive tag or recognition credential.

Profile responsibilities include:

- Match the server-validated `person_uid` to exactly one canonical User through `recognition_person_uid`.
- Load the correct User and permitted Team records, using actual record links rather than matching names or selecting the first ambiguous result.
- Apply consumer permissions before returning private data or accepting a change.
- Preserve the approved current Profile design and its visible interaction patterns.
- Save approved User changes through the existing application route and confirm the designated saved records.
- Retain uncertain changes and stable operation identifiers so a lost reply is not silently treated as success or repeated as a new operation.
- Keep another person's data and pending edits out of the current User's screen and saves.
- Maintain useful source, review and acceptance evidence, with the remaining gaps stated honestly.

## 2. What Profiles does not do

Profile does not identify a visitor, build a replacement sign-in system, issue recognition cookies, validate phone/PIN credentials itself, maintain the recognition device index, retire devices, or own recognition sessions and footprints. Those are Recognize responsibilities.

Profile does not grant Team access merely because recognition succeeded. It does not create Team membership from a recognized identity, infer permission from a photo/tag/favorite, or treat a browser-supplied UID as authority.

Profile does not own WEF collection, timing, results, RingWaze serving or Messages delivery. It may supply approved records or permission information through an agreed interface; each receiving domain retains its own rules and acceptance. Editing a Profile does not imply notification consent or activate an alert.

Profile does not automatically create missing Users, Teams, schemas, credentials, storage services or new integrations. It does not repair production records, substitute manual calls for the approved runner, or publish a website because a build passed. A plugin or specialist role does not enlarge authorization.

## 3. The Recognize → Profile handoff

The existing public entry is `https://ringstatus.com/recognize`. Successful credential entry or **Open profile** opens `https://ringstatus.com/profile`. Silent recognition stays on the current public page.

```mermaid
sequenceDiagram
    participant V as Visitor
    participant R as Recognize
    participant P as Profile application
    participant A as Profiles Airtable records
    V->>R: Existing cookie or supported credential
    R-->>V: Recognized identity / unknown / unavailable
    V->>P: Open Profile
    P->>R: Existing server identity validation helper
    R-->>P: person_uid and server-held identity_only
    P->>P: Enforce private access and actor scope
    P->>A: Resolve exact User and permitted Team records
    A-->>P: Canonical records
    P-->>V: Profile and permitted Team views
```

The browser's existing `recognitionAdapter.current()` returns a `person_uid`, null, or an unavailable error. It does **not** return a newly designed trust envelope. Profile's service sends the cookie and expected `X-RS-Actor-Scope` to `/test/rs-profile/data`.

The Profile server independently calls the existing `remembered(request, env)` helper. It rejects missing identity, a changed actor, and `identity_only` access to private Profile data. An IP-only recognized visitor can use the existing Recognize phone/PIN confirmation. Profile does not invent a second credential panel or turn an IP match into permission.

The recognition cookie is `__Host-rs_recognition_device`. Recognize owns its creation, renewal, expiry and retirement. Profile consumes the validated principal; it does not alter recognition state when a Team operation is denied. Sign-out belongs to Recognize; Profile must clear its own private display and actor state when identity changes.

## 4. Information shared with Recognize

| Information | Purpose and ownership |
| --- | --- |
| `person_uid` | Recognize-owned stable identity consumed by Profile. |
| `recognition_person_uid` on the User | Profile-owned link to that identity; exactly one matching User is required. |
| Credential-change coordination | Profile requests an approved phone/PIN change through existing Recognize-owned helpers; the existing refresh path must update the identity index. Credential authority stays with Recognize. |
| `/profile` destination | Shared navigation agreement, not a permission grant. |
| User status/phone, memberships, Team active status and roles | Existing shared refresh reads these for the separate Inputs-access projection. They are downstream permission inputs, not prerequisites for identifying a person. |

Recognize does not need Profile photos, tags, preferences or the entire Team roster to identify someone. Shared refresh dependencies still exist in the working code; an ownership document does not remove them. Interface changes require coordination with the affected owner and one designated writer.

Recognize coordinator: `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`. Existing chat tools can read status and, with human authorization, send a scoped message. A message is coordination, not automatic acceptance or execution authority.

## 5. Records and the new Teams table

The existing base was renamed from Tenants to **profiles**. Its stable ID remains `app7BXcw801n19QKF`.

| Table | ID | Current meaning |
| --- | --- | --- |
| people | `tblyTBgrgdhT3Tcg7` | Canonical application User records, identity link and Profile fields. |
| tenants | `tbled1saw01Thffs1` | Existing Team records used by the deployed application. |
| tenant-memberships | `tblFf4aWQYA2RRsWo` | Existing User–Team relationship, status and roles. |
| roles | `tblRqRbjmqmNfJtIP` | Existing role records used by membership permissions. |
| teams | `tblaczX2otacXkA9w` | Newly created at owner request, currently with a Name field. Not connected to the deployed application. |
| team-invitations | `tblOmDd6CDOBkf7Fh` | Explicit Owner invitation and recipient accept/decline records; separate from recognition invitations and consent. |
| code-review | `tblggsFkWZ53LtkmJ` | Review date, files/language, source version, reviewer, findings, result and evidence. Creating it does not mean a review has run. |

The new `teams` table has not replaced `tenants`. No records or links have been migrated. Moving the application to it requires an explicit, bounded mapping and compatibility assignment; do not create competing Team masters silently. The owner requested a Teams table in this base; no standalone Teams base has been created.

Recognize's own People table remains in base `app9kOZdIaGyKk5uG`, table `tbly1PM5iFYqVzKSm`. Existing first/last names and credentials are accessed through the established coordinated interface. Do not duplicate or move them merely to simplify a screen.

The base also contains preferences, shared directories, processing records and administration tables. Their retention is being reviewed. A current dependency or unsettled intended use is not evidence that a table is disposable.

### Deprecation marker

Owner policy: `___` marks a table for deprecation after 90 days. On 2 November 2026, the following 24 tables had that marker and a description recording **31 January 2027**:

`___agents`, `___fails`, `___activity`, `___development`, `___workflow`, `___changes`, `___contacts`, `___locations`, `___exercisers`, `___grooms`, `___rs-input-riders`, `___profiles`, `___rs-input-horses`, `___entities`, `___trainers`, `___rings`, `___horses`, `___locations-directory`, `___tenant-level`, `___tenant-locations`, `___tenant-trainers`, `___tenant-assignments`, `___favorites`, `___onboarding-stages`.

Records and links are retained. No automatic deletion job exists or is authorized by the marker. Historical references must remain inspectable while dependencies and final disposition are reviewed.

## 6. User and Team rules

The agreed model distinguishes User levels **Independent / Preferred** from Team roles **Owner / Member**. These labels must not be mapped to existing unrelated access values without an approved decision. Descriptive tags such as owner are not membership permissions.

The model calls for explicit invitations and acceptance, Owner management of Member active status, and at most one membership in V1. It does not include browsing, selecting or switching Teams. Personal hide/show affects that viewer's display only. A User with any existing Team membership cannot create another Team or accept another membership. Disabled membership still counts. A Preferred User without membership can create a Team and become its Owner; acceptance is explicit, never automatic.

The approved implementation connects User levels, Team creation/invitations, photo uploads and actual modified-date sorting through existing records and saves. User level is assigned explicitly, not upgraded by the User. Live acceptance remains a separate check. PIN is set-only and never returned. Multiple memberships are not resolved by picking the first.

## 7. Three Profile specialist responsibilities

Profile uses three specialist capabilities under its coordinator. **These are defined responsibilities, not a claim that three Profile agents have been created or are running.** Recognize's internal specialists remain Recognize-only and were not transferred. A concrete assignment must identify the actual worker, source, approved delta and evidence required.

| Specialist | Owns within an authorized Profile assignment | Does not own |
| --- | --- | --- |
| Data-staging | Inspect canonical tables/links, map real records, prepare approved validation and data changes, trace revisions/request IDs/readback and consumer permissions, identify schema gaps and downstream effects. “Staging” does not mean an automatic new base or copied database. | Recognition identity decisions, arbitrary record repair, unapproved migrations or schemas, UI redesign, production workflow substitution. |
| Astro / publish | Profile routes, module packaging and application delivery preparation; confirm the established app/branch/environment, exact release contents and downstream callers; perform only an authorized deployment and its required proof. | Independent publication rights, shared configuration ownership by default, permission redesign, unrelated runtime/dependency upgrades. “Publish” in the role name is not authorization. |
| UI | Current Profile toolbar/panels, Team views and interactions, clear save states, actor-safe rendering, responsive and accessibility checks applicable to the assignment; preserve the approved design. | Credentials, canonical data authority, invented Team policies or fake success/sample data in production. |

The coordinator keeps requirements and later corrections together, sequences shared changes, reconciles results and owns downstream acceptance. Use one writer per shared mutable resource. Independent review remains separate where required. No specialist may authorize its own scope expansion or treat a passing local test as live completion.

## 8. Connected tools and how information moves

The coordinator can use available connectors and skills for a scoped task. A plugin is a tool connection or guidance package, not another application domain or an automatic data-sharing channel. Tool availability does not establish current account access, permission to write, a deployed integration or an approved transfer of private data.

| Connection | Profile use | Boundary |
| --- | --- | --- |
| Airtable | Inspect schemas and real records; perform specifically approved table/record changes; read back the saved result. | Airtable remains canonical for application records. No guessed identities, blind retries, record repair or duplicate stores. |
| Cloudflare | Available tools can search documentation/API capabilities and access resources within their actual connected permissions. Coordinate any required Profile runtime/binding work with the shared app and Recognize owners. | Existing D1 identity data belongs to Recognize; shared claims/helpers remain protected. No new Worker, D1, R2, scheduled process or data export is implied. No new direct Cloudflare deployment route is established here. |
| Webflow | Inspect or, when authorized, maintain the existing Profile page/custom-code loader and Cloud application metadata through the production Webflow connector. | Website publication and Cloud application deployment are separate actions. The current app route is reused; a saved page is not published and a local build is not deployment proof. |
| RingStatus Operating Skills 0.2.9 | Request Router, Instruction Loader, Rebalancer, Handoff, Code Review, Context Compact and Complaint Reporting supply their applicable operating procedures. | All seven were read in this chat. Reading does not launch every procedure, activate a workflow mode, spawn agents or prove a review/continuity check. Preserve explicit invocation requirements. |
| OpenAI Developers | Available connection supports the actual exposed dashboard/key-setup capabilities. Relevant guidance can support a separately authorized agent/API integration. | No Profile runtime OpenAI integration, AI agent, key setup or private-data transfer is established by this README. An installed plugin does not prove API access or authorize sending User/Team data to a model. |

The currently implemented runtime path is browser → existing Astro Profile route → server identity validation → canonical Airtable records → permission-filtered response. This is distinct from coordinator tool calls used to inspect or maintain the system. Sharing a README, chat message or record ID does not create an API or transfer access.

Keep credentials, PINs, device tokens and raw private chat histories out of documentation and review records. Return only the consumer data required by the authorized operation. Record the source, contract and actual result rather than assuming every connector shares a common session or database.

## 9. Source and delivery boundary

Verified implementation checkout:

`C:/Users/gombc/.codex/worktrees/profile-proactive-20261103/ringstatus`

Current verified application release: `7fda82691e4c41f8b7f44bc5dd4f183e3210991d`, deployment `aff9e411-570e-4e0a-9ddc-d8072fe8adbe`, succeeded on 3 November 2026 at 14:47:21 UTC. It preserves the existing design and sibling changes through baseline `0cfe8b0dd7e281a126d636f2696a0306a34c0bcf`. No website publication occurred. This is dated release evidence, not continuous monitoring.

Profile source, relative to that checkout:

- `webflow-cloud-test/src/assets/rs-profile/client.js`, `profile.css`, shared `rs-ui/screen.css` and recognition `widget.css`: consumer presentation.
- `webflow-cloud-test/src/assets/rs-profile/service.js`: browser identity/data/pending-save adapter.
- `webflow-cloud-test/src/lib/rs-profile.js`: canonical records, permissions and saves.
- `webflow-cloud-test/src/pages/rs-profile/client.js.ts`, `service.js.ts`, `data.js`, `index.astro`: assets, API and standalone entry.

Public paths are `/profile`, `/test/rs-profile/client.js`, `/test/rs-profile/service.js`, `/test/rs-profile/data` and standalone `/test/rs-profile/`. The service script is shared by the existing public consumer and Profile; it is not another data backend.

Existing Cloud app: `d7d97751-20e1-4148-a5cf-ee58671c128a`; environment: `110f06dd-c1ea-4839-98af-d829cbe77941`; mount: `/test`. The existing GitHub branch `work/inputs-crm-20261006` auto-deploys on push. A push is therefore a release action, not a harmless way to save this README.

Recognize widget/IAM/credential/refresh code, D1 claim/save helpers, `rs-tenant-barn.js` and Cloud configuration are protected shared dependencies. Coordinate their callers, designated writer and release before changing them. Preserve unrelated owners' work.

## 10. What has been proved and what remains

The existing core flow was tested through the live /profile controls: real User save/readback/reload, retained interrupted requests, sign-in/out/account isolation, roster and Owner enable/disable. The original test Owner was corrected to one membership under explicit owner instruction; temporary test values were restored. Evidence: `.git/ringstatus-control/recognize-model-correction-20261101/profile-e2e-live-results-20261102.json`.

The earlier completion candidate was `a3a07e80655865eafa40f6aea75e7ba24df1e760`. Independent feature tests22/22, the two reproduced review regressions, one-Team concurrency and shared-call compatibility passed. Its separate upstream display release was preserved, and its build passed. Historical review evidence is in `.git/ringstatus-control/profile-completion-20261102/review-corrected/` and `review-final/`; code-review record `recPoGRbBzhiRmH8n` indexes it. Previous failed-review evidence remains in `review/`.

The owner explicitly authorized manual preparation of two disposable registered Test Users without memberships. This is test setup, not automated registration. Existing User memberships are preserved. Deployment and live control acceptance are recorded in `.git/ringstatus-control/profile-completion-20261102/live-acceptance.json`; the approved controls passed live browser/canonical readback/reload checks. Both disposable Users and their Team were deactivated, temporary values restored, and the original memberships/session preserved. Manual fixture preparation is separate from the tested application flow.

Alerts, Alarm, Checkin, external Opt-ins/Schedules and the preset Agent panel are separate unconnected services. This change does not build those engines, change consent, migrate Team tables or replace Recognize. Full offline page reopening and the broader IndexedDB/cache plan are not established by retained localStorage drafts.

### Five designated pages — 2 November 2026

Actual Member and Owner Profile flows were subsequently tested from Home, Ring Status, About RingStatus, Recognize and Profile. Each reached the correct Profile; Member edits saved, read back, survived reload and were restored; Owner membership changes saved and were restored. Team permissions and one membership per User remained correct. Fresh phone/sign-out entry was checked. Test fixtures were deactivated and the original session/memberships preserved.

Historical finding: the a3 release had a startup interaction issue. Later owner releases already corrected that interaction before the 3 November work; the old one-expression proposal is superseded and must not be applied. Original evidence remains in `.git/ringstatus-control/profile-completion-20261102/all-pages-e2e-20261102.json` and `all-pages-launcher-review/`.

### Approved proactive UX — 3 November 2026

The owner approved early permitted preparation, immediate prepared display, immediate local Save/end editing, quiet existing-endpoint persistence and error handling, preserving the design, recognizer and one-Team rule. Three independently reproduced review findings were separately approved and corrected before release. Source review14/14 delayed-response gates and6/6 service regressions passed; unchanged backend16/16 checks passed. The app build and served service hash match the deployed commit.

Live controls passed: Team creation; invitation, decline, re-invitation and acceptance; exactly one membership per User; Member/Owner separation; immediate Profile field update/editor close and next edit while the POST was deliberately held; ordered tag/email canonical saves and reload; prepared Profile visible with GET deliberately held; Home, Ring Status, About, Recognize and Profile handoffs; mobile roster search; photo upload/readback/reload/removal; Owner member disable/enable; and lost successful member-save reply followed by normal reload replay of the identical request ID/body. The server remained authoritative; pending invitation acceptance granted no premature membership.

Preparation uses existing actor-scoped device storage and the server-resolved device fingerprint, bound to a previously successful Profile response. A replaced identity/device cannot reuse old private memory or in-flight work. It adds no TTL, offline authorization, polling, background service or replacement recognizer. Pending same-record writes remain ordered; newer local edits and unrelated work remain usable. PIN remains excluded from persisted drafts and normal cached fields.

The older disposable Users could not be reused for a second Team creation because their permanent one-Team claims correctly rejected it before a Team write. They were restored to their original inactive state and membership links. A fresh disposable pair with unique test phones completed the live flow under the existing manual test-setup approval; both are now inactive in Profiles and Recognize, their Test Team is inactive, temporary values/photo are empty, and each retains exactly one membership as test evidence. Original User revisions13/2 and memberships were unchanged; original User1 session and normal viewport were restored. No claims were deleted or production records repaired.

Evidence: `.git/ringstatus-control/profile-proactive-20261103/` contains `live-steps.json`, `live-pages.json`, `live-readbacks.json`, `live-lost-reply.json`, `final-cleanup-readbacks.json`, `served-source.json`, `release-build.log` and independent `review-corrected/`. Code-review record `recWDCTu6SDzLVkOw` indexes the reviewed source. Live phone/PIN credential changes were not performed; existing credential helpers and PIN persistence were checked offline, and original phone/PIN sign-in remained working. Full offline private display and disconnected services remain outside this correction.

## 11. Review records and working standard

Use the new `code-review` table when an authorized JS/Python review is actually performed. Record exact files/source version, review date, reviewer, findings, result and inspectable evidence. A row is an index to evidence, not a substitute for the review, required independent checks or live acceptance. No historical review row was fabricated when the table was created.

Keep explanations short and human unless the owner requests detail, as for this README. Keep code small, simple and maintainable. Preserve proven source; do not layer patches, redesign working behavior, add speculative infrastructure or upgrade dependencies outside the approved delta.

## Version history

| Version | Date | Change |
| --- | --- | --- |
| 1.3.0 | 2026-11-03 | Approved proactive Profile UX deployed; delayed-response, Team/invitation, canonical, mobile/photo and lost-reply live acceptance; fixtures and original session restored. |
| 1.2.0 | 2026-11-02 | Five-page live flows and restored test data; confirmed startup launcher correction pending approval. |
| 1.1.0 | 2026-11-02 | Settled one-Team rule, current table markers, approved controls and versioned review/live acceptance evidence. |
| 1.0.0 | 2026-11-02 | Initial owner-requested detailed Profile guide; documents domain cut, specialist capabilities, tool boundaries, new tables, deprecation policy and evidence limits. |

Future revisions should update this header and history, retain exact proof dates, and distinguish a documented decision from an implemented or verified change.

</details>
