# RingStatus Profile

Updated 5 November 2026. Current Profile V3 release: `309ac5f8`; coordinated Schedule return correction: `61b1f899`; launcher correction: `2f893c8a`.

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

See [launcher contract and checks](launcher-contract-20261105.md).

## Coordinated Profile and Schedule links

| Context | Forward to Schedule | Back to Profile | Target |
| --- | --- | --- | --- |
| Public website overlay | `/test/wef-schedule-v2/schedule.html` | `/test/rs-profile-v3/?website=1` | `_self`, inside the existing overlay |
| Standalone staging | `/wef` | `/profile` | `_top` |

The launcher and Agent Schedule links use the same context selection. Overlay navigation keeps the public website URL unchanged. Back reloads Profile through its existing device-held data; it does not preserve the previous mounted Profile document. No new navigation message contract was added.

`ringstatus-profile-widget.sportdog.chatgpt.site` is obsolete and must not be used as the return destination. Schedule owns its static Back link; Profile owns its launcher/Agent links and the website overlay override. The coordinated correction was agreed with **wef-schedule-app**, chat `01a18df3-c380-7f11-afe6-057e10f86308`.

See [coordinated live checks](coordinated-links-20261105.md).

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

The [four-page live check](full-cycle-live-20261105.md) proves its recorded recognition, cache, priority and refresh cases; it did not force an Airtable business-record change and does not prove every later sign-in restarts the timer. Nine Team-scoped horses were verified loading and staying cached. That earlier check does not prove the new GeoIP ping. Its acceptance is recorded in [GeoIP ping evidence](geoip-coordinate-ping-20261105.md). Optional browser-location integration and any untested live save cases must not be presented as verified.

## Known errors and closure rules

1. Record observed errors here with their affected behavior, status, source/release and evidence. Distinguish a confirmed product defect from an unverified condition, a tooling failure or an unimplemented proposal.
2. Do not close an error because a change was committed, deployed or passed local tests. Closure requires verification of the affected live behavior and its protected baseline.
3. Keep unresolved errors open. State the exact remaining proof; do not invent a diagnosis or claim success from missing evidence.
4. Update this register when a correction is delivered or a new failure is observed. Preserve the linked evidence and owner boundaries. The register grants no extra edit or deployment authority.

| Item | Status | Evidence / limit |
| --- | --- | --- |
| Cached Profile waiting for recognition; clicked-list priority; eight-minute refresh | Verified corrected in the recorded four-page check | [Live cycle evidence](full-cycle-live-20261105.md); this is dated acceptance, not a promise about later releases. |
| Profile active state, mobile launcher placement and overflow | Corrected in `2f893c8a` | [Launcher checks](launcher-contract-20261105.md); live DOM checks passed, but live screenshot capture timed out. |
| Obsolete external Profile return link | Verified corrected | Schedule `61b1f899`, Profile `309ac5f8`; [coordinated live checks](coordinated-links-20261105.md). |
| A final horse read repeats after the priority-loaded horse package | Observed; not corrected in these releases | [Priority request trace](full-cycle-live-20261105.md). No claim that redundant reads have been eliminated. |
| Profile GeoIP coordinate ping | Implemented; see separate acceptance evidence | [GeoIP ping evidence](geoip-coordinate-ping-20261105.md). Device metadata only; no Airtable coordinate writes or precise-location UI. |
| Live screenshot capture timeout | Tooling limitation; not a diagnosed product defect | Launcher/navigation were checked through real controls and DOM/URL state. No captured live image proof claimed. |

Schedule Back currently reloads Profile from device data rather than preserving its mounted document. Scroll-up reveal and Expand/Restore are unimplemented proposals. These are recorded limitations/proposals, not verified product failures.

## Verified evidence and limits

- [Four-page live cycle](full-cycle-live-20261105.md): Home, Recognize, About RingStatus and Ring Status; real Recognize/Profile/Schedule/logout cycle, reload with fresh Profile requests blocked, clicked-list priority/resumption and natural eight-minute refresh. No Airtable business records were modified to force refresh changes.
- Launcher: local desktop1440x1000 and mobile393x852 checks, build and12 relevant automated checks passed. Live DOM checks confirmed full bleed, placement above the footer, active toggle and ten actions. Live screenshot capture timed out; no captured image proof is claimed for this correction.
- Coordinated links: actual live website forward/Back retained the Home URL and restored recognized Profile. Standalone Profile's Schedule click reached `/wef`; Schedule's owner independently verified Back to `/profile` on desktop and393.

Historical 2 November evidence and ownership handoff remain at [the coordinator handoff](../coordination/rs-profile-coordinator-handoff-20261102.md). Their old release status and unimplemented-feature list must not be treated as current V3 acceptance.

## Deployment and rollback

Established source: branch `work/inputs-crm-20261006`, existing Webflow Cloud `/test` deployment. Do not initialize another app or deploy mixed unfinished changes. Preserve other owners' Schedule changes and local evidence.

Rollback only the specific correction commit: `309ac5f8` for Profile Schedule links, `2f893c8a` for launcher behavior, or Schedule-owned `61b1f899` for its static Back link. Preserve subsequent changes and apply the existing approval boundaries. This README grants no additional edit, publication, data-repair or workflow authority.
