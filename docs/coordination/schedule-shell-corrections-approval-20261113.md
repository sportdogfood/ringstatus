# Schedule desktop and mobile corrections for approval

Date: November 13, 2026. Status: owner approved local implementation and full fallback; deployment remains unapproved.

Owner approval: implement the listed corrections locally and create a full fallback. No deployment, publication or data changes.

## What the owner showed

The latest browser comment is on https://ringstatus.com/, inside Profile's Schedule frame. It shows the Schedule's list buttons near the bottom, a vertical shared launcher on the right, and the footer message “Full schedule checked 7:37 AM.” This is the live Astro integration, not the separate ChatGPT Sites prototype inspected earlier.

The owner wants maximum available height for schedule data on desktop and mobile. The shared launcher will serve as bottom navigation in place of the marked footer area. The check message moves into the Back row. Mobile scrolling failures and flickering must also be investigated before correction.

## Correction list for approval

| Item | Proposed correction | Preserve |
| --- | --- | --- |
| Shared launcher | Place the existing launcher horizontally inside the bottom of the shell. Remove its desktop right-side placement. Use the same shared component in Schedule and Profile. | Existing controls, order, icons, actions, selected states and brand styling. |
| Launcher container | Keep it integrated into the shell without a separate outer capsule, border, shadow or extra container padding. | Individual control styling and shared spacing. |
| Check message | Move “Day-0 checked…” / “Full schedule checked…” into the existing Back row, between the Profile link and theme icon, without adding row height. Remove its separate footer row. Hide and restore it with the Back row. | Exact message meaning, feed timestamps and refresh cadence. Do not imply fresh schedule data when only a check succeeded. |
| Schedule list navigation | Keep Schedule, Timelines and the disabled future lists as the Schedule's own list controls, above the shared launcher. | Existing enabled/disabled states and list-specific date, trainer and team behavior. |
| Available data height | Give the data all space released by hiding the surrounding controls during downward scrolling. Avoid blank scroll space after the last row and avoid overlapping controls. | Schedule records, row order, publication rules, filters and no wrapping. |
| Vertical scroll containment | Keep vertical scrolling inside the open app. Scrolling over its header, data, horse controls, list navigation, launcher or edges must not move the website behind it. Investigate the actual nested-frame cause first. | Working record scrolling and the website's normal behavior when the app is closed. |
| Horizontal containment | Keep the shell and data columns stable. Allow horizontal movement only in the intended ring, horse and navigation strips. | Existing horizontal controls and their actions. |

## Desktop blocks and scrolling behavior

| Block | At rest / controls visible | Scrolling down through records | Scrolling back up |
| --- | --- | --- | --- |
| Back row | Profile link, check message, theme icon on one row. | Slides out of view. | Returns. |
| Brand row | Logo, PRO, date, trainer and team controls. | Slides out of view. | Returns. |
| Ring anchors | One horizontal row of relevant rings. | Slides out of view. | Returns. |
| Data | Ring headings, classes and entry rollups. | Expands into the released space and continues scrolling. | Remains at the user's place as controls return. |
| Horse filters | Existing horizontal horse selection. | Slides out of view. | Returns. |
| Schedule navigation | Schedule, Timelines and disabled future lists, above the shared launcher. | Slides out of view. | Returns. |
| Shared launcher | Horizontal, inside the bottom of the shell. | Stays visible. | Stays visible. |

Show and hide controls smoothly without jumping, resetting the record position, covering records or creating extra page scroll. The check message has no separate footer height. Keep Schedule list navigation distinct from the shared launcher.

This documents the approved local correction. It is not deployment approval. Do not apply this Schedule-specific hide/show behavior to Profile content. Profile shares the bottom-launcher component and placement; its other behavior stays unchanged.

## Mobile investigation and correction list

The owner reports that mobile sometimes does not scroll and sometimes flickers. These are reported defects; their causes have not yet been established. Do not treat earlier desktop touch emulation as proof that the physical phone works.

| Item | Required check and intended correction |
| --- | --- |
| Vertical scrolling | Reproduce touch scrolling in the actual embedded path, not only standalone Schedule. Check swipes starting on records, horse buttons, edit buttons, anchors, filters, navigation and shell edges. Identify whether a touch handler, horizontal drag control, overlay or nested frame prevents vertical scrolling. Preserve taps and horizontal strip scrolling. |
| Flickering | Capture the visible flicker and its trigger: scrolling, reversing direction, switching lists, opening/closing a drawer, browser toolbar changes or background refresh. Check for repeated hide/show changes, shell resizing, whole-list replacement and frame reloads. These are investigation candidates, not confirmed causes. Correct the proven cause without hiding the symptom. |
| More data height | Use the same discussed behavior: scrolling down hides Back row, brand row, ring anchors, horse filters and Schedule navigation; the shared bottom launcher stays visible. Data takes the released height. Scrolling upward restores controls without losing the user's place. |
| Check message | Place the existing check message in the Back row without adding height; truncate if needed, never wrap. It hides and returns with that row. No separate message footer. |
| Phone viewport | Check portrait, landscape, browser toolbar expansion/collapse and bottom safe area. The launcher must fit inside the visible shell and must not cover records. Keep shell width stable and prevent horizontal page movement. |
| Edge-to-edge mobile shell | Fill the available phone viewport from top to bottom and left to right, without desktop outer gutters or an inset floating-shell gap. Respect the phone's safe areas for controls. Use the extra space when Safari's toolbar collapses; do not require the user to hide it. This is a mobile fit change, not a desktop shell-size change. |
| No sideways shell movement | The shell must not slide or pan left/right. Keep document and nested-frame widths contained. Only the intended horizontal ring, horse and navigation strips may scroll sideways. Verify diagonal swipes as well as straight swipes. |
| Background beneath the shell | In dark mode, use black behind the shell so exposed edges and overscroll do not flash white. In light mode, use the matching light background. Check the host page, Profile frame and Schedule frame together; do not change unrelated website sections. Preserve existing theme colors inside the shell. |
| Scroll containment | Keep the website behind the open app still. Check short content, top and bottom boundaries, and returning from drawers. Scrolling must remain available after each interaction. |

Mobile acceptance must cover dark and light, the owner's actual iPhone/browser when accessible, and recorded reproduction of the reported failures. Record the actual device, browser and viewport used; do not label a 393 or 402 desktop viewport as an iPhone model. Browser-only checks must be reported as browser-only.

Verify that the data visibly gains height, the first and last records remain reachable, changing direction does not cause flicker or repeated layout jumps, and background updates do not reset scroll position or edits. Check all four viewport edges, diagonal swipes, Safari toolbar visible/collapsed, and theme changes while the app is open. Dark mode must not expose a white background; light mode must not expose an unintended dark background. Keep all existing brand styling inside the shell, data, filters, actions and shared components. Limit mobile shell changes to the fit, scrolling and background corrections listed here; no replacement implementation.

## Verification required

- Verify the actual live integration reached from ringstatus.com and /wef, not just a standalone prototype.
- Test Schedule and Profile on desktop in dark and light. Confirm the launcher is inside the shell at the bottom, without a duplicate right rail or outer capsule.
- Test desktop scrolling down, reversing upward, returning to the top, reaching the last record, and using a short schedule. Confirm each block follows the table above and record position does not jump.
- Reproduce the recording's pointer paths over data, horse controls, lower navigation, footer area, launcher and edges. Confirm the background page stays fixed while the app is open.
- Check the Back row message fits without wrapping or increasing its height.
- Test the mobile investigation and acceptance list above separately from desktop. Desktop emulation is not physical iPhone proof.
- Check horizontal overflow in the launcher and Schedule list controls without page-wide horizontal scrolling.
- Verify all ten launcher controls remain available, including Logout. Do not perform Logout or production data edits merely to test styling.
- Confirm date, trainer and team filters behave exactly as before. Team filtering retains the full schedule and changes only related trips.
- Review the exact code changes before delivery. Prepare a rollback to the immediately preceding release before any approved deployment.

## Evidence and limits

The earlier shared desktop correction removed the launcher's outer frame and padding; it did not move the right rail to the bottom. That placement is the new requested correction.

Previous browser touch checks showed records scrolling inside the data panel while the outer page stayed fixed. Physical iPhone behavior has not been verified. It must not be described as verified from desktop emulation alone.

The supplied screenshot also shows “Schedule pending for Nov 13. Showing last loaded schedule.” That is a separate data-freshness concern to review against the existing data contract. This UI proposal does not authorize changing source records, publication rules, scheduled workflows, or substituting a manual run.

## Boundaries

Reuse the existing Astro implementation and shared components. No font, color, icon, action, input contract, data contract, cadence or filter redesign. No Recognize changes. No source-record repairs. Local implementation is approved. No deployment until the owner approves the preview. Native Webflow publication remains with the owner unless explicitly authorized otherwise.

## Recording review: scrolling defects to correct

Evidence: owner's 31.63-second recording, `Screen Recording 2026-11-13 074838.mp4`, supplied November 13. Visual review used ordered frames at half-second intervals. This is a desktop-pointer recording, not physical iPhone proof. Audio was not transcribed.

| Time in recording | What is visible | Proposed correction for approval |
| --- | --- | --- |
| 0–14 seconds | Schedule rows move inside the data panel while the header, list controls and right launcher stay in place. | Preserve working vertical data scrolling. Do not replace it with whole-page scrolling. |
| About 14–31 seconds | The website behind the shell moves through different sections while the same schedule rows remain in place. The pointer moves around lower controls, footer, edges and header during this part. | Keep scrolling contained within the open app. Verify input over the data, footer, list controls, launcher and shell edges; the background page must not move. Determine the exact event/frame cause before editing. |
| About 24 seconds | The schedule content shifts horizontally and left-side content is cut off; the right launcher stays visible. The recording does not identify which element owns that movement. | Restrict horizontal scrolling to the intended horse, ring and navigation strips. Keep the shell and data columns horizontally stable. Identify the actual overflowing element first. |
| Throughout | The desktop launcher occupies a separate right rail, while horse controls, Schedule list buttons and the refresh message occupy bottom space. | Apply the bottom-launcher proposal above and move the refresh message. Do not remove the Schedule list controls or horse controls. |

Acceptance must include the owner's shown embedded path: `ringstatus.com` → Profile → Schedule. Standalone `/wef` checks alone do not cover this nested-frame behavior. Reproduce both scrolling defects before making the approved correction and repeat the same pointer paths afterward in dark and light.

The desktop recording does not establish the cause of the reported phone scrolling or flicker. Mobile investigation is included above; no phone redesign is authorized.

## Local implementation checkpoint

Baseline `821fd1e9e776f62d2f70f37b2864a11ab578a345` preserves the latest Profile changes. Full tracked Webflow Cloud app archive and safety branch are recorded in `schedule-corrections-20261113/FALLBACK.md` in the existing evidence directory.

Local preview uses the retained October 30 dataset strictly as a read-only UI fixture; it does not replace the production feeds or change their cadence. Browser checks cover nested website → Profile → Schedule and direct /wef, dark/light desktop and phone-sized viewports. Physical iPhone Safari and its real toolbar/overscroll behavior remain unverified.

Separate Profile-owner account/header and right-anchor launcher changes are being handled in that owner's preview. They are not silently absorbed into this Schedule patch. No live asset changes or native publishing.

## Connected working prototype requirement (owner clarification)

Owner requires both full-size interactive UI and current prepared Airtable feeds with real sign-in and saving. Scaled review frames and synthetic identity are not acceptance evidence for that requirement. No Schedule deployment is approved.

- Actual existing Astro dev app runs locally on8808;8809/wef supplies the existing /test mount and normal full-size embedding. No synthetic endpoint replacements on8809.
- Existing prepared scope/data loaded from wef-schedule-app, current show2026 ESP Pre-Charity, selected dayNovember13. Existing pending-source messages are retained; a successful fetch does not prove source details are fresh.
- Saving must use the existing authenticated Astro /test/wef-schedule/input route and wef-schedule-app change-log, with immediate device feedback and later confirmation. Synced sources remain unchanged.
- Exact remaining dependency: the Profile/Recognize owner must supply the approved local identity source configuration, session-secret provisioning and populated IAM snapshot through the existing authorized setup. rs-astro found no approved reusable local staging setup. The protected refresh endpoint cannot bootstrap an unauthenticated empty local database. No live cookies copied, fake identity used as real proof, guard bypassed or source records repaired.
- Root requested the existing handoff from wef-profile-prototype and rs-astro. Data/UI checks can continue independently. Real save proof requires a signed-in authorized user and an owner-selected legitimate edit; no invented business edits.
- Local application configuration is ignored by Git; no secret values belong in reports, patches or screenshots.


## Pending-data issue retained for later review — 13 November
Owner requested removing the visible “Schedule pending for Nov 13. Showing last loaded schedule.” message while reviewing the design. Hide that message in Schedule and Timelines only. Preserve feed metadata, stored packages, refresh cadence, source records and existing filters. This does not fix or certify Nov 13 data: investigate why the prepared feed marks this day pending and serves last loaded records after UI corrections; no data repair or runner change authorized by this presentation request.

Back/logo spacing remains owned by the existing V3 stack, Back and brand components. Do not introduce Schedule-specific padding or brand overrides. Verify computed shared padding at desktop and phone sizes before delivery.


## Final owner approval and superseding decisions — 13 November
Owner approved completing the open Schedule corrections, desktop/mobile checks, deployment to /wef and then locking the accepted version. This supersedes the earlier local-only deployment hold. Native site publication remains separate.

- Schedule now shows only the shared Profile icon in its own bottom navigation. The separate full launcher row is hidden in Schedule, preserving Profile launcher behavior outside Schedule. This supersedes the earlier always-visible full Schedule launcher proposal.
- Desktop Schedule becomes a right-edge drawer, using the shared Profile drawer geometry/motion: maximum820px width, viewport-limited width/height, page scroll locked while open and restored on close. Mobile remains edge-to-edge with the approved scroll corrections.
- Preserve the shared Back/logo padding and all brand tokens; no ad hoc restyling.
- Pending-day banner stays removed from presentation. Underlying source conflicts remain recorded for later data review, with no source-record repairs or manual runner substitutes authorized by this UI release.
- Preserve Profile's approved release; integrate on its latest commit before deploying Schedule through the existing branch/environment. Capture exact rollback and verify live routes after deployment.


## Final candidate verification
Integrated approved Profile release6cf49d2d. Schedule consumes its existing shared desktop drawer rules through data-placement=drawer, without replacing components, fonts, colors or data behavior. Source inspection found the earlier presentation max-width outranked the drawer selector on800px desktop; final shared selector resolves that specificity conflict.800x600 uses800px width/600px height;1024x768 uses820px width/768px height.

Actual browser checks: desktop1024x768 and800x600; phone-sized393x852 and402x874; dark/light. Controls hide/return, data expands to full viewport, no document sideways movement, nested background remains fixed and returns to its original350px position on close. Native emulated vertical swipe starting on a horse works; horizontal horse swipe moves only its lane. Trainer Philip narrows40classes to1, Clear restores40; CWF keeps40. Nov15 date selection shows63saved classes; current Nov13 returns40. Schedule/Timelines switching preserves the same frame (no reload).

Phone QC found a genuine invisible drag-handle hit area overlapping Clear. Its shared pseudo-element now ends before the header; visible styling/spacing unchanged. Native trainer and team Clear taps verified afterward. Desktop opening motion observed as running220ms shared transition.

Current prepared data continues to carry shared-source conflicts, logged by the running3/15-minute workflow. This UI release does not certify fresh source details or repair synced records. Physical iPhone Safari toolbar/overscroll behavior and a new real user horse-save operation are not claimed from browser emulation/synthetic identity.
