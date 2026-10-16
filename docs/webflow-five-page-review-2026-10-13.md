# Five-page shared-style review — 13 October 2026

Scope: Ring Status, About Ring Status, RS Recognize, Lainey, LM. Home is the existing baseline. No publication or final lock.

## Saved changes

- Ring Status: prior30 class assignments and4 tab moves retained. Existing hero fixed combo sizes replaced by shared XL expression. The native hero container fallback now agrees with the82vw locked contract.
- About: stale6 October contract replaced with Home rules. One unstyled copy-panel wrapper assigned the existing fsl-vw-panel / is-transparent / is-copy stack. Portfolio text uses the existing shared gaps and one outer gutter.
- Lainey and LM: Home variables/type/control/layout contract applied to the existing native stacks. Portfolio adapter preserves layout/media order while using shared copy spacing, parent query container,1600px content ceiling and outer fluid gutter. Plain is-lainey kickers retain their unboxed treatment.
- Shared portfolio native rules: removed40px inner top/bottom padding; copy gap uses16–24px fluid expression; removed1920px fixed16px gap; removed redundant20px heading bottom margin.
- Recognize: original main/overlay structure retained, existing member32–64px and app-card24–32px heading roles reused;700 weights replaced with600.17 native role corrections map controls/labels/inputs to Home formulas. Original colors, links, bindings, overlay visibility and application scripts retained. Main remains an intentionally narrower application container.
- Active Airtable rs-brand-styles now has50 rows:41 previous plus9 necessary portfolio/application mappings. Deprecated tables excluded.

## Verification completed

- All4 newly saved heads exactly match intended source on fresh readback.
- Lainey, LM and Recognize native element trees are byte-identical before/after; no content, bindings or link changes.
- About tree differs only in the one intended wrapper class assignment.
- All17 Recognize native rule changes match fresh readback.
- Ring Status current Designer:1920px root/scroll1920, H1/XL116/132, controls18px/52px;1440px root/scroll1440, controls15px/46px;1280px root/scroll1280, controls14px/44px. Base headings104.952px and XL125.942px at1280. A1389px check was recorded separately and is not labeled1280.
- Earlier393/667/820 Ring Status native checks remain documented in webflow-ring-status-review-2026-10-13.md.

## Pending acceptance

Refreshed custom-code Preview, responsive rendered checks of About/Lainey/LM/Recognize, tab/current/hover interaction verification and final screenshots are being checked. Source readback alone is not rendered acceptance. Final portable CSS and lock remain deferred until these checks pass.

## Recovery and evidence

Exact before trees and heads: webflow-PAGEID-2026-10-13-before.json for each of the four newly reviewed pages. Ring Status has its earlier named before artifact. Shared breakpoint readback was inspected across main/medium/small/tiny/large/xl/xxl. No site-level scripts, CMS data or forms were submitted.

### CTA cascade verification finding

About Preview exposed embedded .modern-btn CSS overriding the first adapter: fixed16px, weight600,12px radius. Existing adapter corrected in place with portfolio-text/rw5-actions ancestors, preserving embedded behavior. Shared1px border prevents the old is-rs2px border from changing total control height. Native modern-btn and is-rs border updated. Fresh saved-head readback confirmed. Preview DOM was explicitly found to contain the stale old adapter despite toggling custom code; full reload issued. Repeated browser inspection deadlines prevented final About CTA acceptance before moving to independent Recognize verification. Do not treat pre-correction About measurements as passing CTA checks.

### Latest checkpoint — source work preserved; browser-dependent checks blocked

All four latest saved heads have exact fresh readback and preserve original code outside the intended contract. Native CTA type/dimensions, portfolio spacing, hero82vw fallback, and all three Recognize heading weights were freshly read back. The used small logo combo rd-status-xs is-status was also corrected700→600. Airtable count51 read back. Ring Status Preview measurement file validates no page overflow across393,667,820,1279,1280,1440,1920 (plus an explicitly recorded1670 intermediate width).

Remaining rendered checks: About latest CTA adapter and remaining widths; Lainey and LM all final widths; Recognize main/overlay actual visible prototype styles and widths; hover/current visual acceptance. Browser inspection now fails both ordinary DOM inspection and the documented CDP fallback; native accessibility action also returned an unavailable-frame error. One CUA session reset did not restore inspection. An owner request to reconnect Edge control with Designer in front is pending. Verification tab685190891 remains open on Recognize and is marked for handoff. Webflow headless access remains working. Do not keep retrying this inspection path without restored access or new evidence. No publication, final CSS export or final owner lock.

### 14 October — connection restored; fresh Preview results

The owner reconnected Edge. Fresh Designer tabs load the current saved custom code; older sessions had cached the previous CTA adapter. About, Lainey and LM now each have computed measurements at393,667,820,1279,1280,1440,1920. Shared boxed controls measure14–18px/500, minimum height44–52px, corners18/18/18/0. Plain is-lainey kickers retain their intentional unboxed treatment and the same text scale. Page scroll width equals viewport width on all final measurements.

Lainey's initial mobile check exposed actual page overflow:667→713px and393→482px. The existing navigation wrappers' intrinsic minimum widths caused it; shared display:flex also revealed two explicitly hidden footer links. The same existing page adapter was corrected on Lainey and LM: constrain navigation wrappers at≤767px, keep scrolling local to legacy-nav, preserve nav-drop-link.hidden. No CMS, media, copy or destination changes. Both pages passed the seven-width recheck, and both hidden links remain display:none. Before evidence is preserved in webflow-navigation-2026-10-14-before.json; final and failed-before measurements are distinguished in webflow-remaining-preview-2026-10-14.json.

Visuals checked after computed rules: Lainey dashboard at1920 and LM opening section at393. Recognize and interactive-state acceptance remain in progress. Active Airtable rs-brand-styles count53 read back. No publication or final lock.

### 14 October — Recognize results and exact remaining boundary

Recognize fresh Preview main and visible prototype controls/inputs were measured at393,667,820,1279,1280,1440,1920. Page scroll equals viewport throughout. Controls share14–18px/500,44–52px minimum and18/18/18/0 corners; inputs share16–18px/400. Existing application heading roles remain600/1.05. A duplicate1920 measurement is retained as an intermediate read, not counted as another viewport.

Opening the original native overlay was attempted through its existing Open recognition link. It remained display:none. DOM evidence: rs-native-recognize has data-rs-native-enabled=true, data-rs-native-click-ownership=adapter and data-rs-native-load-status=unavailable. The preserved footer loader imports /test/rs-recognition/native-client.js and sets unavailable in its catch path. The older draft script targets rs-component-drafts, which is absent on this page. No handler, ownership gate or module path was changed to force this styling check. Native sheet title has computed24px/600/25.2px at393 while hidden; this is not open-overlay acceptance. Theme switching and open-overlay rendering remain blocked in this Designer session. No forms submitted.

Current status: responsive main-page measurements complete for all five reviewed pages; original Recognize overlay/theme interaction and pointer Hover/Current visual acceptance remain unverified. Final portable CSS and owner lock remain deferred. Airtable evidence now distinguishes passing main-page checks from the blocked overlay. Screenshots saved at C:/Users/gombc/AppData/Local/Temp/rs-lainey-preview-1920-20261014.png, rs-lm-preview-393-20261014.png and rs-recognize-preview-393-20261014.png.

### Final portfolio navigation state check

An actual pointer hover over LM's footer contact link at393 exposed a remaining native combo conflict: blue text rgb(61,142,255) and white20% background. The existing shared Current/Hover selector lists were extended to .lp-container.is-nav .nav-drop-link on Lainey and LM, including Current+Hover. This preserves the same roles rather than adding a class or stylesheet layer. Both saved heads exactly match fresh readback. Before source and failing computed colors are in webflow-navigation-states-2026-10-14-before.json. Refreshed pointer recheck follows.

Fresh LM Preview recheck at393 passes: hovered contact background rgb(70,51,43), white text,14px,43.998px actual height,18/18/18/0 corners. Evidence: webflow-navigation-hover-2026-10-14.json; screenshot C:/Users/gombc/AppData/Local/Temp/rs-lm-final-393-20261014.png. Lainey has exact readback of the same scoped selector correction; that last state-only edit has no separate fresh Lainey pointer check. No layout/type variables changed after the seven-width measurements. Current colors remain explicitly pending final reconciliation/acceptance; native rust tabs historically retained black/white while custom Current uses rust/white.
