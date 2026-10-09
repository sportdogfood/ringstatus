# RingStatus responsive audit and correction requirements

Date: 9 October 2026. Source: this conversation's published-page measurements, screenshots, selected native Webflow inspections, and owner feedback through instruction revision 30.

## Scope and status

Audited Home `/`, Tools `/ring-status`, About `/about-ringstatus`, Contact opened from About, and Lainey `/lainey`. Requested `/tools` returns 404; the published Tools navigation points to `/ring-status`.

This document consolidates findings and the owner's desired corrections. No Webflow correction or publishing has been performed. The owner says corrections will need to be applied; this documentation step does not claim they are implemented. Preserve existing content, order, native elements, working behavior and unrelated pages. Do not publish without an explicit request.

Mobile measurements used the Codex in-app browser at 393px and below, principally 375px; earlier Home measurements included 392px. Its desktop scrollbar consumes approximately 15px, so a 393px viewport can have about 378px of available page width. Contact locks body scrolling and changes that available width. These measurements are not an actual iPhone Air Edge capture. The owner confirmed that their phone shows Home's logo above a horizontal HOME / TOOLS / ABOUT row. Other exact device differences, viewport and zoom remain unverified.

## Owner questions and accepted correction direction

| Topic raised | Recorded requirement / answer |
|---|---|
| Home differs from published iPhone Edge below 393px | Audit one section at a time and identify causes; do not treat Designer snapshots as device proof. |
| Navigation arrangement | Preserve the owner's observed logo-above-horizontal-links arrangement as the comparison reference. |
| Container spills beyond mobile width | Identify the responsible elements; prevent whole-page left/right movement. |
| “Built for / the day / you get” fitted with vw | Keep these three phrases as the intended lines. A calculated 27vw starting size was discussed for this exact copy, not verified as an applied design. |
| What happens with longer content? | vw and container units respond to dimensions, not text length. Allow wrapping and section growth; exact fixed-line fitting requires content measurement. |
| Responsive to parent div | Set an eligible parent container and use container units such as cqw. 30cqw was an illustrative starting proportion, not a universal accepted size. |
| “Know what needs to happen.” | Apply the same parent-responsive heading approach, fitted for this longer heading. |
| PACK / PREP / SHOW / CONNECT | Minimum tab widths, grow for the label, no shrinking into overlap; keep a single row with horizontal scrolling inside the menu. Do not widen the page. Exact minimum not selected yet. |
| Moving Targets / “When the day changes…” | Same parent-responsive heading treatment. Owner reports missing padding; verify the exact affected edges before changing it. |
| Remaining similar Home sections | Apply consistent side padding and consistent spacing between kicker, heading, body and tabs. Let sections grow with longer content. Exact spacing values remain to be established from the existing baseline. |
| Footer | Correct the documented oversized inputs and footer navigation; contain local overflow so the page cannot move sideways. Footer wrapping versus a locally scrolling row is not yet selected. |
| Documentation | Retain all audits, questions, desired corrections and unresolved limits together in this document. |

## Home findings

| Section | Observed cause or measurement | Correction / verification status |
|---|---|---|
| Photo hero | Minimum height 75vh; cover image crop changes with viewport width and height. No 393px breakpoint exists in the native breakpoint list. | Record crop behavior; no replacement or new crop requested. |
| Top navigation | Published mobile wrapping rules create logo above links. Native snapshot showed a single row. | Compare saved/native and published rules; preserve expected mobile arrangement. |
| Introduction | Published heading overrides native 18vw with a clamp whose mobile minimum is 54px. At 393px, each phrase measures about 157–162px at 54px inside about 346px. | Parent-responsive sizing requested; remove/adapt the conflicting rule at its source when implementing. |
| Start with a plan | Heading 54px. Tabs use a 14px wrapper but nested labels inherit 22px through `.l5-p`; CONNECT exceeds its slot and clips. | Parent-responsive heading and minimum-width locally scrolling tabs requested. |
| Moving Targets | Heading 54px, wrapping into multiple lines; same inherited tab-label issue. | Same heading/tab direction. Missing-padding observation requires exact verification. |
| Team update, I'm using it, Back to it | Headings 54px with 43.2px line height on `.l5-heading-dark`; body copy generally 19px. Scroll-triggered callouts can appear faded or absent before animation settles. | Consistent padding/spacing requested. Do not report animation timing or transparent snapshot backgrounds as confirmed styling defects. |
| Horses beside classes | Lazy schedule image changes section height after loading; image has no HTML width/height reservation. | Reserve image space if correcting layout shifts; keep existing asset. |
| Ring 6 | Mobile image and copy stack; lazy image affects height. Screenshot and native chat content both appear. | No duplication defect established or removal requested. |
| Ring 7 / Just ask | Copy is about 40px narrower than adjacent examples, increasing wraps. Lazy image affects height. | Check consistency against intended layout; no arbitrary replacement. |
| Reminders | Stacked mobile copy/image with lazy image height change. | Same image-space consideration. |
| Large photo footer | 100vh section and large display typography; inspected content fit measured width. | No additional confirmed overflow from this section. |
| Final footer navigation | `.div-block-11624` is centered, nowrap. At 393px its container is about 314px; six links total about 414px. HOME begins about 26px outside the left edge; MEMBERS reaches about 388px against about 378px client width. | Confirmed source of Home's document-wide horizontal spill. Resolve nav layout, not just conceal inaccessible links. |
| Footer inputs | Mobile `.sign-up-flex-form .input-16` has `flex: 1 1 200px`; native form is a column. The 200px basis applies vertically, making inputs about 200px tall despite a native 40px height. | Correct the basis/direction conflict; maintain readable inputs and existing form behavior. |

## Tools findings

The top nav uses `.fsl-method.no-margin.is-nav`, not `.rs-custom-nav-center`, so the published mobile wrap selector misses it. It remains nowrap and clips ABOUT. At a 375px viewport the page scroll width remains about 393px while client width is about 360px. Content-section outer widths fit; tab contents overflow locally.

| Section | Additional finding |
|---|---|
| Horse care | RECOVER needs about 95px inside a 60px text slot at 393px; surrounding panel clips overflow. |
| Packing | ARRIVE and RETURN exceed approximately 62px label slots. |
| Schedule | SCHEDULE needs about 108px and RESULTS about 85px inside 62px slots; surrounding panel clips them. |
| Messaging | TWO-WAY SMS, COMMENTS, SUBSCRIPTIONS need about 149px, 118px, 158px inside 60px slots. Labels overlap. Menu has local horizontal scrolling, but labels are still compressed. |
| Back to it | Same heading and copy render in two separate visible sections. Intent not established; do not remove without scope confirmation. |

Tools repeats the 22px label inheritance, lazy schedule-image shift, footer nav overflow and oversized footer input causes already documented on Home.

## About findings

Content sections fit at 393px and 375px. It repeats Tools' unmatched mobile navigation wrapper and footer issues; document width remains about 393px at a 375px viewport.

| Section | Additional finding |
|---|---|
| Introduction | `.paragraph-578` sets 15px at max991px, outranking the generic paragraph rule. Line height 19.5px versus most later copy at 19px / 24.7px. |
| What's the holdup at 7? | Closing waiting-list paragraph lacks `.l5-paragrapg-dark`; computes 16px / 20.8px versus 19px / 24.7px above it. |
| Heading consistency | Responsibilities and RingWaze use 54px / 43.2px, Scratch uses 54px / 51.3px, whole-barn uses 54px / 54px. Existing different classes create different density. |

## Contact findings

Opened through CONTACT on About. Checked 393px and 375 × 667. Panel fits; Send remains visible at the shorter tested height. No form was submitted.

- Name and Email are about 275px wide because `.lpt-input` sets `width:auto`; Message fills the available form width (about 361px at 393px). This creates uneven alignment.
- Overlay and wrapper each add 70px top padding, putting the card about 143px down.
- Fields have placeholders but no persistent labels or explicit accessible labels. The accessibility tree gives generic field names; accessible-name behavior should be verified before making broader accessibility claims.
- Overlay/card use hidden overflow. Keyboard-open and smaller-height behavior was not tested.
- Underlying page overflow remains; opening Contact locks body scrolling rather than repairing the page layout.

## Lainey findings

Already selected when inspected. Outer containers and document fit at 393px and 375px; no whole-page horizontal overflow measured.

| Section | Finding |
|---|---|
| Hero video | Iframe about 1,136px wide inside a 378px hero, creating heavy cropping of video and YouTube overlays. Current crop behavior is observed; intended crop remains to be confirmed. |
| Upcoming schedule | Fixed four columns of about 100/200/200/200px; header about 700px, rows/content up to 740px inside a 363px scrolling panel. Later columns require horizontal scrolling. Event titles use nowrap/hidden/ellipsis. |
| Favorite horses | Horizontal carousel scrolls locally (about 1,104px content within 363px panel); does not widen the page. |
| Favorite videos | Cards about 379px wide inside 363px panel at 393px, and about 361px at 375px. A single card is slightly wider than the available panel. |
| Introduction, portfolio, dashboard, navigation | Containers fit measured widths. Headings differ from main public pages: intro 42px, other section headings 38px. No universal typography replacement requested. |

## Correction verification requirements

Before implementation, inspect the current native target and custom CSS/script ownership; preserve the working baseline and identify the smallest change. Avoid stacking new overrides over the diagnosed conflicting rules.

For the requested Home corrections, verify each affected section at 393px and below (including 375px and a narrower mobile case), and check larger layouts for regressions. Verify parent-responsive headings with current and longer sample content, consistent spacing, complete tab labels, horizontal touch/scroll access inside menus, usable footer inputs, reachable footer links, and no document-wide sideways movement. Verify after fonts, lazy images and animations settle.

Published acceptance requires an explicitly authorized publish and live readback; saved native styles or Designer snapshots alone do not prove published behavior. Actual iPhone Air Edge comparison remains outstanding. No result in this document claims it is solved.

## Supporting audit records

### Implementation progress — owner revision 37

Revision 38 continuation: saved additional native `tiny` updates. `rs0-big-text` and `l5-h` use inline-size containment; intro variants use 30cqw, section headings use 18cqw with line-height1. Intro line grouping is a column and negative line top margins are removed. `.fsl-method.no-margin` uses16px side padding,28px top,58px bottom. Footer inputs use44px height/min-height,16px font and auto flex basis. Selected native snapshots show the new intro layout; exact canvas viewport and full interactive/mobile acceptance remain unverified. Published custom CSS reconciliation still pending and can override native heading/input values. These records do not claim completed published corrections.

Owner authorized Home edits at Webflow mobile portrait below 479px. Three native style updates are saved at `tiny`: footer navigation wraps and centers; tab links use nonshrinking intrinsic sizing with a 100px native minimum; tab menus scroll horizontally inside their width. Successful Webflow responses returned the saved tiny properties. These shared class changes can propagate to other users of the classes; page usage and rendered acceptance are still pending. Published custom CSS sets tab min-width to zero, so the minimum remains subject to reconciliation. Parent-responsive headings, consistent padding and footer input corrections remain pending. Nothing published; no claim of iPhone acceptance or completed repair.

Private session evidence under `.git/ringstatus-control/425f3dcfaa6d6d01567ffd5ee267737d002af25708471859337800f53850ce78/` contains `home-audit-observations.json`, `tools-audit-observations.json` and `about-audit-observations.json`. Contact, Lainey and subsequent owner requirements are consolidated here from the conversation/tool outputs. Earlier receipts do not certify later requirements or completed corrections.

### Desktop styling progress — owner revision 43

Owner authorized consistent Home desktop styling corrections. Native main styles saved: intro and section heading tracking -0.04em; intro line-height 0.9; section line-height 1; removed third intro wrapper -30px offset; removed white heading shadow; removed split heading right inset; quote backgrounds consistently #46332b with 24px horizontal and 12px vertical padding. Existing mobile heading sizes and line-height remain; section mobile tracking explicitly kept at -0.07em. Existing assets and both footer sections retained.

Intro and quote Designer snapshots inspected. Snapshots omit white section backgrounds, limiting full visual acceptance; saved properties alone do not establish final desktop rendering. Published site remains unchanged. Complete Designer verification at 1440/1024 and published precedence reconciliation remain pending. Ring7 asset contains a smaller phone within the same CSS image width, so no blind image enlargement applied.
