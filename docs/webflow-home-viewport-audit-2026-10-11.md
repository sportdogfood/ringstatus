# Home viewport and shared variable audit

Reviewed: 11 October 2026. Published source: https://ringstatus.webflow.io/.
Home page ID: `6982268d7543ac3c801512cd`.
Scope: read-only published Home audit; Airtable documentation updates. No Designer style edits or publishing.

## Coverage and evidence

Measured all visible Home sections at 375, 393, 479, 480, 767, 768, 991, 992, 1280, 1440 and 1920px browser overrides. Added a 478px check to investigate the mobile boundary. Read computed typography, dimensions, padding, gaps, flex sizing and overflow; read published native and embedded CSS. Full-page visual reviews at 478 and 1920 supplement the measurements.

Evidence: `webflow-home-viewport-evidence-2026-10-11.json` contains per-element measurements and matching CSS rules. Screenshots show the live state during inspection; animated quotes and the photo outro need state-specific verification.

The desktop browser reserves roughly 15px for its scrollbar. This is not a physical iPhone/Edge audit. Exact breakpoint edges also require attention to browser rounding; the 479px override produced landscape styling, while 478px matched the mobile rules. Do not interpret desktop screenshots as identical to iPhone CSS viewport behavior.

Native published CSS thresholds: mobile <=479px; landscape <=767px; tablet <=991px; desktop starts992px; large1280px; extra-large1440px; widest1920px. The Airtable layout has no landscape column, so 480–767 findings are recorded in its audit notes instead of silently folded into Mobile L.

| Override | Client width | Document width | Section heading | Paragraph | Waitlist input height |
| --- | ---: | ---: | ---: | ---: | ---: |
| 375 | 360 | 375 | 59.1px | 19px | 200px |
| 393 | 378 | 378 | 62.3px | 19px | 200px |
| 479 | 464 | 585 | 54px | 19px | 45px |
| 480 | 465 | 585 | 54px | 19px | 45px |
| 767 | 752 | 752 | 54px | 19px | 45px |
| 768 | 753 | 753 | 54px | 19px | 45px |
| 991 | 976 | 976 | 54px | 19px | 45px |
| 992 | 977 | 977 | 54px | 19px | 45px |
| 1280 | 1265 | 1265 | 73.3px | 19.1px | 45px |
| 1440 | 1425 | 1425 | 84px | 20.9px | 45px |
| 1920 | 1905 | 1905 | 116px | 26px | 45px |

## Section findings and required corrections

1. **Initial media hero and navigation.** Hero image remains a separate component. Its title is not visibly rendered in the captured state; investigate visibility before changing typography. Mobile navigation shows the logo above a horizontal link row. Keep that established structure and shared gutter. Sticky navigation appearing midway through full-page captures is not evidence of duplicate content.
2. **Built for / the day / you get.** Native mobile rules specify `30cqw` and `.85` line-height. A later embedded contract applies `clamp(54px,9.54vw,130px)` and line-height1 to the same text classes. Computed mobile type is54px, so the parent-based scaling and tighter line spacing are overridden. Bind the actual `rs10-h1-xl` text classes to one intro type set; preserve the130px desktop cap. `rs0-big-text` alone is not the font target.
3. **Start with a plan / Know what needs to happen.** Heading uses18cqw on mobile but the desktop clamp elsewhere. Paragraph actual text is19–26px; its wrapper is22px. Preserve the `copy + tabs` skeleton; consolidate competing type rules and use one gutter owner.
4. **Moving targets.** Same heading/body type as the prior section, but nested transparent panels can add another10px or1vw inline gutter above mobile. Normalize copy/panel spacing, retaining the same complete tab structure.
5. **Keep everyone in the loop.** Same section type; quote is a separate presentation element. At narrow/intermediate widths some animated chat descendants extend beyond the viewport while the document remains contained. Verify the visible animation state; distinguish local clipping from document overflow.
6. **I'm using it, too.** Shared type matches, but quote appears faded/partially hidden in the mobile capture and is absent in the wide capture. This needs animation/visibility review, not a guessed font-variable fix.
7. **Back to it.** Shared type matches. Large open space follows copy in the captured state; standardize spacing only after checking intended animated/tips content and height constraints. Longer headings must wrap without fixed-height clipping.
8. **Four split-media sections.** Horses/classes, Ring6, Just ask, Reminders retain their media structure. Actual `l4-h h2` remains38px and `l4-p p`15px across all measured widths, independent of the main section type scale. Bind these actual children to shared media-copy type variables; preserve split layout and phone/media dimensions. Mobile Ring6 has substantial space between the message and heading: review its top/bottom copy anchors. Active portfolio classes are not present on Home; this audit does not establish Lainey's portfolio definitions.
9. **Photo outro.** Captures show “WHAT IS YOUR”, “RING” and a question mark, with STATUS not visibly present. Check the actual animated/positioned word state and clipping before assigning new values. Include this in overflow acceptance; do not hide it with body overflow.
10. **Footer.** At375 the logo/question-mark descendants contribute to a15px document extension. At479/480 the document reaches585px: footer logo descendants and the nowrap footer menu exceed their parent. `div-block-11624` is roughly400px wide but has519px scroll width. Native mobile wrap/gap0 does not cover the inspected landscape state. Extend wrapping based on available width, size logo to its parent, and retain a modest shared gap. At375/393 form inputs become200px tall because embedded `flex:1 1 200px` acts along a column parent's vertical axis; it overrides the native44px intent. Use axis-safe auto basis, shared44px minimum and content-aware padding/height. Copyright also repeats2026; this is content cleanup, not a CSS variable.

## Shared type, spacing and rule ownership

- Actual main heading targets: `l5-heading-white`, `l5-heading-dark` inside `l5-h`. Desktop clamp54–116px, line1, tracking-.04em; mobile18cqw, line.85, tracking-.07em. Mobile-to-landscape change is abrupt and should be validated with one bounded parent-based expression. No new numeric bounds are approved here.
- Actual paragraph targets: `l5-paragraph-white`, existing misspelled `l5-paragrapg-dark`, inside `l5-p`. Clamp19–26px, line1.3, Outfit400. Wrapper22px/1.35 leaks into other content.
- Actual kicker target: `kicker2`, not just `l5-kicker`. Clamp14–16px, line1.2, Outfit600, tracking.075em. Native width exceptions at XXL and on rust need alignment decisions.
- Method owns the container: width100%, max1600px; inline clamp16px/4.071vw/65px; desktop vertical58px; mobile28px top/58px bottom/16px inline. At1920 the method is1600px and inner panel1470px before additional copy gutters. Panel itself has no max-width. Keep this restrained width growth unless a new target is agreed.
- `fsl-vw-panel.is-transparent` adds inline clamp10px/1vw/64px above mobile. Remove unintended double gutters while retaining necessary media/tab exceptions.
- `l5-hp` has28px gap and additional16px vertical padding; `fsl-vw-copy` has explicit16px mobile gap but lacks matching desktop defaults. Consolidate into shared gap/spacing variables.
- Tab actual labels inherit22px from nested `l5-p`, despite link14px. Embedded link min-width0 overrides native mobile100px. Restore the agreed100px minimum, auto intrinsic width for longer content, no shrink, and scrolling on the actual menu. Do not apply scrolling to the whole tabs/content wrapper. At479/480 CONNECT is wider than its text slot.
- Native classes, global element rules, a legacy L4/L5 embedded system and a later selected-public-page contract coexist. Assign a single owner for each shared property. A variable declaration by itself will not fix stronger/later rules or an incorrect binding.

## Airtable update

Base `appara5767XpIaJIf`, table `rs-webflow-variables` (`tblakTuwVmMmxTYUo`). Added one compact field, **Home audit / needed changes**. All70 existing rows receive dated findings; portfolio rows explicitly remain outside Home verification. Replaced10 illustrative example definitions with observed CSS values/formulas and corrected their actual text-class targets where necessary. Remaining mode cells stay unassigned, not implicitly inherited or approved. Mode value accompanying a calc is its fallback/minimum, not a measured value at every representative width. Changes described in the notes remain required work, not implemented Webflow corrections.

## Acceptance for later implementation

Recheck all listed widths and physical iPhone Edge. Document width must equal client width; locally scrollable tabs must remain usable without document spill. Verify every tab's content, longer text, visible quote animation states, media sections, photo outro, complete navigation, inputs and submit control. Verify both sides of each breakpoint, current Designer settings and published output. Preserve existing content/behavior and the agreed limited combo classes. No implementation or publication occurred in this audit.
