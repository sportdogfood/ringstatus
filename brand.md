# RingStatus brand stylesheet foundation

Updated: 13 October 2026. Status: working draft for review; final portable CSS is deferred until the page reviews below are complete.

This document consolidates the shared Home rules and the owner's subsequent corrections. It supersedes older values where explicitly identified. It records both intended rules and known implementation differences; it does not declare every page consistent or newly locked.

## Principles

- Reuse one section structure and one typography system across pages and viewports.
- Light and dark treatments share typography, dimensions and spacing; theme differences belong to colors and approved combos.
- Kickers, chat messages, buttons, navigation links and tab links share control typography, padding and corner geometry. Their width, alignment, capitalization and interaction states may differ by purpose.
- Prefer a base class plus a small number of intentional combo classes, no more than three classes where practical.
- Use fluid expressions with sensible limits. Avoid duplicate breakpoint typography overrides when the same formula can carry through.
- Font weight never exceeds600. Paragraphs use400; controls use500; headings and large wordmarks use600.
- Inspect structure, native styles, combos, breakpoint overrides and custom CSS first. Verify computed rules next and visual rendering last.
- Preserve content, links, CMS bindings, hidden content and application behavior unless a specific change is authorized.

## Typography

Font family: **Outfit**, with `sans-serif` fallback. Other platforms must load the appropriate400,500 and600 font faces; declaring the family alone does not load the font.

| Role / variable | Fluid value | Weight | Line height | Tracking |
| --- | --- | --- | --- | --- |
| Heading / `--rs-heading-size` | `clamp(54px, 10cqw, 116px)` |600|.85|-.04em|
| XL heading / `--rs-heading-xl-size` | `clamp(54px, 12cqw, 132px)` |600|.85|-.04em|
| H2 / `--rs-h2-size` | `clamp(38px, 3vw, 54px)` |600|.85|-.04em|
| Paragraph / `--rs-paragraph-size` | `clamp(19px, calc(5.3px + 1.08vw), 26px)` |400|1.3|Normal|
| XL paragraph / `--rs-paragraph-xl-size` | `clamp(21px, calc(6px + 1.4vw), 32px)` |400|1.3|Normal|
| Shared control / `--rs-kicker-size` | `clamp(14px, calc(6px + 0.625vw), 18px)` |500|1.2|.075em|
| Large wordmark | `clamp(44px, max(min(12vw, 96px), 7.5vw), 144px)` |600|.85|-.08em|

`cqw` headings require the intended heading parent to establish an inline-size query container. They scale from that parent's available width, rather than an unrelated ancestor. Parents must have a real, nonzero width, `min-width:0`, and a100% maximum so typography cannot force a section beyond the viewport.

### Class mappings

| Role | Existing Webflow classes |
| --- | --- |
| Light/dark heading | `l5-heading-white`, `l5-heading-dark` |
| XL heading | Existing heading class plus `is-xl`; hero uses `rs10-h1-xl` and its color combos |
| Heading parent | `l5-h`; hero uses its existing title wrappers and `rs0-big-text` |
| Light/dark paragraph | `l5-paragraph-white`, `l5-paragrapg-dark` |
| XL paragraph | Either paragraph class plus `is-xl` |
| Paragraph parent | `l5-p` |
| Shared control text | `rs-shared-control-text`, `rs10-tab-label`: inherit parent typography |
| Large wordmark parts | `rd-status-big is-ring`, `rd-status-big is-status`, `rd-status-big-q` |

The existing dark paragraph class is spelled `l5-paragrapg-dark` in Webflow. Preserve that mapping until an explicit naming cleanup; do not silently substitute a new class. Heading role and semantic level are related but separate: an H2 should not acquire H1 scale merely because it appears in the same section.

Large wordmark parts use the same formula and line height, preserving their color differences. This large treatment is separate from the smaller navigation/footer logo variants; those need their own recorded sizing rather than inheriting the large wordmark by accident.

## Shared section structure

```text
ls-section-content
└── fsl-method
    └── fsl-vw-panel
        ├── fsl-vw-copy
        │   ├── l5-kicker
        │   │   └── Kicker2 + approved color/treatment combo
        │   └── l5-hp
        │       ├── l5-h → heading
        │       └── l5-p → paragraph(s)
        └── fsl-vw-tabs (when present)
```

Use `fsl-vw-copy` for the common copy stack. Retrofitted Home split sections have replaced `fs1-copy` with this class. Nav and hero retain their necessary structural differences. CMS stays outside the presentation stack. Portfolio remains a separate structure using shared type and spacing.

### Layout and spacing values

| Property / variable | Current shared value |
| --- | --- |
| Content maximum / `--rs-content-max` |1600px; container width100%, centered|
| Section gutter / `--rs-gutter` | `clamp(16px, 4.071vw, 65px)` |
| Section vertical padding / `--rs-section-space` | `clamp(32px, 4vw, 64px)` |
| Method gap / `--rs-method-gap` | `clamp(24px, 2.78vw, 40px)` |
| Copy / HP gap / `--rs-copy-gap` | `clamp(16px, 1.67vw, 24px)` |
| General stack space / `--rs-stack-space` | `clamp(16px, 2vw, 32px)` |
| Heading parent width / `--rs-heading-parent-width` | `clamp(60rem, 82vw, 90rem)`, max100%, min0 |
| `fsl-vw-copy` and `fsl-vw-tabs` global padding | **Zero on all four sides** |

Section gutters belong to `fsl-method`. Do not add the same gutter again to inner copy or tabs. Zero global copy padding supersedes the earlier extra16–32px copy-padding rule. The1600px content ceiling is the current recorded value, not a claim that every future layout should use that ceiling.

## Shared control family

Families: `Kicker2`, `chat-msg messagex-user`, `chat-msg messagex-bot`, `rs-page-link`, `rs6-tab-link`. Existing member/navigation/submit controls should map to the same type and spacing where their purpose matches.

| Shared property | Value |
| --- | --- |
| Minimum height / `--rs-control-height` | `clamp(44px, calc(28px + 1.25vw), 52px)` |
| Height / maximum height | Auto / none; wrapped content may grow |
| Horizontal padding / `--rs-control-space` | `clamp(16px, 2vw, 32px)` |
| Vertical padding / `--rs-control-block` | `max(8px, calc((var(--rs-control-height) - 2px - var(--rs-kicker-size) * 1.2) / 2))` |
| Border |1px; recorded shared color `#eee`|
| Corners | Top-left18px, top-right18px, bottom-right18px, bottom-left0 |
| Box sizing | Border-box |

44px is the desktop starting minimum, **not a maximum height**. At1280 the shared target is44px/14px text; at1440 approximately46px/15px; at1920 52px/18px. Messages with multiple lines grow beyond these minimums.

### Purpose-specific differences

| Pattern | Alignment / casing | Width behavior |
| --- | --- | --- |
| `Kicker2` | Left / uppercase | Fit-content; min50%, max100%; wrap if content requires |
| User chat message | Left / capitalize | Fit-content; base min50%, max72% |
| Bot chat message | Right / capitalize | Fit-content; base min50%, max82% |
| Navigation link | Left / uppercase | Content-based; retain navigation layout |
| Tab link | Left / uppercase | Min100px; no shrinking; menu owns horizontal scrolling |

`l5-kicker` is a wrapper, not a44px capped control. `is-lainey` intentionally removes the kicker box treatment while retaining the shared text role.

The owner-set `chat-msg messagex-bot is-eee` minimum is65% and is preserved. Native mobile/other combo widths still require reconciliation during the upcoming review; do not represent every chat variant as already using only the base percentages.

### Chat stack

`rs10-tips` contains visible `rs10-chat-left` / `rs10-chat-right` link wrappers, then the message bubble and inheriting text. Hidden/default visibility behavior stays intact.

- Visible tips and link wrappers use100% width, max100%, min0; the bubble alone controls its own width and side.
- Visible tips use stretch alignment and the shared copy gap, with no extra top margin.
- Link wrappers have no horizontal margin/padding.
- Never multiply a percentage wrapper width by another percentage bubble width; that caused the bot to stop short of its stack's right edge.
- Wrapper names are legacy names: use the actual user/bot bubble role to determine alignment, rather than inferring alignment from `chat-left` / `chat-right`.

## Color and interaction states

| Recorded role | Color |
| --- | --- |
| White text | `#ffffff` |
| Shared border | `#eeeeee` |
| Rust used in Home Current rule | `#984924` |
| Brown message / hover | `#46332b` |
| Base chat background | `#1c1f22` |

This is a partial color inventory. Named Webflow color variables, green, error/rust variants and theme backgrounds must be reconciled before exporting the final palette. Preserve their actual values rather than guessing replacements.

- Hover uses explicit brown background/white text and no inversion filter.
- Hover and Current keep shared height, typography, padding and corner geometry.
- Home's existing custom contract currently specifies rust/white Current and brown/white Current+Hover.
- Native Current rust tab combos on Ring Status retain black/white; Current nav uses its existing selected background. These differ from Home's custom contract and must be reviewed before declaring one portable state contract.
- Focus must remain visible; Home currently includes a2px outline with3px offset for nav/tab keyboard focus.
- Test state contrast against each section background, including Hover on an already Current item. No completed full accessibility audit is claimed.

## Social controls

`rs2-social-2`: fit-content, max100%, fluid8–12px padding; removed percentage container-width overrides. Its social targets use matching44–52px square sizes with fluid10–14px inner padding. Shared horizontal social-row gaps scale12–24px. Preserve link destinations and icon artwork.

## Viewports and verification

Webflow modes: Base/Desktop,1280+,1440+,1920+, Tablet≤991, Mobile L≤767, Mobile≤479. Exact boundaries and actual rendered widths must be recorded separately; choosing a breakpoint is not proof of a particular canvas width.

Home control/spacing corrections were checked in Designer and refreshed Preview at393,667,820,1279,1280,1440 and1920. Later chat-wrapper and larger-wordmark corrections were checked in Designer at393,667,820,1279 and1920. All measured bot right edges aligned with their stack edges; wordmark parts matched each other's font/height; page scroll width matched viewport width.

The latest chat/wordmark custom CSS has saved-source readback but has **not** had a refreshed Preview or published-device check. The latest Home Hover/Current custom rules also require refreshed Preview verification. No new component conversion, publication or owner lock is claimed.

## Next page reviews — owner's order

Page identities verified through Webflow metadata on13 October2026. Status: queued for a full comparison with this foundation; previous partial changes do not count as completion.

| Order | Page | Path | Page ID |
| --- | --- | --- | --- |
|1|Ring Status|`/ring-status`|`6ab11bd52642dca0c8e74004`|
|2|About Ring Status|`/about-ringstatus`|`6ab260d76d76d28aa9be185f`|
|3|RS Recognize|`/rs-recognize`|`6ac459b135ad0c4c3b253d97`|
|4|lainey|`/lainey`|`6aa739aad90e0d765affe666`|
|5|lm|`/lm`|`6a987d7a71da602c33cb1d78`|

For each page: map existing sections to these patterns; list only deviations and necessary new patterns; reconcile combos, overrides and custom code; verify computed values and layout at relevant widths, then check visuals and interactive states. About intentionally omits some kickers. Lainey/LM may require portfolio/CMS patterns. Recognize's overlay, main content, forms and controls require their own layout mapping; preserve existing behavior.

As a shared pattern is corrected and verified, reuse it downstream rather than recreating a separate stylesheet per page. New page-specific exceptions remain explicit until accepted.

## Final portable CSS — after review

Execution checkpoint —13 October2026: active Airtable table `rs-brand-styles` created in Webflow base, ID `tblUqEn2npCQEm0uP`, with41 shared rules/mappings. Ring Status's native structure and existing custom contract have been corrected to reuse Home; source readback and mobile/tablet computed checks passed. Wide-breakpoint/Preview/interaction/visual acceptance remains pending. See `docs/webflow-ring-status-review-2026-10-13.md`. About, Lainey, LM and Recognize now have source corrections saved and verified. Their rendered acceptance is in progress. Active table now contains50 rules/mappings. See docs/webflow-five-page-review-2026-10-13.md. No final lock or background execution is implied.

Create **one final CSS file** containing the accepted variables and reusable patterns. Map Webflow class names to portable roles for Sites and other platforms, retaining only necessary adapters. Include font loading requirements, section/type/control/logo rules and agreed state/theme exceptions. Reconcile native styling with existing custom CSS before export so the portable file does not preserve contradictory rules.

Do not generate that final CSS yet. This document is the review foundation.

## Evidence sources

- `docs/webflow-style-lock-2026-10-12.md`: historical owner lock, supplemented by later authorized corrections.
- `docs/webflow-home-consistency-2026-10-13.md`: earlier Home correction and verification record.
- `docs/webflow-home-owner-corrections-2026-10-13.md`: fluid controls, zero copy padding and social sizing.
- `docs/webflow-home-link-states-2026-10-13.md`: Hover/Current conflicts and verification boundaries.
- `docs/webflow-home-chat-wordmark-2026-10-13.md`: full-width chat wrappers and latest larger small-screen wordmark; supersedes the earlier24px minimum wordmark scale.
- Associated JSON files preserve measured evidence. Latest owner corrections take precedence over historical checkpoints.

## Additional reviewed mappings —13 October2026

The active Airtable table now contains51 rules/mappings. About, Lainey and LM reuse the Home contract with one portfolio adapter. Its outer portfolio-content-bg owns fluid gutters; portfolio-text removes duplicate inner padding, shares the copy gap, centers within1600px and establishes the heading query container. Portfolio-heading has no extra20px bottom margin. CMS/media order stays intact.

Recognize retains its narrower application layout and existing overlays. Its existing member heading role is clamp(32px,4cqw,64px),600/1.05; its existing app-card/sheet role is clamp(24px,2.4cqw,32px),600/1.05. These are existing application roles, not replacements for the marketing heading scale. Inputs reuse Home's clamp(16px,1vw,18px),400/1.2 and shared control dimensions. Recognition/recovery scripts and form bindings were not changed or submitted.

Existing modern-btn CTAs had embedded CSS overriding type and corners. Their adapter now uses .portfolio-text .modern-btn and .rw5-actions .modern-btn so the shared rules win without important flags or changing their links. Native CTA type/dimensions/corners agree with this role. The is-rs border is1px so it does not inflate height. Small rd-status-xs is-status wordmark weight corrected700→600; its existing optical sizes/colors retained.

Source changes and exact readbacks are in docs/webflow-five-page-review-2026-10-13.md and docs/webflow-five-page-source-checks-2026-10-13.json. Ring Status refreshed Preview has measurements at393,667,820,1279,1280,1440,1920. About's first measurements exposed the CTA override and are not final CTA acceptance. Later Preview was confirmed to contain stale page-head code; a full reload was issued. Subsequent browser inspection repeatedly timed out, including an unavailable accessibility-frame response. Remaining About/Lainey/LM/Recognize rendered checks require browser control to be restored. This is not a final stylesheet lock.

## Responsive review update —14 October2026

Edge control restored. Fresh Preview tabs confirm the corrected CTA rules on About, Lainey and LM at393,667,820,1279,1280,1440,1920. Boxed controls share14–18px/500 type,44–52px minimum height and18/18/18/0 corners; is-lainey remains an intentional plain kicker. All three final page scroll widths equal their viewports. Measurements: docs/webflow-remaining-preview-2026-10-14.json.

Lainey and LM navigation retain the original single-row arrangement. At≤767px the existing navigation wrappers use width100%, min-width0 and max-width100%; overflow scrolling belongs to legacy-nav. Explicit nav-drop-link.hidden remains display:none. This corrects observed mobile page overflow without changing the shared type scale, CMS or media. Active rs-brand-styles contains53 entries. Recognize and interactive-state verification are still pending; no final lock or portable CSS export is claimed.

Recognize main/visible prototype controls and inputs also pass those seven widths: no page overflow, controls14–18px/500 and44–52px minimum, inputs16–18px/400. Its original native overlay cannot open in Designer: the existing module loader reports data-rs-native-load-status=unavailable. Original loader, ownership gates and form bindings remain preserved. Open-overlay/theme interaction and pointer Hover/Current visual acceptance are not verified. This is the remaining acceptance boundary, not a completed lock. See docs/webflow-five-page-review-2026-10-13.md for exact evidence.

Portfolio navigation state correction: existing shared Current/Hover selectors now include lp-container.is-nav / nav-drop-link on Lainey and LM. Fresh LM393 pointer hover verifies brown#46332b/white, unchanged14px type and44px target with18/18/18/0 corners. Same Lainey source read back exactly; no separate last-state pointer check there. Current palette reconciliation and Recognize overlay/theme remain pending final acceptance. The final shared CSS file is still deferred.

## Owner portfolio-system revision —14 October2026

Latest read-only inspection of /lm supersedes the earlier LM portfolio-text adapter description: all three portfolio-system content stacks now use portfolio-content > portfolio-content-bg > fsl-vw-panel is-transparent > fsl-vw-copy, containing l5-kicker, l5-hp (l5-h + l5-p), and rs10-tips is-show. Headings/paragraphs reuse the global classes and optional is-xl. The CMS/media shell and intentional first/dashboard combos remain separate presentation responsibilities. Preserve them and all existing video/poster/replay code.

Owner confirmed the portfolio-specific odd/even spacer order with a≤767px order:0 override works, including479px; include that exact rule in the lock. Full skeleton, identified changes, preserved CMS video/poster bindings and accepted ordering CSS: docs/webflow-lm-portfolio-system-2026-10-14.md. This pass changed documentation only. Earlier responsive checks precede these owner revisions; no new full lock or post-revision viewport verification is claimed.
