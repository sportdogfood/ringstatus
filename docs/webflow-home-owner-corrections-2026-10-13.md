# Home owner corrections — 13 October 2026

Target: Home `/`, page `6982268d7543ac3c801512cd`. Owner authorized these corrections after listing remaining discrepancies. No publication or component conversion.

## Recorded requests

- Navigation links, kickers, tabs and both chat-message families share fluid control typography and height. 44px is the desktop starting height, not a maximum.
- Remove global padding from `fsl-vw-copy`; `fsl-vw-tabs` must agree.
- `l5-kicker` has no fixed or maximum height.
- `rd-status-big is-ring`, `rd-status-big is-status` and `rd-status-big-q` share font size and line height, approximately 6rem at desktop, scaling up and down.
- Normalize `rs2-social-2`, particularly at 1920.

## Applied rules

- Control minimum height: `clamp(44px, calc(28px + 1.25vw), 52px)`. Height remains auto and maximum height none, so wrapped messages can grow.
- Shared control text: `clamp(14px, calc(6px + 0.625vw), 18px)`, weight 500, line height 1.2. At 1280: 44px/14px; 1440: 46px/15px; 1920: 52px/18px.
- Vertical control padding derives from control height, text line height and border thickness. Existing horizontal padding, colors, widths, alignments and interaction states retained.
- Copy and tab wrappers: zero padding on all four sides. Existing section gutter remains on `fsl-method`.
- Wordmark parts: `clamp(24px, 7.5vw, 144px)`, line height .85, weight 600. Conflicting question-mark breakpoint font overrides cleared; ring/status combos use the same expression across breakpoints.
- Social container: fit-content, max-width 100%, padding 8–12px; percentage-width overrides removed. Social targets use the same 44–52px sizing, and horizontal row spacing scales 12–24px.
- Modified existing `rs-public-contract-v2` in Home head rather than appending another override layer.

Native styles are shared site definitions; existing consumers can inherit these changes. Other page trees were not edited. Saved Home head was read back and matched the intended content exactly. Rendering verification is recorded separately below.

## Verification

- Native Designer checks: 393, 667, 820, 1279, 1280, 1440 and 1920px. At each width, all visible core control families shared one font size and one minimum height; copy and tab wrappers had zero padding; all three large wordmark parts had matching font sizes and rendered heights; all three social targets had matching square dimensions. Document scroll width equalled viewport width throughout.
- Refreshed custom-code Preview: the same seven widths. The initial cached Preview read was excluded. The 1440 result was replaced with an exact-width settled measurement after an intermediate 1670px canvas result.
- Desktop/1280 controls: 44px minimum height, 14px text. 1440: approximately 46px/15px. 1920: 52px/18px. Multiline messages remain taller as content requires.
- Wordmark: approximately 96px at 1280, 108px at 1440, 144px at 1920; all parts line-height .85. Social targets: approximately 44, 46 and 52px respectively.
- Evidence: `webflow-home-owner-corrections-2026-10-13-before.json`, `webflow-home-owner-corrections-2026-10-13-rules.json`, `webflow-home-owner-corrections-2026-10-13-rendering.json`.
- Screenshot shows navigation and message control treatment at 1920. Browser node scrolling failed to frame the social section; its sizes were verified from computed rules and rendered bounds, not a footer screenshot. No published or physical-device verification claimed.
