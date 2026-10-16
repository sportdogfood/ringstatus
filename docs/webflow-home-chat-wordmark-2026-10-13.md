# Home chat alignment and small-screen wordmark — 13 October 2026

Owner reported the bot chat bubble landed incorrectly below 767px and the large wordmark was too small at tablet/mobile.

## Evidence and correction

At 820px, the full copy/`rs10-tips` stack measured753.12px wide; the visible `rs10-chat-left is-show` wrapper measured617.56px (82%), and the bot bubble was constrained again inside that wrapper. Its right edge ended135.56px before the stack edge. `rs10-tips is-split` also had a74% base width. Wrapper-level constraints compounded bubble widths.

The visible left/right chat wrapper combos now use100% width, max100%, min0, zero horizontal margins/padding, and border-box. Both visible tips combos use100% width, stretch alignment, shared fluid copy gap and zero extra top margin. Existing hidden states and links remain intact. Bubble classes retain their own widths, alignment, colors and typography; the owner-set65% minimum on `chat-msg messagex-bot is-eee` was preserved. No wrappers were deleted.

All three large wordmark parts now use `clamp(44px, max(min(12vw, 96px), 7.5vw), 144px)` with .85 line-height. This increases smaller widths while preserving approximately6rem desktop and9rem at1920. Existing Home custom-code expression was updated to agree with native styles.

## Verification

Native Designer checks at393,667,820,1279 and1920: every visible bot bubble ended at its stack's right edge (difference less than .001px); wrappers matched stack widths. All three wordmark parts had matching fonts/heights: approximately47.15px,80.05px,96px,96px and144px respectively. Document scroll width matched viewport width in every check. Rules first, screenshot last.

Evidence: `webflow-home-chat-wordmark-2026-10-13-checks.json`; screenshot `C:/Users/gombc/AppData/Local/Temp/rs-home-chat-aligned-667-20261013.png`. Saved Home head readback matched exactly. Published/physical-device verification and refreshed custom-code Preview were not performed in this turn. Nothing published; no page-tree edits or component conversion. Shared native styles can affect existing consumers.
