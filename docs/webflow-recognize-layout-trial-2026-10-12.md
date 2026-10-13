# Recognize layout trial — 12 October 2026

Page: `6ac459b135ad0c4c3b253d97`. Saved in Webflow; not published.

Reference: RingStatus Barn Onboarding, `https://ringstatus-barn-onboarding.sportdog.chatgpt.site/`. Reference main has a 42rem bound. Desktop recognition dialog measured 640px; mobile dialog measured 393px in the reference device shell.

## Changes

- Added `rs-recognize-main-content`, width 100%, max-width 42rem, min-width 0, border-box, column flex, shared fluid copy gap. Element `9caee66f-c96a-a232-883d-be26c5072ec4` is inside existing `fsl-vw-copy` `ae1c851c-5b09-7e1a-af86-d1ec69fa5c8c`.
- Moved the existing prototype heading stack, profile summary, Open recognition link and simulation note into that wrapper, preserving order.
- Replaced the static preview sheet's class `rs-prototype-sheet-v2` with a separate `rs-recognize-preview-sheet`: width 100%, max-width 40rem, automatic horizontal margins, white background, 18px top corners, overflow auto. Sheet element `ad94db87-ad22-8496-f5fa-3b1221f9761e`.
- No locked shared class edits. Original recognition runtime and its overlay remain unchanged. Added six-state preview remains static and is not connected to runtime actions.

## Verification and limits

Native mutations returned success. Refreshed Designer DOM contains both exact new classes. Computed max-width is 672px for main and 640px for sheet at Mobile, Mobile L, Tablet, 1280, 1440, 1920 and Desktop Base checks. Mobile measured widths were approximately 329px and 361px respectively. Some intermediate canvas measurements lagged breakpoint selection, so these checks prove inherited caps, not complete visual acceptance. Screenshot capture timed out. Published behavior was not tested.

## Revert

Move these four children back to `ae1c851c-5b09-7e1a-af86-d1ec69fa5c8c`, append in order:

1. `54d4016e-c3f7-f511-a9f5-b7a22fcb3e5a`
2. `1cd5e1ad-a806-797d-6da2-6e3e4445ec64`
3. `50188702-8371-961f-52da-80eece8c57c0`
4. `b69dbdf5-4168-277a-5325-7f582890efca`

Restore sheet `ad94db87-ad22-8496-f5fa-3b1221f9761e` to class `rs-prototype-sheet-v2`. Empty new wrapper can remain until owner-authorized cleanup.
