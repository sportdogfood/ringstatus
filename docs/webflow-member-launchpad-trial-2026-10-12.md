# Member launchpad reversible trial — 12 October 2026

## Current revision: shared greeting and action bound

Owner authorized starting changes after supplying the original RingStatus Barn Onboarding prototype. Its rendered `rings-main` rule was inspected: max-width 672px (42rem), side padding 16px. This evidence replaces the initial 32rem button-only trial below.

Added parent `462100b5-1a04-2b72-1107-38c52b6b6913`, class `rs-member-launchpad-content`, inside existing `fsl-vw-copy` (`2a11d909-ab8f-65b3-1d9f-b45abfee26cf`). New parent uses flex column, width 100%, max-width 42rem, min-width 0, border-box, align-self flex-start, and the shared fluid copy gap. Existing section/copy gutters remain in effect; this is an adaptation, not a pixel-identical clone of the reference.

Moved existing `l5-hp` (`94972caa-c848-e056-64e8-7461ca52b27f`) and existing action stack into that parent in their original order. Restored action stack to `rs-member-actions-v2`. No locked class definitions or child button styles changed.

Rendered Designer checks: greeting, parent and button stack had matching widths at 393 (329px), 667 (581px), 820 (672px), 1279 (672px), 1280 (672px), 1440 (672px), and 1920 (672px). This verifies the shared width bound at seven sampled breakpoints; it is not proof of every intermediate width or published behavior. Desktop restored afterward. No publishing performed.

Current revert: move the greeting and action stack back into `fsl-vw-copy`, greeting first and actions second. Leave the empty new wrapper and unused trial classes in place; removal is unnecessary. The action stack already uses its original class. To restore the initial 32rem trial specifically, also reassign the stack to `rs-member-launchpad`.

## Historical initial trial

Page: `6ac91a47a4ac5cd8134b81c0` (RS Member Home).
Element: `8d375619-d8ea-d0e1-9b1f-c813396e17a0`.

Before: style list exactly `[rs-member-actions-v2]`.
Trial: style list exactly `[rs-member-launchpad]`.

New class: flex column, width 100%, max-width 32rem, min-width 0, align-self flex-start, box-sizing border-box, side margins 0, gap `var(--rs-copy-gap, clamp(16px, 1.67vw, 24px))`.

No locked class definitions or child button styles changed. Trial applies only to the prototype launchpad. No publishing performed.

## Checks

Designer canvas 1279px: computed launchpad width 511.996px, maximum 512px, gap 21.3598px. Screenshot confirms alignment with greeting and bounded vertical stack.

Designer canvas 393px: computed launchpad width 328.946px, maximum 512px, gap 16px. Fits the available parent width.

Desktop breakpoint restored after mobile check. Other viewport sizes and published-site behavior were not tested.

## Revert

On this exact element, replace the entire style list with `[rs-member-actions-v2]`. Leave all five child button classes, text and links untouched. Read back the element's style list to confirm restoration. The unused trial class can remain; deleting it is unnecessary. Reverting this one assignment restores the previous full-width stack without changing any shared styles.
