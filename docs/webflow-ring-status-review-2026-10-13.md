# Ring Status shared-style review — 13 October 2026

Page `/ring-status`, ID `6ab11bd52642dca0c8e74004`. Home and brand.md are the baseline. No new typography/spacing scale was introduced.

## Applied

- Created active Airtable `rs-brand-styles` (`tblUqEn2npCQEm0uP`) in base `appara5767XpIaJIf`; seeded38 shared rules, then added3 mappings for the existing Ring Status hero and split-media heading/paragraph roles. Deprecated `___` tables were not used as active authority.
- Reassigned30 existing element class lists: six `fs1-copy` wrappers now use `fsl-vw-copy`; one nested copy panel now uses the copy class; four tab-panel wrappers now use `fsl-vw-tabs`; primary section paragraphs use the existing XL paragraph combo.
- Moved four existing tab wrappers inside the corresponding shared panel, beside its copy. No element was deleted and all410 nodes remained in the tree.
- Replaced the existing stale page contract with the current Home contract, preserving all content outside that style block, including canonical metadata and scripts. The stale block had16px control caps,44px fixed minima, different gutters and heading line-height1.
- Mapped `rs6-hxl` and the three used color combos to the existing Home XL expression, weight600 and line-height.85. Removed their effective fixed150px/130px sizes by replacing them with the shared expression across breakpoints. Color differences retained.

## Verification and limits

Fresh Webflow readback: all30 class assignments matched; all4 tab moves matched; remaining `fs1-copy` count0; tree count410; saved head matched intended replacement exactly.

Native Designer checks after hero combo correction:

| Actual width | Page scroll width | Core control text | Minimum control height | Copy/tab padding | Base section heading / XL hero |
| --- | --- | --- | --- | --- | --- |
|393|393|14px|44px|0|54px /54px|
|667|667|14px|44px|0|61.28px /73.54px|
|820|820|14px|44px|0|75.31px /90.37px|

All visible core control families matched at these widths, and all measured headings used600 with.85 line height. Initial desktop read identified the fixed hero combo sizes and was superseded by their correction; do not treat that initial read as final acceptance. The subsequent wide-breakpoint browser verification timed out and must be completed. Refreshed custom-code Preview, all four tab interactions and final visuals are also pending. No publication or final stylesheet lock.

Before evidence: `webflow-ring-status-2026-10-13-before.json`. Home remains the authority for existing shared roles. About, Recognize, Lainey and LM have not been newly corrected in this execution pass.
