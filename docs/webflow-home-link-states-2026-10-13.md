# Navigation and tab states — 13 October 2026

Owner reported Hover and Current problems as section backgrounds change.

Native evidence: tab hover applied `invert(100%)`; navigation hover reversed bottom corners and inherited text color. Removed inversion and normalized hover colors to brand brown `#46332b` with white text and 18/18/18/0 corners. Applied the same hover declarations to existing rust/error/color-9 tab combos and dark nav combo so specificity cannot restore conflicting colors.

Existing Home custom contract now sets Current to rust `#984924` with white text, Hover to brown with white text, and Current+Hover to brown with white text. All retain shared font, padding, height and border shape. Focus-visible receives an explicit outline. No publication.

The owner moved Designer to Ring Status (`6ab11bd52642dca0c8e74004`) during this turn. Shared native hover definitions apply there; only Home custom code was edited. Native `rs6-tab-link is-rust Current` had reversed bottom corners: bottom-left18/bottom-right0. Corrected through Designer to bottom-left0/bottom-right18, retaining its contrasting black background and white text.

Verification: read back native hover rules and exact saved Home head. On the open Ring Status desktop canvas, a non-current rust tab hovered to brown `rgb(70,51,43)` with white text, no filter, matching corner shape and unchanged height. Hovered Current rust tabs retained black/white contrast; hovered Current nav retained its selected background with white text. All measured Current and Hover corners matched18/18/18/0. Screenshot: `C:/Users/gombc/AppData/Local/Temp/rs-link-states-20261013.png`. Home's updated Current custom rule was verified as saved source; it was not independently reloaded in Preview this turn. No other page custom-code block or page structure was edited, and nothing was published.
