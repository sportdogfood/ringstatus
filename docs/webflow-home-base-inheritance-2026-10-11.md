# Home base inheritance correction — 11 October 2026

Cleared 162 targeted sets of breakpoint overrides from shared typography, section spacing, tab layout, inputs, footer layout, and message base classes. Base declarations now carry through rather than seven separately copied values. The tool's documented `update_style.remove_properties` operation was validated by successful tool responses; original unrelated properties were preserved.

Six split-section H2 elements now use one `rs-shared-h2` base class: `clamp(38px, 3vw, 54px)`, line-height .85. Shared control text uses 14–16px, weight 500, line-height 1.2. Navigation and submit base typography was aligned to these values.

The owner's current white heading expression, `clamp(54px, 10cqw, 116px)`, was retained and applied to the dark heading base class. The isolated l5-h parent-width experiment was not modified.

Evidence: `webflow-home-base-inheritance-checks-2026-10-11.json`. Those measurements preceded the final dark-heading formula alignment and must not be treated as verification of that last change.

Remaining: message color combos still override padding; stable MCP cannot uniquely target duplicate-name combos. Page-head custom CSS and hero typography still require reconciliation. A native custom-property definition was rejected with Invalid style property and was not counted as saved. No publish performed. This is progress, not completed whole-page acceptance.
