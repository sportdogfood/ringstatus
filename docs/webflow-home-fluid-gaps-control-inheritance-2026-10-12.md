# Home fluid gaps and control inheritance — 12 October 2026

Home 6982268d7543ac3c801512cd. Saved native class rules and reconciled existing Home head CSS; footer unchanged. Not published.

fsl-method (including all combos), bqw-container-1 and bqw-b-wrap-1: var(--rs-method-gap, clamp(24px, 2.78vw, 40px)). fsl-vw-copy, fs1-copy and l5-hp: var(--rs-copy-gap, clamp(16px, 1.67vw, 24px)). Cleared breakpoint/combo gap overrides. Existing fs1-copy elements retained but share the copy gap; no structural migration performed.

Desktop split widths now divide remaining width after the gap using original49:45 proportions: text calc((100% - method-gap)*.521277); image calc((100% - method-gap)*.478723). Existing mobile stacking/auto widths retained.

rs-shared-control-text typography now inherits font family, size, weight, line height and tracking from its message/tab parent. Removed independent breakpoint typography and matching head-CSS declarations. Kicker2 and message parents still use shared14–16px scale. Actual browser CSS confirms14px/500/16.8px line/1.05px tracking at smaller viewport and16px/500/19.2px line/1.2px tracking at1440px. Case differs deliberately: kicker uppercase, message text original case. No measured size conflict remained in those checks.

Actual browser computed gaps confirm method24/copy16 at smaller viewport, method40/copy24 at1440px, including split combos. Evidence JSON: webflow-home-fluid-gap-controls-2026-10-12.json and webflow-home-fluid-gap-controls-desktop-2026-10-12.json. Fresh screenshot: webflow-home-gaps-control-type-verified-2026-10-12.png.

Tool behavior: clearing and setting the same property in one update_style action leaves it cleared. Clear obsolete declarations first, then set replacements in a subsequent action; final browser checks caught and resolved this before delivery.
