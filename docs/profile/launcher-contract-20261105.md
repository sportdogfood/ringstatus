# Launcher behavior contract — 5 November 2026

Baseline: 619ed8de, including the existing owner's shared Schedule changes.
Approved correction: launcher placement, active state, width and overflow only. Identity, persistence, loading cadence and Schedule destination remain unchanged.

- Signed out: only the existing Recognize toggle is shown.
- Recognized: Profile toggles the widget. Its active and expanded states follow visibility, including Close and Escape.
- Profile remains outside the horizontally scrolling destinations. All ten existing actions remain available; trailing padding keeps Logout clear of the edge.
- Desktop: right aligned, 393px rendered launcher width; existing Profile desktop scale remains unchanged.
- Mobile: full bleed. While open, the launcher is immediately above the existing footer inside the shared shell. While closed, the launcher remains available at the bottom. The footer retains Not you?.
- Screen changes and viewport changes retain one launcher, never duplicate it.
- Schedule retains its existing separate Astro destination. No auth, data contract or Schedule code changed.
- Desktop Expand/focus mode is a proposed follow-up, not included in this release.

Local validation: 393x852 and 1440x1000 browser viewports; active/open/Close, viewport transitions, ten actions, horizontal overflow with 20px trailing clearance and fixed Profile position, logout to Recognize and sign-in. Build and 12 relevant automated checks passed.
Rollback: revert only this release commit and push the established deployment branch. Preserve subsequent owner changes.
