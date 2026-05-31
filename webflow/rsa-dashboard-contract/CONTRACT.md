# RSA Dashboard Contract

This component is intentionally assembled from three inputs:

- `C:\Users\gombc\Downloads\rsa-stylesheets.webflow (4)\` supplies the original Webflow skeleton, stacked block cadence, and table shape.
- `C:\Users\gombc\Downloads\rsa-dashboard-contract-preview-v0-14.html` and `C:\Users\gombc\Downloads\rsa-dashboard-contract-v0-14.css` supply the v0.14 styling contract, typography classes, clamp rules, and mobile width behavior.
- Session `019e4074-dbbd-7490-95d4-fbd236d046ed` supplies the approved modal/edit/writeback shape.

## Locked Typography Mapping

- Header: `rsa-H1`, `rsa-text`
- Tabs/actions: `rsa-text is-link`
- Table head: `rsa-text is-xs`
- Item names: `rsa-text is-line-item`
- Inline edit: `rsa-text is-inline-edit`
- Numbers: `rsa-text is-number`
- Input: `rsa-text is-inline-input is-link`
- Footer: `rsa-text is-xs`

## Behavior Boundary

`rsa-dashboard.js` owns only:

- search open/close
- filter open/close
- tab active state
- inline quantity input
- sticky table head
- print
- detail modal
- simple `rsa-comment-panel`
- Airtable write adapter boundary

Airtable remains the source of truth. Inline edits, modal edits, and comments must write through `window.RSA_DASHBOARD_CONFIG.apiUrl`. Until the real endpoint/schema is confirmed, `apiUrl` is intentionally blank so failed writes are visible instead of silently appearing saved.
