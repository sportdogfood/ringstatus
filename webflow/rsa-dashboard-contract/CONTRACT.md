# RSA Dashboard Contract

This component is intentionally assembled from three inputs:

- `C:\Users\gombc\Downloads\rsa-stylesheets.webflow (4)\` supplies the original Webflow skeleton, stacked block cadence, and table shape.
- `C:\Users\gombc\Downloads\rsa-dashboard-contract-preview-v0-14.html` and `C:\Users\gombc\Downloads\rsa-dashboard-contract-v0-14.css` supply the v0.14 styling contract, typography classes, clamp rules, and mobile width behavior.
- Session `019e4074-dbbd-7490-95d4-fbd236d046ed` supplies the approved modal/edit/writeback shape.

## First Template Skeleton References

The cleaned HTML keeps the first export's block cadence and class shape while removing Webflow-generated classes:

- `rsa-dashboard > rsa-dashboard-block > rsa-dashboard-container > rsa-main-grid`
- `rsa-top`, `rsa-actions`, `rsa-body`, `rsa-content`
- `rsa-padding`, `rsa-padding-bottom`, `rsa-banner-header`
- first-template row source: `rsa-item-row-2 is-grid2` mapped to `rsa-item-row is-category|is-table-head|is-item`
- first-template blocks: `rsa-item-block-left/right` mapped to `rsa-item-block is-left/right`
- first-template quantity grid: `rs-quantity-block-2 is-grid4` mapped to `rsa-quantity-block is-grid4`
- first-template action labels stay as template placeholders unless the real Airtable/source schema approves a final label set.

## Modal References

The modal is the approved prior pattern translated into RSA classes, not a new modal system:

- prior `lp-modal`, `lp-modal-backdrop`, `lp-modal-card`, `lp-modal-close`
- RSA translation: `rsa-modal`, `rsa-modal-backdrop`, `rsa-modal-card`, `rsa-modal-close`
- prior detail/edit cadence maps to `rsa-detail`, `rsa-detail-list`, `rsa-detail-row`, `rsa-edit-panel`, `rsa-edit-choice`, `rsa-edit-pill`
- write state follows the prior save boundary: pending/success/error must be visible and failed writes cannot look saved.

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

`rsa-webflow-embed.html` loads `rsa-dashboard.html` as the skeleton template and then binds `rsa-dashboard.js`. Do not duplicate or hand-edit the embed markup separately from the skeleton file.
