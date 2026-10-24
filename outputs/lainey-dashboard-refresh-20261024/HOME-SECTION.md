# Home Latest and Horses

Saved in Webflow; not published.

- Component: LP Portfolio / LP Home Latest and Horses (`08d242da-6844-662f-280f-8153f5548a6e`).
- Home page: `/lainey` (`6aa739aad90e0d765affe666`). Instance `a69b1cb9-a456-0e59-d039-d50bf1495e82`, after final content section and before footer.
- Reuses original dashboard markup/styles and shared runtime. Original LP Shared Dashboard V3 markup verified unchanged.
- Only Latest and Horses tabs; Latest defaults to the established 42-day window; Rider columns hidden only in this variant.
- Horses reads complete canonical sourceRows, independent of the Latest window. Horse-row filtering disabled in this variant, since it has no visible filter controls.
- Current published source audit: 321 class rows, 22 horse helpers, 22 matched horse groups, zero unmatched rows. Class totals sum to 321. Source-derived totals: 45 wins, 127 top-three, 213 top-eight.
- Local browser fixture: 321 records, 22 horses, 35 Latest records; both tabs rendered and Rider absent. This fixture checks behavior, not current live results.
- 12 targeted tests pass, including refresh alignment, full-history horse scope, preserved original aggregate function and original dashboard filtering.
- Saved component code and site footer readback match; uploaded runtime bytes match. Designer snapshot returned status false; live Home rendering remains unverified pending publication.
- Shared runtime asset: https://cdn.prod.website-files.com/6982268b7543ac3c80151266/6adca2569b09707657ccb62b_lp-shared-dashboard-runtime-v4-home-20261024.js

Local artifacts: `home-section.html`, `home-site-footer.html`, `dashboard.js`, `baseline-before-home.js`, and `refresh.test.cjs` in this directory. The loader refresh change from the previous step is retained. No CMS records, original component markup, unrelated files, or publishing changed in this step.
