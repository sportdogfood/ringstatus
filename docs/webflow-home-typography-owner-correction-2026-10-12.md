# Home typography owner correction — 12 October 2026

Owner rule: font weight never exceeds 600. Verified Home page 6982268d7543ac3c801512cd, published path /. Page 6aa739aad90e0d765affe666 is Lainey; do not infer page identity from stale iframe URLs.

Hero rs10-h1-xl and rs-h1-xl-text-1-2 now use var(--rs-heading-size, clamp(54px, 12cqw, 132px)), weight600. Removed separate font-size/font-weight declarations from is-white, is-white-2 and is-rust across all seven breakpoints. Home head hero variable likewise uses 12cqw/132px. Remaining heavy Home base classes capped600; related weight overrides cleared. Head 700 rules replaced600 and strong/b set600. Body remains400; controls500. Footer runtime preserved.

Created .l5-paragraph-white.is-xl and .l5-paragrapg-dark.is-xl with var(--rs-paragraph-xl-size, clamp(21px, calc(6px + 1.4vw), 32px)). These are available opt-in variants; no existing paragraph element changed to is-xl. Base paragraph scale remains19–26px.

Native saved-rule readback and head exact readback passed. Actual browser CSS measured all three hero colors at115.198px, weight600 and97.9184px line-height at current desktop width. Not published or locked.
