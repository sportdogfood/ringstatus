# RingStatus member web app and PWA — researched design brief

Date: 12 October 2026. Proposed design, not implementation or a claim of PWA readiness.

## Purpose and sources

Refine the existing Member Home, Recognize, Barn setup, Schedule, SMS alerts and Feed into one consistent responsive app. Preserve the original RingStatus Barn Onboarding's flows, copy, source assets and behavior, with the locked RingStatus style contract. No new product features or backend replacement.

Original: `https://ringstatus-barn-onboarding.sportdog.chatgpt.site/`, project `appgprj_6ac27216ef888191b7fe22568801d0b8`. Brand source: `docs/webflow-style-lock-2026-10-12.md`. Execution checklist: `docs/superpowers/plans/2026-10-12-member-pages-retrofit.md`.

## Design direction

Use a restrained utility app: RingStatus logo, Outfit type, existing rust primary accent, original light/dark palette, clear content hierarchy and the accepted asymmetric control corners. No decorative gradients, oversized marketing headings, new icon vocabulary or dashboard inventions. Modernization means better spacing, bounded content, legible data, meaningful states and reliable interactions.

Shared brand controls retain the same font expression, maximum weight 600, radius, padding and minimum height; primary, secondary, selected, hover, focus and disabled treatments are explicit. Theme changes affect semantic colors, not layout. The locked marketing heading/paragraph rules remain untouched; compact member variants already used in the prototypes must be verified and documented as app-specific roles rather than silently applied to brand bases.

## Responsive layout decisions

- Keep one consistent app header and existing navigation destinations. Use a compact accessible navigation treatment when the actual labels no longer fit; derive that treatment from the original before adding one.
- Member Home and Recognize main: reference-derived 42rem maximum content width, including heading and actions together. Launchpad remains a compact vertical action stack; no full-desktop-width five-button strip.
- Recognize dialog: reference-derived 40rem desktop maximum. Mobile adapts to available width and safe areas. Long content scrolls inside the dialog; close control remains reachable.
- Barn setup and SMS alerts: bounded single-column forms. Add desktop field pairs only when the source groups them and labels remain readable; do not turn every form into a wide grid.
- Schedule and Feed: allow a wider data workspace, with the exact maximum selected from source measurements and real content. Preserve schedule rows and feed hierarchy. Restrict horizontal scrolling to genuinely two-dimensional schedule regions or overflowing filter/tab menus, never the document.
- Fluid gutters and gaps inherit the locked expressions where appropriate; use page-specific wrappers to prevent compounded padding. Width changes must not independently alter control typography.
- Test continuous reflow and breakpoint boundaries, not only seven screenshots. Long labels, increased text size, virtual keyboard and landscape are part of acceptance.

## Component inventory

One app header/navigation; one compact page-heading stack; one action/control family with explicit states; one labeled input family with helper/error treatment; one choice row; one bounded dialog/sheet; one tab/filter row; structured schedule rows; structured feed cards; loading, empty, error and success states from the original. Each specialized layout gets a separate class; no locked base mutations.

## PWA delivery and platform boundaries

1. Verify current app origin, route ownership, existing manifest/service worker, authentication and working deployment before planning files or hosting changes.
2. Preserve existing backend and runtime. Do not replace working implementation to obtain installability.
3. Define manifest identity, app name, existing brand icons, start URL, scope and standalone display for the six routes. Test browser and installed launch paths separately.
4. For offline behavior, verify a same-origin service-worker URL, valid JavaScript response and allowed scope. A script hosted on an unrelated CDN cannot control these pages. Do not assume the current Webflow host can serve the required route/headers.
5. Initial offline scope is an app shell and honest unavailable state. Do not invent offline form submission, cached identity or stale schedule data presented as current. Broader data caching requires explicit retention and freshness decisions based on existing behavior.
6. Preserve SMS functionality; web push is not required or added by this brief. Installability and service-worker support vary by browser, so actual target-device checks are required.
7. Build macOS Apps offers useful desktop layout/state guidance, but SwiftUI/AppKit are not the implementation technology for this web app. No native macOS project is proposed.

## Verification

Rules first: native element structure, selectors and overrides, semantic inputs, theme tokens, computed typography, widths, gaps and padding. Then safe sample interactions, keyboard/focus, labels/errors, modal focus restoration and viewport overflow. Finally compare source and implementation screenshots at matching viewport/state.

Cover 320, 393, 667, 820, 1279, 1280, 1440 and 1920px plus breakpoint boundaries; test 200% text resizing and 400% zoom/reflow where applicable. Our control-height target is at least 44px; WCAG 2.2's minimum target criterion is 24 CSS pixels with stated exceptions, not a blanket 44px requirement. Color pairs must meet relevant contrast thresholds. Test real iPhone browser/installed behavior before claiming iPhone acceptance; desktop emulation is not equivalent.

Record PASS/FAIL/BLOCKED per page and PWA capability. No publish, runtime replacement or hosting migration is implied by this design brief.

## Research sources

- MDN installability and browser differences: https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps/Guides/Making_PWAs_installable
- MDN service-worker same-origin URL and scope: https://developer.mozilla.org/en-US/docs/Web/API/ServiceWorkerContainer/register
- W3C reflow, 320 CSS pixel baseline: https://www.w3.org/WAI/WCAG21/Understanding/reflow
- W3C target-size minimum: https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum.html

No official source inspected establishes that this particular Webflow deployment currently supports PWA files, scope or offline behavior. This remains a specific hosting verification item.
