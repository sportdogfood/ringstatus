# Member Pages Retrofit Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task in this session. No delegation is required.

**Goal:** Complete consistent, responsive native Webflow layouts for all six member pages while preserving existing working behavior and locked shared styles.

**Architecture:** Use RingStatus Barn Onboarding as the behavioral and layout reference, and the locked section inventory as the typography and spacing source. Adapt each existing prototype through separate page-specific layout classes. Keep original interfaces intact until a verified replacement and its exact behavior mapping are ready.

**Tech Stack:** Native Webflow elements, styles and variables through production Webflow MCP; supported browser DOM/computed-style inspection; visual verification last.

**Spec:** Owner instructions in this conversation; `docs/webflow-style-lock-2026-10-12.md`; reference project `appgprj_6ac27216ef888191b7fe22568801d0b8` at `https://ringstatus-barn-onboarding.sportdog.chatgpt.site/`.

## Global Constraints

- Site ID: `6982268b7543ac3c80151266`; never publish without an explicit request.
- Do not modify locked classes, including `fsl-method`, `fsl-vw-copy`, `l5-hp`, heading and paragraph bases. Specialize layouts with separate classes.
- Preserve source copy, form types, runtime IDs, data attributes, event ownership, theme behavior and existing navigation destinations.
- Font weight never exceeds 600. Shared typography comes from the locked contract, with only documented compact member variants.
- Do not turn static prototypes into replacements for working interfaces without verifying all behavior mappings. Do not submit forms or change production member data during testing.
- Every page has a baseline, exact change list, verification results and revert steps. A successful write alone is not completion.

## Review Focus

1. Wider screens: headings, forms and actions must remain bounded together; buttons must not stretch across the viewport.
2. Narrow screens and long labels: no document overflow, clipped text or inaccessible controls; local scrolling only where intended.
3. Light/dark themes: identical geometry and typography; color changes alone.
4. Form and overlay states: preserve input semantics, close/back paths and recovery states; no hidden double padding.
5. Shared selectors: page changes must not alter locked sections or sibling pages through shared styles.

## Task 1: Establish baseline and comparison matrix

**Files:** Create `docs/webflow-member-pages-review-2026-10-12.md` for exact page evidence and rollback. Preserve existing lock and trial documents.

- [ ] Read the lock document and existing trial records; inspect the live element trees, classes, style rules and breakpoint overrides for all six pages.
- [ ] Inspect the supplied reference's matching page and states. Record actual layout bounds and behavior rather than infer them from screenshots.
- [ ] Record each element role, class/combo, font size expression, weight, line height, width/min/max, padding, gap, theme and state overrides in one compact comparison matrix.
- [ ] Identify shared discrepancies and page-specific discrepancies. Select existing approved typography/control styles; avoid new competing token systems.
- [ ] Save original parent/child order, classes and affected settings before each mutation.

## Task 2: RS Member Home — `6ac91a47a4ac5cd8134b81c0`

- [ ] Verify existing `rs-member-launchpad-content` wraps heading and all five actions, with the reference-derived 42rem bound.
- [ ] Verify Schedule, SMS Alerts, Profile, Contact and Log out share text sizing, height, padding and states; preserve primary-action emphasis.
- [ ] Compare prototype destinations and original destinations; document placeholder links and preserve original runtime behavior.
- [ ] Test mobile through wide desktop, long labels and both themes. Accept only when title and action stack remain aligned and bounded with no overflow.

## Task 3: Recognize — `6ac459b135ad0c4c3b253d97`

- [ ] Verify saved main 42rem and preview sheet 40rem bounds against the reference, including stable canvas widths after breakpoint changes.
- [ ] Review all six states: recognized, profile editing, login, recovery, received and unavailable.
- [ ] Remove accidental prototype placeholder copy using exact reference text. Verify heading levels and native input types/labels.
- [ ] Correct compounded section/copy padding through separate Recognize layout classes; preserve locked styles.
- [ ] Verify original recognition open/close/back paths using safe sample state. Keep static preview explicitly separate until its behavior is mapped and verified.
- [ ] Accept only when every state fits mobile, long content scrolls inside the sheet, and both themes retain the same geometry.

## Task 4: Barn setup — `6ac7b2de07b992a509c90029`

- [ ] Review Barn, Users, Riders, Horses and Locations against their reference views.
- [ ] Correct nested section stacks that currently compound gutters; apply bounded page-specific form wrappers.
- [ ] Match selector versus text-input semantics and label/value associations to the reference; verify all 17 prototype fields individually.
- [ ] Review Add barn, Add user, Add rider, Add location and Choose barn previews; preserve tab order and navigation paths.
- [ ] Accept only when tab navigation stays accessible, fields fit the available width, overlays have consistent bounds, and no production save is required for verification.

## Task 5: SMS alerts — `6ac459b4506821bd349ed047`

- [ ] Review enable control, mobile number, Classes, Rider trips, Groom tasks and preferences against the reference.
- [ ] Restore clear label/helper-text hierarchy where the prototype concatenated content; keep copy exact.
- [ ] Verify all checkbox, time and telephone input semantics, label associations, alignment and tap areas.
- [ ] Use separate bounded form/choice layout classes; retain locked typography and spacing expressions.
- [ ] Accept only when toggles and long descriptions wrap correctly at mobile and desktop, and original persistence behavior is preserved without sending SMS or changing saved preferences.

## Task 6: Feed / Notifications — `6ac459b4ad3971b205a5ee11`

- [ ] Compare all 14 cards with the reference; restore structured title, metadata, message and action hierarchy where content was flattened.
- [ ] Match Search, Ring and Rider control types to the reference; do not leave intended selectors as text fields.
- [ ] Review filters, empty results and read-state controls while preserving original data/event hooks.
- [ ] Use separate bounded feed/card/control layout classes. Permit local filter scrolling only when necessary.
- [ ] Accept only when card text remains readable, metadata and actions are aligned, filters fit mobile, and no accidental full-page horizontal scroll occurs.

## Task 7: Schedule — `6ac7d2c7cf113c577ef2a6d7`

- [ ] Review the two-ring/four-class prototype against the reference and inspect the remaining original fixture states.
- [ ] Verify class/ring headers, statuses, entry chips and row alignment at mobile and desktop; restore structured information where needed.
- [ ] Use separate schedule layout classes for grids and local scrolling; do not shrink shared text to make rows fit.
- [ ] Verify both themes and populated/empty/long-label examples available in the source fixtures.
- [ ] Accept only when meaningful schedule data is retained and responsive reflow does not clip labels or force document overflow.

## Task 8: Cross-page acceptance and handoff

- [ ] Check computed rules first at 393, 667, 820, 1279, 1280, 1440 and 1920px. Also check 320px and breakpoint boundaries 479/480, 767/768 and 991/992 for overflow/reflow.
- [ ] Wait for the actual canvas width to match each target before recording measurements; earlier Recognize samples lagged selection.
- [ ] For each page verify heading/paragraph/control typography, width bounds, responsive padding/gaps, minimum 44px control height, wrapping, theme geometry and state overrides. Record exceptions explicitly.
- [ ] Compare locked selector rules before/after and confirm there are no changes. Verify sibling pages do not inherit unintended prototype changes.
- [ ] Inspect screenshots last and save representative mobile/wide-desktop evidence. If capture fails, report visual verification as incomplete rather than claim acceptance.
- [ ] Update the review document with PASS/FAIL/BLOCKED per page, remaining placeholder behavior and exact revert instructions. Update existing Airtable inventory only after inspecting its current schema; do not add more tables or fields.
- [ ] Deliver one concise six-page status report. Leave publishing to the owner's explicit instruction; do not ask the owner to discover defects already covered by these checks.

## Execution policy

Proceed page by page in the order above, completing rules, fixes and verification before advancing. Carry out routine reversible layout corrections independently within the authorized scope. Stop only the affected action if access fails, a locked-base change is necessary, or a behavioral replacement requires a decision not already supplied. Continue unrelated authorized work. Do not repeatedly patch a failed approach; compare with the saved baseline and establish the cause first.
