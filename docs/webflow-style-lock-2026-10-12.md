# Webflow shared style baseline — locked 12 October 2026

Owner confirmation: “lets lock”. This freezes the agreed shared section and typography system. Future work must reuse these blocks and base classes; specialize through existing combo classes, with no more than three classes per specialized element where practical. Changes to the baseline require an explicit owner request.

## Scope

RingStatus site `6982268b7543ac3c80151266`. Home `/` (`6982268d7543ac3c801512cd`), Lainey (`6aa739aad90e0d765affe666`), and the adapted `lm` page (`6a987d7a71da602c33cb1d78`). Tools and About should reuse this baseline when retrofitted; this lock does not assert every downstream element has already been converted.

## Pages containing locked sections

| Page | Path | Webflow page ID |
| --- | --- | --- |
| Home | `/` | `6982268d7543ac3c801512cd` |
| Lainey | `/lainey` | `6aa739aad90e0d765affe666` |
| LM | `/lm` | `6a987d7a71da602c33cb1d78` |

These pages contain the locked section patterns. This inventory identifies reuse of the baseline; it does not assert that every section on each page has been converted.

## Standard section

`ls-section-content → fsl-method → fsl-vw-panel → fsl-vw-copy → l5-kicker + l5-hp`. Inside `l5-hp`, use `l5-h` for headings and `l5-p` for paragraphs. Tabs are a sibling of copy inside the panel. CMS remains an outer data wrapper. Portfolio retains its own structure with shared typography and spacing. Nav and hero retain their necessary structural differences.

## Latest accepted values

- Restore the original section gutters: `var(--rs-gutter, clamp(16px, 4.071vw, 65px))`. Remove the extra gutter override on `fsl-method no-margin is-rs`.
- `fsl-vw-copy` left/right padding: `clamp(16px, 2vw, 32px)`.
- Shared heading parent `l5-h`: `var(--rs-heading-parent-width, clamp(60rem, 82vw, 90rem))`, constrained to its available parent width. Heading children use 100% width. The 90rem ceiling remains part of this accepted value.
- Method gap: `var(--rs-method-gap, clamp(24px, 2.78vw, 40px))`.
- Copy and `l5-hp` gap: `var(--rs-copy-gap, clamp(16px, 1.67vw, 24px))`.
- Headings: weight 600 maximum, line height `.85`; light/dark versions share sizing. Base fallback `clamp(54px, 10cqw, 116px)`; XL fallback `clamp(54px, 12cqw, 132px)`.
- Paragraphs: weight 400; light/dark versions share sizing. Base fallback `clamp(19px, calc(5.3px + 1.08vw), 26px)`; XL fallback `clamp(21px, calc(6px + 1.4vw), 32px)`.
- Kicker, message, button and tab families share control typography and sizing. Color, width, alignment and interaction states belong to their appropriate combos/states. `rs-shared-control-text` inherits the parent font size.
- Kicker: left aligned, uppercase, minimum 50% width. User message: left aligned, minimum 50%, maximum 72%. Bot message: right aligned, minimum 50%, maximum 82%. All remain constrained to their parent.
- Shared control corner pattern: top-left 18px, top-right 18px, bottom-left 0, bottom-right 18px. `is-lainey` intentionally removes the kicker box treatment.

## Preserved evidence

Existing evidence remains in this repository: `webflow-home-exact-control-contract-2026-10-12.md/.json`, `webflow-home-fluid-gaps-control-inheritance-2026-10-12.md`, `webflow-home-control-layout-applied-2026-10-12.json/.png`, `webflow-home-typography-owner-correction-2026-10-12.md`, and `webflow-css-audit.md`. These are historical checkpoints, not a fresh complete export of this lock.

Latest gutter, copy-padding and 82vw changes were saved and read back in the preceding work. Home's existing head stylesheet was reconciled with those values. No publishing was performed by Codex.

## Verification boundaries and follow-up

The owner approved freezing the agreed baseline. This is a documented contract, not a Webflow editing permission lock or a claim that every viewport has passed a new visual audit.

A fresh native style export was attempted at lock time; both the full-breakpoint request and base-only request returned `Transport closed`. Existing evidence is preserved; a complete fresh export remains pending. Airtable has not been updated for this lock.

Known items for a later explicit correction: verify the owner's requested 18px shared kicker ceiling against the saved 16px fallback; reconcile Home's XL variable alias; inspect nested copy wrappers and the remaining legacy control in the `lm` ring section. Do not silently change these while recording the lock.

### Home correction verified — 13 October 2026

Owner explicitly authorized Home-first corrections. The18px shared control ceiling, independentXL heading alias, Home split-copy wrapper reuse, current-tab corners and white-only mobile paragraph margin are now corrected. Home Designer/custom-code Preview checks and remaining acceptance boundaries are recorded in `webflow-home-consistency-2026-10-13.md` and its Preview JSON. This supplements the historical baseline; it is not a new owner lock, component conversion or downstream-page acceptance. LM's remaining legacy control remains outside this correction.
# Owner correction addendum — 13 October 2026

The owner's latest Home corrections supersede earlier global `fsl-vw-copy` padding and fixed control-height values. Copy and tabs now have zero global padding; controls scale from 44px at desktop to 52px at 1920, with shared 14–18px text. The three large wordmark parts share `clamp(24px, 7.5vw, 144px)` and .85 line-height. Full requests, implementation and verification are recorded in `webflow-home-owner-corrections-2026-10-13.md`. This addendum records authorized corrections; it does not declare a new owner lock or component conversion.

