# README New Skeleton Prompt v2

Use this prompt when starting a new targeted skeleton.

---

You are creating a new targeted skeleton from the locked RS Skeleton Contract v2 package.

## Mandatory source

Use only the files inside `rs-skeleton-contract-v2.zip`.

Do not use:
- any earlier colored/shaded teaching exercise
- any prior diagnostic canvas
- any loose file from Downloads
- any memory reconstruction of the skeleton
- any simplified approximation

## First action

Copy `00_APPROVED_BASE_DO_NOT_REBUILD.html` exactly as the starting point.

Do not generate a replacement skeleton. Do not restyle the base. Do not simplify sticky sections. Do not change typography.

## Required files

Keep these files separate:

- `rs-global.css`
- `rs-scripts.js`
- targeted standalone HTML file
- targeted Webflow embed HTML file, if requested

## Validation before edits

Before making the targeted page, confirm these remain present:

- `font-family: Outfit, Inter, Arial, sans-serif`
- `--rs-h1`, `--rs-h2`, `--rs-p`, `--rs-t`
- heading weight `600`
- heading line-height `0.95`
- heading letter-spacing `-0.05em`
- paragraph line-height `1.23`
- small/T text line-height `1.6`
- `.rs-wildcard-sticky` with `position: sticky`
- `.rs-section.is-wildcard` with `min-height: 260svh`
- `.rs-wildcard-scroll-layer` with `margin-top: -100svh`
- nav drawer viewport lock using `body.is-nav-locked`
- separate `rs-scripts.js`

## Allowed edits after validation

After validation passes, you may:

- remove unneeded demo groups
- reorder approved section components
- change content text
- create page-specific section order
- add page-specific modifier classes

## Forbidden edits

Do not:

- change the wrapper contract
- add horizontal padding to multiple nested wrappers
- remove the sticky wildcard mechanics
- alter global typography
- replace the nav pattern
- create a separate mobile nav
- inline the JS into production files
- use the old shaded/colored exercise
