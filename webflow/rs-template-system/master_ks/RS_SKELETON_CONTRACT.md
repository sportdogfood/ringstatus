# RS Skeleton Contract v2

Status: LOCKED CONTRACT

This package replaces the prior handoff language. The runner must use this package as the only source for the approved RS skeleton.

## Source of truth order

1. `00_APPROVED_BASE_DO_NOT_REBUILD.html`
2. `rs-standalone.html`
3. `rs-global.css`
4. `rs-scripts.js`
5. `rs-webflow-embed.html`

The approved base must be copied first. Do not recreate it from memory, previous canvas history, or earlier teaching exercises.

## Deprecated / forbidden sources

Do not use or reference:

- the colored / shaded teaching exercise
- any earlier diagnostic canvas
- any loose `Downloads/rs-scripts.js`
- any reconstructed version of the skeleton
- any simplified approximation of `rs-standalone.html`

The shaded exercise was educational only and is not a design base.

## Non-negotiable shell order

Every standard section must preserve this wrapper order:

```html
<section class="rs-section ...">
  <div class="rs-section-container">
    <div class="rs-section-padding">
      <div class="rs-content-container">
        <div class="rs-content-flex">
          <div class="rs-content">...</div>
        </div>
      </div>
    </div>
  </div>
</section>
```

## Layout ownership rules

- `rs-section` owns full-width background and section boundary.
- `rs-section-container` owns max-width and centering.
- `rs-section-padding` owns responsive outer spacing.
- `rs-content-container` owns vertical alignment.
- `rs-content-flex` owns row/column layout.
- `rs-content` owns readable content width.
- Horizontal padding must not compound across wrappers.

## Typography rules

Do not alter the global typography system unless explicitly asked.

Required base:

- Font stack: `Outfit, Inter, Arial, sans-serif`
- H weight: `600`
- H line-height: `0.95`
- H letter-spacing: `-0.05em`
- P weight: `400`
- P line-height: `1.23`
- T/small text line-height: `1.6`
- Tokens: `--rs-h1`, `--rs-h2`, `--rs-p`, `--rs-t`

## Nav rules

The nav is part of the locked base:

- bottom/fixed logo
- four nav controls
- first dropdown opens full-width mega
- second dropdown opens right drawer
- drawer locks viewport using `body.is-nav-locked`
- no separate mobile nav/hamburger version

## Wildcard sticky rules

Wildcard sections must preserve the sticky pattern:

- `.rs-section.is-wildcard { min-height: 260svh; }`
- `.rs-wildcard-sticky { position: sticky; top: 0; }`
- `.rs-wildcard-scroll-layer { margin-top: -100svh; }`
- left card rail remains under 50% viewport width on desktop
- sticky releases when the parent section ends and the next normal section enters
- mobile can disable sticky and stack normally

## JS rules

All JavaScript belongs in `rs-scripts.js`.

Required behaviors:

- mega menu open/close
- drawer open/close
- viewport lock for drawer
- Escape key closes nav UI
- expandable list with `data-list-limit`

## Build rule

For a new targeted skeleton, first duplicate the approved base exactly. After validation passes, remove or rearrange only page sections that are not needed. Never rebuild the base from scratch.
