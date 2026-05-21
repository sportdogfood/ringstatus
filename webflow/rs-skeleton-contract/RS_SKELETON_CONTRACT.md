# RS Skeleton Contract

Version: 2026-05-21  
Status: LOCKED BASE CONTRACT

## Locked Shell

Every standard section must keep this wrapper order:

```html
<section class="rs-section [modifier]">
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

## Layer Ownership

| Layer | Owns |
|---|---|
| `rs-section` | full-width background, section min-height, section boundary |
| `rs-section-container` | max-width centered page boundary |
| `rs-section-padding` | the only responsive outside padding owner |
| `rs-content-container` | vertical alignment context |
| `rs-content-flex` | split/stack/grid/carousel layout behavior |
| `rs-content` | readable copy/content width |

## Non-Negotiable Rules

- Do not change the wrapper order.
- Do not compound horizontal padding across nested wrappers.
- Do not place global CSS inside individual sections.
- Do not place JavaScript inline unless testing.
- Do not create a separate mobile nav.
- Use modifier classes for new layout behavior.

## Typography Contract

- H: weight `600`, line-height `0.95`, letter-spacing `-0.05em`
- P: weight `400`, line-height `1.23`
- T/small: weight `400`, line-height `1.6`

Tokens:

```css
--rs-h1
--rs-h2
--rs-h3
--rs-p
--rs-t
```

## Responsive Contract

At `max-width: 767px`:

- split sections stack
- grids collapse to one column
- wildcard sticky becomes normal flow
- list rows stack
- nav remains the same nav, not a separate mobile nav

## Nav Contract

Required IDs:

```txt
data-mega-toggle
data-mega-menu
data-drawer-toggle
data-drawer-menu
data-drawer-close
data-nav-scrim
```

Behavior:

- Apps opens full-width mega menu.
- Tools opens right drawer.
- Drawer locks body scroll with `body.is-nav-locked`.
- Escape closes open menu/drawer.

## File Contract

```txt
rs-global.css       global stylesheet only
rs-scripts.js       all JavaScript only
rs-standalone.html  local preview
```

## Included Template Groups

- Base split sections
- Nav mega and drawer behavior
- H/P/T typography examples
