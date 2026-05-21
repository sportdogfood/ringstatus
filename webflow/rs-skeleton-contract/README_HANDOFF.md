# RS Skeleton Handoff

Version: 2026-05-21
Status: Runtime base only

## Runtime Files

- `rs-standalone.html` - local preview and canonical base markup.
- `rs-global.css` - global stylesheet only.
- `rs-scripts.js` - JavaScript behaviors only.

Use these three files as the runtime source of truth.

## First Review

Open:

```txt
rs-standalone.html
```

## Core Shell

```txt
rs-section
  rs-section-container
    rs-section-padding
      rs-content-container
        rs-content-flex
          rs-content
```

## Layer Meaning

- `rs-section`: full-width background and section height.
- `rs-section-container`: max-width page boundary.
- `rs-section-padding`: one smart padding layer.
- `rs-content-container`: alignment context.
- `rs-content-flex`: layout behavior.
- `rs-content`: readable content width.

## Do Not Do

- Do not duplicate global CSS in HTML sections.
- Do not change wrapper order.
- Do not add side padding to multiple wrappers.
- Do not create a different mobile nav.
- Do not add inline JavaScript.
- Do not reintroduce diagnostic template styling into the runtime base.

## Runner Checklist

- [ ] `rs-standalone.html` links to `rs-global.css`.
- [ ] `rs-standalone.html` links to `rs-scripts.js`.
- [ ] CSS is separate.
- [ ] JS is separate.
- [ ] Wrapper order is preserved.
- [ ] Padding only lives on `rs-section-padding`.
- [ ] H/P/T typography appears.
- [ ] Nav opens mega and drawer.
- [ ] Drawer locks viewport.
- [ ] Mobile stacks cleanly.
