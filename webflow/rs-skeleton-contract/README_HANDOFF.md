# README_HANDOFF.md

## RS Skeleton Handoff

Version: 2026-05-21  
Status: Contract locked

## Files

- `RS_SKELETON_CONTRACT.md` — locked rules.
- `README_HANDOFF.md` — human/runner handoff.
- `README_NEW_SKELETON_PROMPT.md` — prompt for starting a new skeleton project.
- `rs-global.css` — global stylesheet.
- `rs-scripts.js` — JavaScript behaviors.
- `rs-standalone.html` — local browser preview.
- `rs-webflow-embed.html` — Webflow embed reference.

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

## Webflow Setup

Load CSS globally:

```html
<link rel="stylesheet" href="https://YOUR-HOST/rs-global.css">
```

Load JS globally:

```html
<script defer src="https://YOUR-HOST/rs-scripts.js"></script>
```

Then paste section HTML from:

```txt
rs-webflow-embed.html
```

## Do Not Do

- Do not duplicate global CSS in every embed.
- Do not change wrapper order.
- Do not add side padding to multiple wrappers.
- Do not create a different mobile nav.
- Do not add JS for static layout.
- Do not modify global variables unless versioning the contract.

## Runner Checklist

- [ ] CSS is separate.
- [ ] JS is separate.
- [ ] Standalone preview opens locally.
- [ ] Webflow embed is HTML-only reference.
- [ ] Wrapper order is preserved.
- [ ] Padding only lives on `rs-section-padding`.
- [ ] H/P/T typography appears.
- [ ] Nav mega opens.
- [ ] Drawer opens and locks viewport.
- [ ] Expanding list reveals 10 rows at a time.
- [ ] Mobile stacks cleanly.
