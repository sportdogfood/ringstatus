# README Handoff v2

## Purpose

This package is the locked RS skeleton base for creating new page skeletons.

It is intended for both humans and runners. The first task is always to copy the approved base exactly before creating a targeted page.

## Files

- `00_APPROVED_BASE_DO_NOT_REBUILD.html`  
  Hard source of truth. Use this first.

- `rs-standalone.html`  
  Browser-ready standalone version. References `rs-global.css` and `rs-scripts.js`.

- `rs-webflow-embed.html`  
  Webflow body/embed version. CSS and JS must be loaded separately.

- `rs-global.css`  
  All global layout, typography, nav, section, wildcard, and component styles.

- `rs-scripts.js`  
  All JS behavior for nav/drawer/list interactions.

- `README_NEW_SKELETON_PROMPT.md`  
  Prompt to use when starting a new targeted skeleton.

- `VALIDATION_CHECKLIST.md`  
  Required checks before modifying a page.

- `CONTRACT.json`  
  Machine-readable gates and forbidden sources.

## Correct workflow

1. Unzip this package.
2. Open `00_APPROVED_BASE_DO_NOT_REBUILD.html`.
3. Copy it exactly to the new project/page.
4. Keep `rs-global.css` and `rs-scripts.js` separate.
5. Run the validation checklist.
6. Only then create the targeted page variation.

## Current master_ks adjustment

This repository copy removes diagnostic shell outlines from the vanilla base. The section wrappers still own the same layout responsibilities, but the visual area lines on `rs-main`, `rs-section`, `rs-section-container`, `rs-section-padding`, `rs-content-container`, `rs-content-flex`, `rs-content`, and the mock footer wrappers are not part of the production kitchen sink.

Intentional component borders remain on controls, cards, drawers, lists, and other UI pieces.

## Wrong workflow

- Do not build from memory.
- Do not use old canvas exercises.
- Do not use the shaded/color teaching exercise.
- Do not rewrite the CSS from notes.
- Do not strip sticky behavior.
- Do not change font rules.
- Do not inline JS unless making a temporary preview.

## Webflow use

For Webflow:

1. Load `rs-global.css` in site/page head.
2. Paste `rs-webflow-embed.html` into the page/body area.
3. Load `rs-scripts.js` before `</body>` or in footer custom code.
4. Do not paste the standalone `<html>`, `<head>`, or `<body>` wrapper into Webflow.
