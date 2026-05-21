# README_NEW_SKELETON_PROMPT.md

## Ready-To-Paste Prompt

```txt
Create a new RS skeleton project from the locked files:

- RS_SKELETON_CONTRACT.md
- README_HANDOFF.md
- rs-global.css
- rs-scripts.js
- rs-standalone.html
- rs-webflow-embed.html

First build the base exactly. Do not add new modifications until the base renders.

Required wrapper order:

rs-section > rs-section-container > rs-section-padding > rs-content-container > rs-content-flex > rs-content

Global ownership rules:

- rs-section owns full-width background and section height.
- rs-section-container owns max-width page boundary.
- rs-section-padding owns responsive spacing.
- rs-content-container owns alignment context.
- rs-content-flex owns layout behavior.
- rs-content owns readable text/content width.

Do not compound padding.
Do not rewrite the shell.
Do not inline global CSS.
Do not inline JS.
Do not create a separate mobile nav.

The base must include visible H, P, and T/small typography examples.

After base confirmation, add new layouts only through modifier classes or new template groups that respect the contract.
```

## Validation Checklist

```txt
[ ] CSS is in rs-global.css.
[ ] JS is in rs-scripts.js.
[ ] Standalone HTML links to both.
[ ] Webflow embed references the same structure.
[ ] Wrapper order is preserved.
[ ] Padding is owned by rs-section-padding.
[ ] Typography tokens remain intact.
[ ] Nav opens mega and drawer.
[ ] Drawer locks viewport.
[ ] Expandable list works.
[ ] Mobile stacks cleanly.
```
