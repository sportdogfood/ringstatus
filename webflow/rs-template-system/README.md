# RS Template System

This directory stores canonical RS template sources. It is not a starter output folder.

## Gold Sources

- `master_ks/` is the locked kitchen-sink source for landing pages, section shells, typography, nav, sticky wildcard sections, footer, global CSS, and shared JS.
- `master_app/` will become the locked source for branded app shells, app CSS, drawers, flyups, cards, lists, and data-ready UI primitives.

Starter runners copy from gold sources, validate the copy, and create targeted output elsewhere. They do not edit gold directly.

## Template Lanes

- Landing pages: consume `master_ks`.
- App templates and CSS: consume `master_app` once locked.
- API-populated templates: consume app or section sources, then wire data through Webflow Cloud, Astro, or another approved API layer.

## Required Rule

Gold sources are copied, never edited, unless the user explicitly unlocks the specific gold source.

