# Section Catalog

Stable section names let runners build pages from explicit parts.

The generated Webflow component pages live in `webflow_component_pages/pages/`.
Do not generate separate embed snippets from this catalog; Webflow embed delivery is handled by the existing project-specific embed workflow.

| ID | Alias | Type | Notes |
|---|---|---|---|
| `topnav` | `nav` | navigation | Top nav, mega menu, drawer trigger, scrim, and drawer. Requires `rs-scripts.js`. |
| `section-01-hero-split` | `s01` | standard | Split hero section. |
| `section-02-split` | `s02` | standard | Split section with visual column. |
| `section-03-split-small-text` | `s03` | standard | Split section with T/small text reference. |
| `section-04-no-split` | `s04` | standard | Single content block. |
| `section-05-centered-no-split` | `s05` | standard | Centered single content block. |
| `section-06-wide-no-split` | `s06` | standard | Wide content section. |
| `section-07-overlay-center` | `s07` | overlay | Centered overlay section. |
| `section-08-overlay-left` | `s08` | overlay | Left-aligned overlay section. |
| `section-09-overlay-bottom-left` | `s09` | overlay | Bottom-left overlay section. |
| `section-10-form-stack` | `s10` | stack | Header above form panel. |
| `section-11-card-grid` | `s11` | stack/cards | Header above card grid. |
| `section-12-carousel` | `s12` | stack/cards | Header above horizontal carousel. |
| `section-13-wildcard-about` | `s13` | wildcard sticky | Sticky story panel with scrolling proof cards. |
| `section-14-wildcard-blog` | `s14` | wildcard sticky | Sticky blog/archive pattern. |
| `section-15-wildcard-products` | `s15` | wildcard sticky | Sticky product-card pattern. |
| `section-16-viewport-list` | `s16` | list | Internal scroll-list panel. |
| `section-17-expanding-list` | `s17` | list | Expanding list. Requires `[data-list-limit]` behavior in `rs-scripts.js`. |
| `section-18-pricing-cards` | `s18` | cards | Pricing/card comparison section. |
| `footer` | `footer` | footer | Full-height footer component. |

## Copy Rule

When a prompt asks for section IDs, copy those exact sections from `master_ks`, keep their wrapper order, and edit only page-specific content unless the user asks for a structural change.

Example:

```txt
Create x-landingpage using topnav, s01, s13, s18, footer.
```
