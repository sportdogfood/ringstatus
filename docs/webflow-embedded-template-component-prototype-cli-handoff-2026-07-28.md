# Codex Webflow CLI Handoff: Embedded Template Component Prototype

Date: 2026-07-28  
Repository: `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus`

## Required outcome

Finish an unpublished, native Webflow Component reconstruction of the visual template rendered by the `ringsapp` embeds.

This is not another typography documentation page. The deliverable must visually resemble the embedded two-ring schedule and include its open class-detail drawer.

Do not modify the source page, its embeds, referenced HTML/CSS/JavaScript, or locked template systems.

## Webflow connection contract

- Use only the installed production MCP server named `webflow`.
- Do not use `webflow-beta`.
- Site: `ringstatus`
- Site ID: `6982268b7543ac3c80151266`
- Call `webflow_guide_tool` exactly once at the start of the new CLI session.
- Load the complete official `webflow-mcp:designer-tools` skill before Webflow work.
- Do not reinstall, remove, replace, or reconnect Webflow as a connection check.
- Launch OAuth only after an explicit `reauthenticationRequired`, `invalid_token`, or `invalid_grant`.
- Never publish.

The current desktop task repeatedly returns:

```text
Auth required
```

The CLI session must establish its own attached, working production Webflow MCP session before continuing. A successful `data_pages_tool` or `data_element_tool` call is the connection proof.

## Immutable source

- Source page: `ringsapp`
- Source page ID: `6a6661ede4ed77a63e6fb485`
- Source tree contains five elements.
- Embed IDs:
  - HTML/source references: `9f05288b-3659-1f64-7c07-71518c5c95cc`
  - Inline CSS: `9f05288b-3659-1f64-7c07-71518c5c95cd`
  - Renderer JavaScript: `9f05288b-3659-1f64-7c07-71518c5c95ce`
- Previously verified embed source lengths: `1255`, `1659`, `16122`
- Public visual reference: `https://ringstatus.com/ringsapp`

Source files previously inspected:

- The complete embedded HTML
- The complete inline CSS
- AG Grid 36 styling
- The pinned `webflow/packing-worksheet/styles.css`
- The matching historical relative stylesheet source
- Outfit weights 400, 500, 600, and 700

Known source issues:

- `rswp-special-rows-grid-template.css` was not served by attempted live relative URLs. Its matching historical source was inspected.
- The pinned packing stylesheet has an unmatched closing brace near line 2215. Keep this `OPEN`; do not guess.

## Existing typography reference page

Do not repurpose or delete this page:

- Name: `Embedded Typography Stylesheet — Source Reference`
- Page ID: `6a689b9225d5638ba9b17739`
- Slug: `embedded-typography-stylesheet-source-reference`
- Published: No

It is a documentation reference, not the visual prototype.

It currently contains ten top-level Webflow Component instances in the component group `Embedded Typography Reference`.

## Visual prototype page

- Name: `Embedded Template Component Prototype`
- Page ID: `6a68fda891bea812dc59c099`
- Slug: `embedded-template-component-prototype`
- Published: No

This is the page to repair and finish.

## Exact visual reference

The source template shows:

1. A centered white schedule surface, approximately 840px wide.
2. A rounded search control labeled `Search these grids`.
3. Two rounded ring schedule cards.
4. Ring titles `RING 1` and `RING 2`.
5. Five grid columns:
   - `TIME`
   - `RING`
   - `CLASS`
   - `TILL`
   - `STATUS`
6. Ring 1 rows:
   - `1:00 pm | Ring 1 | 780 - 1.35m Junior/Amateur Jumper | 8 | live`
   - Chips: `Bee (3)` and `Insider (6)`
   - `1:15 pm | Ring 1 | 781 - 1.40m Junior/Amateur Jumper II.1 | 11 | soon`
7. Ring 2 rows:
   - Chips: `Navigator (2)` and `Fort Knox (5)`
   - `1:30 pm | Ring 2 | 825 - 1.20m Junior Jumper | 9 | next`
   - `1:50 pm | Ring 2 | 826 - 1.25m Junior/Amateur Jumper | 6 | later`
8. Compact Outfit typography, thin gray borders, alternating light-gray rows, rounded cards, and restrained shadowing.

## Drawer contract

Opening the Ring 1 `781` row produces:

- Dark translucent full-page overlay.
- White right-side drawer.
- Approximately 400px wide on desktop.
- Full-height visual treatment with rounded outer corners.
- Close control labeled `Close detail`.
- Title:
  `781 - 1.40m Junior/Amateur Jumper II.1`
- Subtitle:
  `Ring 1 • 1:15 pm`
- Six detail fields:

| Label | Value |
|---|---|
| Drawer Type | Class |
| Time | 1:15 pm |
| Ring | Ring 1 |
| Entries | 11 |
| Status | soon |
| Row Key | sample\|ring_1\|781 |

- Footer:
  `Individual class drawer`

The drawer must be a native reusable Webflow Component. Do not use an HtmlEmbed, CodeBlock, iframe, injected CSS, or React.

The prototype may show the drawer in an intentionally open demonstration state. Interactions should be native Webflow interactions only if the installed MCP exposes sufficient interaction tooling; otherwise keep the open state visible and mark interactive open/close behavior `OPEN`.

## Styles already created

These documentation-safe prototype classes exist on the site:

- `etcp-page`
- `etcp-shell`
- `etcp-search`
- `etcp-ring-card`
- `etcp-ring-title`
- `etcp-grid-header`
- `etcp-row`
- `etcp-row-alt`
- `etcp-class-name`
- `etcp-chips`
- `etcp-card-spacer`
- `etcp-status`

Read every style before reusing it. Update only the `etcp-*` prototype classes if corrections are required.

Add scoped drawer classes, for example:

- `etcp-drawer-overlay`
- `etcp-drawer`
- `etcp-drawer-close`
- `etcp-drawer-heading`
- `etcp-drawer-subtitle`
- `etcp-drawer-details`
- `etcp-drawer-detail`
- `etcp-drawer-label`
- `etcp-drawer-value`
- `etcp-drawer-footer`

Use native Webflow breakpoint styles.

## Components created before the failure

Component group: `Embedded Template Prototype`

- `Embedded Schedule — Search`
  - ID: `bee9d68e-df3e-8944-cf96-b1f57728922c`
- `Embedded Schedule — Ring 1`
  - ID: `2279ef1c-9e58-b79c-2cb9-8840a7944da8`
- `Embedded Schedule — Ring 2`
  - ID: `9236946e-9ce8-a4dd-1255-9784d4cbad43`
- `Embedded Schedule — Full Prototype`
  - ID: `cad6203a-a684-e9c8-c707-9b20aaeb743d`

Important: these component definitions were created before placeholder text was corrected. Do not blindly reinsert them.

## Current broken/incomplete state

The page was initially componentized, then visual verification showed that nested Webflow `DivBlock` text remained:

```text
This is some text inside of a div block.
```

To repair it:

1. The full-page component instance was unlinked.
2. The three nested Search/Ring component instances were unlinked.
3. All 32 schedule String nodes were targeted.
4. The first String node and the remaining 31 String nodes were successfully updated with the exact source values.

The last successful correction batch reported:

```text
count: 31
success: 31
errors: []
```

The page is therefore currently native/unlinked and must be inspected before any transformation.

The search block may still be blank or incomplete. Verify and set `Search these grids`.

Do not assume the old component definitions contain the corrected text.

## Required CLI recovery sequence

1. Load the full official Designer tools skill.
2. Call `webflow_guide_tool` once.
3. Prove the production connection with:
   - `data_pages_tool.get_page_metadata` for page `6a68fda891bea812dc59c099`, or
   - `data_element_tool.get_all_elements`.
4. Read the complete current prototype page tree.
5. Confirm whether the page root currently contains a native Section or a Component instance.
6. Query all remaining text equal to:

   ```text
   This is some text inside of a div block.
   ```

7. Replace every remaining placeholder before component conversion.
8. Confirm the search control reads `Search these grids`.
9. Read all `etcp-*` styles and correct visual mismatches against `https://ringstatus.com/ringsapp`.
10. Build the drawer with native Webflow elements.
11. Transform the corrected blocks into reusable Components:
    - Search
    - Ring schedule card or corrected Ring 1/Ring 2 components
    - Drawer
    - Full prototype wrapper containing nested Component instances
12. Prefer one reusable Ring Schedule component with editable properties if practical. If MCP property conversion is unreliable, separate corrected Ring 1 and Ring 2 Components are acceptable.
13. Do not overwrite the old component definitions unless the tool explicitly supports safe definition editing and the result is read back.
14. Read back the complete page tree.
15. Confirm the top-level page content is a Component instance and the nested schedule/drawer blocks are Component instances.
16. Query the `Embedded Template Prototype` group and report exact component IDs.
17. Open the page in Designer and visually verify it.
18. Verify desktop and responsive widths.
19. Re-read the source `ringsapp` page and confirm the same five elements and three embed IDs remain.
20. Do not publish.

## Visual verification gates

At minimum verify:

- 320px
- 375px
- 479px
- 480px
- 767px
- 768px
- 991px
- 992px
- 1280px
- 1440px

At each width confirm:

- Search control remains visible and usable.
- Both ring cards remain readable.
- Column/data hierarchy remains understandable.
- Class names truncate or wrap without page overflow.
- Chips do not create unintended page overflow.
- Drawer remains visible and usable.
- Drawer does not clip its title, details, close control, or footer.
- Mobile drawer behavior remains source-like.
- No unintended horizontal page overflow.

## Prohibited

- Do not modify source page `6a6661ede4ed77a63e6fb485`.
- Do not modify any source embed.
- Do not modify the referenced HTML, CSS, or JavaScript.
- Do not use an HtmlEmbed.
- Do not use a CodeBlock.
- Do not use an iframe.
- Do not inject CSS.
- Do not rebuild in React.
- Do not publish.
- Do not claim `PASS` from structural readback alone.

## Completion report

Report:

- Site and page IDs.
- Final prototype page tree.
- Components created, reused, replaced, or left obsolete.
- Exact style changes.
- Drawer implementation details.
- Exact widths visually verified.
- Source page/embed readback.
- Remaining items marked `OPEN`.
- Overall `PASS` or `FAIL`.
- `Published: No`.

