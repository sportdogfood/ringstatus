# LPX Approved Page Updates Handoff

Date: 2026-07-25

Repository:

`C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus`

## Objective

Apply the approved LPX structure and behavior to the existing Lainey portfolio
pages without redesigning, adding alternatives, or publishing.

This is a simple personal college-recruitment website. Preserve the existing
brand CSS and native Webflow structure. Do not introduce new infrastructure,
page variants, modals, drawers, or one-off embeds.

## Exact Webflow Reference

- LPX reference page ID: `6a5e13c99a97109aa0ba053c`
- Public reference URL: `https://ringstatus.webflow.io/lpx`
- Use the LPX page as the visual and structural source.
- Apply the approved shared changes to each existing LP page.
- Do not publish without separate explicit approval.

## Approved Page Set

- Home
- Profile
- Horses
- Videos
- Competitions
- Classes

## Approved Webflow Changes

### 1. Hero Copy

- Preserve the LPX `lpt-hero-copy` size.
- Preserve white hero-copy typography.
- Hero copy content is editable.
- Font size and shade remain fixed Webflow styling and are not Airtable content
  fields.

### 2. Contact Control

- Use the new `lpt-contact-hero` button.
- Every page contains the same Contact section.
- The Contact section is hidden by default.
- Clicking `lpt-contact-hero` slides the existing Contact section up.
- Opening locks page scrolling.
- A second click slides the section down and restores scrolling.
- The Contact section `x` closer performs the same close behavior.
- Do not create a modal, drawer, duplicate form, or alternate Contact
  implementation.

### 3. Section Headers

- Applicable `lpt-section-head` elements contain `lpt-section-link`.
- Carry this exact structure into the existing page templates.

### 4. Cards

- Lock the LPX `lpt-card` markup and dimensions before slider work.
- Each card contains `lpt-pill-grid`.
- Show no more than four pills per card.
- Each card supports `lpt-badge`.
- Card, pill, pill-grid, and badge sizing are fixed Webflow styles.

### 5. Slider

- Do not reuse the old slider JavaScript. The markup has changed too much.
- Build one new scoped slider implementation against the locked LPX card
  contract.
- Support card lanes, horizontal overflow, and previous/next controls.
- Slider behavior must not resize cards.
- Keep the JavaScript scoped to the LPX/LP page root and shared across pages.

### 6. Social Links

- Keep Instagram.
- Keep YouTube.
- Remove Contact from the Social links section.

## Airtable State

Base:

`appUGgVeAZFae3tEb`

### Page Tables

All six page tables now contain:

- `hero-title` as single-line text
- `hero-text` as multiline text

Page table IDs:

| Page | Table ID |
| --- | --- |
| Home | `tblkYgi3XeRnpwLwc` |
| Profile | `tblbjAI6oeXy3UOeP` |
| Horses | `tblDSBmKai7fyrMlP` |
| Videos | `tblFH0Q8shgsW6NHs` |
| Competitions | `tbl9KVngACOnuMZyv` |
| Classes | `tbl7X5Bj2ON2FOwH9` |

### Profile Data

- Canonical identity/USEF table: `profile_data`
- Table ID: `tblkTwq8YyjSoEaLr`
- Rendered Profile page stack: `profile`
- Table ID: `tblbjAI6oeXy3UOeP`

The rendered Profile stack contains these active ordered records:

1. `lpt-nav-global`
2. `lpt-hero`
3. `lpt-nav-page`
4. `lpt-filters`
5. `lpt-section: bio`
6. `lpt-section: education`
7. `lpt-section: riding`
8. `lpt-drawer`
9. `lpt-bottom-margin`

### Bio

- Table: `bio`
- Table ID: `tblGxeI6TRWa2n3h1`
- Seven active sample records exist:
  - two About records
  - two Education records
  - three Riding records
- The copy is placeholder content for owner review.

### Content Contracts

- Table: `content_contracts`
- Table ID: `tbllnBLGsk4QaEgnG`

Current relevant contracts:

- `hero-title`
- `hero-text`
- `background-image`
- `bio-title`
- `bio-year`
- `bio-string`
- `nav-pill-background-shade`
- `section-head-background-shade`
- `filter-pill-background-shade`

Contract meaning:

- `background-image` is the editable `lpt-hero` background image linked through
  the Images table.
- Bio title, year, and string apply to About, Education, and Riding records as
  appropriate.
- Navigation-pill, section-head, and filter-pill shades are editable design
  values.
- Hero-copy font size and white typography are fixed in Webflow and are not
  editable content contracts.

## Webflow Execution Requirements

Use the installed Webflow MCP v2.0 server.

Required sequence:

1. Call the current Webflow guidance tool first.
2. Verify the exact workspace, site, page, locale, and branch.
3. Read the site Agent Instructions.
4. Inspect page `6a5e13c99a97109aa0ba053c`.
5. Inspect all six target pages before writing.
6. Capture the current structure or snapshots where available.
7. Apply only the approved changes in this handoff.
8. Read back every changed element, class, attribute, and custom-code reference.
9. Capture post-change snapshots where the Bridge is available.
10. Do not publish.

The approval to apply the changes in this handoff has already been given. If
the exact live target differs from this document, stop and report the mismatch
instead of guessing.

## Preservation Rules

- Preserve the existing CSS and brand system.
- Preserve native Webflow elements and components.
- Preserve existing page content unless this handoff explicitly changes it.
- Do not remove existing embeds during the native-element pass.
- Do not add alternative UX, new components, or additional controls.
- Do not create page-specific slider implementations.
- Do not expose Airtable keys in browser code.
- Do not publish.

## Verification Gate

Return `PASS` only when:

- all six existing pages use the approved LPX structure;
- the Contact section slides up and down on every page;
- scroll locks and restores correctly;
- Social links contain Instagram and YouTube only;
- applicable section headers contain `lpt-section-link`;
- cards use the locked contract with no more than four pills and an optional
  badge;
- the new shared slider works without resizing cards;
- all changed Webflow elements and code references pass MCP readback;
- no page was published.

Return `FAIL` with the exact page, element, and MCP error when any required
verification fails.

## Paste-Ready Next-Run Prompt

```text
Use the installed Webflow MCP v2.0 server and follow:
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\lpx-approved-page-updates-handoff-2026-07-25.md

The owner has approved the exact Webflow changes in that handoff. Verify the
workspace, site, LPX reference page ID 6a5e13c99a97109aa0ba053c, all six target
pages, locale, branch, and Agent Instructions before writing. Apply only the
approved shared changes. Preserve the existing brand CSS, native elements,
content, and embeds. Read back and visually verify all changes. Do not publish.
Stop with FAIL instead of guessing if any target or contract differs.
```
