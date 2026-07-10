# AG Base-Template Retrofit Method

Date: 2026-07-10
Status: Reusable approved method

## Purpose

Provide a repeatable method for adapting the approved AG base template to a report, input, or form without redesigning the template or rediscovering contracts through trial and error.

The method separates four concerns:

1. locked template behavior;
2. use-case functionality;
3. data and writeback contracts;
4. explicitly approved option layers.

## First Principle

Starting from a base template means preserving the base, not producing a visual approximation.

The default implementation strategy is:

```text
locked template
  + explicit element mapping
  + is-hidden for unused elements
  + output-specific data/handlers
  + named approved option packs
```

Do not copy the screenshot, reinterpret the layout, replace responsive behavior, or assume that a sample button label defines its future responsibility.

## Step 1: Identify and Freeze the Base

Record all of the following before designing the use case:

- canonical source file;
- deployed reference URL;
- route file;
- contract and handoff documents;
- AG Grid version;
- template DOM root and major stack elements;
- CSS block or shared stylesheet;
- responsive breakpoint rules;
- print rules.

Capture a regression fixture or hash for the locked template CSS and structural markers. This makes “do not change the template” mechanically testable.

## Step 2: Inventory Existing Functionality

Before moving controls, list every current behavior:

- load sequence;
- data sources;
- row identity;
- columns and renderers;
- edit mode;
- actions and handlers;
- pickers/flyups;
- status and errors;
- print/share;
- submit/writeback;
- post-submit state.

Do not treat broken functionality as absent. Diagnose it and state whether the retrofit preserves, repairs, removes, or defers it.

## Step 3: Build an Element Mapping Matrix

Map every base-template element before editing code.

| Base element | Use-case responsibility | Visibility | Handler/data owner | Styling authority |
|---|---|---|---|---|
| Header | Title/context | Visible | Output config | Locked base |
| Mini actions | Named actions | State based | Existing handler | Locked base |
| Main actions | Named actions or none | Visible/`is-hidden` | Existing handler | Locked base |
| Anchors | Named anchors or none | Visible/`is-hidden` | Filter option pack | Locked base |
| Grid frame | Output AG Grid | Visible | Column/data contract | Locked base |
| Bottom actions | Named actions | State based | Existing handler | Locked base |
| Status line | Count/status | Visible | Runtime state | Locked base |

Every unassigned element remains structurally present and receives `is-hidden` when approved. Do not delete or restyle locked elements for convenience.

## Step 4: Classify Every Proposed Change

Every change must fit exactly one class:

1. **Reuse unchanged** — same element and behavior.
2. **Reassign behavior** — same locked element, different approved label/handler.
3. **Hide** — add only `is-hidden`.
4. **Data wiring** — map endpoint fields to row/config fields.
5. **Approved option layer** — a named behavior such as form review, picker, writeback, focus, or three-column print.

If a change does not fit one class, stop and request explicit design approval.

## Step 5: Separate Interactive Columns from Print Columns

Never infer interactive AG columns from a print layout.

- `column_schema_options` owns interactive grid fields.
- `print_options` owns the dedicated print sheet.
- `3column-print` means three newspaper-style print columns, not three AG fields.
- Ring grouping in print does not automatically mean Ring is removed from the interactive grid.

Document both shapes explicitly.

## Step 6: Define Canonical Data Mappings

For every visible, searchable, hidden, editable, and submitted field, record:

- canonical source field;
- fallback fields, if allowed;
- display field;
- search text;
- row/payload key;
- unresolved behavior.

Example:

```text
display: barn_name
search: barn_name + horse
forbidden fallback: rider
unresolved: visible unresolved state, never silent rider substitution
```

Inspect live endpoint shapes before implementation. Do not assume response keys such as `results`, `matches`, or `top_matches` are interchangeable.

## Step 7: Choose the Search Boundary

For small, already-loaded sets, use AG Grid's Client-Side Row Model and cached Quick Filter.

- Precompute normalized search text.
- Use `cacheQuickFilter: true` when an option pack benefits from it.
- Establish a minimum character threshold.
- Avoid network calls per keystroke.
- Use tiered source expansion only when the first candidate set is intentionally bounded.

Keep canonical display text separate from broader searchable aliases.

## Step 8: Preserve Responsive and Print Locks

Unless explicitly approved, a retrofit may not:

- edit existing media queries;
- add breakpoint-specific JavaScript;
- swap column definitions by viewport;
- reorder template elements;
- introduce mobile-only controls;
- alter print orientation, columns, or grouping;
- change locked sizing, spacing, or overflow.

Desktop/mobile/print checks verify preservation. They do not authorize redesign.

## Step 9: Update Airtable and Git Contracts

Airtable AG reference tables document intent:

- `ag_grids`
- `ag_tables_allowed`
- `ag_fields_allowed`
- `ag_end_points_allowed`

Git remains executable truth. When the approved design changes columns, actions, sources, endpoints, or writeback, update both the Git design/contract and the relevant Airtable reference row. Do not let Airtable notes silently replace source code.

## Step 10: Implement with Drift Guards

Before implementation, write tests that fail when:

- locked CSS changes;
- locked structural markers disappear or move;
- new media queries appear;
- unauthorized responsive column logic appears;
- an unused element is deleted instead of hidden;
- expected actions or data fields drift;
- print and interactive column contracts are conflated.

Then implement the smallest mapping and option-layer changes needed to pass.

## Step 11: Render and Verify

Minimum verification matrix:

| Surface | Required proof |
|---|---|
| Static contract | Locked template unchanged |
| Data | Live response shape mapped explicitly |
| Desktop | Base layout and output behavior |
| Mobile breakpoint | Locked behavior preserved |
| Print | Approved dedicated print shape |
| Actions | Visibility, labels, handlers, disabled states |
| Pickers | Initial tier, expanded tier, manual fallback |
| Writeback | Success and failure responses through server boundary |
| Source parity | Routed source matches tested source |

Do not publish from visual confidence alone.

## Step 12: Publish Through the Owned Route

After approval and verification:

1. copy or update the routed source;
2. run contract tests;
3. run the framework build;
4. render locally;
5. deploy through the approved Webflow Cloud project and mount;
6. verify the live GET/rendered page;
7. do not use production-data mutation as proof unless separately approved.

## Retrofit Handoff Template

Every future retrofit design should contain:

```text
Base source:
Live reference:
Locked CSS/responsive/print rules:
Use-case source:
Behavior inventory:
Element mapping matrix:
Interactive column schema:
Print schema:
Canonical field mappings:
Search boundary:
Approved option packs:
Explicitly allowed CSS additions:
Non-goals:
Static tests:
Rendered checks:
Publication route:
```

## Lessons Captured from Barn Entry

- Confirm placement by workflow, not by sample button labels.
- `is-hidden` is safer than deleting locked template elements.
- “Three columns” must be qualified as interactive fields or print columns.
- A locked mobile template must not receive a new mobile interpretation.
- Canonical display values must be mapped before AG rendering.
- Live endpoint keys must be inspected; helper horses were returned under `top_matches`, not the keys the old form read.
- Small candidate sets should use cached local AG filtering rather than repeated Airtable calls.
- Documentation must capture the mapping and lock before implementation so the next output can be correct on the first pass.
