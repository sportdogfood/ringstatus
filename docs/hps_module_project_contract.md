# HPS Module Project Contract

This contract defines how work on the HPS Horses module and related paired modules should proceed. Use this document when the project starts to drift, overbuild, or assume facts that have not been verified.

## Core Architecture

The approved architecture is:

- Webflow hosts the static user-facing pages and embeds.
- Webflow Cloud hosts the API/runtime endpoints.
- Airtable is the source database and audit-log target.
- Frontend modules are mobile-first HTML, CSS, and vanilla JavaScript.
- The shared CSS skeleton is the visual and layout contract.

Do not introduce React, a different frontend framework, a different hosting layer, a different API platform, a different database, or a separate styling system unless that change is planned and approved in advance.

## Module Role

Horses is a core pairing module in the larger RingStatus model.

It is not only a standalone page. It is the shared horse context layer that other apps can pair with:

- Schedules + Horses
- Trips + Horses
- Packing + Horses
- Turnout + Horses

Future Rider and Trainer modules may exist, but Horses is the first core module that sets the pattern for tenant isolation, embed/API shape, styling, and audit logging.

## Horses Module Responsibilities

The Horses module has two roles:

1. **Dataset update surface**
   - Users make controlled horse updates here.
   - Updates PATCH Airtable source records.
   - Updates create audit rows.

2. **Reusable display component**
   - The same styled component can be embedded with other apps.
   - Paired versions may hide editable fields or make fields read-only.
   - Reuse the same component and styling wherever practical.

Do not fork the module per tenant or per paired app unless there is a clear, approved reason.

## Styling Contract

All modules must use the same CSS skeleton and brand contract so paired apps fit together predictably.

Rules:

- Use the shared stylesheet as the baseline.
- Preserve the existing colors, spacing, padding, row structure, pills, panels, modals, typography, and interaction states.
- Keep app-specific layout changes inside the existing skeleton.
- New CSS should be additive, minimal, and compatible with paired modules.
- Do not create a parallel visual system.
- If a new layout pattern is needed, review it as a shared skeleton pattern before adding it.

The goal is that paired modules feel like one interface, not separate embedded apps.

## Scope And Audience

This is a convenience app for a relatively small, known audience.

Rules:

- Do not overbuild.
- Do not add heavy authentication, roles, login flows, or complex permission systems unless planned and approved.
- Keep the app low-friction and easy to use.
- Keep tenant isolation and server-side validation because they prevent accidental cross-tenant data exposure.
- Treat audit logging as the main accountability layer for edits.

## Data And Assumption Rules

This project is evolving. Current evidence matters more than older memory or assumptions.

Rules:

- Do not invent data, fields, tables, routes, or requirements.
- Do not assume based on past memory.
- Verify current repo, docs, API, Webflow page output, or Airtable evidence when facts matter.
- Treat the user's newest context as authoritative.
- If a field/table/route is not verified, mark it as unconfirmed instead of building around it.

## Implementation Rules

When changing the module:

- Keep changes scoped to the requested module or workflow.
- Avoid unnecessary splits, abstractions, or rabbit holes.
- Preserve the Webflow + Webflow Cloud + Airtable architecture.
- Preserve mobile-first HTML/CSS/vanilla JS.
- Preserve the shared CSS skeleton.
- Prefer one reusable module with tenant-specific embed config.
- Do not add feature expansion before the current connector is stable.

## Stability Before Expansion

Before adding new module features or paired-app behavior, the current HPS connector must be stable:

- Webflow page loads the module.
- Webflow Cloud endpoint returns the expected tenant data.
- The tenant-specific Airtable view is correct.
- Edits PATCH the source table.
- Audit rows are written.
- Tenant id is included in logs.
- Active/inactive writes the real source checkbox.
- The component still fits the shared visual skeleton.

If the stability gate is not met, fix the connector before adding new features.

## Drift Stop Point

If work starts to drift, stop and return to this contract.

The correct next move is usually one of:

- verify the current live behavior
- inspect the exact source file or embed
- update the contract or README with confirmed facts
- make the smallest scoped fix
- defer new tasks until the stable connector is proven

Do not continue by inventing architecture, fields, workflows, or future systems.
