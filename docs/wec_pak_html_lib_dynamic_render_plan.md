# WEC Dynamic HTML Library Render Plan

Status: concept plan. This is not P2/P3 work.

## Purpose

This plan documents the dynamic content rendering concept used by the WEC app when Webflow/Airtable should define the approved markup shape and Codex should only assemble and populate that shape.

This is the expanded version of the earlier blue-box test. The blue-box test proved a value can land inside an approved visual container. This plan covers the fuller build:

```text
source data -> allowed fields -> pak_html_lib template -> typography/style references -> rendered approved HTML
```

The renderer must not invent a layout, create one-off classes, or rebuild a component from memory.

## Naming Rule

Do not use `lp-*` classes in new WEC/Pak markdown, docs, examples, templates, or generated HTML library records.

Use the approved `pak-*` direction for new Pak-specific fragments, or approved global `rs-*` classes when the component is intentionally shared.

## Concept

`pak_html_lib` is the approved HTML/template library.

It stores reusable element patterns such as:

- header title/subtitle block
- value box
- metric/count value
- button content
- small row fragment
- record detail line
- icon/text fragment

The template is not the source of data and not the source of business logic. It is the approved markup shape.

Data comes from a source table/view/record. Styling comes from a typography/style library or approved component/style references. The renderer combines them.

## Required Chain

The full chain should be explicit:

```text
pak_page_stack
  -> component_key
  -> pak_components
  -> html_key
  -> pak_html_lib
  -> style/typography references
  -> source table/view/record
  -> allowed fields
  -> rendered HTML
```

No renderer should skip directly from source data to custom HTML if a `pak_html_lib` template exists for that component.

## Table Roles

### `pak_page_stack`

Defines where a component appears in the page stack.

Expected responsibilities:

- `stack_key`
- `component_key`
- `html_key`
- `table_source`
- `lookup_view` or equivalent view field, when needed
- `source_record_key`, when targeting a specific source record
- source field mappings such as `title_field`, `subtitle_field`, `value_field`
- active/hidden flags
- sort/order

### `pak_components`

Defines the component identity.

Expected responsibilities:

- `component_key`
- component label
- component type or role
- allowed html keys if needed
- approved class/style references if needed
- active flag

### `pak_html_lib`

Defines approved HTML fragments.

Expected responsibilities:

- `html_key`
- `pattern_label`
- root class or wrapper class
- html pattern
- allowed slot names
- allowed class tokens
- notes/status

The HTML pattern should contain explicit slots/placeholders, for example:

```html
<section class="pak-header">
  <div class="pak-header-copy">
    <div class="pak-title" data-rs-slot="title"></div>
    <div class="pak-subtitle" data-rs-slot="subtitle"></div>
  </div>
</section>
```

### Typography / Style Library

The typography/style source should define reusable presentation tokens, not one-off page styling.

It should answer:

- root class
- text class
- font size
- font weight
- line height
- spacing
- min/max/clamp behavior
- responsive behavior
- active/hover/click states for interactive fragments

This can live in `pak_system_styling`, a typography-specific table, or another approved styling registry, but it must be referenced rather than guessed.

## Renderer Responsibilities

The renderer should:

1. Load the page stack row.
2. Resolve the component row.
3. Resolve the `pak_html_lib` template.
4. Resolve approved typography/style references.
5. Load only allowed source fields.
6. Map source fields to named slots.
7. Escape text values.
8. Render the approved HTML pattern.
9. Leave missing optional values blank or show an approved empty state.
10. Report missing required slots/templates as warnings in the blueprint review.

The renderer should not:

- hardcode visual class names outside the approved library
- use `lp-*` classes
- create fallback values such as fake counts
- create a new component shape when `pak_html_lib` defines one
- silently ignore missing required slots
- pull whole Airtable records when allowed fields exist

## Current Partial Build

The current blueprint code already has a partial header-chain test:

```text
pak_page_stack -> pak_html_lib -> pak_components -> wec_pack_waves -> rendered slots
```

That is only a proof/check path. It is not yet the full generalized renderer.

The rest of the build still needs:

- reusable slot resolver for any `pak_html_lib` record
- allowed slot validation
- typography/style reference resolution
- allowed field enforcement per source table
- blueprint warnings for missing template/style/source/field mappings
- front-end preview of the rendered fragment beside the source/template chain
- removal of `lp-*` examples from WEC/Pak markdown and new generated examples

## Header Example Without `lp-*`

Approved direction:

```html
<header class="pak-header">
  <div class="pak-header-copy">
    <div class="pak-title" data-rs-slot="title"></div>
    <div class="pak-subtitle" data-rs-slot="subtitle"></div>
  </div>
</header>
```

Example source mapping:

```text
title slot -> wec_pack_waves.wec_report_title
subtitle slot -> wec_pack_waves.wec_report_subtitle
```

The final output is not hand-coded by Codex. It is assembled from the approved HTML pattern and approved source fields.

## Blueprint Review Requirements

The blueprint preview should show:

- stack row used
- component row used
- html library row used
- styling/typography references used
- source table/view/record used
- source fields used
- rendered slots
- final rendered HTML preview
- warnings for missing pieces

This is how we avoid guessing and make the build inspectable before it affects the app.

## Definition Of Done

This concept is ready to implement when:

- `pak_html_lib` has approved records for at least header, value box, button label, and row fragment.
- Each template has named slots.
- Each stack/component row points to the required html template.
- Typography/style references are stored and inspectable.
- The blueprint preview can show the chain and rendered output.
- No new WEC/Pak examples use `lp-*`.

