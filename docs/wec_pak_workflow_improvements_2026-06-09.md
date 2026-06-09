# WEC PAK Workflow Improvements

Status: working improvement list.
Date: 2026-06-09.

This document captures the current PAK workflow improvements that need to be folded into the Airtable model, blueprint review, and future app wiring. It is intentionally focused on workflow structure, table scope, and naming clarity.

## 1. PAK Reference Taxonomy

Use four consistent reference lanes:

```text
pak_area
pak_system
pak_module
table_role
```

Examples:

| pak_area | pak_system | pak_module | table_role |
| --- | --- | --- | --- |
| packing | plan | horse_kits | items |
| packing | plan | quantity | links |
| packing | plan | per_horse | items |
| packing | plan | per_groom | items |
| directory | core_entity | horses | profile |
| directory | core_entity | users | profile |
| directory | support_entity | contacts | profile |
| directory | support_entity | locations | profile |
| views | filtered_list | purchase_onsite | source_view |
| views | filtered_list | unresolved | source_view |
| communications | comments | horse | comments |
| operations | ration | feed | rations |
| operations | schedule | turnout | slots |
| operations | schedule | schooling | slots |
| operations | service | braiding | links |
| operations | service | farrier | links |
| operations | route | travel | routes |
| operations | route | places | pinned_places |
| operations | assignment | stalls | assignments |
| operations | assignment | trailers | memberships |
| operations | assignment | boxes | assignments |

Use this language when assigning or reviewing `pak_index` rows.

## 2. `pak_index` Improvements

`pak_index` should become the PAK registry for all related tables, not only tables that start with `pak_`.

Needed fields:

```text
table_name
pak_area
pak_system
pak_module
table_role
main_group
related_tags
active
unrecognized
non_pak_reference
notes
```

Purpose:

- Identify every table that belongs to the PAK model.
- Group tables by actual workflow, not by table prefix.
- Make comments and tasks precise, for example: "review operations | ration | feed".
- Prevent accidental table creation when a list/list-item/member pattern is enough.

## 3. Lists And List Items

Default model:

```text
list
list_item
list_membership
```

Do not create a full table family when the question is only:

```text
is this horse/item/user a member of this list?
```

Use list membership for:

- `purchase_onsite`
- `unresolved`
- `needs_attention`
- `wave_one`
- `wave_two`
- `not_going`
- trailer membership
- simple roster membership
- simple item membership

Promote to a dedicated workflow only when there is extra behavior such as:

- quantity/state changes
- rations
- scheduling
- assignment
- logs
- lane/slot behavior
- add/edit entity behavior

## 4. Feed Table Scope

Feed should be treated as:

```text
operations | ration | feed
```

Current feed table scopes:

| Table | Scope |
| --- | --- |
| `pak_feed_lists` | List of feed lists, such as AM grain, PM grain, hay, supplements, travel feed |
| `pak_feed_types` | Lookup/category table, such as grain, hay, supplement, medication |
| `pak_feed_items` | Feed item catalog, such as specific grain, hay, supplement |
| `pak_feed_rations` | Per-horse instruction: horse + feed item + feed list + amount/unit/timing |
| `pak_feed_lanes` | Optional board/display lanes if the UI needs them |
| `pak_feed_slots` | Optional time/place slots if timing needs a structured table |
| `pak_feed_logs` | Ration changes, confirmations, skipped feed, edit history |
| `pak_feed_links` | Questionable; only needed if plain membership must be separate from ration instructions |

Lean feed base:

```text
pak_feed_lists
pak_feed_types
pak_feed_items
pak_feed_rations
pak_feed_logs
```

Optional:

```text
pak_feed_lanes
pak_feed_slots
```

Avoid `pak_feed_links` unless there is a confirmed use case where membership exists without ration detail.

## 5. Assignment Systems

Keep assignment simple.

Use:

```text
operations | assignment | stalls
operations | assignment | trailers
operations | assignment | boxes
```

Meaning only:

```text
horse/equipment assigned to stall
horse/equipment member of trailer
item assigned to box
item assigned to truck/trailer
```

Do not expand this into a broader logistics workflow unless a separate requirement is approved.

For horses:

```text
roster_membership
stall_assignment
truck_or_trailer_assignment
```

For packing items:

```text
strict_tracking
box_assignment
truck_or_trailer_assignment
```

## 6. Route And Places

Add a route layer for trip movement and destination context.

Use:

```text
operations | route | travel
operations | route | places
```

Travel should represent:

- start location
- destination
- return destination
- intermediate destination
- split rosters, such as some horses returning home and others going to another show

Places should represent:

- pinned places along a route
- local places around a destination
- useful destination-radius references

This should support planning context. It should not become detailed tracking unless approved.

## 7. Strict Tracking For Count Plans

For `quantity`, `per_horse`, and `per_groom` item rows, add:

```text
strict_tracking
```

Behavior:

```text
strict_tracking = true
```

Means:

- exact count is required
- each packed/unpacked change matters
- shortages are real
- item can be used for return/reserve planning
- packed count must be reconciled

```text
strict_tracking = false
```

Means:

- target is suggested quantity
- packed count is advisory
- missing count is not an auditable shortage
- do not use for reserve/return planning

Example:

```text
Markers, target 100, packed 40, strict_tracking false
```

Interpretation:

```text
Bring about 100 markers. Current tracked count is 40, but reality may differ.
```

Example:

```text
Medication packets, target 100, packed 40, strict_tracking true
```

Interpretation:

```text
60 are missing and must be reconciled.
```

This setting belongs on item rows, not as a global plan setting.

## 8. Filtered Packing List Views

Every packing list should support standard toggles:

```text
all
not_packed
packed
```

For strict items:

```text
not_packed = packed_count < needed_count
packed = packed_count >= needed_count
```

For non-strict items:

```text
packed/not_packed are advisory views only
```

This should be Airtable-driven where possible through views, fields, or precomputed status, not frontend-only logic.

## 9. Entity Systems

Horses and users are core entities:

```text
directory | core_entity | horses
directory | core_entity | users
```

Contacts and locations are support entities:

```text
directory | support_entity | contacts
directory | support_entity | locations
```

Horses need a focused stack:

```text
profile
tabs
attributes
contacts
locations
feed
turnout
kits
comments
```

Initial horse profile fields:

```text
name
barn_name
show_name
aliases
color
gender
discipline
age
emergency_contact
riders
vet
```

Users need:

```text
roles
permissions
comments linkage
assignments
tasks
sessions
tenant ownership/access
phone_number as uid candidate
```

## 10. Blueprint And Runner Rendering

Keep the dynamic HTML approach:

```text
source data -> allowed fields -> pak_html_lib template -> slots -> rendered HTML
```

Rules:

- Airtable owns the HTML pattern.
- Airtable owns which source fields feed the slots.
- The runner resolves, escapes, fills approved slots, and returns final HTML.
- The runner must not invent layouts or one-off classes.
- Use `pak-*` or approved shared `rs-*` classes.
- Do not use `lp-*` in new PAK markup.

The earlier blue-box test remains the simple version:

```html
<div class="your-blue-box">
  <span data-rs-value="horse_count">0</span>
</div>
```

The runner/bridge fills only the value. Webflow owns the styled box.

## 11. Open Improvements To Apply

Priority items:

1. Add or normalize `pak_index` fields for the four-lane reference model.
2. Tag every PAK-related table, including non-`pak_` supporting tables.
3. Identify overbuilt table families that should become list/list-item/membership.
4. Confirm feed uses `pak_feed_rations` as the core membership/instruction table.
5. Add `strict_tracking` to relevant plan item tables.
6. Add box/trailer assignment capability for packing items.
7. Add roster/stall/trailer assignment capability for horses.
8. Add filtered list toggles: all, not packed, packed.
9. Keep route/places as planning context, not detailed tracking.
10. Ensure horse and user entity stacks are modeled as core entities.
11. Ensure comments remain a cross-module system.
12. Keep all rendered components tied to approved stack/html/component records.

## 12. Guardrails

- Do not create new tables without checking whether list/list-item/membership is enough.
- Do not treat suggested quantities as auditable counts.
- Do not turn trailer/stall assignment into detailed logistics unless approved.
- Do not put systems, modules, features, and table roles in the same field.
- Do not hardcode workflow meaning in frontend code when `pak_index`, views, or Airtable fields can represent it.
- Do not rename or remove existing tables without a review step.
