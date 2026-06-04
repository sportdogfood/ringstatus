# RingStatus Merged Infrastructure README

**Snapshot date:** June 4, 2026  
**Merged from:**

- Current RingStatus Packing App README / current conversation snapshot
- `ringstatus_skeleton_documentation_2026-06-01.rev2.md`

**Purpose:** merge the newer packing-specific implementation detail with the older broader RingStatus skeleton so the documentation is more complete without breaking the current app naming, routes, tables, or plan logic.

---

## 1. Executive Summary

The latest documentation is stronger for the **Packing App** because it includes real implementation details: plan routes, Airtable table names, actions, `pak_groups`, stack sections, root HTML, CSS class family, current app state, and still-open work.

The previous June 1 skeleton is stronger for the **platform context** because it defines the broader RingStatus operating model: tenants, tenant events, event ops, recognizers, subscribers, people, horses, places, schedules, boards, tasks, alerts, SMS, comments, Ring Waze, integrations, risks, and implementation guardrails.

The merge should not replace the latest packing registry. It should wrap the packing app inside the broader RingStatus event-ops system.

**Merged rule:**

```text
Use the latest README as the Packing App source of truth.
Use the June 1 skeleton as the broader platform and operating-context source.
```

---

## 2. Core Positioning

**RingStatus is the mobile travel layer between your barn software and the showgrounds.**

RingStatus does not replace a barn’s main software. It connects to existing systems and fills the lightweight, mobile-first gap needed for travel, show operations, packing, feeding, turnout, setup, showing, return, comments, alerts, SMS lookups, and clean sync-back.

The current implementation focus is the **Packing App**, but the broader product direction is a tenant-aware event-ops platform.

---

## 3. Comparison: Latest vs Previous Version

| Area | Latest Packing README | Previous June 1 Skeleton | Merge Decision |
|---|---|---|---|
| **Scope** | Packing app and show travel workflows | Full RingStatus event-ops architecture | Keep both; latest is module-specific, previous is platform-wide |
| **Naming** | `Shows > Show`, `Waves`, `Packing`, `quantity`, `per_horse`, `per_groom`, `horse_specific` | `Event Ops`, `Tenant Events`, `Event Groups/Series/Weeks/Days` | Use `Shows > Show` for front-facing; use `Event Ops/Tenant Event` for architecture |
| **Packing** | Very detailed plan registry and app stack | Generic lists/pack waves/pack lists/horse kits | Latest wins; previous only adds lifecycle context |
| **Horses** | Horse entity, horse profile, wave assignments, feed/turnout/braiding links | Horses as tenant-owned master records integrated into event ops | Merge; tenant-owned master horse remains the source concept |
| **Feeds / Turnout / Braiding** | Operational list/entity patterns | Feed/turnout boards and tenant-event board context | Merge; latest defines models, previous adds board context |
| **Schedules / Rings** | Simple day/hour calendar and horse state model | Deep show-day/ring/class/entry/trip/live/results model | Merge; previous adds the deeper showing/schedule roadmap |
| **Boards** | Not fully built in latest; stack sections for packing app | Turnout, Feed, Stall, Show-Day boards built from slots/lanes/bindings | Import as future operational layer |
| **Alerts / SMS / Ring Waze** | Mentioned as broader future context | Fully described | Import into broader infrastructure section |
| **Tech / Integrations** | Retool, Webflow, Codex, Git, Cloudflare, Airtable | Adds Webflow Cloud/Astro, jsDelivr, Twilio, MCP, live data sources, lane preflight | Merge fully |
| **Risks / Guardrails** | Packing-specific open work and blockers | Naming drift, tenant/event nesting, live-write safety, SMS freshness, lane confusion | Import guardrails fully |

---

## 4. Merged Top-Level Structure

### 4.1 Front-Facing Structure

This is the simple product/navigation structure.

```text
RingStatus
└── Solutions
    ├── System Bridge
    ├── Horses
    └── Shows
        └── Show
            ├── Packing
            ├── Getting There
            ├── Setup
            ├── Ops
            ├── Showing
            └── Breakdown / Return
```

### 4.2 Granular Architecture Structure

This is the more robust build structure.

```text
RingStatus
├── Recognizers
├── Subscribers
├── Tenants
├── People
├── Horses
├── Show Riders
├── Places / Pins / Routes
├── Event Ops
├── Tenant Events
├── Linking Modules
├── Schedules
├── Lists
├── Boards
├── Tasks
├── Alerts
├── SMS Two-Ways
├── Ring Waze
├── Comments
├── Packing App
└── Technology / Integrations
```

### 4.3 Naming Rule

```text
Front-facing label: Shows > Show
Architecture label: Event Ops / Tenant Event
Implementation label: route/table/plan names already in use
```

Do not rename built packing routes or tables to match the older conceptual skeleton.

---

## 5. Tenant / Event / Show Ownership Model

The previous skeleton makes the ownership model more robust.

### 5.1 Core Rule

```text
Tenants and Events stay separate.
Tenant Events join them.
```

### 5.2 Why This Matters

A tenant/barn may attend some events but not all events. A tenant may own many horses and people, but only some of them attend a specific show or event week.

Therefore:

```text
Tenant = barn/client/master organization
Event Ops = show/travel/ops container
Tenant Event = one tenant participating in one event/week/day
Horse = tenant-owned master record
People = reusable person/profile records
Show Rider = show-specific rider participation record
```

### 5.3 Merged Working Shape

```text
Tenants
└── Tenant
    ├── Horse Master Records
    ├── People / Profiles
    └── Tenant Events
        └── Tenant Event
            ├── Event / Show Context
            ├── Attending Horses
            ├── Attending People
            ├── Waves
            ├── Packing
            ├── Setup
            ├── Ops
            ├── Showing
            ├── Schedules
            ├── Boards
            ├── Tasks
            ├── Alerts
            ├── SMS Permissions
            ├── Comments
            └── Places / Pins / Routes
```

---

## 6. Current Front-Facing Descriptions

| Area | Description |
|---|---|
| **System Bridge** | Connects existing barn software to lightweight mobile workflows for travel, showgrounds, return, and clean sync-back. |
| **Horses** | Core horse records used across shows, packing, feeding, turnout, setup, showing, schedules, reporting, SMS, and print outputs. |
| **Shows > Show** | Active workspace for one trip/event. |
| **Packing** | Prepare, track, pack, adjust, return, and resolve show-related supplies. |
| **Getting There** | Travel movement from home barn to showgrounds. |
| **Setup** | Arrival setup, stalls, tack rooms, feed areas, aisle setup, and readiness. |
| **Ops** | Daily workflows such as feed, turnout, braiding, tasks, notes, and exceptions. |
| **Showing** | Ring, schooling, class, trip, live status, and horse schedule workflows. |
| **Breakdown / Return** | Pack up, go home, unpack, reset, record missing/damaged items, and handoff back home. |

---

## 7. Entity / Listing Shell

Every major entity/listing uses the same basic shell.

```text
Entity / Listing
├── Meta
├── Add
├── Edit
├── Comments
├── Activity
├── Logs / Changes
└── Reports / Outputs
```

### 7.1 Applies To

```text
Tenant
Show / Event
Tenant Event
Wave
Horse
Person / Profile
Show Rider
Feed
Feed Item
Turnout
Braiding
Packing
Packing Item
Kit
Kit Item
Setup
Schooling
Showing / Ring
Schedule
Report
Place / Pin / Route
Task
Alert Thread
Ring Waze Comment
```

### 7.2 Meta Means

```text
name
label
status
owner/context
date/time where relevant
active/inactive
display notes
source identifiers where relevant
```

---

## 8. Linking Rule

The current packing discussion established a strong linking rule.

```text
A linking module should connect exactly two records.
Avoid 3-way or 4-way linking tables unless absolutely necessary.
Use fields, child records, actions, logs, notes, or fanout JSON for extra detail.
```

Examples:

```text
Horse + Show = Show Horse
Horse + Wave = Horse Wave Assignment
Horse + Feed = Feed Member
Feed Member + Feed Item = Feed Member Item
Feed Member Item + Ration = Feed Member Item Ration
Horse + Turnout = Horse Turnout
Horse + Braiding = Horse Braiding
Horse + Schooling = Horse Schooling
Horse + Ring = Horse Showing Assignment
Horse + Kit = Horse Kit Assignment
```

---

## 9. Slots, Lanes, Bindings, and Boards

The previous skeleton adds a useful board-builder model.

```text
Slots = when
Lanes = where / workflow column
Bindings = who or what is assigned
Boards = display surfaces built from bindings
```

### 9.1 Current Locked Boards

```text
Boards
├── Turnout Board
├── Feed Board
├── Stall Board
└── Show-Day Board
```

### 9.2 Board Behavior

```text
Turnout Board → horse + turnout slot + handler + status
Feed Board → horse + feed slot + feed instructions + status
Stall Board → horse + stall assignment + setup status + notes
Show-Day Board → horse + rider + ring/class/schedule + readiness/status
```

### 9.3 Merge Decision

The current packing app already uses stack sections, lanes, controls, and action rows. The broader board-builder model should be preserved for future Ops/Showing work, but should not rewrite the current `pak_groups` pattern.

---

## 10. Feeds Model

Feeds are roster/list based.

A horse must first be a **Feed Member**. Then the horse can receive feed items, rations, days, times/slots, and done/not done task rows.

```text
Feeds
├── Feed
├── Feed Slots
├── Feed Members
├── Feed Items
├── Feed Member Items
├── Rations
├── Feed Member Item Schedule
└── Feed Task Rows
```

### 10.1 Rule

```text
Feed membership says the horse is on the feed list.
Feed items, rations, days, and times define what they get.
Feed task rows track whether each day/time instance is done.
```

### 10.2 Slot vs Lane

```text
Slot = when, such as AM / Midday / PM / Night
Lane = physical or operational path, not currently required in Feeds
```

---

## 11. Turnout Model

Turnout is an operational entity, but simple.

```text
Ops
└── Turnout
    ├── Turnout
    ├── Turnout Slots / Time
    └── Horse Turnout
```

Fields on Horse Turnout:

```text
Horse
Turnout
Time
Status
Notes
```

Rule:

```text
For Turnout, Stall is optional context, not part of the core link.
```

---

## 12. Braiding Model

Braiding is a roster-based prep list, not a checkout/check-in workflow.

```text
Ops
└── Grooming / Prep
    └── Braiding
        ├── Braiding Roster
        ├── Braiding Styles
        ├── Horse Braiding
        ├── Braiding Status
        └── Braiding Notes
```

Example:

```text
Horse: Bee
List: Braiding
Style: Hunter Braids
Day: Tuesday
Status: Done / Not Done
```

Rule:

```text
Braiding tracks horse + day + style + done/not done.
```

---

## 13. Schedule / Calendar Model

The current model needs a simple calendar by day/hour, while the previous skeleton provides the deeper show-day/ring/class/trip hierarchy.

### 13.1 Simple Calendar Layer

```text
Schedules
├── Schedule Sources
│   ├── Stall
│   ├── Turnout
│   ├── Feed
│   ├── Schooling
│   └── Showing / Rings
│
├── Schedule Items
│   ├── Horse
│   ├── Source Type
│   ├── Source Record
│   ├── Date
│   ├── Start Time
│   ├── End Time
│   ├── Status
│   └── Notes
│
└── Calendar Views
    ├── By Day
    ├── By Hour
    └── By Horse
```

Rule:

```text
At any scheduled time, a horse is in one primary state:
Stall, Turnout, Schooling, or Showing.
```

### 13.2 Deeper Show-Day Schedule Roadmap

```text
Schedules
├── Show Days
├── Show Day Outlook
├── Show Day Print List
├── Show Day Roster Active Riders
├── Show Day Roster Active Horses
├── Show Day Rings
├── Show Day Ring Status
└── Show Day Ring
    ├── Show Day Class Times
    ├── Show Day Groups
    ├── Show Day Classes
    ├── Show Day Entries
    ├── Show Day Trip Times
    └── Show Day Trips
        ├── Show Day Live
        └── Show Day Results
```

Freshness rule:

```text
Every live/status response must carry an as_of timestamp.
```

---

## 14. Packing App Overview

The current implementation focus is the Packing App.

Packing has four plan pages/modules:

```text
Packing Plans
├── quantity
├── per_horse
├── per_groom
└── horse_specific
    └── label: Horse Kits
```

---

## 15. `pak_groups` Blueprint

`pak_groups` is the blueprint/page instruction map.

It is not packing data.

For each plan, `pak_groups` tells the app:

```text
what sections to render
what order they appear in
what physical Airtable table each section uses
what component type it should behave like
whether it supports search, filters, aggs, drawer, comments, logs, etc.
```

Each plan uses the same stack:

```text
header
primary_tabs
summary_aggs
secondary_controls
secondary_count_aggs
lane_controls
search
main_table
state_links
change_log
item_source
lane_source
slot_source
comments
```

### 15.1 What `pak_groups` Does

```text
Webflow page loads one plan route, like /wec-packing/quantity.
Backend reads pak_groups view for that plan.
Backend uses those rows to know which Airtable tables to pull.
Frontend receives the stack and renders the same UI structure/classes every time.
Actions write only to the plan's link table and log table.
Comments use the comments stack.
Print uses the same plan data.
```

### 15.2 What `pak_groups` Does Not Do

```text
It does not invent rows.
It does not backfill data.
It does not calculate fake counts.
It does not touch horse kits or legacy tables.
It does not decide business logic by itself.
```

Rule:

```text
pak_groups keeps every plan page consistent while each plan uses independent tables and rules.
```

---

## 16. Packing Plan Registry

### 16.1 `quantity`

```json
{
  "quantity": {
    "status": "built",
    "api_route": "/wec-packing/quantity",
    "print_route": "/wec-packing/quantity/print",
    "preview_url": "http://127.0.0.1:8792/packing-plan-preview.html?plan=quantity",
    "tables": {
      "source": "pak_byqtys",
      "items": "pak_byqty_items",
      "links": "pak_byqty_links",
      "logs": "pak_byqty_logs",
      "lanes": "pak_byqty_lanes",
      "slots": "pak_byqty_slots"
    },
    "pak_groups_stack_rows": 14,
    "controls": {
      "views": ["all", "wave_one", "wave_two", "not_going"],
      "lanes": ["open", "need", "packed", "left"],
      "aggs": ["items", "need", "touched", "packed", "left"]
    },
    "actions": ["session_ping", "set_item_count", "adjust_needed", "save_comment"],
    "data_status": "tables empty"
  }
}
```

### 16.2 `per_horse`

```json
{
  "per_horse": {
    "status": "built",
    "api_route": "/wec-packing/per-horse",
    "print_route": "/wec-packing/per-horse/print",
    "preview_url": "http://127.0.0.1:8792/packing-plan-preview.html?plan=per-horse",
    "tables": {
      "source": "pak_byhorses",
      "items": "pak_byhorse_items",
      "links": "pak_byhorse_links",
      "logs": "pak_byhorse_logs",
      "lanes": "pak_byhorse_lanes",
      "slots": "pak_byhorse_slots"
    },
    "pak_groups_stack_rows": 14,
    "controls": {
      "views": ["all", "wave_one", "wave_two", "not_going"],
      "lanes": ["open", "need", "packed", "left"],
      "aggs": ["items", "need", "touched", "packed", "left"]
    },
    "actions": ["session_ping", "set_item_count", "save_comment"],
    "data_status": "tables empty"
  }
}
```

### 16.3 `per_groom`

```json
{
  "per_groom": {
    "status": "built",
    "api_route": "/wec-packing/per-groom",
    "print_route": "/wec-packing/per-groom/print",
    "preview_url": "http://127.0.0.1:8792/packing-plan-preview.html?plan=per-groom",
    "tables": {
      "source": "pak_bygrooms",
      "items": "pak_bygroom_items",
      "links": "pak_bygroom_links",
      "logs": "pak_bygroom_logs",
      "lanes": "pak_bygroom_lanes",
      "slots": "pak_bygroom_slots"
    },
    "pak_groups_stack_rows": 14,
    "controls": {
      "views": ["all", "wave_one", "wave_two", "not_going"],
      "lanes": ["open", "need", "packed", "left"],
      "aggs": ["items", "need", "touched", "packed", "left"]
    },
    "actions": ["session_ping", "set_item_count", "save_comment"],
    "data_status": "tables empty"
  }
}
```

### 16.4 `horse_specific` / Horse Kits

```json
{
  "horse_specific": {
    "label": "Horse Kits",
    "status": "existing built module",
    "api_route": "/wec-packing/horse-kits",
    "print_route": "/wec-packing/horse-kits/print",
    "preview_url": "http://127.0.0.1:8792/horse-kits-static-proof-preview.html",
    "tables": {
      "entity_1": "pak_horses_roster",
      "kits": "pak_kits",
      "kit_items": "pak_kit_items",
      "links": "horse_packing_kits",
      "logs": "horse_kit_changes",
      "lanes": "pak_lanes",
      "comments": "wec_commenting"
    },
    "pak_groups_view": "horse_specific",
    "controls": {
      "views": ["all", "wave_one", "wave_two", "not_going"],
      "lanes": ["open", "need", "packed", "left"],
      "item_filters": ["all", "not_packed", "packed", "not_needed"]
    },
    "actions": ["session_ping", "set_packing_kit_state", "save_comment"],
    "logic": {
      "kit_assignment": "preloaded before page",
      "one_horse": "one kit",
      "item_state": ["not_packed", "packed", "not_needed"],
      "counts": ["need", "packed", "left"],
      "no_quantity": true
    }
  }
}
```

---

## 17. Packing Plan Logic

### 17.1 Shared Rule

```text
Needed can be static or dynamic.
Packed is always ledger/action based, except Horse Kits where item state is status based.
Remaining is calculated.
Changes and reversals are logged.
Exceptions can move an item into unresolved, purchase_onsite, packed_max, or another approved state.
```

### 17.2 Quantity Plan

```text
Manual starting quantity, such as 10 or 20.
Entries debit/credit against the quantity.
Reversals write the opposite debit/credit.
Starting value can be adjusted with a reason.
```

### 17.3 Per Horse Plan

```text
Each item has a multiplier.
Wildcard is total scoped horses from the wave table.
Needed = item_multiplier × wave_horse_count.
Horse count can change, so needed can change.
Packed does not change unless someone logs a packing action or reversal.
```

### 17.4 Per Groom Plan

```text
Needed is based on total horses and groom ratio/count from the wave table.
Horse count and groom ratio can change.
Needed can change.
Packed remains transaction-based.
```

### 17.5 Horse Kits / `horse_specific`

```text
Paks/Kits are template-driven.
Prepopulated kits include examples such as wec-kit, devon-kit, hot-weather-kit.
Each kit contains kit_items, not quantity/per_horse/per_groom items.
Kit items already include quantity if quantity is not 1.
Do not create every horse × kit item row upfront.
Create one Horse + Kit assignment.
Persist kit item state only when changed.
```

Statuses:

```text
not_packed
packed
not_needed
```

---

## 18. Packing App HTML / CSS Stack

All 4 plans use the same stack HTML pattern and the same CSS class family under:

```html
<main id="packing-app" class="rsa-dashboard"></main>
```

### 18.1 `quantity`, `per_horse`, `per_groom` Root

```html
<main id="packing-app" class="rsa-dashboard">
  <div
    id="packing-plan"
    data-rs-plan
    data-plan-key="quantity"
    data-api-url="https://ringstatus.com/test/wec-packing/quantity"
    data-print-url="https://ringstatus.com/test/wec-packing/quantity/print"
    data-pack-wave-key="wave_one"
  >
    Loading packing plan...
  </div>
</main>
```

### 18.2 Horse Kits Root

```html
<main id="packing-app" class="rsa-dashboard">
  <div
    id="horse-kits"
    data-api-url="https://ringstatus.com/test/wec-packing/horse-kits"
    data-print-url="https://ringstatus.com/test/wec-packing/horse-kits/print"
    data-pack-wave-key="wave_one"
  >
    Loading horse kits...
  </div>
</main>
```

### 18.3 Shared Stack Sections

```html
<section class="rs-stack-section is-header"></section>
<section class="rs-stack-section is-primary-tabs"></section>
<section class="rs-stack-section is-summary-aggs"></section>
<section class="rs-stack-section is-secondary-controls"></section>
<section class="rs-stack-section is-count-aggs"></section>
<section class="rs-stack-section is-lane-controls"></section>
<section class="rs-stack-section is-search"></section>
<section class="rs-stack-section is-main-table"></section>
<section class="rs-stack-section is-comments"></section>
```

### 18.4 Stylesheets

```html
<link rel="stylesheet" href="rsa-stylesheets.locked.css">
<link rel="stylesheet" href="styles.css">
<link rel="stylesheet" href="horse-kits-static-proof.css">
<link rel="stylesheet" href="packing-plan.css">
```

`packing-plan.css` is loaded by the new plan pages.

### 18.5 Core Shared Classes

```css
#packing-app .rs-page-header {}
#packing-app .rs-page-title {}
#packing-app .rs-page-subtitle {}
#packing-app .rs-stack-section {}
#packing-app .rs-stack-label {}
#packing-app .rs-stack-tabs {}
#packing-app .rs-stack-pill {}
#packing-app .rs-stack-pill.is-active {}
#packing-app .rs-stack-aggs {}
#packing-app .rs-stack-agg {}
#packing-app .rs-stack-agg-value {}
#packing-app .rs-stack-agg-label {}
#packing-app .rs-search-wrap {}
#packing-app .rs-search {}
#packing-app .rs-search-clear {}
#packing-app .rs-airtable-scroll {}
#packing-app .rs-airtable-grid {}
#packing-app .rs-sort-head {}
#packing-app .rs-row-gutter {}
#packing-app .rs-col-entity {}
#packing-app .rs-col-count {}
#packing-app .rs-entity-cell {}
#packing-app .rs-entity-horse {}
#packing-app .rs-entity-sub {}
#packing-app .rs-cell-number {}
#packing-app .rs-drawer-overlay {}
#packing-app .rs-record-drawer {}
#packing-app .rs-drawer-head {}
#packing-app .rs-drawer-title-group {}
#packing-app .rs-drawer-close {}
#packing-app .rs-drawer-body {}
#packing-app .rs-summary-metrics {}
#packing-app .rs-kit-progress {}
#packing-app .rs-kit-progress-track {}
#packing-app .rs-kit-progress-bar {}
#packing-app .rs-kit-item-row {}
#packing-app .rs-kit-item-main {}
#packing-app .rs-kit-item-title {}
#packing-app .rs-kit-actions {}
#packing-app .rs-state-button {}
#packing-app .rs-state-button.is-active {}
#packing-app .rs-comments {}
#packing-app .rs-comment-form {}
#packing-app .rs-comment-short {}
#packing-app .rs-comment-input {}
#packing-app .rs-comment-row {}
```

---

## 19. Reports / Display / Print

Reports are separate from activity, logs, and comments.

```text
Reports
├── Report Definitions
├── Display Views
└── Print Outputs
```

Rule:

```text
Reports define the data.
Display Views show it in the app.
Print Outputs turn it into paper/PDF.
```

Packing-specific print routes exist, but final templates still need work.

---

## 20. Fanout JSON + Notes

```text
Fanout JSON = app-ready calculated output
Notes = human-readable context, exceptions, instructions, and warnings
```

Rule:

```text
Fanout JSON tells the app what to do.
Notes tell the human what to know.
```

---

## 21. Places / Pins / Routes

The previous skeleton adds this broader system.

```text
Places
├── Pins
│   ├── home
│   ├── showgrounds
│   └── route points
├── Route Places
├── Equine Places
├── Dining Places
├── Stay Places
├── Locale Places
└── Attractions
```

Rule:

```text
Lists = things to complete, bring, check, reference, or group.
Pins / Places = physical locations used by lists, routes, Waze, alerts, and comments.
```

This should remain broader platform context, not part of the current packing plan registry.

---

## 22. Alerts / SMS / Ring Waze

### 22.1 Alerts

Alerts are one-way subscription feeds.

```text
Subscribers → Subscriptions → Alert Threads → Alert Thread Items
```

### 22.2 SMS Two-Ways

SMS Two-Ways are request/response lookup apps.

```text
SMS Two-Ways
├── Ring Lookup
│   ├── ring_now
│   ├── ring_next
│   ├── team_next_focus
│   └── as_of
├── Horse Lookup
│   ├── horse_now
│   ├── horse_next
│   ├── horse_next_focus
│   └── as_of
├── Rider Lookup
└── Ring Waze
    └── last 3 ring comments
```

Rule:

```text
SMS Two-Ways do not own schedule data.
They read from schedule, horse, people, comments, and Ring Waze data.
```

### 22.3 Ring Waze

Ring Waze is a ring-specific human update/comment/check-in system. It is not GPS navigation.

```text
Ring Waze
├── Ring Waze Rings List
├── Ring Waze Ring
├── Ring Waze Ring Comments
├── User Check-In At Ring
├── SMS Comment Intake
└── Web Comment Intake
```

Rule:

```text
A Ring Waze comment can enter from SMS or Web UI,
but should land in the same ring comments feed.
```

---

## 23. Current Tool Infrastructure

```text
Retool
- Internal admin apps, dashboards, operational interfaces.

Webflow
- Public/front-facing pages, embeds, CMS-driven UI, mobile presentation layer.

Webflow Cloud / Astro
- API routes, server-side endpoints, health checks, protected connectors.

Codex
- Development, editing, local implementation, repo-aware handoff work.

GitHub
- Source files, docs, static JS/CSS, templates, source packages.

jsDelivr
- CDN delivery of pinned GitHub static assets into Webflow.

Cloudflare Workers
- Workers, APIs, SMS webhook handling, live lookup/proxy/intake endpoints.

Twilio
- Incoming SMS, two-way SMS replies, subscribe/unsubscribe, Ring Waze SMS comment intake.

Airtable
- Operational database, live records, show data, waves, horses, packing plans, comments, subscriptions, board data.

Webflow MCP / Designer Tools
- Native Designer/page/component work when exposed.

Live show-data sources
- Ring, class, trip, result, heartbeat, order, and status data inputs.
```

---

## 24. Required RingStatus Preflight

Before RingStatus implementation work, read/use:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\ringstatus_runner_options_overview.md
```

Required preflight:

```text
1. Identify the correct lane.
2. Check exposed Codex skills/plugins/MCP/connectors.
3. Run git status --short.
4. State likely touched files.
5. Confirm live Airtable schemas, env vars, Webflow page IDs, deployment routes, or loaded browser assets.
6. Use lane-specific handoff docs.
7. Verify locally, in browser, or through live GET/health endpoints.
8. Require explicit approval before live Airtable PATCH/POST/write actions unless already approved in the current task.
```

---

## 25. Integration Lanes

```text
1. Native Webflow Designer / MCP edits
2. Manual Webflow embeds
3. GitHub / jsDelivr pinned static assets
4. Webflow Cloud / Astro API routes
5. Two-way Airtable read/write connectors
6. Static dataset + Airtable enrichment flows
7. RS template / source package work
8. Schedule, heartbeat, trips, and runner workflows
9. Daily schedule app UI work
10. Cloudflare SMS / live lookup workers
11. Equestrian caption app work
```

Rule:

```text
Route first. Confirm live surfaces second. Touch only the needed lane third. Verify before publish. Ask for approval before live writes.
```

---

## 26. Current Packing App State

Current state:

```text
First end-to-end lane is built but not complete.
Main blocker: new plan tables are empty.
State/action behavior cannot be fully proven until real item rows exist.
```

### 26.1 Needed Real Airtable Data

```text
Populate pak_byqtys, pak_byqty_items.
Populate pak_byhorses, pak_byhorse_items.
Populate pak_bygrooms, pak_bygroom_items.
Add real starting quantities, multipliers, wildcard keys, units, active flags, sort order.
```

### 26.2 Action Verification

```text
Test +1, -1, MAX.
Test reversal debit/credit logs.
Test quantity needed adjustment.
Confirm links write only to each plan's own link table.
Confirm logs write only to each plan's own log table.
```

### 26.3 Sessions

```text
Session record creation works.
Still need heartbeat/polling cadence.
Still need idle timeout behavior.
Still need duplicate-action prevention.
```

### 26.4 Comments

```text
Add works structurally.
Still need edit/save behavior.
Still need comment thread accordion pattern.
Still need comment log/trail behavior.
```

### 26.5 Webflow Embeds / Pages

```text
Need final embeds for quantity, per_horse, per_groom.
Need Webflow page slugs/paths confirmed.
Need CDN build/hash path decided.
```

### 26.6 Print / PDF

```text
Print HTML routes exist.
Need final PDF worker/direct print path wired like production.
Need print buttons verified from Webflow page context.
```

### 26.7 CSS / UI QA

```text
Mobile 393px.
Drawer.
Table overflow.
Active buttons.
Aggs.
Comments.
Empty vs populated rows.
```

### 26.8 Documentation

```text
Need final JSON shapes per plan.
Need allowed read/write fields per route.
Need embed docs.
Need do-not-use-legacy-tables contract.
```

---

## 27. Still Open

```json
{
  "still_open": {
    "home_overview": {
      "status": "not built",
      "purpose": "top-level packing dashboard",
      "needs": ["summary aggs", "plan links", "active wave status", "alerts"]
    },
    "action_lists": {
      "status": "not built",
      "lists": ["purchase_onsite", "needs_attention", "unresolved", "packed_max"],
      "needs": ["list views", "counts", "search", "open row detail"]
    },
    "entities_ui": {
      "status": "not built",
      "purpose": "add/edit source records",
      "entities": ["horses", "items", "kits", "quantity items", "per horse items", "per groom items", "comments"],
      "needs": ["add", "edit", "save", "audit log"]
    },
    "printing_templates": {
      "status": "routes exist but templates need final build",
      "templates": {
        "packing_list": "print active item list with need/packed/left",
        "logs": "print change/audit history"
      }
    },
    "full_list_search": {
      "status": "not built",
      "views": {
        "packing_list": "search all plan/list items",
        "logs": "search all action/change logs"
      },
      "needs": ["global search", "plan filter", "wave filter", "status filter"]
    },
    "comments_feed": {
      "status": "not complete",
      "needs": ["add", "edit", "save", "thread accordion", "latest-first sorting", "log trail"]
    },
    "sessions": {
      "status": "session create works, heartbeat/polling not complete",
      "needs": ["heartbeat cadence", "idle timeout", "click-triggered sync", "avoid duplicate in-flight actions"]
    }
  }
}
```

---

## 28. Known Trouble Spots / Guardrails

### 28.1 Naming Drift

Risk:

```text
Shows, Events, Event Ops, Showgrounds, Series, Weeks, Days can become mixed.
```

Guardrail:

```text
Use Shows > Show for front-facing.
Use Event Ops / Event Week / Event Day for architecture when needed.
Do not rename built packing routes/tables.
```

### 28.2 Tenant vs Event Nesting

Guardrail:

```text
Tenants and Events stay separate.
Tenant Events join them.
```

### 28.3 Horses Under Events

Guardrail:

```text
Horses are tenant-owned master records.
Tenant Events link attending horses into event operations.
```

### 28.4 Boards Becoming Independent Apps

Guardrail:

```text
Slots, Lanes, and Bindings come first.
Boards display the bindings.
```

### 28.5 Lists vs Places vs Pins

Guardrail:

```text
Lists are operational containers.
Places/Pins are location primitives.
Routes relate places/pins.
```

### 28.6 SMS Freshness

Guardrail:

```text
Every ring/horse response must include as_of.
Do not claim live certainty without a fresh timestamp.
```

### 28.7 External Data Shape Drift

Watch for:

```text
ring_number string vs number
class id string vs number
class groups optional
entries missing trips
completed vs live status
estimated_start_time vs go_time
curr_updated_at vs last_updated
```

### 28.8 Live Write Safety

Guardrail:

```text
Use read-only inspection first.
Require explicit approval before live Airtable writes or production changes.
```

### 28.9 Webflow Lane Confusion

Guardrail:

```text
Identify the lane before touching files or routes.
Confirm what is live before recommending publish.
```

### 28.10 Overbuilding Before Proof

Guardrail:

```text
Start with the smallest test that proves the lane.
For packing, seed real rows first, then verify actions.
For broader RingStatus, recognition → tenant event → one horse → one ring lookup → one Ring Waze comment → one visible board/list result.
```

---

## 29. Merge Decisions

### 29.1 Keep From Latest

```text
Packing plan registry.
pak_groups blueprint.
Built routes and print routes.
Airtable table names.
Plan-specific actions.
Root HTML pattern.
Shared CSS class family.
Current blocker and still-open list.
```

### 29.2 Import From Previous Skeleton

```text
Tenant Events join model.
Event Ops / Event Series / Event Week / Event Day time hierarchy.
Recognizers and Subscribers.
People / Profiles / Show Riders distinction.
Places / Pins / Routes.
Boards built from Slots / Lanes / Bindings.
Tasks.
Alerts / SMS Two-Ways / Ring Waze.
Technology lane preflight.
Integration pitfalls and guardrails.
Growth path.
```

### 29.3 Do Not Import As Overrides

```text
Do not rename current packing plans.
Do not replace pak_groups with generic pack_lists.
Do not move Horse Kits into quantity/per_horse/per_groom logic.
Do not treat old Lists section as current packing implementation.
Do not assume older conceptual names are actual Airtable tables.
```

---

## 30. Best Next Step

For the current app, the next step remains:

```text
Seed a small real Airtable dataset for quantity, per_horse, and per_groom.
Then verify +1, -1, MAX, reversals, quantity adjustment, comments, sessions, links, and logs.
```

For the broader infrastructure, the next section to define later is:

```text
Tenant Event + Linking Modules + Board Builder
```

Reason:

```text
Tenant Events decide who/what participates.
Slots, Lanes, and Bindings decide how boards, lists, tasks, and schedules connect.
Without these, app surfaces will drift.
```
