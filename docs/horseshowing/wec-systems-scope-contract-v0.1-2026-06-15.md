# WEC Systems Scope Contract

Version: `0.1`

Date: `2026-06-15`

Status: current scope boundary map

Purpose: define the specific WEC systems, their ownership, their integration boundaries, and the rule that shared data may flow between systems but must not collide or silently override another system's responsibility.

## Top-Level Systems

```text
wec-horses
wec-pak
wec-onsite
```

These systems may share data through approved Airtable, Catalyst, Webflow, Git, and local heartbeat paths. They must not share uncontrolled state, duplicate source-of-truth ownership, or create competing workflows for the same output.

## System Scope

### wec-horses

Session:

```text
019e453e-2742-7be0-b265-b1f91b23c1aa
```

Purpose:

```text
horse/helper data, horse display names, barn names, horse-related support data
```

Boundary:

```text
may feed helper values into wec-onsite
must not own schedule, alerts, print/mobile render payloads, class start times, or entry go times
```

### wec-pak

Session:

```text
019e8abb-bb05-7d42-8ce6-c4eebea060d4
```

Purpose:

```text
PAK-specific workflow and package/system context
```

Boundary:

```text
may share approved helper or operator context
must not own wec-onsite schedule, live status, alerts, print, mobile, or Catalyst schedule source of truth
```

### wec-onsite

Session:

```text
019ecc35-c5f9-79e0-bb20-3881f7071c7d
```

Purpose:

```text
onsite WEC operational workflow
schedule surfaces
alerts
results surface
mobile/print outputs
Catalyst schedule lane
Waze/two-way/boards extensions
```

wec-onsite contains:

```text
wec-boards
wec-alerts
wec-results
wec-mobile
wec-print
wec-mobile-pro
wec-catalyst
wec-waze
wec-twoway
```

Boundary:

```text
wec-onsite owns the operational schedule experience
wec-onsite must be aware of Webflow, Airtable, Catalyst, Git repo, and local heartbeat integrations
wec-onsite must not treat helper systems as schedule truth
wec-onsite must not allow one sub-system to silently override another sub-system's output
```

## wec-onsite Subsystems

### wec-boards

Session:

```text
019ecade-ec16-76b2-8754-b63039c6313f
```

Purpose:

```text
board/display output lane
```

Boundary:

```text
may consume current schedule/render data
must not write core schedule truth
```

### wec-results

Session:

```text
019ecade-9f16-71d0-ab1e-9abc46040a64
```

Purpose:

```text
results output lane
```

Boundary:

```text
may consume schedule identifiers and class context
must not drive class schedule, active focus day, alerts, mobile, or print
```

### wec-mobile-pro

Session:

```text
019ecbea-bf53-74a0-aa75-562d91eb0d61
```

Purpose:

```text
pro/mobile enhancement lane
```

Boundary:

```text
may build on current mobile data contracts
must not replace wec-mobile source paths or change live mobile behavior without explicit adoption
```

### wec-catalyst

Session:

```text
019ecb76-16ff-7ad2-a8d7-e310b5dd008a
```

Purpose:

```text
Catalyst schedule lane
core Horseshowing data ingestion
live render payloads
mirror support into Airtable
```

Boundary:

```text
owns operational schedule state for wec-onsite
must keep Airtable mirrors aligned
must not treat Airtable mirrors as separate schedule truth
```

### wec-waze

Session:

```text
019ec80a-cfe5-7163-9cef-6f43d5467b27
```

Purpose:

```text
Waze/navigation support lane
```

Boundary:

```text
may consume show/focus context and onsite metadata
must not write schedule, alerts, mobile, print, or class timing truth
```

### wec-twoway

Session:

```text
019ecc4a-ce97-7fe2-bf54-0e02961bec84
```

Purpose:

```text
two-way edit/control lane
```

Boundary:

```text
no production behavior until a contract, source of truth, and approval path exist
```

### wec-print

Session:

```text
019ec157-f561-7f61-9677-36fa2a42deef
```

Purpose:

```text
print schedule surface and PDF/share path
```

Boundary:

```text
reads current Catalyst render payload
static JSON is fallback only
must not own schedule logic or helper values
must not diverge from wec-mobile schedule source
```

### wec-mobile

Session:

```text
019ec158-ef84-7bf0-9e97-c5d81af66479
```

Purpose:

```text
front-facing mobile schedule surface
filters
trainer/horse rollups
print link
hide-mode UI where approved
```

Boundary:

```text
reads current Catalyst render payload
static JSON is fallback only
must not own schedule truth
must not independently compute conflicting rollups when Catalyst provides them
```

### wec-alerts

Session:

```text
019ec158-90cc-7261-a81c-1b3a39c65e6b
```

Purpose:

```text
alert records from class_start_times and entry_go_times
```

Boundary:

```text
alerts are downstream results
alerts must not drive schedule, class_start_times, entry_go_times, mobile, or print
```

## Related WEC / Codex Context

### wec-pro_app

Session:

```text
019e276e-6ba0-7c23-a0df-66e8461e51d8
```

Purpose:

```text
WEF/pro app context
```

Boundary:

```text
may provide known patterns
must not be copied into WEC without explicit fit check and approval
```

### wec-codex

Sessions:

```text
019ec69b-52a4-70a2-9a3d-8415587d373f
019e5486-050c-7473-8dd3-62bb39fabc17
019e50fa-222d-7c83-a021-1b396df2ffe0
```

Purpose:

```text
CLI / MDX / Airtable / Webflow Cloud / Catalyst operating context
```

Boundary:

```text
may provide tooling and implementation support
must not redefine WEC source of truth, cadence, or output ownership
```

## Integration Boundaries

### Airtable

Owns:

```text
manual controls
helper edits
operator review
mirrors of Catalyst output
```

Must not:

```text
become an unapproved competing schedule source
silently override Catalyst core data
drive mobile/print through stale mirrors when Catalyst has newer current state
```

### Catalyst

Owns:

```text
wec-onsite operational schedule state
core Horseshowing ingestion
live status enrichment
render-ready payloads for mobile and print
```

Must not:

```text
ignore Airtable-owned helper/manual controls
use stale helper values after approved sync
create schedule rows from helper tables alone
```

### Webflow

Owns:

```text
published surfaces
embed placement
page shell
```

Must not:

```text
contain divergent mobile and print data contracts
depend on manual schedule JSON as primary source
hide source-path failures behind stale embeds
```

### Git Repo

Owns:

```text
source files
docs
local drops
workflow scripts
audit artifacts
```

Must not:

```text
be treated as current production state without live verification
contain undocumented workflow forks
```

### Local Heartbeat

Owns:

```text
local cadence runner
workflow orchestration
scheduled checks
```

Must not:

```text
change shared epoch/system heartbeat behavior for WEC only
run WEC when active controls conflict
continue after hard workflow failure
```

## Collision Rules

No subsystem may introduce:

```text
new source of truth
new table ownership
new cadence path
new render source
new helper sync direction
new write-back path
```

unless the change is documented and approved in the relevant system contract.

If two systems need the same data, the allowed pattern is:

```text
one owner
one current source
many consumers
mirrors clearly labeled as mirrors
fallbacks clearly labeled as fallbacks
```

If two systems disagree, the workflow must stop at the boundary and resolve ownership before writing data.

## Required Cross-System Check Before Changes

Before modifying any WEC system, identify:

```text
system name
subsystem name
session/thread id
source of truth affected
integration affected
customer-facing output affected
mirror/fallback affected
heartbeat/cadence affected
audit artifact required
```

No change should proceed when the affected owner is unclear.
