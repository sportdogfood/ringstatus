# RingStatus Skeleton Hierarchy + Operating Documentation

Version: 2026-06-01
Status: Working architecture draft
Scope: RingStatus event-ops, tenant participation, schedules, lists, boards, tasks, alerts, SMS, comments, Ring Waze, profiles, horses, places, and linking modules.

---

## 1. Purpose

RingStatus is not only a show schedule app. The real working container is broader: tenant-aware event operations.

The system needs to support barns traveling to and operating at shows, including packing, shipping, arrival, stalls, turnout, feed, schedules, alerts, SMS lookups, comments, Ring Waze updates, return packing, and handoff back to home systems.

Core operating idea:

- Tenants are paying barns / active clients.
- Events are show/travel/ops containers.
- Not all tenants attend all events.
- Tenant Events connect one tenant to one event/week/day and bring in the correct horses, people, lists, boards, schedules, alerts, and SMS permissions.
- Horses and people remain master records, but they must be deeply integrated into event operations.

---

## 2. Top-Level RingStatus Skeleton

```text
RingStatus
├─ Recognizers [APP]
├─ Subscribers [APP]
├─ Tenants [DATA]
├─ People [DATA + MODULE]
├─ Horses [DATA + MODULE]
├─ Show Riders [DATA + MODULE]
├─ Places [DATA + APP]
├─ Events / Event Ops [DATA + APP]
├─ Tenant Events [MODULE]
├─ Linking Modules [MODULE]
└─ Technology / Integrations [REFERENCE]
```

---

## 3. Core Definitions

### 3.1 Recognizers

Recognizers are thin user-recognition or login-gate surfaces.

Purpose:

- Recognize users.
- Show login when needed.
- Help determine whether a visitor is known, unknown, subscribed, tenant-linked, or role-linked.

Tag:

```text
Recognizers [APP]
```

---

### 3.2 Subscribers

Subscribers sit outside the tenant container.

Purpose:

- Allow users to subscribe.
- Allow users to unsubscribe.
- Support SMS/email/feed participation even when a person is not a full tenant user.

Important distinction:

```text
Subscribers = public opt-in / opt-out identity layer
Subscriptions = what a subscriber is subscribed to
Tenants = paying barns / established clients
```

Subscribers should not be nested inside Tenants because outside users may need alerts, feeds, or unsubscribe controls without being tenant staff.

Tag:

```text
Subscribers [APP]
```

---

### 3.3 Tenants

Tenants are active established barns, usually paying clients.

Purpose:

- Hold barn/client identity.
- Hold minor personalization.
- Own master horse records.
- Own or relate to people/user profiles.

Working shape:

```text
Tenants [DATA]
└─ Tenant Profile [DATA]
   └─ minor personalization for active barns / paying clients
```

Important rule:

```text
Tenants do not contain every event.
Tenant Events connect tenants to the events they actually attend.
```

---

### 3.4 People / Profiles

The `*_profiles` naming is not locked. It was used because it was easier to explain. Use conceptual names first; table names can be decided later.

Purpose:

- Store reusable user/person records.
- Store roles and permissions.
- Support trainers, grooms, riders, staff, and other users.

Working shape:

```text
People [DATA + MODULE]
└─ Person Profiles [DATA]
   ├─ trainers
   ├─ grooms
   ├─ riders
   ├─ staff
   └─ users / roles / permissions
```

Important distinction:

```text
Person Profiles = reusable people/users/roles
Show Riders = show-specific rider participation records
```

---

### 3.5 Horses

Horses are critical master records and must integrate deeply with event operations.

Purpose:

- Store horse input attributes.
- Store feed instructions.
- Support feed lists, feed items, and feed rations.
- Bind horses into tenant-event schedules, boards, tasks, SMS lookups, and alerts.

Working shape:

```text
Horses [DATA + MODULE]
└─ Horse Profile [DATA]
   ├─ input attributes
   ├─ feed instructions
   ├─ feed lists
   ├─ feed items
   └─ feed rations
```

Important rule:

```text
Horses remain tenant-owned master records.
Events/Ops cannot function correctly without linked horses.
```

---

### 3.6 Show Riders

Show Riders should stay separate from general profiles.

Purpose:

- Represent show-specific rider participation.
- Allow daily snapshots.
- Keep rider entries, schedule involvement, and show-day context separate from the general person profile.

Working shape:

```text
Show Riders [DATA + MODULE]
└─ show-specific rider records
```

Important rule:

```text
A person can be a rider in general, but a Show Rider is a show/event/day-specific record.
```

---

## 4. Event Ops Model

“Shows” was too narrow. The better working parent is:

```text
Event Ops
```

Reason: the system needs to support the full travel/show lifecycle, not just show schedule data.

Use case example:

```text
Travel to WEC Summer Series
├─ pack
├─ ship
├─ manage onsite stalls
├─ manage onsite turnout
├─ complete arrival tasks
├─ allow comments
├─ populate places for location
├─ populate places on route if far
├─ manage onsite feed
├─ show schedules
├─ show alerts
├─ show SMS
├─ show Ring Waze
├─ reverse pack
├─ ship home
└─ handoff to home systems
```

Working shape:

```text
Events / Event Ops [DATA + APP]
├─ Event Groups
├─ Event Series
├─ Event Weeks
├─ Event Days
├─ Schedules
├─ Lists
├─ Boards
├─ Tasks
├─ Alerts
├─ SMS
├─ Comments
└─ Waze / Ring Waze
```

---

## 5. Event Naming Layers

The old “Shows” label can create confusion. Examples like WEF, USHJA, WEC, and ESP are not the same kind of thing as a show week or show day.

Working concept:

```text
Event Group / Show Brand / Venue / Circuit
├─ WEF
├─ USHJA
├─ WEC
└─ ESP

Event Series
└─ WEC Summer Series

Event Week
└─ WEC Summer Series + start_date + end_date

Event Day
└─ WEC Summer Series + date
```

Open naming decision:

```text
Possible parent names:
- Events
- Event Ops
- Show Ops
- Showgrounds
- Circuits
```

Current recommendation:

```text
Event Ops = working system container
Event Series / Event Weeks / Event Days = schedule/time hierarchy
```

---

## 6. Tenant Events

Tenant Events are the bridge between Tenants and Event Ops.

Purpose:

- Connect one tenant to one event/week/day.
- Determine which horses are attending.
- Determine which people/profiles are attending.
- Scope lists, boards, tasks, alerts, SMS, comments, and permissions.

Working shape:

```text
Tenant Events [MODULE]
├─ Tenant
├─ Event Week / Event Day
├─ Attending Profiles
├─ Attending Horses
├─ Event Lists
├─ Event Boards
├─ Event Tasks
├─ Event Alerts
├─ Event SMS
└─ Event Places / Pins / Routes
```

Rule:

```text
Tenants = barns / clients
Events = show/travel/ops containers
Tenant Events = which tenant participates in which event
```

Example:

```text
Tenant: Blue Ribbon Barn
Event: WEC Summer Series Week 2
Horse: Navy
Profiles: Trainer, Groom, Rider
Tenant Event connects all of these for one operational event context.
```

---

## 7. Places, Pins, and Routes

Places should be a reusable location system. They support lists, routes, Waze/Ring Waze context, travel planning, comments, and event operations.

Working shape:

```text
Places [DATA + APP]
├─ Pins
│  ├─ home
│  ├─ showgrounds
│  └─ route points
│
├─ Route Places
│  └─ places along route
│
├─ Equine Places
│  ├─ tack
│  ├─ vet
│  └─ feed
│
├─ Dining Places
│  ├─ AM / breakfast
│  ├─ lunch
│  └─ dinner
│
├─ Stay Places
│  ├─ RV
│  ├─ hotel
│  └─ suites
│
├─ Locale Places
│  ├─ pharmacy
│  ├─ grocery
│  └─ walk-in clinics
│
└─ Attractions
   └─ away from barns
```

Examples:

```text
places-wec-ocala-fl
pins-home-wellington-fl
pins-wec-ocala-fl
route-home-wellington-fl-to-wec-ocala-fl
```

Important distinction:

```text
Lists = things to complete, bring, check, reference, or group
Pins / Places = physical locations used by lists, routes, Waze, alerts, and comments
```

---

## 8. Lists

Lists are operational containers. They may be checklists, reference lists, packing lists, place lists, or horse-specific kits.

Working shape:

```text
Tenant Event
└─ Lists [APP]
   ├─ Pack Waves
   │  └─ outbound / onsite / return / home-handoff
   │
   ├─ Pack Lists
   │  └─ one list per wave, category, tenant, or trip
   │
   ├─ Pack List Items
   │  └─ individual checklist rows
   │
   ├─ Pack Horse Kits
   │  └─ horse-specific packing sets
   │
   └─ Places Lists
      ├─ places-wec-ocala-fl
      └─ places-on-route-home-to-wec
```

Specific examples:

```text
pack_waves = big movement phases
pack_lists = actual checklists inside a wave
pack_list_items = checklist rows
pack_horse_kits = reusable or event-specific bundles tied to horses
places-wec-ocala-fl = location-based reference list
```

Rule:

```text
Lists are operational containers.
Pins are location primitives.
Horse Kits are horse-linked list bundles.
```

---

## 9. Boards

Boards are tenant-event working surfaces. They are not master records and not general show records.

Locked current board set:

```text
Boards [APP]
├─ Turnout Board
├─ Feed Board
├─ Stall Board
└─ Show-Day Board
```

From use cases:

```text
manage-onsite-stalls      → Stall Board
manage-onsite-turnout     → Turnout Board
manage-onsite-feed        → Feed Board
show-day                  → Show-Day Board
```

Recommended internal board structure:

```text
Boards
├─ Board Types
│  ├─ turnout
│  ├─ feed
│  ├─ stalls
│  └─ show-day
│
├─ Boards
│  └─ tenant + event + board_type
│
├─ Board Sections
│  └─ AM / Midday / PM / Barn / Ring / Done / Needs Review
│
└─ Board Items
   └─ horse + profile + task/feed/stall/slot/status
```

Board behavior:

```text
Turnout Board
└─ horse + turnout slot + handler + status

Feed Board
└─ horse + feed slot + feed instructions + status

Stall Board
└─ horse + stall assignment + setup status + notes

Show-Day Board
└─ horse + rider + ring/class/schedule + readiness/status
```

Rule:

```text
Boards = operational dashboards for one tenant at one event.
Board Items = actual rows/cards tied to horses, profiles, slots, tasks, or schedules.
```

---

## 10. Linking Modules

Slots, lanes, and bindings must be established before building reliable boards.

Working shape:

```text
Linking Modules [MODULE]
├─ Slots
├─ Lanes
└─ Bindings
```

Definitions:

```text
Slots = when
Lanes = where / workflow column
Bindings = who or what is assigned
Boards = display surfaces built from bindings
```

Example binding:

```text
Horse Navy + Groom Sarah + AM Slot + Turnout Lane + WEC Summer Week
```

That binding can appear on the Turnout Board.

Potential slot examples:

```text
AM
Midday
PM
Evening
Custom Time
Date
Day
Week
```

Potential lane examples:

```text
Barn
Ring
Turnout
Feed
Stall
Ship
Done
Review
```

Important rule:

```text
Boards should not invent their own timing/columns independently.
They should read from shared Slots, Lanes, and Bindings.
```

---

## 11. Schedules

Schedules are their own app group under Event Ops.

Top-level schedule surfaces:

```text
Schedules [APP]
├─ Day Boards
├─ Print Lists
├─ Schedule App
└─ Pro Schedule App
```

Distinction:

```text
Day Boards = schedule-by-day working board
Print Lists = printable/exportable schedule output
Schedule App = simple user-facing schedule view
Pro Schedule App = deeper manager/admin schedule surface
```

Important rule:

```text
Day Boards are schedule-derived boards.
They are not the same as Turnout, Feed, Stall, or Show-Day boards.
```

---

## 12. Schedule Basics Detailed Shape

User-provided schedule basics:

```text
Schedules
├─ Show Days
│  └─ Show Day
│
├─ Show Day Outlook
├─ Show Day Print List
├─ Show Day Roster Active Riders
├─ Show Day Roster Active Horses
│
├─ Show Day Rings
├─ Show Day Ring Status
│
└─ Show Day Ring
   ├─ Show Day Class Times
   │  └─ start_time
   │
   ├─ Show Day Groups
   │  └─ optional
   │
   ├─ Show Day Classes
   │  └─ groups + classes
   │
   ├─ Show Day Entries
   │  └─ classes + entries
   │
   ├─ Show Day Trip Times
   │  └─ go_time
   │
   └─ Show Day Trips
      └─ entries + trips
         ├─ Show Day Live
         └─ Show Day Results
```

Recommended normalized shape:

```text
Schedules [APP]
├─ Show Days [DATA]
│  └─ Show Day [DATA]
│
├─ Show Day Outlook [APP]
│  └─ high-level daily overview
│
├─ Show Day Print List [APP]
│  └─ printable/exportable daily schedule
│
├─ Show Day Roster [APP/DATA]
│  ├─ Active Riders
│  └─ Active Horses
│
├─ Show Day Rings [APP]
│  ├─ Show Day Ring Status [APP]
│  │  └─ ring-level status feed
│  │
│  └─ Show Day Ring [APP/DATA]
│     ├─ Show Day Class Times [DATA]
│     │  └─ start_time
│     │
│     ├─ Show Day Groups [DATA]
│     │  └─ optional class group layer
│     │
│     ├─ Show Day Classes [DATA]
│     │  └─ groups + classes
│     │
│     ├─ Show Day Entries [DATA]
│     │  └─ classes + entries
│     │
│     ├─ Show Day Trip Times [DATA]
│     │  └─ go_time
│     │
│     └─ Show Day Trips [DATA]
│        └─ entries + trips
│           ├─ Show Day Live [APP/DATA]
│           └─ Show Day Results [APP/DATA]
```

Rule:

```text
Show Day = date container
Show Day Rings = ring index / ring board
Show Day Ring = one ring detail
Classes / Entries / Trips = schedule data layers
Live / Results = status/output layers
```

Likely standalone schedule UI/app surfaces:

```text
Show Day Outlook
Show Day Print List
Active Riders
Active Horses
Show Day Rings
Show Day Ring Status
Show Day Ring
Show Day Live
Show Day Results
```

Open decision:

```text
Is Show Day Rings the main user-facing schedule board,
or is Show Day Outlook the main landing screen?
```

---

## 13. Alerts

Alerts are one-way feeds and subscriptions.

Important distinction:

```text
Alerts = one-way feed subscriptions
SMS Two-Ways = request/response lookups
Comments/Ring Waze = human updates that SMS can reference
```

Revised alert structure:

```text
Alerts [APP]
├─ Alert Threads [APP / MODULE]
│  └─ subscribable alert feeds
│
├─ Alert Thread Items [DATA]
│  └─ individual alerts inside the feed
│
├─ Subscriptions [MODULE]
│  └─ subscriber + alert_thread
│
├─ Active Alerts List [APP]
│  └─ visible current alerts for user / tenant / event
│
└─ Active Alert List Items [DATA]
   └─ current/live alert rows
```

Rule:

```text
Alert Thread = feed they can subscribe to
Alert Thread Item = one message/update in that feed
Subscription = who receives that feed
Active Alerts List = what is currently displayed
```

Examples:

```text
Alert Thread: WEC Summer Week 2 - Schedule Alerts
Alert Thread: Blue Ribbon Barn - Horse Alerts
Alert Thread: WEC Ocala Route Alerts
Alert Thread: Feed Board Alerts
```

Locked alert flow:

```text
Subscribers → Subscriptions → Alert Threads → Alert Thread Items
```

---

## 14. SMS Two-Ways

SMS Two-Ways are request/response lookup apps.

Working shape:

```text
SMS [APP]
└─ SMS Two-Ways [APP]
   ├─ Ring Lookup [APP]
   │  ├─ ring_now
   │  ├─ ring_next
   │  ├─ team_next_focus
   │  └─ as_of
   │
   ├─ Horse Lookup [APP]
   │  ├─ horse_now
   │  ├─ horse_next
   │  ├─ horse_next_focus
   │  └─ as_of
   │
   ├─ Rider Lookup [APP]
   │  └─ daily rider snapshot
   │
   └─ Ring Waze [APP]
      └─ last 3 ring comments
```

Rule:

```text
SMS Two-Ways do not own schedule data.
They read from schedule, horse, people, comments, and Ring Waze data.
```

Read sources:

```text
Schedules → Rings / Classes / Entries / Trips / Live
Horses → Horse Profiles / Horse Event Links
People → Rider Profiles / Show Rider Records
Comments → Ring Comments
Waze → Ring Waze Comments
```

Definitions:

```text
ring_now / ring_next = ring-level status
team_next_focus = tenant/team-filtered next important ring item

horse_now / horse_next = horse-level status
horse_next_focus = horse-filtered next important item

rider = daily snapshot only
ring_waze = latest comment feed for movement/context
```

Routing layer to define:

```text
SMS Request Types
├─ ring
├─ horse
├─ rider
└─ waze
```

---

## 15. Ring Waze

Ring Waze is a ring-specific human update/comment/check-in system. It is not GPS navigation.

Working shape:

```text
Ring Waze [APP]
├─ Ring Waze Rings List [APP]
│  └─ list of active rings for the show day
│
├─ Ring Waze Ring [APP]
│  └─ one ring-specific view
│
├─ Ring Waze Ring Comments [APP/DATA]
│  └─ comments/check-ins tied to one ring
│
├─ User Check-In At Ring [APP]
│  └─ user + ring + timestamp + optional comment
│
├─ SMS Comment Intake [APP]
│  └─ user sends sms: ring + comment
│
└─ Web Comment Intake [APP]
   └─ user posts comment: ring + comment
```

Comment intake paths:

```text
user-check-in-at-ring
user-sends-sms ring + comment
user-posts-comment ring + comment
```

Data shape:

```text
ring_waze_comment
├─ show_day
├─ ring
├─ user / subscriber / profile
├─ source: sms | web | check-in
├─ comment
├─ timestamp
└─ status
```

Rule:

```text
A Ring Waze comment can enter from SMS or Web UI,
but should land in the same ring comments feed.
```

Important reuse:

```text
SMS Two-Ways can use Ring Waze Comments for:
ring-waze = last 3 ring comments
```

---

## 16. Tasks

Tasks are operational action items, not schedule data and not board cards by default.

Known task examples:

```text
complete-arrival-tasks
setup tasks
shipping tasks
return-home tasks
handoff to home systems
```

Working shape:

```text
Tasks [APP]
├─ Task Lists
├─ Task Items
├─ Arrival Tasks
├─ Setup Tasks
├─ Shipping Tasks
├─ Return Tasks
└─ Home Handoff Tasks
```

Rule:

```text
Tasks can be displayed on boards,
but the task record should remain reusable outside the board view.
```

---

## 17. Comments

Comments are general user-posted notes. Ring Waze comments are a special ring-specific subtype.

Working distinction:

```text
Comments = general event/tenant/task/list comments
Ring Waze Comments = ring-specific comments/check-ins
```

Potential comment scopes:

```text
Tenant Event
Show Day
Ring
Board
Board Item
List
List Item
Task
Horse
Place
Route
```

Rule:

```text
A comment must always have a scope.
Avoid unscoped comments that cannot be displayed or filtered later.
```

---

## 18. Empty Shape JSON

The empty shape JSON is also provided as a separate file: `ringstatus_empty_shape_2026-06-01.json`.

It is intentionally not a final database schema. It is a conceptual skeleton for nested planning, app grouping, and future table design.

---

## 19. Sections Still To Address

Use this checklist to keep future conversations from drifting.

```text
1. Naming decisions
   - Events vs Event Ops vs Shows
   - People Profiles vs Profiles
   - Show Riders vs Riders
   - Waze vs Ring Waze
   - Alert Threads vs Active Alerts Lists

2. Tenant/Event ownership
   - What belongs to tenant master data
   - What belongs to event master data
   - What belongs to tenant-event participation

3. Horse integration
   - Horse profile fields
   - Feed instructions
   - Feed lists/items/rations
   - Horse event attendance
   - Horse-specific SMS and alerts

4. People/role integration
   - Trainers
   - Grooms
   - Riders
   - Staff
   - Subscribers
   - Permissions

5. Schedule data model
   - Show days
   - Rings
   - Ring status
   - Groups/classes/entries/trips
   - Live/results
   - as_of timestamp rules

6. Board builder model
   - Board types
   - Board sections
   - Board items
   - Slots
   - Lanes
   - Bindings

7. List model
   - Pack waves
   - Pack lists
   - Pack items
   - Horse kits
   - Reverse lists
   - Home handoff lists

8. Places/pins/routes
   - Home pins
   - Show pins
   - Route places
   - Equine services
   - Dining/stay/locale/attractions

9. Alerts model
   - Alert threads
   - Alert thread items
   - Subscriptions
   - Active alerts list
   - Alert delivery rules

10. SMS model
   - Two-way request types
   - Ring lookup
   - Horse lookup
   - Rider snapshot
   - Ring Waze lookup
   - Permissions and identity matching

11. Ring Waze model
   - Rings list
   - Ring detail
   - Comments
   - Check-ins
   - SMS intake
   - Web intake

12. UI/app tagging
   - Which pieces need standalone UI
   - Which are modules only
   - Which are reference data only

13. Airtable schema
   - Tables
   - Field names
   - Linked records
   - Rollups/lookups
   - Avoiding unsafe keys

14. Webflow/App surfaces
   - Public subscriber pages
   - Tenant dashboard
   - Schedule app
   - Pro schedule app
   - Boards app
   - Lists app
   - Alerts app
   - Ring Waze app

15. Cloudflare/Twilio/API routes
   - Incoming SMS
   - Web comment intake
   - Airtable read/write connector
   - Live schedule lookup
   - Caching/freshness

16. Security and permissions
   - Subscriber identity
   - Tenant membership
   - Horse/person visibility
   - Unsubscribe rules
   - Write permissions

17. Testing and verification
   - Local test data
   - Airtable test tables
   - Read-only first
   - No live PATCH/POST until approved
   - Freshness/as_of checks
```

---

## 20. Codex Prompt: Master Architecture Review

Use this prompt when asking Codex to inspect or organize the architecture without writing code immediately.

```text
You are working on the RingStatus project. Do not write code yet. Review the architecture below and produce a concise implementation plan only.

Goal:
Build a tenant-aware event-ops skeleton for RingStatus. This is not only a show schedule app. It must support tenants, horses, people, event participation, schedules, lists, boards, tasks, alerts, SMS two-way lookups, comments, places, pins, routes, and Ring Waze comments.

Required model:
- Tenants are paying barns / active clients.
- Subscribers are outside Tenants and support subscribe/unsubscribe.
- Tenant Events connect Tenants to Event Ops because not all tenants attend all shows.
- Horses and People are master records but must link heavily into Tenant Events.
- Show Riders are show-specific and separate from general People Profiles.
- Slots, Lanes, and Bindings are linking modules used to build boards.
- Boards are working surfaces: Turnout, Feed, Stall, Show-Day.
- Alerts use Alert Threads as subscribable feeds.
- SMS Two-Ways are request/response lookup apps and should not own schedule data.
- Ring Waze is ring-specific human comments/check-ins, not GPS navigation.

Instructions:
1. Identify the correct implementation lane before recommending work.
2. Do not assume Airtable schema, Webflow page IDs, env vars, or live routes are current.
3. Run/read available project docs first if available.
4. Use read-only inspection before proposing writes.
5. State likely touched files and data tables.
6. Identify risks, missing decisions, and next smallest test.
7. Do not perform live writes without explicit approval.
```

---

## 21. Codex Prompt: Airtable Schema Planning

```text
You are helping design Airtable test tables for RingStatus. Do not create code yet. Produce a table plan only.

Design around these concepts:
- recognizers
- subscribers
- tenants
- tenant profiles
- people/person profiles
- horse profiles
- show riders
- places/pins/routes
- event ops
- event groups/series/weeks/days
- tenant events
- schedules/show days/rings/classes/entries/trips/live/results
- lists/pack waves/pack lists/pack list items/horse kits
- boards/board types/board sections/board items
- slots/lanes/bindings
- tasks
- alerts/alert threads/alert thread items/subscriptions/active alerts list/items
- SMS two-way request types
- Ring Waze rings/comments/check-ins

Output:
- One proposed table list.
- For each table, give purpose and key linked records.
- Mark whether the table is DATA, APP, MODULE, or REFERENCE.
- Keep field names simple and avoid overbuilding.
- Do not use phone numbers as primary keys.
- Include test-table naming if this is a POC.
- Include risks and minimum viable test.
```

---

## 22. Codex Prompt: Board Builder / Linking Modules

```text
You are designing the RingStatus board builder. Do not write production code yet.

Goal:
Boards must be built from shared linking modules instead of each board inventing its own timing and columns.

Required primitives:
- Slots = when
- Lanes = where / workflow column
- Bindings = who or what is assigned
- Boards = display surfaces built from bindings

Locked boards:
- Turnout Board
- Feed Board
- Stall Board
- Show-Day Board

Required board item links:
- tenant_event
- horse
- person/profile where relevant
- slot
- lane
- board_type
- status
- optional task
- optional schedule item

Output:
- Recommended data shape.
- Minimal test records.
- UI surfaces needed.
- Known pitfalls.
- No live writes unless explicitly approved.
```

---

## 23. Codex Prompt: SMS + Ring Waze Planning

```text
You are designing SMS Two-Ways and Ring Waze for RingStatus. Do not write production code yet.

Goal:
SMS Two-Ways provide request/response lookups. Ring Waze provides ring-specific human comments/check-ins.

SMS request types:
- ring
- horse
- rider
- waze

Ring lookup response must support:
- ring_now
- ring_next
- team_next_focus
- as_of

Horse lookup response must support:
- horse_now
- horse_next
- horse_next_focus
- as_of

Rider lookup:
- daily snapshot only

Ring Waze:
- rings list
- ring detail
- ring comments
- user check-in at ring
- SMS comment intake: ring + comment
- Web comment intake: ring + comment
- last 3 ring comments available for SMS reply

Important rules:
- SMS does not own schedule data.
- SMS reads schedule, horse, people, comments, and Ring Waze data.
- Ring Waze is not GPS navigation.
- Every SMS reply with live data needs an as_of timestamp.
- Do not use phone number as the only key.

Output:
- Route map.
- Data shape.
- Identity/permission concerns.
- Minimum viable test.
- No live writes unless approved.
```

---

## 24. Codex Prompt: Schedule Data + Freshness

```text
You are planning the RingStatus schedule data layer. Do not write production code yet.

Schedule shape:
- show_days
- show_day
- show_day_outlook
- show_day_print_list
- show_day_roster_active_riders
- show_day_roster_active_horses
- show_day_rings
- show_day_ring_status
- show_day_ring
- show_day_class_times with start_time
- show_day_groups optional
- show_day_classes groups + classes
- show_day_entries classes + entries
- show_day_trip_times with go_time
- show_day_trips entries + trips
- show_day_live
- show_day_results

Important:
- show_day is the date container.
- show_day_rings is the ring index/board.
- show_day_ring is one ring detail.
- groups/classes/entries/trips are layered schedule data.
- live/results are status/output layers.
- Every live/status response must carry an as_of timestamp.

Output:
- Data hierarchy.
- Proposed sources/read strategy.
- Freshness rules.
- UI/app surfaces.
- Risks and minimum viable test.
```

---

## 25. Human Overview Description

RingStatus is a tenant-aware event operations system for barns traveling to and managing shows.

The core problem is not simply “what classes are running.” The real problem is: which barn is attending which event, which horses and people are involved, what needs to be packed or shipped, what needs to happen on arrival, which stalls/turnout/feed/show-day items must be managed, what alerts and SMS information need to be available, and how people can share live ring context through comments and Ring Waze.

The system separates master records from event participation.

- Tenants are barns/clients.
- Horses and people are master data.
- Events/Event Ops represent show and travel operations.
- Tenant Events connect a barn to a specific event/week/day.
- Lists, boards, tasks, schedules, alerts, SMS, and comments are then scoped through that Tenant Event.

This prevents the common mistake of putting every show under every tenant, or putting horses directly under shows. Horses stay owned by tenants, but the Tenant Event determines which horses are actually attending a specific event.

---

## 26. Human Detail Description

A tenant may have many horses and people, but only some of them attend a given event. Because of this, Event Ops must live outside Tenants, and Tenant Events must act as the join layer.

For example:

```text
Tenant: Blue Ribbon Barn
Event: WEC Summer Series Week 2
Attending Horses: Navy, Scout, River
Attending People: Trainer, Groom, Rider
```

That Tenant Event becomes the operating container for:

```text
packing lists
horse kits
shipping lists
arrival tasks
stall board
turnout board
feed board
show-day board
schedule view
alerts
SMS lookups
Ring Waze comments
return-home lists
handoff tasks
```

The schedule system is layered from show day to ring to classes to entries to trips. Live status and results are outputs on top of that schedule data.

The board system is built from linking modules:

```text
Slots = when
Lanes = where/workflow
Bindings = who/what assigned
Boards = visual surfaces
```

This keeps boards flexible. The same horse/person/slot/lane binding can be displayed on a Turnout Board, Feed Board, Stall Board, or Show-Day Board depending on context.

Alerts are one-way subscription feeds. SMS Two-Ways are different: they are request/response lookup tools for ring, horse, rider, and Ring Waze information. Ring Waze is a ring-specific comment/check-in system, not GPS navigation.

---

## 27. Known Trouble Spots and Pitfalls

### 27.1 Naming drift

Risk:

```text
Shows, Events, Event Ops, Showgrounds, Series, Weeks, Days
```

can become mixed.

Guardrail:

```text
Use Event Ops as the working container.
Use Event Series / Event Weeks / Event Days for time hierarchy.
```

---

### 27.2 Tenant vs Event nesting

Risk:

Putting all events inside Tenants or all tenants inside Events can confuse participation.

Guardrail:

```text
Tenants and Events stay separate.
Tenant Events join them.
```

---

### 27.3 Horses under events

Risk:

Putting horses directly under events loses tenant ownership and reusable horse profile data.

Guardrail:

```text
Horses are master records.
Tenant Events link attending horses into event operations.
```

---

### 27.4 Profiles vs riders

Risk:

General people profiles and show-specific riders can collapse into one confusing table.

Guardrail:

```text
People Profiles = reusable person/user/role records.
Show Riders = show-specific participation records.
```

---

### 27.5 Subscribers vs subscriptions

Risk:

Subscribers, subscriptions, tenants, and profiles can get mixed.

Guardrail:

```text
Subscribers = outside opt-in identity.
Subscriptions = feed connections.
Tenants = barns/clients.
Profiles = people/roles.
```

---

### 27.6 Alert Threads vs Active Alerts

Risk:

The feed they subscribe to and the alerts currently displayed can be confused.

Guardrail:

```text
Alert Thread = subscribable feed.
Alert Thread Item = message in feed.
Active Alerts List = currently visible alert list.
Active Alert List Item = current alert row.
```

---

### 27.7 Boards becoming independent apps with incompatible data

Risk:

Each board invents its own time slots, columns, statuses, and assignments.

Guardrail:

```text
Slots, Lanes, and Bindings come first.
Boards display the bindings.
```

---

### 27.8 Lists vs places vs pins

Risk:

Places and pins can accidentally become checklist items.

Guardrail:

```text
Lists = operational containers.
Places/Pins = location primitives.
Routes = relationships between places/pins.
```

---

### 27.9 Ring Waze confusion

Risk:

Ring Waze may be mistaken for GPS navigation.

Guardrail:

```text
Ring Waze = ring-specific human comments/check-ins.
It may reference routes or places, but it is primarily a live ring-context feed.
```

---

### 27.10 SMS freshness

Risk:

SMS replies may send stale information.

Guardrail:

```text
Every ring/horse response must include as_of.
Do not claim live certainty without a fresh timestamp.
```

Known useful rule from prior RingStatus work:

```text
For ring-level replies using alternate ring payloads:
- use estimated_start_time as Starts
- use max curr_updated_at as inferred ring-level as_of
- derive Starts in from estimated_start_time - as_of
- reliable for ring schedule/status replies
- not reliable for live Ends in or rider/order-of-go unless detailed live/trip endpoint is available
```

---

### 27.11 Source and status shape drift

Risk:

External show data may use different shapes for rings, classes, entries, trips, class groups, live now, upcoming, completed, status codes, and timestamps.

Guardrail:

```text
Normalize source payloads into RingStatus-owned shapes before UI or SMS uses them.
```

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

---

### 27.12 Phone numbers as keys

Risk:

Phone numbers are mutable and may not be unique enough for a durable key.

Guardrail:

```text
Do not use phone number as the only primary key.
Use subscriber/person/device/alias relationships.
```

---

### 27.13 Live write safety

Risk:

Accidental Airtable PATCH/POST, Webflow publish, or production route modification.

Guardrail:

```text
Use read-only inspection first.
Require explicit approval before live Airtable writes or production changes.
```

---

### 27.14 Prompt/version drift

Risk:

Instructions from top-runner, bottom-runner, blog-runner, RingStatus, and Webflow work can get mixed.

Known prior pain points to avoid:

```text
shape drift: list-only vs list_only
mixing blog-runner/top-runner specs into bottom-runner
stale prompt/instruction versions in memory
inconsistent file naming
```

Guardrail:

```text
Each lane gets its own handoff/prompt.
Do not reuse a runner prompt outside its lane without checking scope.
```

---

### 27.15 Webflow lane confusion

Risk:

Native Designer edits, custom code embeds, GitHub/jsDelivr assets, Webflow Cloud/Astro routes, Airtable connectors, and worker endpoints can be mixed.

Guardrail:

```text
Identify the lane before touching files or routes.
Confirm what is live before recommending publish.
```

---

### 27.16 Overbuilding before proof

Risk:

Large schema/app plans can stall simple tests.

Guardrail:

```text
Start with the smallest test:
subscriber/person recognition → tenant event → one horse → one ring lookup → one Ring Waze comment → one visible board/list result.
```

---

## 28. Current Technology and Established Tools / Integrations

This section documents the current RingStatus integration lanes and established tools. It is based on the RingStatus routing map referenced at:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\ringstatus_runner_options_overview.md
```

The local Windows path must be treated as the authoritative project file when working inside the user’s machine/repo. In this ChatGPT session, the contents are represented through the user-provided operating contract and known RingStatus project context. Before any real implementation work, the file should be opened from the repo and used as the lane-routing source.

Core rule:

```text
Do not treat RingStatus technology as one generic Webflow/Airtable/SMS stack.
Every task must be routed into the correct lane before changing files, data, routes, embeds, or schemas.
```

---

### 28.1 Required RingStatus Preflight / Operating Contract

Before starting any RingStatus work, the worker must read and use:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\ringstatus_runner_options_overview.md
```

Purpose of that file:

```text
It is the routing map for choosing the correct RingStatus lane, tools, handoff docs, likely touched files, verification path, and approval boundaries.
```

Required preflight for every RingStatus task:

```text
1. Identify the correct lane from ringstatus_runner_options_overview.md.
2. Check which Codex skills, plugins, MCP tools, connectors, and local capabilities are installed or exposed in the current session.
3. Run git status --short.
4. State the exact files likely to be touched.
5. Confirm live Airtable schemas, env vars, Webflow page IDs, deployment routes, or loaded browser assets before relying on memory.
6. Use the lane-specific handoff docs listed in the overview.
7. Verify locally, in browser, or through live health/GET endpoints before recommending publish.
8. Require separate explicit approval before live Airtable PATCH/POST/write actions unless already approved in the current task.
```

Operating distinction:

```text
Webflow work is not one generic lane.
Separate native Designer/MCP edits, manual embeds, static CDN assets, Webflow Cloud/Astro API routes, and Airtable-backed two-way connectors.
```

---

### 28.2 Integration Lane Index

Current recognized RingStatus lanes:

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

Use this as the routing checkpoint before planning work.

---

### 28.3 Lane: Native Webflow Designer / MCP Edits

Use this lane when the work requires direct Webflow Designer-level changes.

Examples:

```text
- Create or modify Webflow sections
- Change native page elements
- Work with Webflow components
- Adjust native classes using approved naming conventions
- Use webflow-mcp:designer-tools
- Use webflow-mcp:flowkit-naming
```

Likely touched surfaces:

```text
Webflow page
Webflow component
Native class structure
Designer-managed element tree
```

Required checks:

```text
- Confirm the target page ID or component ID.
- Confirm whether the current session has Webflow MCP access.
- Confirm whether changes are meant for Designer-native elements or custom embeds.
- Do not assume manual embed structure can be edited through Designer tooling.
```

Pitfall:

```text
Do not mix native Designer edits with static JS/CDN asset work unless the task explicitly requires both.
```

---

### 28.4 Lane: Manual Webflow Embeds

Use this lane when the user needs a block of HTML/CSS/JS to paste into Webflow manually.

Examples:

```text
- Small proof-of-concept UI
- Thin form/embed for subscriber recognition
- Ring Waze comment form
- Static shell for schedule/list/board preview
- Webflow embed that calls an existing endpoint
```

Likely touched surfaces:

```text
Webflow Embed element
Inline HTML
Inline CSS
Inline script
External script reference
```

Required checks:

```text
- Confirm where the embed will be pasted.
- Confirm whether the embed should be self-contained or load external assets.
- Confirm existing loaded browser assets before duplicating scripts.
- Keep the embed small when testing recognition, SMS intake, or comment intake.
```

Pitfall:

```text
A manual embed should not silently become a full app architecture.
Use it for thin surfaces or tests unless the lane is intentionally escalated.
```

---

### 28.5 Lane: GitHub / jsDelivr Pinned Static Assets

Use this lane when RingStatus UI or browser logic is served as static JS/CSS from GitHub and loaded into Webflow through jsDelivr or another static CDN path.

Examples:

```text
- ringstatus board JS
- schedule app browser bundle
- Ring Waze browser module
- CSS shared across Webflow embeds
- stable pinned asset loaded by Webflow
```

Likely touched surfaces:

```text
Git repo file
Static JS file
Static CSS file
Webflow embed script tag
jsDelivr pinned URL
```

Required checks:

```text
- Run git status --short.
- Confirm repo, branch, and target file path.
- Confirm whether Webflow is loading a pinned version, latest version, or local dev version.
- Confirm the browser is loading the intended asset before debugging app behavior.
```

Pitfalls:

```text
- Editing the repo does not update Webflow if Webflow is pinned to an old CDN URL.
- Browser cache can make a fixed script appear broken after a correct change.
- Multiple embeds may load multiple versions of the same helper.
```

---

### 28.6 Lane: Webflow Cloud / Astro API Routes

Use this lane when the task involves server-side or route-based behavior connected to Webflow Cloud, Astro, or project API routes.

Examples:

```text
- API route for RingStatus data
- Endpoint that reads Airtable and returns normalized app data
- Route that handles form submissions
- Health endpoint for verifying deployment
- Server-side token-protected lookup
```

Likely touched surfaces:

```text
Astro route
Webflow Cloud route
Environment variables
Deployment route
Health / GET endpoint
```

Required checks:

```text
- Confirm deployment route.
- Confirm env vars exist and are named correctly.
- Confirm whether endpoint is live, local, preview, or production.
- Verify through a live health/GET endpoint before recommending publish.
```

Pitfall:

```text
Do not assume an Astro/Webflow Cloud endpoint is active just because a file exists locally.
Route deployment and env vars must be confirmed.
```

---

### 28.7 Lane: Two-Way Airtable Read/Write Connectors

Use this lane when RingStatus needs to read from or write to Airtable.

Examples:

```text
- Subscriber opt-in / opt-out
- Phone alias lookup
- Device recognition
- Tenant event links
- Board item status update
- Ring Waze comment creation
- Active alert subscription write
- Schedule enrichment writeback
```

Likely touched surfaces:

```text
Airtable base
Airtable table
Airtable schema
Airtable records
Worker/API connector
Token/env var
```

Required checks:

```text
- Confirm live table names and field names.
- Confirm schema before writing.
- Use test tables when testing.
- Require explicit approval before live PATCH/POST/write actions unless approval is already granted in the current task.
```

Pitfalls:

```text
- Memory of table names is not enough.
- Similar test/prod table names can cause accidental writes.
- A single wrong field name can silently break an Airtable automation or connector.
- Do not use phone number alone as the durable key for identity.
```

---

### 28.8 Lane: Static Dataset + Airtable Enrichment Flows

Use this lane when part of the RingStatus app should be stable/static and another part should be enriched or updated through Airtable.

Examples:

```text
- Static places/pins dataset enriched with Airtable notes
- Static schedule snapshot enriched with tenant-specific horse/rider focus
- Static location lists enriched with comments or route notes
- Static board templates populated by tenant-event bindings
```

Likely touched surfaces:

```text
Static JSON
GitHub-hosted dataset
Airtable enrichment table
Browser merge logic
Worker/API merge logic
```

Required checks:

```text
- Identify source of truth for each field.
- Do not overwrite static canonical fields with user-entered enrichment unless that is the intended design.
- Confirm whether enrichment is global, tenant-specific, event-specific, or user-specific.
```

Pitfall:

```text
Avoid shape drift between the static JSON and Airtable-enriched fields.
```

---

### 28.9 Lane: RS Template / Source Package Work

Use this lane when working on reusable RingStatus templates, skeletons, source packages, naming conventions, or portable app shells.

Examples:

```text
- RingStatus skeleton hierarchy
- Board/list/schedule source package
- Shared app layout templates
- Shared CSS naming system
- Documentation-driven source package
```

Likely touched surfaces:

```text
Docs
Template files
Source package folders
Shared CSS
Shared JS modules
Example JSON shapes
```

Required checks:

```text
- Confirm this is source/template work, not live Webflow editing.
- Keep templates generic enough to reuse across schedules, lists, boards, alerts, SMS, and Ring Waze.
- Separate conceptual documentation from implementation files.
```

Pitfall:

```text
Do not let template work accidentally imply a live Airtable schema or deployed app route already exists.
```

---

### 28.10 Lane: Schedule, Heartbeat, Trips, and Runner Workflows

Use this lane when working with live or semi-live show data, ring status, class times, entries, trips, results, heartbeat pages, or runner scripts.

Known source families from prior RingStatus work:

```text
horseshowing.com
rings.php heartbeat-style pages
get_ring_day_oc.php status payloads
get_orders.php behavior
showgroundslive-style APIs
wellingtoninternational / SGL-style endpoints
LiveScoreWidget-style payloads
LiveClassData-style payloads
ListAjax-style payloads
```

Known schedule layers:

```text
show-days
show-day
show-day-outlook
show-day-print-list
show-day-roster-active-riders
show-day-roster-active-horses
show-day-rings
show-day-ring-status
show-day-ring
show-day-class-times
show-day-groups
show-day-classes
show-day-entries
show-day-trip-times
show-day-trips
show-day-live
show-day-results
```

Known timing/status principles:

```text
- Ring status must carry an as_of timestamp.
- Start time and go time are different concepts.
- Ring-level status is not the same as trip-level live status.
- Heartbeat data may be useful for freshness but may be too thin for rider/order-of-go details.
```

Known payload risks:

```text
- Small status objects may repeat about every 10 seconds.
- Ring numbers and class IDs may appear as strings in one payload and numbers in another.
- Browser-populated objects may differ from static GET responses.
- Completed classes should not always be re-pulled on every run.
- External source status can be stale, partial, or inconsistent.
```

Required checks:

```text
- Confirm show_id, show_day, ring, customer_id, source endpoint, and expected payload shape.
- Normalize before storing or displaying.
- Preserve source timestamps and derived as_of timestamps.
- Verify with live GET/health/browser output before calling data current.
```

Pitfall:

```text
Do not treat external live show data as the app model.
It is source input that must be normalized into RingStatus shape.
```

---

### 28.11 Lane: Daily Schedule App UI Work

Use this lane when building or refining user-facing schedule surfaces.

Current schedule UI surfaces:

```text
- Show Day Outlook
- Show Day Print List
- Show Day Roster Active Riders
- Show Day Roster Active Horses
- Show Day Rings
- Show Day Ring Status
- Show Day Ring
- Show Day Live
- Show Day Results
- Schedule App
- Pro Schedule App
```

Relationship to data:

```text
Daily Schedule App UI reads normalized schedule data.
It should not scrape, normalize, or own raw live-source logic directly.
```

Required checks:

```text
- Confirm which schedule surface is being built.
- Confirm whether the UI is public, tenant-specific, manager-only, or pro/admin.
- Confirm whether it needs print output, board output, SMS lookup support, or alert-thread support.
```

Pitfall:

```text
Do not confuse schedule day boards with operational boards like turnout, feed, stalls, or show-day boards.
Schedule day boards are schedule-derived.
Turnout/feed/stall/show-day boards are tenant-event operational boards built from slots, lanes, and bindings.
```

---

### 28.12 Lane: Cloudflare SMS / Live Lookup Workers

Use this lane for Twilio webhook intake, SMS recognition, two-way SMS lookup, and live-data response handling.

Known SMS use cases:

```text
sms-two-ways
ring_now
ring_next
team_next_focus
horse_now
horse_next
horse_next_focus
rider daily snapshot
ring_waze last 3 ring comments
user sends sms: ring + comment
```

Likely flow:

```text
Twilio incoming SMS
→ Cloudflare Worker
→ normalize phone/message
→ recognize subscriber/person/device/alias
→ classify request type
→ read Airtable / normalized schedule / Ring Waze comments
→ return SMS response OR store comment/check-in
```

Known request families:

```text
ring
horse
rider
waze
comment/check-in
subscribe/unsubscribe
```

Required checks:

```text
- Confirm whether endpoint is receive-only or response-producing.
- Confirm Twilio webhook URL.
- Confirm worker route and environment variables.
- Confirm Airtable schema before read/write.
- Confirm data freshness before returning live schedule answers.
```

Pitfalls:

```text
- Do not allow stale Airtable snapshots to answer live ring questions unless clearly labeled as stale/as_of.
- Do not collapse subscriber, person, profile, and device into one identity record.
- Do not write Ring Waze comments without confirming the ring and show_day context.
```

---

### 28.13 Lane: Ring Waze / Comment Intake

Ring Waze is a ring-specific human update/comment/check-in system, not GPS navigation.

Known Ring Waze pieces:

```text
ring-waze
user-check-in-at-ring
ring-specific
user-sends-sms ring + comment
user-posts-comment ring + comment
ring-waze-rings-list
ring-waze-ring
ring-waze-ring-comments
```

Intake paths:

```text
SMS → ring + comment
Web UI → ring + comment
Check-in UI → user + ring + timestamp + optional comment
```

Shared output:

```text
All intake paths should land in the same ring-specific comments feed.
SMS Two-Ways can read the last 3 comments for ring-waze responses.
```

Required checks:

```text
- Confirm show_day.
- Confirm ring identity.
- Confirm recognized user/subscriber/person.
- Confirm whether comment should be public to tenant, event, or all subscribers to that ring feed.
```

Pitfall:

```text
Do not create separate comment stores for SMS comments and web comments unless there is a clear moderation reason.
They should converge into one ring comments model.
```

---

### 28.14 Lane: Equestrian Caption App Work

Use this lane when the task is about captioning, equestrian content generation, media support, or content workflows that are not directly schedule/list/board/SMS infrastructure.

Examples:

```text
- Caption app workflow
- Equestrian social/media captioning
- Show/event content blocks
- Human-facing descriptive content generation
```

Required checks:

```text
- Confirm whether the task is content/caption work or operational RingStatus app work.
- Do not mix caption workflows into schedule runner or SMS worker logic.
```

Pitfall:

```text
Caption/content tooling can reuse event, venue, horse, rider, and tenant context, but should not become the source of operational truth.
```

---

### 28.15 Current External / Service Integrations

Known service families and their RingStatus roles:

```text
Webflow
- Pages, embeds, Designer/MCP edits, app surfaces, manual UI placement.

Webflow Cloud / Astro
- API routes, server-side endpoints, health checks, env-var protected connectors.

GitHub
- Source files, docs, static JS/CSS, templates, source packages.

jsDelivr
- CDN delivery of pinned GitHub static assets into Webflow.

Airtable
- Operational data, test schemas, tenant/event links, subscriptions, boards, comments, enrichment.

Cloudflare Workers
- SMS webhook handling, live lookups, proxy/intake endpoints, Airtable/API mediation.

Twilio
- Incoming SMS, two-way SMS replies, subscribe/unsubscribe, Ring Waze SMS comment intake.

Codex
- Repo-aware implementation assistant, local file editing, git checks, lane-specific work.

Webflow MCP / Designer Tools
- Native Webflow page/element/component work when exposed.

Flowkit Naming / Custom Code Management
- Naming and custom-code lanes when exposed.

Live show-data sources
- Ring, class, trip, result, heartbeat, order, and status data inputs.
```

---

### 28.16 Current Integration Boundaries

Locked boundaries:

```text
Tenants do not own all events.
Tenant Events connect tenants to the events they attend.

Subscribers are outside tenants.
Subscriptions connect subscribers to alert threads or feeds.

Profiles/Horses are master records.
Tenant Events bind the correct horses/people into event operations.

Schedules are not boards.
Schedules can generate day boards and print lists.
Operational boards are built from slots, lanes, and bindings.

Ring Waze is not GPS.
Ring Waze is ring-specific human comments/check-ins.

SMS Two-Ways do not own schedule data.
They read normalized data and respond with current/as_of context.
```

---

### 28.17 Current Integration Pitfalls to Watch

Known trouble spots:

```text
1. Treating Webflow as one generic lane.
2. Confusing manual embeds with Designer/MCP edits.
3. Editing GitHub assets while Webflow still loads an old pinned jsDelivr URL.
4. Assuming an API route exists or is deployed because a local file exists.
5. Relying on remembered Airtable schemas instead of checking live tables/fields.
6. Writing to Airtable without explicit approval.
7. Using phone number as the only durable user identity.
8. Mixing subscribers, profiles, devices, and tenants into one table too early.
9. Treating external show payloads as final app data instead of source input.
10. Losing as_of timestamps on ring/horse/SMS answers.
11. Mixing schedule-derived day boards with tenant-event operational boards.
12. Creating separate comment systems for SMS and web Ring Waze comments.
13. Allowing stale schedule data to power live SMS answers without warning.
14. Overbuilding before the smallest recognition → event → horse → ring → comment test works.
15. Shape drift between docs, JSON, Airtable tables, browser state, and workers.
```

Immediate working rule:

```text
Route first. Confirm live surfaces second. Touch only the needed lane third. Verify before publish. Ask for approval before live writes.
```

---

## 29. Growth and Technology

### 29.1 Growth Direction

RingStatus can grow from a simple show-day utility into a tenant-aware barn operations platform.

Growth layers:

```text
1. Recognition + subscription
2. Tenant Event participation
3. Schedule lookup
4. Ring Waze comments
5. Lists and packing
6. Boards for turnout/feed/stalls/show-day
7. Alerts threads and subscriptions
8. SMS two-way info
9. Places/pins/routes
10. Home handoff systems
```

---

### 29.2 Minimum Viable Growth Path

Recommended first meaningful test:

```text
1. Recognize subscriber/person
2. Connect person to tenant event
3. Link one horse
4. Load one show day and one ring
5. Show ring now/next with as_of
6. Allow one Ring Waze comment
7. Return last 3 comments by SMS or simple UI
```

Why this test matters:

```text
It proves recognition, tenant-event linking, schedule freshness, comments, SMS/read flow, and UI display without building the entire platform.
```

---

### 29.3 Technology Growth

Near-term:

```text
Airtable test schema
Cloudflare receive/read endpoints
Twilio SMS intake
Simple Webflow/HTML app surfaces
GitHub/jsDelivr static JS
```

Mid-term:

```text
Stable normalized schedule cache
Tenant-event board builder
Alerts subscription engine
Role-based permissions
Reusable places/pins/routes
Printable lists
Pro schedule app
```

Long-term:

```text
Dedicated app shell
Offline-tolerant mobile use
Better identity/device model
Automated schedule ingestion
Advanced alert routing
Tenant analytics
Multi-show/multi-tenant operations dashboard
```

---

## 30. Current Best Next Step

Lock the conceptual skeleton before table creation.

Next concrete section to define:

```text
Tenant Events + Linking Modules
```

Reason:

```text
Tenant Events decide who/what is participating.
Slots, Lanes, and Bindings decide how boards, lists, tasks, and schedules connect.
Without these, the app surfaces will drift.
```
