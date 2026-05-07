# SGL Endpoint Contract

This document locks the currently verified SGL API endpoint patterns used by the RingStatus fetch pipeline.

Pipeline operating scope is versioned separately in [`ringstatus_pipeline_scope_2026-05-07.md`](./ringstatus_pipeline_scope_2026-05-07.md). Treat that document as the current evolving pipeline scope before adding or changing endpoint behavior.

Do not treat `status_code: 200` alone as a successful fetch. Each endpoint must be validated by expected payload shape and known soft-failure patterns.

## Base Rules

- Use `sglapi.wellingtoninternational.com` API URLs for data fetches.
- Do not scrape or depend on `www.wellingtoninternational.com/showgrounds/...` page routes.
- Ignore browser noise such as Google Analytics, ads, Cookiebot, CSS, and page assets.
- Do not commit live cookies, JWTs, bearer tokens, or browser session tokens.
- Prefer unauthenticated fetches where the endpoint works without browser/session context.
- Log the exact URL, endpoint family, status code, body length, auth/session flags, and validation result for every fetch.
- Treat short/error payloads as soft failures even when HTTP status is `200`.

---

## Endpoint Map

### 1. Day Schedule

**Purpose:** Base day schedule, rings, classes, class groups, estimated starts, estimated ends.

**Pattern:**

```text
https://sglapi.wellingtoninternational.com/schedule?date={YYYY-MM-DD}&show_id={SHOW_ID}&customer_id=15
```

**Example:**

```text
https://sglapi.wellingtoninternational.com/schedule?date=2026-05-03&show_id=200000060&customer_id=15
```

**Auth/session:** Not required.

**Observed valid result:**

```text
status_code: 200
body_length: 58758
session_json_used: false
cookie_header_used: false
authorization_used: false
```

**Expected payload shape:**

```text
show.show_id
show.show_name
show_date
show_days_list[]
rings[]
rings[].ring_name
rings[].ring_number
rings[].ring_id
rings[].classes[]
rings[].classes[].class_id
rings[].classes[].class_group_id
rings[].classes[].class_group_sequence
rings[].classes[].class_number
rings[].classes[].class_name
rings[].classes[].estimated_start_time
rings[].classes[].estimated_end_time
rings[].classes[].total_trips
```

**Validation rule:**

A schedule fetch is valid only when:

```text
status_code == 200
body_length > 1000
body != {}
show.show_id == requested show_id
show_date == requested date
rings is array
at least one ring has classes[]
```

---

### 2. Day Class Signup / Order Readiness Feed

**Purpose:** Day-level ring/class group feed with upcoming classes, signup windows, order-of-go readiness flags, class groups, and class IDs.

**Pattern:**

```text
https://sglapi.wellingtoninternational.com/classsignup?show_date={YYYY-MM-DD}&show_id={SHOW_ID}&customer_id=15
```

**Example:**

```text
https://sglapi.wellingtoninternational.com/classsignup?show_date=2026-05-03&show_id=200000060&customer_id=15
```

**Auth/session:** Not required when `team_id` is omitted.

**Important correction:**

Do not include `team_id` unless deliberately testing a team-scoped browser path.

This failed/soft-error pattern was observed when `team_id=1543` was included:

```json
{
  "error": "You are not authorized to edit this entry."
}
```

The same endpoint without `team_id` returned the full public payload.

**Observed valid result without `team_id`:**

```text
status_code: 200
body_length: 49704
session_json_used: false
cookie_header_used: false
authorization_used: false
```

**Expected payload shape:**

```text
show.show_id
show.show_name
show_days_list[]
rings[]
rings[].ring_name
rings[].ring_number
rings[].ring_id
rings[].upcoming_classes[]
rings[].upcoming_classes[].class_group_id
rings[].upcoming_classes[].estimated_start_time
rings[].upcoming_classes[].estimated_end_time
rings[].upcoming_classes[].start_classsignup_date
rings[].upcoming_classes[].stop_classsignup_date
rings[].upcoming_classes[].group_name
rings[].upcoming_classes[].total_trips
rings[].upcoming_classes[].all_orders_set
rings[].upcoming_classes[].under_saddle_class
rings[].upcoming_classes[].schedule_break
rings[].upcoming_classes[].classes[]
rings[].upcoming_classes[].classes[].class_id
rings[].upcoming_classes[].classes[].class_number
rings[].upcoming_classes[].classes[].name
rings[].upcoming_classes[].classes[].class_group_id
rings[].upcoming_classes[].classes[].order_of_go_set
```

**Validation rule:**

A classsignup fetch is valid only when:

```text
status_code == 200
body_length > 1000
body does not contain error
show.show_id == requested show_id
rings is array
rings[].upcoming_classes[] exists
```

**Known soft failure:**

```text
status_code == 200
body_length around 54
payload.error == "You are not authorized to edit this entry."
```

---

### 3. Ring Detail / Ring Status

**Purpose:** Ring-specific status/detail payload.

**Pattern:**

```text
https://sglapi.wellingtoninternational.com/ring/{RING_ID_OR_NUMBER}?show_date={YYYY-MM-DD}&show_id={SHOW_ID}&customer_id=15
```

**Example:**

```text
https://sglapi.wellingtoninternational.com/ring/3?show_date=2026-05-02&show_id=200000060&customer_id=15
```

**Auth/session:** Not proven as required. Browser-style request worked.

**Observed valid result:**

```text
status_code: 200
raw_content_length: 532
```

**Expected payload shape:**

```text
show.show_id
show.show_name
show_id
ring
```

**Validation rule:**

A ring fetch is valid only when:

```text
status_code == 200
body_length > 100
body != {}
show.show_id == requested show_id
ring exists
```

**Note:**

This is a small payload. Do not apply the same large-body threshold used for `/schedule`.

---

### 4. Class Detail / Class Order

**Purpose:** Class-specific detail, trips, class status, group order of go, ring info, timing, judges, prizes, and table info.

**Pattern:**

```text
https://sglapi.wellingtoninternational.com/classes/{CLASS_ID}?show_id={SHOW_ID}&customer_id=15&cgid={CLASS_GROUP_ID}
```

**Example:**

```text
https://sglapi.wellingtoninternational.com/classes/200024766?show_id=200000060&customer_id=15&cgid=200023612
```

**Auth/session:** Not required.

**Observed valid result:**

```text
status_code: 200
body_length: 10843
session_json_used: false
cookie_header_used: false
authorization_used: false
```

**Expected payload shape:**

```text
class.class_id
class.number
class.name
class.show_id
class.schedule_ring_id
class_related_data.status
class_related_data.class_group_id
class_related_data.ring
class_related_data.ring_name
class_related_data.total_trips
class_related_data.completed_trips
class_related_data.remaining_trips
jumper_table_info
total_entry_trips
trips[]
trips[].entry_id
trips[].number
trips[].horse
trips[].rider_name
trips[].order_of_go
class_group_order_of_go.entries[]
class_group_order_of_go.entries[].class_id
class_group_order_of_go.entries[].entry_id
class_group_order_of_go.entries[].order_of_go
```

**Important distinction:**

```text
trips[] = entries for the requested class_id only
class_group_order_of_go.entries[] = combined order for the whole class_group_id
```

**Validation rule:**

A class detail fetch is valid only when:

```text
status_code == 200
body_length > 1000
class.class_id == requested class_id
class_related_data.class_group_id == requested cgid
trips is array
class_group_order_of_go.entries is array
```

---

### 5. Entry Detail

**Purpose:** Entry-level detail: horse, owner, trainer, rider list, show, classes, scheduled class rows, and entry riders.

**Pattern:**

```text
https://sglapi.wellingtoninternational.com/entries/{ENTRY_ID}?eid={ENTRY_ID}&show_id={SHOW_ID}&customer_id=15
```

Optional team-scoped variant:

```text
https://sglapi.wellingtoninternational.com/entries/{ENTRY_ID}?eid={ENTRY_ID}&show_id={SHOW_ID}&customer_id=15&team_id={TEAM_ID}
```

**Example tested:**

```text
https://sglapi.wellingtoninternational.com/entries/200230238?eid=200230238&show_id=200000060&customer_id=15&team_id=1543
```

**Auth/session:** Not required for tested detail response.

**Expected payload shape:**

```text
entry.entry_id
entry.number
entry.horse_id
entry.horse
entry.trainer_id
entry.trainer
entry.entryowner_id
entry.owner
entry.rider_list
entry.show_id
classes[]
classes[].class_id
classes[].class_number
classes[].name
classes[].rider_id
classes[].rider_name
classes[].placing
classes[].ring
classes[].scheduled_date
classes[].schedule_starttime
classes[].entryxclasses_uuid
classes[].scratch_trip
classes[].ring_id
classes[].ring_name
entry_show
company_name
entry_riders[]
entry_riders[].entry_id
entry_riders[].rider_id
entry_riders[].rider_name
```

**Validation rule:**

An entry detail fetch is valid only when:

```text
status_code == 200
body != {}
entry.entry_id == requested entry_id
entry.show_id == requested show_id
classes is array
entry_riders is array
```

---

### 6. People Detail / Person Activity

**Purpose:** Person-linked show activity across entries/classes/trips.

**Pattern:**

```text
https://sglapi.wellingtoninternational.com/people/{PEOPLE_ID}?pid={PEOPLE_ID}&show_id={SHOW_ID}&customer_id=15
```

**Example:**

```text
https://sglapi.wellingtoninternational.com/people/8778?pid=8778&show_id=200000060&customer_id=15
```

**Auth/session:** Not required for tested response.

**Observed valid identity:**

```text
people.people_id: 8778
people.name: ALAN KOROTKIN
total_trips: 135
```

**Expected payload shape:**

```text
people.people_id
people.name
people.city
people.state
people.country
total_trips
trips[]
trips[].entry_id
trips[].entry_number
trips[].class_id
trips[].class_number
trips[].class_name
trips[].horse_id
trips[].horse
trips[].rider_id
trips[].rider_name
trips[].placing
trips[].entryxclasses_uuid
trips[].trips_count
```

**Validation rule:**

A people fetch is valid only when:

```text
status_code == 200
body != {}
people.people_id == requested pid
trips is array
```

---

## Fetch Pipeline Order

Recommended fetch order:

```text
1. /schedule
2. /classsignup
3. Compare schedule rings/classes against classsignup upcoming_classes
4. Fetch /classes/{class_id} only for active, target, or needed classes
5. Fetch /entries/{entry_id} only for entry detail enrichment
6. Fetch /people/{pid} only for person-based enrichment or lookup
7. Fetch /ring/{ring_id} only for ring-specific status checks
```

---

## Soft Failure Rules

A fetch should be marked as soft-failed when any of these occur:

```text
status_code == 200 but body == {}
status_code == 200 but body is an error object
status_code == 200 but body_length is far below expected for that endpoint family
status_code == 200 but expected shape keys are missing
requested show_id/date/class_id/entry_id/pid do not match returned payload
```

Known soft failure:

```json
{
  "error": "You are not authorized to edit this entry."
}
```

This was observed for:

```text
/classsignup?show_date=2026-05-03&show_id=200000060&customer_id=15&team_id=1543
```

Correct public form:

```text
/classsignup?show_date=2026-05-03&show_id=200000060&customer_id=15
```

---

## Logging Contract

Every fetch lane should log:

```text
lane
endpoint_family
url
show_id
show_date/date
class_id if present
class_group_id if present
entry_id if present
people_id if present
status_code
body_length
raw_content_length
session_json_used
cookie_header_used
authorization_used
validation_passed
validation_error
soft_failure_reason
```

---

## Do Not Use As Data Endpoints

Do not use these browser/page routes as RingStatus data sources:

```text
https://www.wellingtoninternational.com/showgrounds/ring/detail?ring_id=...
https://www.wellingtoninternational.com/showgrounds/ring-status/...
```

These are useful only as browser clues for discovering underlying `sglapi` calls.

Ignore:

```text
google-analytics.com
analytics.google.com
googlesyndication.com
consent.cookiebot.com
wp-content CSS/JS assets
```
