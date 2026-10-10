# RingStatus — barns, show data, inputs and alerts

Consolidated October 9, 2026. **Start here for this design discussion.**

This is the complete record of the behavior discussed so far. It consolidates the working architecture map and opt-in review, including the subsequent weekly participation, search, red-line and audit-lane discussion. It does not declare the unfinished features built or tested.

**Status terminology:** Locked = explicitly locked by the owner. Agreed = accepted in the subsequent discussion. Existing = present in inspected source or recorded implementation. Proven = supported only to the extent stated in the evidence section. Open = no decision has been made.

## 1. Overall structure

Owner's application headings: **Webflow static | Astro1 | Astro2**. Native Webflow owns presentation; Astro maps data and actions through endpoints. The separate responsibilities of Astro1 and Astro2 remain open.

The business areas are Recognize, tenants/barns, opt-ins, horses, riders, locations, users, roles, rings and shows. These names do not automatically require separate applications, bases, tables or agents.

Recognize identifies the person/browser. Barn membership, participation, personal follows and alert choices are distinct concepts. Recognize is not the barn profile or onboarding form.

Horses are shared entities used by packing, tack, WEF, WEC and other services. Reuse the entity and its relationships rather than recreating a horse record inside every service. How source-specific horse and rider IDs map to shared identities remains open.

## 2. Barn entity and roles — locked

**The barn is the tenant.** Castlewood is the example entity. Its horses, riders, users, staff, grooms, locations, roles and opt-ins are associated with that entity.

Barn association does not assert legal horse ownership. Castlewood may manage or care for a horse owned by someone else; actual ownership is separate.

| Role or relationship | Agreed behavior |
| --- | --- |
| Entity owner | Manages the entity, its trainer associations and enabled horses/riders; manages groom assignments and alert preferences. |
| Main trainer | May be designated by the entity owner. The main trainer/entity owner's show entries supply their horses and riders. |
| Enabled rider | Has entity access even when not competing this week or today; may maintain personal favorites. |
| Staff | May also hold a rider role. Other staff permissions are not yet specified. |
| Groom | Does not hold a rider role; sees only specifically assigned items. Barn-wide inclusion never overrides that restriction. |
| Unaffiliated trainer being followed | Supplies show horses only. Their riders do not appear, and following does not make the trainer a barn member. |

Appearing in a show's rider data does not automatically grant user access. An enabled entity membership grants access; the current show entry list does not decide it.

## 3. Trainer grouping and two follow lanes — locked

Show entries are grouped by **trainer**, not by barn name. Castlewood's three trainers connect the entity to trainer-grouped entries.

### Barn lane

- The main trainer/entity owner's horses and riders appear from their show entries.
- The owner can disable individual horses or riders. Disable retains the underlying record and allows reactivation.
- The owner may add an unaffiliated trainer's show horses and unfollow individual horses from that list.
- **No riders from that unaffiliated trainer are included.**
- Barn members see the resulting barn information subject to their roles; grooms remain assignment-only.

### Personal follow lane

- Enabled riders and trainers may maintain additional personal favorites.
- Example: a Castlewood rider sees Castlewood's relevant horses plus five additional followed horses.
- Following another horse does not grant access to another barn's private information.

“Disable” describes the owner-managed main entity list. “Unfollow” describes exclusions from follows. Their exact storage representation is not selected. Precedence when the same horse arrives through multiple trainers, a personal favorite and an exclusion is still open.

## 4. Season, show week and today — agreed

Each WEF show covers one week. Its entries contain only the horses and riders entered for that week. Daily participation is a further subset.

| Scope | Example horses | Example riders | Meaning |
| --- | ---: | ---: | --- |
| Whole show week | 2,400 | 1,500 | All entries available for the selected show week. |
| Castlewood season membership | Not specified | 31 | The entity's allowed rider list, independent of current participation. |
| Castlewood this week | 33 | 20 | Castlewood participants entered this week. |
| Castlewood today | 13 | 10 | Castlewood participants competing today. |

Thus, 11 of Castlewood's 31 riders are not competing this week and 21 are not competing today. An enabled rider among those absent still has entity access and can follow horses.

These are illustrative counts, not a live data inventory. Do not turn a show-week filter into an access revocation or remove an entity association when a participant is absent.

## 5. Search, favorites and remembered associations — agreed

### Search on a phone

Maintain one searchable show-week index shared across users. A phone submits a search by horse name, show number or trainer to an endpoint and receives a small batch of matching results. Show the name, number and trainer needed to select a horse. Do not download or render all 2,400 horses just to choose five favorites.

The exact search/index technology, batch size and matching rules remain open. This is a behavior requirement, not an instruction to build a separate search service.

### Narrow the search using entity history

The entity accumulates followed horses and horses recently seen showing with it. Previously associated horses that are entered in the selected week appear as suggestions before the wider show search.

Example: Blue, Green and Orange show with Castlewood this week; Blue and Orange return next week. Green remains known to Castlewood, but is not presented as entered next week without entry evidence.

Suggestions support selection; they are not proof of a current entry, ownership, membership or permission. Historical associations persist across show weeks, while the active show-week index reflects that week's entries. No automatic re-follow rule or retention duration has been selected.

## 6. Red-line and audit-lane — agreed

Both are **additional forms within the existing barn-inputs**, not separate applications. Rider inputs must also be supported.

### Red-line: a missing horse or rider

1. A user cannot find Purple even though they expect it to be showing today.
2. “Missing a horse?” accepts the name and any known number, trainer or class. The rider equivalent handles missing riders.
3. The submission reaches the admin as pending review. It is not silently treated as a verified new show entry.
4. The admin investigates. If Purple is already listed as Mister P, resolve the submission to that existing horse and retain Purple as an alternate name for later searches.
5. Apply the same path to a rider name mismatch when appropriate.

Do not create a duplicate horse merely because a supplied name differs from the source name. Exact alias ownership and matching rules remain open.

### Audit-lane: reconcile an entire expected list

A staff member submits a list of expected horses and classes for today. Compare that list with the full displayed daily schedule and identify missing or mismatched horse/class entries for admin review.

Example: Purple is displayed at 2:00 in ring 2, but its expected 4:00 class in ring 5 is missing. The fact that Purple already appears somewhere does not satisfy the second entry.

Another example: a submission has 14 horses and 15 classes, while the system shows 14 horses and 14 classes. Compare the actual horse/class associations; totals alone cannot identify the discrepancy or prove correctness.

This is the owner's intended **audit-trace/audit-lane**: a submitted list reconciliation. It is not merely a generic log of who changed a record. Submission format, comparison keys and review statuses still need definition.

## 7. Complete input inventory

“Existing” below means an existing form/implementation; it does not mean every action is live-qualified.

| Input area | Existing inputs | Agreed additions or changes |
| --- | --- | --- |
| Barn | Select/add/edit barn; barn name | Main and affiliated trainer associations; owner-managed entity scope. |
| Users | User name; optional email; recognized-profile reuse control | Role and entity membership management, including staff/rider overlap. |
| Riders | Rider name; optional user-account link | Enabled/disabled membership; personal favorites for enabled riders; missing-rider reporting. |
| Horses | Horse name; rider link; location link | Shared horse selection, barn association versus actual ownership, enable/disable, follows and exclusions. |
| Locations | Location name; optional address/description | No additional location fields decided here. |
| Grooms / staff | Existing user records are available; specialized role forms are not established by this review | Groom-specific assignments; owner-managed groom preferences. |
| Trainer follows | No current follow selector established by this review | Add unaffiliated trainers' horses; exclude individual horses; exclude their riders entirely. |
| Personal favorites | No current favorites selector established by this review | Weekly horse search, entity suggestions and individual selections. |
| SMS opt-ins | SMS on/off, phone, alert toggles and timing choices | Apply preferences to the relevant barn/follows/assignments; owner manages groom preferences. |
| Red-line | Not established as built | Missing horse/rider form and admin resolution. |
| Audit-lane | Not established as built | Full expected horse/class list submission and reconciliation. |

Reuse the existing inline add/edit/save forms where applicable. Do not replace working forms just because their required coverage is expanding.

## 8. Opt-in form: current behavior and agreed direction

Existing page: [SMS opt-ins](https://ringstatus.com/rs-barn-onboarding-alerts).

The inspected form stores **one personal preference set**: SMS enabled, destination phone, saved time zone and 18 alert selections. It does not currently choose a barn, horse, rider, groom, assignment or show. The time zone is initialized from the browser and displayed; the inspected client has no time-zone editing control.

Agreed direction:

- Personal SMS consent and default preferences belong to the person.
- Alert applicability comes from that person's barn scope, follows and assignments.
- The entity owner manages groom alert preferences and assignments; the groom does not manage those settings.
- Recipient SMS consent remains separate from owner-managed preferences.
- Groom alerts must stay within the groom's assignments.

No exact new screen placement, scope selector, default/override precedence or revised timing choices have been locked. Saving preferences is not proof that the engines deliver those alerts.

### Eight alerts with option fields

| Alert | Existing key | Current choices |
| --- | --- | --- |
| Class reminder 1 | class_starts_in1 | 30, 45, 60, 75, 90 minutes before expected start |
| Class reminder 2 | class_starts_in2 | 15, 30, 45, 60 minutes before expected start |
| Ride reminder 1 | trip_starts_in1 | 30, 45, 60, 75, 90 minutes before expected trip |
| Ride reminder 2 | trip_starts_in2 | 15, 30, 45, 60 minutes before expected trip |
| Rides to go | rider_oog10 | 7, 10 or 15 entries remaining; default value 10 |
| Groom task reminder 1 | groom_tasks_at1 | Clock time, HH:mm |
| Groom task reminder 2 | groom_tasks_at2 | Clock time, HH:mm |
| Groom task reminder 3 | groom_tasks_at3 | Clock time, HH:mm |

All eight also have on/off selections. Current minute/count validation accepts the preset choices only; the separate numeric range check does not enable arbitrary minute values. Groom task reminders currently use fixed clock times, not calculated offsets from a horse's ride.

### Ten on/off-only alerts

Class delayed; class moved earlier; class started; class in progress; class finished; rider order; rider started; first score; first time; rider results.

## 9. Admin show/ring data and WEF engines

The owner/admin manages rings and weekly WEF shows. Rings are exposed by show. Detailed admin entry and ring exposure behavior remain open.

The WEF show-engine prepares static groups, classes, entries and rings by schedule. Live feeds take precedence over that prepared static data. Freshness thresholds and fallback rules are not yet specified.

| Engine / function | Stated inputs | Stated output |
| --- | --- | --- |
| Show-engine | WEF classes-data, entries-data and live-data | Prepared groups/classes/entries/rings and live updates. |
| rings-engine | Live data | Ring-related output. |
| Class-time and go-times | Live data | Their own timing output; whether separate engines is open. |
| alerts-engine | Live data and rings | Its own alert output. |
| results-engine | Live data, rings and alerts | Triggers when necessary to produce results output. |
| next-engine | Not yet specified | Preparation for tomorrow. |
| publish-engine | Not yet specified | Schedule output through endpoints; detailed processing is open. |

These are processing responsibilities and data dependencies, not agent assignments. WEC remains a separate engine/system; do not assume WEF logic applies to it.

## 10. Device behavior — previously locked, implementation pending

The [shared data behavior contract](shared-data-behavior-contract.md) remains controlling for this aspect:

- Display cached relevant data promptly and refresh through endpoints; identify its last successful update.
- Keep show/week/day context correct. Do not present obsolete data as today's schedule.
- Reflect edits immediately; persist dirty/pending edits on-device and preserve them through reloads, refreshes and conflicts.
- Mark Saved only when the corresponding edit is confirmed by the server; preserve newer edits still pending.
- Keep profile copies longer than show caches, with background refresh; keep authoritative saved records in Airtable.
- Isolate each person's data and pending edits. Do not discard unsaved edits when changing show context.
- Recognition-cookie retention is separate, with the agreed 365-day target subject to browser limits.
- IndexedDB cache/pending-edit behavior and fully offline page reopening require their own proof. Closed-browser background execution is not guaranteed.

This plan is not a claim that the existing forms already implement persistent optimistic behavior. Explicit SMS consent remains an explicit action, not an automatically synchronized draft consent.

## 11. Current implementation and evidence

| Area | Evidence and exact boundary |
| --- | --- |
| Barn and rider forms | Recorded live native-UI add/edit/save/reload plus Airtable/event readback. This was a basic barn/rider proof, not all entity types or full onboarding release. |
| Users, horses and locations | Existing native form bindings were inspected. This discussion does not establish fresh live qualification of all those forms. |
| SMS preferences | Recorded live save/reload, parameter retention and opt-out with Airtable/event readback. No SMS engine delivery activation was proven by that test. |
| Recognize | Retains its separately documented partial status; this design document does not close it. |
| active_iams | Six-field slim register was retained in the cleanup; recorded as empty and not runtime-wired at that checkpoint. |
| Recognize schema cleanup | 33 unused empty fields removed; seven reviewed tables reduced from 121 to 88 fields; records and canonical relationships retained. |
| New follow/search/red-line/audit and owner-managed groom behavior | Design requirements recorded here; no completed implementation or test is claimed. |
| CSS audit and Schedule | Remain open in the prior task record. |

Evidence dates above are October 9 records/source inspections, not fresh live retests performed while consolidating this document. Recognize currently also holds profile/input data; a separate profiles-base migration is not performed or authorized by this consolidation.

## 12. Remaining decisions before the corresponding implementation

1. Exact Webflow/Astro1/Astro2 responsibilities and storage placement.
2. Stable shared horse/rider identity, WEF/WEC source IDs and admin alias matching.
3. Membership enable/disable behavior versus show-follow visibility where the same rider has both; precise controls must distinguish them.
4. Follow/exclusion precedence across overlapping trainers and personal favorites, and carryover behavior between weeks.
5. Detailed staff permissions, owner-managed groom assignment controls and whether grooms need any favorites capability.
6. Search/index method, result batch size and suggestion history rules.
7. Red-line/audit submission format, comparison keys, admin resolution states and how users see the outcome.
8. Opt-in location, scope controls, default/override behavior, revised option choices and groom task timing semantics.
9. Live-data freshness/fallback, timing calculations, next-day preparation and publishing behavior.
10. Device save/refresh/retry intervals and the first offline/pending-edit proof.

These are unresolved specifics, not permission to invent replacements for existing working methods or to add all features to the current implementation at once.

## 13. Acceptance examples for future scoped work

Use these as behavior checks when the relevant feature is assigned; none is marked passed merely by appearing here.

| Example | Required result |
| --- | --- |
| Enabled Castlewood rider absent this week/today | Still has permitted entity access; absence is not disablement. |
| Main trainer versus unaffiliated trainer | Main list contains horses and riders; unaffiliated list contains horses only. |
| Rider with five personal favorites | Sees permitted Castlewood data plus those follows, without other barns' private access. |
| Groom | Sees only assigned items; preferences are managed by the owner; recipient consent remains separate. |
| Search among 2,400 horses | Small endpoint result batches; no mandatory full horse-directory download. |
| Green absent next week | Remains a known association, not falsely represented as entered. |
| Purple is Mister P | Admin resolves to the existing horse and preserves the alternate name without duplicate creation. |
| Submitted 14 horses / 15 classes versus system 14 / 14 | Identify the missing/mismatched horse-class entry, not just the count difference. |
| Dirty edit while offline | Edit survives reload and refresh, then receives one confirmed save or a visible unresolved conflict. |

## 14. Evidence and related documents

- [Earlier architecture notes](data-and-engines-working-map-20261009.md) — earlier locked discussions, consolidated here.
- [Opt-in option source review](optin-options-review-20261009.md) — exact current keys/options and inspected source path.
- [Basic Inputs live proof](inputs-basic-proof-20261009.md).
- [SMS opt-in connection and live proof](sms-optin-connection-20261009.md).
- [Shared device data behavior contract](shared-data-behavior-contract.md) — previously locked detailed behavior.
- [Recognize partial checkpoint](recognize-partial-checkpoint-20261009.md).
- [Temporary completion/storage sequence](temporary-completion-sequence-20261009.md).
- [Recognize fields and relationships audit](recognize-fields-links-audit-20261009.md).
- [Field cleanup and recovery evidence](recognize-fields-cleanup-20261009.md).

Inspected native Barn form bindings: `C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test/src/assets/rs-barn-onboarding/barn-native-adapter.mjs` and `barn-native-view.mjs` in the same directory. Existing SMS client: `C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test/src/assets/rs-input-subscriptions/native-client.js`.

**Change boundary for this consolidation:** documentation only. No application code, Webflow page, Airtable schema/data, hooks, automation or deployment changed.
