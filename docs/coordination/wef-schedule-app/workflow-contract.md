# WEF schedule app workflow contract

Feed refresh contract. The runner cadence and activation are pending.

The runner owns the [schedule-feed](https://airtable.com/appqWC58FLtmHcUAW/tblzsgMl5UNsPMk5W) table. It reads only the shared data inside `wef-schedule-app`. Astro will read the saved feed on its own cadence. The browser currently checks Day 0 every 3 minutes and the full show every 15 minutes.

| Lane | Proposed runner cadence | Scope |
| --- | --- | --- |
| Day 0 | Every 3 minutes | Send only additions, changes, and removals for the current priority-zero date. Build the comparison using Data plus verified shared Live values for that date. |
| Full show | Every 15 minutes | Upsert the latest validated Data package by `show-day-key` for all available dates, including Day 0. Serve those saved packages as the full schedule. |

Astro serves both feeds from the same feed table. Keep the Data package separate from the Day 0 diff. The Day 0 diff names the show, date, and exact Data version it was compared against. The browser applies it to that Data version, leaving other dates alone. If the versions do not match, fetch the full feed before applying the diff. When Day 0 changes, discard the previous date's diff.

Each Day 0 diff is relative to the matching full-feed Data copy, so a browser can miss a check and still catch up with the latest diff. If the effective Day 0 view returns to the Data copy, publish an empty diff to clear any prior Live changes. If nothing changed since the last published diff, leave the saved diff untouched.

Give Day 0 priority when both lanes are due. Use one writer so the two lanes cannot overwrite each other. Two Astro responses do not require two Airtable tables.

## What the schedule needs

The November 11 and 12 packages are shell test data. They validate package shape and links, not coverage of every value the screens display. This audit uses only data shared into `wef-schedule-app`.

| Screen | Confirmed from shared data | Still needs a verified value |
| --- | --- | --- |
| Schedule and Today | Show date, ring label, scheduled start, row order, class name and number, sequence, type, scheduled class status, and entry count | Which class is actually running or next, minutes to start or finish, ring early/late minutes, and walking time |
| Class details | Class and group identity, linked entries and trips, and horse, rider, and trainer labels | Current trip, live class progress, and a valid order of go |
| Entry details | Horse, back number, rider, trainer, and matched trip identity | Running order, go time, countdown, first-round time and score, official place, and groom assignments |

November 11 contained 14 schooling/reset rows. November 12 contained 8 classes marked Not Started. Its shared trip rows had no valid order of go or nonzero estimated go time. The shared Live tables and the scoped `live-observation-json` fields were empty. Result links were present, but the class results were not verified. Missing values remain unavailable. A scheduled start is not a live estimate, a group status is not a class status, and a result link is not an official result.

## Field rules to verify

All table names below refer to tables inside `wef-schedule-app`. Apply the Live column only to Day 0. Use applicable, verified Live first for agreed overlapping values while the show is active. Use Data as fallback and keep Data authority for its own schedule facts. The rows below identify sources for verification. They do not establish that an available field has the meaning the screen needs.

| Schedule field | Base field | When Live is populated |
| --- | --- | --- |
| Show and date | `w-shows.is_default`, `show-key`, `show_name`; `w-days.show`, `date`, `day-priority` | Keep the shared show and day. Live cannot change Day 0. |
| Rows, order, and scheduled time | `wef-data.schedules.schedule-key`, `ring`, `group`, `class`, `kind`, `source-position`, `name`, `start-time`, `record-status` | Keep Data row membership, order, and scheduled start. |
| Ring and group names | `wef-data.schedules.source-json.group_name`; scoped `w-rings.ring-nickname` or the saved source ring name | Keep Data labels. Apply a master nickname only for a verified ring match. |
| Class and status | `wef-data.classes.class-number`, `class-name`, `status`; `wef-data.schedules.source-json.cancelled`, `class_type`, `schedule_sequencetype` | Keep class status and cancellation from Data. Show `wef-live.groups.status` and `is-live` only as group state. |
| Entry, trip, and names | `wef-data.trips.trip-key`, `trip-id`, `entry`, `horse`, `rider`, `trainer`, `record-status`, `source-json.number`; matching `w-horses`, `w-riders`, `w-trainers` nickname, fallback, and name fields | Keep Data membership and identity. Live must match an existing trip before it can add trip details. |
| Planned order, actual order, scratch | `wef-data.trips.order-of-go`, `actual-order`, `scratch` | Use `wef-live.entries.order-of-go`, `actual-order`, or `scratch` individually when present and verified for the same trip. A blank or ambiguous scratch value cannot clear a saved scratch. Actual order alone does not prove the running order label. |
| Estimated time | `wef-data.trips.estimated-go-time` for an individual trip | Use `wef-live.groups.estimated-start-time` as the matched group's estimate. Keep it separate from scheduled start and trip go time. |
| Current and next | Data schedule position for the planned sequence | Use `wef-live.rings.current-group`, `next-group`, and `completed-groups` for the ring's observed group state. Use `wef-live.classes.current-trip-id` only if it resolves to a trip in that class. |
| Counts and gone-in | `wef-data.classes.entry-count`; `wef-data.schedules.source-json.total_trips` and `completed_trips`; `wef-data.trips.gone-in` | Keep Data counts and gone-in. Live `total` and `gone` may be shown separately for their own group or class scope. `wef-live.entries.gone` does not replace gone-in or identify the current trip. |

**Live match rule.** Require one match for the same customer, show, date, and ring. Match groups by group ID, classes by class ID, and entries by the matched class plus trip ID. Follow the Live ring and class links to verify scope. Use original observation time to establish age. A later Data write does not replace a valid Live observation just because it was written later. A missing, ambiguous, or stale Live value allows Data fallback. A failed read is a run error, not an empty Live result. See the [existing Live-first agreement](../wef-freshness-completion-contract-20261030.md:66).

## Run

1. Resolve the default show from shared `w-shows` and Day 0 from shared `w-days`. Build each date from shared Data records. Apply Live fields only to the Day 0 package when the shared observation and its match are verified. See the [Data map](https://airtable.com/appqWC58FLtmHcUAW/tblOqjxfb9MsUNPeo) and [Live map](https://airtable.com/appqWC58FLtmHcUAW/tblMyWUpi4vd7qFzk).
2. Validate scope, required joins, and completeness. An incomplete source read cannot remove rows from the feed.
3. For Day 0, compare with the saved full-feed Data version for the same `show-day-key`. Report added, changed, and removed schedule and trip keys. Compare display values, day priority, and ring details. Check timestamps alone do not count as schedule changes. Save only a new or changed diff.
4. For the full schedule, upsert the latest validated Data by `show-day-key`. Use the existing record when that key already exists. On error in either lane, preserve the last good feed and record the failed step.

## Airtable evidence

Write to [wef-schedule-app](https://airtable.com/appqWC58FLtmHcUAW/) using the existing fields. Reuse one run ID for the same lane and scheduled slot across the three tables.

| Table | On success | On error |
| --- | --- | --- |
| [run-logs](https://airtable.com/appqWC58FLtmHcUAW/tblWhGshVZuUlKHbO) | One completed row with lane, show, date, start and finish. Record the diff or no-change result for Day 0 and upsert counts for the full schedule. | One completed row with the failed step and result. |
| [heartbeat](https://airtable.com/appqWC58FLtmHcUAW/tblXeSnesC31P8P5S) | Update the lane's latest outcome and run ID. | Update the lane's latest outcome and run ID with the error. |
| [errs-log](https://airtable.com/appqWC58FLtmHcUAW/tbl0q9iniaffVoXAa) | — | One record per distinct failure, keyed by run ID and failed step. |

Use `wef-schedule-app|day0` and `wef-schedule-app|full` as heartbeat keys. Use ISO timestamps and the source show date. Leave scope fields blank if scope could not be resolved. Upsert by the existing keys so delivery of the same outcome cannot create duplicate evidence. An older run must not replace a newer heartbeat. If Airtable logging fails, preserve the outcome in the runner's own receipt and report the logging failure.

The [cleanup table](https://airtable.com/appqWC58FLtmHcUAW/tblIIxlPF7AtjEDeu) is a separate control. Retention and cleanup execution require their own contract.

## Acceptance

Verify full schedule upserts reuse the same show-day keys. Verify a changed Day 0 run publishes only the diff and an unchanged check leaves it untouched. Verify a missed check, a changed full-feed version, and a new Day 0 date. Verify an error preserves the last good dataset and writes the error evidence. Verify Astro reads both feeds on its own cadence and Live changes affect only Day 0.

## Local build and verification

November 10, 2026. Code is in the existing Schedule worktree at `C:/Users/gombc/.codex/worktrees/schedule-shared-scale-return-20261105`, based on `5bb23242`. The authorized change is the two schedule feeds, their app consumer, and the existing feed and outcome tables. The browser cadence, UI layout, Profile, upstream collectors, Airtable schema, and deployment are outside the change.

- `webflow-cloud-test/scripts/refresh-wef-schedule-feed.mjs` is the prepared sole-writer entry point. The scheduler would supply the lane and UTC slot. It upserts dated Data packages, stores a versioned Day 0 diff beside its unchanged Data package in `payload-json`, and writes outcome evidence. Astro's existing scope and data routes read the shared base and saved feed. No runner was activated.
- The Astro build and 22 local checks passed. Checks include the actual route handlers using a local Airtable substitute, unchanged updates, matched and rejected Live values, changed base versions, rollover, and run/error/heartbeat writes against the substitute. Code review compared the changes with the approved scope and all 17 Airtable field lists with the shared base schema. Rollover keeps the previous show date in the full feed.
- Captured shared records produced 14 November 11 rows and 23 November 12 rows with 46 active trips. Current full-show source coverage has 366 schedule rows across November 11–14 and no rows for November 15, which is still listed as a show date. The local replay rejected the incomplete full feed before any writes. A confirmed full-show run needs saved rows or verified empty-day evidence for that date.
- The shared Live tables were empty. Live rules passed with test observations. Acceptance with populated shared Live rows remains unverified. Packages that exceed the existing [Airtable field limit](https://support.airtable.com/articles/6193043870-long-text-fields-in-airtable) are rejected before any full-feed writes. The full-show payload sizes remain unverified.
