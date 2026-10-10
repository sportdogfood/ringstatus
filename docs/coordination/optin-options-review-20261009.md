# Opt-in option fields — review, October 9, 2026

**Consolidated reference:** [Barns, show data, inputs and alerts](barn-show-inputs-reference-20261009.md). This file retains the supporting source-level option inventory.

Existing page: https://ringstatus.com/rs-barn-onboarding-alerts

Inspected source: C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test/src/assets/rs-input-subscriptions/native-client.js (definitions and save validation). This is a source review; no fresh live form test or modification was performed.

## Eight alerts with option fields

| Alert | Key | Current choices |
| --- | --- | --- |
| Class reminder 1 | class_starts_in1 | 30, 45, 60, 75, 90 minutes before expected start |
| Class reminder 2 | class_starts_in2 | 15, 30, 45, 60 minutes before expected start |
| Ride reminder 1 | trip_starts_in1 | 30, 45, 60, 75, 90 minutes before expected trip |
| Ride reminder 2 | trip_starts_in2 | 15, 30, 45, 60 minutes before expected trip |
| Rides to go | rider_oog10 | 7, 10, 15 entries before the rider; default value 10 |
| Task reminder 1 | groom_tasks_at1 | Clock time, HH:mm |
| Task reminder 2 | groom_tasks_at2 | Clock time, HH:mm |
| Task reminder 3 | groom_tasks_at3 | Clock time, HH:mm |

Each alert also has an on/off selection. The minute/count choices are preset-only in current save validation. The separate 1–1440 minute range check does not enable arbitrary minute values because the preset check runs first. Task times use the saved person-level time zone.

## Ten on/off-only alerts

Class delayed; class moved earlier; class started; class in progress; class finished; rider order; rider started; first score; first time; rider results.

## Other existing inputs

Master SMS on/off; mobile number. The time zone is initialized from the browser, stored and displayed; this inspected client does not provide a time-zone editing control.

## Agreed direction and gaps

The current form saves one personal preference set; it does not select a barn, horse, rider, groom, assignment or show. Existing save/reload evidence is in sms-optin-connection-20261009.md and does not establish engine delivery.

The owner agreed that consent/default preferences belong to the person and alert applicability must reflect barn/follow lists and assignments. Groom preferences and assignments are managed by the entity owner, not the groom. The recipient's own SMS consent remains separate. This owner-management behavior is not implemented by the inspected personal form.

Modification decisions remain open: which existing choices to change, exact alert scope controls, and whether groom task reminders should remain fixed clock times or have another basis. Do not infer new timing choices or task semantics. No application, schema, consent or delivery changes authorized or performed by this review.
