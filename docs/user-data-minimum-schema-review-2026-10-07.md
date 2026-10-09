# User-data storage — CRM review and Recognize build

Updated 7 October 2026 (America/New_York; verification continued after UTC midnight).

## Built in Recognize

The user-data structure is now in the existing [Recognize base](https://airtable.com/app9kOZdIaGyKk5uG). **Six new tables and 15 fields on existing input tables were added.** The base now has 18 tables and 238 fields, compared with 12 tables and 151 fields before this build. All 151 original field definitions were preserved.

CRM was inspected read only. CRM module selection and construction remain deferred until the owner reviews this structure. This is the storage schema build; new forms, CRM mirroring, migration and automatic sending are not implemented by it.

There are 11 logical user-data groups, represented by **12 physical input/entity tables** because the existing riders table stays for prototype compatibility. Four recognition tables and two reference/index tables remain alongside them. Trainer, rider, groom, exerciser and staff responsibilities use people plus memberships/assignments, rather than separate copies of each person.

## Minimum fields and their location

The minimum is scoped to onboarding entities and alert inputs. Existing compatibility fields remain even when optional. Mutable user changes also retain **owner_uid, revision, request_uid and request_hash** according to the current input contract. Owner UID is the application owner, not Zoho's staff Owner ID. Lifecycle status applies to entity records; it does not establish consent or permissions.

| Live table | Baseline required data | Purpose / conditional requirements |
|---|---|---|
| [rs_input_barns](https://airtable.com/app9kOZdIaGyKk5uG/tblRvTwo3HYPUkZou) | `entity_uid`, `barn_uid`, `name`, `owner_uid`, `status`, `record_mode` | Keep the current entity_uid/barn_uid distinction. Barn organization, not horse nickname. |
| [rs_input_users](https://airtable.com/app9kOZdIaGyKk5uG/tblYgoeLEey05xgw9) | `entity_uid`, `barn_uid`, `name`, `status`, `record_mode` | email or phone_e164 only for features needing them. recognition_person_uid preserves recognition mapping; aliases optional. A person row does not grant login. |
| [rs_input_riders](https://airtable.com/app9kOZdIaGyKk5uG/tblnd2ToLs7dTzLAM) | `entity_uid`, `barn_uid`, `name`, `status`, `record_mode` | Compatibility table for the existing prototype. user_uid when linked to a resolved person. Do not duplicate every staff role into a separate identity. |
| [rs_input_horses](https://airtable.com/app9kOZdIaGyKk5uG/tblpyyaOMgjLzLvkP) | `entity_uid`, `barn_uid`, `name`, `status`, `record_mode` | rider_uid/location_uid optional during initial onboarding. barn_name means nickname; aliases optional. |
| [rs_input_locations](https://airtable.com/app9kOZdIaGyKk5uG/tblEuwOr1rUKnj1j3) | `entity_uid`, `barn_uid`, `name`, `record_mode`, `status` | address when an address/map feature needs it. Current prototype is barn-scoped; shared venue ownership requires an explicit application rule. |
| [rs_input_events](https://airtable.com/app9kOZdIaGyKk5uG/tblwts3huk3w1ACjh) | `event_uid`, `actor_uid`, `entity_uid`, `barn_uid`, `request_uid`, `occurred_at`, `input_hash`, `record_mode` | Preserve action, kind, outcome and result_json according to the existing input-event writer. This table's alternate event shapes need their current application validation, not a blanket required setting on all fields. |
| [rs_input_shows](https://airtable.com/app9kOZdIaGyKk5uG/tblju8nkVP717nh1L) | `entity_uid`, `name`, `start_date`, `end_date`, `timezone`, `record_mode`, `status` | Show occurrence metadata only; WEC/WEF source mapping lives in rs_source_ids. No schedules, classes, results or engine state. |
| [rs_input_rings](https://airtable.com/app9kOZdIaGyKk5uG/tbl1pjpN208IXVqRe) | `entity_uid`, `name`, `location_uid`, `record_mode`, `status` | Stable ring reference and venue scope only. Ring state and timing remain in each engine. |
| [rs_input_memberships](https://airtable.com/app9kOZdIaGyKk5uG/tbl0cNCmNwSwsCUF6) | `entity_uid`, `barn_uid`, `user_uid`, `roles`, `status`, `record_mode` | People-to-barn roles. Trainers and staff are users/people with memberships; this table does not itself grant authentication permissions. |
| [rs_input_assignments](https://airtable.com/app9kOZdIaGyKk5uG/tbluBFpQQMKDdVvEP) | `entity_uid`, `barn_uid`, `horse_uid`, `user_uid`, `role`, `status`, `record_mode` | Horse-to-person assignments for riders, trainers and staff. Keep optional show scope when an assignment is specific to a show. |
| [rs_source_ids](https://airtable.com/app9kOZdIaGyKk5uG/tbl0oVtuFNRy7zWsX) | `mapping_uid`, `entity_type`, `entity_uid`, `source_system`, `source_entity_type`, `source_scope`, `external_id`, `record_mode` | Lossless external identity mapping across WEC, WEF, legacy Airtable and future CRM. This is identity metadata, not engine data or a running synchronization. |
| [rs_input_subscriptions](https://airtable.com/app9kOZdIaGyKk5uG/tblbpALv7flu3NNEE) | `entity_uid`, `barn_uid`, `user_uid`, `engine_scope`, `target_type`, `target_uid`, `alert_types`, `status`, `consent_state`, `record_mode` | User alert selections and consent state. No sending or automatic CRM synchronization is configured. Unknown consent never becomes Granted by migration. |

Additional subscription requirements: active SMS needs phone_e164, Granted consent, consent_at and consent_source. Preserve notice_version when the collection flow uses versioned notices, and revoked_at after withdrawal. Unknown legacy consent stays Unknown. WEC and WEF are separate engine_scope choices; subscription logic stays with each engine. This table covers SMS only; other channels are not implemented.

Shows need valid start/end dates and an IANA timezone. A ring needs venue/location scope. External IDs remain text, including leading zeros, and are scoped by source system, source entity type and source namespace. No bare source ID is assumed globally unique.

**Required fields, UID relationships, uniqueness, valid date ranges, consent and access rules are application requirements documented here and in field descriptions. Airtable does not enforce these through the added text fields.** UID references follow the existing prototype rather than introducing linked-record fields. No automatic relationship, validation, synchronization or sending behavior was added.

### Exact live fields

- **rs_input_barns** (9 fields): `entity_uid`, `barn_uid`, `name`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `status`, `record_mode`.
- **rs_input_users** (13 fields): `entity_uid`, `barn_uid`, `name`, `email`, `recognition_person_uid`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `status`, `record_mode`, `phone_e164`, `aliases`.
- **rs_input_riders** (10 fields): `entity_uid`, `barn_uid`, `name`, `user_uid`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `status`, `record_mode`.
- **rs_input_horses** (13 fields): `entity_uid`, `barn_uid`, `name`, `rider_uid`, `location_uid`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `status`, `record_mode`, `barn_name`, `aliases`.
- **rs_input_locations** (10 fields): `entity_uid`, `barn_uid`, `name`, `address`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `status`, `record_mode`.
- **rs_input_events** (12 fields): `event_uid`, `actor_uid`, `entity_uid`, `barn_uid`, `action`, `request_uid`, `occurred_at`, `input_hash`, `result_json`, `kind`, `outcome`, `record_mode`.
- **rs_input_shows** (12 fields): `entity_uid`, `name`, `start_date`, `end_date`, `timezone`, `location_uid`, `status`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `record_mode`.
- **rs_input_rings** (11 fields): `entity_uid`, `name`, `location_uid`, `ring_number`, `aliases`, `status`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `record_mode`.
- **rs_input_memberships** (10 fields): `entity_uid`, `barn_uid`, `user_uid`, `roles`, `status`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `record_mode`.
- **rs_input_assignments** (12 fields): `entity_uid`, `barn_uid`, `horse_uid`, `user_uid`, `role`, `show_uid`, `status`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `record_mode`.
- **rs_source_ids** (8 fields): `mapping_uid`, `entity_type`, `entity_uid`, `source_system`, `source_entity_type`, `source_scope`, `external_id`, `record_mode`.
- **rs_input_subscriptions** (19 fields): `entity_uid`, `barn_uid`, `user_uid`, `engine_scope`, `target_type`, `target_uid`, `alert_types`, `phone_e164`, `status`, `consent_state`, `consent_at`, `consent_source`, `notice_version`, `revoked_at`, `owner_uid`, `revision`, `request_uid`, `request_hash`, `record_mode`.

### Fields added to existing tables

- **rs_input_barns:** `status`, `record_mode`.
- **rs_input_users:** `status`, `record_mode`, `phone_e164`, `aliases`.
- **rs_input_riders:** `status`, `record_mode`.
- **rs_input_horses:** `status`, `record_mode`, `barn_name`, `aliases`.
- **rs_input_locations:** `status`, `record_mode`.
- **rs_input_events:** `record_mode`.

The six new tables contain 72 fields. These 15 additions bring the total added field count to 87. Existing rows were not backfilled: a blank record_mode is unclassified, not Live. New records must be explicitly Test or Live. Conditional contact, alias, mapping and request fields are not all mandatory visible form inputs.

## CRM: live account findings

The connected **ringstatus** CRM account exposes **50 modules**. None is reported as a custom-generated module. Field metadata and standard layouts were read for all **17 creatable business record modules**; Notes and Attachments are related-content modules rather than proposed user entities. The remaining inventory consists of navigation, reporting, system or related-content modules.

| CRM module | Fields inspected | System-required fields | Standard layout-required fields |
|---|---:|---|---|
| Leads | 55 | Last_Name | Standard: Company, Last_Name |
| Contacts | 60 | Last_Name | Standard: Last_Name |
| Accounts | 55 | Account_Name | Standard: Account_Name |
| Deals | 29 | Deal_Name, Stage | Standard: Deal_Name, Closing_Date, Account_Name, Stage |
| Tasks | 21 | Subject | Standard: Subject |
| Events | 35 | Event_Title, Start_DateTime, End_DateTime, Meeting_Venue__s | Standard: Event_Title, Meeting_Venue__s, Start_DateTime, End_DateTime |
| Calls | 27 | Call_Type, Call_Start_Time, Call_Duration | Standard: Call_Type, Call_Start_Time, Call_Duration |
| Products | 31 | Product_Name | Standard: Product_Name |
| Quotes | 46 | Subject, Quoted_Items | Standard: Subject, Account_Name, Quoted_Items |
| Sales_Orders | 51 | Subject, Ordered_Items | Standard: Subject, Account_Name, Ordered_Items |
| Purchase_Orders | 49 | Subject, Vendor_Name, Purchase_Items | Standard: Subject, Vendor_Name, Purchase_Items |
| Invoices | 49 | Subject, Invoiced_Items | Standard: Subject, Account_Name, Invoiced_Items |
| Campaigns | 21 | Campaign_Name | Standard: Type, Campaign_Name |
| Vendors | 30 | Vendor_Name | Standard: Vendor_Name |
| Price_Books | 14 | Price_Book_Name | Standard: Price_Book_Name |
| Cases | 29 | Status, Case_Origin, Subject | Standard: Status, Case_Origin, Subject |
| Solutions | 19 | Solution_Title | Standard: Solution_Title, Question, Answer |

These are returned metadata constraints, not a claim that a minimal production API create was tested. Conditional business rules and permissions can impose additional constraints. No CRM records were created or changed.

**Accounts** already contains RS_Entity_UID, RS_Owner_UID, RS_Revision, RS_Request_UID and RS_Request_Hash. RS_Entity_UID is unique without case sensitivity. These fields predate this build. Account_Name is mandatory. This makes Accounts an existing barn-organization candidate, not a module choice imposed by this task.

**Contacts** requires Last_Name. Email is optional but configured as unique without case sensitivity. No RS custom fields were present. The current full display name in Recognize cannot be assumed to supply a valid surname automatically. This affects the eventual mapping/form decision.

There are no dedicated custom Horse, Show, Ring, Location, Membership, Assignment or Subscription modules in the returned inventory. CRM Events is Meetings; it is not automatically a horse-show occurrence. Products is not automatically a horse model. These module choices are intentionally deferred.

### Complete CRM inventory

`Home`, `Workqueue__s`, `Leads`, `Contacts`, `Accounts`, `Deals`, `Activities`, `Tasks`, `Events`, `Calls`, `Reports`, `Analytics`, `Products`, `Quotes`, `Sales_Orders`, `Purchase_Orders`, `Invoices`, `Feeds`, `SalesInbox`, `Campaigns`, `Vendors`, `Price_Books`, `Cases`, `Solutions`, `Documents`, `Forecasts`, `Visits`, `DealHistory`, `Quoted_Items`, `Social`, `Email_Sentiment`, `Ordered_Items`, `Email_Analytics`, `Email_Template_Analytics`, `Purchase_Items`, `Invoiced_Items`, `Notes`, `Attachments`, `Emails`, `Actions_Performed`, `Forecast_Quotas`, `Forecast_Items`, `Forecast_Groups`, `Locking_Information__s`, `Functions__s`, `Email_Template__s`, `Email_Drafts__s`, `Approvals`, `CalendarBookings__s`, `Unknown__s`

## Verification and reversal

- Live schema readback verified all six new tables, all 15 additions to existing tables, types, descriptions and choice values.
- All **151 original field IDs, names, types, descriptions and detailed configurations** match the before-build snapshot. Original table IDs, names and primary fields were preserved.
- One explicitly synthetic Test record in each new table passed create/read and update/read checks. Checks included date/time, single/multiple choice, integer revision and lossless leading-zero external IDs.
- All six synthetic records were deleted by their exact returned record IDs. A final read confirmed all six new tables empty.
- No real recipient or Granted/Active SMS subscription was created. No CRM writes, legacy data import, engine execution, RingWaze changes, Webflow changes, form wiring or automatic synchronization occurred.
- The Recognize automation listing returned no automations before these writes. Existing prototype runtime behavior was not exercised; unchanged source fields are schema-compatibility evidence, not a live application test.

The [build evidence](user-data-storage/recognize-build-2026-10-07.json) contains complete before/after schema snapshots, exact new table/field IDs, mutation results, CRM metadata summary and synthetic test readbacks.

To unwind **this build only**: first check whether any subsequent form, adapter or user record now uses the added tables/fields and export any such data. Remove the six newly created tables identified in the manifest, then only the 15 added fields on original tables. Use the manifest IDs, not approximate names. Do not delete any original table/field, legacy record or recognition record. This task did not create CRM changes to undo. No rollback was executed.

## Legacy review retained for mapping

The following review covers all **42 distinct legacy tables** in the user-data index: 14 WEC and 28 RingStatus, with 936 field definitions and up to three sample records per table. The complete index had 66 rows and 64 distinct base/table references, including Recognize and RingWaze. Duplicate WEC hs_horses/rings index entries were reviewed once. Verified source base IDs are WEC app6XS1RvsPNRT6os and RingStatus apptdhhNzduxm5gjn; malformed stored index pointers were not edited.

“WEF” below refers to the RingStatus legacy source in the index, which also contains shared and WEC-related material. **Proposed merge/consolidation entries are mappings, not executed migration or permission to delete source tables.** Samples do not establish complete identity reconciliation, safe deletion or every runtime dependency.

## WEC — each table by category

“Fields to carry” names source fields for the proposed minimum or retained adapter. It is not the full set a legacy runtime currently requires. Counts are the connector-reported totals at review time.


### Horse names

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [barn_name](https://airtable.com/app6XS1RvsPNRT6os/tblLpAnH2N9GaZbIP) | 65 / 8 | Merge nickname representation | **horses.barn_name; alias support if needed**. Source: barn_name, horses/ww_horses; horse as source display | Samples pair Munster with MASTERMIND and Coffee with IRISH COFFEE VHL. This is a horse nickname helper, not a barn organization. |
| [horse_aka](https://airtable.com/app6XS1RvsPNRT6os/tblIaWvZ0KKRHZvnt) | 22 / 3 | Fold aliases into horse entity | **horses.aliases**. Source: Name, horses/ww_horses | Preserve every alias and source identity; resolve disagreements before coalescing. No new alias table is required unless aliases acquire their own metadata. |

### Horses

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [horses](https://airtable.com/app6XS1RvsPNRT6os/tblgWogH7B6Cvusvm) | 1408 / 47 | Canonical entity candidate | **horses; source_ids; horse_assignments**. Source: horse, barn_name, aka/horse_aka, hs_horses; riders/trainers as resolved assignments | Current documented barn-name editing targets horses.barn_name. Preserve that integration until a separately verified migration. entry_no and schedule/result links are engine context. |
| [hs_horses](https://airtable.com/app6XS1RvsPNRT6os/tblHs7HIlohqAIb2F) | 1406 / 28 | Retain source adapter; map identity | **horses; source_ids**. Source: horse_key, horse_name, barn_name, horse_aka, horses | 1,406 records versus 1,408 in horses. Same-name or same-count assumptions cannot establish a one-to-one merge. Preserve catalyst_ROWID and sync fields in the adapter. |
| [ww_horses](https://airtable.com/app6XS1RvsPNRT6os/tbl3HY1et6DnHxLka) | 60 / 61 | Consolidate curated entity data | **horses; source_ids; horse_assignments**. Source: horse_id, ﻿horse/show_name, horse_aka, horses; rider/trainer/groom links only after identity resolution | Imported legacy copy with packing/feed and display fields mixed in. Keep conditional horse-care fields with their feature; exclude wave/print/sync state from the new entity core. |

### Riders

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [hs_riders](https://airtable.com/app6XS1RvsPNRT6os/tblcdYx173IwTYV1H) | 739 / 20 | Retain source adapter; map identity | **people; source_ids; horse_assignments**. Source: rider_key, rider_name, rider_aliases, riders | 739 records; sync metadata stays in the WEC adapter. A rider is not automatically an authenticated user or barn member. |
| [riders](https://airtable.com/app6XS1RvsPNRT6os/tbl75W08G7nB4MYAl) | 739 / 26 | Merge person identity; preserve source links | **people; source_ids; horse_assignments**. Source: rider, hs_riders; horses/trainers only as resolved relationships | 739 records does not prove equivalence with hs_riders. cwf/follow and current-entry links are not identity fields. |

### Rings

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [hs_rings](https://airtable.com/app6XS1RvsPNRT6os/tblgD6YyXk9VlUoeM) | 28 / 17 | Retain adapter; consolidate reference data | **rings; source_ids**. Source: ring_key, ring_no, ring_name, ring_aliases | 28 source helper records versus 19 rings and 11 ring_names. Preserve source/venue scope and sync metadata; do not merge by number alone. |
| [ring_names](https://airtable.com/app6XS1RvsPNRT6os/tblcHfnJzCYLoBhjf) | 11 / 17 | Fold names into ring reference | **rings.name/aliases/display_order**. Source: ring_name, rings; priority if the UI needs it | One name helper may link multiple ring instances. Resolve venue/source identity before merging. |
| [rings](https://airtable.com/app6XS1RvsPNRT6os/tbl5WKTbwL6IVrjyI) | 19 / 35 | Canonical reference candidate | **rings; source_ids**. Source: ring_no, ring_name, ring_names, shows, source | Current status, schedule and RingWaze relationships remain in their systems. The new ring record holds stable reference data only. |

### Subscription

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [subscribe](https://airtable.com/app6XS1RvsPNRT6os/tblGY7XoPGHaIWd91) | 37 / 4 | Exclude from user-record storage | **Workflow documentation; no user table**. Source: trigger, Finding, Notes, trigger_lanes | The 37 records are trigger/policy notes. Samples describe alert spacing and entry triggers, not recipient opt-ins. |

### Trainers

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [hs_trainers](https://airtable.com/app6XS1RvsPNRT6os/tblqsyyp6g8FCErXJ) | 266 / 22 | Retain source adapter; map identity | **people; source_ids; barn_memberships**. Source: trainer_key, trainer_name, trainer_aliases, trainers | Keep allowed/active/follow and Catalyst sync behavior in the source adapter until their consumers are verified. Do not turn tenant_name into a confirmed barn link automatically. |
| [trainers](https://airtable.com/app6XS1RvsPNRT6os/tblB72MubQbWfEqdf) | 266 / 27 | Merge person identity | **people; source_ids; barn_memberships**. Source: trainer, trainer_display, hs_trainers; resolved barn/horse relationships | Active engine allowlists and downstream linked records remain in place. Trainer identity alone does not establish membership or permissions. |

### Users

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [profiles](https://airtable.com/app6XS1RvsPNRT6os/tbl0Rpv3zuZQwaoLI) | 9 / 13 | Merge identity; split alert targeting | **people; barn_memberships; alert_subscriptions**. Source: profile_name, subscriberTo, active; resolved trainers/riders/horses | profiles and schedule links cannot substitute for a stable person ID. No inspected field proves SMS consent. |

## WEF / RingStatus legacy — each table by category


### Barns / tenants

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [ww_tenants](https://airtable.com/apptdhhNzduxm5gjn/tblKqQdTmkoMj406a) | 12 / 44 | Map organization after resolving tenant meaning | **barns; source_ids**. Source: tenant_id, tenant_official_name, tenant_active | 12 records. Do not assume every tenant is one barn. Logo/acronym optional; milestones/print/publish settings stay with their features. Some heartbeat lookups are invalid in the live schema. |

### Entitlements

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [profile_levels](https://airtable.com/apptdhhNzduxm5gjn/tblg39G5RSqt2sLwV) | 4 / 3 | Use controlled plan values unless richer metadata needed | **barn_memberships.plan or feature entitlement**. Source: profile_id, ww_profiles | Sample values sms/custom/pro mix capability and plan concepts. They are not person identities or explicit opt-ins; preserve meaning until resolved. |

### Horse names

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [barn_names](https://airtable.com/apptdhhNzduxm5gjn/tblbnLy38IBKN29d0) | 176 / 4 | Merge nickname representation | **horses.barn_name/aliases**. Source: barn_name, ww_horses | 176 records. Samples and links identify horse nicknames, not barn businesses. Preserve alternate names rather than overwriting one with another. |

### Horses

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [q_horses](https://airtable.com/apptdhhNzduxm5gjn/tbluu8iZZl0VmJgaK) | 51 / 20 | Merge horse identity and chosen attributes | **horses; source_ids**. Source: horseId, name, showName, ww_horses | membershipId/microchip, birth date, sex, color and hands are conditional profile data. Feed relationships stay with feeding. Numeric microchip values need lossless review before migration. |
| [ww_horses](https://airtable.com/apptdhhNzduxm5gjn/tblliyUZ1ZS88Kfvl) | 153 / 121 | Consolidate curated entity data | **horses; source_ids; horse_assignments**. Source: horse_id, horse/show_name, barn_name, ww_tenants; resolved rider/trainer/staff links | 153 rows with 121 fields. Code currently upserts by horse name. Operational entry/run fields and derived lists cannot be removed from the existing table during this review. |

### Horses / identifiers

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [horse_ids](https://airtable.com/apptdhhNzduxm5gjn/tbllMpqoMcF34svp5) | 123 / 4 | Consolidate external identifiers | **source_ids**. Source: horse_id, ww_horses, active | 123 records. Preserve provider and entity type; external IDs are text in the proposed mapping, not globally unique bare numbers. |

### Horses / reference

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [horse_colors](https://airtable.com/apptdhhNzduxm5gjn/tbla7YZzFZZKzTTJx) | 7 / 3 | Replace helper table if no independent metadata | **horses.color**. Source: color, ww_horses | Seven records; LP enrichment is a linked consumer. Color is not mandatory for onboarding identity. |
| [horse_disciplines](https://airtable.com/apptdhhNzduxm5gjn/tbldZm5TB485LqFtJ) | 3 / 2 | Replace helper table if no independent metadata | **horses.disciplines**. Source: discipline, ww_horses | Three records. Candidate for controlled values; retain source links until dependencies are migrated. |
| [horse_genders](https://airtable.com/apptdhhNzduxm5gjn/tblxqqI41sB4RGDrm) | 2 / 3 | Replace helper table if no independent metadata | **horses.sex**. Source: gender, ww_horses | Two records; LP enrichment also links here. Do not remove a shared helper until that consumer is accounted for. |

### Riders

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [ww_riders](https://airtable.com/apptdhhNzduxm5gjn/tblI6yrqv0Y4QTvyi) | 75 / 53 | Merge person identity | **people; source_ids; horse_assignments**. Source: rider_id, name/rider_name, usef_no/fei_id if used, ww_trainers/ww_tenants | 75 rows. Inspected rider_info.js uses rider_id as its upsert key. Daily trip lists, show/run flags and empty-trip caches stay outside the core. |

### Riders / aliases

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [rider_names](https://airtable.com/apptdhhNzduxm5gjn/tblTRxi244zCDptqt) | 77 / 5 | Fold names into person aliases | **people.aliases; source_ids**. Source: rider_name, ww_riders | 77 records, linked to horses and trips. Retain matching relationships until those integrations are migrated. |

### Riders / identifiers

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [rider_ids](https://airtable.com/apptdhhNzduxm5gjn/tbl33ZuOpMzMU40iz) | 78 / 4 | Consolidate external identifiers | **source_ids**. Source: rider_id, ww_riders, active | 78 IDs versus 75 rider rows. Preserve one-to-many aliases and source keys; do not discard extra IDs. |

### Riders / teams

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [team_names](https://airtable.com/apptdhhNzduxm5gjn/tblga68dBmf3N501r) | 83 / 8 | Keep as conditional relationship data | **people/team or barn relationship after clarification**. Source: team_name, contacts, ww_riders | 83 rows. A team is not necessarily a barn. A dedicated teams table is justified only if distinct team identity, membership or attributes are actually required. |

### Rings

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [ring_names](https://airtable.com/apptdhhNzduxm5gjn/tblZIcLqJ6ANcMvTs) | 20 / 6 | Fold aliases into reference | **rings.name/aliases/display_order**. Source: ring_name, ring_short, ring_number, ww_rings, active | 20 name rows versus six ww_rings rows. Ring number is not a globally safe key. |
| [ww_rings](https://airtable.com/apptdhhNzduxm5gjn/tbl8CoOJEvQJbN7kZ) | 6 / 64 | Consolidate stable ring reference | **rings; source_ids**. Source: ring_id, ring_number, ring_name, customer_id, aliases, coordinates if used | Six rows. Map coordinates and labels are conditional display data; travel-time estimates, seasonal endpoints and live ring state stay in their systems. |

### Shows in index; actually horse names

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [show_names](https://airtable.com/apptdhhNzduxm5gjn/tblWscKpFAFFAQnNp) | 165 / 2 | Reclassify in proposal; merge horse name helper | **horses.show_name/aliases**. Source: horse, ww_horses | 165 records. Sample values are horse competition names. This does not provide show-event entities. |

### Staff

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [ww_exercisers](https://airtable.com/apptdhhNzduxm5gjn/tbl9RX9IJdSz7yTBX) | 4 / 8 | Merge people with role and assignments | **people; barn_memberships; horse_assignments**. Source: record_name, ww_riders, ww_horses, ww_tenants/active_tenants | No separate exerciser entity table needed in the proposed core. Do not deduplicate solely by name. |
| [ww_grooms](https://airtable.com/apptdhhNzduxm5gjn/tblKRxPAlapW1h8V2) | 11 / 12 | Merge people with role and assignments | **people; barn_memberships; horse_assignments**. Source: groom_name, groom_phone, active, contacts, ww_horses, ww_tenants | 11 rows. Keep one person with groom role; resolve contact identity before importing assignments. |

### Subscriptions

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [active_subscribers](https://airtable.com/apptdhhNzduxm5gjn/tblA0Z82fEyKgUP6L) | 14 / 22 | Consolidate recipients and scoped opt-ins | **alert_subscriptions; people; input_events**. Source: subscriberTo, active, ww_profiles, ww_tenants, ww_horses/ww_riders/ww_trainers/shows | 14 rows. No inspected consent timestamp/source or notice version. Existing active=true must not be promoted to newly verified consent. |

### Trainers

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [ww_trainers](https://airtable.com/apptdhhNzduxm5gjn/tblRJOVhOLuEJOkgF) | 20 / 64 | Merge person identity | **people; source_ids; barn_memberships; horse_assignments**. Source: trainer_id, name/lf_name, aka, ww_tenants; master_trainer if required | 20 rows. trainer_info.js keys by trainer_id; horse/rider lists, scheduler and publishing links are separate operational concerns. |

### Users

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [contacts](https://airtable.com/apptdhhNzduxm5gjn/tbl5NCW3XimP39wC8) | 118 / 17 | Merge person/contact identity | **people; barn_memberships; horse_assignments; source_ids**. Source: rec_name/rec_label, rec_id, contact_types; q_users/ww_grooms/ww_horses links | Contact, rider and user records may represent the same person but require verified mapping. Retain contact-only people without granting login. |
| [q_users](https://airtable.com/apptdhhNzduxm5gjn/tblTuIRgvFaghVaAV) | 61 / 15 | Merge person data and memberships | **people; barn_memberships; source_ids**. Source: id, userFullName or firstName/lastName, contacts, roles/rs_roles, status | Preserve Questri source identity without assuming role strings authorize access. stripeCustomerId is conditional billing metadata, not onboarding minimum. |
| [ww_profiles](https://airtable.com/apptdhhNzduxm5gjn/tblceAsIcT75zplhc) | 10 / 32 | Split identity, roles and subscriptions | **people; barn_memberships; alert_subscriptions**. Source: profile_name, sms, active, profile_type/ww_roles, ww_tenants/active_tenants, rs_people_test | profile_level is an entitlement/plan choice; can_subscribe is capability, not consent. Preserve existing links until consumers are mapped. |

### Users / recognition

| Source table | Records / fields | Proposed treatment | Fields to carry / target | Reason and retained dependency |
|---|---:|---|---|---|
| [active_iams](https://airtable.com/apptdhhNzduxm5gjn/tblOLUZ6pI3zzvID4) | 11 / 25 | Retain recognition boundary; map person | **Recognition store; people/source_ids**. Source: rec_id, rec_name, active, ww_profiles, rs_people_test | IP and phone suffix-based lookups are not durable identity. OTP and visit fields remain recognition concerns. |
| [rs_devices_test](https://airtable.com/apptdhhNzduxm5gjn/tbl1bzNc3qJcySxgd) | 3 / 8 | Retain separately; migration undecided | **Recognition store**. Source: device_uid, device_token, person, status, first_seen_at, last_seen_at, recognition_source | Legacy table has three records. Do not merge into people or move device tokens into the CRM mirror. Active Recognize prototype remains separate. |
| [rs_people_test](https://airtable.com/apptdhhNzduxm5gjn/tbltL8Q9ICYtFOHeG) | 4 / 19 | Map shared identity; preserve authentication contract | **people; source_ids; recognition store**. Source: person_uid, person_name, primary_phone_e164, status; email if supplied | access_level and member_pin are recognition/access data, not ordinary CRM contact metadata. Do not copy secrets to the mirror. |
| [rs_phone_aliases_test](https://airtable.com/apptdhhNzduxm5gjn/tblgtizEhGGoWkJe1) | 2 / 6 | Retain identity aliases separately | **Recognition store**. Source: alias_uid, alias_phone_e164, person, alias_type, status | Two records. Phone aliases and verification/recovery behavior must survive independently of the main contact phone. |
| [rs_recognition_sessions_test](https://airtable.com/apptdhhNzduxm5gjn/tblBMet2FdxbQ48AH) | 1167 / 34 | Keep event ledger separate | **Recognition event store**. Source: session_event_uid, session_uid, event_type, event_result, event_at, person/device, idempotency_key | 1,167 records, not 1,167 users. Retention/enrichment and automation fields need a recognition review; none are silently removed by this proposal. |


## Recognize compatibility and recognition boundary

Existing rs_input_barns, rs_input_users, rs_input_riders, rs_input_horses, rs_input_locations and rs_input_events were extended only as listed above. Existing rs_devices_test, rs_people_test, rs_phone_aliases_test, rs_recognition_sessions_test, quick-references and recognize-index were preserved. Recognition secrets/session/device data are not copied into CRM contact metadata.

The legacy RingStatus base contains older similarly named recognition tables. Similar names do not prove current deployment usage; their retirement remains undecided.

## RingWaze — separate system

These 11 indexed WEC tables remain RingWaze scope. No consolidation into onboarding entities is proposed:

- waze_session_footprints
- waze_users
- wec_class_comments
- wec_comment_presets
- wec_comments
- wec_entry_comments
- wec_observations
- wec_question_templates
- wec_ring_checkins
- wec_ring_comments
- wec_sessions

RingWaze may reference a shared person, show or ring UID after an explicit identity mapping. Comments, observations, check-ins, question templates, presets and session footprints remain owned by RingWaze. Its complete minimum field design was not requested in this entity review.

## Findings that change the consolidation decision

1. **show_names is not the show-events table.** Its primary field is horse and samples contain competition horse names. Merge into horse name/alias representation, not Shows.
2. **barn_names / barn_name are horse nickname helpers.** They are not barn organizations. Organization candidates are confirmed tenant records and rs_input_barns.
3. **WEC subscribe is documentation, not subscriptions.** Its 37 records contain trigger findings and notes. It cannot supply recipient consent.
4. **hs_* helpers are source adapters, not automatically redundant tables.** They carry source keys, Catalyst IDs, sync actions and errors. The new core can absorb identity while these adapters retain engine-specific behavior.
5. **Legacy subscriber active flags do not prove an opt-in record.** Inspected schemas lack explicit consent timestamp/source. Preserve unresolved provenance without inventing consent.
6. **Core data and operational fields are interleaved.** Examples include horse entry/run state, rider trip caches, trainer publication links and ring travel-time calculations. Those fields stay with existing operational consumers until separately migrated.
7. **There are gaps in the index.** Actual shows tables, stable barn business identity and locations are not adequately represented by the legacy category labels. Live shows schemas were inspected as a supplementary reference: WEC has show_no/show_name/show_start/show_end; RingStatus has show_id/show_name/start_date/end_date. No source show records were imported.
8. **The new core minimum is not a deletion list.** A full consumer audit is required before removing any field/table from the current bases.


## Decisions deliberately left for the next stage

- CRM module choice and mappings after the owner reviews Recognize, including Contacts surname handling and Account identity mapping.
- Form/backend integration and enforcement of required fields, UID integrity, duplicate rules and opt-in conditions.
- Legacy record reconciliation, including tenant-versus-barn and team meanings; no name-only merges.
- CRM/Airtable authority, synchronization, conflict handling and failover implementation. CRM remains the intended authority; no two-way editing or automatic failover is enabled.
- Engine storage placement, RingWaze integration and high-volume recognition retention remain separate work.
- Conditional care/feed, billing, maps and other feature data should be added only when the specific consuming feature is defined. Existing source data remains intact.

## Local dependency evidence inspected

- [WEF horse ingestion](../lib/airtable/horse-info.js): existing upsert key is horse; writes source/entry/run fields.
- [WEF rider ingestion](../lib/airtable/rider_info.js): existing upsert key is rider_id; maintains trip-derived lists.
- [WEF trainer ingestion](../lib/airtable/trainer_info.js): existing upsert key is trainer_id; maintains rider/horse lists.
- [WEC two-way edit contract](horseshowing/wec-airtable-two-way-edit-contract.md): documented barn-name edits target horses.barn_name and depend on helper synchronization.
- [WEC sync conflict register](horseshowing/wec-catalyst-airtable-sync-conflict-register.md): approved-field synchronization and source identity must be preserved.
- [Recognition session design](superpowers/specs/2026-07-14-rs-recognition-session-test-design.md): person, devices, phone aliases and append-only event records have separate responsibilities.

Repository evidence describes local code/contracts, not proof of current deployed behavior. No runner, endpoint or production-data workflow was executed for this review.
