# Tenants relationship schema — verified structural checkpoint

Scope: existing agreed rules, three missing relationships. Worker: `/root/tenants_schema_completion`. No business rows, runtime changes, access grants, automation or deployment.

+STRUCTURAL READBACK — 2026-10-10
Primary/additional trainer association -> tenant-trainers tblnjrsLihxbtMkb5: Tenant fld1L3vGroQcvSOBY -> tenants; Trainer fldL1k6SjBabQKzvH -> trainers; Association fld9R9PJ6gdWKtHBd has Primary/Additional; Status fld6yzI0lV3FL7R17 Enabled/Disabled. Primary includes horses/riders; Additional horses only.
Tenant-local horse/rider exclusions -> tenant-roster tblwcczj8lFhVcbyt: Tenant fld2cVDu9Vx5yT7l9; Horse fldjg1nLULNprFKAg OR Rider fldRK86Yw3OtFgTk4; Status fldhbBQM8zxYdsILv Enabled/Disabled; Trainer association fldek4DnPqjDVzHmw. No global inactive flag changed.
Staff/groom assignments -> tenant-assignments tblnFOT8lGxCXD4Xh: Membership fldfvSRJqGkXp0SMi -> tenant-memberships; Horse fldMf44bM61AOKvyT; Status flduUszU3bH1PSMjz Active/Ended. Membership supplies tenant/person/roles.
Role overlap -> existing tenant-memberships.Roles and people.Rider retained. No role records invented.
Personal favorites -> existing favorites.Membership/Horse/Rider/Trainer/Status retained and linked separately from tenant-roster.
Owner-managed options -> existing optins.Membership recipient; Managed by person must resolve to enabled owner membership in same tenant; Notification scope includes Assignments; Selected favorites must match recipient membership. optin-options retains Opt-in/Choice/Value. Consent fields preserved; no consent granted.
Detailed schema readback verified all link destinations and select values; schema readback also confirmed four amended field descriptions. New tables contain zero records. This is structural representation, not runtime validation or enforcement.
Airtable link fields do not enforce exactly-one, cross-tenant consistency, required fields, uniqueness or permissions; those validations remain in existing runtime tasks.
One connector validation failure rejected prefersSingleRecordLink before creating anything; removed unsupported option and successful creation/readback followed. No retries of a production workflow.

## Existing operation reused

`tenant-memberships` identifies the person and tenant and stores tenant-specific roles. It survives absence from show-week/day entries. Staff can also reference a rider identity via `people.Rider`; groom assignment scope is independent of personal favorites. `favorites` remains personal, and neither follows nor roster appearance grants membership.

`optins.Membership` supplies recipient and tenant. Existing `Managed by` is a person reference; validate that person's enabled owner membership in the same tenant for owner-managed groom options. No extra manager identity table is needed. Scope `Assignments` resolves through active `tenant-assignments`. Revoked consent remains revoked. `optin-options` retains readable option choices and values.

## Next exact actions

1. Existing task `rec2YJdtU25OHZSDn`: verify deployed page/route/source/base bindings.
2. Existing task `recjNCFtq6dJB7sk3`: implement submission validation using these verified IDs, including one target, uniqueness, matching tenant, primary-only rider inclusion and owner/consent checks; preserve proven input implementation.
3. Existing task `recwzt7S1umtaMyvj`: runtime acceptance across two tenants, including one shared horse disabled in one tenant only, additional-trainer horses without riders, staff/rider overlap, groom assignment visibility and revoked consent.

Schema task `recW9Cy6E6JO4eh9e` is Done for structural representation only. Base health remains Needs work. New tables deliberately contain zero guessed business records.

Change record: `recQvmqpQcpD0Apvn`. Activity record: `recaUaRNGO43G4cHu`.
