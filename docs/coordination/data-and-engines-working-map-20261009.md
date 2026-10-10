# RingStatus data and engines — working map

**Consolidated reference:** [Barns, show data, inputs and alerts](barn-show-inputs-reference-20261009.md). Start there; this earlier note preserves the initial discussion and locks, before later weekly/search/red-line/audit details were added.

Recorded October 9, 2026 from the owner's current explanation in the coordination chat.

Status: working architecture discussion, with the barn membership and follow-list rules below explicitly locked by the owner on October 9, 2026. This records design decisions, not implementation or test completion. No application, Airtable schema or agent instructions are changed by this note.

## Presentation and application headings

Owner's headings: **Webflow static | Astro1 | Astro2**.

The responsibilities and division between Astro1 and Astro2 have not yet been explained. Do not infer them.

The owner listed these areas beneath that structure:

- Recognize
- Tenants / barns
- Opt-ins
- Horses
- Riders
- Locations
- Users
- Roles
- Rings
- Shows

This list does not assign a separate base, table, application or agent to every area.

## Tenant and barn data

**The barn is the tenant.** A barn will add:

- Horses
- Riders
- Locations
- Users
- Roles
- Opt-ins

The following membership and follow-list rules are locked. Detailed permissions and storage relationships remain to be specified.

## Locked: barn membership and show follows

Owner approval: “lock,” following the Castlewood/trainer/horse discussion on October 9, 2026.

- **Barn entity:** Castlewood groups its linked horses, riders, staff, locations, roles and opt-ins. Assigned users have access within that entity according to their roles.
- **Horse association is not ownership:** Castlewood may add horses it manages or cares for even when it does not own them. Actual ownership is recorded separately from the barn association.
- **Barn lane:** Castlewood members receive Castlewood's horses and activity, subject to their roles.
- **Personal follow lane:** Each trainer or rider may maintain an additional follow list. Following does not grant private access to another barn's data.
- **Show grouping:** Show entries are grouped by trainer, not by barn name. Castlewood's three trainers connect the barn to those trainer-grouped entries.
- **Main trainer:** The entity owner may designate a main trainer.
- **Unaffiliated trainers:** The entity owner may follow an unaffiliated trainer without making that trainer a Castlewood member. This adds that trainer's show horses only; the unaffiliated trainer's riders do not appear. Individual horses from this list can be unfollowed.
- **Main trainer/entity owner:** Their show entries supply all their horses and all their riders. The entity owner can disable individual horses or riders from the barn's show follows. Disable retains the record and permits reactivation.

## Locked: riders, staff and grooms

Owner approval: “lock this,” following the rider/staff/groom clarification on October 9, 2026. These rules refine the earlier general follow-list discussion; do not use that earlier wording to include riders from unaffiliated trainers.

- **Rider appearance:** Riders associated with the main trainer/entity owner's show entries appear in the barn's show data. Appearance alone does not automatically grant user access.
- **Rider favorites:** Riders enabled within the entity may maintain their own personal favorites list.
- **Staff:** A staff member may also hold a rider role.
- **Grooms:** Grooms do not hold a rider role. They can view only what is specifically assigned to them; barn-wide inclusion does not override this restriction.
- **Permissions still unspecified:** No additional staff access rules or groom favorites permissions are established by this lock.

These decisions do not select storage fields, identity-matching methods or refresh rules. Follow-list precedence, exclusions across overlapping follows and carryover between show weeks are not yet specified.

## Shared horses and riders

Horses and riders are exposed for several reasons. The barn/follow and role rules above are locked; the remaining shared-entity handling is not yet locked. “Exposed” is the owner's term here; it does not by itself establish public access.

Horses are used across packing, tack, WEF, WEC and other services. The stated intent is to reuse horse data rather than continually recreate or duplicate it inside each service, adding unnecessary weight.

Do not apply horse inclusion rules indiscriminately to riders: an unaffiliated trainer contributes horses only, while the main trainer/entity owner contributes both horses and riders.

The trainer-based show grouping and two follow lanes are locked above. How WEF-specific horse/rider records match the shared entities remains open. No matching, import, deduplication or storage method has been selected here.

## Admin rings and shows

- Rings and shows are managed by the owner/admin.
- Rings are exposed by show; the exact behavior will be explained later.
- Shows in this discussion are WEF-specific and entered by week; further details are pending.

## WEF show-engine

The show-engine discussed here is **WEF-specific**. Do not apply its processing logic to WEC by inference.

The first outline identified classes-data, entries-data and live-data.

The expanded outline identifies groups, classes, entries and rings, prepared by schedule as static data. For these areas, **live feeds take precedence over prepared static data**.

The precedence rule is stated; freshness thresholds, fallback timing and field-level merging have not yet been defined.

## Engine inputs and outputs

| Engine / function | Stated input | Stated purpose / output |
| --- | --- | --- |
| rings-engine | Live data | Produces ring-related output. |
| Class-time and go-times | Live data | Produce their own timing output. Whether these are separate engines is not yet specified. |
| alerts-engine | Live data and ring output | Produces its own alert output. |
| results-engine | Live data, rings and alerts | Triggers when necessary to produce its own results output. |
| next-engine | Not yet specified | Sets up for tomorrow. |
| publish-engine | Not yet specified | The initial outline associates this with schedules delivered through endpoints; the expanded explanation has not yet detailed it. |

These describe data dependencies, not a required agent hierarchy or an instruction to build a new implementation.

## Decisions still open

1. Webflow static / Astro1 / Astro2 responsibilities.
2. Shared versus WEF-specific horse/rider identity matching and storage; barn and follow-list behavior is locked above.
3. Remaining reasons and rules for rider/horse exposure.
4. Show-specific ring exposure and weekly WEF show entry details.
5. Exact storage boundaries, identifiers and entity relationships.
6. Detailed timing, triggering, next-day preparation and schedule-publishing behavior.

## Existing context to retain

- Earlier shared persistence/caching discussion: `docs/coordination/shared-data-behavior-contract.md`. This working map does not revise that contract.
- Current Recognize field audit: `docs/coordination/recognize-fields-links-audit-20261009.md`.
- Completed field cleanup record: `docs/coordination/recognize-fields-cleanup-20261009.md`.
- Existing working implementations are evidence to inspect and reuse, not permission to reinvent methods. The map above does not authorize rebuilding them.

Next discussion: the owner's remaining headwind, preserving the locked barn, follow-list and role rules above.
