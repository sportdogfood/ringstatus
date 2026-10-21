# WEF tenants — decisions and field discussion

Decision date: **20 October 2026 (America/New_York)**.

## Confirmed owner decision

Keep the fabricated CWF walking times for now. Do not replace them with measured times or build walking paths at this stage. Keep every `ring-walk-is-example` flag checked. These minutes are demonstration data; retaining them does not make them measured estimates.

The decision is also recorded in the CWF tenant's Airtable notes.

| Ring number | Source ring ID | Ring | CWF example minutes |
| --- | --- | --- | --- |
| 1 | 10 | Equestrian Village Derby Field | 8 |
| 2 | 3 | Annex Derby Field | 10 |
| 3 | 44 | VanKampen Covered Arena | 6 |
| 4 | 13 | Equestrian Village International Ring | 12 |
| 5 | 48 | GDF Hunter Ring 1 | 14 |
| 6 | 45 | GDF Hunter Ring 2 | 16 |

## Verified present state

Base: `wef-schedules` / `appUq5Qz9kARqFija`.

- tenants: `tblWxfcCx2FKR7AEU`; CWF record `rec7Q4OoHsaGTHKUb`.
- Existing tenant fields: tenant-name, tenant-key, notes and reciprocal rings-helper links.
- CWF's current key and display name are both CWF. No equivalence to any existing global tenant ID is assumed.
- rings-helper: `tblgFqVUAx7QlbnK8`; six existing records retain their example walking minutes, example flags and walk-tenant links to CWF.
- show-details and daily rings/schedules are already linked. Tenants do not yet have a show association or roster links in this independent base.

## Tenant design principles for the next discussion

Tenant means the barn/business container. Keep tenant identity durable across shows. The show engine's default show/date does not prove a tenant is attending that show.

Keep the existing tenant-key, tenant-name and notes. If CWF already has a canonical RingStatus tenant identifier, verify and reuse/map it rather than invent a competing identity. No canonical tenant, membership, role, consent, runtime or specialist workflow is changed by this discussion.

Candidate links depend on the next operational step:

- **Show participation:** link a tenant to the shows it actually attends; show metadata stays in show-details.
- **Show-barn location:** belongs to the tenant at the relevant show/venue, with optional coordinates later. It must not be confused with the permanent barn/business identity.
- **Horses and riders:** reuse established identities/tenant associations when matching a tenant's classes. Missing today's entries must not remove tenant roster membership.
- **People:** reuse existing People/membership ownership if the next stage needs users or contacts; do not create a second access or consent system here.
- **Walking baseline:** currently a deliberately temporary CWF-only ring-helper value. Before multiple tenants need different times to the same ring, define tenant + starting location + ring walking records. Do not overwrite CWF minutes to represent a second tenant. No extra walking association table is created yet.

Owner answered **Both together** on 20 October 2026: determine show-barn location and the horse/rider associations needed for class selection in the same next stage. Candidate fields/links remain proposals; this answer does not create or deploy a new data pipeline.

## Existing identity and relationship evidence — 20 October 2026

Read-only Airtable inspection found the existing CWF tenant in the Tenants base `app7BXcw801n19QKF`, table `tbled1saw01Thffs1`, record `recZ9MyqRplHD9AY8`: tenant_id 8778, tenant_official_name Castlewood Farm, tenant_accronym CWF, tenant_active checked. Its retained source_record_id is recUKyrLUr0khLCjV in the prior source base. The independent WEF tenant is still rec7Q4OoHsaGTHKUb and has not yet been mapped through a numeric tenant-id field.

The existing tenant has linked shared horse, rider and trainer records. These are reusable source references, not proof of current-show attendance or a verified up-to-date selected roster. The newer tenant-trainers, tenant-roster and tenant-locations association tables returned no records linked to this specific tenant. Do not infer that CWF has no horses/riders/locations: the older shared links exist. Do not backfill associations or repair records under this design task.

Current Tenants system checkpoint was read. Its completed identity/profile work and outstanding unrelated UI gates are outside this WEF design task; none are resumed.

## Proposed minimum structure for both goals

| Record | Fields / links | Ownership |
| --- | --- | --- |
| WEF tenant identity | Existing tenant-key CWF, tenant-name; proposed tenant-id 8778 and source tenant record reference | Reference the established Castlewood Farm identity; do not invent another tenant ID |
| Tenant at a show | Tenant link + show-details link; show-barn name/location reference; participation state; optional barn latitude/longitude later | Show-specific association, so changing shows does not change the permanent tenant identity |
| Tenant's horses/riders | Reuse existing shared horse_id/rider_id records and tenant/trainer relationships; record explicit inclusion/exclusion and the source trainer association | A horse/rider may remain associated with CWF when absent from today's entries. Primary trainer provides horses/riders; additional trainer provides horses only under existing rules |
| Matching class entries | Show ID + horse/rider/entry identifiers + source class ID, linking to collected schedules | A later normal-run entries stage is required; current class schedules alone do not identify individual horses/riders |

Walking values remain the six existing CWF examples. No path calculations or invented coordinates. Before adding another tenant's walking baselines, define the tenant/start-location/ring association discussed above.

Location clarification was subsequently resolved below. Existing trainer/roster inclusion still requires source verification before enabling any new collection stage. The identity/relationship investigation above made no new fields, tables, workflow changes, manual collector runs or canonical record edits.

## CWF home starting point saved — 20 October 2026

Owner explicitly instructed using the home details from the supplied Google Maps link. Opened RingMap, selected its `home` entry (displayed DMS 26°37′59.8″N, 80°16′38.0″W), and copied the selected pin's decimal coordinates from its Google Maps location URL: **26.633278, -80.277224**. These are the pin coordinates, not the map viewport center.

Saved on the existing independent WEF CWF tenant record rec7Q4OoHsaGTHKUb:

- barn-name = home (fld6cIs5N8Q2dX3D6).
- barn-lat = 26.633278 (fld3wdNj7mRDO19tV, six decimal places).
- barn-lon = -80.277224 (fldL6rj0tb2e8iTyK, six decimal places).
- barn-map = the exact selected Google Maps pin URL (fldzpvB3LlmrNE3i4).
- Existing notes retain the earlier decision and append the owner selection, coordinates, source and date.

This is CWF's default starting point. A separate show-specific location override remains a future design step; no tenant-show table or attendance assertion was introduced. The global Tenants base and its existing CWF record were not edited. All six example walking times/flags, tenant key/name and ring-helper links were read back unchanged. No routes, calculated distances, scheduler/code changes or manual collector runs.
