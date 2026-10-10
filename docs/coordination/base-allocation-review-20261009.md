# Base-by-base data allocation — review

October 9, 2026. Proposed allocation of the documented RingStatus structure. This is the review map requested by the owner, not approval to create 16 physical bases. All existing/candidate names remain visible until the owner decides merges or removals.

| Base / candidate name | Data that belongs here | Current state | Boundary or decision |
| --- | --- | --- | --- |
| agents | Agent/task registry, skills and connector references, project/base register, decisions and agent failures. | Existing base. | Operational coordination data; no business horse/rider/show records. |
| recognize | IAM index, recognized devices/cookies, sessions and recognition footprints; references to people and their authoritative opt-in state. | Existing base currently also holds profile/input data. | Target is recognition-only responsibility. Moving current profiles/inputs requires a separate migration; nothing moved here. |
| profiles | Shared barns/tenants, people/users, trainers, riders, horses, staff/grooms, roles, memberships, assignments, stable location references, onboarding, entity history, follows/favorites and authoritative opt-ins. Proposed home for barn-inputs red-line submissions, whole-list audit submissions and admin resolutions. | Proposed base; no separate ID verified. | Shared horse/rider identity reused by packing, tack, WEF and WEC. Show-week entries/results remain in engine bases. Physical split and detailed table layout are for review. |
| wef-engine | WEF weekly shows, trainer-grouped participants, source horse/rider IDs, groups, classes, entries, rings-in-show, trips, daily/live schedules, timing, results and states; rings/class-time/go-time/next-day/publish outputs. | Proposed base; current WEF/legacy storage remains in ringstatus. | Reference shared profile identities. Whether rswef is this same base or a separate staging destination is undecided. Engine names do not each require a base. |
| rswef | WEF show-engine staging data, as named in the earlier input/output outline. | Proposed name; no separate ID verified. | Potential same destination as wef-engine. Listed for comparison only; not a decision to maintain two WEF bases. |
| sms-engine | Alert/message work items, notification-feed, outbound sends, delivery outcomes, responses and processing history. Reference authoritative personal consent and the relevant follows/assignments. | Proposed base role/name. | Could use the existing messages shell. Destination and name remain undecided; no duplicate SMS store is prescribed. |
| messages | Existing empty shell available for the SMS-engine data listed in the sms-engine row. | Existing base; owner confirms empty shell, not a working SMS system. | Review reuse/rename/merge with sms-engine. Consent/profile data has its own authoritative home. |
| rscom | Company/static content and CMS staging for RingStatus. | Existing base; role stated by owner. | Keep separate from show-engine live data. Detailed table inventory and any relationship to rsco remain to review. |
| rsco | Purpose not yet established; do not assign company or coordination data solely from its name. | Existing registered base. | Inspect existing contents before deciding its responsibility or overlap with rscom/agents. |
| ringstatus | Existing legacy and operational data, including current WEF and historical shared data. | Existing base; current consumers preserved. | Source for scoped allocation/migration review, not a second proposed editable master for new profiles or engine data. |
| WEC | WEC-specific source participants, shows/rings/classes/entries and operational engine data. Existing Waze/commenting foundation currently remains here. | Existing base. | WEC logic stays separate from WEF. Shared identities belong to the shared profile model; existing source records are not deleted or rewritten by this proposal. |
| ring-maps | Master geographic places: venues, rings, barns/stabling, gates, parking, coordinates and distances. | Candidate base; separate ID not registered. | Location Layer references these places. Preserve supplied WEF Maps source/IDs; map display is not an engine or new copy of the geography. |
| locations | Horse/person check-ins, current location, movement, ETA context and responsibility changes. | Candidate base; existing foundations overlap Waze. | Reference ring-maps places and shared profile entities. Decide storage overlap with Waze after reviewing existing implementation; do not duplicate place masters. |
| waze | Comments, observations, ring check-ins, scoped ring/class/entry commentary, presets/questions, interaction sessions and footprints. | Candidate separate base; implemented foundation currently uses the WEC dataset. | Reuse existing WEC Comments and Observations code/schema. Physical ownership overlap with locations remains open; not a WEC engine calculation store. |
| tackapp | Tack/packing application data tied to shared horses; exact existing project tables must be mapped from its working implementation. | Existing built concept; separate base ID not registered. | Do not recreate the horse master or assume a new schema. Review its overlap with wec-pak before deciding separate storage. |
| wec-pak | Packing blueprints/source configuration, horse kits, plans, packing modules and print-related state from the existing RSWP system. | Existing built system; separate base ID not registered. | Reference shared horses and reuse existing pak_* implementation. Review overlap with tackapp before deciding physical separation. |

## Cross-base ownership

- Shared horse/rider/person records: proposed profiles home; source participation and source IDs: their WEF/WEC engine home. Barns and personal favorites reference those identities.
- Entity roster and access persist across season/week/day; weekly and daily participation are engine data, not membership revocations.
- Red-line and audit-lane are barn-input forms. Their proposed records live with profiles/inputs and refer to the relevant show/entry data; they do not require separate bases.
- Personal consent/preferences: profiles target. Runtime delivery and notification-feed: SMS destination. Owner-managed groom preferences still reference the actual recipient and assignments.
- Geographic place master: ring-maps candidate. Location/movement observations: locations/Waze allocation still to decide.
- Webflow and Astro consume these responsibilities; Astro1/Astro2 and individual processing engines are not automatically separate Airtable bases.

## Review decisions, not automatic actions

1. rswef and wef-engine: one WEF destination or a justified staging split?
2. messages and sms-engine: reuse the existing empty shell under the chosen name?
3. profiles: approve final shared-data boundary before migrating current Recognize records.
4. rsco: establish contents/purpose before assigning or removing.
5. ring-maps, locations and Waze: retain distinct responsibilities while deciding shared physical storage.
6. tackapp and wec-pak: map existing working tables and decide whether they need separate bases.
7. ringstatus: preserve active legacy consumers until explicit migrations are verified.

## Evidence and scope

Design source: [Barns, show data, inputs and alerts](barn-show-inputs-reference-20261009.md). Earlier target split: [temporary completion sequence](temporary-completion-sequence-20261009.md). Current registry: https://airtable.com/appZahVgD156cMAe3/tbltaHwEyBfcuOHIm.

Full prior descriptions, IDs, links and flags preserved in [registry snapshot](rs-bases-before-allocation-review-20261009.json). This revision updates only the description of each of the 16 registry records; names, IDs, project links and flags are preserved. No physical bases, business tables, application code, migrations, messages or engine runs changed. Built concepts and source-backed historical tests are not fresh runtime certification.

