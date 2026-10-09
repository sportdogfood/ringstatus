# Project map

Owner assessment, 8 October 2026. Maturity labels are the owner's portfolio view, not fresh runtime certification.

| Category | Projects / work |
|---|---|
| Core projects | Show-engines, RingStatus, WEC |
| Built; work needed before release | Full packing app; small tack packing app |
| Existing details and proof work | RingWaze; Location Layer; WEF Ring Maps |
| Needs completion | SMS engine; Recognize; user input; Barn onboarding; Schedule |
| Largest rebuild | WEF show-engine |
| Shared foundation | Documented connections and skills |
| Unresolved operating model | Best use of agents/subagents; no proven general autonomous controller |
| Major hardware change | Owner's term: Surface Ultra 128GB; existing Surface Studio. Model/specification/purchase details not verified. |

Current selected build priority is Barn setup. No relative ordering of the remaining projects has been approved. Do not turn this map into new execution assignments.

## Boundaries

- WEF, WEC and global/input-output responsibilities stay distinct. WEF and WEC can use similar stage names with different logic.
- WEC is owner-reported on Zoho Catalyst; WEF and an existing heartbeat are owner-reported local. These hosting statements have not been freshly audited. The heartbeat was discussed as a possible supervisor trigger, not implemented by this chat.
- WEF Maps / `ring-maps`: where places are; geographic master for rings, barns, gates, parking, coordinates and distances. **ChatGPT Maps** is its named display tool.
- Location Layer: where horses/people are and where they are going; movement/check-ins, ETAs and responsibility context. It can consume WEF Maps references without taking ownership of that project.
- RingWaze: human observations, comments and check-ins. Existing WEC-backed work is reusable system work, not proof of a complete location tracker or engine timing feedback loop.
- Recognize/user data: shared entities, onboarding inputs, identity mappings, memberships, assignments and subscriptions. Engine schedules/timing/results stay separate. A stored subscription is not itself proof of consent enforcement or sending behavior.
- `tackapp` and `wec-pak` are existing built concepts. Missing separate base IDs never means the applications are unbuilt. WEC packing has a concrete overview/code reference; the exact scope of the tackapp alias versus the tack-horse connector remains unverified.

## Base registry

| Registry name | Registered base ID |
|---|---|
| agents | appZahVgD156cMAe3 |
| recognize | app9kOZdIaGyKk5uG |
| messages | apphwDLkVCp8ut2Hd |
| ringstatus | apptdhhNzduxm5gjn |
| WEC | app6XS1RvsPNRT6os |
| waze, locations, ring-maps, tackapp, wec-pak | No separate base IDs registered at this handoff |

Bases loosely correlate with projects, many-to-many. Base existence, built code/concepts, historical tests and current live proof are different facts. Reuse existing implementation before deciding final physical table allocation; no migrations or duplicate systems were authorized by the registry review.
