# Saved participation mapping findings

29 October 2026. Read-only investigation; no product changes, source calls, manual runs or repairs.

Implementation follow-up, 29 October: the owner subsequently approved the narrow matcher. It is implemented locally in participation-linker.js; see ../../MATCHER.md. Ordinary unique show/class/date schedule association is separate and continues when group eligibility is unresolved. The final group rule requires FULL roster equality, not the subset wording in the original proposal below. No production switch occurred.

## Finding

The saved source can support a bounded observed group association, but **own-class trip identity plus class_related_data.class_group_id is insufficient**. A saved same-run counterexample has a group order list inconsistent with that parent group. Validate the entire group order list against the same accepted inventory before selecting a schedule. Never combine eligibility across historical observations.

## Positive saved association

All paths below are relative to the repository.

- Receipt: wef-live/runtime/00912a72-e56f-4c7e-8ebc-c7ee66943700.json, 2026-10-23T19:52:13.082Z through 19:54:12.068Z.
- Class archive: wef-live/runtime/history/class-200030347/9ef05e4b73f0cd56e6e62b9899b7bbae04f8ec0f8c9eef3f1e61ac637e884bd7.json.gz.
- decoded.trips[0] and decoded.class_group_order_of_go.entries[9] match trip 200398136, UUID 85300B7F7F914821A0F7E09E8FAF03DA, entry 200245642, horse 200002216, rider 40889. The OOG row class_id equals decoded.class.class_id 200030347. detail.rows[0].id also matches the trip.
- decoded.class.customer_id=15; show_id=200000075; class_related_data.date=2026-10-23; ring=1; class_group_id=200029518. Ring number 1 is distinct from class.schedule_ring_id 10.
- Same receipt's source-groups archive: wef-live/runtime/history/source-groups/65c736e8d8048ac21c77cdea1281f7158ced19b3130dd1c915e3de1cca1a7f44.json.gz, [0].json_data[0]. It confirms group 200029518, day October 23, ring_number 1, ring_id 10 and classes 200030347/200030362/200030393. Those are the group order list's class identities.
- Existing saved operational reference: wef-live/evidence/ambiguous-schedule-examples.json [9] has active stage-key 200000075|2026-10-23|200029518_770, local Live schedule ID recZE0IZAkr7bG5pQ. [8] is the old dropped group 200027909_770 and must not be selected.
- This proves a saved source association and an existing operational key. The separate schedule reference file is not asserted to be a same-run or current schedule-health snapshot. It is not new adapter deployment proof.

## Same-run counterexample to the simpler rule

- Receipt: wef-live/runtime/a251f69e-3046-4a04-9305-84d7a665575b.json, started 2026-10-24T13:12:12.969Z; class archived at 13:13:18.766Z.
- Class archive: wef-live/runtime/history/class-200030245/068f20d563dbbb31436bd31ba42c7a6f3ebdbfa75ed34106efcc802b8a302705.json.gz.
- decoded.trips[3] and decoded.class_group_order_of_go.entries[8] match own-class 200030245, trip 200398234, UUID D7D2CE6F6DF04B81B646065D98098615, entry 200247138, horse 200005304, rider 70539.
- Parent class_related_data says group 200029469, date October 24, ring number 5. However, the group order list contains classes 200030103/200030104/200030245.
- Same receipt's source-groups: wef-live/runtime/history/source-groups/1795c60b485f139a45ff045319b7252fdbaf53ed0013863dc064a97b0542aac6.json.gz. [0].json_data[13] says group 200029469 contains 200030245/200030015/200030016. [14] says group 200029514 contains 200030245/200030103/200030104. Both are date October 24, ring_number 5, ring_id 48.
- Therefore exact own-class trip matching does not cure the contradictory parent-group context. Reject this observation for schedule attribution. Do not silently substitute group 200029514 using the class roster alone.
- Earlier receipt f4493c70-cdf9-4432-a3b9-ba88e12362fd archived class-200030245/8c15a922b5ac481934d0539ab10ea244783599f069f4a6f823a180d629bdbf60.json.gz at 12:08:41.630Z. It has the same trip and OOG class set but parent group 200029514. Historical observations must not be unioned into current multi-group eligibility.

## Smallest proposed adapter rule

1. Consume one accepted, hash-verified Data revision and its original class/inventory observations; retain provenance and observation time. Do not correlate unrelated captures by file modification time.
2. Require the participation's own class, explicit trip ID/UUID, entry, horse and rider to agree between trips and class_group_order_of_go.entries. Do not assign other classes' OOG rows to the requested class.
3. Require source/customer/show/date/ring-number/group agreement with the accepted inventory. Every OOG row's explicit class must belong to that group. Reject contradictory parent context; roster similarity cannot replace a group identity.
4. Require exactly one active operational occurrence matching that source group/class/date/ring and the source occurrence where available, using stored operational fields/row-json. Copy its stage-key and resolve the destination-local record. Do not synthesize a key, fan out groups or union historic evidence. Multiple indistinguishable occurrences remain unresolved.
5. Missing lists, stale observations, conflicting identity/context or ambiguity preserve previous verified links and report unresolved. A complete empty class remains a valid empty collection; no whole-day-empty inference.

This is a proposed bounded observed-association rule, not proof of universal eligibility. No natural multi-group acceptance has been established. Review against current Data retained input is still required.

## Delta and approval

Proposed code boundary: Entries retained-input projection plus its staged model/collector and offline fixtures. Reuse existing schedule keys and plural-link transport. Add the original source group-order membership and accepted inventory to the input contract, not a new collector endpoint. Optional messages/results/waze remain nonblocking.

Requested implementation approval should cover this narrow adapter, tests using the positive and contradictory saved observations, deployment through the existing route after input/consumer compatibility, and two normal Entries runs. No manual runs or record repairs. CWF reader migration is a separate release dependency and must be separately scoped; it is not an Entries-core selection requirement.

Scan scope: 1,610 saved class archives, 8,802 public detail rows and 8,758 decoded trip rows; 1,608 archives had a group-order list. 821 distinct own-class exact trip identities were inspected; 31 had different same-day parent groups across snapshots. Those historical differences alone are not treated as proof of source error; the concrete same-run contradiction above is the relevant rejection case.
