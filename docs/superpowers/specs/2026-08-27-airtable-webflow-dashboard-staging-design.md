# Airtable to Webflow Dashboard Staging Design

## Objective

Use Airtable as the calculation and staging system for Stats slices and selected Class evidence while retaining the existing Webflow collections. Do not create additional Webflow collections. Lainey is the aggregate subject; Year, Horse, and optionally Competition are scopes over her Classes.

## Source model

The Class is the atomic performance fact for one rider, Lainey. Each qualifying Class has one Horse, one Competition occurrence, one Year, one Result, one Height, one Discipline, and usually one Skill. Lainey, Horses, and Competitions are aggregate perspectives over those Class facts.

About/profile content, transcripts, videos, and `Pgs` remain outside the performance hierarchy. Videos may reference a Class, Horse, and optional Skill, but do not define performance truth.

The presentation model has two drill paths over the same Class facts:

- Lainey -> Competition -> Classes -> Class evidence.
- Lainey -> Horse -> Classes -> Class evidence.

Competitions organize chronology. Horses organize relationship/story arcs. Classes supply evidence. Stats summarizes a selected path.

## Airtable staging model

Create exactly two Airtable tables in base `appUGgVeAZFae3tEb`:

1. `webflow_stats_stage`: one row per aggregate slice: all-time Lainey, Year, Horse, or optional Competition.
2. `webflow_classes_stage`: one row per Class selected for Webflow.

The source Class records retain two independent controls:

- `include_in_aggregates`: determines whether the Class contributes to dashboard totals.
- `publish_class_to_webflow`: determines whether a Class receives a staged Webflow Class item.

Removing a Class from Webflow must not remove it from aggregate calculations unless `include_in_aggregates` is also false.

## Aggregate contract

Every Stats-stage record carries nonnegative integer counts for summary totals, results, years, heights, disciplines, and skills. Canonical keys are:

- Summary: `classes_count`, `horses_count`, `competitions_count`, `years_count`.

- Results: `results_count`, `wins_count`, `second_count`, `third_count`, `top3_count`, `top8_count`.
- Years: `years_count`, `year_2024_count`, `year_2025_count`, `year_2026_count`.
- Heights: `heights_count`, `height_2ft_3in_count`, `height_3ft_3in_count`, `height_2ft_9in_count`, `height_3ft_6in_count`, `height_3ft_9in_count`, `height_3ft_count`, `height_2ft_6in_count`.
- Disciplines: `disciplines_count`, `hunter_count`, `jumper_count`, `equitation_count`.
- Skills: `skills_count`, `ushja_jump_seat_count`, `usef_medal_count`, `this_childrens_medal_count`, `uset_talent_search_count`, `nhs_hamel_count`, `aspca_maclay_count`, `age_group_eq_count`, `premier_cup_count`, `wihs_count`.

Aggregates are recomputed from qualifying source Classes; stored counts are never incremented blindly.

## Identity and change detection

- `scope_type` is one of `all`, `year`, `horse`, or `competition`.
- `scope_key` is stable and unique within its type, such as `lainey`, `2026`, `owin`, or `wef-1-2026`.
- `source_record_id` identifies the Airtable source record for Horse or Competition scopes and is blank for the all-time scope.
- The staging record ID is written to Webflow `rec-id` and is the sync identity.
- `webflow_item_id` stores the returned Webflow item ID.
- `payload_hash` is computed from the normalized Webflow payload; unchanged hashes skip writes.
- Class updates identify both old and new Horse/Competition scopes so moves recalculate both sides.

## Webflow policy

- Reuse existing Stats, Laineys, Horses, Competitions, Classes, Videos, and `Pgs` collections.
- Create no new Webflow collections.
- All initial writes are staged/draft-only.
- Publishing requires separate explicit approval and is a separate workflow action.
- Existing CMS bindings and fields are not removed until their replacement bindings pass readback.
- Route aggregate slices to the existing Stats collection.
- Keep Laineys focused on profile/about/resume and as the story entry point; its schema mutation remains disabled during the initial pilot.
- Keep Horses and Competitions as relatively thin navigation/story entities rather than duplicating every aggregate field on them.
- Route selected factual evidence to the existing Classes collection.

## Runtime ownership

Codex may inspect, implement, and verify the repeatable sync code. A configured runner or scheduled workflow owns recurring execution. Manual calls are diagnostic and do not count as cadence proof.

## Acceptance gates

1. Pure aggregate tests cover every category and update/move edge case.
2. Staging upserts are idempotent.
3. One Horse-scope Stats pilot matches Airtable source Classes and Webflow staged readback exactly.
4. One Year-scope Stats pilot matches exactly.
5. Class exclusion changes Webflow Class staging without changing aggregates.
6. No publishing occurs during implementation or pilot verification.
7. Full reconciliation reports zero mismatches before any legacy binding or Class-item removal.
