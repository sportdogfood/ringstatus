# RingWaze Patent Documentation

## Purpose and package status

This folder packages the current RingWaze patent analysis separately from the broader RingWaze product concept.

The documents record the invention boundary and the analysis established in the source conversation. They are working materials for patent counsel, not a filed application, legal opinion, patentability guarantee, freedom-to-operate opinion, or substitute for a current professional prior-art search.

## Controlling patent center

> Maintaining an authoritative ordered event schedule as an operationally current participant-specific timing system by receiving independently witnessed execution facts from a broad population of physically situated observers between intermittent authoritative updates, evaluating those observations for reliability, applying accepted observations to the authoritative schedule, and using the resulting current execution information together with historical execution data to repeatedly update expected execution times for downstream participants.

The strongest present emphasis is the complete relationship:

1. An external authoritative source defines the official event structure, schedule, participant order, and official changes.
2. Authoritative updates can be intermittent. A valid official delay or hold can become operationally stale as execution continues.
3. A population of physically situated observers, not restricted to a predetermined set of competitors, organizers, officials, vendors, or event operators, reports witnessed execution facts.
4. Observers do not predict the schedule or downstream timing.
5. The system evaluates potentially conflicting, stale, incomplete, or inaccurate reports and accepts qualifying observations as execution evidence.
6. Accepted observations become timing anchors within the authoritative ordered structure.
7. The system combines those anchors with authoritative information and historical execution information.
8. It repeatedly recalculates expected execution times for different downstream participants according to their positions in the authoritative order, without requiring a new official update for every execution transition.

## Critical distinctions

The proposed invention is not any one of the following by itself:

- the RingWaze app or RingStatus platform;
- generic crowdsourcing;
- spectators reporting a sporting event;
- generic confidence, reputation, or voting;
- generic schedule adjustment;
- generic ETA prediction;
- sequence reconstruction;
- reporter ranking;
- a list of observation labels;
- horse-show software as a category.

The crowd does not replace the authoritative source. The official schedule and order remain the controlling structure. Crowd evidence supplies higher-frequency facts about actual execution between official updates.

## Documents

- [ringwaze-overview.md](ringwaze-overview.md) — the broader app and product concept, with patent-core and product-only functions labeled.
- [invention-boundary.md](invention-boundary.md) — the frozen patent boundary, inputs, processing relationship, outputs, and exclusions.
- [patentability-assessment.md](patentability-assessment.md) — the current, qualified posture under 35 U.S.C. §§ 101, 102, 103, and 112.
- [prior-art-matrix.md](prior-art-matrix.md) — the references discussed and the element-by-element distinctions established in the conversation.
- [examples.md](examples.md) — concrete horse-show examples showing observed facts, authoritative facts, historical information, and machine-generated timing as separate categories.
- [specification-support-checklist.md](specification-support-checklist.md) — disclosure needed to support the present boundary under §112.

## Current posture in one page

| Requirement | Current conversation-level assessment | Qualification |
| --- | --- | --- |
| §101 | Viable, claim-sensitive | Strongest when claimed as a concrete machine-maintained event-state process, not merely collecting data and predicting times. |
| §102 | Provisionally favorable | No single reviewed reference was found to disclose the entire required relationship arranged as presently defined. This is not a final search conclusion. |
| §103 | Primary risk | An examiner can plausibly combine crowd-observation, ordered-event scheduling, confidence filtering, and ETA/history references. The defense depends on the particular interaction, not the individual ingredients. |
| §112 | Supportable if fully disclosed | The specification must expressly teach authoritative state, observation acceptance, location association, post-update timing refresh, historical execution models, and participant-position-specific propagation. |
| Utility | Straightforward | The system has a specific operational use in improving current participant and event timing. |

## Drafting guardrails

- Keep authoritative facts, observed facts, historical information, inferred/calculated state, and predicted timing distinct.
- Do not say the crowd predicts that an entry is on course. `entry_on_course = 456` is a reported observation.
- Do not rely on “more users” or “faster crowd” as the novelty center. The broader population explains the architecture and the need for confidence filtering, but crowd sports reporting is known.
- Do not rely on sparse sequence reconstruction, interruption handling, or reporter accuracy alone. Each has relevant prior art.
- Do not reduce the independent concept to “schedule + crowd + history + prediction.” The CMU transit reference is uncomfortably close at that abstraction level.
- Preserve the post-authoritative-update relationship: accepted subsequent execution observations keep the temporal consequences of an official change current.
- Preserve participant-position-specific propagation. The output is not only a revised class start; it includes different expected times for multiple downstream participants.

## Source boundary

This package summarizes the referenced “Patent Analysis Assessment” conversation. Patent-reference findings are recorded as the findings of that discussion and should be rechecked against the actual claims, specifications, prosecution histories, priority dates, and the eventual claim language before reliance or filing.
