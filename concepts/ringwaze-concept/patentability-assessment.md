# RingWaze Patentability Assessment

## Scope, status, and limits

This is a working assessment derived from the referenced conversation. It is not legal advice, a patentability opinion, a freedom-to-operate analysis, or a guarantee of allowance. The conclusions remain dependent on exact claim language, specification support, filing and priority dates, the legal status of each reference, and a current search of patents and non-patent literature.

## Assessed invention

The assessment applies only to the boundary in [invention-boundary.md](invention-boundary.md), especially:

> An authoritative ordered event schedule receives intermittent authoritative changes; later accepted observations of actual execution from a broad, physically situated, non-authoritative observer population keep the temporal consequences of that official state current; and the refreshed state, with historical execution information, produces position-dependent timing for multiple downstream participants.

It does not assess the RingWaze product as a whole.

## §101 — Patent eligibility

### Current posture: viable, claim-sensitive

At a high level, the concept can be characterized adversely as:

> receive information → evaluate information → predict future information.

That framing creates eligibility risk for a software/data-processing claim if the implementation is expressed only as results on generic computers.

The stronger framing is a concrete machine-maintained state process:

- maintain a canonical authoritative ordered event structure;
- receive asynchronous location- and time-associated evidence of external execution;
- evaluate competing candidate execution states under defined reliability constraints;
- accept observations as anchors tied to positions or conditions in the authoritative structure;
- preserve authoritative, observed, inferred, and calculated provenance;
- update execution pace while treating interruptions separately where applicable;
- propagate the accepted current state through dependent downstream participant positions;
- repeat when qualifying authoritative or observed evidence changes.

This does not guarantee eligibility. It is materially stronger than claiming “collect crowdsourced data and predict event times.”

### Drafting implications

- Claim the state-maintenance and propagation relationship, not only the desired timing result.
- Tie observation acceptance to the independently authoritative ordered structure.
- Describe how asynchronous, incomplete, and conflicting evidence changes a machine-maintained event state.
- Avoid relying only on generic processors, generic databases, or generic mathematical output.

## §102 — Novelty

### Current posture: provisionally favorable within the reviewed set

The discussion did not identify a single reference that disclosed the complete current combination arranged in the required relationship.

The closest abstraction-level concern was CMU transit, US20130041941A1, which was found to combine a transit-agency schedule, crowd-derived live data, historical trip information, and updated arrival/departure predictions. That means the following is too broad to serve as the patent center:

> official schedule + physically present crowd + history + updated predictions.

The distinctions not found together in CMU transit were the authoritative participant/activity order, semantic observations of independent event execution, conflicts producing competing sequence states, reliability selection among those states, anchoring an accepted observation to a participant position, and propagating participant-specific timing through the remaining ordered field.

Other reviewed references disclosed important parts, but the discussion did not find one containing all of these together:

- an independently authoritative ordered event structure;
- an observer population not restricted to operators or scheduled participants;
- reports of actual execution between official updates;
- confidence filtering of the resulting untrusted observation stream;
- accepted observations attached to the authoritative order;
- maintenance of the continuing effect of an authoritative change;
- historical execution information; and
- position-dependent expected times for multiple downstream participants.

“No single reference found” is a provisional search result, not a final novelty conclusion.

## §103 — Non-obviousness

### Current posture: primary risk; defensible argument depends on the exact interaction

The individual ingredients are substantially represented in the reviewed art:

- ScoreStream, Tapstats, Azra, CBS, and Fanmountain address crowd or spectator input in live sports/event settings, with varying confidence, location, scoring, or current-event functions.
- Pegasus and Autodesk address ordered event/activity progression and schedule changes or downstream timing.
- Waze/traffic, CMU transit, and wait-time art address live crowd evidence, historical information, and updated future timing.
- Other process and tracking references address missed intermediate states, incomplete traces, schedule propagation, and separation of productive time from interruption time.

A realistic examiner theory discussed in the thread is:

> CMU transit or Waze supplies live crowd evidence plus history and timing prediction; Azra, ScoreStream, or Tapstats supplies multiple sports observers and confidence/reliability; Pegasus or Autodesk supplies the ordered event schedule, actual progression, and downstream schedule adjustment.

The strongest specific three-reference combination identified was:

1. US20130041941A1 (CMU transit): schedule + crowd-derived live state + historical data + updated prediction.
2. US11514341B2 (Azra): multiple independent sports observers, conflicting reports, corroboration/confidence, location, and misreporting history.
3. US20250124410A1 (Pegasus): published participant order, progress relative to the order, deviation, and adjustment of later timing.

### Strongest defense identified

The defense is not that the pieces were unknown. It is that the reviewed references did not establish the same functional dependency:

> An authoritative source defines and changes the official ordered event structure; a broad independent crowd supplies later witnessed execution facts between those updates; reliability processing decides which evidence may alter the current execution position within that official order; accepted facts keep the timing consequences of the official state current as execution proceeds; and that current state is propagated differently to multiple downstream participants according to their positions.

The post-authoritative-update relationship is especially important:

> Official delay or hold → later accepted facts of execution → repeated refresh of the official change's continuing timing effect → different updated times for downstream participants.

The claim should make this chain mandatory. A list of coexisting modules—crowd, schedule, history, prediction—is more vulnerable.

### What cannot carry non-obviousness alone

- a large observer population;
- increased speed or coverage;
- crowd confidence or reporter history;
- observers reporting facts instead of predictions;
- sparse sequence reconstruction;
- historical pace blending;
- separating interruption duration from normal throughput;
- generic participant ETA calculation.

These can strengthen the integrated disclosure, but the conversation identified substantial adjacent art for each.

## §112 — Written description, enablement, and definiteness

### Current posture: supportable if the application deliberately teaches the full mechanism

The largest §112 risk is claiming the strongest relationship more broadly than the filed specification actually supports.

The specification should expressly describe:

- authoritative event structures and updates;
- observer/location association;
- structured and free-form observations of actual execution;
- conflicting, stale, or incomplete evidence;
- acceptance, rejection, deferral, weighting, and competing candidate states;
- how an accepted observation becomes a timestamped execution anchor;
- association with participant positions, transitions, interruptions, and activities;
- how later observations update the effect of an earlier official delay, hold, move, or other change;
- historical trip, transition, interruption, event, and location timing;
- at least one complete calculation from inputs to multiple participant-specific outputs;
- recalculation after later authoritative information or accepted observations;
- provenance distinctions among authoritative, observed, inferred, and calculated state.

### Enablement

Avoid result-only disclosure such as “AI determines accurate times.” At least one working implementation should be described. The conversation identified possible models including deterministic sequence calculations, weighted moving averages, median observed pace, historical/live blending, Bayesian or confidence weighting, and machine-learning models. These are examples to disclose, not established claim requirements.

### Definiteness

Terms such as “broadly available,” “physically situated,” “real time,” “continuously,” and “operationally stale” should be objectively defined or expressed as concrete operations. See [invention-boundary.md](invention-boundary.md#objective-terminology-for-eventual-drafting).

## Utility

### Current posture: straightforward

The system has a specific and substantial operational use: maintaining current event and participant timing so participants and event operators can act on more accurate information. Utility was not identified as a meaningful risk.

## Present conclusion

| Requirement | Present working view |
| --- | --- |
| §101 | Viable but sensitive to whether the claim recites a concrete state-maintenance process rather than information collection and forecasting in the abstract. |
| §102 | Provisionally favorable against the reviewed references for the complete interaction; further searching and exact claim comparison are required. |
| §103 | The principal patentability risk. The best argument is the required functional chain that keeps an authoritative ordered schedule temporally current and propagates participant-position-specific timing. |
| §112 | Likely supportable only if the specification expressly teaches each part of the mechanism and at least one complete implementation. |
| Utility | Straightforward. |

The strongest present center should be preserved without broadening it into the overall RingWaze product or weakening it into generic crowd-assisted prediction.
