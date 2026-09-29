# RingWaze Prior-Art Matrix

## Reading and verification status

This matrix records findings made in the referenced patent-analysis conversation. It does not independently re-verify the patents, their prosecution histories, legal status, priority dates, or every disclosure. Before filing or reliance, compare the actual proposed claims against the complete documents and current law.

The prior analysis used these labels:

- **CLAIMED** — identified in a claim of the reference.
- **DISCLOSED IN SPECIFICATION** — identified in the written description but not established as claimed in the discussion.
- **NOT FOUND** — not located in the reviewed material; not a representation that it cannot exist elsewhere in the document.
- **UNCERTAIN** — the discussion did not establish the element confidently.

## Core element key

| Key | Element |
| --- | --- |
| A | External authoritative schedule baseline |
| B | Ordered participant/activity execution structure |
| C | Multiple physically situated users observing actual execution |
| D | Confidence-based reconciliation of conflicting crowd observations |
| E | Historical reporter accuracy used in confidence |
| F | Observations mapped to sequence position/current execution state |
| G | Historical scheduled/authoritative execution information |
| H | Historical crowd-observed execution information |
| I | Future timing or schedule forecasts |
| J | Recalculation from subsequent authoritative or crowd information |

## Element matrix preserved from the prior assessment

| Reference | A | B | C | D | E | F | G | H | I | J |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| US20250124410A1 — Pegasus | NOT FOUND | CLAIMED | NOT FOUND | NOT FOUND | NOT FOUND | DISCLOSED IN SPECIFICATION | NOT FOUND | NOT FOUND | DISCLOSED IN SPECIFICATION | NOT FOUND |
| US11521178B2 — Autodesk | NOT FOUND | CLAIMED | NOT FOUND | NOT FOUND | NOT FOUND | DISCLOSED IN SPECIFICATION | CLAIMED | NOT FOUND | CLAIMED | NOT FOUND |
| US11514341B2 — Azra / sports data crowdsourcing | NOT FOUND | NOT FOUND | DISCLOSED IN SPECIFICATION | CLAIMED | DISCLOSED IN SPECIFICATION | NOT FOUND | NOT FOUND | NOT FOUND | NOT FOUND | NOT FOUND |
| US8718910B2 — crowd-sourced traffic reporting | NOT FOUND | NOT FOUND | CLAIMED | UNCERTAIN | NOT FOUND | NOT FOUND | NOT FOUND | NOT FOUND | CLAIMED | NOT FOUND |
| US20130041941A1 — CMU crowd-sourced transit | DISCLOSED IN SPECIFICATION | UNCERTAIN | CLAIMED | NOT FOUND | NOT FOUND | UNCERTAIN | DISCLOSED IN SPECIFICATION | CLAIMED | CLAIMED | UNCERTAIN |

The exact labels above are retained as conversation findings, not freshly verified conclusions.

## References specifically discussed

### US20250124410A1 — Pegasus, Systems and Methods for Management of Equestrian Events

**What the discussion found:** ordered participants; published schedules; monitoring of participant progress; deviations from order; and adjustment or prediction of later timing in the specification.

**Important correction established in the thread:** “real-time participant progress” concerns competitors executing the event and updates from organizers, stakeholders, or sensors. Where spectators were mentioned, they were described as recipients of role-based notifications, not as a crowd supplying live execution observations.

**Not found in the discussion:** multiple independent spectators reporting witnessed execution facts; conflicts among those reports; reliability selection; or use of accepted spectator observations between official updates.

**Relevance:** strong event-order and downstream-timing component for a §103 combination; not found to anticipate the whole invention.

### US11521178B2 — Autodesk, Techniques for Crowdsourcing and Dynamically Updating Computer-Aided Schedules

**What the discussion found:** an ordered schedule; real-time input from the people conducting or participating in the scheduled activity; historical scheduling information; and downstream adjustment.

**Important correction established in the thread:** the “participants” are meeting attendees, delegates, and the organizer—the people engaged in or controlling the scheduled activity—not a separate crowd observing someone else's execution.

**Not found in the discussion:** a broad non-operating observer population, untrusted witnessed execution facts, confidence reconciliation, or mapping accepted independent observations into an authoritative participant order.

**Relevance:** strong schedule-progression reference for §103; not found to teach the separate observer architecture.

### WO2022026819A1 — Tapstats

**What the discussion found:** multiple spectators make witnessed taps; crowd aggregation/comparison; observer accuracy; rejection or evaluation of bad observations; a fight schedule in the specification.

**Clarification:** the discussion corrected an earlier characterization and did not treat Tapstats as predicting competition outcomes. It primarily aggregates witnessed input and evaluates accuracy relative to crowd results.

**Not found in the discussion:** an independently authoritative ordered execution schedule, mapping observations to the current position in that order, maintaining an earlier official change through later observations, or participant-specific downstream schedule forecasts.

**Relevance:** strong evidence that crowd observation, greater observer coverage, and confidence/accuracy cannot be treated as the novelty center.

### US10220290B1 — ScoreStream

**What the discussion found:** crowd reporting of live sports information and confidence processing. A machine-learning “predicted result” was discussed as part of confidence in a reported score, not as downstream event-schedule prediction.

**Not found in the discussion:** an independently authoritative participant order used to convert accepted crowd facts into participant-position-specific future timing.

**Relevance:** observer and confidence component for §103; not found to disclose the full chain.

### US11514341B2 — Azra, Systems and Methods for Sports Data Crowdsourcing and Analytics

**What the discussion found:** multiple independent reports about a live sports event; incomplete observations; location information; conflicts resolved with additional reports; increased confidence with more users; report frequency and prior misreporting information.

**Not found in the discussion:** an external authoritative schedule/order, current position in that order, historical execution timing, or downstream participant-time propagation.

**Relevance:** particularly important to confidence/reconciliation. It makes a strong combination with CMU transit and Pegasus under §103.

### US8718910B2 — Crowd Sourced Traffic Reporting (Waze-type traffic art)

**What the discussion found:** active reports and passive current traffic information; timeliness; current conditions; combination with other or historical information; and predicted travel times.

**Useful analogy:** users report current real-world conditions and the system makes future predictions. That supports the RingWaze distinction between observed facts and system-generated forecasts.

**Risk created by the analogy:** “Waze for events” is not a sufficient patent boundary. An examiner can argue that applying current-condition crowd reporting and history to future event timing is predictable.

**Not found in the discussion:** an authoritative event participant order, current execution position within that order, or different execution times propagated to multiple ordered participants.

### US20130041941A1 — CMU Crowd-Sourced Transit

**What the discussion found:** a transit-agency schedule; real-time data from multiple riders' devices; historical models from prior trips; schedule-based model updates; and predicted arrival/departure times.

**Why it is especially important:** it approaches the abstraction “official schedule + crowd + history + updated prediction.” That abstraction is therefore unsafe as the patent center.

**Not found in the discussion:** an authoritative participant/activity execution order comparable to a class order of go; semantic observations of independent participant execution; competing sequence states; reporter-history selection among those states; accepted observations anchored to participant positions; inferred participant progression; and participant-specific execution times propagated through the field.

**Relevance:** closest architecture-level concern and a principal §103 building block.

### US20130321388A1 — CBS live-event reporting

**What the discussion found:** local events may lack personnel for live updates; members of the public attending an event can provide substantially real-time information; multiple users may report simultaneously.

**Not found in the discussion:** an external authoritative participant sequence, use of reports to establish current position in that sequence, or downstream schedule forecasting.

**Relevance:** the motivation “spectators provide coverage that limited personnel cannot” already exists. Speed and coverage cannot carry patentability alone.

### US20190374839A1 — Fanmountain

**What the discussion found:** multiple observers; locations and timestamps; current competitor; order or sequence of performers; “time until up”; next-event information; real-time crowd input; and confidence concepts in the specification. The identified claims focused on spectator judging/scoring with server synchronization and authorized users.

**Not found in the discussion:** a spectator observation such as `entry_on_course = 456` being accepted, located in an independently authoritative order, used to maintain the current effect of an official change, and propagated into downstream participant timing.

**Relevance:** structurally closer live-event art; important to review carefully, but not found to contain the complete required relationship.

### US5,526,479 — live-event observer art

**What the discussion found:** a human observer enters witnessed actions into a computer as a live event represented by an ordered sequence of sub-events occurs.

**Not found in the discussion:** independently authoritative schedule/order, broad multiple-observer layer, authoritative schedule changes, confidence reconciliation, historical execution-duration modeling, maintenance of a delay through later execution facts, or participant-specific downstream timing.

**Relevance:** demonstrates that “a person reports what is witnessed during a live event” is old and should not be claimed as the inventive center.

## Additional references discussed

| Reference | Conversation-level relevance | Limitation noted in the discussion |
| --- | --- | --- |
| US9516460B2 — checkpoint condition sharing | Crowd/current-condition and possible confidence or user history | No authoritative ordered participant execution structure found |
| US20150081348A1 — crowdsourced wait times | Freshness, location, history, confidence, outlier rejection, future waits | No ordered authoritative execution structure found |
| US7199715B2 — tracking ID tags | Inference of missed intermediate states | Does not supply the RingWaze crowd/authoritative/timing chain |
| US20140165140A1 / US9646155 — reference-sequence evaluation | Ordered sequence and state evaluation | Does not supply physically situated crowd observation and participant timing chain |
| CA2784572A1 — incomplete process traces | Inferred activity from incomplete traces | Sparse reconstruction cannot carry novelty by itself |
| US11526916B2 — intelligent queue prediction | Historical/current wait-time prediction | No authoritative participant order and crowd-to-order state chain found |
| US20170147925A1 — event milestone timing | Historical milestone prediction | No broad observer/reconciliation/order chain found |
| US7171375B2 — schedule buffers | Ordered scheduling and downstream timing effects | No independent crowd observation layer found |
| US7379782B1 — manufacturing throughput | Separates productive execution from delay/downtime | Interruption handling is supporting, not a novelty center |
| US20260245024A1 — planned versus observed prediction/deviation | Planned/observed state and future timing | Prior-art status depends on filing dates and statutory rules; do not assume from publication date alone |

## Strongest combination risk

The most serious combination identified is:

```text
CMU transit or Waze
  live/current crowd evidence + history + future timing

Azra / ScoreStream / Tapstats
  multiple sports observers + conflicts + confidence/reliability

Pegasus or Autodesk
  authoritative or published order + progress + downstream schedule adjustment
```

The working defense must therefore be the mandatory interaction, not the presence of the individual components.

## Current single-reference conclusion

Within the reviewed set, the discussion did not find a single reference containing the entire current RingWaze boundary arranged as required. That finding is provisional and claim-dependent. A fresh professional search and direct review of the actual documents remain necessary.
