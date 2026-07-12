# RingStatus ChatGPT and Codex Operating Package

Version: `2.0`

Revised: `2026-07-12`

Source task: `019f4240-49ce-78e3-b89a-13d3a19cb02b`

Status: `REVIEW REQUIRED`

## Purpose

This package defines how RingStatus workflow decisions, Codex tasks, code
changes, deployment, and scheduled acceptance must be separated so that an
unsupported interpretation cannot silently become production work.

It also records the same-thread failure that occurred while this package was
being prepared. That incident is operational evidence: a model can have the
relevant context available and still select the wrong frame, answer
confidently, and compound the error through successive corrections.

This package does not authorize code, Airtable, schema, schedule, endpoint, or
production-data changes.

## Current Authority State

The following distinction is mandatory:

```text
REFERENCE
  this package
  workflow documentation
  conversation history

HUMAN CONTROL SURFACE
  Airtable hs_endpoints / work_stage

IMPLEMENTATION EVIDENCE
  checked-in code
  deployed commit
  schemas

OPERATIONAL PROOF
  scheduled execution evidence
  resulting production rows
```

The repository does not currently contain the proposed generated
`workflow-manifest.json` or `.codex/workflow-state/stage-N.json` checkpoints.
Therefore, the enforcement system described below is a required next control,
not an already-operational safeguard.

Until those controls exist, no single source may silently override another.
Any contradiction must produce `CONTEXT_FAIL` and stop interpretation.

## Invalidated Guidance

The following recent assistant conclusions are withdrawn and must not be used:

- The five names `live`, `time-engine`, `statewise`, `time-triggers`, and
  `results` were five newly defined sequential stages.
- The five names were proven to be five independent scheduled lanes.
- `statewise_now` was proven to be an output inside the currently deployed
  Time Engine merely because related endpoint code exists.
- The current legacy implementation defined the target replacement structure.
- Airtable rows required immediate correction based on the assistant's
  interpretation.
- The 751-line predecessor package required immediate correction before its
  intended scope was read back.

No Airtable correction and no code correction was approved from those replies.

# 1. What Happened In The Last 30 Minutes

## Incident Summary

The discussion concerned how to package and divide future work. It was not a
request to inspect code, redesign the workflow, or edit Airtable.

The assistant nevertheless moved through these incompatible frames:

```text
conceptual planning
  -> invented five-stage sequence
  -> current legacy implementation map
  -> replacement-stack interpretation
  -> code inspection
  -> Airtable inspection
  -> proposed document and Airtable corrections
```

Each answer was locally plausible. Collectively they were incompatible.

## Exact Error Classes

| Error | What occurred | Detection signal | Required response |
|---|---|---|---|
| Target substitution | July 13 was used when the requested proof was the July 11 run targeting July 12 | A date appears without an exact request readback | `CONTEXT_FAIL: target mismatch` |
| Mode drift | Concept and handoff discussion became code and Airtable review | Tools or implementation claims appear without an implementation request | Stop; return to the declared mode |
| Ontology invention | Five user-supplied names were converted into stages and dependencies | New arrows, gates, stage numbers, or ownership not quoted from authority | Mark every invented relation `UNKNOWN` |
| Current/target collapse | Legacy behavior was used to define replacement structure | One table mixes "currently does" and "should do" | Split into `CURRENT` and `TARGET` |
| Artifact/status collapse | Table, endpoint, producer, deployment, and cadence were treated as equivalent | "Implemented" appears without the proof level | Use lifecycle states only |
| Authority skipping | Answers were produced without reading the package and control surface | No authority readback or source identifiers | Do not answer substantively |
| Correction cascade | Each challenge caused a new confident architecture | Consecutive replies change the object being discussed | Stop after first contradiction; audit instead of correcting |
| Scope expansion | Airtable corrections were proposed during a conceptual review | Suggested mutations exceed the requested output | Withdraw mutations; preserve all artifacts |
| Unsupported authority | A generated manifest was discussed as operational although no file exists | Named control cannot be located | State `NOT IMPLEMENTED` |
| Cause misdiagnosis | The failure was attributed mainly to a long thread | The model had the material but selected the wrong frame | Identify selective-context failure, not missing context |

## Root Mechanism

This was not simply missing history. The relevant context existed in the same
task.

The model reconstructs the active task from selected context on each response.
Recent wording, tool results, and the latest correction can outweigh earlier
intent. When challenged, the model can overcorrect toward the latest clue and
produce another coherent but incompatible answer.

Confidence is a property of generated language. It is not evidence that the
correct authority, mode, or contract was retrieved.

## Why Documentation Alone Did Not Prevent It

A document is passive. It does not force retrieval before an answer, prevent a
new interpretation, reject a broad diff, or block lifecycle promotion.

The earlier package correctly proposed mechanical controls but those controls
had not been created. As a result, the package described prevention without
enforcing prevention.

# 2. How To Listen For Drift

## Human-Visible Warning Signals

Stop the response when any of these appears:

1. The assistant introduces stage numbers, arrows, gates, owners, or wake rules
   that were not in the selected contract.
2. "Current" and "target" appear in the same description without separate
   evidence.
3. A table or endpoint is called operational without a producer and scheduled
   proof.
4. The answer says "you are right" and immediately presents a new model instead
   of auditing the contradiction.
5. The assistant begins code, schema, Airtable, or deployment inspection during
   concept-only work.
6. The answer recommends correcting an authority source based on an
   interpretation that was not approved.
7. A command includes a show, date, action, or environment that was not read
   back first.
8. The response relies on "the docs" without naming the exact file and section.
9. The meaning of `stage`, `stack`, `lane`, `table`, `producer`, or `task`
   changes between replies.
10. A previous answer is revised more than once without a formal context audit.

## Machine-Readable Stop Response

When a contradiction or missing authority is found, ChatGPT or Codex must
return only:

```text
CONTEXT_FAIL
mode:
requested_object:
conflicting_sources:
unknowns:
mutations_performed: none
next_required_control_action:
```

It must not fill the gap with advice.

## Evidence Vocabulary

Use only these lifecycle states:

```text
DISCUSSED
SCHEMA_READY
CODE_READY
DEPLOYED
SCHEDULED_PASS
```

Definitions:

| State | Required evidence |
|---|---|
| `DISCUSSED` | Business rule recorded; no implementation claim |
| `SCHEMA_READY` | Required tables and fields verified |
| `CODE_READY` | Bounded code and tests complete; not deployed proof |
| `DEPLOYED` | Exact commit deployed; not cadence proof |
| `SCHEDULED_PASS` | Required scheduled executions and resulting rows verified |

An HTTP 200, endpoint payload, manual run, schema, unit test, or local function
does not independently establish `SCHEDULED_PASS`.

# 3. Required Context Gate

Before advice, code inspection, execution, or mutation, the agent must read
back:

```text
MODE
REQUESTED OBJECT
AUTHORITY FILE OR RECORD
AUTHORITY VERSION OR HASH
CURRENT VERIFIED STATE
TARGET STATE
IN SCOPE
OUT OF SCOPE
NEXT ALLOWED ACTION
```

If any field is unavailable, return `CONTEXT_FAIL`.

## Modes

Use exactly one mode:

```text
CONCEPT REVIEW
CHANGE PACKET PREPARATION
READ-ONLY CODE AUDIT
IMPLEMENTATION
DEPLOYMENT
SCHEDULED ACCEPTANCE
```

Mode cannot change implicitly during a task. Tool access does not grant a mode
change. User frustration, urgency, or a correction does not grant a mode
change.

## Fixed Vocabulary

```text
stack       owned implementation boundary
deliverable table, producer, endpoint, or event contract inside a stack
task        one Codex assignment governed by one packet
lane        business data path only when explicitly defined
stage       legacy or numbered workflow boundary only when explicitly defined
acceptance  read-only scheduled proof against a packet
```

User-provided nouns remain nouns. They cannot be converted into workflow edges
without an approved contract.

# 4. One Project, Bounded Tasks

Use one RingStatus Codex project with separate tasks:

```text
00 Control
01 Stack 1 - frozen
02 Stack 2 - frozen
03 Stack 3A/3B - frozen
04 Stack 4 - frozen
05 Stack 5 - Live
06 Stack 6 - Time Engine and Statewise
07 Stack 7 - Rider Results
08 Acceptance - read-only
09 Cleanup - behavior preserving
```

Only one implementation task may edit shared legacy files at a time.

`00 Control` owns assignment, packet hashes, lifecycle status, and handoffs. It
does not write business code or reinterpret policy.

`08 Acceptance` does not repair, manually substitute, or edit. It observes the
approved scheduled path and reports `PASS` or `FAIL`.

`09 Cleanup` begins only after corrected behavior reaches `SCHEDULED_PASS`.

## Replacement Task Process

When a task reaches its context or operational limit:

1. Stop edits and deployment.
2. Update the stack checkpoint.
3. Commit the exact incomplete state if appropriate.
4. Record packet hash, HEAD, deployed commit, last proof, blocker, and next
   action.
5. Create a fresh task in the same project and on the same owned branch.
6. Give it only the approved packet and checkpoint.
7. Require `RESUME_READY` readback before ownership transfers.
8. Archive the old task after the readback matches.

Do not reconstruct state from conversation memory.

# 5. ChatGPT And Codex Division

## ChatGPT

Use ChatGPT for:

- business-rule discussion;
- rider-needs clarification;
- conceptual formula review;
- preparing one bounded change packet.

ChatGPT must not claim current production status without live evidence and must
not produce implementation instructions from an unresolved concept discussion.

## Codex

Use Codex for:

- authority readback;
- repository, schema, and deployment inspection when the packet permits it;
- one bounded implementation;
- tests and diff verification;
- deployment when approved;
- scheduled evidence review in the Acceptance task.

Codex must not reinterpret business policy because repository code behaves
differently.

## Handoff Rule

Do not transfer a conversation. Transfer one approved packet containing:

```text
mode
stack
owner function
current verified behavior
required behavior
inputs
outputs
allowed files
must not change
acceptance gate
stop conditions
approval status
```

# 6. Mutation And Deployment Protection

Every approved change needs:

```text
required change
allowed surface
forbidden surface
machine-verifiable PASS condition
```

Before editing:

1. Verify packet hash.
2. Verify branch and HEAD.
3. Capture the current diff.
4. Verify allowed files.
5. Confirm mode is `IMPLEMENTATION`.

After editing:

1. Reject files outside the allowlist.
2. Reject new tables, endpoints, triggers, schedules, or fallback behavior not
   named in the packet.
3. Reject formatting and dependency churn outside the required change.
4. Run stack-specific regression tests.
5. Update the checkpoint.

Before deployment:

1. Compare changed files with the packet.
2. Compare schema and trigger changes with the packet.
3. Record the exact commit.
4. Do not promote past `CODE_READY` without deployment evidence.

After deployment:

1. Record deployed commit.
2. Wait for the approved scheduled path.
3. Manual endpoints remain diagnostic only.
4. Promote to `SCHEDULED_PASS` only after the packet's scheduled gate passes.

# 7. Current Workflow Completion Packets

These packets preserve the approved direction from the predecessor package.
They do not authorize implementation until separately approved.

## Packet 05 - Live Current State

```text
MODE
  IMPLEMENTATION after separate approval

STACK
  Stack 5 - Live

TARGET FLOW
  get_rings
    -> hs_ring_status
    -> hs_class_start_times
    -> hs_entry_go_times

REQUIRED OUTCOMES
  entry_count_now
  n_gone_now
  n_to_go_now
  reliable is_live
  observed and frozen class start
  snapshot-delta pace accepted only from 105 to 285 seconds
  current entry position and go-time estimates
  ring now, next, end, and lateness after schedule slack

MUST NOT CHANGE
  proven Core behavior
  Time Engine trigger policy
  Results behavior
  outputs
  schedules

PASS
  two scheduled Live cycles
  HTTP 200
  approved fields populated
  no Stack 1-4 changes
```

## Packet 06 - Time Engine And Statewise

```text
MODE
  IMPLEMENTATION after Packet 05 acceptance and separate approval

STACK
  Stack 6 - Time Engine and Statewise

DELIVERABLES
  time calculations
  time_engine_triggers
  statewise_now producer

REQUIRED SCOPE
  tracked classes come from unique classes in hs_entry_go_times

REQUIRED TRIGGERS
  class_start_60
  class_start_30
  class_live
  entry_go_40
  entry_go_20
  entry_class_10_gone
  entry_10_away
  ring_late_15
  ring_late_30

INTERNAL EVENTS
  ring_class_change
  result_ready
  statewise_snapshot_due

STATEWISE
  produce statewise_now every twelve minutes

REMOVE FROM CUSTOMER TRIGGERS
  ring_live
  inferred ring_gate

MUST NOT CHANGE
  Core
  Live source acquisition
  Results polling
  output design
  schedules

PASS
  two scheduled Time Engine cycles
  one scheduled statewise interval
  required trigger scope and types present
  zero duplicate events on unchanged cadence
```

## Packet 07 - Rider Results

```text
MODE
  IMPLEMENTATION after Packet 06 acceptance and separate approval

STACK
  Stack 7 - Rider Results

TARGET FLOW
  tracked result_ready class
    -> results polling
    -> class_no and entry_no match
    -> placed or terminal no_place
    -> hs_rider_results
    -> stop polling completed class

MUST NOT CHANGE
  Core
  Live
  Time Engine calculations
  existing legacy result outputs
  schedules

PASS
  scheduled Results execution creates live hs_rider_results records
  terminal outcomes prevent repeated polling
  existing legacy result outputs remain unchanged
```

## Execution Order

```text
Packet 05
  -> scheduled acceptance
Packet 06
  -> scheduled acceptance
Packet 07
  -> scheduled acceptance
global scheduled review
cleanup only afterward
```

# 8. Required Task Readback

Every new Codex implementation task must begin with only:

```text
CONTRACT_READ
packet_file:
packet_hash:
mode:
stack:
current_verified_state:
target:
allowed_files:
must_not_change:
scheduled_pass_gate:
unknowns:
next_allowed_action:
```

No tools that inspect code, Airtable, schemas, deployments, or production state
may run before this readback when the task is concept-only or packet
preparation.

If the readback does not match, return:

```text
HANDOFF_FAIL
```

# 9. Enforcement Work Still Required

The following controls remain to be built and separately approved:

1. Export selected Airtable `work_stage` records into
   `workflow-manifest.json`.
2. Validate required fields and reject contradictory or duplicate ownership.
3. Generate a deterministic manifest hash.
4. Add Stack 5, 6, and 7 checkpoint files.
5. Add a changed-file allowlist check.
6. Add a contract test for every producer, not only every endpoint.
7. Record deployed commit beside scheduled proof.
8. Prevent lifecycle promotion without required evidence.

Until these exist, the process is procedural rather than mechanically
enforced. Agents must say so explicitly.

# 10. Final Operating Rule

```text
Conversation may discover a decision.
Only an approved packet may define work.
Only a bounded diff may implement it.
Only scheduled evidence may complete it.
Any contradiction stops as CONTEXT_FAIL.
```

This package is ready for human review. It does not modify or authorize changes
to Airtable, code, schemas, schedules, endpoints, or production data.
