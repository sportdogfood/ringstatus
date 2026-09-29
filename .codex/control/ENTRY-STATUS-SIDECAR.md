# Entry Status Sidecar

This file classifies preserved evidence. It does not replace or shorten the source records.

Status vocabulary:
- **SOLUTION FOUND**
- **PARTIAL SOLUTION**
- **NO PUBLISHED MECHANICAL SOLUTION FOUND**
- **EVIDENCE ONLY — NOT A DISTINCT CONTROL REQUIREMENT**

Published basis keys are defined in `.codex/control/FAILURE-SOLUTION-MAP.md`.

## A. Existing September failure record

| Ref | Preserved failure | Status |
|---|---|---|
| A01 | Work beyond requested task/project scope | **SOLUTION FOUND** |
| A02 | Unrequested final thought/recommendation/clarification | **PARTIAL SOLUTION** |
| A03 | Working code changed during narrow/reproduction work | **SOLUTION FOUND** |
| A04 | Patch-on-patch behavior | **PARTIAL SOLUTION** |
| A05 | Correction converted into diagnosis/generalized rule/edit plan | **PARTIAL SOLUTION** |
| A06 | Memory/assumption used instead of authoritative source | **SOLUTION FOUND** |
| A07 | Latest feedback silently reverses prior decisions/task | **PARTIAL SOLUTION** |
| A08 | Complete/fixed/working claimed without proof | **SOLUTION FOUND** |
| A09 | Repeated retry/patch after related failures | **SOLUTION FOUND** |
| A10 | User becomes QA/debugger | **SOLUTION FOUND** |
| A11 | Guardrails invented reactively without research | **SOLUTION FOUND** |
| A12 | Responses bury user in explanations/additional suggestions | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |

## B. Agent Hierarchy Review — "What you expressed"

Source: `.codex/control/history/AGENT-HIERARCHY-REVIEW-2026-09-29.txt`

| Ref | Preserved requirement/failure | Status |
|---|---|---|
| B01 | Answers driven by tone/latest wording | **PARTIAL SOLUTION** |
| B02 | Assumptions presented with authoritative confidence | **PARTIAL SOLUTION** |
| B03 | Advice built from partial context | **PARTIAL SOLUTION** |
| B04 | Infrastructure/architecture designed before end-to-end business goal is understood | **PARTIAL SOLUTION** |
| B05 | Code delivered quickly then repeatedly patched because workflow was not understood/tested | **PARTIAL SOLUTION** |
| B06 | Local fixes presented as complete while recurring process remains wrong | **PARTIAL SOLUTION** |
| B07 | Complete/fixed/working claims without verification | **SOLUTION FOUND** |
| B08 | User must correct, restate scope, and babysit tasks | **PARTIAL SOLUTION** |
| B09 | Long self-diagnoses/promises/guardrail language without behavioral change | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |
| B10 | Agent answers first and user catches mistakes later | **PARTIAL SOLUTION** |
| B11 | Multi-agent expansion multiplies unreliable reasoning/supervision burden | **PARTIAL SOLUTION** |
| B12 | Guidance must be factual/current-best-practice/document-grounded | **PARTIAL SOLUTION** |
| B13 | Missing requirements should be identified even when user did not think to mention them | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |
| B14 | Architecture must be designed around RingStatus business goals, not repo tree | **PARTIAL SOLUTION** |
| B15 | Business goals/documents must be verified before answering | **PARTIAL SOLUTION** |
| B16 | Drift consumes time/money/tokens/business friction | **PARTIAL SOLUTION** |
| B17 | One question expands and direction repeatedly changes | **PARTIAL SOLUTION** |
| B18 | User carries quality-control burden | **PARTIAL SOLUTION** |

## C. Agent Hierarchy Review — "How I failed"

| Ref | Preserved failure | Status |
|---|---|---|
| C01 | Validated original tree too quickly | **PARTIAL SOLUTION** |
| C02 | Redesigned from tree instead of business | **PARTIAL SOLUTION** |
| C03 | Claimed business goals from remembered context before inspecting sources | **PARTIAL SOLUTION** |
| C04 | Changed architectural direction reactively | **PARTIAL SOLUTION** |
| C05 | Replaced task execution with self-diagnosis | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |
| C06 | Behavioral promises made without evidence | **PARTIAL SOLUTION** |
| C07 | Latest criticism became strongest input | **PARTIAL SOLUTION** |
| C08 | Evidence was overstated | **PARTIAL SOLUTION** |
| C09 | Architecture recommendations continued before complete source set was proven | **PARTIAL SOLUTION** |
| C10 | Agent created more work for user | **PARTIAL SOLUTION** |

## D. July 12 operating-package exact error classes

Source: `docs/horseshowing/chatgpt-codex-operating-package-2026-07-12.md`

| Ref | Preserved error class | Status |
|---|---|---|
| D01 | Target substitution | **PARTIAL SOLUTION** |
| D02 | Mode drift | **PARTIAL SOLUTION** |
| D03 | Ontology invention | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |
| D04 | Current/target collapse | **PARTIAL SOLUTION** |
| D05 | Artifact/status collapse | **PARTIAL SOLUTION** |
| D06 | Authority skipping | **PARTIAL SOLUTION** |
| D07 | Correction cascade | **PARTIAL SOLUTION** |
| D08 | Scope expansion | **PARTIAL SOLUTION** |
| D09 | Unsupported authority | **PARTIAL SOLUTION** |
| D10 | Cause misdiagnosis | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |

## E. Current-session evidence

Source: `.codex/control/history/CURRENT-SESSION-EVIDENCE-2026-09-29.md`

The exact statements are preserved verbatim in the source file. They are retained even when they are consequences/reactions rather than distinct technical requirements.

| Ref | Underlying control issue | Status |
|---|---|---|
| E01 | User forced to decide technical mechanism without sufficient basis | **PARTIAL SOLUTION** |
| E02 | Enforceability checked after design instead of before | **PARTIAL SOLUTION** |
| E03 | Guidance given without verified platform limitations | **PARTIAL SOLUTION** |
| E04 | Evidence preservation concern | **SOLUTION FOUND** for preservation mechanism; live completeness still requires verification |
| E05 | Excessive steps/details shifted implementation burden to user | **PARTIAL SOLUTION** |
| E06 | Old custom instructions may conflict with test | **PARTIAL SOLUTION** |
| E07 | Sandbox/approval state blocked read-only verification | **PARTIAL SOLUTION** |
| E08 | User cannot see active work while agent explains instead | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |
| E09 | New branch controls could remain isolated from RingStatus main | **SOLUTION FOUND** as a Git promotion/merge mechanism; not yet merged |
| E10 | Original entries not preserved and individually classified | **SOLUTION FOUND** by verbatim source preservation + this sidecar; completeness must be verified against available source records |
| E11 | Agent continues explaining failure instead of delivering repair | **NO PUBLISHED MECHANICAL SOLUTION FOUND** |

## Classification rule

A later implementation may improve a status, but it may not delete the source entry. Any status change requires:
1. named published basis or a clearly labeled RingStatus-specific mechanism;
2. implementation evidence;
3. verification evidence when the claim is mechanical.
