# Failure → Published Procedure Map

**Nothing from the prior problem record is deleted.**

| ID | Failure | Status | Published basis |
|---|---|---|---|
| P01 | Agent performs work beyond the requested task/project scope. | **SOLUTION FOUND — PUBLISHED PROCEDURE + PLATFORM ENFORCEMENT** | OPENAI_SCOPE, OPENAI_CODEX, OPENAI_GUARDRAILS |
| P02 | Agent adds a final thought/recommendation/clarification that was not requested. | **PARTIAL SOLUTION — PLATFORM MECHANISM FOUND; NO PUBLISHED EXACT POLICY FOUND** | OPENAI_SCOPE, OPENAI_GUARDRAILS |
| P03 | Working code is changed while reproducing or making an unrelated narrow edit. | **SOLUTION FOUND — PUBLISHED PROCEDURE + RINGSTATUS EXACTNESS POLICY** | OPENAI_SCOPE, OPENAI_CODEX |
| P04 | Agent applies patches, then patches patches, creating downstream instability. | **PARTIAL SOLUTION — PUBLISHED ROOT-CAUSE/RESET PROCEDURE; LOCAL PATCH POLICY REQUIRED** | OPENAI_CODEX, OPENAI_SCOPE, OPENAI_GUIDE |
| P05 | User correction is converted directly into agreement, diagnosis, generalized rule, and edit plan. | **PARTIAL SOLUTION — PUBLISHED EVIDENCE/SCOPE PROCEDURE + BLOCKING MECHANISM; NO NAMED OPENAI PATTERN FOUND** | OPENAI_SCOPE, OPENAI_CODEX, OPENAI_GUARDRAILS |
| P06 | Agent answers from remembered context or assumes it has the authoritative documents. | **SOLUTION FOUND — PUBLISHED SOURCE-OF-TRUTH/SCOPE PROCEDURE + PLATFORM STATE** | OPENAI_SCOPE, OPENAI_CONTEXT, OPENAI_GUARDRAILS |
| P07 | Latest feedback causes the system to reverse previous decisions or silently redefine the task. | **PARTIAL SOLUTION — PLATFORM STATE MECHANISMS FOUND; DECISION-IMMUTABILITY POLICY IS APPLICATION-SPECIFIC** | OPENAI_CONTEXT, OPENAI_SESSIONS, OPENAI_GUARDRAILS |
| P08 | Agent claims complete/fixed/working without actual proof. | **SOLUTION FOUND — PUBLISHED VALIDATION/TESTING + OUTPUT GUARDRAIL** | OPENAI_CODEX, OPENAI_TESTING, OPENAI_GUARDRAILS |
| P09 | Agent keeps retrying/patching after repeated related failures. | **SOLUTION FOUND — PUBLISHED COMPLEXITY RESET + FAILURE THRESHOLD** | OPENAI_SCOPE, OPENAI_GUIDE, OPENAI_CODEX |
| P10 | User becomes QA/debugger and must repeatedly detect regressions. | **SOLUTION FOUND — PUBLISHED TESTING/VALIDATION PROCEDURE** | OPENAI_CODEX, OPENAI_TESTING |
| P11 | Guardrails themselves are invented reactively from examples without researching established methods. | **SOLUTION FOUND — GOVERNANCE PROCEDURE DERIVED FROM PUBLISHED SCOPE DISCIPLINE + EXPLICIT USER POLICY** | OPENAI_SCOPE, OPENAI_GUIDE, ANTHROPIC_AGENTS |
| P12 | Responses bury the user in explanations, failure analysis, and additional suggestions. | **NO PUBLISHED MECHANICAL SOLUTION FOUND — REQUIREMENT PRESERVED** | None located |
| P13 | Assistant fails to route a cross-system/local-UI task to the appropriate capable workflow early, leaving the user to perform repeated manual setup and troubleshooting. | **UNRESOLVED — ROUTING/CAPABILITY PATH MUST BE VERIFIED BEFORE CLAIMING SOLVED** | No verified implementation recorded yet |
| P14 | Assistant recommends Work mode as the solution for local Windows/Codex control without first verifying that Work can access the user's machine, local checkout, or desktop app. | **NO VERIFIED EXECUTION PATH FOUND YET — REQUIREMENT PRESERVED** | Capability must be verified before routing or claiming completion |
| P15 | Deterministic hook tests passed but the first real Codex write-block test still created the prohibited file. | **IMPLEMENTATION FAILURE FOUND — CONTROL BROADENED TO ALL PreToolUse PATHS; LIVE RETEST REQUIRED** | OPENAI_GUARDRAILS / Codex Hooks supported-tool behavior |

## Source key

- **OPENAI_SCOPE** — OpenAI Agents SDK repository — AGENTS.md / Scope Discipline and Complexity Reset — https://github.com/openai/openai-agents-python/blob/main/AGENTS.md
- **OPENAI_CODEX** — OpenAI Codex base instructions — https://github.com/openai/codex/blob/main/codex-rs/core/gpt_5_1_prompt.md
- **OPENAI_GUARDRAILS** — OpenAI Agents SDK — Guardrails — https://openai.github.io/openai-agents-python/guardrails/
- **OPENAI_HITL** — OpenAI Agents SDK — Human-in-the-loop — https://openai.github.io/openai-agents-python/human_in_the_loop/
- **OPENAI_TESTING** — OpenAI Agents SDK — Testing — https://openai.github.io/openai-agents-python/testing/
- **OPENAI_CONTEXT** — OpenAI Agents SDK — Context management — https://openai.github.io/openai-agents-python/context/
- **OPENAI_SESSIONS** — OpenAI Agents SDK — Sessions / Running agents — https://openai.github.io/openai-agents-python/sessions/
- **OPENAI_ORCHESTRATION** — OpenAI Agents SDK — Agent orchestration — https://openai.github.io/openai-agents-python/multi_agent/
- **OPENAI_GUIDE** — OpenAI — A practical guide to building agents — https://openai.com/business/guides-and-resources/a-practical-guide-to-building-ai-agents/
- **MICROSOFT_ORCHESTRATION** — Microsoft — Multi-agent orchestration patterns — https://learn.microsoft.com/en-us/microsoft-copilot-studio/guidance/multi-agent-patterns
- **ANTHROPIC_AGENTS** — Anthropic — Building effective agents — https://www.anthropic.com/engineering/building-effective-agents
