# Codex handoff — RingStatus opt-in workflow

Date: October 1, 2026. Target: `sportdogfood/ringstatus`, default branch `main`. Repository access is verified; verify the actual Codex checkout and remote before editing, rather than assuming `/ringgstatus` is its filesystem path. A GitHub write is not proof that a local Codex checkout has received it or that a Codex task has started.

## Start here

Read `AGENTS.md`, applicable directory instructions, `docs/RingStatus-Workflow-Overview.md`, and `.agents/skills/ringstatus-workflow/SKILL.md`. Inspect current branch, Git status/diff, available tools, and local checkout identity. Preserve unrelated work; do not switch to `ringstatus-data` or a different project. If these files are missing locally, report the checkout/ref mismatch; do not rebuild them from memory or overwrite local changes.

This handoff transfers the opt-in workflow work. It does not authorize application development, deployment, hook/config changes, or recurring tasks merely because Codex reads it.

## Established scope

| Choice | Deliverable | Boundary |
|---|---|---|
| Mode 1 | Factual advice | Read-only; concise evidence and limitations |
| Mode 2 | Advice plus new-project plan | Stop before code; chat-only unless a plan file is requested |
| Mode 3 | Plan, code, testing, independent review, evidence, handoff | Only approved task/files; deployment separately authorized |
| No choice | Normal operation | Do not invoke the workflow automatically |

Use `$ringstatus-workflow mode 1`, `mode 2`, or `mode 3`. Keep scope selected for the current task; do not silently escalate modes. Ground/rebalance remains mandatory within a selected mode. Cycled stages, SMS-in/out, and monitors are future products of mode 3, not more workflow modes.

## Exact locations and status

- `.agents/skills/ringstatus-workflow/SKILL.md`: saved three-mode instructions.
- `.agents/skills/ringstatus-workflow/agents/openai.yaml`: saved explicit-only policy, `allow_implicit_invocation: false`.
- `.agents/skills/ringstatus-workflow/references/build-cadence.md`: mode 3 responsibilities and handoffs.
- `.agents/skills/ringstatus-workflow/references/support-and-limits.md`: documented capabilities and limitations.
- `docs/RingStatus-Workflow-Overview.md`: full purpose, status, owner requirements, evidence, and downstream view accompanying this handoff.
- Root `AGENTS.md`: existing Webflow guidance; not changed by the workflow installation or this handoff.
- A personal workflow copy also exists in ChatGPT skills. Repository edits do not establish that the personal copy changed; record the actual edited copy and do not imply automatic cross-copy synchronization.

Verified so far: repository workflow files were fetched back; explicit-only policy checked; independent static instruction review passed; one agent produced a mode 2 chat-only plan. Actual discovery/invocation/nonactivation in the owner's Codex client remains unverified.

## Pending work for the receiving Codex task

Complete the already requested lightweight modes 1–2 refinement within the workflow instruction files, after establishing current scope and baseline. Keep one lead; add a bounded investigator only when a specific independent question warrants it. No build controllers, implementation/testing machinery, or elaborate logs for a simple advice/plan question. Keep brief mandatory checkpoints and clear mode boundaries.

Make the instructions easy to revise when inspected failure evidence supports a precise correction. Keep workflow-specific rules in the opt-in skill. Use a small, appropriately scoped `AGENTS.md` only for rules the owner explicitly intends to apply to ordinary project work too; do not silently move opt-in procedures into global guidance. Do not edit root `AGENTS.md` merely to advertise or auto-start the workflow.

Use a concise plan before editing; identify exact allowed instruction files. Validate metadata, references, and scope consistency. Exercise bounded advice-only and planning examples, independent instruction review, and the actual client selection path when available. Use disposable fixtures for any code-bearing evaluation. Record PASS / FAIL / BLOCKED per criterion; never replace unavailable client evidence with a static pass. Apply no guessed remedy if explicit invocation fails; inspect the actual client/version and official documentation.

## Required standards

- Use factual, inspected evidence and published supported solutions. Never invent or silently assume requirements, causes, capabilities, approvals, or results.
- Deliver complete, implementation-ready affected code/files when code is requested; no patch-on-patch workarounds or owner reconstruction burden.
- Correct the authorized behavior against the authoritative baseline; never rewrite unrelated working code or automatically optimize, redesign, refactor, rename, or improve approved work.
- Never introduce CSS `!important`; resolve the responsible cascade/structure within scope.
- Inspect upstream producers and downstream consumers/contracts before code changes. Keep existing naming, behavior, and dependencies unless an explicit change is approved.
- One code writer at a time. Give agents exact instructions, targets, actions, evidence requirements, ownership, and stop conditions. Independent reviewers must not be code authors.
- Preserve the plan → implementation → testing → review → evidence → handoff cadence for mode 3; disable only irrelevant optional product lanes, recording the choice.
- Stop the same repair path after two failed fixes. Report a supported blocker; no third invented workaround or scope change.
- Replies: at most two prose sentences plus a compact evidence table when needed; no failure monologues, unrequested teaching, or speculative next features.

## Downstream work — separate authorization and verification

Approved mode 3 code will feed scheduled/event-driven execution, output/audit contracts, and separate SMS/monitor products. A scheduler executes approved code; it does not re-plan or rewrite the application each cycle. Bounded monitoring agents may investigate actual logs/status and return findings; fixes return through mode 3, with no automatic source edits or publishing.

Before recurring operation, establish host, access, cadence, code version, run contract, maximum duration, overlap behavior, stop/pause, failure response, and evidence. Do not invent missing values or select a provider from examples in the overview. Keep the first operating prototype synthetic and limited to one WEF slot unless the owner changes scope.

Historical disposable task CLI checks were 10/10, then 18/18, plus 19 coordinator subprocess checks. A manual synthetic slot passed 8/8. These are application checks, not agent reliability benchmarks, actual RingStatus integration, repeated scheduling, SMS, monitors, or unattended safety. Original task fixture cleanup remained unproven; the synthetic slot did not receive independent agent code review.

Parent interruption left a worker running in the cancellation exercise. User-side Stop, disconnect/reconnect, and independent cancellation while disconnected remain unverified. Freeze cause is unknown. Do not create a custom watchdog, cancellation runtime, hook system, or claimed kill switch to fill that gap.

## Evidence and final return

Return exact edited paths, scope compliance, observed behavior, and unresolved client/platform limits. Do not call the workflow operationally complete unless its actual target/client criteria pass. Do not launch downstream products as an unrequested continuation of this instruction handoff.

Current source and historical evidence are summarized in the accompanying overview. Prototype application files remain outside RingStatus; do not assume a receiving Codex environment can access this chat's scratch paths. If raw prototype evidence is needed, explicitly identify what must be transferred rather than claiming local availability.
