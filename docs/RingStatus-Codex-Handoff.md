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

Verified so far: repository workflow files were fetched back; explicit-only policy checked; independent static instruction review passed; one agent produced a mode 2 chat-only plan. Fresh owner-client tasks also verified explicit mode 1 invocation, no-selection nonactivation, and explicit mode 3 invocation/build cadence. A fresh owner-session mode 2 explicit-invocation task was not included in the cited checks.

## Completed modes 1–2 refinement

The requested lightweight modes 1–2 refinement is present as the pre-existing uncommitted change to `.agents/skills/ringstatus-workflow/SKILL.md` and was preserved byte-for-byte during fresh mode 3 execution task `01a0f8c3-8fc5-7e81-8a52-0bd4a312d9b2` (parent/source task `01a0f890-9530-7ba2-bd6d-ef15b29957db`). Mode 1 keeps factual advice read-only and lead-owned, with at most one useful bounded read-only investigator. Mode 2 adds a project plan and acceptance/handoff detail but stops before code; checkpoints remain in the response or an explicitly authorized plan file. Both modes avoid implementation/testing/controller machinery.

Make the instructions easy to revise when inspected failure evidence supports a precise correction. Keep workflow-specific rules in the opt-in skill. Use a small, appropriately scoped `AGENTS.md` only for rules the owner explicitly intends to apply to ordinary project work too; do not silently move opt-in procedures into global guidance. Do not edit root `AGENTS.md` merely to advertise or auto-start the workflow.

Use a concise plan before editing; identify exact allowed instruction files. Validate metadata, references, and scope consistency. Exercise bounded advice-only and planning examples, independent instruction review, and the actual client selection path when available. Use disposable fixtures for any code-bearing evaluation. Record PASS / FAIL / BLOCKED per criterion; never replace unavailable client evidence with a static pass. Apply no guessed remedy if explicit invocation fails; inspect the actual client/version and official documentation.

## Fresh mode 3 verification — task `01a0f8c3-8fc5-7e81-8a52-0bd4a312d9b2`

Parent/source task: `01a0f890-9530-7ba2-bd6d-ef15b29957db`.

The verified worktree was `C:\Users\gombc\.codex\worktrees\f88d\ringstatus` (detached worktree). The disposable prototype was created only at `C:\Users\gombc\Documents\Codex\2026-10-01\ringstatus-workflow-mode3-task-cli-01a0f890`; it was absent before creation. Deployment authorization was **none**. RingStatus application integration, scheduling/cadence, hooks, Webflow, SMS, monitors, heartbeat/enforcement, recurring/unattended operation, and cancellation-safety work were explicitly disabled.

Prototype files:

- `PROJECT_PLAN.md`: lead-owned objective, boundaries, acceptance criteria, evidence paths, dependencies, disabled lanes, and checkpoints.
- `BASELINE_EVIDENCE.md`: task/worktree, absent-target, Git baseline, protected pre-existing skill modification, and no-deployment record.
- `task_cli.py`: standard-library persistent task CLI with `add`, `list`, `complete`, and `reopen`.
- `test_task_cli.py`: 11 frozen original subprocess tests, 7 additive reopen tests, and 4 additive stored-data validation tests.
- `implementation-evidence.md`: interfaces, named test inventories, exact phase results, repair evidence, and final application/test hashes.

The lead recorded grounding/rebalancing at startup, before implementation, before initial and post-repair testing handoffs, before the `reopen` enhancement, before initial/final review, and at completion. The sole code writer was `/root/prototype_writer`. The separate read-only process tester was `/root/independent_tester`; the separate read-only reviewer was `/root/independent_reviewer`. No other agent wrote prototype code.

### Commands and observed results

All automated commands were run from the disposable prototype folder:

```powershell
python -m unittest -v test_task_cli.py
```

| Phase/run | Result |
|---|---|
| Initial writer run (`add`/`list`/`complete`) | PASS — 11/11, exit 0, `OK`, 1.510s |
| Initial lead rerun | PASS — 11/11, exit 0, `OK`, 1.432s |
| `reopen` writer full regression | PASS — 18/18 (11 original + 7 reopen), exit 0, `OK`, 5.333s |
| `reopen` lead rerun | PASS — 18/18, exit 0, `OK`, 3.640s |
| Initial independent tester suite | PASS — 18/18, exit 0, `OK`, 4.145s |
| Review repair writer run | PASS — 22/22 (11 original + 7 reopen + 4 validation), exit 0, `OK`, 4.526s |
| Review repair lead rerun | PASS — 22/22, exit 0, `OK`, 4.782s |
| Post-repair independent tester suite | PASS — 22/22, exit 0, `OK`, 4.115s |
| Evidence-only immutable-inventory tester suite | PASS — 22/22, exit 0, `OK`, 3.685s; pre/post inventory exact match |

The independent tester ran separate CLI processes against unique temporary JSON fixtures. `add`, `list`, `complete`, and `reopen` returned expected exit-0 output and persisted state across processes. Invalid commands, blank descriptions, invalid/nonpositive/missing IDs, duplicate completion, already-open reopening, invalid JSON, and malformed-but-valid stored records returned controlled exit 2. Each command left the malformed record fixture byte-for-byte unchanged. Fixture/capture evidence remains under `C:\Users\gombc\AppData\Local\Temp\ringstatus-mode3-independent-tester-ee12eb024fa34ad590c80c0449c6e485` and `C:\Users\gombc\AppData\Local\Temp\ringstatus-mode3-postrepair-tester-04c595016e4d43d1bb2f339b4d89fb33`.

A fresh evidence-only assignment to `/root/independent_tester` resolved the strict no-create boundary. Every Python process used `PYTHONDONTWRITEBYTECODE=1` and `python -B`; no tester cleanup was performed. Recursive prototype inventories before and after behavior checks and the full suite each contained the prototype root plus five files (count 6), shared digest `66883A246BE754A230CB43E5F0379BC6C42C45FF82730A95209F7A01DB8CC928`, matched exactly, and had zero differences. Retained fixtures, captures, and inventory JSON are outside the prototype at `C:\Users\gombc\AppData\Local\Temp\ringstatus-mode3-evidenceonly-tester-5ede45dc9d7346db80719f40b1b9810c`. The earlier transient bytecode-cache deviation remains part of the historical record but is superseded for the acceptance criterion by this clean independent run.

### Actual owner-client checks

| Client criterion | Status | Directly inspected task evidence |
|---|---|---|
| Explicit mode 1 discovery/invocation | PASS | Task `01a0f8b1-29f4-7810-af5b-e758b6f5f027` stated it was applying `ringstatus-workflow` mode 1, enforced read-only advice, distinguished mode 2 planning, and reported no file changes. |
| No-selection nonactivation | PASS | Task `01a0f8b1-29f8-7a21-970d-4f72ee90a416` answered only “Never publish unless the user explicitly requests publishing” from root `AGENTS.md`; it did not activate or mention the workflow. |
| Explicit mode 2 owner-session invocation | BLOCKED | No fresh mode 2 task ID was among the cited owner-session checks. The previously recorded chat-only mode 2 plan remains evidence of plan behavior, not fresh explicit selection in this check set. |
| Explicit mode 3 discovery/invocation and cadence | PASS | Task `01a0f8c3-8fc5-7e81-8a52-0bd4a312d9b2` stated it was using `ringstatus-workflow` mode 3 and executed plan → implementation → testing → independent review → evidence → handoff. |

The initial independent review was PASS with no critical/high/medium findings and two low findings: malformed-but-valid stored records were not validated, and final hashes were absent from implementation evidence. The sole writer corrected both; testing increased from 18 to 22 tests. Final re-review confirmed both findings resolved, no application findings at any severity, all 11 original and 7 reopen test identities retained, and current hashes matching evidence. Its one low evidence finding—a stale phase-status line in the lead plan—was corrected by the lead.

### Criterion status

| Mode / criterion | Status | Evidence or limitation |
|---|---|---|
| Mode 1 advice boundary and actual client invocation | PASS | Static boundary plus task `01a0f8b1-29f4-7810-af5b-e758b6f5f027` verify read-only mode 1 behavior. |
| Mode 2 planning boundary | PASS | Existing refinement and preserved chat-only plan evidence keep mode 2 at advice + plan and stop before code. |
| Mode 2 fresh explicit owner-client invocation | BLOCKED | Not included in the cited owner-session tasks; no substitute static claim is made. |
| No-selection workflow nonactivation | PASS | Task `01a0f8b1-29f8-7a21-970d-4f72ee90a416` followed only ordinary root instructions. |
| Mode 3 actual client invocation, plan, and authorization | PASS | Task `01a0f8c3-8fc5-7e81-8a52-0bd4a312d9b2` explicitly invoked mode 3; lead plan records exact target, allowed writes, evidence, dependencies, disabled lanes, and deployment authorization `none`. |
| Initial add/list/complete phase | PASS | 11 named original tests passed writer and lead runs. |
| Reopen enhancement and compatibility | PASS | All 11 original tests were retained by identity and passed with 7 reopen tests in writer, lead, and tester runs. |
| Persistence and invalid-input behavior | PASS | Independent separate-process checks passed; invalid/malformed data returned exit 2 without overwrite. |
| Independent testing and no-create boundary | PASS | Fresh tester used bytecode suppression; 22/22 passed in 3.685s and exact pre/post recursive inventories matched. Earlier transient bytecode creation remains recorded as historical superseded evidence. |
| Independent code review | PASS | Separate reviewer; both initial low findings resolved; final review found no application defects. |
| Repository write scope | PASS | This handoff is the only task-created repository modification; root `AGENTS.md`, the pre-existing workflow-skill refinement, application code, other docs, hooks, and configuration remained unchanged. |
| Deploy/publish/integrate/schedule/SMS/monitor | BLOCKED by scope | Not authorized and not attempted; no operational claim is made. |

### Rerun and remaining limits

From the prototype folder, rerun without generating bytecode:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
python -B -m unittest -v test_task_cli.py
```

For a manual isolated check, use `python -B task_cli.py --data <temporary-json-path> add <description>`, then invoke `list`, `complete <id>`, and `reopen <id>` as separate processes against the same path. Final recorded hashes are `451569DCAF7E8AC7DE4A651D8A5ECC8E416B97E827492782C1286378DB42E872` for `task_cli.py` and `6A1D67F192611D8FE81E3D2B805B75D120F3B13A5D1C5D1E95517047D7247F28` for `test_task_cli.py`.

Remaining limitations: the disposable folder has no VCS history, so the frozen inventory proves the 11 original test identities were retained but not that their bodies are independently byte-for-byte comparable to an initial snapshot. The fresh tasks verify observed mode 1 invocation, no-selection nonactivation, and mode 3 invocation in this owner client/session; they do not prove universal enforcement across clients or versions, and fresh explicit mode 2 selection remains unverified. Local prototype tests do not establish deployment, RingStatus integration, scheduling, SMS, monitors, unattended reliability, or cancellation safety.

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
