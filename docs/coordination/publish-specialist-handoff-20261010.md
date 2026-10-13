# Publish — new runner handoff

Prepared 2026-10-10. Execution location: new separate chat. Prepared, not dispatched. Start on receiving this handoff; complete the authorized base setup rather than stopping after inspection.

## Assignment and target
Normalize existing Airtable **publish**, `appZY1hcqGvUvxIuV`, through the Airtable connector. Do not create another base. Initial inspection found only `Table 1` (`tblAzdr8GmPG1gUaW`) with three placeholder records whose Name fields are blank; inspect full records before repurposing. User described it as empty. Do not delete records.

Build the minimum useful operating structure and documented workflow, using the reusable base guide and relevant Recognize/Tenants examples. Those examples are NOT evidence that their applications or schemas are fully accepted. This assignment is base setup and source/reference mapping, not application implementation, deployment or public publishing.

## Locked purpose
Publish owns front-facing schedule, pro-schedule, results, notifications and RingWaze painter. Preserve the existing print-pdf lane as a planned capability pending its requirements. Astro is a shared delivery method: Publish uses endpoints and Astro templates to deliver displays. It is not another name for Publish. Webflow owns static/CMS presentation; rs-webflow does not own Astro implementation. Preserve native presentation where already agreed in Recognize/Tenants.

Messages owns incoming SMS normalization, show/intent lookup, endpoint response and outgoing SMS; data-triggered outbound messages; logging and notifications-feed updates. Publish displays those outputs. Recognize owns silent recognition/authentication and identity/session contracts. Tenants owns onboarding, inputs, inline editing, saves, tenant permissions/options. RingWaze input patterns are still developing. Reference their IDs and contracts; do not duplicate their masters or invent endpoints.

Website, Recognize, Tenants, Messages and Publish must be established and stable before new engine work begins. Engines process data and supply outputs; this handoff does not authorize engine work.

## Owner-supplied page references — latest clarification
These are owner-supplied examples and intended mappings, not freshly verified live behavior. Site: RingStatus `6982268b7543ac3c80151266`. Resolve exact page IDs, loader/source and endpoint from the existing implementation before edits.

| Page slug | Role and expected behavior |
| --- | --- |
| rs-barn-onboarding-schedule-v24 | Pro-schedule front-facing page; intended to populate from an endpoint. Preserve the existing design/binding contract, do not infer replacement by a new template. |
| print2 | Working print-proxy / print-to-PDF example supplied by owner. Inspect refresh/loading of latest files, actual file source and print behavior; reuse the proven method. |
| lite | Legacy schedule example; inspect its refresh/latest-file loading path. |
| master | Legacy schedule example; inspect its refresh/latest-file loading path. |
| cwf | Legacy schedule example; inspect its refresh/latest-file loading path. |
| rs-barn-onboarding-feed | Likely Messages-related page. Messages owns feed content/events; document Publish display boundary without finalizing ownership from this tentative statement. |
| rs-barn-onboarding-alerts | Likely Messages-related page; existing SMS opt-in context. Keep separate from outbound delivery and do not recreate it as a Publish schedule page. |

Owner describes the working examples as refreshing and loading latest files. Preserve that as an existing pattern to inspect; do not assume it proves the planned pro-schedule endpoint binding works or invent polling intervals. Record exact observed behavior and the smallest compatibility delta in existing workflow/tasks. Base setup has since been completed by new chat `01a12778-d699-7c92-baa7-81f46ddb1b64`; earlier Prepared/empty-base statements above are the initial handoff snapshot. Current Resume here: https://airtable.com/appZY1hcqGvUvxIuV/tblTFXEyvmO0JTKfN/rec1sXjVK2pmzvxeC . Use current checkpoint for actual assignment/state.

## Tokens and dynamic state presentation
- Current verified library: Webflow Airtable base `appara5767XpIaJIf`, `rs-token-lib` table `tblVcIhTfG1pNsCyi`: https://airtable.com/appara5767XpIaJIf/tblVcIhTfG1pNsCyi . Use its semantic meaning, aliases, presentation, css-class, background-css, text/border colors, source IDs and verification-scope fields. Preserve opacity/shade information, not only hex-color.
- Owner-supplied Webflow reference page ID `6a63f491030cc3634eb83d92`; historical source identifies ProApp `/audit/proapp`. This is evidence for final dynamic Schedule x ProApp token/shade application, not permission to recolor or redesign the existing page.
- Existing decision/reference: `docs/coordination/decisions.md` and `docs/coordination/schedule-v24-new-draft.md:17`. Those documents retain historical Agents base/table IDs (`appZahVgD156cMAe3/tblB0ZpdeOzpR9EqL`); use the verified current Webflow-base library above for new mapping work. Historical count/verification is not current live rendering proof.
- Map actual endpoint state/meaning to existing semantic token and then its existing class/style. Record exact token record, source state and fallback for unmatched states in Publish workflow/task evidence. Do not invent missing meanings, polling cadence, colors or assignments; unresolved mappings remain an explicit task. Inspect existing assignments and source before implementation; no new token system is authorized by adding these references.

## Read at startup
Repository: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`.
1. Current AGENTS.md.
2. `docs/coordination/new-airtable-base-guide.md` in full. Use its eight supporting tables plus the base index, minimum fields and actual content/readback requirements. Do not copy domain business tables wholesale.
3. `docs/coordination/context-check-procedure.md`.
4. Agents `bases` row `recXokz7XGPnv2XzR`, linked lanes, project `recmjScmqrxpGjj22`, surfaces and current tasks. Agents base `appZahVgD156cMAe3`.
5. Company Home: https://chatgpt.com/space/page_6aca71a8c7d881918e6687969a6aad48
6. Relevant examples: Recognize system `app9kOZdIaGyKk5uG/tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe`; Tenants system `app7BXcw801n19QKF/tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y`. Read structure/content selectively; do not import their business data or completion claims.

## Skills and connectors
Read actual applicable skills, not just names. Paths below were available to the preparing chat; verify access in the new chat. Do not load every vendor skill for recordkeeping.
- Startup/routing: `C:/Users/gombc/.agents/skills/request-router/SKILL.md`, `instruction-loader/SKILL.md`, `boundaries/SKILL.md`, `planning/SKILL.md` under the same skills root.
- Coordination/handoff: `C:/Users/gombc/.codex/skills/rs-coordinator/SKILL.md`; `C:/Users/gombc/.agents/skills/handoff/SKILL.md`. Central Agents has since deprecated `agents`/`handoffs`: use **threads** (`tblvIft5zuUKuw3Si`) for chat identity and **tasks** (`tbluT1yhP1FJMb8Cm`) for assignments/prompts. Old IDs in references are historical. Domain-local agents tables remain per the base guide.
- Airtable: `C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/airtable/7.0.0/skills/airtable-overview/SKILL.md`; load airtable-filters/SKILL.md beside it only when needed.
- Evidence: `C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/superpowers/6.4.2/skills/verification-before-completion/SKILL.md`.
- Conditional source/hosting mapping: `C:/Users/gombc/.agents/skills/webflow-cli-cloud/SKILL.md`; Webflow native bindings: `webflow-mcp-custom-code-management/SKILL.md` in that root. Load only for the relevant authorized inspection.
- Later coding/review, when separately assigned: coding-instructions and code-review in `.agents/skills`; frontend-testing-debugging in the installed build-web-apps plugin. No standalone Astro or Publish skill is claimed installed.
- Primary connector: Airtable, with successful schema/record reads verified in Publish and Agents. Use MCP/connector rather than browser when available.
- Repository filesystem/Git: proven-source inspection. GitHub connector optional when local history is insufficient.
- Webflow production MCP only, site `6982268b7543ac3c80151266`; call guide once before requested Webflow work. No proactive OAuth/Bridge repair or publishing. Cloudflare only if an actual inspected dependency requires it. Twilio belongs to Messages; not needed for Publish base creation.
- Connections record exact destinations and observed access. Never store secrets or claim access because a tool is installed.

## Work through completion
1. Register your actual new chat ID in Agents threads and claim the existing Publish handoff task. No worker exists merely because this document exists.
2. Read base, guide and current schemas. Build/reuse system, tasks, agents, fails, activity, development, workflow, changes and publish-index. One-word/lowercase-hyphen table names. Keep only necessary fields and links. Use the guide's meaningful initial records, including START HERE and How to use this base.
3. Record endpoint-to-template-to-display workflow for each agreed capability. Map existing code before suggesting implementation. Historical proofs: WEC Packing commit `647ea24f8`, `webflow/packing-worksheet`, `webflow-cloud-test/src/pages/wec-packing`, `src/lib/wec-packing.js`, and `docs/wec_packing_comprehensive_overview_2026-06-06.md`. These show hosted templates/endpoint patterns, not proof Publish is currently wired. Existing schedule source should be inspected narrowly; do not invent a fresh engine.
4. Add only business configuration tables justified by inspected consumers. Planned lanes do not each require a separate table. Put unknown endpoint/entitlement/cadence details into exact open tasks or development; do not block independent setup or ask the owner to rediscover documented facts.
5. Read back physical schema, names, records and links. Reconcile publish-index and Agents base-lanes, retaining planned capabilities as Expected and distinguishing table-backed records. Set Agents bases.Resume here only to the verified Publish system record. Preserve project/base IDs.
6. Record actual health, last stop point, open tasks, responsibilities, code/version references, skill/connection states, next action, and latest activity in Publish. Add Agents activity linked to Publish and Agents; record meaningful changes. No healthy/live-complete claim from documentation alone.

## Acceptance and limits
Setup is complete when a fresh agent can open Resume here and understand scope, expected workflow, actual state, all open work, downstream contracts, source evidence, needed skills/access and first executable next action without this conversation. Every table/record/link asserted created must be read back. Application completion remains a separate task requiring live display behavior, not local tests or inventory counts.

Budget: 45 minutes from actual startup, final 10 minutes reserved for reconciliation/readback. No automatic extension. Continue independent authorized setup through routine checkpoints; stop only at completion, hard limit or a genuine remaining blocker. No new hooks/monitors, business pipeline runs, code edits, automation activation, publishing, SMS, migration or deletion. Shared source currently belongs to Recognize/Tenants correction work; inspect only. Do not dispatch subagents unless explicitly authorized for a specific subtask. This handoff does not authorize messaging other chats; use shared records unless the owner grants it.

At handoff leave exact completed evidence and unfinished items. No ambiguous outro; distinguish prepared, dispatched, working and complete. Model recommendation: GPT-6.1 Sol Medium; do not silently change settings.
