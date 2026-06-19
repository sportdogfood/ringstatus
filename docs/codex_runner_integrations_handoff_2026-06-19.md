# Codex Runner Integrations Handoff

Date: 2026-06-19

Purpose: give a Codex runner in the existing RingStatus project a concise map of available integrations, MCP/CLI lanes, Webflow/Webflow Cloud lanes, Airtable lanes, and installed skills before it touches code or live systems.

## Paste Into Codex Project Instructions

Use this repo's existing docs before acting. Do not infer architecture from a single file or from memory.

First-read sources:

```text
docs/ringstatus_runner_options_overview.md
docs/horseshowing/locked-workflow-gate-mcp-first-2026-06-17.md
docs/horseshowing/wec-end-to-end-handoff-v0.1-2026-06-17.md
docs/horseshowing/webflow-embed-options.md
docs/webflow_cloud_dataset_template_handoff.md
docs/hps_duplicate_connector_new_chat_prompt.md
docs/hps_horses_webflow_airtable_connector_readme.md
docs/8778_tack_horses_change_log_handoff.md
skills/rs-css-audit/SKILL.md
```

Required runner behavior:

```text
1. Confirm the active lane before editing: Webflow MCP, manual Webflow embed, Webflow Cloud/Astro API, Airtable connector, Catalyst/WEC workflow, Cloudflare/PDF worker, local runner, or static GitHub/jsDelivr asset.
2. Check installed tools/skills in the current Codex session before assuming access.
3. Prefer MCP/CLI/live evidence over memory.
4. Never expose Airtable tokens in browser code.
5. For two-way Airtable behavior, browser -> Webflow Cloud/API -> Airtable -> response/log.
6. For WEC/Horseshowing, Catalyst is the workflow system and Airtable is mainly mirror/manual-control support unless the contract names an Airtable lever.
7. Use Superpowers skills when debugging, planning, TDD, verification, or code review would reduce drift.
8. Stop before live writes unless explicitly approved.
```

## Core Integration Map

| Lane | Use For | Source Of Truth | Main Tools |
| --- | --- | --- | --- |
| Airtable MCP / Airtable CLI | Inspect bases, schemas, records, filters, current row evidence | Live Airtable plus repo contracts | `airtable:airtable-cli`, `airtable:airtable-filters`, Airtable MCP CLI |
| Webflow MCP | Native Webflow page, custom code, Designer, CMS, assets, publish | Live Webflow site/page | `webflow-mcp:*` skills, Webflow MCP tools when exposed |
| Webflow CLI / Cloud | Webflow Cloud apps, code components, deployed API/runtime | `webflow-cloud-test/`, Webflow Cloud project | `webflow-cli:cloud`, `webflow-cli:code-component`, Webflow Cloud docs |
| Webflow manual embed | Copy/paste embed blocks and pinned static assets | `webflow/`, `docs/horseshowing/webflow-drops/` | repo files, browser proof, jsDelivr pinned commits |
| Webflow Cloud/Astro API | Server boundary for Airtable reads/writes | `webflow-cloud-test/src/pages/...` | Astro build/tests, deployed `/test/...` endpoints |
| Catalyst / Horseshowing | WEC schedule workflow, runner stages, focus-day output | `ringstatus-data/catalyst-workspaces/horseshowing` plus WEC docs | Catalyst CLI/MCP, PowerShell runner only as operator |
| Cloudflare / PDF Worker | Proxy/runtime/fallback/PDF rendering when explicitly part of the lane | Cloudflare Worker source or live worker URL | Cloudflare skills/tools, live smoke proof |
| GitHub/jsDelivr assets | Static JS/CSS loaded by Webflow pages | this repo plus pinned commit URL | Git, browser network proof, jsDelivr URL |
| Local runner scripts | Airtable runners, scheduled jobs, workflow audits | repo scripts and docs | PowerShell/node scripts, logs, audit output |
| Superpowers | Process discipline for debugging, TDD, verification, review | installed Superpowers skills | `superpowers:*` skills |

## Airtable

Installed/session capabilities to check first:

```text
airtable:airtable-cli
airtable:airtable-filters
airtable:airtable-overview
```

Known repo rules:

```text
- For current schema, use live Airtable metadata/MCP/CLI rather than stale checked-in inventory.
- For browser apps, do not call Airtable directly from public frontend JavaScript.
- Use Webflow Cloud/Astro or another approved server boundary for Airtable tokens.
- For WEC/Horseshowing, Airtable is mainly manual controls, helper lookups, mirror visibility, logs, and alerts.
- For HPS-style connectors, confirm table, view, env vars, editable fields, log table, root id, and route before copying patterns.
```

Important WEC Airtable base:

```text
app6XS1RvsPNRT6os
```

Important WEC manual/control surfaces from current contracts:

```text
focus_show
update_schedule_staging
helpers: horses, riders, trainers, rings, ring_names, class_hide, alert_templates
```

## Webflow MCP

Installed/session Webflow MCP skills:

```text
webflow-mcp:custom-code-management
webflow-mcp:designer-tools
webflow-mcp:flowkit-naming
webflow-mcp:safe-publish
```

Use this lane only when Webflow itself is the target:

```text
- native page/Designer changes
- custom code registration/application
- CMS/site/page/asset operations
- controlled publish
```

Before using Webflow MCP:

```text
1. Confirm tools are exposed in the current Codex session.
2. Confirm the site/page from Webflow, not from guessed repo paths.
3. Confirm whether the task is a Webflow-native edit or only a repo/static asset edit.
4. For custom-code writes, confirm the Webflow app has required custom_code read/write access.
```

## Webflow CLI And Webflow Cloud

Installed/session Webflow CLI skills:

```text
webflow-cli:cloud
webflow-cli:code-component
```

Known RingStatus Webflow Cloud pattern:

```text
Webflow embed
  -> static frontend JS/CSS
  -> Webflow Cloud/Astro route under /test/...
  -> server-side Airtable API call
  -> JSON response
```

Primary source:

```text
webflow-cloud-test/
```

Read first:

```text
docs/webflow_cloud_dataset_template_handoff.md
docs/wec_packing_project_overview_handoff.md
docs/8778_tack_horses_change_log_handoff.md
docs/hps_horses_webflow_airtable_connector_readme.md
```

Rules:

```text
- Webflow Cloud is the Airtable credential boundary.
- After env changes, redeploy Webflow Cloud.
- Verify health endpoint, live GET, then stop for explicit approval before live PATCH/POST.
- Do not let frontend code look saved if Airtable rejected the write.
```

## Manual Webflow Embeds And Static Assets

Manual embed/drop source examples:

```text
docs/horseshowing/webflow-drops/
webflow/<module>/
```

Static asset rule:

```text
Use pinned jsDelivr commit URLs when testing. Avoid relying on @main because CDN cache can hide whether the intended build is running.
```

Typical proof:

```text
1. HTML/TXT parity if a pasteable embed has both.
2. Browser DOM confirms live page has expected markers.
3. Browser network confirms intended static asset URLs.
4. Published page is checked, not only local preview.
```

## Catalyst / WEC / Horseshowing

First-read contract:

```text
docs/horseshowing/locked-workflow-gate-mcp-first-2026-06-17.md
docs/horseshowing/wec-end-to-end-handoff-v0.1-2026-06-17.md
```

Known tooling from contract:

```text
Catalyst org: 700800454
Catalyst project: horseshowing | 5614000000393031
Catalyst CLI: zcatalyst-cli@1.26.1
Airtable MCP CLI: @airtable/mcp-cli@0.2.5
```

Operating rule:

```text
Catalyst is the workflow system.
Airtable is a mirror/support surface except for approved manual levers.
PowerShell is an operator tool only; it must not become the workflow.
```

Stage order:

```text
focus_show
  -> get_ring_days
  -> update_schedule
  -> update_schedule_staging
  -> class_start_times
  -> class_oog
  -> entry_go_times
  -> get_orders / get_rings live enrichment
  -> alerts
  -> results
  -> rich endpoint
  -> wec-mobile / wec-mobile-pro / wec-print / SMS
```

## Cloudflare / PDF Worker

Use Cloudflare only when it is the actual runtime/proxy/PDF lane, not as a default answer.

Known current example:

```text
PDF worker: https://ringstatus-pdf.gombcg.workers.dev/
```

For print/PDF work:

```text
- Treat Webflow print page as the precise render source when that is the contract.
- Treat the PDF worker as the user-facing PDF conversion route.
- Verify the PDF response header/content, not just that a URL opens.
- Keep mobile and print embeds separate.
```

Installed/session Cloudflare skills include:

```text
cloudflare:cloudflare
cloudflare:workers-best-practices
cloudflare:wrangler
cloudflare:web-perf
cloudflare:durable-objects
cloudflare:agents-sdk
cloudflare:building-mcp-server-on-cloudflare
cloudflare:building-ai-agent-on-cloudflare
cloudflare:sandbox-sdk
```

## Installed Skills Inventory

This inventory is session-derived. A runner must still check the active Codex session because tools/skills can change.

### RingStatus Repo-Local

```text
skills/rs-css-audit/SKILL.md
```

Use for selector-level CSS audit work. Preserve existing Webflow shell/class/layout contracts.

### Airtable

```text
airtable:airtable-cli
airtable:airtable-filters
airtable:airtable-overview
```

### Webflow

```text
webflow-cli:cloud
webflow-cli:code-component
webflow-mcp:custom-code-management
webflow-mcp:designer-tools
webflow-mcp:flowkit-naming
webflow-mcp:safe-publish
```

### Superpowers

```text
superpowers:using-superpowers
superpowers:systematic-debugging
superpowers:test-driven-development
superpowers:verification-before-completion
superpowers:writing-plans
superpowers:executing-plans
superpowers:requesting-code-review
superpowers:receiving-code-review
superpowers:finishing-a-development-branch
superpowers:dispatching-parallel-agents
superpowers:subagent-driven-development
superpowers:using-git-worktrees
superpowers:brainstorming
superpowers:writing-skills
```

Reference rule:

```text
Use Superpowers when the task is debugging, TDD, verification, review, planning, or multi-agent execution. It is a discipline layer, not a replacement for repo contracts or live evidence.
```

### GitHub / Deployment / Web Apps

```text
github:github
github:gh-address-comments
github:gh-fix-ci
github:yeet
build-web-apps:frontend-app-builder
build-web-apps:frontend-testing-debugging
build-web-apps:react-best-practices
build-web-apps:shadcn
build-web-apps:stripe-best-practices
build-web-apps:supabase-postgres-best-practices
vercel:vercel-api
vercel:vercel-cli
vercel:deployments-cicd
vercel:nextjs
vercel:react-best-practices
vercel:verification
```

### Browser / Design / Documents

```text
browser:control-in-app-browser
figma:figma-use
figma:figma-generate-design
figma:figma-code-connect
canva:canva-branded-presentation
documents:documents
presentations:Presentations
spreadsheets:Spreadsheets
product-design:get-context
product-design:ideate
product-design:image-to-code
```

### Microsoft / Communication

```text
outlook-email:outlook-email
sharepoint:sharepoint
teams:teams
```

### Twilio

Installed Twilio Developer Kit skills cover messaging, SMS, WhatsApp, SendGrid, voice, webhooks, compliance, sender setup, reliability, and security. Use the specific Twilio skill for Twilio work, but do not assume Twilio is part of a RingStatus lane unless a repo contract or user decision says so.

Examples:

```text
twilio-developer-kit:twilio-messaging-overview
twilio-developer-kit:twilio-sms-send-message
twilio-developer-kit:twilio-webhook-architecture
twilio-developer-kit:twilio-sendgrid-email-send
twilio-developer-kit:twilio-voice-twiml
twilio-developer-kit:twilio-security-api-auth
```

## Runner Preflight Checklist

Before editing or live writes:

```text
1. Read docs/ringstatus_runner_options_overview.md.
2. Identify lane: Airtable, Webflow MCP, Webflow Cloud, manual embed, Catalyst/WEC, Cloudflare/PDF, static asset, or local runner.
3. Check current session tools/skills.
4. Inspect git status.
5. Confirm source-of-truth doc/source/live surface.
6. Confirm exact page, route, base, table, view, env var, or endpoint.
7. For data writes, prove GET/read first.
8. Stop for approval before PATCH/POST/live publish unless already approved.
9. Verify with live API/browser/render evidence after changes.
```

## Drift Warnings

Do not:

```text
- Treat Webflow page content and Webflow Cloud API routes as the same thing.
- Paste a mobile embed into a print page or a print embed into a mobile page.
- Use Airtable as a direct browser write target.
- Invent Airtable field/table names.
- Use PowerShell as a production workflow.
- Treat static JSON as current truth when Catalyst is the active workflow source.
- Apply HPS/8778 assumptions to WEC unless the user explicitly asks for that pattern and the fields/routes are verified.
- Claim PASS from local proof only when the live page/API is the target.
```

## Minimal Handoff Prompt For A New Runner

```text
You are in the RingStatus repo. Start by reading docs/ringstatus_runner_options_overview.md and this handoff. Before acting, identify whether the task belongs to Airtable MCP/CLI, Webflow MCP, Webflow Cloud/Astro, manual Webflow embed, Catalyst/WEC, Cloudflare/PDF worker, static GitHub/jsDelivr assets, or local runner scripts. Check installed skills and prefer MCP/CLI/live evidence over memory. Use Superpowers for debugging/TDD/verification when applicable. Do not expose Airtable tokens in browser code. Do not live-write or publish without explicit approval. For WEC, Catalyst is the workflow system and Airtable is support/manual-control unless the contract says otherwise.
```
