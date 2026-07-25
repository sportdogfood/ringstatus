# Webflow MCP v2.0 Runner Handoff

Date: 2026-07-24  
Repository: `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus`  
Official release: <https://developers.webflow.com/home/changelog/2026/7/21>

## Runner objective

Use the installed Webflow MCP v2.0 server for native Webflow inspection and approved changes. Do not assume that Webflow Designer or the Bridge app must be open for ordinary element, component, style, variable, page, asset, custom-code, form, font, sitemap, analytics, or agent-instruction operations.

## Connection state

**INSTALL PASS**

- MCP name: `webflow`
- Endpoint: `https://mcp.webflow.com/mcp`
- Transport: streamable HTTP
- OAuth: completed successfully after a clean reinstall on 2026-07-24
- Confirmed tool initialization: `mcp__webflow.webflow_guide_tool` completed successfully in a fresh Codex process

**SITE ACCESS PROOF OPEN**

- `mcp__webflow.data_sites_tool` was discovered and reached.
- Its `list_sites` call was cancelled by the approval boundary in a non-interactive test process.
- The next interactive runner must request and receive approval for that read, then return the live site names and IDs.

## Mandatory first pass

The first pass is read-only.

1. Load the Webflow MCP tools in a new Codex task.
2. Call the current Webflow guidance tool first when the loaded tool contract requires it.
3. Inspect the live v2 tool schemas. Do not guess action names or payloads from an older skill.
4. Call the site-list action through the current site tool.
5. Present the MCP approval request to the user.
6. After approval, report the exact site count, names, IDs, and workspace scope.
7. Return `PASS` only if Webflow returns the site list. Otherwise return `FAIL` with the exact tool and error.

No direct API, browser automation, legacy endpoint, or manual command may substitute for MCP proof.

## MCP v2.0 operating model

Most design work is now server-side and does not require the Bridge app:

- query and edit element trees;
- create, move, transform, and remove elements;
- edit text, links, images, attributes, tags, settings, visibility, and display names;
- build page elements from schemas or HTML/CSS;
- create and manage components, instances, props, and variants;
- create and update classes, combo classes, variables, collections, and modes;
- manage pages, assets and folders, custom code, uploaded fonts, forms and submissions, sitemap indexing, Webflow Analyze data, and site Agent Instructions.

The Bridge app is still required for:

- element snapshots;
- current Designer selection and canvas navigation;
- current page, mode, branch, and breakpoint state;
- page-folder creation;
- uploading an image directly from a public URL.

Webflow Interactions/IX3 are not supported by MCP v2.0.

## Legacy-tool migration warning

Do not blindly follow old Webflow skills or prompts. MCP v2.0 renamed or relocated actions. Known examples:

- `set_id` became `set_dom_id`;
- `add_or_update_attribute` became `set_attributes`;
- selection and canvas navigation moved to Designer-session capabilities;
- asset and folder management moved away from the URL image-upload tool.

The runner must use live tool discovery and the current tool schema as the contract. If a repo skill conflicts with the exposed v2 schema, stop using the stale action name and report the mismatch.

## Pre-write gate

No Webflow mutation is authorized by this handoff.

Before any create, update, move, delete, custom-code change, asset operation, form-submission change, or other write:

1. Verify the exact workspace, site, page, locale, branch, and target IDs through MCP readback.
2. Read any Webflow Agent Instructions returned for the site.
3. Present a pre-write report containing:
   - exact target;
   - current readback;
   - proposed actions;
   - expected result;
   - rollback or preservation path;
   - whether the operation needs the Bridge app.
4. Obtain explicit user approval.
5. Execute only the approved bounded changes.

Do not publish unless the user separately and explicitly authorizes publishing.

## Verification rules

After an approved write:

1. Read back every changed field or element through MCP.
2. Compare the live result with the approved plan.
3. If visual proof is required, open Webflow Designer, connect the Bridge app, and capture an element snapshot.
4. Treat a failed or unavailable required snapshot as `FAIL`.
5. Preserve the previous subtree/content until readback and required visual verification pass.
6. Do not clean up old content or publish after a failed verification.

Publishing to a staging domain can support an independent browser-based visual loop, but it still requires explicit publish authorization.

## Authorization and workspace boundary

One OAuth authorization grants access to one Webflow workspace. If the expected site is absent:

- do not guess another site or workspace;
- report the authorized workspace and returned sites;
- request reauthorization for the correct workspace.

If initialization reports an invalid or expired OAuth grant, return `FAIL`. Do not claim the MCP is operational based only on its local registration.

## Exact next-run prompt

```text
READ-ONLY VERIFY ONLY. Use the installed Webflow MCP v2.0 server. Load and follow the current Webflow MCP tool schemas; do not rely on legacy action names. Call the Webflow guidance tool first if required, then request approval for the read-only list-sites action. Do not create, update, move, delete, upload, or publish anything. Return PASS only with the exact MCP tools called and the live workspace/site count, names, and IDs. Otherwise return FAIL with the exact tool and error.
```

## Installation record

The detailed installation and OAuth evidence is in:

`docs/webflow-mcp-v2-install-handoff-2026-07-24.md`
