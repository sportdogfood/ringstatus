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

## Current six-page normalization contract

LPX is the canonical page stack. The runner must resolve and read back all six exact page IDs before proposing any mutation.

| Area | Status | Current condition | Required work |
|---|---|---|---|
| Canonical page stack | **OPEN** | Pages do not share one identical structural stack | Define LPX as the canonical stack and normalize all six pages to it |
| `lpt-shell` | **OPEN** | Shell contents and nesting are inconsistent between pages | Make the shell hierarchy identical across all pages |
| Horses nesting | **OPEN** | Contact and Social are grouped inside an extra wrapper/div | Remove the inconsistent wrapper and place both sections at the canonical stack level |
| Contact default state | **OPEN / FAIL** | Contact appears visible | Ensure `lpt-contact-panel-native` is applied and natively hidden by default |
| Contact toggle | **OPEN / FAIL** | Contact JavaScript was removed during the slider-only correction | Implement the approved Contact behavior without contaminating slider-only JavaScript |
| Contact classes | **KNOWN** | Base: `lpt-contact-panel-native`; open state: `lpt-contact-panel-open` | Verify both classes exist and are applied consistently on every page |
| `lpt-social` | **OPEN / FAIL** | Social structure is not identical across pages | Normalize identical native structure and Instagram/YouTube links across every page |
| Filters using `lpt-pill` | **UNKNOWN / OPEN** | The correction run was stopped without a final readback; one style call may have completed | Read back first, then ensure zero `lpt-pill` descendants inside `lpt-filters` |
| `lpt-pill-grid` desktop | **OPEN** | The 4x1 correction was interrupted before verified completion | Set native grid to four equal columns x one row |
| `lpt-pill-grid` small screens | **OPEN** | Previous 2x2 state was verified before later interrupted work | Re-read and verify two columns x two rows |
| Card lanes | **PASS** | Native 3-up desktop, 2-up tablet slider, 1-up mobile slider | No change |
| Card slider JavaScript | **PASS** | Slider-only previous/next scrolling | No change |
| Slider controls styling | **OPEN** | Controls lack an approved, verified native visual treatment | Style controls natively |
| Slider control visibility | **OPEN** | End-state hiding was not completed | Hide group when no overflow; hide Previous at start and Next at end |
| `lpt-nav` | **PASS** | Native sticky desktop and fixed-bottom mobile | No change |
| `lpt-hero-title` | **PASS** | Native small-mobile reduction | No change |
| Publishing | **PASS** | Nothing published | Keep unpublished |

### Required execution order

1. Read back all six page trees, relevant styles, custom code, links, and responsive grid state.
2. Preserve every **PASS** lane exactly; do not rewrite card lanes, slider-only JavaScript, `lpt-nav`, or `lpt-hero-title`.
3. Resolve the **UNKNOWN / OPEN** filter state from live readback before including it in a write proposal.
4. Compare each page with LPX and produce a page-by-page structural delta.
5. Produce the pre-write report required by this handoff and obtain explicit approval.
6. Normalize the canonical stack and `lpt-shell`, including removal of the extra Horses wrapper.
7. Normalize Contact structure, native default-hidden state, class application, and approved toggle behavior while keeping Contact logic separate from slider-only JavaScript.
8. Normalize `lpt-social`, including identical native Instagram and YouTube link structure.
9. Correct and verify `lpt-pill-grid` at desktop and small-screen breakpoints.
10. Apply the approved native slider-control treatment and end-state visibility behavior.
11. Read back every changed page and return this same matrix with each item marked **PASS** or **FAIL** and concrete MCP evidence.
12. Keep every page unpublished.

## Exact next-run prompt

```text
Use the installed Webflow MCP v2.0 server and the current six-page normalization contract in docs/webflow-mcp-v2-runner-handoff-2026-07-24.md. Start READ-ONLY: load the current MCP schemas, call Webflow guidance first if required, list sites, resolve the six exact pages, and read back their element trees, styles, custom code, links, and responsive states. Treat LPX as the canonical stack. Preserve every PASS lane and keep all pages unpublished. Return the page-by-page delta and a precise pre-write report, then stop for explicit approval before making any mutation.
```

## Installation record

The detailed installation and OAuth evidence is in:

`docs/webflow-mcp-v2-install-handoff-2026-07-24.md`
