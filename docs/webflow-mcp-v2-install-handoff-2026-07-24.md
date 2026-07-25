# Webflow MCP v2.0 Installation Handoff

Date: 2026-07-24  
Repository: `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus`

## Objective

Install and authenticate Webflow MCP v2.0 for Codex, then prove availability through a read-only Webflow operation.

## Current status

**INSTALL PASS**

- The stale `webflow` MCP registration was removed.
- The official Webflow MCP endpoint was registered cleanly:
  - Name: `webflow`
  - URL: `https://mcp.webflow.com/mcp`
  - Transport: streamable HTTP
  - Enabled: `true`
- Webflow OAuth completed successfully after the clean reinstall.
- A fresh Codex process initialized Webflow MCP and successfully called:
  - `mcp__webflow.webflow_guide_tool`

**READ-ONLY SITE-LIST PROOF OPEN**

- The fresh process reached `mcp__webflow.data_sites_tool`.
- The call did not execute because its approval request was cancelled inside the separate non-interactive acceptance-test process.
- This was not an MCP initialization or OAuth failure.
- No Webflow site data was changed.

## Exact completed commands

```powershell
codex mcp remove webflow
codex mcp add webflow --url https://mcp.webflow.com/mcp
codex mcp login webflow
codex mcp get webflow
```

The successful OAuth result was:

```text
Successfully logged in to MCP server 'webflow'.
```

## Acceptance evidence

Fresh-process tool results:

```text
mcp: webflow/webflow_guide_tool started
mcp: webflow/webflow_guide_tool (completed)
mcp: webflow/data_sites_tool started
mcp: webflow/data_sites_tool (failed)
user cancelled MCP tool call
```

This proves the Webflow MCP server and v2 tool surface loaded. It does not yet prove site-data access because the read approval was not accepted.

## Next-task instructions

1. Open a new Codex task so the newly installed Webflow tools are loaded into the task inventory.
2. Keep the first pass strictly read-only.
3. Call `mcp__webflow.webflow_guide_tool` first if required by the loaded Webflow guidance.
4. Call `mcp__webflow.data_sites_tool` with the `list_sites` action.
5. Approve the Webflow read request when Codex presents it.
6. Report:
   - exact tool names called;
   - accessible site count;
   - accessible site names and IDs;
   - `PASS` only if the site-list call returns successfully.
7. Do not create, update, delete, or publish anything during this acceptance test.

## Copy/paste prompt for the next task

```text
READ-ONLY VERIFY ONLY. Test the newly reinstalled Webflow MCP v2.0 connection. Call the Webflow guidance tool first if required, then call data_sites_tool with list_sites. Present the approval request for me to approve. Do not create, update, delete, or publish anything. Return PASS only with the exact tools called and the returned site count, names, and IDs; otherwise return FAIL with the exact error.
```

## Important distinction

The MCP installation and OAuth authentication are complete. The only remaining gate is an approved read call that returns the accessible Webflow sites.
