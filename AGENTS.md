# RingStatus Codex Instructions

## Webflow MCP

The production Webflow MCP connection is already installed and authorized.

Persistent configuration:

```toml
[mcp_servers.webflow]
url = "https://mcp.webflow.com/mcp"
default_tools_approval_mode = "approve"
```

Required operating procedure:

1. Use only the MCP server named `webflow`.
2. Call `webflow_guide_tool` once at the beginning of Webflow work.
3. Call the requested Webflow tool directly after the guide.
4. Do not run `codex mcp login webflow`, logout, remove, reinstall, or replace the server as a connection check.
5. Do not launch OAuth proactively.
6. Do not add or use `webflow-beta` unless the user explicitly requests the beta server.
7. Do not repeat site discovery when the task supplies a verified site ID. The RingStatus site ID is `6982268b7543ac3c80151266`; revalidate only when the requested operation or live response indicates the identity may have changed.
8. If a Webflow tool returns an explicit authentication error such as `reauthenticationRequired`, `invalid_token`, or `invalid_grant`, stop and report that exact error. Do not automatically start a new authorization flow.
9. Webflow MCP 2.0 element-tree, component, style, variable, and page-building operations do not require the Bridge App. Use the Bridge only for element snapshots, selection/canvas navigation, current page/mode/branch/breakpoint reads, page-folder creation, or uploading an image from a public URL. Do not reinstall or reauthorize MCP because one of those Bridge-only operations is unavailable.
10. Never publish unless the user explicitly requests publishing.

Connection success is proven by a successful Webflow tool call. Configuration inspection, OAuth screens, and repeated health checks are not substitutes for the requested task.
