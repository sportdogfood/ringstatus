# RingStatus Codex Instructions

## Preserve proven implementations — owner rule, 9 October 2026

- Never reinvent, replace or build a parallel solution where the owner supplies a working implementation or documented proof of concept, unless the owner explicitly requests that replacement. A request to connect, migrate, style, repair or finish is not replacement authorization.
- Before planning code or dispatching a coding agent, inspect the supplied implementation and relevant existing repository history. Identify its exact path/commit, observed working behavior, dependencies, current target and the smallest authorized delta. Preserve this baseline in the task record and handoff; do not require the owner to rediscover it.
- Changed base IDs, schemas, credentials, hosting or presentation call for identifying the compatibility gap and adapting only the authorized boundary, not recreating the established logic. Historical proof does not establish current live operation, but missing fresh proof is not permission to rebuild.
- Never patch over a failed patch. On failure, compare the failing path with the proven baseline, establish the root cause and account for prior edits before proposing the next correction. Do not keep modifying a guessed replacement to make it work. Preserve user changes; any rollback or source correction requires applicable authorization. A failed approved runner workflow still stops under the runner rules.
- An owner reminder that working code already exists immediately requires rebalance to that source before further dependent edits. If the source cannot be accessed or cannot meet a stated requirement, report the exact gap; do not invent an alternative. Explicit owner authorization is required for replacement.
- Completion evidence must show the requested delta works and the baseline behavior remains intact. More passing local tests, a new design or a rewritten implementation does not substitute for the original end-to-end acceptance.

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
