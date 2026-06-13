# Webflow Embed Options

Version: 2026-06-12

Status: reference only. This is not part of the current WEC end-to-end workflow recovery stages.

## Recommended Direction

Use Codex MCP with the official Webflow MCP server for direct Webflow access when Webflow itself must be changed.

Do not make Codex repeatedly rewrite Webflow embed code for normal schedule or URL-hash logic changes.

Best setup:

```text
Webflow page or embed
-> stable loader
-> hosted JS file
-> Codex updates hosted JS in repo
-> deploy/publish hosted JS
```

Webflow should only need republish when the embed, script URL, page custom code, or Designer structure changes.

## Codex Side

Use Codex CLI or IDE extension with an MCP server.

Codex MCP can be configured through:

```text
~/.codex/config.toml
codex mcp
```

## Webflow Side

Use the official Webflow MCP server.

The Webflow MCP/API lane is for:

```text
site data
CMS
custom code
assets
styles
elements
Designer operations
```

## Permission Requirement

For custom-code updates, Webflow requires a Webflow App/OAuth flow with:

```text
custom_code:read
custom_code:write
```

Site tokens are not enough for custom-code endpoints.

## Best Architecture For URL Hash Updates

Preferred pattern:

```text
Webflow embed contains one stable loader.
Hosted JS controls URL-hash behavior.
Codex edits the hosted JS file in GitHub/local repo.
The hosted script is deployed.
Webflow is not edited unless the loader or script URL changes.
```

Hosted JS owns:

```text
read current window.location.hash
activate matching tab, section, or state
update hash on user click
listen for hashchange
```

## When To Use Direct Webflow MCP Injection

Use Webflow MCP/custom-code updates only for:

```text
adding the initial embed
changing the script URL
applying page-level custom code
creating or modifying Designer elements
```

Webflow custom-code flow:

```text
register script
apply script to site or page
publish
```

Page custom-code application requires:

```text
custom_code:write
```

## Recommendation For RingStatus/WEC

For URL-hash updates and WEC page behavior:

```text
Use Codex + GitHub/local repo + hosted JS file + one Webflow embed/custom-code loader.
```

Do not use:

```text
Codex directly rewriting Webflow embed blocks for every behavior change.
```

Reason:

```text
version control
rollback
local testing
deploy testing
less Webflow Designer/API drift
fewer embed parity failures
```

## WEC Stable Loader Install - 2026-06-12

Install these one-time Webflow embeds:

```text
Mobile page: docs/horseshowing/webflow-drops/wec-mobile-stable-loader.txt
Print page: docs/horseshowing/webflow-drops/wec-print-stable-loader.txt
```

Do not keep replacing full mobile/print embeds for routine WEC fixes.

Hosted render source:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-mobile-embed-html
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-print-embed-html
```

Catalyst asset source:

```text
ringstatus-data/catalyst-workspaces/horseshowing/functions/horseshowing_sync/webflow-embeds/wec-mobile.html
ringstatus-data/catalyst-workspaces/horseshowing/functions/horseshowing_sync/webflow-embeds/wec-print.html
```

When WEC render code changes:

```text
Update Catalyst asset.
Deploy horseshowing_sync.
Verify hosted action.
Verify local stable loader render.
Do not touch Webflow unless the loader endpoint changes.
```
