# Approved shared pacing

Owner requested1second pause across agents, local-first, fallback/URLs unchanged, preview beforedeployment,no collectors. Explicitly APPROVED TO ADD shared queue following proposal in evidence/shared-pacing-proposal-20261106.

One Durable Object SglPacer, bindingSGL_PACER, one globally named queue, next-allowed timestamp persisted. Serialize proxy requests through existing local-first/fallback handler; one-second gap after response consumption. Only queue module, shared entry/export, configbinding/migration, focused testloader/tests and documentation/evidence change. No runner/header/token/fallback logic, credential, URL, dependency or collector changes. Direct SGL bypasses and internal renewal calls remain outside agent-request pacing.

Use an authenticated isolated Cloudflare preview Worker because SQLite-backed objects do not support legacy remote development. It has its own namespace, routes and temporary copied credentials; no production binding used. Delete only these specifically named temporary resources after proof. No public preview access withoutexistingkey.
