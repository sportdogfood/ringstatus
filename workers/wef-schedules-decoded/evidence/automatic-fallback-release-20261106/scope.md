# Automatic fallback authorization

Owner: Deploy this as automatic fallback; keep working local path and agent URLs unchanged; verify schedule/class via fallback without stopping runner; no collectors. Earlier approved empty-data guard is local candidate with12passing tests.

Baseline: shared-worker.mjs.before is active local-first implementation; runtime runner files match previously verified production. Preserve all preexisting edits.

Delta: add only direct-TLS schedule and class-detail fallback on primary failure or empty object, reuse existing decoder and existing SGL_COOKIE secret. Leave other routes, local runner/tunnel, credentials, primary timeout, task definitions, URLs and dependencies unchanged. Add a secret-authorized fallback verification header (existing RUNNER_KEY) and source-lane response header so live proof can bypass local without stopping it. No public diagnostic route. Preview tests precede deployment. Runtime decoder is not replaced or runner restarted.
