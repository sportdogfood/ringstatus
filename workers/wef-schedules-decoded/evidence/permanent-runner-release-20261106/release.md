# Permanent runner release

Owner authorized permanent runner/tunnel restoration, deployment and live schedule/class-detail verification, with agent URLs unchanged and no collectors.

Started existing wef-decoder-runner-relay scheduled task. Existing permanent wef-decoder-named-tunnel stayed running. Installed relay, decoder and public-route files match the successful isolated preview sources; none changed. Redeployed existing shared-worker.mjs through existing Wrangler config workers/wef-schedules-decoded/wrangler.jsonc. Deployment version: 25310435-3dda-4d8b-912f-bbbca5a62e53.

Live proof: live-verification.json; eight HTTP200 usable decoded responses across wef-schedules-decoded and broad-tooth-b8ed, schedule/class detail, two rounds 20 seconds apart. Schedule show200000077/date2026-11-06:7rings. Class200031233:8trips with entry IDs. Actual field content checked, not only HTTP status. No collectors, records, credentials, task definitions or agent URLs changed.

Runtime state readback: runtime-state.json; relay and tunnel Running, one listener18467, existing restartCount99 and no execution time limit. Cause of prior process interruption remains unknown. No claim of unattended long-term reliability.

Known earlier review finding remains outside this deployment: decoder accepts empty upstream objects as success. This release establishes useful output for the tested live routes; it does not claim that validation defect is repaired. Existing review packet: ../cloudflare-direct-preview-20261106/code-review/review.md.
