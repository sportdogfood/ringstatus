# Exact User-Agent on four direct WEF Live requests — November6,2026

Direct owner authorization: add only the supplied User-Agent to getLiveClassStatus,ListAjax,LiveScoreWidget,getLiveClassData. Preserve existing URLs,queryparameters,Accept/Cache-Control,timeout/retry/parsing and Cloudflare proxy. Verify each feed through normal scheduled WEF Live runs; no manual collector or provider probes. No other workflow behavior/schema/task/cadence/credentials/dependencies changed.

Current baseline:25installed wef-live JS hashes in runtime-before.json, task-before.xml and exact activity.js/source.js/shared reader snapshots. Deployment target is existing module loaded by existing hidden Windows task wef-live-collector. Source failure yielded hourly due checks; next real status check is09:39Eastern. Do not change checkpoint/timer to obtain proof.

Delta is two module files only. activity.js adds header to the sole direct status request. source.js injects a request callback into the existing reader only for the three exact Wellington host/path pairs, preserving all reader retry/response/decode behavior. Shared lib remains byte-identical; other collectors and decoded proxy retain original headers/URL. No header/config redesign,extra worker,endpoint or added gate. Evidence/docs here do not change production logs or behavior.

Offline request capture will verify exact header values,unchanged targets/other headers,all four request paths,proxy/default untouched,existing bounded retry behavior. Author review before installation. Normal source receipt/coverage and Airtable readback prove scheduled collection separately. Existing role/failed-path stops and unrelated lock/holds remain.
