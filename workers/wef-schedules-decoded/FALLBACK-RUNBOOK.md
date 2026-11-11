# Decoder recovery test runbook

Updated November 10, 2026, America/New_York.

## Installed recovery order

The permanent runner is first. If it fails or returns unusable data, the existing Cloudflare fallback tries these profiles in order.

1. `original-header`
2. `profile-fb-header1`
3. `profile-fb-header2`
4. `windows-clean`
5. `android-clean`

Stop on the first usable decoded response. If all five fail, renew the anonymous token once and repeat the same five once. If renewal fails, stop that recovery cycle and return failure. The existing shared queue and at least one second between recovery attempts remain in force.

The token request adds `Sec-Fetch-Site: same-site` to its existing headers. Cookie and credential bindings are unchanged. Token renewal precedes schedule/class fetching when the cache is absent or near expiry. A token failure can prevent every data profile from reaching SGL.

The two new profiles are the exact combinations tested in preview. Windows uses desktop Edge 158 and matching Windows client hints with no ordinary authority/method/path/scheme fields. Android uses the tested Android Edge 157 User-Agent and omits ordinary path. Existing profiles remain available in their previous order.

## First checks during an outage

1. Read the existing [health record](https://airtable.com/appCaSmL819KK7UFJ/tblcYgKFwKtILSdU8/recBHZPxfVwugzO6s) and note its timestamp. Read attempts, lane, profile, runner status, stage, and reason.
2. For runner HTTP 530 or connection errors, inspect the two existing Windows tasks and test TCP port `18467`. Inspect the Cloudflare tunnel's connection count. An active tunnel alone does not establish that the relay is listening.
3. For `token-before-schedule`, inspect the matching [token refresh log](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP). Record HTTP status and the redacted error. A successful HTTP status with no `_t` remains a token failure.
4. For data failures, distinguish upstream HTTP errors, redirects, invalid JSON, decode failure, and empty objects. Preserve the source show and date. Compare the exact installed runner with the dated snapshot because its runtime directory is Git-ignored.
5. Use the existing scheduled health result to verify normal recovery. It must have a new timestamp, `ok=true`, and a usable `runner` attempt to verify the primary path. Accept legitimate zero rings. Nine rings is the expected result for the specific dated examples below, not a permanent gate.

No collector, token refresh, restart, credential rotation, or record repair is a substitute for the approved recurring verification. Diagnostic requests need authorization. Do not invoke health manually when the instruction is to observe the next scheduled check.

## Offline verification before deployment

From the approved repository root, run

```powershell
node --test workers/wef-schedules-decoded/tests/fallback.test.mjs workers/wef-schedules-decoded/tests/schedule-health.test.mjs workers/wef-schedules-decoded/tests/pacing.test.mjs workers/wef-schedules-decoded/tests/worker.test.js
```

These tests cover both new winners, existing profile order, first-success stop, five-attempt exhaustion, one renewal and one repeated sweep, token rejection, redaction, the shared one-second pause, runner-first behavior, and empty-source rejection. They use fixtures and do not contact SGL or Airtable.

## Cloudflare preview before deployment

Use the existing Wrangler remote-preview route. Copy the candidate modules to an isolated temporary directory and load only the existing SGL cookie required by the preview. Keep production route bindings and runner files unchanged. Never print token or cookie values.

For each new profile, inject failures for the earlier profiles only inside preview. Exercise the real shared recovery function and real direct TLS fallback for the selected profile. The preview-only failure wrapper must not be included in the deployed Worker.

Verify a fresh anonymous token, then request

| Request | Expected result for this release |
| --- | --- |
| `/schedule?date=2026-11-14&show_id=200000078&customer_id=15` | HTTP 200, returned show/date match, nine rings |
| `/schedule?date=2026-11-25&show_id=200000081&customer_id=15` | HTTP 200, returned show/date match, nine rings |
| A class ID read from the November 14 schedule | HTTP 200, returned class ID matches, decoded trips array |

Both `windows-clean` and `android-clean` must pass. Record lane, selected profile, identity, counts, and status. Stop the temporary preview and remove its private cookie file.

## Live diagnostic verification after approved deployment

Allow the existing Durable Object rollout to finish. New ingress requests can temporarily execute recovery inside the previous queue version. Check the version ID on a stored `Using direct source fallback` log against the deployment ID before accepting or repeating the fallback test. A response from the old version must be retained as old-version evidence. Do not patch or redeploy solely because that old version still fails.

Use the established shared Worker URL. An ordinary schedule request verifies the runner-first path. The existing private `X-RS-Fallback-Test` header, set to the installed runner key, exercises fallback without stopping the runner. Load that key from its existing private environment file and never print it. This test switch already exists and does not add a route or change agent URLs.

The ordinary schedule should report `X-RS-Source-Lane: runner`. A forced fallback should report `cloudflare-fallback` and `X-RS-Header-Profile`. Inspect the response body for show/date identity and the rings or class/trips result. A forced request can stop on an earlier successful profile. Individual-profile isolation belongs in preview.

Leave at least one second between diagnostic requests. Save redacted receipts. Verify protected hashes for installed runner modules, pacing, transport, Wrangler configuration, and caller code. Observe the next normal scheduled health separately.

## Evidence and history

The existing hourly monitor appends health checks to [decoder-run-log](https://airtable.com/appCaSmL819KK7UFJ/tbluyUciGZCt3xnsB) using the saved check timestamp as part of run-id. Warning events are mirrored once in [decoder-fail-alerts](https://airtable.com/appCaSmL819KK7UFJ/tblJZga4wx3JV3LCX) with the existing warning ID and native warning link. Delivery status is a lookup from that warning. Copy its exact delivery-at timestamp to the alert after submission; preserve failed-delivery details in the existing warning. Record verified changes in [decode-change-log](https://airtable.com/appCaSmL819KK7UFJ/tblhL9n1COVPvr851). Run records must distinguish normal scheduled health, preview, and authorized live diagnostics. Do not relabel historical imports or diagnostic results as new scheduled execution.

The same hourly monitor reads the Cloudflare tunnel status and connection count. A single QUIC connection timing out and re-registering within seconds is a reconnect event. The supplied November 10 logs showed connections 0 and 2 re-registering within two and eighteen seconds respectively. Preserve those timestamps as tunnel events. Alert on confirmed loss of all connections and on recovery once. Current task configuration has no persistent tunnel logfile or continuous console watcher. The hourly check can miss events between checks.

The [README incident history](README.md#repeat-incident-checks) records October and November causes, corrections, and unresolved stop events. The [November 10 release evidence](evidence/fallback-profile-release-20261110/) records the current baseline, tests, Cloudflare preview, review, deployment, and live verification. Record failures as well as successes. A changed request profile does not prove why SGL's behavior changed.
