# Recognize current binding review — 10 October 2026

Worker: `/root/recognize_completion`, actual internal subagent dispatched by coordinator `/root`. Scope: read-only application discovery and existing work-record updates. No source edits, deployment, publication, live messages or business-record changes.

## Established baseline

Inspected checkout: `C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus`, HEAD `9d11db06d34f4c24964b4f36b0eb4b089e6b7f84`.
Live Cloud environment `110f06dd-c1ea-4839-98af-d829cbe77941`, app `d7d97751-20e1-4148-a5cf-ee58671c128a`, branch `work/inputs-crm-20261006`, mount `/test`, reports success at `2026-10-10T15:11:33.176Z`; D1 binding `RS_RECOGNITION_CONTROL_DB` exists. Existing release evidence identifies deployment `22961c5a-b046-4bd9-8632-7d45bf5ffb01`; current environment read independently confirms matching success time, not a fresh server-source export.

Fetched public JavaScript was compared with existing source after CRLF normalization: both `client.js?v=1` and `native-client.js` match their source files exactly. Public HTML and script GETs were read without executing JavaScript, cookies, recognition requests or SMS. This is binding/source evidence, not visitor-journey proof.

Original owner proof `f012c3077:.../src/assets/rs-recognition/client.js` was inspected: it has the same legacy path allowlist and generated UI approach. Its existing behavior is evidence to preserve where applicable, not the intended native public-footer implementation.

## Actual page → code → data map

| Target | Current evidence | Required action |
|---|---|---|
| `/`, `/ring-status`, `/about-ringstatus` | All publish `/test/rs-recognition/client.js?v=1`. Legacy script allowlist includes `/` but excludes the other two. It injects its own recognition UI and uses cross-origin `ringstatus.webflow.io/test/rs-recognition/{device,session,action}`. | Use existing native silent operation with no presentation or navigation on these three public pages. Preserve original `/recognize` proof and other legacy pages. |
| `/rs-recognize` | Published `data-rs-native-loader=v1` imports current-origin `/test/rs-recognition/native-client.js`, supplies native presenter and `launcherPath: /rs-member`. | Preserve this binding and native styling; no redirect correction needed here. |
| Native client | Converts base URL to current-origin `/test/rs-inputs/native-recognition`; same-browser `otp_start` / `otp_check`; silent operation posts session UID and page path. | Public use needs explicit no-redirect behavior; current `runSilentRecognition` navigates on recognized success. |
| `/rs-member` | HTML GET 200; no recognition loader identified. | Do not infer a gate or invent launcher-button destinations. Keep gate designation as existing open task. |
| Recognition cookie | Server validates device/person; issues/refreshes `__Host-rs_recognition_device`, HttpOnly, Secure, SameSite=Lax, Path=/, 365 days. | Retain; browser retention is not guaranteed. Live expiry/revocation journey still untested here. |
| Browser session | `rs_native_recognition_session_v1` reused in sessionStorage. No reusable recognized-result state in native silent path. | Preserve server revalidation; maintain minimal server-confirmed state and clear it on rejected recognition. Do not introduce cached authorization. |
| Sessions | Live table `tblBgvP7pYSv8o20j` exists with session_uid/person/device/started_at/last_activity_at/status. No reference to its ID anywhere in inspected `src`. | Connect existing session ID to this existing table; no new table/database. |
| Footprints | `rs-recognition-config.js` key `sessions` maps to `tblWjbASVMIjFLyW8` (Footprints), used by `recordRecognitionSession`. Silent event deduplicates by hash(session UID + device token). | Preserve current event behavior; session summary is separate. Do not silently invent per-page footprint cadence. |

Core destination is strictly `app9kOZdIaGyKk5uG`: People `tbly1PM5iFYqVzKSm`; Recognition devices `tblfkRSJAEMzuzApR`; aliases `tblgDWKi0Bb6OcoqS`; Footprints `tblWjbASVMIjFLyW8`; SMS verification requests `tblxC4SYtzYhJNcJS`; existing SMS attempt/outcome log `tblF1Hdqi3yoKTZPV` (`___rs_sms_events`). Prefixing legacy tables did not remove these runtime dependencies. Six legacy Inputs addresses are already stable IDs; do not redo that completed work.

## Existing SMS path — current configuration read

Automation `wfl6oiGMgUPmEkUuS`, `sms-recovery`: deployed, valid; deployedVersion null means no distinct deployed configuration returned. Trigger reads SMS verification requests and its existing status/record-mode conditions. Three nodes: authenticated `sms_prepare` callback → existing native Twilio send action → authenticated `sms_outcome` callback. Callback host is `ringstatus.webflow.io/test/rs-inputs/native-recognition`. Outcome intentionally records unknown with `native_twilio_no_provider_reference`; do not relabel it delivered. No credentials or message bodies are reproduced here.

OTP code already normalizes 10-digit US numbers to +1, uses existing D1 OTP store, consumes codes before binding the initiating browser and remains restricted to configured owner test recipient. This restriction is current source policy, not an observed error. Broader rollout requires the owner-approved release scope; do not remove it during discovery.

## Minimal completion sequence

1. Existing page task `recpJKchoFBgvqyj9`: adapt existing native silent function with an explicit public/no-navigation mode, bind only the three designated public pages; remove only their conflicting legacy loader invocation. Preserve `/rs-recognize` launcher and original legacy proof. Test known/unknown public stays, gated redirect, and no injected UI.
2. Existing session task `rec7FIzlG6f9Qd9f3`: use existing browser session UID to create/reuse the existing Sessions summary with server-confirmed person/device and activity; retain Footprints writer and its current deduplication. Reuse existing atomic storage if necessary, never a process-local lock or new service. Test refresh/navigation, rejection/revocation, and account switch without stale identity.
3. Existing journey task: qualify actual desktop same-browser code entry, mobile, silent return, public noninterruption, /rs-member, correct session/footprint records, invalid/expired/reused code and retry behavior against the published release. Current discovery did not run any of these write-producing tests. Use only the owner-authorized recipient when live sending is separately in scope.

Rollback: preserve this exact baseline and saved page-loader snippets; reverse only approved deltas, preserving unrelated Inputs and owner changes. Current assignment ends at an actionable scoped implementation handoff, not runtime completion.

## Instructions actually read

- Repository `AGENTS.md` and supplied global/runner boundary.
- `docs/coordination/recognize-completion-handoff-20261010.md` and workflow overview; stale not-dispatched and six-name statements superseded by actual dispatch and naming record.
- Airtable `airtable-overview/SKILL.md`.
- Webflow `webflow-cli-cloud/SKILL.md` relevant existing-project/deployment sections; no CLI deployment run.
- `webflow-mcp-custom-code-management/SKILL.md`; Webflow guide called, no mutations.
- `verification-before-completion/SKILL.md`.

Tool limitations recorded: first environment request rejected missing label/extra site_id without execution; corrected request succeeded. Unsupported list_deployments action rejected without execution; no further guessing or retries. No runtime failure was induced or diagnosed, and no browser-testing/debugging skill is claimed loaded.
