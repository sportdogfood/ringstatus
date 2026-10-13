# Remaining server summary slice — rec7FIzlG6f9Qd9f3

Existing source baseline `18e6e993aeaff6fd53168dac4d6f1b7b46baf195` under `C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test`. Protected local public no-navigation and browser confirmation cleanup remain. No deployed proof for either delta.

Target is existing Sessions `tblBgvP7pYSv8o20j`; verified schema `recognize-sessions-schema-20261010.json`. Existing Footprints key config.sessions remains `tblWjbASVMIjFLyW8` and its dedup/event hash is unchanged. Do not rename the old key or introduce a new store/schema/service.

Required local behavior: create/reuse one summary for eligible per-tab session; server-confirmed person/device only; preserve started_at, monotonically update last_activity_at; correct terminal lifecycle without identity rebinding. Rejected visitor never gains identity or permissions. Client tab UID is not authenticated identity.

Use existing D1 claim table for a distinct summary ownership namespace. Immutable footprint claims must remain unchanged. Summary updates must have one durable winner; contenders fail closed and may retry through normal client flow. After acquiring ownership, use current provider state/time so activity cannot regress. Validate previous bound person/device and server-side token fingerprint before changing an existing summary. Never use caller-supplied person/device IDs. Unknown/denied guessed UID must not retire another device's summary.

An uncertain external outcome retains claimed ownership, following the existing no-release safety convention. Do not expire a claim, retry a create until a result appears, or assert Airtable and D1 are transactional. Report the availability limitation honestly. An existing terminal/identity-conflicting session must not silently reopen or rebind; define and test the minimal safe client rotation/error response.

Actual adapter `recognizeDevice` exposes active person/device IDs, and device='retired' for retired rejection; other denied results lose IDs/reason. Therefore previous bound summary may conservatively end on denial after server-side token ownership matches; explicit revoked status only with supported retired evidence. No inferred revocation reason. Unbound unknown visits need no authenticated summary link. Shared adapter is protected; any necessary change requires a new exact reservation before edits.

Additive success metadata, if needed: session {session_uid,person_record_id,device_record_id,started_at,last_activity_at,status}, recorded to Agents and Tenants before implementation. Existing response fields/cookie/gated behavior/tenant access remain. Metadata is display context, never authorization.

Acceptance must cover create/reuse/refresh/navigation, monotonic concurrent activity, one winner, uncertain write failure, retry reconciliation, forged UID, account switch, terminal status and unchanged Footprints dedup/Inputs tests. Local mocks/build are not published acceptance. No production endpoint, record write, SMS, deployment, publication, new runner, new database, or commit authorized here. Preserve unrelated owner/sibling changes.
