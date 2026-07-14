# RingStatus Recognition Session Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a mocked, repeatable recognition-session contract test plus the minimum endpoint and browser fixture it exercises.

**Architecture:** Preserve the existing read-only device route and three wired identity tables. Add one focused library that validates/enriches session events and writes `rs_recognition_sessions_test`, one thin POST route, and one repository-tracked HTML fixture that calls the existing lookup then records its result.

**Tech Stack:** Astro 6 edge routes, Cloudflare Workers runtime, JavaScript ES modules, Node `node:test`, Airtable REST API.

## Global Constraints

- Do not invoke the live endpoint or fire the live Airtable automation during verification.
- Do not create, rename, or modify Airtable fields or tables.
- Preserve `rs_people_test`, `rs_devices_test`, `rs_phone_aliases_test`, and the current device response fields.
- Write one session record per event with `automation_status = Pending` and a required idempotency key.
- Never store raw IP or raw user-agent values in Airtable.
- Keep unrelated working-tree changes untouched.

---

### Task 1: Session event contract

**Files:**
- Create: `webflow-cloud-test/test/rs-recognition-session.test.js`
- Create: `webflow-cloud-test/src/lib/rs-recognition-session.js`

**Interfaces:**
- Consumes: `{ env, fetch, request, payload }` supplied by the route.
- Produces: `recordRecognitionSession({ env, fetchImpl, request, payload }) -> Promise<object>`.

- [ ] **Step 1: Write the failing contract tests**

Cover required event fields, exact Airtable field mapping, linked record IDs, HMAC signal fields, idempotent duplicate handling, and upstream errors.

- [ ] **Step 2: Run the focused test and verify failure**

Run: `node --test test/rs-recognition-session.test.js`

Expected: FAIL because `src/lib/rs-recognition-session.js` does not exist.

- [ ] **Step 3: Implement the minimal session library**

Implement payload normalization, SHA-256 HMAC helpers, IPv4 `/24` and IPv6 `/48` network normalization, coarse Cloudflare context extraction, Airtable duplicate lookup, and Airtable record creation.

- [ ] **Step 4: Run the focused test and verify pass**

Run: `node --test test/rs-recognition-session.test.js`

Expected: all session contract tests PASS with mocked fetch calls.

### Task 2: Thin POST route

**Files:**
- Create: `webflow-cloud-test/src/pages/rs-recognition/session.js`
- Test: `webflow-cloud-test/test/rs-recognition-session-route.test.js`

**Interfaces:**
- Consumes: JSON event payload and Cloudflare request context.
- Produces: JSON `{ ok, duplicate, record_id, session_event_uid, session_uid }`.

- [ ] **Step 1: Write a failing route test**

Set the mutable test `cloudflare:workers` environment, mock Airtable fetch, call `POST`, and assert HTTP 201 for create, 200 for duplicate, 400 for invalid payload, and 502 for Airtable failure.

- [ ] **Step 2: Run the route test and verify failure**

Run: `node --test --experimental-loader=./test-support/cloudflare-workers-loader.mjs test/rs-recognition-session-route.test.js`

Expected: FAIL because the route does not exist.

- [ ] **Step 3: Implement the route**

Read `AIRTABLE_TOKEN`, `AIRTABLE_BASE_ID` or `AIRTABLE_BASE`, optional `AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE`, and `RS_RECOGNITION_SIGNAL_SECRET`; delegate all behavior to the session library.

- [ ] **Step 4: Run the route test and verify pass**

Run the command from Step 2. Expected: all route tests PASS.

### Task 3: Existing device compatibility

**Files:**
- Modify: `webflow-cloud-test/src/pages/rs-recognition/device.js`
- Test: `webflow-cloud-test/test/rs-recognition-device-contract.test.js`

**Interfaces:**
- Preserves all existing response keys.
- Adds `device_record_id` when a device record is found.

- [ ] **Step 1: Write failing tests for `Active` status and device record ID**

Mock the two existing Airtable reads and assert an `Active` device is recognized and its record ID is returned.

- [ ] **Step 2: Run and verify the current route fails the new assertions**

Run: `node --test --experimental-loader=./test-support/cloudflare-workers-loader.mjs test/rs-recognition-device-contract.test.js`

Expected: FAIL because status matching is case-sensitive and the record ID is absent.

- [ ] **Step 3: Make the backward-compatible fix**

Compare device status using `toLowerCase()` and add `device_record_id: device.id` to found-device responses.

- [ ] **Step 4: Run and verify pass**

Run the command from Step 2. Expected: all device contract tests PASS.

### Task 4: Browser test fixture and full verification

**Files:**
- Create: `webflow/rs-recognition/rs-recognition-test.html`

**Interfaces:**
- Calls `GET https://ringstatus.webflow.io/test/rs-recognition/device?device_token=...`.
- Calls `POST https://ringstatus.webflow.io/test/rs-recognition/session` with one event envelope.

- [ ] **Step 1: Add the self-contained Webflow test embed**

Preserve the current test controls, create a stable `session_uid` in `sessionStorage`, emit unique idempotency keys, include client timezone/viewport/page context, and never expose Airtable credentials.

- [ ] **Step 2: Run all recognition tests**

Run: `node --test --experimental-loader=./test-support/cloudflare-workers-loader.mjs test/rs-recognition-*.test.js`

Expected: all tests PASS.

- [ ] **Step 3: Build the Astro project**

Run: `npm run build`

Expected: Astro exits 0 and emits the recognition device and session routes.

- [ ] **Step 4: Inspect the final diff**

Run: `git diff --check` and `git status --short`.

Expected: no whitespace errors; only the approved recognition files and pre-existing unrelated changes are present.

