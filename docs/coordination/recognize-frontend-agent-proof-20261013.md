# Recognize frontend-agent proof

For the requested shared native/API-agent behavior and required changes, read [the 14 October workflow review](recognize-shared-workflow-review-20261014.md). The local SMS demonstration below remains historical proof; it is not a hosted desktop/mobile implementation of that target workflow.

Owner supplied this proven reference on 13 October 2026. Source chat: [rs-recognize-api](codex://threads/01a134dd-e5c9-71d2-977a-7006af22af7d). Preserve and inspect this implementation before proposing any replacement frontend agent.

## What was demonstrated

The owner confirmed that the disposable frontend agent requested a real SMS to the approved test number, accepted the code in the originating browser, and reached `/rs-member`. The explicit confirmation is turn `01a13530-91e5-7ec2-9797-ec1455fa491b`, following the three-step confirmation question in `01a13530-54aa-7a33-a811-d53a242c61d5`. This is owner-confirmed live journey evidence, not merely a local build/test count.

The source chat also reports a successful GPT-5.4 mini model response with HTTP 200 in turn `01a1352d-1bb2-7503-a135-59a5305e2292`. Its final overview and local-LLM caveat are in turn `01a13532-3f17-7142-944c-a7c05a51343a`. These historical results were read here; no model call, SMS or live verification was repeated.

## Existing implementation and division of responsibility

Located local implementation: `C:/Users/gombc/.codex/recognize-agent-test-20261013/`.

- `README.md`, `server.mjs`, `start.ps1`: disposable local service and operating instructions.
- `extension/content.js`, `extension/adapter.js`, `extension/background.js`, `extension/manifest.json`: browser extension and native-control integration.
- `policy.mjs`, `policy.test.mjs`, `verify-model.mjs`, `verify-browser.mjs`, `verify-native.mjs`: existing policy and verification artifacts. File existence is verified here; these tests were not rerun.

According to the source overview, the LLM guides the interaction and proposes actions; the extension operates the existing native Recognize controls. Recognize continues to own verification, device identity, cookies, sessions and footprints. The private code input bypasses the model. The demonstration added no independent SMS sender or Airtable writer and required no production source change/publication. Preserve these boundaries rather than replacing the working verification flow.

Only `16318752160` was authorized as the SMS test recipient in the source conversation. This reference does not authorize another send or restart. Never copy secrets, private codes or recovery URLs into this document or a handoff. The implementation's current service/access state must be checked only when a new execution is authorized.

## Exact proof boundary and future work

This supports the feasibility of a frontend agent guiding Recognize through a real owner-confirmed phone/code/launcher journey. It is separate from the standard native-browser acceptance and does not close all Recognize tasks. Linked Airtable request/device/session/footprint readback, return persistence, mobile behavior, production failure handling and production integration were not established by this demonstration. A local LLM/hybrid model was discussed as a cost option; compatibility, quality and savings were not tested.

Follow the owning current system/task checkpoint for operational status. Consult `resume-recognize-tenants-20261011.md` for the later shared release evidence; do not infer a fresh release or current service health from this proof. No product work or other agent was resumed while registering it.
