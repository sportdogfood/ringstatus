# Messages alerts correction — 14 October 2026

Status: scoped source corrections complete; local affected checks PASS. Overall cross-domain acceptance remains open for combined release and hosted verification. No deployment or publishing authorized/performed.

## Scope and preserved baseline

Owner approved remaining corrections. Existing prepared source retained at C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test, HEAD bf9a93531209212f303d7e8175af5c9fb01ba2bd. Prior working-tree changes are the supplied implementation, not this task's authored delta. Before copies of the three allowed files are in C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/messages-alerts-correction-20261014/.

Only authored changes:
- src/assets/rs-input-subscriptions/native-client.js: successful confirmed save now displays Saved; newer unsaved choices still display Pending.
- test/tenants-alerts-browser.test.js: TABLES.intake corrected to existing TABLES.sub; desktop test explicitly enters ?view=desktop and verifies data-view.
- test/rs-inputs-persistence-native.test.js: regression verifies pending status and confirmed Saved status/button.

No recognition source/test, shared fixture/provider/processor, release package, environment or business records edited. No full Astro build, activation, SMS or new agent/schedule.

## Verification

Flow: exact archived /rs-barn-onboarding-alerts page 6ac459b4506821bd349ed047 -> edit choices -> existing intake/processor confirms canonical preferences -> Saved -> reload, opt-out, failed request/reload retry, account switch.

Browser plugin unavailable; used existing bundled Playwright and Edge, all page/API requests intercepted into the existing local real-handler/processor fixture. This is local application proof, not hosted deployment or live Airtable proof. Stylesheet links are stripped by the supplied harness; no visual fidelity/screenshot claim.

Command from source app:
`node --experimental-loader ./test-support/cloudflare-workers-loader.mjs --test test/rs-inputs-persistence-native.test.js test/tenants-alerts-browser.test.js`

Final: 12/12 PASS, exit 0, 81.758 seconds. Nine native persistence tests plus three browser tests. Scoped git diff --check passed.

| Gate | Evidence |
| --- | --- |
| Known entry stays and loads | PASS: exact pathname, allowed phone loaded, controls enabled |
| Immediate input/options | PASS: entered phone, checkbox state, 1 of 18 counter before save |
| Confirmed save + reload | PASS: status Saved, canonical enabled preferences, phone survives reload |
| Opt-out + history | PASS: canonical Revoked; original prior event unchanged |
| Failure retained + same-ID retry | PASS: Retry shown, reload resubmits original request ID and retained phone |
| Actor isolation | PASS: switch disables/clears previous actor view and transmits no new POST |
| Mobile | PASS: 390x844, data-view mobile |
| Desktop | PASS: 1280x900, explicit data-view desktop |
| Browser runtime errors | PASS: pageerror collection empty in both known browser flows |
| Unknown entry routing | PASS: Recognize URL has exact alerts return_to; this test stops at entry |
| Complete unknown identification/return | Separate Recognize controller-double proof, not a rendered hosted journey |
| Hosted/live acceptance | NOT RUN: deployment and live business writes excluded |

Initial plain-node browser invocation failed before running browser assertions because cloudflare:workers requires the repository's test loader. Corrected invocation uses that existing loader; no application patch was made for it. The first added pending-button assertion expected Saving while awaiting async confirmation, but existing behavior returns button to Save while status says Saving. Test corrected to the actual status contract; this assignment does not change that separate pending-button behavior.

## Source-complete hashes (SHA256)

- Alerts client: 3FC2583EEB9450D294575F58321C499BA43A15D9D532BFA499B348E92E6BEC7C
- Persistence test: 0DD8FF3EC8BA8CB24F188A422D792F22238194E8197E7002B1146276F5DEECCF
- Alerts browser test: 18B49DDE027210CF055584A6F68C7365A64D2C62D93584CEEAB59B62FC28E8D4
- Recognition dependency observed: B998EED3FC4113EB1838A02765AF4EFF6D2B5352C11052ACBB07AF3D416B218F

Recognize peer reports 32/32 affected checks in docs/coordination/recognize-alerts-return-correction-20261014.md. Those checks were not repeated by Messages.

## Next action

Parent coordinator integrates these exact source files with Recognize's result and passes them to Tenants for the one final combined verification/build/package. Preserve prior Barn/Rider evidence. Keep no-publish/no-activation boundary; hosted unknown-return and actual deployed preference processing remain release acceptance, not claimed here.

Agents current task: appZahVgD156cMAe3/tbluT1yhP1FJMb8Cm/reccAPPd1hMxt22z9. Retain In progress for full cross-domain acceptance; scoped correction is complete.
