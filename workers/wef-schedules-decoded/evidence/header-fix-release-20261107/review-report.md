# Three-header correction review

Approved delta only: omit ordinary authority,method,scheme headers from installed runner worker.js and hosted/fallback worker.js. Each candidate differs by exactly one request-header construction line; path and all remaining headers unchanged. No dependency/runtime/config/route/queue/token/cookie changes. Regression check inspects both real code paths and preserves Authorization,Cookie,source origin and path.

Executed26 focused staged tests PASS. Actual local runner preview on ephemeral port returned7schedule rings and8class trips in two rounds. Preview uses copied relay/public module unchanged except its isolated ephemeral test port; stopped after checks. Existing authenticated Cloudflare preview deployed successfully and forced independent TLS fallback returned7rings/8trips. Real fallback preview is distinct from earlier local transport inference. No production collector execution or business writes.

No findings in approved delta. Header cause isolated previously; historical activation of SGL gateway filtering remains unknown. This correction does not solve unrelated open paused class-detail work, show-time freshness or earlier reported Airtable422. No broader acceptance claim. Production installation/live verification pending at this review point. Existing Durable Object code-update delay must expire before live fallback acceptance.

## Production verification

Production deployment dea6c94e-d71e-481a-8be1-ea29a5579855. Exact previewed header removals installed in hosted and installed runner worker.js; existing runner task restarted once, task definition unchanged.26 installed regressions passed, all15 protected source hashes unchanged, all5 task XML definitions unchanged, legacy3 tasks disabled.

Initial immediate live schedule check502 retained in live-primary-before-ready.json. After documentedDOrollout window, an over-strict check rejected a usable200 fallback response because it required runner-only; retained in live-normal-lane-criterion-too-strict.json. Existing approved normal behavior permits local OR automatic fallback; no implementation was edited to make this check pass. Final normal+forced fallback checks in live-final-results.json all200 with usable data: schedule7rings via runner; class8trips via automatic fallback; forced fallback schedule7rings/class8trips. Measured inter-request gaps6035/5872/5514ms (other scheduled traffic can occupy queue); all exceed1000ms minimum.

Known remaining limitation: actual installed runner sample class request still returns{}, and Worker logs say runner-empty-or-invalid; automatic fallback supplies valid class data. Do not claim primary class recovery or close the separately paused class-detail task. Local preview class passed but does not override this live result. Source diagnostic tokenA/B observations did not restore local class; no credential or cache change made. This is outside the authorized three-header correction; no extra header was removed.

No collectors manually executed, no new route/component/schedule or dependency change. Existing authenticated isolated preview was reused, not a new production lane. Local temporary preview stopped; live-tail observation session stopped.
