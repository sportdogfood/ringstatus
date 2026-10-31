# Day-priority and Live overlap validation — October 31, 2026

Owner approved use of Shows day-priority and verified Live overlap values includingzero/false, Datafallback, preservedlinks/logs, testing and two normalruns.

The required runtime guard was already installed in publication.js/publication-api.js (preexisting uncommitteddelta preserved; mtimes03:39:40Z). No newruntimepatch or deployment was needed. Added4 targetedregressions and updatedfixture to expose existing priorityZero contract. Review of actual installeddelta and43 targetedtests PASS; review packet integrityPASS. Exact installed hashes in installed.json; no sibling/productfield changes.

Canonical table is wef-shows.show-days (tbl14MaCCDw6dAywH), not a newlycreateddays table. Data overlay eligible only exact canonical show/day with numericday-priority0 and matching selected_date; checks repeatbefore dispatch. Live sourcecollection remains TODAY. At eveningplanningadvance, todayLive collection cancontinue while Data overlay iswithheld if today isnotpriority0. No tomorrowLivecollection by implication.

Existing approved live-observation-json componentmapping remainssolecrossbasewrite. Verifiedzero/false retained; missing staysdistinct. Acquisition/nativefields leftintact for Datafallback; Data laterwrites cannotoverwriteoverlay. Effectiveinterpretation by externalconsumers isseparatelyowned, notclaimedverifiedhere.

Normalexistinginstalledruns inspected: c4815132-8fb9-4a62-a85d-75ec1cb15c11 (07:56:12-07:57:53Eastern), da414dc8-ca4c-42f7-972d-8ca6e697fb68 (08:02:13-08:04:01Eastern). They precede thisreview/testcompletion and are NOT claimed asnewpostdeploymentruns. No runtimedelta required. 28+29 Datawrites have hashverifiedimmutableintent/outcome, exactoverlayreadback and unchangednativearrays.81 explicitzerovalues acrosswrittencomponents;falsecoveredofflineonly. CurrentAirtablepriorityZero=true; sampledData overlayspresent. Both heartbeat andrunlogsPartial;13and11errorrows present.

Operationalacceptance PARTIAL, notclean2runPASS: remainingDatawritesdeferred316/355 with1uncertain each, ambiguousoccurrences/missingtripmatches, unresolvedsourceidentities; logretentionBlocked requirespriorownerreview. No retries/manualruns/recordrepairs/stopclear undertaken. Sourceacquisitionfailurepaths andretentionstop preserved. This work doesnotfixfinalgroupbeforeSkip ormodifyTiming.

Evidence: targeted-tests.json, review.md, review-commands.json, review-receipt.json, review-verifier.json, normal-run-proof.json, airtable-readback.json, baseline.json andinstalled.json. Nextaction: ownerdecision for separatelyscopedpublicationcapacity/identity/logretentionwork; no automaticobserver.
