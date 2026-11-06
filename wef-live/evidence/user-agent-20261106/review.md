# Four direct-feed User-Agent review — November6,2026

Live author review,not independent review. Exact owner request changes only headers for four specified direct Wellington requests. Current activity.js/source.js and shared reader inspected against saved baseline; active task/25runtime manifest captured.

activity.js adds only exact User-Agent header on current getLiveClassStatus URL. source.js injects request callback only for exact HTTPS Wellington origin and three existing feed pathnames. Original target string,queryparameters,Accept/Cache-Control,GET default,30second timeout,body parsing and common bounded retries preserved. Decoded proxy remains on unchanged default reader,headers and x-rs-decoded requirement. Shared library bytes unchanged,so other collectors unaffected. No endpoint,credentials,task,cadence,state,schema,logging,cleanup or gate changes. Other Wellington paths retain existing behavior.

Seven offline request-capture tests PASS: four exact request headers/URLs,proxy without new header,unlisted path without new header,503retry with same exact headers. All fetches mocked; no provider/manual collector calls. Unchanged broader collection tests not repeated because delta is request construction only.

No new actionable finding within approved delta. Code/offline review PASS. Installation/hash/task readback and real normal scheduled feed verification must be recorded separately. Current inactive hourly due checkpoint says09:39Eastern; do not move it or invoke a collector to accelerate proof. Failed scheduled verification stops its affected path; no extra edits/retries without separate authority. Existing unrelated holds/findings remain.
