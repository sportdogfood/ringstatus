# GetShowData checks — November 11, 2026

## Result

The existing Edge browser session returned the full show 200000078: 429 unique classes and 82 divisions. The same browser GET/headers, changing only credentials from include to omit, returned HTTP 200 body DoLogin. A freshly issued anonymous config token also returned DoLogin on this exact iphone URL with both iPhone and Windows header profiles. This establishes browser cookie/session dependence for the controlled pair; the individual decisive cookie and an unattended session-renewal method were not isolated. No _t was returned by the successful payload.

## Useful data

One response can preload the show name/dates/series plus its class/division directory, class numbers, names, source dates, cancellation flags and division links. All nonzero division links resolve. Twenty classes have source date 0000-00-00 and must remain undated. There are no ring/start-time/trip/OOG/result arrays in this captured object; no conclusion about participant identity coverage is made. No full schedule or class-detail replacement is established.

## Saved payload

The complete original body is browser-response.json (SHA-256 d38a7afbcfcc52554c123f1fd48160a3800fd86aa81b73e63a6df32e4a7d4d00). Its compact equivalent is 100,320 characters. The Airtable connector rejected a 97,834-character single-field write with HTTP 413, so no columns were added: payload-1 holds all top-level fields/divisions plus first 190 classes; payload-example holds the remaining 239 classes. Concatenate the class arrays and keep the other payload-1 keys. Readback reconstructs the complete original JSON object with no omitted fields/rows.

## Evidence and boundaries

findings.json records redacted controls and anonymous-token attempts; analysis.json inventories every returned field and date/count; airtable-verification.json records exact saved readback. An earlier cross-origin cache-control probe failed CORS preflight and is excluded from source-access proof. A stale json field ID was rejected during read; schema was refreshed before writes. Existing failed-attempt evidence was retained in access-evidence. No tokens or cookie values were saved. No production code/configuration/credentials, collectors, schedules, legacy tasks or source business records changed. Findings are diagnostics, not scheduled-collection proof.
