# SGL browser discovery: default show and source date

## Ownership handoff, November 6

Owner assigned new-show discovery to `wef-shows`; show-time/clock remains with `wef-decoder`. Handoff sent to existing Shows chat `01a16976-f0a6-72a1-a75f-75de7f19a815`. Sending is confirmed; recipient acceptance and implementation are not claimed.

Shows should compare a verified freshly discovered default ID against its existing canonical show to detect a change. Preserve current publication and collection controls. The saved-selection qualification below must be resolved before calling the browser method reliable default-show discovery. This handoff does not authorize code edits, task changes, production writes or manual collectors. Decoder clock freshness verification remains outstanding.

## Owner request and boundary

Capture the show ID and date that SGL adds to the browser address after opening Show Schedule, avoiding a direct API call by our agent. Document only; no implementation, collectors, production changes or credential changes authorized by this request.

## Verified on November 6, 2026

Opened https://www.wellingtoninternational.com/ and clicked its Show Schedule link. The link initially opened:

https://www.wellingtoninternational.com/showgrounds/show-schedule/

After SGL loaded, the address became:

https://www.wellingtoninternational.com/showgrounds/show-schedule/?date=2026-11-06&sid=200000077

Read `sid` as show ID and `date` as the source-selected show date. Do not hardcode the observed values.

This is a JavaScript history update, not an HTTP redirect. A plain HTTP request that follows redirects will not reproduce it. A browser must load and execute the page.

## Evidence behind the address update

Inspected the loaded https://sglapp.showgroundslive.com/static/js/main.js bundle and captured requests from the actual homepage-to-schedule navigation using browser CDP.

- `/shows?customer_id=15` returned HTTP 200 with `default_show=200000077` in the application request. That request included a bearer. Direct browser navigation to the same API URL returned 401, `This content is not authorized for viewing`.
- The schedule component calls `/schedule`, then sets URL `date` from `response.show_date` and `sid` from `response.show.show_id` using `window.history.pushState`.
- Captured `/schedule?date=00%2F00%2F00&show_id=0&customer_id=15` returned HTTP 200 with `show.show_id=200000077` and `show_date=2026-11-06`. This application request included a bearer. This is source evidence, not approval to add a new API route or change proxy parameter validation.
- Other captured requests used the resolved show ID and then the resolved date.

## Proposed browser capture; not implemented

1. Open the clean Show Schedule URL in a browser.
2. Wait until the address contains a positive numeric `sid` and a valid `YYYY-MM-DD` `date`.
3. Read both parameters; retain capture time and final URL as evidence.
4. If the parameters do not arrive, report capture failure. Never save an empty value as successful discovery.

The SGL page still makes its own API requests and handles its own browser authentication. Our agent would read the resulting address rather than make those API calls itself.

## Default-show qualification

The bundle also reads a saved `show_id` and `show_id_expiry_time` from browser storage, and honors an explicit `sid`. Therefore, a populated address proves which show the page selected; a reused browser session is not yet proven to select the current default every time. A fresh-browser/no-saved-selection check and repeat proof are still required before accepting this as reliable default-show automation. Do not clear the owner's existing browser storage or log them out to test it without authorization.

## Related version.json finding

SGL fetches https://sglapp.showgroundslive.com/version.json with a timestamp query and `cache: no-store` immediately and every 60 seconds. It stores the first nonempty `version`, then reloads the page when that value changes. This is an app-build update trigger, not evidence that show IDs or schedule data changed. The reusable pattern is polling a small change marker and acting only when it changes. No such trigger was implemented.

## Handoff

### version.json Date header: clock reference

Owner decision: document this clock option and move to the next topic. No implementation authorized by this documentation request.

A read-only GET of https://sglapp.showgroundslive.com/version.json returned HTTP 200 and the response header:

`Date: Fri, 06 Nov 2026 20:13:32 GMT`

Convert that header with the IANA timezone `America/New_York`: November 6, 2026, **3:13:32 p.m. Eastern**. Use the timezone conversion rather than subtracting a fixed number of hours, so daylight saving changes are handled automatically.

Distinguish these three values:

- `Date` response header: response clock reference; could be old if served from cache. Freshness must be verified before operational reliance. It does not establish a show-specific timezone or live-data freshness.
- `builtAt` JSON field: app build time, not the current time. Owner supplied `version=0.1.44`, `builtAt=2026-11-02T11:00:41.355Z`.
- `?_=1793995473602`: request timestamp added by the browser, not a server-returned time. SGL source uses `Date.now()` and `cache: no-store` for its version checks.

The local diagnostic returned `{}` as its body while exposing the Date header. The owner's browser showed the version/builtAt JSON above. Do not represent the local diagnostic as independent confirmation of that JSON body.

Remaining proof, if this option is implemented later: confirm successive responses carry fresh Date values and inspect cache behavior. Nothing has been deployed or scheduled for this clock option.

Current task is documentation, not deployment. Next implementation decision is whether to authorize a browser-based discovery check, followed by verification that saved selections cannot be mistaken for the default show. Keep API, browser-selection and scheduled-workflow proof distinct. No collectors were run.

Airtable reference: agents / agent-endpoints, base `appZahVgD156cMAe3`, table `tbll8POX9XPPb8z55`. `/shows` record `recCLFE7Gkx7FtUKu`; show-time reference `rec4XuG2P99bkzFuf`. The existing show-time reference concerns `/ring` time fields and is separate from this browser address discovery.
