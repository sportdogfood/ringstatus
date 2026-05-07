# SGL Fetch Session

The RingStatus SGL data fetch path uses local PowerShell `Invoke-WebRequest`
through `sgl_fetch.ps1`. Do not commit live cookies, JWTs, or browser session
tokens.

## Runtime Options

Preferred: store a local JSON file outside the repo and point the task at it:

```powershell
$env:SGL_FETCH_SESSION_JSON = 'C:\actions-runner\ringstatus\sgl_fetch_session.json'
```

Example file shape:

```json
{
  "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0",
  "authorization": "<redacted>",
  "cookies": [
    {
      "name": "jwt",
      "value": "<redacted>",
      "domain": "sglapi.wellingtoninternational.com",
      "path": "/"
    },
    {
      "name": "sgl",
      "value": "<redacted>",
      "domain": ".wellingtoninternational.com",
      "path": "/"
    }
  ]
}
```

Alternate environment-only mode:

```powershell
$env:SGL_AUTHORIZATION = '<redacted>'
$env:SGL_COOKIE_HEADER = 'jwt=<redacted>; sgl=<redacted>; __cf_bm=<redacted>'
```

`SGL_AUTHORIZATION` may be a raw JWT/token. `sgl_fetch.ps1` adds the
`Bearer ` prefix when the stored value does not already include it.

When an endpoint requires the same browser-session shape as the website, store
the non-cookie browser headers as environment values rather than editing the
script:

```powershell
$env:SGL_USER_AGENT = '<browser user agent>'
$env:SGL_SEC_CH_UA_MOBILE = '?1'
$env:SGL_SEC_CH_UA_PLATFORM = '"Android"'
$env:SGL_RECAPTCHA_TOKEN = '<redacted>'
```

`SGL_RECAPTCHA_TOKEN` and `SGL_X_RECAPTCHA_TOKEN` are treated the same. The
fetch output reports `user_agent_override_used`, `client_hints_override_used`,
and `recaptcha_used` as booleans so the run can be diagnosed without printing
token values.

## Smoke Test

Use a known active show/date:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\sgl_fetch.ps1 `
  -Url 'https://sglapi.wellingtoninternational.com/schedule?date=2026-05-03&show_id=200000060&customer_id=15' `
  -OutputPath "$env:TEMP\ringstatus-sgl-smoke.json"
```

Expected: `ok:true`, status `200`, and a body length much larger than `{}`.

If SGL returns HTTP `200` with a body of `{}`, `sgl_fetch.ps1` reports
`ok:false` with `reason:"soft_payload_empty"`. Treat that as a blocked/stale
browser session or soft-throttle signal, not a valid empty dataset.
