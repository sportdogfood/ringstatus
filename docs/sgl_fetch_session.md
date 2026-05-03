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
  "authorization": "Bearer <redacted>",
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
$env:SGL_AUTHORIZATION = 'Bearer <redacted>'
$env:SGL_COOKIE_HEADER = 'jwt=<redacted>; sgl=<redacted>; __cf_bm=<redacted>'
```

## Smoke Test

Use a known active show/date:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\sgl_fetch.ps1 `
  -Url 'https://sglapi.wellingtoninternational.com/schedule?date=2026-05-03&show_id=200000060&customer_id=15' `
  -OutputPath "$env:TEMP\ringstatus-sgl-smoke.json"
```

Expected: `ok:true`, status `200`, and a body length much larger than `{}`.
