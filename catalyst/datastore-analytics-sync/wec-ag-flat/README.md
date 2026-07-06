# WEC AG Live Slate App

Static Slate frontend for the WEC live AG Grid surface.

Data flow:

1. Slate app loads in the browser.
2. The browser calls `wec_live_grid` first.
3. `wec_live_grid` is the server-side bridge where direct workflow/Data Store reads should live once the exact Catalyst table contract is verified.
4. The legacy `horseshowing_sync` render endpoint is kept as an explicit fallback while the bridge is being adopted.

Deploy from the Catalyst project root:

```powershell
catalyst deploy --only functions:wec_live_grid
catalyst deploy --only slate:wec-ag-live
```

If the Slate app calls the function across domains, add the Slate domain to Catalyst Authentication -> Whitelisting -> Authorized Domains with CORS enabled.
