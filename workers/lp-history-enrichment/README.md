# LP History Enrichment Worker

Isolated Cloudflare Worker for LP History edit-mode enrichment writes.

This Worker is intentionally separate from existing account Workers:

- `ringstatus-sms`
- `broad-tooth-b8ed`
- `ringstatus-proxy`
- `ringstatus-pdf`

## Routes

- `GET /health`
- `OPTIONS /lp-history/enrichment`
- `POST /lp-history/enrichment`

## Required Secrets

Set with Wrangler:

```powershell
npx wrangler secret put LP_HISTORY_EDIT_KEY
npx wrangler secret put AIRTABLE_API_KEY
npx wrangler secret put AIRTABLE_BASE_ID
npx wrangler secret put AIRTABLE_TABLE_NAME
```

Optional:

```powershell
npx wrangler secret put ALLOWED_ORIGIN
```

## Payload Contract

```json
{
  "recordType": "horse",
  "recordKey": "horse:6072529",
  "recordState": "active",
  "status": ["overview", "favorite"],
  "data": {
    "show_name": "FORT KNOX"
  }
}
```

The Worker stores flexible enrichment JSON in Airtable while keeping stable keys for later rebuilds.
