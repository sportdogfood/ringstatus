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

Aliases also supported:

```text
AIRTABLE_TOKEN -> AIRTABLE_API_KEY
AIRTABLE_TABLE -> AIRTABLE_TABLE_NAME
```

Optional:

```powershell
npx wrangler secret put ALLOWED_ORIGIN
```

## Airtable Field Contract

The Airtable field contract is fixed from `lp_history_enrichment-Grid view (3).csv`:

```text
record_key
record_type
payload_json
horse
barn_name
show_name
raw_payload
status
kind
source
competition_type
video
source_id
record_state
class_type
class_sequence
horse_type
horse_disciplines
horse_color
class
competition
horse_gender
horse_age
image_url
video_url
embed_url
thumbnail_url
playlist
group_tags
tags
notes
updated_at
```

The Worker writes only these fields. Additional client payload keys are ignored except inside `raw_payload`.

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
