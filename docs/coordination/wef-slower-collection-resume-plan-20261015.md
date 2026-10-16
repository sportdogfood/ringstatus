## Entries verification and smallest resumable plan - 15 October 2026

Verified from owner-supplied captured JSON, not a new endpoint call: entries total=666, records_per_page=10, page=1, ten returned rows with ten distinct entry_id and horse_id values. At that captured total, a complete entries pass requires 67 pages. Together with the earlier 758-person capture (76 pages), these examples imply 143 page requests; they are separate captures, not a synchronized live count. Peak planning of 2,500 each at ten per page implies 500 requests. The ten-per-page behavior is now evidenced for both supplied responses, not proven as an unchangeable API maximum.

Source: C:/Users/gombc/.codex/attachments/0c811fa5-14f3-41ba-a56a-13cae349167d/Pasted text.txt.
Supplied request: https://sglapi.wellingtoninternational.com/entries?sort_on=number&sort_type=asc&page=1&search_text=&show_id=200000074&customer_id=15

### Identity evidence
- People capture provides people_id; entries provide trainer_id, entryowner_id and riders[].rider_id. These role IDs must retain their labels; cross-role equality with people_id is not proven by this one page and must not be guessed.
- Entries carry entry_id, show_id, customer_id, horse_id and number. Example entry 200246104 belongs to show 200000074 and horse 147368, displayed as number 2. Number/name are not horse identity keys.
- trips_dailyv2.js:1980-2059 groups riders by rider_id, horses by horse_id and entries by entry_id; 2195-2229 emits entry key String(entry_id) and its class links. Lines 2616-2639 use upsertByNumberId for rider_id and horse_id; 1472-1530 matches existing numeric IDs and detects changed pid. This proves the existing key choice, not universal stability across providers/customers.
- Current horse rows include show/date/entry fields (2094-2112). Reusing their writer unchanged would not establish preserved weekly history. Reuse the ID-based matching pattern, keep show participation separate, and preserve manual values rather than copying the full operational writer.
- Proposed directory identity scope: source/customer plus people_id or horse_id; show entry association: source/customer/show_id/entry_id. These are proposed scoped uses of observed IDs, not new verified upstream composite keys. Retain all riders[] links; do not flatten to the operational entry builder's single rider slot.

### Smallest resumable plan (documentation only)
1. Keep one progress record per source/customer/show/collection (people or entries): pass, next_page, reported_total, records_per_page, status, last successful save and last error. Use the same sort/search parameters throughout that pass.
2. On each scheduled invocation request only next_page, using the existing single-page proxy forwarding behavior as the reuse reference. Confirm deployed route parameters before implementation; do not assume the local worker file is deployed. Do not invoke worker2's all-pages helper.
3. Save/upsert that page's identities and show associations by verified IDs. Preserve prior-show associations and manual fields. Save all returned rider relationships. Only after all page writes succeed advance next_page. Interrupted writes leave the page pending, so replay updates the same records instead of duplicating them.
4. A failed, malformed or unexpectedly empty response does not advance the page or remove records. Retain the error and resume at that page on a later normal invocation. No automatic manual runs or separate repair path.
5. At the reported final page, record pass completion only when the collected unique-ID count is consistent with the reported total; otherwise keep an incomplete/reconciliation status. Name/number sorting can shift as source records change: a multi-day crawl is not a source snapshot, and no missing-record deletion is authorized. Further normal passes refresh additions/changes.
6. Use the owner's proposed approximately 500 requests over 60 hours, then another sweep over 90 hours as the pacing example, adjusted to actual page totals. Keep the previous show's identities and participation when starting the next show's progress records. Exact scheduler settings/storage are not selected here.
7. Faster tenant/follow-specific requests keep their own cadence and progress. They do not wait for the directory sweep; the new lane must not change their code, settings or existing writes.

No new tables, code, deployment, scheduler changes or manual business runs. Existing inspected paths have no durable all-people/all-entries page checkpoint; the progress record above is the small proposed addition.

