# Payload correction review — November 11, 2026

CONFIRMED: 13 unrelated endpoint records contained the identical class 200031716 payload. Their request labels were endpoint-specific, but payload assembly reused class data. The prior verification compared Airtable to those same faulty generated samples; that was insufficient source validation. Original endpoint response captures remained available.

Only payload-1, response-1 and observation-1 were corrected on those 13 records. Each was rebuilt from its own original request label and URL. Source response bytes matched the recorded SHA-256. A separate recursive verifier compared every displayed leaf and object key with the actual retained source response, allowing only declared first-row array excerpts and sensitive-value redaction. All 13 passed. Exact Airtable readback also matched all 39 corrected fields.

Audited all 229 records for this repeated class-body defect. Zero class-200031716 bodies remain outside the actual class-detail endpoint. GetShowData's current complete two-part payload and owner checkboxes/Notes were preserved. Active local individual-fields evidence now reflects the correction; faulty previous samples are retained only in the clearly named before snapshot.

No new SGL requests, collectors, product code, production configuration, credentials, schedules or business-record repairs. No identity coverage conclusion added.
