# wef-schedules-decoded

Separate Cloudflare Worker for decoded WEF source reads. Existing legacy routes
are not routed through this Worker. It has no scheduled trigger and performs no
Airtable writes. SGL_TOKEN and SGL_COOKIE are server-side secret bindings copied
from the existing authorized proxy configuration; values are not in this project.

Supported GET routes: /health, /ring, /schedule, /people/:id and /classes[/id].
Only explicit source query parameters are accepted; customer_id must be15.
Encrypted fields use Wellington's public AES-GCM decoding method. Schedule `_`
evidence is retained for existing source-monitor columns and composite keys.

Status 2026-10-21: redirect handling corrected under new owner approval.
Uses manual redirects and rejects3xx without forwarding credentials. Pre-switch
live ring/schedule/people tests passed; both independent collectors now use this
lane. Two normal runs of each collector passed:435 schedule class IDs and four
linked tenant entries with stable record IDs and no duplicates. Existing empty
Oct22 show-day warning remains; no failed collection days. Original helper values
and five-minute task definitions are preserved. No manual runs or record repairs.

Offline tests, cached schedule proof and normal-run receipts are in evidence/.
See docs/coordination/wef-isolated-decoder-redirect-fix-20261021.md for release
evidence. Legacy proxy routes and task definitions remain unchanged.
