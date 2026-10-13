# Messages agreed lanes

OWNER-CONFIRMED LANES — 10 October 2026
OTP: Recognize owns verification and its existing OTP path. Include this lane in the Messages map; do not migrate or recreate it.
SMS/alerts: its own messaging logic and queue, receiving inputs from show-engines and RingWaze. Preserve producer facts and existing implementations; Messages does not recreate show calculations.
Two-way: message received -> normalize message -> identify show and intent (ring or rider) -> query the appropriate endpoint -> send the response back. Preserve existing working deterministic lookup logic. Unresolved show/intent must not be guessed.
Notifications-feed: shared output updated by every lane alongside logging. Log actual processing/delivery outcomes, not inferred success. OTP codes/private verification links must not appear in the feed.
These are expected workflows, not claims that queue/feed integration or live delivery is implemented. Map each lane's exact existing source, trigger, inputs, output, logging/feed destination, current evidence and smallest remaining gap. Do not ask owner to explain this lane architecture again. Infrastructure isolation remains deferred; this direction does not authorize live sends, code replacement, deployment or automation activation.

Authoritative operating record: https://airtable.com/apphwDLkVCp8ut2Hd/tblycxFymN5gaPKlT/recjGDLwQ5U0VliiE
