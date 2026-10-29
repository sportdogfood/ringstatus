# Season retained history

The four existing bases retain trainers, riders, horses and dated rings using unchanged Data keys. Actual table and field IDs are pinned in schema.json. Each base owns shows, show-days and its entity table, plus the existing heartbeat, run-logs and err-logs. Native links are local to that base.

Source is only a verified Data retained revision or verified outgoing archive. Entries establish show membership; actual trips/classes establish show-day membership; rings link directly. Directory-only entries do not establish day attendance. Names never merge identities. Membership is additive and older/unknown source times cannot overwrite newer facts.

One registered hidden task wakes every five minutes; only its normal trigger executes season.py. Each entity has an independent checkpoint and stop. Full readback and a fresh wef-shows fence precede checkpoint advancement. Same-revision repeats skip writes. No Data-owned writes, source endpoint acquisition, key changes or deletions.

The existing runner owner must be logged in. Runtime, secrets and logs remain private. Separate logs record every wake and outcome, retaining undelivered events locally during Airtable outages. A declared failure stops its affected path pending fresh owner authorization.
