# Publishing API Concept

## Purpose

Define a clean data-delivery backbone that lets Webflow render RingStatus information from stable JSON without making Airtable the public runtime interface.

## Scope

The current target flow is:

```text
Airtable/staging -> Catalyst Datastore -> Catalyst Function/API -> Webflow fetches JSON -> renders
```

This concept covers the published data contract, validation boundaries, API behavior, and browser consumption. It does not yet choose every table, endpoint, or deployment detail.

## Current direction

Keep Airtable as the staging and operational source, move approved records into Catalyst Datastore, expose only the required read models through a Catalyst Function/API, and let Webflow fetch and render that JSON. The same delivery layer can later serve location and Ring Waze views when their contracts are defined.

## Next considerations

- Define the first Webflow view and the smallest JSON contract it needs.
- Specify stable identifiers, timestamps, freshness, status, and error shapes.
- Decide validation, authorization, caching, CORS, and failure behavior at each boundary.
- Build one end-to-end read-only proof before expanding the API surface.
