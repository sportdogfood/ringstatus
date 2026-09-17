# In The Irons USEF data-flow investigation

## Purpose

This folder preserves a read-only investigation of the public-facing In The Irons application and representative JSON supplied during the review. It documents observable client behavior and data contracts. It does not prove the implementation of In The Irons' private server-side USEF acquisition.

Observed on 2026-09-14/15 against `https://intheirons.com/`.

## Executive summary

The browser does not appear to contact a USEF domain directly. The frontend calls same-origin In The Irons endpoints, receives USEF-shaped competition data, computes most dashboard aggregations in the browser, and persists the normalized source history in browser storage.

Two separate data lanes are visible:

1. Historical person results: the frontend bundle calls `GET /apiv5/people?usef={USEF_NUMBER}&start=12/01/1990`.
2. Live/upcoming competitions: the running application called `GET /api/get-live-competitions?usef={USEF_NUMBER}`.

The browser evidence cannot establish whether the In The Irons server uses an official USEF API, scraping, a mirrored database, or another upstream service.

## Observed flow

```text
USEF person number
  -> intheirons.com/apiv5/people
  -> person plus competition history
  -> client normalization and classification
  -> local browser persistence
  -> Dashboard / Horses / Years / Types / Insights

USEF person number
  -> intheirons.com/api/get-live-competitions
  -> separately discovered live/upcoming competitions
  -> Live view
```

### Historical import

The compiled frontend contains this request pattern:

```http
GET https://intheirons.com/apiv5/people?usef={USEF_NUMBER}&start=12/01/1990
```

The response is accepted as either `response.competitions` or a top-level array. A missing or empty array is treated as an import failure.

The client converts date strings into dates and organizes the history into maps for:

- competitions;
- horses;
- riders;
- years; and
- discipline/type.

It totals classes, first-place wins, national points, zone points, and prize money. Horse keys prefer `usefHorseId`, then normalized horse name, then back number. Rider keys prefer `usefPersonId`, then normalized rider name.

Discipline classification is performed in the frontend from section and class titles:

- Derby;
- Classic;
- Equitation, including Maclay, medal, WIHS, Dover, and jumping-seat terms;
- Jumper;
- Hunter; or
- Misc.

### Browser persistence

The frontend uses the local-storage key `equestrian-storage`. Its persisted subset includes:

- competitions;
- USEF number;
- horse ID;
- person name;
- data mode;
- horse profile;
- video-fetch state;
- watchlist/scroll state; and
- data-loaded timestamp.

On rehydration, the client rebuilds the horse, rider, year, and type maps from the persisted competitions. This explains why an ordinary reload can show a complete dashboard without repeating the historical `/apiv5/people` request.

### Fallback behavior

If historical import fails, the frontend can load a bundled demonstration dataset and mark the session as demo data. A populated interface is therefore not, by itself, proof of a successful fresh USEF request.

### Live endpoint

The following request was directly observed during a normal reload:

```http
GET https://intheirons.com/api/get-live-competitions?usef=5550974
```

The observed response was HTTP 200, `application/json`, `Cache-Control: no-store`, served by nginx. At that moment it contained the person ID, a discovery timestamp, and an empty `competitions` array.

The large person-history fixture in this folder is not the 138-character live response observed during that reload. Its shape matches a full person/history contract and must not be labeled as live-endpoint proof without the original request record.

## Application surfaces reviewed

The persistent top navigation contains:

- Dashboard;
- Live;
- Horses;
- Years;
- Types; and
- Insights.

The Dashboard summarizes competitions, classes, wins, prize money, recent activity, and top horses.

The Horses view provides horse search, alphabetical sorting, a videos-only switch, and per-horse competition/class/win/money summaries.

The Years view groups competition, class, win, and prize-money totals by year.

The Insights view includes performance by type, specialty distribution, top horses, monthly performance, year comparisons, best shows by prize money, and activity distribution.

The Website Builder contains these editor tabs:

- Basic Info;
- Home Page;
- Bio Page;
- Resume;
- Videos; and
- Testimonials.

Captured builder-state samples preserve the form model for identity/contact fields, stories, bio sections, resume achievements, videos, testimonials, theme, slideshow settings, and contact-form configuration.

## Included samples

| File | Raw bytes | SHA-256 | Description |
| --- | ---: | --- | --- |
| `person-history-response-5550974.json` | 2,425,507 | `26ce39a05b83724a2bcd8a023f60429c2e2a0a498ecc61e0d9c377b5ce097386` | Person resolution, identity, candidates/questions, 127 competitions, audit data, and producer metadata. Exact originating request remains unconfirmed. |
| `website-configuration-5550974.json` | 122,929 | `5d2856becde6861c25ac00849388a1e4001ef8cdf92f7fa88a2e6082a9a7bc39` | Versioned multi-page and SPA website configuration. |
| `portfolio-builder-state-5550974.json` | 150,340 | `aa2ae5ebbf2a14ec6ac4e559ecbca981984febffa79360d6ccb0c478150bc95e` | Populated portfolio-editor state, including bio, home, resume, videos, testimonials, contact form, and theme. |
| `empty-portfolio-state.json` | 14,388 | `0aa8fd6a9138689fa4ecd51d5066da60ea44fcf76072ece2462b6ac9adba2e3f` | Smaller empty/default portfolio state with sections, template type, theme, and navigation state. |

Hashes describe the exact supplied attachment bytes committed here.

## Safety and provenance

The samples were parsed successfully before commit. A recursive key scan found no password, authorization, cookie, session, API-key, credential, or token fields. Detected email addresses and telephone numbers use obvious example values.

The files do contain a named rider, USEF identifiers, public competition history, and configured portfolio content. Treat them as research fixtures rather than production configuration.

## Evidence limits

- Browser DevTools can prove requests made by the frontend, their responses, and client-side processing.
- It cannot reveal the private server's upstream USEF transport.
- The exact request that produced each user-supplied attachment was not captured alongside the file.
- Cached or demo data can render the same application surfaces as freshly imported data.
- Direct/manual endpoint calls are diagnostic only and do not prove a scheduled workflow, deployed runner, or business-state transition.

## Next evidence to collect

For a complete server-path comparison, preserve a sanitized Network record from one normal, uncached USEF import. Record the request URL, method, status, timing, response headers, response body, and Initiator chain for `/apiv5/people`. Do not commit cookies, authorization headers, or account tokens.
