# WEC P1 Session and Navigation Contract

## Scope

P1 adds session awareness and connected navigation surfaces on top of the P0 global component contract.

It must not change packing quantities, item states, P0 CSS classes, drawer shape, or table shape.

## Session

- The hybrid app creates one browser session key in `sessionStorage`.
- The device key is stored in `localStorage`.
- The app calls `POST /wec-packing/session` on session start, click/input activity, visibility return, and polling.
- Session pings coalesce while one request is already in flight.
- Polling quietly refreshes home/module data without showing the loading state.
- Polling pauses when the document is hidden or idle past `sessionIdleMs`.
- Item-state writes include the active `sessionKey`, `deviceId`, lane, module, and filter.

## Navigation

- `HOME` shows module rows from the home endpoint.
- `HORSES` defaults to `ROSTER`.
- `HORSES > ROSTER` shows the shared `pak_horses_roster` horse list using the same table contract.
- `HORSES > PROFILES` and `HORSES > ATTRIBUTES` are explicit not-connected placeholders.
- `COUNTS` defaults to `HORSE KITS`.
- `COUNTS > HORSE KITS`, `QUANTITY COUNTS`, `PER-HORSE ITEMS`, and `GROOM SUPPLIES` use the connected plan reports.
- `LISTS` defaults to a list overview of the known list modules.
- `LISTS`, `ITEMS`, and other future modules render explicit not-connected placeholders until their endpoints are wired.
- `ITEMS` defaults to an item-family overview of the known item modules.
- `COMMENTS` shows the current comments surface from the connected horse-kits report.

## Backend

- `session_ping` writes or patches `pak_sessions`.
- `session_ping` must not build or return a full plan report.
- Quantity/state endpoints remain responsible for their own writes and logs.

## Verification

- Syntax check every touched JS file.
- Load the local hybrid preview.
- Confirm the header renders.
- Confirm top navigation and child trays render.
- Confirm `HORSES > ROSTER` renders rows.
- Confirm `COUNTS > HORSE KITS` opens the table and drawer.
- Confirm session keys exist in browser storage.
- Confirm no user-facing loading flash appears during quiet polling.
