# WEC P2 Horse Entity Contract

## Scope

P2 connects the hybrid app to the horse entity data source while preserving the P0/P1 app shell.

It does not enable writes, inline edit, add horse, delete horse, or profile form updates yet.

## Backend

- `GET /wec-packing/horses` returns the existing horse entity report.
- The source table is `pak_horses_roster`.
- Attribute support tables are read through the existing horse entity module.
- The route lives under the WEC packing path so the hybrid app does not need a standalone horse entity page.

## Frontend

- `HORSES > ROSTER` renders live horse entity rows.
- `HORSES > PROFILES` renders profile-link and comment-count rows.
- `HORSES > ATTRIBUTES` renders horse rows plus attribute option counts.
- Horse views support `ALL`, `WAVE ONE`, `WAVE TWO`, and `NOT GOING`.
- `COMMENTS` renders a combined read-only feed from horse-kits comments and horse entity comments.
- These surfaces use the same hybrid stack, search, table, labels, and nav patterns.

## Not Included

- Add horse.
- Inline edit.
- Attribute apply/save.
- Comment add/edit.
- Dedicated horse drawer profile form.
