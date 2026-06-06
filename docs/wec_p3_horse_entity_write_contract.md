# WEC P3 Horse Entity Write Contract

## Scope

P3 adds the first controlled horse entity write surface inside the hybrid app.

It does not touch packing quantities, kit item states, plan logs, or packing plan tables.

## Backend

- `POST /wec-packing/horses` supports the existing horse entity actions:
  - `add_horse`
  - `edit_horse`
  - `apply_horse_attribute`
- Writes are restricted by `HORSE_ENTITY_ALLOWED_WRITE_FIELDS`.
- `dryRun: true` validates allowed fields without creating or patching Airtable records.
- Real writes continue to create `horses_change_log` audit records through the existing horse entity module.

## Frontend

- Horse rows expose an `OPEN` action.
- `ADD HORSE` appears on horse roster/profile/attribute surfaces.
- The drawer uses the existing global drawer, row, label, input, and button classes.
- Editable fields are limited to:
  - `barn_name`
  - `show_name`
  - `notes`
  - `active`
  - `inactive`
  - `wec_wave_1`
  - `wec_wave_2`
  - `wec_not_going`
- Save only sends allowed fields that changed.
- Save payload includes the current session key and device id.

## Verification

- Use `dryRun: true` for backend write verification.
- Browser verification may open the drawer and edit draft fields, but should not click real `SAVE` unless explicitly approved.
