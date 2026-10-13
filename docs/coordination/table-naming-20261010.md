# Table naming normalization — 2026-10-10

Owner rule: one word where practical; otherwise lowercase words separated by hyphens. Preserve ___ legacy prefix. Applies to Recognize and Tenants in this task. Existing table IDs, data and fields retained.

- app9kOZdIaGyKk5uG/tblfkRSJAEMzuzApR: Recognition devices → recognition-devices
- app9kOZdIaGyKk5uG/tblWjbASVMIjFLyW8: Footprints → footprints
- app9kOZdIaGyKk5uG/tbly1PM5iFYqVzKSm: People → people
- app9kOZdIaGyKk5uG/tblgDWKi0Bb6OcoqS: rs_phone_aliases_test → rs-phone-aliases-test
- app9kOZdIaGyKk5uG/tblxC4SYtzYhJNcJS: SMS verification requests → sms-verification-requests
- app9kOZdIaGyKk5uG/tblpyyaOMgjLzLvkP: ___rs_input_horses → ___rs-input-horses
- app9kOZdIaGyKk5uG/tblBgvP7pYSv8o20j: Sessions → sessions
- app9kOZdIaGyKk5uG/tblYgoeLEey05xgw9: ___rs_input_users → ___rs-input-users
- app9kOZdIaGyKk5uG/tblF1Hdqi3yoKTZPV: ___rs_sms_events → ___rs-sms-events
- app9kOZdIaGyKk5uG/tbl5Kng9jk0lhE2f9: ___active_iams → ___active-iams
- app9kOZdIaGyKk5uG/tbl0oVtuFNRy7zWsX: ___rs_source_ids → ___rs-source-ids
- app9kOZdIaGyKk5uG/tblbpALv7flu3NNEE: ___rs_input_subscriptions → ___rs-input-subscriptions
- app9kOZdIaGyKk5uG/tblRvTwo3HYPUkZou: ___rs_input_barns → ___rs-input-barns
- app9kOZdIaGyKk5uG/tblwts3huk3w1ACjh: ___rs_input_events → ___rs-input-events
- app9kOZdIaGyKk5uG/tblnd2ToLs7dTzLAM: ___rs_input_riders → ___rs-input-riders
- app9kOZdIaGyKk5uG/tbluBFpQQMKDdVvEP: ___rs_input_assignments → ___rs-input-assignments
- app9kOZdIaGyKk5uG/tbl0cNCmNwSwsCUF6: ___rs_input_memberships → ___rs-input-memberships
- app9kOZdIaGyKk5uG/tblEuwOr1rUKnj1j3: ___rs_input_locations → ___rs-input-locations
- app7BXcw801n19QKF/tbleGDeOz4XDIMAGd: tenant_level → tenant-level
- app7BXcw801n19QKF/tblOSFSJOAvDzI6wT: ___rs_input_riders → ___rs-input-riders
- app7BXcw801n19QKF/tbld6HQpYGoko0KKE: ___rs_input_horses → ___rs-input-horses

Alias compatibility: existing Inputs recognition adapter now selects alias table ID only for Recognize base; legacy-base behavior retained. 44 recognition/access tests and Astro build passed. Commit 18e6e993aeaff6fd53168dac4d6f1b7b46baf195 deployed successfully as 6aa4b4de-a837-40a6-9c33-341041e38b50 at 2026-10-10T15:33:38.686Z. Alias renamed after deployment success. No business workflow test or business records written.

Final live readback: all 21 renames applied; all 29 Recognize and 39 Tenants table names conform. Each index contains exactly one matching name and stable table link for every physical table; zero mismatches. Naming completion does not establish application workflow completion.

