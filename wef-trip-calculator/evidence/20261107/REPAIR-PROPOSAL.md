# Smallest correction after failed verification

The approved trip-status table exists and its 31 field names/types were read back. New code is frozen, not connected to a business runner. No deployment, task/schedule change or business-row write is included in this proposal.

Explicit APPROVED TO EDIT is required by the owner's failed-verification rule before implementing these corrections or retesting the affected calculator path:

1. `wef-trip-calculator/test.js`: derive the expected retrospective output count from the actual November6 Active-trip subset. The saved fixture has656total rows:592active and64dropped. Preserve the original rows, source timestamps and production Active filter; do not hardcode592 or alter the fixture to force a pass.
2. `wef-trip-calculator/trip-status.js`: resolve current/underway timing using the compatible saved Ring and Live state rather than requiring the older Data class status to already say Underway. Preserve original source times and fixed2.5746pace. If Ring/Live identity/state or the anchor cannot support a pending prediction, leave the affected derived ETA unavailable; do not publish a past start-based ETA as a pending go time. Preserve gone/scratched history and separate provider estimates.

After approval: one focused corrected offline execution against the saved November6 data and explicit contract cases, then independent Code Review of the exact final diff and applicable checks. Preserve the existing cleanup implementation, independently passed cleanup evidence, all upstream Ring/Class/Data/Live/Entry/legacy code, synced tables and schema. Do not add a deployment route, schedules, Records alerts, source acquisition, record repairs or upgrades. Existing missing November6 Class/Ring answers remain visible and are not repaired or fabricated.

Independent review findings and their final disposition are recorded separately under reviewer/. This proposal grants no repair, retest or deployment authority.
