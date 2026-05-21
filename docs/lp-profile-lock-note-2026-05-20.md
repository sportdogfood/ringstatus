# LP Profile Styling Lock Note

Date: 2026-05-20

Current checkpoint:
- Stop layout and styling changes here.
- The profile shell now uses top-level fixed bottom navigation.
- Riding uses nested tabs for Competitions, Classes, Horses, and Videos.
- Horses uses nested tabs for Ponies, Horses, Owned, and Hacked.
- Videos uses nested mock tabs and grid content, not a carousel.
- Contact uses a contact form instead of nested tabs.
- Bottom navigation clearance is handled by the shared `.lp-bottom-nav-offset` helper, not per-panel padding.

Next discussion:
- Inline edit behavior.
- Airtable persistence model.
- Which tabs/records should be editable.
- How change logs should be written.
- Whether edits should share the existing LP history enrichment endpoint or move to profile-specific Airtable tables.
