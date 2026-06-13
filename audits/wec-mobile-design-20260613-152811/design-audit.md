# WEC Mobile Design Audit

Audit date: 2026-06-13
Target: https://ringstatus.com/wec-mobile
Viewport: 390 x 844
Scope: Mobile UI design advice only. Print/PDF behavior was not audited.

## Source Of Truth

- Live surface captured from `https://ringstatus.com/wec-mobile`.
- Local mobile embed source family confirmed at `docs/horseshowing/webflow-drops/wec-mobile-webflow-embed.html`.
- HTML/TXT drop parity confirmed: `wec-mobile-webflow-embed.html` and `wec-mobile-webflow-embed.txt` have matching SHA-256 hashes.

## Captured Evidence

1. `01-mobile-top.png`
   - Initial mobile state with header, date, horse filter chips, GRAND schedule card, and bottom ring nav.
   - General health: usable, dense, and operational, but hierarchy is compressed.

2. `02-mobile-scrolled.png`
   - Scrolled schedule state around INDR_1 content with active bottom ring nav.
   - General health: data remains readable, but repeated rows lose grouping and scan rhythm.

3. `03-date-edit-open.png`
   - Attempted date-edit state.
   - General health: no distinct date-edit UI was visually confirmed from this capture, so date-edit design advice remains a verification gap.

## Strengths

- The mobile screen is strongly task-focused: users immediately get show name, date, ring schedule, horse filters, and ring navigation.
- The sticky bottom ring nav is the right interaction pattern for a multi-ring schedule.
- The active ring state is visually clear enough to orient the user after scrolling.
- Trainer/horse rollups above class rows are useful and should stay close to the affected class group.
- The current palette is restrained and avoids looking like a marketing page, which fits an operational show-day tool.

## UX Risks

1. The top filter chips and bottom ring nav compete.
   - Both are horizontal chip rows with similar shape, weight, and behavior. Users must infer that the top row filters horses while the bottom row jumps rings.
   - Recommendation: make the top row visually lighter and label it implicitly through content treatment, for example smaller horse chips, less border weight, and a subtle selected/filtered count only when active.

2. The active context is too easy to lose while scanning.
   - The top header disappears from the captured scrolled state, while the bottom ring nav remains. Users can tell the active ring, but not always the selected date or whether a horse filter is active.
   - Recommendation: keep a compact sticky status strip above the schedule: date + active horse filter state + active ring. Keep it text-light, not a second header.

3. Schedule rows are readable but not optimized for fast triage.
   - Time and class name columns are consistent, but long class names truncate without a clear priority hierarchy. The money/class prefix often competes with the actual class description.
   - Recommendation: use a two-line row pattern: time as fixed-width left rail, primary class title on line one, money/section/rule detail as smaller secondary text on line two.

4. Group headers need stronger separation from classes.
   - Trainer badges and horse names are valuable, but current group blocks blend into the row stack after several screens.
   - Recommendation: keep group headers sticky within each ring section only if technically simple; otherwise add more vertical breathing room and a lighter background band for group starts.

5. Empty messages are too repetitive.
   - Multiple "No classes scheduled." rows create noise and feel like data errors when repeated between normal rows.
   - Recommendation: collapse consecutive empty states into one quieter row per group or ring segment.

## Accessibility Risks

1. Chip target sizing may be tight in dense states.
   - The chips appear close to minimum comfortable size. Users in show environments may be walking, outdoors, or using one hand.
   - Recommendation: maintain at least 44px effective tap height for ring nav and horse chips, even if the visual pill is slightly smaller.

2. Truncated class names hide important information.
   - Screenshot-only audit cannot verify accessible names, but visually truncated class text may block decision-making.
   - Recommendation: allow row expansion on tap or use a controlled two-line wrap before truncation.

3. Active states rely heavily on color/fill.
   - The selected bottom ring uses a filled purple treatment. This is likely readable, but it should also include shape or weight differences.
   - Recommendation: keep the filled active pill, and add a stronger font-weight or top indicator so selection survives low contrast or glare.

4. Date edit state was not visually confirmed.
   - Recommendation: verify that date-change controls are reachable, obvious, and have clear focus/active states before changing the date UI.

## Recommended Mobile Direction

Use a "show-day control surface" model:

- Header: compact show name and date, with date edit as a small secondary action.
- Sticky context strip: active date, active ring, active horse filter state.
- Horse filter row: lighter treatment than ring nav, with an "All" chip when no horse is selected.
- Ring schedule: grouped by trainer/horse, with fixed time rail and two-line class rows.
- Bottom ring nav: keep it sticky, but preserve horizontal scroll position and make active ring unmistakable.

## Priority Changes

1. Separate horse filters from ring navigation visually.
2. Redesign class rows around fixed time rail plus two-line class detail.
3. Add a compact sticky context strip for active date/ring/filter state.
4. Reduce repeated empty-state noise.
5. Verify and improve the date-edit interaction after capturing it in a confirmed top-of-page state.

## Verification Status

- Source of truth confirmed: pass.
- Workflow logic confirmed: pass for mobile navigation/filter structure from live DOM and local embed source.
- Customer-facing output confirmed: pass for live mobile screenshots.
- Browser/render check on exact target page: pass for `https://ringstatus.com/wec-mobile` at 390 x 844.
- API/data check: not applicable; no data or endpoint changes.
- TXT/HTML/drop parity: pass for mobile embed files.
- PDF/share/print path: not applicable by scope.
- Audit confirms no missing priority records: fail; this was a design audit, not a full data completeness audit.
- Live/published surface confirmed: pass for captured live mobile page.
