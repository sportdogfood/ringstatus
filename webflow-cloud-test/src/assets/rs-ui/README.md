# Shared screen styles

Edit `screen.css` once. Live Profile and Recognize import it; their existing response routes embed that exact source. The Profile, Recognize and Opt-ins prototypes link to `/test/rs-ui/screen.css` on ringstatus.com. Do not copy this stylesheet into prototypes or add screen-specific overrides.

Shared stack: back-row (when needed), brand-row (including close-button), page-row/screen-title, team-row (when known), person-row (when needed), content sections, screen-footer (when needed). Reuse the shared classes for rows, titles, fields, buttons, search/sort and icons. Missing content omits its row; it does not introduce a different style.

These styles do not grant identity, alter permissions, connect mock destinations, or change Opt-ins browser-local preferences.
