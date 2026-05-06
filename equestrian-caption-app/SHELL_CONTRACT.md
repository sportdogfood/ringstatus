# Branded Mobile Shell Contract

This document is the handoff and operating contract for the branded mobile app shell in this Vite React application. It describes the reusable skeleton, navigation cadence, persistence model, tap behavior, styling system, and the rules future implementers must follow when retrofitting this shell to new data, a new use case, or new responsibilities.

This is not a use-case specification. The current caption-builder content can change. The branded shell must not change unless the owner explicitly instructs that the shell itself should be changed.

## Authority

This contract is authoritative for the shell.

Codex, automated runners, agents, refactor tools, UI generators, and future implementers are not permitted to make styling, layout, cadence, navigation, component-shape, spacing, color, sizing, or shell-behavior changes to this branded shell unless the owner explicitly asks for that exact shell change.

The owner may ask for new data, new responsibilities, new labels, new flows inside existing screens, new generation logic, new storage fields, new integrations, or new business logic. Those changes must be fitted into the existing shell. They do not grant permission to redesign the shell.

If there is uncertainty, the correct behavior is to stop and ask before changing the shell.

## Non-Negotiable Rules

1. Do not redesign the app shell.
2. Do not create new visual systems.
3. Do not introduce one-off status dots, one-off active buttons, one-off panel styles, or one-off navigation treatments.
4. Do not replace the bottom bar cadence.
5. Do not replace the top header cadence.
6. Do not change the phone-shell dimensions, dark brand palette, row cadence, tap feedback, or screen structure without owner approval.
7. Do not change component styling to fit new data. New data must fit existing components.
8. Do not add decorative backgrounds, hero sections, marketing layouts, oversized cards, or new visual motifs.
9. Do not move persistence out of the 7-day device-memory model unless the owner explicitly asks.
10. Do not weaken mobile-first behavior for desktop convenience.

## Codebase Overview

Project root:

```text
equestrian-caption-app/
```

Primary files:

```text
src/App.tsx
src/index.css
src/components/ui/
src/types/rita.d.ts
package.json
vite.config.ts
```

Framework:

```text
Vite + React + TypeScript
```

Important dependencies:

```text
react
react-dom
framer-motion
lucide-react
fuse.js
rita
tailwindcss
@tailwindcss/vite
```

`src/App.tsx` currently contains most app logic and shell composition. `src/index.css` contains the branded shell tokens and component classes. Future refactors may split data or logic into separate files, but they must preserve the shell contract and class semantics.

## Shell Structure

The app shell is:

```text
.page
  .app
    .app-header
    .app-main
    .app-nav
```

This hierarchy is locked.

`.page` centers the phone shell and owns the outer viewport behavior.

`.app` is the branded mobile viewport. It has a fixed phone-like canvas on larger screens and fills the viewport on small screens. It owns the dark background, rounded outer border on desktop, safe-area padding, max width, max height, and hidden overflow.

`.app-header` is the top navigation/header region. It is compact, fixed height, dark, and uses a three-column layout:

```text
Back slot | Title | Action slot
```

`.app-main` is the current screen content area. It flexes, scrolls only when needed, and keeps screen content inside the branded dark radial background.

`.app-nav` is the bottom navigation bar. It is persistent, horizontal, scrollable when needed, and must remain the app-wide route switcher.

## Screen Cadence

The app uses four top-level screens:

```ts
type Screen = "start" | "create" | "logs" | "dashboard";
```

This top-level set is the shell cadence. New responsibilities should map into this cadence before adding new top-level destinations.

### Start

Start is the session entry point. It uses row controls, not a card-heavy landing page.

Current row cadence:

```text
In-session
Summary
Restart session
```

These rows use shared row styles:

```text
row
row--tap
row--active
row-title
row-tag
row-tag--boolean
row-tag--positive
```

There must not be custom start-only status indicators. For example, do not add `start-status-dot`, `is-on`, or similar one-off status classes. Status indicators must use the shared `row-tag--boolean` pattern.

Start also displays subtle device autosave status text and a subtle one-line Voice Profile toggle. The Voice Profile control is intentionally quiet. It must not become a large panel unless the owner explicitly asks.

### Create

Create is a wizard flow inside the shell.

Current create steps:

```ts
type CreateStep = "postType" | "tags" | "image" | "captions";
```

The first three steps use the progress indicator:

```text
Post
Tags
Image
```

The final caption screen does not show the progress bar.

Step behavior:

```text
Post Type -> Tags -> Image + Description -> Caption Options
```

The top header owns Back and Next/Gen where appropriate. The screen body owns sticky bottom actions where continuity requires a bottom button.

### Logs

Logs show saved items inside the existing panel/list shell. Any new log-like data must use the existing card and row conventions. Do not introduce new list card styling unless explicitly approved.

### Dashboard

Dashboard shows summary/progress information inside the existing panel structure. New metrics should use existing rows, tags, progress tracks, and panels.

## Top Header Contract

The top header is `.app-header`.

It must stay:

```text
left action slot
center title
right action slot
```

Current header action classes:

```text
header-back
header-action
is-invisible
header-title
```

The Back button appears on create screens. The action button appears when a create step needs Next or Gen. Invisible header slots maintain alignment and must not be replaced by layout hacks.

Do not remove the three-column header structure. Do not center titles by absolute positioning. Do not add a second header row unless explicitly requested.

## Bottom Navigation Contract

The bottom nav is `.app-nav` containing `.nav-strip` and `.nav-btn`.

Current bottom destinations:

```text
Start
Create
Logs
Dashboard
```

Buttons must not use icons unless the owner explicitly asks. Text must not spill beyond button bounds. The nav strip may scroll horizontally when all buttons cannot fit.

Active bottom nav styling is shared through:

```text
nav-btn
nav-btn is-active
nav-btn[aria-pressed="true"]
```

The active button uses the primary blue active treatment. Do not create a second bottom-nav active style.

## Tap Cadence

The brand interaction model is tap-first.

Every tappable control should feel like the TackLists cadence:

```text
compact
rounded pill or panel
dark surface
thin border
active blue border/fill
fast press movement
clear selected state
no oversized marketing controls
```

Core classes:

```text
row
row--tap
row--active
row-title
row-tag
row-tag--boolean
row-tag--positive
tap-panel
tap-panel-header
tap-panel-title
tap-panel-content
tap-button
tap-button--primary
tap-button--secondary
tag-pill
tag-pill is-active
nav-btn
nav-btn is-active
```

These classes are shell primitives. Reuse them. Do not clone them under feature-specific names.

## Row Contract

Rows are the primary shell unit.

Use rows for:

```text
start options
post type selection
summary choices
single-select options
compact navigation choices
status rows
```

Row anatomy:

```tsx
<button className="row row--tap {active ? 'row--active' : ''}">
  <span className="row-title">Label</span>
  <span className="row-tag">Value</span>
</button>
```

Boolean status row anatomy:

```tsx
<span className="row-tag row-tag--boolean {positive ? 'row-tag--positive' : ''}" />
```

`row-tag--boolean` is locked at 18px by 18px. Do not create alternate dot sizes.

## Panel Contract

Panels are the second shell unit.

Use panels for grouped screen content:

```text
Post Type
Tags
Image + Description
Caption Options
Logs
Dashboard
```

Panel classes:

```text
tap-panel
tap-panel-header
tap-panel-title
tap-panel-content
```

Do not nest cards inside cards. Do not make page sections floating decorative cards. Do not add card-heavy dashboard or marketing layouts.

## Tag/Pill Contract

Pills are selection hints, not content that must be pasted into output text.

Tag lanes use:

```text
tag-purpose-group
tag-purpose-label
tag-filter-input
tag-pill-wrap
tag-pill
tag-pill is-active
```

The detail lane and tone lane each have a filter input above the horizontal pill grid. The inputs use Fuse.js for local search. Selected pills remain visible even when the filter changes.

Pill data may include metadata:

```ts
source
aliases
attributes
appliesTo
selectedBehavior
priority
```

Pill metadata is used for filtering, ranking, and context hints. It must not force a literal word into generated text.

Do not style detail pills and tone pills with separate visual systems. Their meaning differs; their shell behavior does not.

## Caption Engine Contract

Caption generation is not part of the branded shell. It is a swappable responsibility behind the shell.

Current local engine:

```text
starterPools
highlightedLinesByPostType
selected tag hint scoring
RiTa grammar candidate
description token hints
banned phrase cleanup
line limits
dedupe
```

Fuse.js is used for tag search. RiTa is dynamically imported only during caption generation so the main app shell stays lighter.

Future AI or local API generation may replace the caption engine. That must not change the shell. The shell should continue to send structured context and render four returned options in the existing Caption Options screen.

Selected pills are hints. They are not caption copy.

## Device Memory And 7-Day Persistence

The app uses device memory through `localStorage`, not a browser cookie.

The owner may refer to this as a 7-day cookie because the product behavior is "remember my session for 7 days." Technically, the implementation is local device storage.

Current key:

```ts
const SESSION_STORAGE_KEY = "lainey-caption-builder-session-v1";
```

Current time-to-live:

```ts
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
```

Persistence functions:

```text
readStoredSession
writeStoredSession
clearStoredSession
formatSessionTime
```

Persisted state includes:

```text
postType
description
photoUrl
photoName
generationRound
selectedCaption
savedPosts
showVoiceProfile
selectedTagIdsByType
savedAt
expiresAt
```

This is what allows data to remain intact after:

```text
browser refresh
Safari close/reopen
normal tab close/reopen
temporary app interruption
```

The persistence model expires the session after 7 days. Reading an expired session removes it. Restart session clears storage and resets the app.

Do not replace this with React-only state. Do not move this to sessionStorage. Do not reduce the TTL. Do not clear on refresh. Do not clear on Safari close.

If a backend account system, cookie, server session, or cloud sync is added later, the 7-day device-memory behavior must remain unless the owner explicitly approves replacing it.

## Mobile-First Contract

The shell is mobile-first.

Primary target widths include:

```text
390px x 844px
430px x 932px
```

Desktop is only a preview container for the phone shell. Desktop convenience must not change mobile behavior.

The shell must:

```text
use full viewport height
respect safe-area padding
avoid vertical scrolling unless content height requires it
allow horizontal scrolling for crowded nav and pill lanes
keep text inside controls
preserve tap-sized controls
avoid layout jumps on active state
```

## CSS Token Contract

Brand tokens live in `:root` in `src/index.css`.

Important tokens include:

```text
--bg
--bg-grad-1
--bg-grad-2
--fg
--fg-soft
--muted
--surface-1
--surface-2
--header-surface
--border
--border-soft
--accent
--accent-2
--accent-3
--pill-radius
--card-radius
--header-h
--h-row
--h-nav
--app-maxw
--app-maxh
```

Do not add competing brand colors or component-level color systems. Extend tokens only with owner approval.

## Styling Lock

The following are locked shell decisions:

```text
dark radial app background
phone-shell max width and max height
rounded desktop phone shell
full viewport mobile shell
compact top header
bottom nav bar
row/pill tap cadence
18px boolean row indicator
blue active treatment
green positive boolean indicator
thin borders
small typography
horizontal pill lanes
subtle Voice Profile row
sticky bottom action areas
```

Future content must use this shell. Do not restyle components because a new use case "feels different." If new data does not fit, change the data mapping, labels, grouping, or flow inside existing components before touching shell styles.

## Allowed Changes Without Owner Approval

Allowed when implementing a new use case:

```text
Add or change data arrays.
Add or change labels.
Add or change post/use-case types.
Add metadata fields to data objects.
Change generation/business logic.
Change persistence payload fields while preserving 7-day device memory.
Add parsing, filtering, ranking, or API integration behind existing screens.
Add tests or docs.
Split App.tsx into smaller files without changing behavior or classes.
Fix bugs that preserve shell behavior exactly.
```

## Forbidden Changes Without Owner Approval

Forbidden unless the owner explicitly asks:

```text
Changing root shell layout.
Changing .page, .app, .app-header, .app-main, or .app-nav structure.
Changing dark brand palette.
Changing button, row, tag, panel, or nav active styling.
Adding feature-specific duplicates of shell classes.
Adding one-off dot/status classes.
Changing row-tag--boolean away from 18px.
Changing bottom nav labels into icons.
Removing horizontal overflow behavior from nav or pill lanes.
Replacing Start/Create/Logs/Dashboard cadence.
Adding landing-page or hero-page patterns.
Adding decorative orbs, bokeh, gradients outside the existing shell background, or illustration-first UI.
Changing mobile-first viewport behavior.
Removing 7-day localStorage persistence.
Clearing user data on refresh or Safari close.
Changing autosave/restart semantics.
```

## Retrofit Procedure For New Data Or New Responsibilities

When adapting this shell to a new use case:

1. Identify which existing screen owns the responsibility.
2. Map new data into existing row, panel, pill, nav, or log patterns.
3. Add metadata to data objects before adding new components.
4. Keep active state derived from app state, not baked into static data.
5. Keep persistence in the existing 7-day localStorage pattern.
6. Use existing class names.
7. Add new shell classes only when there is no existing primitive and only with owner approval.
8. Run the verification checklist.

## Verification Checklist

Before handing off changes:

```powershell
npm run build
npm audit --omit=dev
rg "start-status-dot|is-on" src
```

Expected:

```text
npm run build passes
npm audit --omit=dev reports 0 vulnerabilities
rg finds no one-off start status classes
```

Also manually check:

```text
390px x 844px viewport
430px x 932px viewport
Start rows
Create > Post Type rows
Create > Tags filters and horizontal lanes
Create > Image + Description
Caption Options
Logs
Dashboard
Bottom nav horizontal overflow
7-day storage survives refresh
Restart session clears storage
```

## Handoff Summary

This app is a branded mobile shell with replaceable content and replaceable business logic. The shell is the product surface. Future work should treat the shell as locked infrastructure.

New data can change. New responsibilities can be added. Caption engines can change. APIs can be added. The shell must stay consistent.

Any agent or runner that edits this project must assume:

```text
Do not change shell styling.
Do not create new visual components when existing shell primitives fit.
Do not clear user data on refresh or Safari close.
Do not deviate from the Start/Create/Logs/Dashboard cadence.
Do not touch the branded shell unless the owner specifically instructs it.
```

