# Lainey in the Ring - USEF Ride History Scope

Last updated: 2026-05-19

## Purpose

Create a Webflow-embeddable ride history experience for "Lainey in the Ring" that presents USEF history, videos, horses, competitions, and classes from static JSON feeds, with an optional inline enrichment mode for curating what appears in the public overview.

The public experience should be polished, mobile-first, and consistent across all section types. The edit experience should be simple enough to curate records without leaving the page, but it is not a full CMS.

## Current Assets

Local working folder:
`C:\Users\gombc\Documents\Codex\2026-05-18\files-mentioned-by-the-user-lp`

Repo/Webflow asset folder:
`C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\webflow\lp-history`

Generated Webflow assets:
- `lp-history-webflow-embed.html`: small Webflow Embed block loader.
- `lp-history.css`: scoped styles for `#lp-history-app`.
- `lp-history.js`: dependency-free client application.
- `lp-history-history.json`: normalized/static history feed.
- `lp-history-layer.json`: enrichment and curation layer.

Generator:
- `build_lp_history_embed.mjs`: source generator for the standalone preview, hydrate preview, and split Webflow assets.

Preview URL:
- `http://127.0.0.1:8787/lp-history-webflow-hydrate-template.html`
- Edit mode: `http://127.0.0.1:8787/lp-history-webflow-hydrate-template.html?key=edit`

## Embed Contract

Webflow should use the small loader in `lp-history-webflow-embed.html`.

The loader pulls these repo-hosted files:
- CSS from `https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history.css`
- JS from `https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history.js`
- History JSON from `https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history-history.json`
- Layer JSON from `https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history-layer.json`

The app must remain scoped under `#lp-history-app` and must not depend on Webflow classes or global site styles.

## Primary Public Views

Top-level tabs:
- Overview
- Videos
- Horses
- Competitions
- Classes

The tab cards also act as the top navigation and carry the assigned section colors.

Overview:
- Uses default curated content until a section has explicit overview selections.
- Once any records in a section are marked `status: overview`, that section uses only those overview records.
- Records marked inactive or ignore are hidden.
- Year pills filter the underlying data. Default active years are 2025 and 2026.
- Sections appear in this order: Videos, Horses, Latest competitions, Latest classes.

Videos:
- Favorites carousel at top.
- All videos list below.
- Video detail shows video/media at top, then detail head below the media.

Horses:
- Favorites/ribboned horse carousel at top.
- All horses below in the approved card/list pattern, not a separate unmanaged table.
- Horse card should visually match video cards.
- Horse detail includes hero image/media at top, then detail head below.

Competitions:
- List/card view for competitions.
- Competition detail should look like the parent list/detail pattern and include relevant class rows.

Classes:
- List/card view for classes.
- Class detail should stay consistent with competition detail patterns, using rows/tables that match the parent list language.

## Detail View Rules

All detail views should use the same visual system:
- A clear title and subtitle.
- Optional media/hero area above the detail head for videos and horses.
- Placement/ribbon tokens only when actual placing data exists.
- Detail rows should use the same row language as parent lists where practical.
- No sections label should appear in the public UI.
- No points or money aggregates should appear.
- USEF links render as `usef`, blue, small, and open in a new tab.

Class detail:
- Do not show points.
- Do not show back number or class code as top aggregate cards.
- Keep placement token constrained and clean.
- Details should include rows such as Horse, USEF #, Competition, Date, Entries, USEF link.

Horse detail:
- Includes placement summary only for achieved placings.
- Includes Classes and Competitions aggregates.
- Includes relevant class rows.
- Includes enrichment profile fields in edit mode.

Competition detail:
- Includes placement summary only for achieved placings.
- Includes Classes and Horses aggregates.
- Includes relevant class rows.

Video detail:
- Media/player is top.
- Metadata sits below media.
- Video row metadata should not include broad cadence labels like weekly/monthly unless the feed explicitly requires them.

## Enrichment Layer Contract

The history feed is source data. The layer feed is curation/enrichment data.

Layer record namespaces:
- `horses`
- `competitions`
- `classes`
- `videos`

Every enriched record may include:
- `recordState`: single choice, `active` or `inactive`.
- `status`: multiple choice, any of `overview`, `favorite`, `ignore`.
- `tags`: multiple choice where applicable.
- `notes`: optional free text where applicable.

Visibility rules:
- `recordState: inactive` means hidden everywhere.
- `status` containing `ignore` means hidden everywhere.
- `status` containing `overview` means include on the Overview page once that section is being curated explicitly.
- `status` containing `favorite` means show a favorite marker/icon hook. Final icon artwork is still TBD.

Status does not replace record state:
- Record state controls whether a record exists publicly.
- Status controls curation and emphasis.

Edit mode:
- Enabled by `?key=edit`.
- Saves draft changes to browser `localStorage`.
- Export/copy can produce an updated `lp-history-layer.json`.
- The page does not directly write to GitHub or Webflow.

## Enrichment Fields

Horses:
- `recordState`: active/inactive, single choice.
- `status`: overview/favorite/ignore, multiple choice.
- `imageUrl`
- `imageUrl_2`
- `image_upload`: browser-draft data URL only unless replaced by a hosted image URL.
- `barn_name`
- `show_name`
- `horseType`: Pony/Horse, single choice.
- `color`: Black/Bay/Chestnut/Grey/Paint/Palomino/Liverchestnut, single choice.
- `gender`: Gelding/Mare, single choice.
- `disciplines`: Hunters/Jumpers/Equitation, multiple choice.
- `age`: small numeric input.

Competitions:
- `recordState`: active/inactive, single choice.
- `status`: overview/favorite/ignore, multiple choice.
- `type`: Hunters/Jumpers/Equitation, multiple choice.
- `class_sequences`: Over Fences or Under Saddle/Flat, single choice.
- `tags`: seat/maclay/uset/ushja/wihs/3'3"/3'6"/classic/handy, multiple choice.

Classes:
- Same enrichment shape as competitions.

Videos:
- `recordState`: active/inactive, single choice.
- `status`: overview/favorite/ignore, multiple choice.
- `videoUrl`
- `embedUrl`
- `thumbnailUrl`
- `playlist`
- `tags`: seat/maclay/uset/ushja/wihs/3'3"/3'6"/classic/handy, multiple choice.

## Editing UI Rules

Inline edit controls should not invent a new visual system.

Rules:
- Use `lp-row is-static is-detail` row language for enrichment rows.
- Use pill controls for choices.
- Choice pills stay in one horizontal row.
- On smaller widths, pills scroll left/right with `overflow-x: auto`; they do not wrap into tall stacks.
- Labels align like detail table labels.
- Inputs use normal readable font sizing and left-justified text.
- Export/copy controls belong in edit mode only.

## Styling Contract

Typography:
- Google Font: Outfit.
- Headings: weight 600, line-height `0.95em`, letter-spacing `-0.06em`.
- Body/text: weight 400, line-height near `0.90em`, letter-spacing `-0.02em`.
- Typography is black by default.
- Hot links can be blue.

Color:
- Each section has an assigned shade.
- Active tab uses a darker version of its assigned shade.
- Rows and row hover states use diluted versions of the assigned shade.
- Rollovers should be subtle, not white and not overly raised.
- Section titles use the active shade background with white text.

Current section assignments:
- Overview: `#46332b`
- Videos: blue family
- Horses: green family
- Competitions: purple family
- Classes: red family

Placement colors:
- 1st: `#0057B8`
- 2nd: `#D71920`
- 3rd: `#FFD200`
- 4th: `#FFFFFF`
- 5th: `#F4A6C1`
- 6th: `#00843D`
- 7th: `#6F2DA8`
- 8th: `#8B5A2B`

Placement/ribbon rules:
- Only show a ribbon token when a real numeric placing exists.
- DNP is hidden.
- Points do not equal placing.
- The token must be constrained with clear min/max dimensions.

## Data Rules

Current history feed includes USEF class history through the available source payload.

Data normalization produces:
- competitions
- horses
- class rows
- mock/current video records until a real video feed is attached

Do not invent placings, ribbons, money, or points.

Rows with null class code remain valid. The class title is the primary class label.

Sections from source data are internal categorization only and should be hidden from the public UI unless this scope changes.

## Future Feeds

Additional feed/layer work expected:
- May 3 to current history feed.
- Ignore/favorite/overview curation lists.
- Horse profile data: image, gender, type, color, age, active/inactive.
- Video metadata: video URL, embed URL, thumbnail URL, playlist, tags.
- Global tagger output from `group_tags-global-tagger.csv` or future equivalent.
- Potential automatic tag qualification before manual edits.

The public UI should read combined source + layer data, but layer data should remain the curation authority.

## Save/Publish Workflow

Current workflow:
1. Open local edit URL with `?key=edit`.
2. Make inline curation/enrichment edits.
3. Draft saves to browser localStorage.
4. Copy/export layer JSON.
5. Replace/update `lp-history-layer.json` in the repo asset folder.
6. Push to GitHub.
7. Webflow embed loads updated CDN assets.

Important limitation:
- Browser image upload currently produces a local draft data URL. For production, images should be hosted and stored as stable URLs in the layer JSON.

## Non-Goals For Current Phase

Not in scope yet:
- Full CMS backend.
- Direct GitHub writes from the browser.
- Authentication beyond the simple `?key=edit` edit-mode gate.
- Live fetch from USEF or RingStatus runtime endpoints.
- Rebuilding as a framework app.
- Reintroducing money or points aggregates.

## Acceptance Checks

Before treating a change as ready:
- Preview loads at `http://127.0.0.1:8787/lp-history-webflow-hydrate-template.html`.
- Edit mode loads at the same URL with `?key=edit`.
- Top tabs switch correctly.
- Overview shows default content until a section has explicit overview selections.
- Once a section has explicit overview selections, Overview shows only those selected records for that section.
- `recordState: inactive` hides records.
- `status: ignore` hides records.
- `status: favorite` adds the marker hook.
- Modal close button and backdrop close work.
- Enrichment pills click and save to draft.
- Pill rows remain horizontal and scroll on narrow widths.
- Horse, competition, class, and video details open.
- USEF links open in a new tab.
- No sections, points, money, or invented placing logic appears.
