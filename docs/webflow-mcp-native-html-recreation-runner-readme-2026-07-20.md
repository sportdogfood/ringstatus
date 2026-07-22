# Webflow MCP native HTML recreation runner

Date: 2026-07-20

## Purpose

Use this handoff when a new Codex task must inspect HTML that already exists on a Webflow page and recreate it as native Webflow Designer elements through the Webflow MCP Bridge.

This is a staged Designer operation. It is not a publishing workflow, an Airtable workflow, or permission to change JavaScript or production behavior.

## Hard boundaries

- Use only the authenticated Webflow MCP Bridge tools.
- Call `webflow_guide_tool` before every other Webflow tool in the session.
- Do not use browser automation, direct API calls, page scraping, embeds, custom code, or a runner workflow as substitutes.
- Do not change Airtable.
- Do not change JavaScript, endpoints, cookies, storage, member gates, form behavior, or button-state logic unless separately authorized.
- Do not publish.
- Before any Webflow write, report the exact site, page, parent element, old subtree, new hierarchy, IDs, classes, and intended cleanup. Wait for explicit approval.
- After a failed write or verification gate, report `FAIL` and stop. Do not silently retry with a different implementation.

## Required source material

The task should provide:

1. The expected Webflow site name and site ID.
2. Enough repository evidence to identify the intended page without guessing.
3. The HTML source, or instructions to read the existing HTML from the verified page.
4. The styling source, if visual recreation is required.
5. A list of IDs that must be preserved.
6. Explicit production-behavior exclusions.
7. Whether an existing incomplete native subtree should be retained or replaced.

## Runner sequence

### 1. Read references completely

Read every named handoff, HTML file, CSS file, and page-identity reference before using Webflow.

Treat repository files as planning evidence. Confirm the current site, page, elements, and styles through Webflow before writing.

### 2. Perform the Webflow preflight

In this exact order:

1. Confirm Webflow MCP tools are attached.
2. Call `webflow_guide_tool` once and first.
3. Call the sites tool with `list_sites`.
4. Require the expected site name and exact site ID.
5. Search the site's agent instructions and read any relevant results.
6. List the live pages.
7. Match repository evidence to the live page title, slug, path, and page ID.
8. Stop with `FAIL` if page identity is ambiguous.

### 3. Inspect the current page read-only

1. Read the complete element tree or the bounded candidate subtree.
2. Confirm the exact parent element that will receive the native recreation.
3. Record the IDs of any incomplete or obsolete subtree that may later be removed.
4. Open the verified page in Webflow Designer.
5. Confirm the current page and Designer mode.
6. Take a pre-change element snapshot.

Do not write yet.

### 4. Prepare a Webflow-compatible build

Prefer `data_whtml_builder` when converting one bounded HTML tree into native Designer elements.

The WHTML input must follow these rules:

- Supply exactly one root element.
- Do not include `<style>` tags.
- Pass CSS separately in the `css` field.
- Preserve every required DOM ID in the HTML.
- Use semantic elements where the builder supports them.
- Add explicit classes when an element needs styling; do not depend on tag selectors.
- Create the new subtree beside the old subtree first. Do not remove the old subtree until the new one passes readback and snapshot verification.

### 5. Report the proposed write and wait

The approval request must state:

- site name and ID;
- page title, slug/path, and page ID;
- exact parent element ID and insertion position;
- exact old subtree ID, if replacement is intended;
- element hierarchy and every required DOM ID;
- classes/styles to be created;
- any necessary selector conversion;
- behavior explicitly excluded;
- `NOT PUBLISHED`.

Do not treat the original task request as approval for this write gate when the task explicitly requires a pre-write approval.

### 6. Create, verify, then clean up

After approval:

1. Insert the new native subtree beside the old subtree.
2. Require a successful builder response and capture the returned root element ID.
3. Query that exact new root with all descendants.
4. Verify every required DOM ID, element type, text value, input attribute, link, and class.
5. Take a snapshot of the exact new root.
6. Only after readback and snapshot pass, remove the previously approved obsolete subtree.
7. Query the page again to prove the new subtree remains and the old subtree is gone.
8. Take a final snapshot.
9. Report `PASS` or `FAIL` and stop.

## Known MCP Bridge obstacles

### CSS accepts only single-class selectors

The WHTML builder may reject:

- ID selectors such as `#card`;
- tag selectors such as `input`, `label`, `form`, or `h2`;
- universal selectors such as `*`;
- descendant selectors such as `.card .title`;
- compound selectors such as `.card.is-open`;
- selectors combining states and descendants;
- `!important` declarations.

For the most portable build, flatten CSS so every rule targets one class only:

```css
.recognition-card { ... }
.recognition-title { ... }
.recognition-input { ... }
```

Add the corresponding class directly to every HTML element. Preserve the DOM IDs separately for JavaScript and identity.

Do not translate behavior selectors into visual classes without calling out the semantic change. Hover, focus, disabled, hidden, open/closed, and member-gate rules may require manual Designer work or a separately approved implementation.

### Preview CSS should use final computed values

If the source CSS ends with preview-only overrides, the MCP-safe preview may use those final computed values directly on dedicated preview classes. Keep those classes confined to the staging/recognition page so they do not become production behavior.

### Webflow can rename styles

If a class name already exists, Webflow may create a new style with a suffix such as `-1` while keeping the requested DOM ID intact.

Read back every `styleNames` value. Do not assume the requested class name was retained. Report collisions for manual review before publishing.

### Native form conversion adds structure

HTML `<form>` elements may become native Webflow structures containing:

- `FormWrapper`;
- `FormForm`;
- `FormSuccessMessage`;
- `FormErrorMessage`.

The success and error blocks may be generated even when they were not in the supplied markup. Review them explicitly.

Some submitted attributes or button text may not appear in ordinary element-tree readback. Verify at least:

- submit-button labels;
- placeholders;
- `required`;
- `maxlength`;
- `pattern`;
- `inputmode`;
- `autocomplete`;
- input type;
- link destinations.

Use the element settings tool when the normal tree response does not expose a required setting.

### HTML buttons may be represented differently

The builder can return some button-like elements as `Link` or native `FormButton` elements. Verify the resulting element type, attributes, text, and intended nonfunctional preview behavior. Do not attach JavaScript to compensate unless separately authorized.

### Snapshots are a separate verification gate

A successful element insertion and successful element readback do not guarantee that `element_snapshot_tool` will succeed.

If snapshot returns `status:false`:

- report `FAIL` for the visual verification gate;
- retain the old subtree;
- do not publish;
- do not remove or rebuild elements without new approval.

### Webflow breakpoints are fixed

The WHTML builder accepts Webflow breakpoint media queries rather than arbitrary widths. Common accepted breakpoints are:

```css
@media screen and (max-width: 991px) { ... }
@media screen and (max-width: 767px) { ... }
@media screen and (max-width: 479px) { ... }
```

If the supplied CSS uses a different breakpoint, report the conversion instead of silently claiming an exact CSS recreation.

## Required verification report

| Gate | Required evidence |
| --- | --- |
| Connection | Expected Webflow site name and exact site ID matched live |
| Page identity | Live page title, slug/path, and page ID matched repository evidence |
| Baseline | Existing subtree read and pre-change snapshot captured |
| Approval | Exact proposed write approved explicitly |
| Creation | Builder returned success and the new root element ID |
| Identity | Every required DOM ID found exactly once in the intended new subtree |
| Structure | Expected native hierarchy and element types read back |
| Content | Required text, links, inputs, and attributes verified |
| Styles | Actual Webflow style names and collisions reported |
| Visual | New-root and final snapshots succeeded |
| Cleanup | Only the approved obsolete subtree removed after verification |
| Behavior | No unauthorized JavaScript, API, cookie, storage, gate, or form behavior changes |
| Publish | `NOT PUBLISHED` |

`PASS` requires every applicable gate. Any missing identity, rejected build, readback mismatch, snapshot failure, premature cleanup, or unauthorized behavior change is `FAIL`.

## Copy-ready task prompt

```text
Create a native Webflow Designer recreation of the existing HTML on the specified page using only the authenticated Webflow MCP Bridge.

Connection and references:
- Expected site: [SITE NAME]
- Expected site ID: [SITE ID]
- Page identity evidence: [REPOSITORY FILE OR EXACT REFERENCE]
- HTML source: [FILE PATH OR VERIFIED EXISTING PAGE SUBTREE]
- CSS source: [FILE PATH, IF APPLICABLE]
- Required DOM IDs: [LIST OR SOURCE THAT MUST BE PRESERVED]

Hard boundaries:
- Verify Webflow MCP tools are attached and call webflow_guide_tool first.
- Confirm the expected site with list_sites.
- Search site-specific agent instructions.
- Identify the exact live page from repository evidence plus the live page registry; do not guess.
- Open the verified page in Webflow Designer.
- Inspect the current native element tree and take a pre-change snapshot.
- Do not use an embed, browser automation, direct API, custom code, or another workflow as a substitute.
- Do not change Airtable.
- Do not change JavaScript, endpoints, cookies, storage, member-gate behavior, form submission behavior, or button-state logic.
- Do not publish.

Build requirements:
- Recreate the supplied HTML as native Webflow elements.
- Preserve every required DOM ID.
- Use one root element for WHTML insertion.
- Convert styling to Webflow-compatible single-class selectors when necessary. Do not use ID, tag, universal, descendant, compound, pseudo-state, or !important selectors in the WHTML CSS.
- Report every selector conversion and any Webflow-renamed style.
- Keep preview-only styling isolated to this page.
- Insert the new subtree beside the old subtree first.
- Do not remove the old subtree until the new subtree passes complete readback and snapshot verification.

Approval gate:
- Before any Webflow write, report the exact site, page, page ID, parent element ID, insertion position, old subtree ID, new hierarchy, required IDs, classes/styles, selector conversions, and proposed cleanup.
- Wait for explicit approval.

After approval:
- Create the native subtree once through the approved path.
- Query the returned root and all descendants.
- Verify all required IDs, text, links, button labels, input attributes, native form wrappers, and actual style names.
- Use element settings reads when normal tree readback omits required settings.
- Snapshot the new root.
- Only after every gate passes, remove the explicitly approved old subtree.
- Read back and snapshot the final page state.
- Report PASS or FAIL and stop.
- If any write or verification gate fails, do not retry through an alternate method, do not remove the old subtree, report FAIL, and stop.
```

## Final manual review before publishing

Even after a runner reports `PASS`, inspect the staged Designer page for:

- suffixed or duplicated styles;
- form success/error states;
- button labels and element types;
- placeholders and validation attributes;
- hover, focus, disabled, hidden, and open/closed states;
- breakpoint behavior;
- duplicate DOM IDs elsewhere on the page;
- any old subtree still present;
- production scripts that may bind to the preserved IDs.

Publishing remains a separate explicit task.
