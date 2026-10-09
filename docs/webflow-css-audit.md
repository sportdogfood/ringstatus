# Webflow CSS audit

Reusable procedure for auditing and making authorized styling corrections to an existing Webflow page. This document is a workflow reference, not an installed Codex skill. Refer to this file in a future prompt to reuse it.

## Purpose and boundaries

Inspect the actual page and explain why its rendered styling differs across widths or between Designer and the published site. Preserve existing content, native elements, assets, order, interactions, and working implementations. Audit first; apply only the corrections the owner authorizes. Do not publish unless explicitly requested.

Stay focused on typography, spacing, alignment, responsive layout, media proportions, tabs, navigation, forms, and overflow. Link destinations, semantics, and business behavior are separate audit scope unless requested or necessary to explain a visible defect.

## Applicable skills and references

Load applicable sources that are available; record what was actually read. Do not claim an entire plugin or every available skill was loaded.

| Skill or reference | Purpose |
| --- | --- |
| Instruction Loader, when explicitly invoked and available | Identify applicable instructions and retain task scope. |
| Webflow MCP: Designer Tools | Inspect native elements, styles, breakpoints, and Designer state; make native editable corrections. |
| Build Web Apps: Frontend Testing and Debugging | Inspect rendered geometry, overflow, and responsive behavior. |
| Superpowers: Systematic Debugging | Establish the evidenced cause before proposing a repair. |
| Superpowers: Verification Before Completion | Separate saved properties from rendered verification. |
| Computer Use | Inspect screenshots and browser state, measure layouts, and test local scrolling. |
| RingStatus Control, for RingStatus work | Preserve working baselines and enforce scoped corrections and evidence. |
| Webflow MCP: Custom Code Management, when overrides are relevant | Inspect accessible custom CSS/script sources and their precedence. |
| Webflow MCP: Accessibility Audit, only for relevant checks | Inspect readability and control usability; avoid expanding a styling audit into a full accessibility audit. |
| Webflow MCP: Flowkit Naming, when class naming is relevant | Understand naming; do not rename existing classes as an unsolicited cleanup. |

Frontend App Builder was loaded during the original conversation, but a targeted audit of an existing page does not require building or redesigning an app. Other Superpowers skills should be selected for their actual applicability. Global/repository AGENTS.md, the Webflow tool guide, and project evidence controls are instructions or references, not skills.

## Establish the target

1. Confirm the current Designer page name, page ID, and slug without switching the owner's page unnecessarily.
2. Identify the published URL, reference appearance, requested widths, and whether the work is audit-only or includes corrections.
3. Record the current native classes, relevant breakpoint properties, and rendered baseline before edits.
4. For RingStatus, use the existing authorized Webflow MCP and verified site ID `6982268b7543ac3c80151266`. Call the guide once at the beginning of Webflow work. Do not repeat site discovery or initiate OAuth as a routine check.
5. Preserve previous mobile corrections when the new request concerns desktop. Changes to base classes can cascade into smaller breakpoints and other pages; inspect class usage and overrides before writing.

## Audit one section at a time

Capture the section visually, inspect its native elements, and measure its rendered geometry. Record the observation, exact element/class, relevant rule, demonstrated cause, and smallest correction.

| Area | Checks |
| --- | --- |
| Typography | Font loading, size, weight, tracking, line-height, wrapping, manual breaks, negative margins, and fit inside the actual parent. |
| Spacing | Side gutters, section padding, heading/body gaps, kicker alignment, and repeated-section consistency. |
| Navigation and tabs | Minimum label width, intrinsic sizing, wrapping, shrink behavior, alignment, and local scroll access. |
| Images and video | Wrapper size, intrinsic dimensions, aspect ratio, internal blank space, cropping, and reserved space for lazy media. |
| Forms and footer | Input/control dimensions, text fit, alignment, footer nav wrapping, and visible contrast. |
| Overflow | Compare document scroll width with client width; identify the actual offending element and its parent layout. |

Distinguish intentional local scrolling from unwanted whole-page sideways movement. Do not use blanket horizontal clipping to conceal inaccessible content.

## Typography scaling

Choose scaling based on the actual layout:

- Use `cqw` when text should grow with a defined parent query container. Establish `container-type: inline-size` on a suitable parent and check its sizing effects.
- A parent capped at a maximum width also caps container-relative growth. If the owner wants gentle growth on wider screens despite that fixed column, a capped viewport-based expression can be appropriate.
- Use `clamp(minimum, preferred, maximum)` to retain a readable floor and a restrained ceiling. Set values from the current baseline and requested widths, rather than copying one percentage to every heading.
- Keep proportional line-height. Test current copy and longer copy for wrapping; fluid sizing does not automatically fit arbitrary text to one line.
- Do not scale the entire page with transforms or enlarge every value indiscriminately.

Illustrative restrained viewport scaling, beginning around 800px and capped around 1920px:

```css
/* Main heading: 32px to 38px */
font-size: clamp(32px, calc(27.7143px + 0.535714vw), 38px);

/* Regular text: 16px to 18px */
font-size: clamp(16px, calc(14.5714px + 0.178571vw), 18px);
```

These are examples from the Barn setup typography request, not universal site defaults. Inspect current classes and breakpoint overrides before reusing them.

## Apply and verify authorized corrections

1. Update the smallest relevant native class or scoped combo class at the authorized breakpoint. Do not replace the page or its working behavior.
2. Read back saved properties and compare with the baseline, including inherited and more-specific rules.
3. Check whether published custom styles override the native changes. Report inaccessible sources explicitly; do not guess unsupported tool actions or claim the discrepancy is resolved.
4. Verify the complete affected layout at each requested width, including intermediate widths. Suggested widths when the owner has not specified others: 375/393px for phone, 1024/1440/1920px for desktop.
5. Check heading wrapping, consistent gutters, complete tab labels, usable controls, local scroll access, and no document-wide horizontal overflow after fonts and media settle.
6. Inspect shared-class effects on other in-scope pages. Do not silently expand correction scope to additional pages.
7. Restore temporary browser viewport overrides. Do not submit forms, send messages, or modify production data during styling verification.

Saved styles prove the write was retained. They do not prove the final screen is correct. Designer snapshots that omit backgrounds or use fallback fonts are limited evidence; disclose that limitation. Published-page inspection cannot verify unpublished changes. Report exactly what was saved, visually verified, and remains pending.

## Findings format

| Page/section | Width | Visible issue | Element/class and evidenced cause | Minimal correction | Status/evidence |
| --- | --- | --- | --- | --- | --- |
| Example | 393px | Footer extends sideways | Measured link total exceeds nowrap container width | Allow the footer navigation to wrap within its parent | Proposed; not applied |

## Example prompt

> Use `docs/webflow-css-audit.md` as the workflow reference. Load the applicable available Webflow Designer Tools, Frontend Testing and Debugging, Systematic Debugging, Verification Before Completion, and Computer Use skills, plus applicable project instructions. Confirm the page currently open in Webflow Designer and audit its CSS styling one section at a time at 393, 1024, 1440, and 1920px. Focus on typography, spacing, alignment, media proportions, tabs, footer controls, and horizontal overflow. Inspect the actual native classes and rendered rules before identifying causes. Apply the smallest native styling corrections needed for consistency, preserving content, assets, order, interactions, and existing mobile work. Keep typography growth restrained at 1920px with explicit caps. Check shared-class effects before editing. Do not publish. Verify saved properties and rendered results separately, document findings and changes, and state any verification that remains pending.

For an audit without edits, replace the correction sentence with: **“Audit only; document evidenced causes and proposed corrections without changing styles.”**
