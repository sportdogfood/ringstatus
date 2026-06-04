---
name: rs-css-audit
description: Audit and minimally fix existing frontend CSS for RingStatus-style pages without redesigning. Use when Codex needs to inspect CSS, layout, responsive behavior, Webflow/Astro/frontend styling, selector conflicts, missing or bad rules, or visual regressions while preserving the existing template shell, approved class system, typography classes, and layout contract.
---

# RS CSS Audit

## Overview

Use this skill to diagnose and fix CSS defects in an existing frontend surface without changing the design direction. Preserve the current template shell, class architecture, typography utilities, section/container contract, and visual intent unless the user explicitly asks for redesign.

## Required Workflow

1. Identify the exact surface.
   - Locate the rendered route, embed, component, stylesheet, and source templates involved.
   - Separate Webflow shell HTML/CSS, frontend JavaScript, Astro/Webflow Cloud routes, CDN embeds, and local build artifacts when applicable.
   - Treat approved template shells and existing section/container classes as source-of-truth contracts.

2. Reproduce or inspect the visual symptom before editing.
   - Use browser/dev-server inspection, screenshots, computed styles, DOM search, stylesheet search, or static source review as appropriate.
   - Prefer selector-level evidence over broad guesses.
   - Record the viewport, route/file, element, and visible symptom.

3. Trace the selector-level cause.
   - Find the exact selector(s), cascade order, specificity, media query, inherited value, missing rule, stale rule, or runtime class mismatch that causes the symptom.
   - Check whether a rule is absent, overridden, too broad, scoped to the wrong wrapper, duplicated, or no longer matching the DOM.
   - Inspect neighboring approved rules before introducing new selectors.

4. Report before applying changes when feasible.
   - List failing selector(s).
   - List bad, missing, or overridden rules.
   - Describe the visual symptom.
   - State the minimal fix and why it preserves the existing contract.
   - If the user requested direct implementation and the fix is low-risk, include this analysis in the working update or final summary rather than stopping for approval.

5. Apply the smallest CSS change that fixes the proven cause.
   - Edit existing selectors when that preserves intent.
   - Add narrowly scoped rules only when no approved selector already owns the behavior.
   - Keep class names, typography classes, wrapper structure, and section layout contracts intact.
   - Avoid unrelated cleanup, broad resets, new utility systems, new layout systems, and visual refreshes.

6. Verify the result.
   - Recheck the affected viewport(s) and at least one adjacent viewport or nearby component if the rule is shared.
   - Confirm there are no overlaps, clipped text, wrapping regressions, layout shifts, or class-contract breaks.
   - Run existing lint/build/tests when available and proportional to the change.

## Guardrails

- Do not redesign.
- Do not replace the template shell.
- Do not create a new class system, spacing system, grid system, typography scale, or component framework.
- Do not rename approved classes unless the user explicitly asks.
- Do not add decorative styling, palette changes, hero treatments, card systems, gradients, or visual refreshes as part of a CSS bug fix.
- Do not infer that a screenshot problem requires markup changes until selector, cascade, and responsive rules have been inspected.
- Do not modify generated or deployed artifacts when the source stylesheet/template is available, unless that artifact is the intentional deliverable.

## Audit Notes Format

Use this compact format in updates or final summaries when CSS defects are found:

```text
Selector: .existing-selector
Symptom: Text wraps over adjacent controls at <= 480px.
Cause: min-width on the child exceeds the parent grid track; media query does not override it.
Fix: Add a scoped max-width/min-width override inside the existing mobile breakpoint.
Contract: Preserves existing wrapper, typography class, and component layout.
```

When multiple selectors fail, group them by symptom and fix surface. Keep the report focused on actionable CSS causes, not broad design commentary.

## Minimal Fix Heuristics

- Prefer narrowing an overbroad selector over adding a stronger competing selector.
- Prefer restoring an existing variable/class contract over hard-coding new one-off values.
- Prefer local media-query corrections over global breakpoint changes.
- Prefer fixing the rule that owns layout behavior over compensating in markup.
- Prefer deleting obsolete conflicting rules only when proven unused or superseded by the approved contract.
- Preserve typography utility usage; adjust container constraints, wrapping, or spacing around it when the visual failure is layout-related.

## Evidence To Capture

Before and after editing, capture enough evidence for the user to trust the fix:

- File path and selector.
- Bad or missing property.
- Viewport or state that fails.
- Screenshot, computed-style output, or source line reference when practical.
- Verification command, browser check, or screenshot after the fix.
