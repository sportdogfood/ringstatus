# Webflow shared structure and style decisions

Date: 11 October 2026.
Purpose: retain the owner's agreed direction and inspected findings so a later implementation plan can use this record. This is documentation, not a completed implementation plan or authorization to publish.

## Goals

1. Create consistent, complete sections using a small set of shared structures.
2. Render those sections correctly across viewports, with iPhone/mobile the priority and restrained growth at large desktop widths, including 1920px.
3. Keep exceptions close to defaults. Use combo classes only for necessary specialization, with no more than three classes total per element.
4. Review each page's structure and class chains first; then apply shared style corrections that affect every page using those defaults. Verify each affected viewport rather than assuming a saved style proves correct rendering.

## Public pages

Scope: Home `/`, Tools `/ring-status`, About `/about-ringstatus`, Lainey `/lainey`.

```text
ls-section-content
└─ fsl-method
   └─ fsl-vw-panel
      ├─ fsl-vw-copy
      │  ├─ l5-kicker             optional
      │  ├─ l5-hp
      │  │  ├─ heading
      │  │  └─ paragraph
      │  └─ rs10-tips             optional
      └─ fsl-vw-tabs              optional
```

- Tools should match Home's standard copy/tab structure and spacing. Separate tab panels, nested copy panels, and wrapper differences are unintended, not reasons for exception combos.
- About deliberately hides or omits kickers. Preserve that choice.
- Home's hidden `l5-is-hold` folder is slated for complete removal during implementation.
- The exact shared copy class is `fsl-vw-copy`. Home also uses `fs1-copy` in split-media sections; preserve functional media layouts while mapping shared copy styles.
- Lainey's introduction, horses, videos, and dashboard copy should conform to the same standard, preserving CMS bindings, actions, content, and media behavior.
- Portfolio is a second reusable media-and-copy skeleton: manage its layout separately, reuse shared typography, spacing, and sizing variables. Its precise final structure remains to be planned.
- Schedule grids and horse/video card internals retain their functional layouts. Nav and hero keep their own structures while joining the shared styling system where appropriate.

## Variables and Airtable

Base: `appara5767XpIaJIf`.
Table: `rs-webflow-variables`, ID `tblakTuwVmMmxTYUo`.
Link: https://airtable.com/appara5767XpIaJIf/tblakTuwVmMmxTYUo

Agreed table format: one record per variable, not six repeated records per viewport. Separate `Variable`, `Class`, and `Property` fields. For each mode (Base, L, XL, XXL, Tablet, Mobile L), retain separate mode/value, calc, min, and max fields. Use short variable names and exact expressions such as calc/clamp. Blank overrides can indicate inheritance only after the values have been inspected and their meaning established.

Current table status: 70 rows. Original 10 contain illustrative values; the 60 added rows have unfilled mode values. They are a proposed inventory, not verified Webflow variables or class bindings. Blank new values mean uninspected, not confirmed inheritance. Existing example class/property assignments must also be checked: wrappers can differ from the actual text-bearing classes.

For `rs-webflow-inventory`, recommended simple page fields are Page name, Page ID, Path, Status, Review notes, Corrections needed, Last reviewed. Removing workflow/ownership fields and the detailed-inventory relationship was discussed, not performed.

## Recognize and form pages

Recognize page ID: `6ac459b135ad0c4c3b253d97`.
Agreed direction: keep the existing `rs-recognize-*` classes and bind typography, spacing, padding, and width to shared variables. Preserve its forms, identity behavior, states, and interaction structure rather than forcing it into the public copy-section skeleton. Inspect existing overrides and bindings before changes.

Add shared control styles for:

- Buttons: family, size, weight, line height, horizontal/vertical padding, minimum height, radius.
- Inputs/selects: family, size, line height, padding, minimum height, radius.
- Forms: field gaps, label/input gaps, container width.

Discussed mobile targets: input text at least 16px and controls at least 44px high. Scale gently across wider viewports. Keep colors and interaction states separately controlled. These are planning targets, not a statement of verified accessibility or device acceptance.

## Barn setup inspection

Page ID: `6ac7b2de07b992a509c90029`.
Native element tree and selected base styles inspected 11 October 2026. Existing `rs-barn-v24-*` classes can remain and consume shared variables.

| Class | Observed base settings |
| --- | --- |
| rs-barn-v24-input | Outfit; font-size clamp(16px, calc(14.5714px + 0.178571vw), 18px); fixed height and min-height 44px; padding 10px vertical / 12px horizontal; line-height 1.35; weight 400; radius 8px; width 100% |
| rs-barn-v24-primary / secondary | Outfit; font-size clamp(15px, calc(13.5714px + 0.178571vw), 17px); min-height 44px; padding 10px vertical / 12px horizontal; line-height 1.25; weight 600; radius 8px |
| rs-barn-v24-label | Same 16–18px font expression as inputs; line-height 1.35; weight 500 |
| rs-barn-v24-h1 | clamp(32px, calc(27.7143px + 0.535714vw), 38px); line-height 1.12; weight 700; tracking -1.05px |
| rs-barn-v24-form | Flex column; row gap 16px; top padding 18px |
| rs-barn-v24-field | Flex column; row gap 6px; min-width 0 |
| rs-barn-v24-main | Absolute positioning; top 125px; width 100%; fixed max-width 672px; side padding 16px; top padding 28px; bottom padding 64px; vertical scrolling |

Finding: text already scales; control padding and main width remain fixed. Shared responsive variables should cover those values. Fixed input height needs reconsideration when text/padding grow so content remains comfortably contained. Preserve existing scrolling, sheet forms, hidden states, and behavior; do not infer that apparently repeated/default form children can safely be removed.

## Planning and verification still required

- Lock exact shared skeletons, variable names, baseline values, mode/breakpoint mapping, and permitted combo exceptions.
- Inspect current native rules, custom CSS precedence, variable definitions, component internals, CMS bindings, and behavior dependencies before changing classes or nesting.
- Map old wrappers/classes to the shared system without replacing working implementations.
- Verify content wrapping, longer copy, consistent padding, local tab scrolling, control sizing, form/sheet scrolling, and no document-wide horizontal spill at affected widths.
- Prioritize 393px and narrower iPhone layouts; test larger widths with restrained caps. Actual iPhone Edge verification is separate from desktop browser or Designer evidence.
- Read back saved properties and separately verify rendered results. Publish only when explicitly requested.

Related records: `docs/webflow-three-page-consistency-2026-10-11.md` (includes Lainey), `docs/webflow-responsive-audit-2026-10-09.md`, and `docs/webflow-css-audit.md`.

Status at this handoff: discussion and comparison documented; recent comparison/control inspection made no Webflow changes. Historical styling changes recorded in earlier audit documents are not proof of current published rendering. No normalization or shared-variable implementation is claimed complete.
