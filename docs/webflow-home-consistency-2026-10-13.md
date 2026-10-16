# Home consistency correction — 13 October 2026

Target: Home `/`, page `6982268d7543ac3c801512cd`, site `6982268b7543ac3c80151266`. User authorized Home-first correction and reuse of the existing section system. No publish or runtime work.

## Saved and read back

- Existing Home head contract: shared control ceiling corrected from 16px to agreed 18px; XL heading variable no longer aliases base heading sizing; current tab selector included in existing common control corner rule.
- Native Kicker2, message user/bot, page links and tab links: same 14–18px size expression and minimum-height expression. Bot family aligned with other controls.
- Four Home split copy wrappers changed from `fs1-copy` to existing `fsl-vw-copy`, reusing its fluid padding and copy gap. Exact IDs and prior assignments retained in before snapshot.

Native style definitions are global and may affect existing consumers of these same classes. No other page element trees were edited. Home's head change is page-local. Shared changes restore the already agreed ceiling rather than introducing new visual values.

## Evidence and limits

Before snapshot: `webflow-home-consistency-2026-10-13-before.json` includes native tree, head and seven-breakpoint reads of relevant base rules. Successful supported full metadata/base-property read replaced earlier failed export method. Effective desktop before measurement showed common six-section method/copy gaps, equal light/dark heading size and matching paragraph sizes within base/XL roles. It also showed split copies lacked standard padding and current tabs used different corners.

Saved head readback exactly matches the intended corrected content. All four copy assignments read back as `fsl-vw-copy`. Native control after-read checks cover seven breakpoint definitions. This is saved-rule proof, not final rendered acceptance.

Earlier Edge disconnection blocked verification. Owner reconnected Bridge/Edge and verification resumed against Home. The previous blocked state is superseded by the results below. No component conversion, new lock or publish performed.

## Next action

Reuse this verified Home styling baseline when separately authorized to work on downstream pages. Preserve original behavior, content, hidden folders and unpublished status. Component conversion remains a separate structural step; none was created in this correction.

Tool/schema mistakes in this run: unsupported query/flag names were rejected before writes; corrected native read route used get_variant_styles base plus get_styles include_properties. Browser recovery consumed time but did not establish acceptance. Exact per-page historical elapsed allocation remains unknown; this new Home correction did not resume the stopped member-page retrofit.

## Final verified styling evidence

- Seven actual Designer widths: 393, 667, 820, 1279, 1280, 1440, 1920. Root client/scroll widths match. Shared primary heading sizes match between light/dark sections, with .85 line height. All standard copy wrappers share computed padding. Control fonts scale 14–18px with matching corner pattern.
- The 320px/479px failures were traced to the large footer logo's fixed-size status combo; corrected the base/logo-question-mark and status-combo scaling. Status weight is 600 rather than 700. Native 320px and479px retests both have matching root client/scroll widths. Small nav wrapping and removal of its320px minimum prevent cramped-row overflow.
- Native Current tab corner override corrected in Designer at base; the MCP pseudo enum does not expose Current. Current and ordinary tabs now share18/18/0/18 corners. Color/state differences retained.
- Member/contact/submit controls, footer kicker and legal links also use14–18px rather than a separate16px ceiling. White-only mobile10px paragraph top margin removed. l5-p and hero copy-grid/left gaps now use the existing16–24px fluid copy token.
- Preview initially loaded stale head code. After reloading, DOM inspection confirms saved18px control ceiling and separateXL heading clamp. Refreshed custom-code-enabled Preview passes the same seven actual widths with no horizontal page overflow; all ten checked control/text families have a single computed size per width, common box families have matching corners, and light/dark paragraph margins are0. `webflow-home-consistency-2026-10-13-preview.json` contains these final results and supersedes the earlier Preview measurements.
- First tab group's Prep interaction becomes Current with the same font/corners. This limited interaction proof does not assert a full runtime/accessibility audit of every form, hidden state or second tab group.
- Six main sections inspected visually at mobile; final mobile/desktop screenshots: `C:/Users/gombc/AppData/Local/Temp/rs-home-consistent-mobile-20261013.png` and `C:/Users/gombc/AppData/Local/Temp/rs-home-consistent-desktop-20261013.png`. Measurements precede visuals. Designer and Preview proof is not published/iPhone-device acceptance.

Global native style changes can propagate to pages using those classes; downstream page trees were not edited or separately accepted. The existing Home head block was corrected rather than adding another stylesheet or replacement runtime. No component lock enforcement is claimed.
