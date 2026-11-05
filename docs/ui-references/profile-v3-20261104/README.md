# Profile V3 styling reference

Saved 4 November 2026 for reference only, at the owner's request. These files are not connected to the Schedule or deployed.

Source: https://ringstatus-profile-v3.sportdog.chatgpt.site/?tab=all
Components: https://ringstatus-profile-v3.sportdog.chatgpt.site/component-library/?theme=dark&size=desktop
Site ID: appgprj_6aeb21e6769c81919858de00d635b966. Sites listed version14 at capture.

Captured published assets: styles.css, primitives.css, fonts.css and four Outfit fonts. Original URLs and capture details are in asset-manifest.json. The CSS files are original reference copies; relative font URLs have not been rewritten.

## Component catalog observed

Stack: Action row, Back row, Brand row, Footer, Profile row, Search-sort, Section, Tab row.
Controls/content: Buttons, Create dropdown, Launcher widget, Outputs, Profile editor, Simple list, Utility controls.

The component page has working demonstrations. Preserve its source styling for comparison; importing or applying it needs a separate implementation decision.

## Render reference

Viewed the site's own desktop dark/light and compact dark/light preview controls. The rendered panel measured approximately440px wide in desktop preview and392px in compact preview. Team/Shows action buttons were36px high with13px text; the close control was34px high. These are observed reference values, not approved sizing for Schedule.

Owner clarified that the mismatch concerns fonts. The 440px forced-preview width does not establish the intended desktop font rendering. Use the published Outfit typography as the reference and preserve the established Schedule shell and mobile sizing. Shell controls and behavior are shared brand components.

Reference variants:
- /?theme=dark&size=desktop
- /?theme=light&size=desktop
- /?theme=dark&size=compact
- /?theme=light&size=compact

The four previews were visually inspected. Screenshots were displayed during inspection but are not included in this saved bundle.
