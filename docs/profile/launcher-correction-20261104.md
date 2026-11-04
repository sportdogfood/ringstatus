# Profile responsive and launcher correction

Approved scope from owner screenshots and follow-ups: mobile full bleed; Profile launcher toggles entire panel, stays sticky left; more space at launcher ends; no large empty background when closed. Preserve Recognize and current desktop settings. Baseline 528f30abb.

Reference verified by wef-schedule-app: v2.css mobile override at <=600px, full width/no outer border/radius. Desktop remains 820px, 14px controls, 40px minimum height; no whole-app zoom. Profile remains right aligned. Only app.js launcher toggle/expanded attribute and shared styles.css changed. No backend, recognition, dependencies or Airtable changes.

Checks: existing 8 Profile v3 tests PASS; build PASS; rendered 393px mobile panel spans content width with zero radius and zero outer padding; closed panel and background hidden/transparent; Profile toggles closed/open from Team; sticky Profile icon and 12px launcher edge padding; 1440px desktop shell 820px, controls14px/40px, ten controls within bounds. See launcher-correction-layout.json.

Horse correction 528f30abb deployed successfully earlier: nine exact Airtable horse IDs loaded200; cached nine rendered while horse network reply held; browser performance records show next Team refresh starts 480011.8ms after initial horse load finished, then horse refresh completes200. Network debugger missed periodic events; browser resource timing supplies timing/status proof. No production record edits were made to force changed data. Final screen may be a horse detail, so latest list-presence boolean is not used as failure proof. See horse-correction-live.json.

Rollback: targeted revert of this responsive/launcher commit only, preserving horse and other owner changes.
