# Onboarding draft consistency audit — 8 October 2026

## Revision131 continuation — latest result

Owner revision131 superseded the deadline stop for this same audit until20:25Z, report reserve20:20Z. Instructions read19:59:25Z; full original acceptance and historical failures retained. **Overall PARTIAL; dependent preview navigation BLOCKED.** No new assignment, fixture, implementation or acceptance reduction.

Feed6ac459b4ad3971b205a5ee11 was confirmed by current URL/title/draft. Native screenshot capture succeeded for settled white/dark mobile baselines: All selected,14of14,Filters collapsed. Temporary outer1440x925; native raster app crop x523,y72,width393,height852. [White capture](C:/Users/gombc/OneDrive%20-%20Sport%20Dog%20Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/feed-r131-white-native.png), [dark capture](C:/Users/gombc/OneDrive%20-%20Sport%20Dog%20Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/feed-r131-dark-native.png). Shared content visually agrees in heading, gutters, pills, first-card position, row borders and dark tokens with preserved source references. Full fidelity remains PARTIAL: source rasters are392x851 after fractional rounding, include phone chrome/navigation/focused theme treatment, while target crops393x852; fresh computed measurements timed out. No pixel-exact or full viewport-matrix PASS. The missing source navigation and phone chrome are excluded scaffolding, not asserted page defects.

|Feed safe action|Actual observed result|Gate|
|---|---|---|
|Help / Updates / All|1of14 Ring request help /11of14 /14of14, selected state changes|Scoped PASS|
|Search rs-feed-query, zz-audit-no-match|0of14,No matching messages,disabled Mark shown as read|Scoped PASS|
|Show all messages / Clear filters|14of14;All rings/All riders restored|Scoped PASS|
|Ring rs-feed-ring = Derby|4of14,Derby records|Scoped PASS|
|Rider rs-feed-rider = Jessica|2of14,Jessica records|Scoped PASS|
|Unread only|Value0→1→0;all14 samples unread|Toggle PASS;read-message exclusion NOT TESTED|
|Desktop view|Mobile view label and wide rendered content|Switch PASS;responsive fidelity PARTIAL|
|Mark shown as read / persistence / real submits|Not invoked|NOT TESTED;write boundary|

Readbacks are feed-r131-*-ax.txt; mobile captures/crops and desktop dark capture are feed-r131-*.png. Earlier SMS replies2of14 and Tab focus evidence remains valid and retained. Current source integrity check again matched all140 tracked hashes: source-hashes-r131-check.json. No source edits.

The restored Feed measurement channel failed Runtime.evaluate after3000ms; successful native screenshot method was used, without a CDP capture loop. Native page selector visibly offered the exact Barn rebuild, but selection yielded a blank transitional canvas and then a screenshot titled master with “Compiling custom code”. This screenshot is an access incident, **not Barn rendering evidence**. Direct exact Barn page-ID URL navigation then timed out30s/kernel reset; the single supported tab-rebind recovery also timed out30s/kernel reset. No further retry for this incident. Temporary viewport.reset succeeded independently during cleanup. Browser navigation stopped on the actual blocker, before the deadline.

Barn6ac7b2de07b992a509c90029 checkpoint continuation.saved is settled: linked sheets, page-owned selected/theme interactions, h1/h2#111, tracking0 and12px/400 muted view switch are saved, with rendered acceptance explicitly NOT PASSED. Historical F1–F5 are correction/retest history; do not instruct the owner to reapply those changes blindly. F8 padding6px is the later source cascade and is not a confirmed defect. SMS6ac459b4506821bd349ed047 and Schedule6ac7d2c7cf113c577ef2a6d7 were not navigated after the blocker. Their prior source/native evidence stays valid; remaining rendered/theme/width/control cells stay unchecked. Recognize activation-policy gates remain unchanged.

Confirmed saved-structure discrepancy suitable for a minimal manual correction: Schedule6ac7d2c7cf113c577ef2a6d7, rsv24s-close Link b2745984-7fa8-639a-1a90-0cfdfa8972a4 (and equivalent drawer/flyup close Links), observed attrs={} and visible×, expected source accessible name “Close details”. In the Designer, select that close element and add custom attribute aria-label=Close details; repeat for equivalent fixture close controls. This corrects the saved label only and does not establish button semantics, closure or focus return. No correction applied by this audit. Other proposed sizing/state changes require current render before editing.

Exact owner-assisted next step within the authorized window: manually restore the authenticated preview to Barn page6ac7b2de07b992a509c90029 and verify the top title says “RS Barn Onboarding — Barn setup v24 rebuild”,Draft,with content visible. A concrete restored state permits a new incident assessment; current failure is not an authentication error and no OAuth/reinstallation is called for. Then continue Barn→SMS→Schedule, retaining all eight targets and14width/two-theme matrix. No automatic extension beyond20:25Z. [Continuation evidence](C:/Users/gombc/OneDrive%20-%20Sport%20Dog%20Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/r131-evidence.json).

## Owner-refreshed Feed — actual access restored19:39–19:41Z

Owner confirmed “feed draft - is ready” after manual refresh: a concrete access change. Existing authenticated Edge tab rebound successfully without navigation/reload; exact Feed page6ac459b4ad3971b205a5ee11,title RS Barn Onboarding — Feed,draft confirmed in actual preview. This supersedes the blanket authenticated-access blocker for this refreshed surface.

Current first read:canvas1279x724,mobile app392.998x851.994 atx443.012,y−120.971—clipped. Temporary outer browser1440x925 produced canvas1279x852 and contained mobile app392.998x851.994 atx443.012,y0. Viewport override subsequently reset. [Current computed render](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/feed-refreshed-current.json), [matched geometry/read log](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/feed-refresh-read-success.json).

Matched CDP screenshot failed **Timed out after5000ms waiting for CDP command Page.captureScreenshot.** One supported native screenshot recovery succeeded. Initial native recovery screenshot lagged the DOM/control state and is not final selected-state proof. Final native screenshot19:41:30 visibly matches SMS replies selected,2of14messages,expanded filters and Updates keyboard focus. [Final actual screenshot](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/feed-refresh-final-native.png), [safe actions](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/feed-refresh-safe-actions.json), [keyboard readback](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/feed-refresh-keyboard.json). No data submission,mark-read persistence or page mutation.

**Scoped interaction PASS:**dark theme changed todata-theme=dark;heading computedrgb(232,236,245);Filters aria-expanded=true/displayblock;SMS replies aria-pressed=true andcount2of14;Tab moved toUpdates anchor withtabindex0,:focus-visible=true andrgb(143,184,255)solid1.766px outline. These are current actual UI checks,not all-control or business integration proof.

Typography observed matches shared roles:page heading32px/700/35.84px,−1.05px;theme13px/17.55px;view12px/16.2px;pills13px/500/17.55px;allnormal tracking. White heading#111 anddark#e8ecf5 agree with savedsource. Actual font familyname RS Recognize Outfit versus source Outfit remains separate from proving identical rendered font files.

**Feed393x852 geometry checked;full paired visual fidelity remains PARTIAL.** Stable app-only paired screenshots at equal content dimensions were not completed—the matched capture timedout and final native screenshot followed override reset. No white/dark layout-fidelity PASS issued. Remaining Feed width/theme/state cells,unrun safe controls and all other pages’current rendered matrix remain NOT TESTED within the unchanged19:42Z deadline. Prior blanket “all target actions untested/access blocked” statements below are historical and superseded for these scoped Feed checks. Stop browser actions at19:41:30Z;preserve remaining work,not a narrower completion claim.


## Settled Schedule recheck —19:35–19:39Z

Same audit reopened only for new executable settled evidence. Read worker checkpoint and fixture map after construction stopped19:32:27Z/readback19:32:41Z. Deadline remains19:42Z;no source/page/style edit,no browser retry,new task or extension. Independent production headless tree read19:36:17.755Z and style read19:36:34.645Z supersede the earlier provisional336-node Schedule snapshot.

**Native inventory PASS in this narrow scope:**3314 nodes;71 page-owned styles;30 application fixture frames;all28 mapped additional frame/main/header/schedule/toolbar/tabs IDs present under expected parents,plus both white/dark Rings/All bases. Root Tabs eb8f5f50-f3de-c95f-6b92-ce2f58404fb0 labels are exactly “White · Rings”,“Dark · Rings”,“Visual states”;nested Tabs eaf9d183-30f7-a917-967f-3a0d923d47e1 labels “Entries fixtures”,“Filters / Rollups”,“Details fixtures”. This verifies native inventory,not actual tab switching.

|Fixture family|White|Dark|Independent saved-structure check|Rendered/action gate|
|---|---:|---:|---|---|
|Rings/All bases|1|1|Both frames present|BLOCKED|
|Entries all/ring1/ring2|3|3|6 mapped frames and structure present|BLOCKED|
|Rings ring1/ring2/rollups-only|3|3|6 mapped frames and structure present|BLOCKED|
|Class drawers780/781/825/826|4|4|8 surfaces include expected classID and full class name|BLOCKED|
|Entry flyups Bee/Insider/Navigator/Fort Knox|4|4|8 surfaces include expected classID/name and selected entry name|BLOCKED|
|Total|15|15|30 native fixture frames|No visual PASS|

[Independent inventory and structure checks](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/schedule-settled-verification.json), [full compact native tree](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/schedule-settled-native-flat.json), [style values](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/schedule-settled-styles.json), [mapped state inventory](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/schedule-settled-fixture-map.json), [source content comparison](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/schedule-settled-content-checks.json).

Control tags:130 DOM nodes comprise129 button-tagged nodes and1div. **128 generated class/entry rows have DOM settings tag=button**,with complete rsv24s-row-native/-dark styles. Native tree also contains348 Link nodes and6 TabsLink nodes. Returned Link settings do not establish their actual rendered HTML button tag. Close controls remain Link-native nodes containing× with empty attributes;source close action is labelled “Close details”. White/Dark mode Links also have empty attributes—no saved aria-pressed value in this read. These are static visual fixtures and must not be reported as working theme/filter/close controls. Original white controls and rollups glyph labels remain partial,as retained in builder checkpoint.

|Schedule family|Source expectation|Final native observation|Impact / proposed narrow correction|
|---|---|---|---|
|Class/entry row button reset|Transparent,border0,inherit font;16px name/600/1.35;ellipsis|128 button-tag rows;row-native styles border0/transparent/inherit;name-text16px/600/1.35,nowrap/ellipsis,margin0|Native declaration agreement;rendered clipping/action/focus still blocked|
|Page/record heading|Page32px/700/1.12/−1.05px;record15px/500/1.35/−.25px|heading and surface-head/ringtitle preserve distinct roles|Intentional hierarchy;not compare record15px to section25px|
|Dark tokens|bg#090b11,panel#10141f,text#e8ecf5,muted#9aa3b4,blue#8fb8ff,green#49d17d,accent#e9a08a,borderalpha0.14|Own dark definitions store these values|Native token agreement;selected/hover/focus/disabled/rendering unverified|
|Mobile logo/theme controls|Source mobile logo108px;mobile modes no65px minimum;desktop134px/65px|logo-dark/base134px;mode65px base;complete responsive overrides not established|Reconcile mobile-specific declarations after actual rendered boundary tests;no blanket redesign|
|Mobile drawer versus desktop drawer|Source mobile top64,left28,rounded left corners;desktop top0/right0/radius0|drawer/-dark base top0/right0,widthmin(560px,100%−32px),no mobile shape in returned base|Potential mobile source difference;need actual cascade/render,then narrow page-owned variant correction|
|Flyup size|Source RecordSurface flyup min-height0,content-driven without primary action;captured Bee mobile flyup is compact|flyup/-dark height80vh,widthmin(560px,100%−32px)|Declared sizing differs from content-driven source;render dimensions required before correction|
|Paragraph normalization|Source compact token text does not carry default paragraph spacing|838 Paragraph nodes;0 assigned rsv24s-flat,although that style exists;some roles independently setmargin0|Builder's550 normalization successes do not prove assigned combo or final margins;inspect actual Paragraph spacing,correct only affected roles|
|Close/focus/selected semantics|Close details accessible name,working closure/focus return,selected/expanded state|× Link with empty attributes;White/Dark Link attributes empty;controls mainly fixtures|Native fixture inventory is not action proof;finish only separately authorized functional semantics and verify keyboard|

Fixed bottom-right fixture navigator is explicit audit scaffolding outside application fixtures,not accepted source UI. It must be excluded from source-fidelity comparison while retained for fixture selection. No rendered view was acquired;all14 widths/both themes and explicit393x852 remain BLOCKED. No exhausted same-incident browser recovery repeated.

Barn remains PROVISIONAL while its worker is active;no latest settled rendered acceptance inferred. Recognize is now reported idle by coordinator with disabled bindings and7saved events. Read activation-prerequisites.json confirms owner policy/authentication/mutation-audit/recovery/deployment and current visual/playback gates remain prerequisites;continuation-manifest reports deployed=false/records_accessed=false. Saved events are not playback proof;no activation or policy change is authorized or performed by this audit.

The full audit remains PARTIAL. New native Schedule fixture verification is executable work completed,not a substitute for remaining rendered/interaction gates.


## Authorized continuation — current evidence supersedes initial observations

Owner continuation read from [continuation-20261008.md](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/continuation-20261008.md>): same audit, no new page/task, audit exclusively owns browser navigation. Continuation first action19:12:43Z; deadline19:42:00Z; report/verification reserve19:32Z. Original elapsed/corrections retained. Publishing, data submissions and all source/page/style mutations remain prohibited for this audit. Other builders are authorized to change their exact Barn/Schedule rebuilds, so those current reads are provisional until their checkpoints settle.

**Current outcome remains PARTIAL.** Mobile source containment is resolved through existing controls; authenticated target preview failed through the actual Edge/Designer surfaces. The old IAB login and competing-session hold are historical, not the current access diagnosis.

### What advanced

- Source mobile mode inside outer1440x1000 yields full app392.998x851.994 atx523.586,y71.992,fully inside the viewport. Captured actual app-region white/dark references for Feed,SMS,Recognize,Barn,Schedule; native source files remain unchanged. [Source mobile measurement matrix](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-settled-mobile-matrix.json). Crops use the app rectangle, excluding the outer device-selector/stage; composited phone corner/status-bar treatment remains visible and is not a proved chrome-free matched fidelity pass.
- Feed expanded filters and pointer-focused search baseline captured; source focused input computed border rgb(49,94,158),16px/21.6px Outfit,outline none0px. [Focus readback](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-feed-focus-white.json). This actual source observation takes precedence over assuming its declared CSS outline is visible.
- Source Schedule dark class drawer and Bee entry flyup captured without save/submit. Escape closure/focus-return was sampled immediately during transition and was not established; subsequent source close navigation worked. [Drawer](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-schedule-drawer-dark.png), [flyup](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-schedule-flyup-dark.png). These are reference-only states, not target passes.
- Source desktop clipping diagnosed at1440x1000: phone-device fixedleft−43px/top−42px,zoom1,transformnone. Mobile/desktop view controls and existing device picker were inspected (iPhone/Pixel10 only). Desktop source matrix remains blocked without source repair; no repair performed.
- Native Feed/SMS/Recognize trees refreshed19:17:10.576/17.273/23.481Z:541/350/159 nodes. Current control inventory includes DOM-tagged controls omitted from type-only counts:15/67/31 control-related nodes respectively. [Current inventory](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/continuation-control-inventory.json).
- Provisional Barn read19:20:28.047Z:390 nodes,35 used styles; Schedule19:20:35.163Z:336 nodes,46 used styles. Barn74 control-related nodes; Schedule15,including native fixture TabsLinks and Link controls. **The historical claim “Schedule has0 control nodes” is no longer current.** Source-equivalent behavior remains NOT TESTED.
- Native styles read around19:19Z show Barn h1/h2 now explicitly#111111,mode/link tracking0px,and new dedicated view-switch12px/400/#646873/4px0 padding assigned to the header link. Earlier F2/F3/F4 failures are **saved corrections / rendered retest pending**, not current confirmed failures. [Current styles](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/continuation-style-properties.json).
- Schedule now has dark state-frame,panel,row,mode,tab,pill,badge and fixture-navigation definitions. Historical absence of dark definitions is superseded. Native dark declarations alone do not establish the dark rendered gate.

### Actual authenticated access failure

At19:12Z the existing authenticated Edge Designer showed Barn v24 with readable native preview. Navigation to the exact Feed page was issued. Following that navigation,DOM Runtime.evaluate timedout3seconds;one supported native AX recovery timedout30seconds and reset the browser-tool kernel. Independent Designer get_current_page subsequently timedout with the documented Bridge-launch message. Binding the authenticated tab for that documented recovery then failed Page.enable10seconds (toolwall27.948s); no further same-incident retries were made. No OAuth,reinstall,beta,source manipulation or alternate production endpoint used. [Continuation status](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/continuation-status.json).

This is a browser/Designer capability failure after actual authenticated access, not an inference from an IAB login screen. Headless reads continue to work. No current target screenshot,theme toggle,tab/open/close action or keyboard-focus result was obtained in this continuation.

### Current gate matrix

|Gate|Current disposition|
|---|---|
|393x852 mobile saved-source containment / white-dark reference|PASS source-only geometry; paired source-target fidelity still NOT VERIFIED|
|375,479,480,481,767,768,899,900,991,992,1280,1440,1920 saved-source desktop fidelity|BLOCKED by offset desktop source; no source-file repair|
|All14 target widths × both themes ×8 identified drafts|BLOCKED by actual authenticated Edge/Designer failure; all current cells unverified|
|Explicit target393x852|BLOCKED; historical Barn393x684 observation is not current matched proof|
|Feed/SMS/Recognize target safe controls + keyboard|NOT TESTED; source-only controls are not target proof|
|Barn/Schedule current style/control inventory|PROVISIONAL while builders write; native changes captured, final rendered recheck still required|
|Barn headings/tracking/view-switch corrections|Saved native correction; current rendered retest pending|
|Barn selected-tab,icons,sheets,remaining states|Historical defects retained; current final status not established|
|Schedule dark/alternate/drawer/flyup/native fixtures|New dark/fixture definitions saved; complete inventory/behavior/rendering not accepted|
|Source immutability|140 tracked working hashes still match initial audit at19:18Z|
|Audit protection boundary|No external writes,form/data submissions,messages,publishing or deployment|

The initial report below remains historical evidence from18:43–18:54Z. Its old source-containment blocker,competing-session hold,expired-budget stop and pre-continuation Barn/Schedule findings are superseded where stated above; they are not silently reused as current acceptance.

## Initial audit record — historical18:43–18:54Z

Continuation checkpoint19:23Z stops on the actual authenticated browser/Designer capability blocker after independent source/native evidence collection. Historical active audit11m10s plus continuationabout10m20s;wall elapsedabout40m10s includes the owner-directed stopped interval. Deadline19:42Z was not exhausted. Builders had not supplied settled continuation checkpoints at this read; affected target state and final rendered recheck remain provisional. No partial native correction is promoted to full completion.



Result: PARTIAL — native inventory and style/code inspection recorded; complete rendered consistency and interaction acceptance remain BLOCKED. This audit is a one-off report, not acceptance of consistent design or completion of any build.

Read-only scope: site `6982268b7543ac3c80151266`. No application, source, page, style, script, component, variable, Airtable/schema, publishing or deployment corrections. No messages, monitoring or next task were created. Recognize worker `01a11cc4-39cd-7eb2-872f-89ef41193275` was not contacted, interrupted or navigated.

## Timing and access

Conservative UTC start **2026-10-08T18:42:50Z**, supplied native turn start; first clock read18:43:14Z. Deadline **19:27:50Z**; final10minute reserve begins no later than19:17:50Z. No build-budget reset. Read window18:43–18:53Z; rendered Barn observation18:48:30.084Z; cascade read18:50:14.514Z. This report stops on the rendering/interaction access blocker after available independent native reads, not on a time-limit success claim.

Production Webflow guide called once. Eight native trees, native style definitions/properties, and freeform code read successfully. One `get_styles` attempt used unsupported `style_names`/`query:specific`; it failed schema validation and was corrected once to the already-supported `query:all,include_properties:true`. No repeated aliases, auth recovery, OAuth, reinstall or beta. Initial browser inventory timed out30seconds; one same-API recovery succeeded. Both recoveries under5minutes.

The isolated in-app draft preview redirected to **Login - Webflow**. No login/OAuth was attempted. The existing Edge tab was already on Barn v24 at393px and was observed only: no click, resize, reload, navigation, focus test or Bridge call. It is not owned by this audit and cannot safely supply the requested cross-page matrix while the separate integration assignment is active. [Login screenshot](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/isolated-preview-login.png).

`get_page_scripts` returned a per-action **404 resource_not_found / Requested resource not found: Custom code block not found** for all eight pages, even though outer `isError` was false. This is recorded as unavailable registered-script evidence; it is not an authentication failure and is not proof that no script can execute. Freeform code reads succeeded.

## Exact page inventory

One `list_pages` read returned the newest100 of188 site pages. All seven supplied pages plus the additional DNU Component Drafts page appear there. Unrelated pages are excluded. This proves the eight identified drafts; the older88 pages were not enumerated because the assignment limits listing to once. An exhaustive assertion that no other older onboarding page exists is NOT VERIFIED.

|Role|Exact title|Page ID|Slug|Draft|Last updated UTC|
|---|---|---|---|---|---|
|Current Schedule replacement|RS Barn Onboarding — Schedule v24 rebuild|6ac7d2c7cf113c577ef2a6d7|rs-barn-onboarding-schedule-v24|true|2026-10-08T18:11:36.440Z|
|Current Barn replacement|RS Barn Onboarding — Barn setup v24 rebuild|6ac7b2de07b992a509c90029|rs-barn-onboarding-setup-v24|true|2026-10-08T16:03:12.902Z|
|Current draft|RS Barn Onboarding — Feed|6ac459b4ad3971b205a5ee11|rs-barn-onboarding-feed|true|2026-10-08T02:34:11.607Z|
|Current draft|RS Barn Onboarding — SMS alerts|6ac459b4506821bd349ed047|rs-barn-onboarding-alerts|true|2026-10-08T03:31:28.149Z|
|Legacy Schedule|RS Barn Onboarding — Schedule|6ac459b3a4b228d44a68c990|rs-barn-onboarding-schedule|true|2026-10-06T12:53:23.228Z|
|Legacy Barn — DNU|DNU RS Barn Onboarding — Barn setup|6ac459b3a4b228d44a68c935|rs-barn-onboarding-setup|true|2026-10-08T15:07:25.114Z|
|Current draft|RS Barn Onboarding — Recognize|6ac459b135ad0c4c3b253d97|rs-barn-onboarding-recognize|true|2026-10-07T22:51:45.971Z|
|Legacy component inventory — DNU|DNU RS Barn Onboarding — Component Drafts|6ac45308b54c4e1ed0223ef1|rs-barn-onboarding-component-drafts|true|2026-10-08T15:07:19.645Z|

[Metadata readback](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/inventory.json). All eight are draft=true; shouldPublish=true is not evidence of publishing.

## Source and changing baseline

Saved v24 source `C:/Users/gombc/AppData/Local/Temp/recognize-v24-01a11835`, HEAD `8881e0027bc3c1be3a982d7802d7fd44688cc0fe`, verified live locally. Working status contains pre-existing modified files; HEAD alone does not establish cleanliness. All140 tracked working-file SHA256 values matched between this audit’s start/end inventory. No source edits were made. [Start hashes](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-hashes-start.json), [end hashes](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-hashes-end.json), [working status](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-status-start.txt).

|Working source|SHA256|
|---|---|
|Prototype.tsx|519d55bbfdb5c29f75f42b4cd41c5ee4ff42fe13ecfc5bd9f8714c77dd7b72ef|
|prototype.css|3a5e22c27eed90b0965bc9f848f0bbaea9cc7b8e938a36917b5bead9c19b313f|
|design-system.css|9eb99b41e1fb436c797771162b36ac6de05ff4132862fc15a0c30966e77dc873|
|styles.css|844c67b27cf5e38ba395153145d058c929c6479199d245cd046f3646badd719d|

At outer browser393x852, mobile source application measured392.998x851.994 atx−5.636,y71.992; its bottom extends beyond the viewport. Desktop presentation measured393.378x852.097 atx−42.998,y−41.998. Both clip; neither is a valid paired rendered source. [Mobile Feed screenshot](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-feed-393x852.png), [desktop source screenshot](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-feed-desktop-393x852.png). No source repair was authorized or performed.

Recognize remains an accepted design reference with historical deviations, not an automatically perfect baseline. The prior verification summary records heading color#030303 versus source#111111, dark heading remaining#030303 versus source#e8ecf5, narrow logo108px source versus134px target at479, and target mobile heightabout901 versus source852. Historical interactions and screenshots were NOT reused as current passes: current target/source version equality is not established, and integration is active. [Historical summary](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/1911bfd3ae618e9cf6cfb4f276710a87ec3e4107f9b7361da3180a0317e924c8/recognize-verification-summary.json). Current interaction baseline is **PROVISIONAL**; [timestamped final freeform read](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/recognize-final-code.json).

Airtable token library `appZahVgD156cMAe3 / tblB0ZpdeOzpR9EqL` and ProApp `6a63f491030cc3634eb83d92` remain reference evidence only. No new token/schema audit was needed or performed; no recoloring authority inferred. Comparison uses the saved source’s role definitions and own colors.

## Native style comparison

Read374 relevant/used native style definitions, including dynamic dark/mobile/narrow/selected combo classes. [Complete native values](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/comparison-styles.json); [used styles/tree counts/actions](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/native-summary.json). Values below are declared native values unless explicitly marked computed. Variable references in Recognize remain unresolved IDs; they are not silently converted to colors. The returned property map covers base properties and combo selectors, not a proven complete breakpoint/pseudo-state cascade.

|Family / role|Saved-source expectation|Feed / SMS|Barn v24 / Schedule v24|Disposition|
|---|---|---|---|---|
|Body|Outfit,sans-serif;16px;400;1.35|RS Recognize Outfit;16px;400;1.35;0px tracking|Barn Outfit;16px;400;1.35; Schedule Outfit,sans-serif, inherited body values|Native family names differ; actual face equivalence unverified|
|Page heading|32px/700/1.12;−1.05px tracking;16px bottom margin|Both declare these values and#111111|Barn same dimensions, color inherited; computed#030303. Schedule own heading|Barn color FAIL; dimensions agree as declarations|
|Section heading|25px/700/1.2;−.5px tracking|Feed empty title/SMS groups use section role|Barn same dimensions; computed#030303|Color FAIL Barn; do not compare25px section to15px record role|
|Record/context heading|15px/500/1.35;−.25px|Feed main-context; distinct from page/section headings|Schedule ringtitle; Barn sheets incomplete|Role separation intentional; rendered gates unverified|
|Primary/secondary action|44px min;15px/600/1.25;10px12px padding;8px radius;1px border|SMS primary/Feed secondary follow skin|Barn action same main metrics, inherits−.56px tracking; Schedule absent|Native metric agreement; Barn tracking FAIL; action not proven|
|Linked/scope/preset pill|32px min;13px/500/1.35;2px10px padding;5px radius;1px currentColor border|Feed/SMS match;gap4px;tracking0|Barn main skin match, computed tracking−.56px; Schedule main skin match but static blocks|Declaration agreement; Barn tracking FAIL; static Schedule is incomplete|
|Field|44px min;16px/400/1.35;10px12px;8px radius;#111 text/#fff background|Both match|Barn matches main dimensions; select computed line-height normal, input21.6px|Native agreement, rendered select differs; focus/keyboard NOT TESTED|
|Theme mode|36px min;13px/400/1.35;6px10px;5px radius;65px minimum at wide desktop,0 at narrow|Feed min-width65;SMS0 in base; both have narrow handlers|Barn no minimum declared; computed44px tall; Schedule65 but static|Desktop copies inconsistent; narrow native overrides do not prove rendered behavior|
|View switch|12px/400;muted#646873;44px target;4px0 padding|Feed/SMS match|Barn uses shared14px/500 accent link,0 padding; Schedule static|Barn like-for-like FAIL|
|Tabs|44px;14px/500/1.35;8px0;gap16;selected transparent,#111,2px accent underline|Source page roles intentional, Feed message-kind uses pills|Barn native tabs; selected computed gray#c8c8c8/muted; Schedule labels|Barn selected FAIL; Schedule action missing|
|Badge/token|10px/500/1.2;4px7px;5px radius;1px currentColor|Feed native styles match; colors#315e9e/#187140; dark#8fb8ff/#49d17d|Schedule badges/IDs partly plain text; Barn icon/text fixtures incomplete|Source roles intentionally compact; Schedule incomplete|
|Body/help|16px/1.35;help14px/400/1.4|Feed message body16px/1.4/pre-wrap/anywhere intentional; Feed/SMS help14px|Barn help/row metadata present|Message-body1.4 differs intentionally from base1.35|
|Card/row|Panels1px border,8px radius;record rows differ from Barn edit rows|Feed panel#fff border#dfe1e5;message rows/card layout|Barn row min64px,6px38px6px0 vs source10px38px10px0; Schedule grid min49px|Barn row vertical padding drift; cross-role row comparisons not flattened|
|Container|Shared header/page max672px,gutters16px;source page role includes scroll area|Feed/SMS header/main max672,16px; root100vh/overflow hidden; content scroll classes|Barn fixed root,main top125+padding28; Schedule main padding-top153|Declared top153 intended agreement; actual overflow/scroll matrix blocked|
|Icon|Source pill12px,action20px,close24px;44px icon targets|Native icon families inspectable via full style values/tree|Barn chooser/edit uses glyph⌄/✎, not source vectors|Barn icon fidelity incomplete; full state placement NOT TESTED|

Feed has91 own classes,27 dark,6 mobile,3 narrow,4 selected; SMS74,22 dark,7 mobile,3 narrow,4 selected. Barn39 with no own dark/mobile/narrow/selected class observed. Schedule34 with no own dark/mobile/narrow and2 static selected classes. Recognize41 own classes in this filtered set; its theme logic/variables are separate and not judged by missing class-name suffixes. These counts establish saved definitions only.

Hover/focus/focus-visible/disabled state rendering and complete breakpoint overrides were NOT TESTED. Feed/SMS freeform scripts declare role/button,tabindex0 and Space/Enter handling for anchors; this supports intended keyboard action, not a working keyboard pass. Generic copied source CSS alone is not proof it targets page-prefixed native elements. No absence of focus support is inferred from the base-only read.

## Findings and proposed narrow corrections — no corrections applied

|Finding|Expected vs observed|Evidence and impact|Proposed narrow correction|
|---|---|---|---|
|F1 Barn `rs-barn-v24-tab.w--current`,Horses|Transparent selected background,#111 text,accent underline versus computed#c8c8c8 background,#646873 text|393x684 observation shows gray block; selected state inconsistent|Page-owned current-tab style matching source skin; retest native tabs|
|F2 Barn `rs-barn-v24-h1/h2`|#111 versus computed#030303|Actual white headings inherit different color; same historical Recognize deviation|Explicit page-owned heading color in both themes; preserve accepted Recognize pending separate authorization|
|F3 Barn action/pill/tab/mode anchors|Source normal tracking versus computed−0.56px|Maya Reed pill width80.981 versus source86.024 with same13px/500 skin; narrower text and wrapping risk|Set page-owned tracking to source normal/0 for affected roles; inspect actual cascade before edit|
|F4 Barn `rs-barn-v24-link` used as Mobile view|Source12px/400 muted,4px0 versus14px/500 accent,0 padding|Actual view-switch styling differs from other current copies|Separate page-owned view-switch skin; retain action-link skin for Add rider/location|
|F5 Barn mode/tabs/icons/sheets and page actions|Source interactive themed controls/complete sheets versus empty page freeform code and incomplete fixtures|Native Tabs are present; link actions are not proven, dark fixture absent; glyph icons differ|Keep incomplete status; finish only separately authorized visual/behavior scope and original budget extension|
|F6 Schedule v24 `rsv24s-*` controls|Source buttons/checks/drawer/flyup versus0 native control nodes,static Paragraph/Block labels and empty page freeform code|Labels imitate buttons without working controls; dark/alternate/expanded fixtures absent|Preserve partial saved draft; separately authorize missing native fixtures/behavior, no restart in this audit|
|F7 SMS `rs-alert-v24-mode` wide minimum width|Source65px wide/0 narrow versus0px base;Feed65px base|Native like-for-like difference; actual wide render unverified|Reconcile wide/narrow mode width only after rendered access; no blanket recolor|
|F8 Barn `rs-barn-v24-row`|Source10px vertical padding versus6px native; min64px retained|Compact native row copy differs from same source edit-row role|Verify min-height/content result then adjust page-owned row padding only|
|F9 Recognize continuation route — PROVISIONAL|Replacement Barn slug setup-v24 versus root data-rs-setup-path legacy setup and script location.assign fallback to same legacy slug|Current headless read supports legacy route; worker is actively integrating; not clicked|Worker/owner resolve exact intended destination after integration settles; no interference from audit|
|F10 Feed stale setup-path metadata — advisory|Current replacement exists;Feed root still declares legacy setup path|No visible Feed setup navigation handler was established; not claimed as a working routing defect|Review only if that attribute is used by the accepted navigation contract|

[Barn rendered values](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/barn-observed-393.json), [source Barn computed values](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-barn-393.json), [Barn screenshot](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/barn-observed-393.png), [source screenshot](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/source-barn-393.png). These establish computed style differences, not equal-height visual fidelity. A CSS-rule-origin lookup returned no readable matching rules; the inherited cascade’s exact originating selector is NOT VERIFIED.

## Per-page control matrix

Complete per-element native inventory, labels,IDs,classes,attributes,declared visibility and NOT TESTED status: [control inventory](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/onboarding-consistency-audit/all-control-inventory.json). Counts include native input wrappers and hidden fixtures; they are not a claim of the number of user-visible working controls.

|Page|Native control-related nodes|Families and states|Safe target action/focus result|
|---|---:|---|---|
|feed|15|View/mobile;White/Dark;All/SMS replies/Updates/Help;Filters open/close;search/ring/rider;Unread only;clear;mark shown read|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|
|sms|64|View/mobile;White/Dark;SMS enabled;phone;18 alert check/preset/time families;Save preferences submit|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|
|barn|73|View/mobile;White/Dark;barn chooser/edit;5 native tabs;record/pill/edit;fields/selects;add/link/save;empty/error/success/sheet fixtures|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|
|schedule|0|Mobile/White/Dark,Rings/Entries,Rollups,All/Ring1/Ring2,row/entry labels are static;no native action nodes|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|
|recognize|21|View/mobile;White/Dark;close/reopen;details/cancel;login/recovery;continue/profile/member-link/error states|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|
|old-barn|19|Barn selector;setup tabs;linked/edit/add/save;legacy fixture roles|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|
|old-schedule|17|Rings/Entries;All/Ring1/Ring2;class/entry actions;legacy fixtures|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|
|components|89|Combined recognition/setup/schedule/alerts/feed controls;legacy unstyled control inventory|NOT TESTED — protected session / isolated login; write-producing submit/save/recovery excluded|

No target interaction pass was issued. Barn’s currently selected native Horses tab was observed; the audit did not click it. Source-only view/page navigation was used to inspect comparison roles and is not target action proof. Submit/save,real opt-ins,member-link/recovery,mark-read persistence and messages were not invoked on targets.

## Required rendered viewport matrix

Each cell is **white / dark**. **B**=BLOCKED/no current paired target render. **O**=single target-only observation at a different height, not an acceptance pass. Every column/page remains unverified at the stated complete gate; no unchecked width/theme is omitted.

|Width|Feed|SMS|Barn v24|Schedule v24|Recognize|Old Barn|Old Schedule|DNU Components|
|---:|---|---|---|---|---|---|---|---|
|375|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|393|B/B|B/B|O/B (393x684)|B/B|B/B provisional|B/B|B/B|B/B|
|479|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|480|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|481|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|767|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|768|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|899|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|900|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|991|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|992|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|1280|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|1440|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|1920|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|
|393x852 explicit|B/B|B/B|B/B|B/B|B/B provisional|B/B|B/B|B/B|

Priority393/768/1440 could not be executed on isolated targets. Source393x852 was checked and fails settled geometry; source-only captures are not paired screenshots. Actual Barn393x684 is the only new target render. Desktop/tablet/mobile overflow,scroll,sticky/footer positioning,alignment,wrapping and icon placement across the matrix remain BLOCKED. Dark/hover/focus/selected/disabled/expanded/drawer/flyup cells remain unverified except the observed Barn white selected-tab computed defect. No historical screenshot is relabeled current proof.

## Retained Schedule and Barn facts

Schedule v24 white Rings/All sample was saved;buttons partly static,count badges plain text,dark/Entries/filter/drawer/flyup absent. Prior source5188 was clipped;no visual gate passed. Its original start17:27:47Z/deadline18:27:47Z remains ended. [Schedule checkpoint](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/schedule-v24-new-draft.md).

Barn v24 stopped PARTIAL at original budget15:07:39Z–16:07:39Z (checkpoint finish16:07:30.453Z). Native five tabs/forms and demo records saved;four linked sheets/nested states,chooser,icons,themes,mobile/view controls,selected treatment,all error/success/empty/edit states and complete width/393x852 matrix remain incomplete. Prior target samples were about684px high;no rendered fidelity PASS. [Barn checkpoint](C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/barn-v24-01a11c0e/checkpoint.json). This audit does not restart either build or authorize corrections.

## Gate disposition

|Gate|Status|Evidence / remaining work|
|---|---|---|
|Seven supplied identities + additional DNU Component Drafts|PASS scoped inventory|Exact current metadata; older88 pages not enumerated, so exhaustive site-wide inventory NOT VERIFIED|
|Saved HEAD and140 tracked working hashes unchanged|PASS local scope|Matching hash inventories;pre-existing status preserved|
|All eight native trees/freeform reads|PASS read scope|Per-page readbacks in evidence folder|
|Complete style/cascade/interaction consistency|PARTIAL / FAIL findings|374 native styles and rendered Barn values;full pseudo/breakpoint/computed matrix missing|
|Barn selected/heading/tracking/view-switch fidelity|FAIL observed scope|Expected/observed values and actual screenshot above|
|Schedule/Barn complete visual/functional page|FAIL incomplete|Existing saved checkpoints retained;no completion inferred|
|Matched white/dark width and393x852 matrix|BLOCKED|Isolated login;active Designer protected;source geometry invalid|
|Safe target navigation/tab/toggle/open/close + keyboard|NOT TESTED / BLOCKED|No isolated authenticated target;active browser only observed|
|Submit/save/recovery/real messaging|NOT TESTED excluded actions|No write-producing controls invoked;not full integration proof|
|Report/checkpoint/evidence preservation|PASS document scope|This report + task checkpoint + isolated evidence|
|No authorized-boundary mutations|PASS issued-action scope|Only local audit artifacts/checkpoint authored;no external write call issued;other worker activity not frozen|

Known evidence-control limitation: initial read-only metadata/source/tree observations preceded this session’s bind; binding occurred before continued audit collection and document authoring. No application or Webflow mutation occurred before or after binding. Receipt integrity cannot make the missing visual/interaction gates pass.

The remaining decision is access to an isolated authenticated draft preview and a settled saved-v24 source for the blocked checks. Any implementation or extension of expired Barn/Schedule budgets requires separate owner authorization. No next task was dispatched.

## Deadline save-and-stop
Fixed deadline2026-10-08T19:42:00Z passed;coordinator instructed save-and-stop without extension. No further browser/build/correction/retry actions. Owner-refreshed Feed evidence and settled Schedule native evidence remain retained;overallResultPARTIAL. Remaining exact next step requires an owner-authorized bounded verification window:stable app-only paired Feed393x852 white/dark captures,remaining viewport/control matrix,and settled Barn/Schedule preview checks. No completion or automatic continuation claimed.

## Owner stop — revision136

Owner explicitly moves on from visual audit and will make manual styling edits. This audit is STOPPED BY OWNER, not completed by acceptance. No further browser/Designer activity, access recovery, width checks, new audits or style fixes. Existing source/native/render/interaction evidence and all prior failures remain preserved. Revision131 restoration/continuation steps above are superseded and must not trigger automatic resumption.

Remaining manual corrections and verification:
- Confirmed saved Schedule close-label gap: page6ac7d2c7cf113c577ef2a6d7, rsv24s-close Link b2745984-7fa8-639a-1a90-0cfdfa8972a4 and equivalent close Links have attrs={} and visible×; source expects accessible name Close details. Minimal manual correction: add aria-label=Close details to these exact close controls. Closure, button semantics and focus return remain unchecked.
- Barn historical F1–F5 changes are saved and require rendered retest, not blind reapplication. Preserve later source row padding6px; earlier F8 proposed10px correction is superseded. Main chooser/glyph icons, initial selected indicator, dark logo, mobile/view switch, linked-sheet variants, keyboard/focus/overflow and error/success/empty states remain unchecked or incomplete per settled checkpoint.
- SMS wide theme minimum width differs in saved base declarations (source65px wide/0 narrow; target0px base). Actual breakpoint cascade/render was not verified; inspect before making a manual change.
- Schedule mobile logo/mode sizing, mobile drawer shape, content-driven flyup height and paragraph margins require rendered comparison before correction. Saved base values are not a confirmed computed defect. Static fixture labels do not establish functional controls.
- Recognize legacy setup route remains provisional; Feed stale setup-path metadata is advisory only. Recognize activation-policy requirements remain outstanding. No action/data wiring or deployment proof is supplied by this audit.
- Full original matrix remains incomplete: all eight target drafts, white/dark at375,393,479,480,481,767,768,899,900,991,992,1280,1440,1920 plus393x852, applicable selected/hover/focus/disabled/expanded/drawer/flyup states, scrolling/overflow and safe controls. Scoped Feed checks and settled Schedule native inventory remain valid only in their recorded scope. No design acceptance issued.

Coordinator owns the separate already-discussed Webflow-owned styling/Astro data-and-action wiring stage. This stop does not authorize this audit to implement it.
