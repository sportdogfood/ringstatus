# LM portfolio-system — owner revision, 14 October 2026

Source: native Webflow element tree read from /lm, page `6a987d7a71da602c33cb1d78`. Compared with the saved13 October tree. Documentation only; no Webflow edits or publication.

## Reusable CMS section

```text
portfolio-system
└─ portfolio-container
   └─ Collection List Wrapper
      └─ rs-section-list
         └─ Collection Item
            └─ portfolio-item
               ├─ ls-portfolio-section-spacer
               │  └─ portfolio-video-frame
               │     ├─ portfolio-image
               │     ├─ portfolio-video-poster
               │     ├─ portfolio-video
               │     └─ portfolio-video-replay
               └─ portfolio-content
                  └─ portfolio-content-bg
                     └─ fsl-vw-panel is-transparent
                        └─ fsl-vw-copy
                           ├─ l5-kicker
                           │  └─ Kicker2 is-lainey
                           ├─ l5-hp
                           │  ├─ l5-h
                           │  │  └─ l5-heading-dark / l5-heading-white
                           │  └─ l5-p
                           │     └─ shared paragraphs, optional is-xl
                           └─ rs10-tips is-show
                              └─ optional hashtags / existing CTA
```

Collection Item's sibling `lp-spacer-2vw add-bottom`, empty states, and the `portfolio-js-css` embeds remain present. The three portfolio-system sections share this content stack while retaining their media/layout variants.

## Changes identified

| Earlier wrapper/pattern | Current LM pattern |
| --- | --- |
| portfolio-text | fsl-vw-copy |
| portfolio-heading | l5-hp > l5-h |
| portfolio-paragraphs | l5-hp > l5-p |
| fsl-vw-panel is-transparent is-copy | fsl-vw-panel is-transparent |
| First section's unnamed tips wrapper | rs10-tips is-show |
| Dashboard actions outside the copy stack | rs10-tips is-show inside fsl-vw-copy |
| Ordinary paragraph roles throughout repeated content | First two repeated-content paragraphs use l5-paragrapg-dark is-xl; third retains base |
| Dashboard paragraph base role | l5-paragrapg-dark is-xl |
| Repeated media frame without portfolio-image | portfolio-image retained alongside poster/video/replay |

Kickers now occupy the shared l5-kicker position in each content stack. The first section deliberately uses `l5-kicker is-hidden`; repeated content and dashboard use `Kicker2 is-lainey`. Hashtags use `Kicker2 is-hash`; first-section hashtags are hidden. These are existing owner choices, not proposed corrections.

The first section retains `ls-portfolio-section-spacer-static is-ringstatus`, `portfolio-content is-first` and `portfolio-content-bg is-rs`. Dashboard retains `portfolio-content is-left`, `portfolio-content-bg is-ld`, and `ls-portfolio-section-spacer is-white is-dashboard`. Do not flatten these intentional presentation variants.

## What this standardizes

The CMS supplies content and media; the portfolio shell controls their arrangement. Content inside the shell now uses the same copy, kicker, heading/paragraph and tips wrappers as the shared section inventory. Reuse the established global typography/gap rules for these classes rather than creating a separate portfolio type scale.

Earlier documentation of portfolio-text / portfolio-heading / portfolio-paragraphs describes the prior LM structure. Those wrappers are absent from the three current LM portfolio-system content stacks. Do not restore them or remove site classes globally: other pages were not reinspected in this read-only pass.

## Preserve media and behavior

Owner-confirmed CMS binding from the supplied Designer screenshots:
- portfolio-video-poster is a separate img, src bound to poster-src, aria-hidden=true.
- portfolio-video is a video, src bound to video-src, muted, playsinline, preload=auto, aria-hidden=true.
- Current tree confirms both elements, video-frame, replay button and data-portfolio-video hook. Do not replace the separate poster with a video poster attribute or alter playback/replay code.
- Keep portfolio-css, portfolio-videojs and videojs embeds intact.

## Approved responsive ordering — include in lock

Owner confirmed this working code on14 October. Preserve it as the portfolio-specific layout rule; do not replace the system:

```html
<style>
.rs-section-list .w-dyn-item:nth-child(odd) .ls-portfolio-section-spacer {
  order: -1;
}
.rs-section-list .w-dyn-item:nth-child(even) .ls-portfolio-section-spacer {
  order: 1;
}
@media screen and (max-width: 767px) {
  .rs-section-list .w-dyn-item:nth-child(odd) .ls-portfolio-section-spacer,
  .rs-section-list .w-dyn-item:nth-child(even) .ls-portfolio-section-spacer {
    order: 0;
  }
}
</style>
```

At767px and below, including479px, the spacer returns to Navigator order. Desktop alternating placement remains. The static spacer is not targeted by this class selector.

## Evidence / acceptance boundary

Exact current structure: `docs/webflow-lm-portfolio-owner-2026-10-14.json`.
Earlier comparison: `docs/webflow-6a987d7a71da602c33cb1d78-2026-10-13-before.json`.
This documents the owner revision. Previous viewport measurements precede this revision and are not fresh validation of it. No new visual/viewport audit or full stylesheet lock is claimed.

