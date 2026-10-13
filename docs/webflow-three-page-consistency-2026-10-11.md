# Three-page section consistency review
# Four-page scope and agreed normalization

Pages: Home `/`, Tools `/ring-status`, About `/about-ringstatus`, and Lainey `/lainey`.

Agreed direction: standard copy sections across these pages share `ls-section-content > fsl-method > fsl-vw-panel > fsl-vw-copy`, with `l5-kicker` optional and `l5-hp` containing heading and paragraph. Optional `fsl-vw-tabs` is a sibling of copy inside the same panel. Tips remain inside the copy wrapper.

- Tools' separate tab panels, nested copy panels, and copy spacing differences are unintended and should conform to Home.
- About deliberately omits or hides kickers; preserve that choice.
- Home's hidden `l5-is-hold` folder is to be removed completely during authorized implementation.
- Lainey's introduction, horses, videos, and dashboard copy should conform to the same copy skeleton, preserving content, CMS bindings, actions, and media behavior.
- Lainey's `l5-content-wrapper` is a copy-wrapper variation to normalize; the introduction's separately wrapped paragraph should join the standard heading/paragraph structure.
- Portfolio is a second reusable media-and-copy skeleton. Manage its layout separately while reusing shared heading, paragraph, spacing, and sizing variables. Preserve its CMS and video behavior.
- Schedule grids and horse/video card internals retain their functional structures. Nav and hero retain their own structures.
- Use shared defaults for common corrections. Keep specialization close to defaults and use no more than three classes per element; record necessary combo exceptions.

Status: comparison and agreed direction documented; normalization has not been applied. Airtable `rs-webflow-variables` remains a proposed inventory with example/unfilled values, not verified viewport settings. Rendering must be checked at each viewport after structural and style corrections.


## Lainey added to comparison

Page: /lainey, ID 6aa739aad90e0d765affe666. Native element tree inspected 11 October 2026. No edits; rendering and component/CMS runtime behavior not verified.

- Horses, videos, and dashboard headings share fsl-method > fsl-vw-panel > l5-content-wrapper > l5-kicker + l5-hp, with l5-h and l5-p underneath. l5-content-wrapper is the copy-wrapper variation to map to fsl-vw-copy.
- Intro has l5-hp directly under the panel; its l5-p is in a separate Div Block 11642. It also uses h1-xl + is-lp and rs-px inside the shared wrappers.
- Portfolio uses panel > portfolio-text > portfolio-heading + portfolio-paragraphs. A nested portfolio-heading duplicates that class on parent and child. Preserve CMS bindings and media behavior when mapping to shared copy typography.
- Schedule, horse cards, and video cards are specialized functional layouts; normalize surrounding copy without forcing their internal controls/grids into the copy skeleton.
- Lainey has lp-page-wrapper/lp-main-wrapper/lp-footer and lp-section-nav, unlike the public page wrappers and shared hero/nav components.
- Repeated lp-spacer-2vw + add-bottom elements control spacing; two appear consecutively before portfolio-system.
- l5-debug blocks occur in video and dashboard copy; visibility has not been verified.
- No inspected element has more than three assigned classes.

### Owner corrections to initial findings

Tools copy spacing and tab nesting are unintended inconsistencies and should conform to Home; they require no special-case combos. About's omitted kickers are intentional. Home's hidden l5-is-hold folder is slated for complete removal; it has not been removed during this read-only comparison.

### Lainey class outline

```text
 lp-page-wrapper
  lp-main-wrapper
   rs10-stack
    lp-section-nav
    ls-section-content
     lp-yt-hero
      lpt-hero-copy
       lpt-hero-text
     herojs
        fsl-method + no-margin
         fsl-vw-panel + is-transparent + is-copy
          l5-hp
           l5-h
            h1-xl + is-lp
            h1-xl + is-lp
          Div Block 11642
           l5-p
            rs-px
    lp-section + is-tab
     lp-container + is-1600
      lp-transcript-flex
       Tab Content Wrapper
        lp-card-outer
         Div Block 11564
          schedule-head
          lp-transcript-tabs-content-wrapper
           Div Block 11565
            schedule-row + is-head
             tt-class-title
              schedule-text
             tt-class-title
              schedule-text
              schedule-text
              schedule-text
            lp-full-class-list
                schedule-row
                  div-top
                   tt-class-month
                  div-bottom
                 tt-class-name
                  div-top
                   tt-class-title
                  div-bottom
                 tt-class-location
                  div-top
                   tt-class-title
                   tt-class-title
                   tt-class-title
                  div-bottom
                   schedule-state
                 tt-class-location
                  div-top
                   tt-class-barn-name
                  div-bottom
                   schedule-nested-event
                    Div Block 11605
                    event-title
                     schedule-text
                     schedule-text
                    Div Block 11606
                     schedule-text
                     schedule-text
     agg-painter
    lp-spacer-2vw + add-bottom
    ls-section-content
       bg-horses
        fsl-method + no-margin
         fsl-vw-panel + is-transparent + is-copy
          l5-content-wrapper
           l5-kicker
           l5-hp
            l5-h
             Heading 1655
            l5-p
    lp-spacer-2vw + add-bottom
    l4-section
     Wrapper Container + Header
      rs-new-cont + is-horse-grid
       Div Block 11535
        l4-section-bg
         lp-heading-wrapper
          l4-section-flex
           l4-card + is-horses
            l4-card-inner
             l4-copy
              l4-hp + is-hidden
               l4-kicker
               l4-h
               l4-px
             horse-card-bottom
                Paragraph 567
               Text Block 4349
              Link Block 12
               Link 154
        l4-controls
         l4-prev
         l4-next
    lp-spacer-2vw + add-bottom
    ls-section-content
       is-videos
        fsl-method + no-margin
         fsl-vw-panel + is-transparent + is-copy
          l5-content-wrapper
           l5-kicker
           l5-hp
            l5-h
             Heading 1654
            l5-p
           l5-debug
    lp-spacer-2vw + add-bottom
    l4-section
     Wrapper Container + Header
      rs-new-cont + is-video-grid
       Div Block 11535
        l4-section-bg
         lp-video-panel
          lp-video-mount
          lp-temp
           yt__wrapper
            ytp-cardv2
             ytp-media
              sizer
               ytp-poster
               ytp-overlay + is-favs
                ytp-play
                ytp-expand
            yt__video
             yt__info
               Div Block 11397
                yt__date
                Link 152
               lp-video-value-card__content
                lp-video-value-card__title
              Code Embed 151
            ytp-drawer
             ytp-drawer-inner
              ytp-section-head
               ytp-section-title
               ytp-pill-button
               ytp-pill-button
               ytp-pill-button
              ytp-drawer-media
               ytp-drawer-poster
               ytp-overlay
                ytp-play
              ytp-card-meta
    lp-spacer-2vw + add-bottom
    lp-spacer-2vw + add-bottom
    portfolio-system
     portfolio-container
       rs-section-list
         portfolio-item
          ls-portfolio-section-spacer
           portfolio-video-frame
            portfolio-video-poster
            portfolio-video
            portfolio-video-replay
          portfolio-content
           portfolio-content-bg
            fsl-vw-panel + is-transparent + is-copy
             portfolio-text
              portfolio-heading
               portfolio-heading
              portfolio-paragraphs
         lp-spacer-2vw + add-bottom
      portfolio-js-css
       videojs
       is-dash
        fsl-method
         fsl-vw-panel + is-transparent + is-copy
          l5-content-wrapper
           l5-kicker
           l5-hp
            l5-h
             Heading 1656
            l5-p
           rw5-actions
            modern-btn-css
            modern-btn
           l5-debug
    lp-footer
   rs10-contact=form
   rs5-page-control
    l5-sys
     l5-css
     l5-js
     l5md
    l4-css-js
     l4-css
     l4-css
     l4-js
     swipejs
```


11 October 2026. Live native Webflow element trees and shared base styles inspected. No Webflow edits or publishing. Viewport rendering remains unverified; this is a structure and class review.

Target skeleton: ls-section-content > fsl-method > fsl-vw-panel > fsl-vw-copy (l5-kicker + l5-hp) and optional sibling fsl-vw-tabs. Nav and hero retain their own structures. Maximum three classes per element.

## Findings

- All three page trees contain no element with more than three assigned classes. Component internals are not expanded in this count.
- Home standard copy sections use fsl-vw-copy and place tabs as its sibling inside the same panel.
- Tools tab sections commonly use fs1-copy and two sibling panels (is-copy / is-tabs). Two copy sections nest fsl-vw-panel inside another panel instead of the copy wrapper.
- About standard copy sections use fs1-copy and l5-hp without l5-kicker. Do not invent kicker content; decide whether the kicker is optional in the standard.
- Split-media sections use fs1-copy > bqw-container-1, preserving a distinct media layout that must be mapped before normalization.
- Home uses native hero content; Tools and About use rs10-hero and rs9-nav component instances. Review those component definitions before claiming complete nav/hero consistency.
- Home has an additional l5-is-hold typography example block in its rust tab section. Confirm intended visibility before any removal.

## Shared style implications

- fsl-vw-copy has no explicit base properties; fs1-copy explicitly uses flex column, 100% height, grow/shrink, and 16px gaps. These wrappers are not currently equivalent.
- l5-h uses var(--l5-h-font, 72px), var(--l5-h-line, 0.9), and var(--l5-h-letter, -0.055em).
- l5-p uses var(--l5-p-font, 22px), var(--l5-p-line, 1.35), and var(--l5-p-width, 72%). These are fallback values, not proof of final computed values.
- Kicker2 supplies the kicker typography: 14px, weight 600, line-height 1.2em, tracking 0.075em. l5-kicker is its wrapper.
- fsl-vw-panel has clamp(24px,4vw,64px) padding on all sides and hidden horizontal/vertical overflow. Nested panels can compound padding; clipping can conceal tab overflow.
- The Airtable examples are not an audited replacement for these current rules. Resolve variable definitions, component internals, and breakpoint overrides before filling verified mode values.

## Section structures

### / — 6982268d7543ac3c801512cd

Section 1

```text
ls-section-content
  lp-yt-hero
    rs-members-open
      member-link + is-hero
    lpt-hero-copy
      lpt-hero-title
      Collection Item 1079
        Div Block 11548
  herojs
```

Section 2

```text
ls-section-content + is-hero
  fsl-method + no-margin
    fsl-vw-panel + is-transparent
      fsl-vw-copy
        fsl-method-top
          rs0-big-text
        fsl-method-bottom
          fsl-method-copy-grid
```

Section 3

```text
ls-section-content + is-rust
  fsl-method + no-margin
    fsl-vw-panel + is-transparent
      fsl-vw-copy
        l5-kicker
          Kicker2 + is-dark
        l5-hp
          l5-h
          l5-p
        l5-is-hold
          fsl-vw-h5
          fsl-vw-h1 + is-white
          fsl-vw-h2
          fsl-vw-p
          fsl-vw-text
      fsl-vw-tabs
        rs-tap-message
          Paragraph 583
        fsl-vw-tab-wrapper
          fsl-tab-system
```

Section 4

```text
ls-section-content + is-dark
  fsl-method + no-margin
    fsl-vw-panel + is-transparent
      fsl-vw-copy
        l5-kicker
          Kicker2
        l5-hp
          l5-h
          l5-p
      fsl-vw-tabs
        rs-tap-message
          Paragraph 583
        fsl-vw-tab-wrapper
          fsl-tab-system
```

Section 5

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent
      fsl-vw-copy
        l5-kicker
          Kicker2
        l5-hp
          l5-h
          l5-p
        rs10-tips + is-show
          rs10-chat-left + is-show
          rs10-chat-right + is-hidden
```

Section 6

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent
      fsl-vw-copy
        l5-kicker
          Kicker2
        l5-hp
          l5-h
          l5-p
        rs10-tips + is-show
          rs10-chat-left
          rs10-chat-right + is-show
```

Section 7

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent
      fsl-vw-copy
        l5-kicker
          Kicker2
        l5-hp
          l5-h
          l5-p
        rs10-tips + is-show
          rs10-chat-left + is-show
          rs10-chat-right
```

Section 8

```text
ls-section-content
  fsl-method + no-margin + is-split
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        bqw-container-1
          bqw-b-wrap-1
          bqw-b.2-wrap-1
  lp-spacer-2vw + add-bottom
```

Section 9

```text
ls-section-content
  fsl-method + no-margin + is-split
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        bqw-container-1
          bqw-b.2-wrap-1 + no-margin
          bqw-b-wrap-1
  lp-spacer-2vw + add-bottom
```

Section 10

```text
ls-section-content
  fsl-method + no-margin + is-split
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        bqw-container-1
          bqw-b-wrap-1 + sm-padding
          bqw-b.2-wrap-1
  lp-spacer-2vw + add-bottom
```

Section 11

```text
ls-section-content
  fsl-method + no-margin + is-split
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        bqw-container-1
          bqw-b-wrap-1
          bqw-b.2-wrap-1
  lp-spacer-2vw + add-bottom
```

Section 12

```text
ls-section-content
  rs10-bigg-text
    Collection List Wrapper 296
          Div Block 11548 Copy
```

Section 13

```text
ls-section-content + is-top-footer
  fsl-vw-panel-footer
    rs10-footer-wrapper
      rs-footer-stretch-height
          Div Block 11628
        Div Block 11598
          Div Block 11624
      rs-footer-stretch-height
        rs10-waitlist-block
          rs10-waitlist-title
          rs10-waitlist-input
          rs10-waitlist-bottom
      rs10-footer-bottom
        rs-legal-block
          rs-copyright-block
          rs-tos-block
          rs-yearjs
```

Section 14

```text
ls-section-content + is-cloudy
  rs-split-bg + is-cloudy
    rs-cutom-cont
      rs-split-height2
        rs-grid12c
          l6-card-wrapper + is-22
          rs-split-img + is33
```

Section 15

```text
ls-section-content
    rs-cards-cont
      rs-card-height
        l4-section
          l4-section-bg
          l4-controls
```


### /ring-status — 6ab11bd52642dca0c8e74004

Section 1

```text
ls-section-content + is-hero
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fsl-vw-copy
        fsl-method-top
          rs0-big-text
      fsl-method-bottom
        fsl-method-copy-grid
          fsl-method-copy-left
          fsl-method-copy-right
```

Section 2

```text
ls-section-content + is-dark
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-kicker
          Kicker2 + is-color-is-err
        l5-hp
          l5-h
          l5-p
    fsl-vw-panel + is-transparent + is-tabs
      rs10-tab-block
        tabs-message-white
          fsl-vw-tab-menu + is-dark
```

Section 3

```text
ls-section-content + is-color-is-err
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-kicker
          Kicker2 + is-rust
        l5-hp
          l5-h
          l5-p
    fsl-vw-panel + is-transparent + is-tabs
      rs10-tab-block
        tabs-message-white
          fsl-vw-tab-menu
```

Section 4

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fsl-vw-panel + is-transparent + is-copy
        l5-kicker
          Kicker2 + is-color-is-err
        l5-hp
          l5-h
          l5-p
      rs10-tips
        rs10-chat-left
          chat-msg messagex-bot
        rs10-chat-right + is-hidden
          chat-msg messagex-user
```

Section 5

```text
ls-section-content
  fsl-method + no-margin + is-split
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        bqw-container-1
          bqw-b-wrap-1
          bqw-b.2-wrap-1
```

Section 6

```text
ls-section-content + is-color-is-err
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-kicker
          Kicker2 + is-rust
        l5-hp
          l5-h
          l5-p
    fsl-vw-panel + is-transparent + is-tabs
      rs10-tab-block
        tabs-message-white
          fsl-vw-tab-menu
```

Section 7

```text
ls-section-content + is-dark
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-kicker
          Kicker2 + is-color-is-err
        l5-hp
          l5-h
          l5-p
    fsl-vw-panel + is-transparent + is-tabs
      rs10-tab-block
        tabs-message-white
        Tabs 468
          fsl-vw-tab-menu + is-dark + add-auto
```

Section 8

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-kicker
          Kicker2 + is-color-is-err
        l5-hp
          l5-h
          l5-p
      rs10-tips
        rs10-chat-left
          chat-msg messagex-bot
        rs10-chat-right + is-hidden
          chat-msg messagex-user
```

Section 9

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fsl-vw-panel + is-transparent + is-copy
        l5-kicker
          Kicker2 + is-color-is-err
        l5-hp
          l5-h
          l5-p
      rs10-tips
        rs10-chat-left
          chat-msg messagex-bot
        rs10-chat-right + is-hidden
          chat-msg messagex-user
```


### /about-ringstatus — 6ab260d76d76d28aa9be185f

Section 1

```text
ls-section-content + is-hero
  fsl-method + no-margin
      fsl-vw-copy
        fsl-method-top
          rs0-big-text
      fsl-method-bottom
        fsl-method-copy-grid
```

Section 2

```text
ls-section-content
  fsl-method + no-margin + is-split
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        bqw-container-1
          bqw-b-wrap-1
          bqw-b.2-wrap-1
```

Section 3

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-hp
          l5-h
          l5-p
        rs10-tips + is-show
          rs10-chat-left + is-show
          rs10-chat-right + is-hidden
```

Section 4

```text
ls-section-content + is-color-9
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-hp
          l5-h
          l5-p
        rs10-tips
          rs10-chat-left
          rs10-chat-right + is-hidden
```

Section 5

```text
ls-section-content + is-dark
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-hp
          l5-h
          l5-p
        rs10-tips
          rs10-chat-left
          rs10-chat-right + is-hidden
```

Section 6

```text
ls-section-content
  fsl-method + no-margin
    fsl-vw-panel + is-transparent + is-copy
      fs1-copy
        l5-hp
          l5-h
          l5-p
        rs10-tips
          rs10-chat-left
          rs10-chat-right + is-hidden
```


