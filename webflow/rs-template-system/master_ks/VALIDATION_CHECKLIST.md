# Validation Checklist v2

Run this before any page-specific modifications.

## File source

- [ ] The page was copied from `00_APPROVED_BASE_DO_NOT_REBUILD.html`.
- [ ] No old colored/shaded teaching exercise was used.
- [ ] No loose Downloads file was used as source.
- [ ] CSS is in `rs-global.css`.
- [ ] JS is in `rs-scripts.js`.

## Shell

- [ ] Standard sections use `rs-section > rs-section-container > rs-section-padding > rs-content-container > rs-content-flex > rs-content`.
- [ ] Section owns full-width background.
- [ ] Container owns max-width.
- [ ] Padding owns responsive spacing.
- [ ] Content-flex owns layout.
- [ ] Content owns readable max width.
- [ ] Horizontal padding does not compound.

## Typography

- [ ] Font stack is `Outfit, Inter, Arial, sans-serif`.
- [ ] `--rs-h1` exists.
- [ ] `--rs-h2` exists.
- [ ] `--rs-p` exists.
- [ ] `--rs-t` exists.
- [ ] Heading font-weight is `600`.
- [ ] Heading line-height is `0.95`.
- [ ] Heading letter-spacing is `-0.05em`.
- [ ] Paragraph line-height is `1.23`.
- [ ] T/small line-height is `1.6`.

## Nav

- [ ] Logo is fixed/bottom.
- [ ] First nav dropdown opens full-width mega.
- [ ] Second nav dropdown opens right drawer.
- [ ] Drawer locks viewport with `body.is-nav-locked`.
- [ ] Escape closes open nav UI.
- [ ] No separate mobile nav was created.

## Wildcard

- [ ] `.rs-section.is-wildcard` exists.
- [ ] `.rs-section.is-wildcard` has `min-height:260svh` or equivalent exact rule.
- [ ] `.rs-wildcard-sticky` has `position:sticky`.
- [ ] `.rs-wildcard-sticky` has `top:0`.
- [ ] `.rs-wildcard-scroll-layer` has `margin-top:-100svh`.
- [ ] Next normal section pushes sticky out of view.

## Expandable list

- [ ] `[data-list-limit]` exists in the HTML if expandable list component is used.
- [ ] `rs-scripts.js` controls See more / See less.
