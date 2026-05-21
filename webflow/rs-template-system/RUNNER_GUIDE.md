# Runner Guide

Use this guide for simpler starter-shell tasks.

## Step 1: Identify The Lane

- Landing page or marketing/page shell: use `master_ks`.
- Branded app shell or app UI: use `master_app` after it is locked.
- Data-populated embed: use `api_templates` guidance and the correct app/page source.

## Step 2: Copy, Then Validate

- Copy from the gold source first.
- Keep CSS and JS separate.
- Validate typography, nav, sticky wildcard behavior, and expandable list behavior before edits.
- Do not rebuild from memory or earlier previews.

## Step 3: Select Sections By Name

Starter prompts may request sections by stable ID or alias:

```txt
topnav, s01, s05, s06, footer
```

Copy only those named pieces, preserve wrapper order, then edit page text.

## Step 4: Output Outside Gold

Create targeted files outside `master_ks` and `master_app`.

Do not edit a gold source unless the user explicitly unlocks it.

