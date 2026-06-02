# RingStatus PDF CSS Lane

Separate Cloudflare PDF lane for templates that must honor their CSS `@page` size.

## HPS stall card use

Use the card-sized source page:

```text
https://ringstatus.com/hps-print-card?tenantId=8778&horseRecordId=<record_id>
```

Send that page through the worker with `lane=css`:

```text
https://ringstatus-pdf.gombcg.workers.dev/?lane=css&url=<encoded hps-print-card url>&filename=<horse>-stall-card.pdf
```

The `css` lane does not send `format: "letter"` to Cloudflare Browser Rendering. It sends:

```js
{
  preferCSSPageSize: true,
  printBackground: true,
  margin: { top: "0in", right: "0in", bottom: "0in", left: "0in" }
}
```

This allows `@page { size: 5.5in 3.75in; margin: 0; }` on the source page to define the PDF page size.

## Default lane

Requests without `lane=css` keep the old behavior:

```js
{
  format: "letter",
  printBackground: true,
  margin: { top: "0in", right: "0in", bottom: "0in", left: "0in" }
}
```

## Deployment

This folder is deployable as its own worker, or the `buildPdfOptions()` and `readInput()` changes can be copied into the existing `ringstatus-pdf` worker.

Required secrets/vars:

```powershell
wrangler secret put CF_BROWSER_TOKEN
wrangler secret put CF_ACCOUNT_ID
wrangler deploy
```
