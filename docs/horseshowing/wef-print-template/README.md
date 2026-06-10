# WEF Print Template Source

Captured from the WEF schedule print/PDF lane for reuse by the Horseshowing/WEC lane.

Files:

- `wef-print-style.fragment.html` - original `<style>` block.
- `wef-print-shell.fragment.html` - required page/body shell.
- `wef-print-script.fragment.html` - original `<script>` renderer.

Required DOM ids/classes from the script:

- `#title`
- `#subtitle`
- `#btnPdf`
- `#wrap`
- `#colL`
- `#colR`
- `.topbar`
- `.ringWrap`
- `.ringClose`

Important renderer behaviors:

- Measures rendered ring heights and splits rings into left/right columns.
- Scales the rendered page to fit letter-size PDF output.
- Builds a PDF worker URL using `https://ringstatus-pdf.gombcg.workers.dev/`.
- Reads tenant-specific display fields like `8778_sched_display`.

Next adapter target:

Generate a WEC schedule JSON with WEF-compatible field names, then point the renderer at that JSON instead of the WEF Cloudflare schedule URL.
