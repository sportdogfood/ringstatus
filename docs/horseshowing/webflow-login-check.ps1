Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
$cloudRoot = Join-Path $repoRoot "webflow-cloud-test"

Set-Location $cloudRoot

Write-Host "Webflow CLI version:"
webflow --version

Write-Host ""
Write-Host "Opening Webflow CLI login..."
Write-Host "Complete the browser login, then return to this PowerShell window."
webflow auth login

Write-Host ""
Write-Host "Current Webflow CLI auth status:"
$status = webflow auth status --skip-update-check 2>&1
$status | ForEach-Object { Write-Host $_ }

Write-Host ""
if ($status -match "custom_code:write") {
  Write-Host "PASS: CLI token includes custom_code:write"
} else {
  Write-Host "FAIL: CLI token does not include custom_code:write"
  Write-Host "Webflow custom-code API requires OAuth custom_code:read and custom_code:write."
  Write-Host "If this still fails after login, we need the Webflow MCP/OAuth bridge token, not the standard CLI token."
}
