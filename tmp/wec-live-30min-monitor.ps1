param($LogPath)
$ErrorActionPreference = "Continue"
$sha = "16a0b199d9e03f103af4cd93e8a0a6da8db5e035"
$checks = @(
  @{ name="rswp_page"; url="https://ringstatus.com/rswp"; kind="text" },
  @{ name="rswp_print_page"; url="https://ringstatus.com/rswp-print?packWaveKey=wave_one&target=overview"; kind="text" },
  @{ name="state"; url="https://ringstatus.com/test/wec-packing/state?packWaveKey=wave_one"; kind="json" },
  @{ name="print_overview"; url="https://ringstatus.com/test/wec-packing/print?packWaveKey=wave_one&target=overview"; kind="text" },
  @{ name="cdn_js"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/wec-packing.js"; kind="text" },
  @{ name="cdn_css"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/styles.css"; kind="text" },
  @{ name="cdn_print_js"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/wec-packing-print.js"; kind="text" }
)
$end = (Get-Date).AddMinutes(30)
$cycle = 0
while ((Get-Date) -lt $end) {
  $cycle++
  foreach ($check in $checks) {
    $row = [ordered]@{ ts=(Get-Date).ToString("o"); cycle=$cycle; name=$check.name; url=$check.url; ok=$false; status=$null; ms=$null; items=$null; sourcePackItems=$null; worksheetItems=$null; bytes=$null; error=$null }
    $sw = [Diagnostics.Stopwatch]::StartNew()
    try {
      $resp = Invoke-WebRequest -Uri $check.url -UseBasicParsing -TimeoutSec 25
      $sw.Stop()
      $row.status = [int]$resp.StatusCode
      $row.ms = [int]$sw.ElapsedMilliseconds
      $row.bytes = ($resp.Content | Measure-Object -Character).Characters
      $row.ok = ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300)
      if ($check.kind -eq "json") {
        $json = $resp.Content | ConvertFrom-Json
        $row.ok = $row.ok -and [bool]$json.ok -and @($json.items).Count -gt 0
        $row.items = @($json.items).Count
        $row.sourcePackItems = [int]$json.counts.sourcePackItems
        $row.worksheetItems = [int]$json.counts.worksheetItems
      }
    } catch {
      $sw.Stop()
      $row.ms = [int]$sw.ElapsedMilliseconds
      $row.error = $_.Exception.Message
    }
    ($row | ConvertTo-Json -Compress) | Add-Content -LiteralPath $LogPath
  }
  if ((Get-Date).AddSeconds(60) -lt $end) { Start-Sleep -Seconds 60 } else { break }
}
