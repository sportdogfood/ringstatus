param([string]$LogPath)
$ErrorActionPreference = "Continue"
$sha = "16a0b199d9e03f103af4cd93e8a0a6da8db5e035"
$checks = @(
  @{ name="rswp_page"; url="https://ringstatus.com/rswp"; kind="text"; mustContain="packing-app" },
  @{ name="rswp_print_page"; url="https://ringstatus.com/rswp-print?packWaveKey=wave_one&target=overview"; kind="text"; mustContain="wec-packing-print" },
  @{ name="state"; url="https://ringstatus.com/test/wec-packing/state?packWaveKey=wave_one"; kind="json"; mustContain="" },
  @{ name="print_overview"; url="https://ringstatus.com/test/wec-packing/print?packWaveKey=wave_one&target=overview"; kind="text"; mustContain="WEC" },
  @{ name="cdn_js"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/wec-packing.js"; kind="text"; mustContain="WEC_PACKING_CONFIG" },
  @{ name="cdn_css"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/styles.css"; kind="text"; mustContain="packing-app" },
  @{ name="cdn_locked_css"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/rsa-stylesheets.locked.css"; kind="text"; mustContain="rsa" },
  @{ name="cdn_print_js"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/wec-packing-print.js"; kind="text"; mustContain="wec-packing-print" },
  @{ name="cdn_print_css"; url="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@$sha/webflow/packing-worksheet/wec-packing-print.css"; kind="text"; mustContain="wec-packing-print" }
)
$start = Get-Date
$end = $start.AddMinutes(30)
$cycle = 0
while ((Get-Date) -lt $end) {
  $cycle++
  foreach ($check in $checks) {
    $row = [ordered]@{ ts=(Get-Date).ToString("o"); cycle=$cycle; name=$check.name; ok=$false; status=$null; ms=$null; bytes=$null; contains=$null; items=$null; sourcePackItems=$null; worksheetItems=$null; error=$null }
    $sw = [Diagnostics.Stopwatch]::StartNew()
    try {
      $resp = Invoke-WebRequest -Uri $check.url -UseBasicParsing -TimeoutSec 25
      $sw.Stop()
      $content = [string]$resp.Content
      $row.status = [int]$resp.StatusCode
      $row.ms = [int]$sw.ElapsedMilliseconds
      $row.bytes = $content.Length
      $row.ok = ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300)
      if ($check.mustContain) {
        $row.contains = $content.Contains($check.mustContain)
        $row.ok = $row.ok -and $row.contains
      }
      if ($check.kind -eq "json") {
        $json = $content | ConvertFrom-Json
        $row.items = @($json.items).Count
        $row.sourcePackItems = [int]$json.counts.sourcePackItems
        $row.worksheetItems = [int]$json.counts.worksheetItems
        $row.ok = $row.ok -and [bool]$json.ok -and $row.items -gt 0 -and $row.sourcePackItems -gt 0 -and $row.worksheetItems -gt 0
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
