$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $RepoRoot

$fetchScript = Join-Path $RepoRoot 'sgl_fetch.ps1'
if (-not (Test-Path -LiteralPath $fetchScript)) {
    throw "Missing SGL fetch helper: $fetchScript"
}

$outDir = Join-Path $RepoRoot ("tmp\sgl_schedule_samples\" + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$tests = @(
    @{
        name = 'schedule_200000061_2026-05-09'
        url = 'https://sglapi.wellingtoninternational.com/schedule?date=2026-05-09&show_id=200000061&customer_id=15'
        expected_show_id = 200000061
        expected_show_date = '2026-05-09'
    },
    @{
        name = 'schedule_200000060_2026-05-02'
        url = 'https://sglapi.wellingtoninternational.com/schedule?date=2026-05-02&show_id=200000060&customer_id=15'
        expected_show_id = 200000060
        expected_show_date = '2026-05-02'
    }
)

$results = foreach ($test in $tests) {
    $rawPath = Join-Path $outDir "$($test.name).json"
    $prettyPath = Join-Path $outDir "$($test.name).pretty.json"

    Write-Host ""
    Write-Host "FETCH $($test.name)"

    $meta = & $fetchScript -Url $test.url -OutputPath $rawPath | ConvertFrom-Json
    $jsonText = Get-Content -LiteralPath $rawPath -Raw
    $json = $jsonText | ConvertFrom-Json

    $keys = @($json.PSObject.Properties.Name)
    $rings = @($json.rings)
    $classes = @($rings | ForEach-Object { @($_.classes) })
    $showId = $json.show.show_id
    $showDate = [string]$json.show_date

    $json | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $prettyPath -Encoding UTF8

    [pscustomobject]@{
        name = $test.name
        ok = [bool]$meta.ok
        status_code = $meta.status_code
        body_length = $meta.body_length
        raw_content_length = $meta.raw_content_length
        empty_object = ($keys.Count -eq 0)
        show_id = $showId
        show_date = $showDate
        show_id_matches = ([int]$showId -eq [int]$test.expected_show_id)
        show_date_matches = ($showDate -eq [string]$test.expected_show_date)
        ring_count = $rings.Count
        class_count = $classes.Count
        raw_file = $rawPath
        pretty_file = $prettyPath
    }
}

Write-Host ""
Write-Host "SGL schedule sample results"
$results | Format-Table -AutoSize

Write-Host ""
Write-Host "Files written to:"
Write-Host $outDir
