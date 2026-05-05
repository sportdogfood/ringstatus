$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $RepoRoot

$fetchScript = Join-Path $RepoRoot 'sgl_fetch.ps1'
if (-not (Test-Path -LiteralPath $fetchScript)) {
    throw "Missing SGL fetch helper: $fetchScript"
}

$outDir = Join-Path $RepoRoot ("tmp\sgl_people_samples\" + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$peopleId = 8778
$customer = 15

$tests = @(
    @{
        name = 'people_8778_200000061'
        url = "https://sglapi.wellingtoninternational.com/people/$peopleId`?pid=$peopleId&show_id=200000061&customer_id=$customer"
        expected_people_id = $peopleId
        expected_show_id = 200000061
    },
    @{
        name = 'people_8778_200000060'
        url = "https://sglapi.wellingtoninternational.com/people/$peopleId`?pid=$peopleId&show_id=200000060&customer_id=$customer"
        expected_people_id = $peopleId
        expected_show_id = 200000060
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
    $trips = @($json.trips)
    $entries = @($json.entries)
    $classes = @($json.classes)
    $peopleIdReturned = $json.people.people_id
    $showIdReturned = $json.show_id
    if ($null -eq $showIdReturned -and $json.people.show_id) {
        $showIdReturned = $json.people.show_id
    }

    $json | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $prettyPath -Encoding UTF8

    [pscustomobject]@{
        name = $test.name
        ok = [bool]$meta.ok
        status_code = $meta.status_code
        body_length = $meta.body_length
        raw_content_length = $meta.raw_content_length
        empty_object = ($keys.Count -eq 0)
        people_id = $peopleIdReturned
        people_id_matches = ([int]$peopleIdReturned -eq [int]$test.expected_people_id)
        show_id = $showIdReturned
        show_id_matches = if ($null -eq $showIdReturned) { $null } else { [int]$showIdReturned -eq [int]$test.expected_show_id }
        total_trips = $json.total_trips
        trip_count = $trips.Count
        entry_count = $entries.Count
        class_count = $classes.Count
        raw_file = $rawPath
        pretty_file = $prettyPath
    }
}

Write-Host ""
Write-Host "SGL people sample results"
$results | Format-Table -AutoSize

Write-Host ""
Write-Host "Files written to:"
Write-Host $outDir
