param(
    [string]$InputPath,
    [string]$Destination = "C:\actions-runner\ringstatus\manual_sgl_payloads",
    [int]$ShowId,
    [string]$ShowDate,
    [switch]$AlsoCopyToRepo
)

$ErrorActionPreference = "Stop"

function Get-JsonText {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        throw "No JSON text was provided."
    }

    $trimmed = $Text.Trim()
    if ($trimmed.StartsWith("{") -and $trimmed.EndsWith("}")) {
        return $trimmed
    }

    $start = $trimmed.IndexOf("{")
    $end = $trimmed.LastIndexOf("}")
    if ($start -ge 0 -and $end -gt $start) {
        return $trimmed.Substring($start, $end - $start + 1)
    }

    throw "Could not find a JSON object in the provided text."
}

function Count-ScheduleRows {
    param([object]$Payload)

    $rows = 0
    $estimated = 0
    $startDefault = 0
    $estimatedEnd = 0
    $classId = 0
    $totalTrips = 0

    foreach ($ring in @($Payload.rings)) {
        foreach ($class in @($ring.classes)) {
            $rows++
            if (-not [string]::IsNullOrWhiteSpace([string]$class.estimated_start_time)) { $estimated++ }
            if (-not [string]::IsNullOrWhiteSpace([string]$class.start_time_default)) { $startDefault++ }
            if (-not [string]::IsNullOrWhiteSpace([string]$class.estimated_end_time)) { $estimatedEnd++ }
            if ($null -ne $class.class_id) { $classId++ }
            if ($null -ne $class.total_trips) { $totalTrips++ }
        }
    }

    return [pscustomobject]@{
        rows = $rows
        estimated_start_time = $estimated
        start_time_default = $startDefault
        estimated_end_time = $estimatedEnd
        class_id = $classId
        total_trips = $totalTrips
    }
}

if ($InputPath) {
    $resolvedInputPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($InputPath)
    $raw = Get-Content -LiteralPath $resolvedInputPath -Raw
} else {
    $raw = Get-Clipboard -Raw
}

$jsonText = Get-JsonText -Text $raw
$payload = $jsonText | ConvertFrom-Json

$payloadShowId = [int]$payload.show.show_id
$payloadShowDate = [string]$payload.show_date
if (-not $payloadShowId) {
    throw "Payload is missing show.show_id."
}
if ([string]::IsNullOrWhiteSpace($payloadShowDate)) {
    throw "Payload is missing show_date."
}

if ($ShowId -and $ShowId -ne $payloadShowId) {
    throw "Payload show_id $payloadShowId does not match expected ShowId $ShowId."
}
if ($ShowDate -and $ShowDate -ne $payloadShowDate) {
    throw "Payload show_date $payloadShowDate does not match expected ShowDate $ShowDate."
}

$stats = Count-ScheduleRows -Payload $payload
if ($stats.rows -le 0) {
    throw "Payload has no schedule class rows."
}
if ($stats.estimated_start_time -le 0) {
    throw "Payload has no estimated_start_time values; refusing to save as full schedule payload."
}

$epoch = [int64][Math]::Floor(([DateTimeOffset]::UtcNow).ToUnixTimeSeconds())
$fileName = "schedule_${payloadShowDate}_show_id_${payloadShowId}_${epoch}.json"

New-Item -ItemType Directory -Path $Destination -Force | Out-Null
$outPath = Join-Path $Destination $fileName
[System.IO.File]::WriteAllText($outPath, $jsonText, [System.Text.UTF8Encoding]::new($false))

$repoCopyPath = $null
if ($AlsoCopyToRepo) {
    $repoDir = Join-Path $PSScriptRoot "manual_sgl_payloads\schedule-preview"
    New-Item -ItemType Directory -Path $repoDir -Force | Out-Null
    $repoCopyPath = Join-Path $repoDir $fileName
    [System.IO.File]::WriteAllText($repoCopyPath, $jsonText, [System.Text.UTF8Encoding]::new($false))
}

[pscustomobject]@{
    ok = $true
    output_path = $outPath
    repo_copy_path = $repoCopyPath
    show_id = $payloadShowId
    show_date = $payloadShowDate
    rows = $stats.rows
    estimated_start_time = $stats.estimated_start_time
    start_time_default = $stats.start_time_default
    estimated_end_time = $stats.estimated_end_time
    class_id = $stats.class_id
    total_trips = $stats.total_trips
} | ConvertTo-Json -Depth 4
