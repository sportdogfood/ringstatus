param(
    [string]$PeopleId = '8778',
    [string[]]$ShowIds = @('200000061', '200000060'),
    [string]$CustomerId = '15',
    [string]$OutputRoot = '',
    [string[]]$AuthOnlyUrls = @(),
    [switch]$OpenFolder
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $RepoRoot

$fetchScript = Join-Path $RepoRoot 'sgl_fetch.ps1'
if (-not (Test-Path -LiteralPath $fetchScript)) {
    throw "Missing SGL fetch helper: $fetchScript"
}

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $RepoRoot 'tmp\sgl_auth_health'
}
$outDir = Join-Path $OutputRoot (Get-Date -Format 'yyyyMMdd-HHmmss')
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$defaultSessionJson = 'C:\actions-runner\ringstatus\sgl_fetch_session.json'
$authEnvNames = @(
    'SGL_AUTHORIZATION',
    'SGL_BEARER_TOKEN',
    'SGL_COOKIE_HEADER',
    'SGL_FETCH_SESSION_JSON'
)

function Save-EnvValues {
    param([string[]]$Names)

    $saved = @{}
    foreach ($name in $Names) {
        $item = Get-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
        $saved[$name] = if ($item) { $item.Value } else { $null }
    }
    return $saved
}

function Restore-EnvValues {
    param(
        [hashtable]$Saved,
        [string[]]$Names
    )

    foreach ($name in $Names) {
        if ($null -eq $Saved[$name]) {
            Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
        } else {
            Set-Item -LiteralPath "Env:$name" -Value $Saved[$name]
        }
    }
}

function Test-AuthConfigured {
    foreach ($name in $authEnvNames) {
        $value = [Environment]::GetEnvironmentVariable($name, 'Process')
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $true
        }
    }
    return (Test-Path -LiteralPath $defaultSessionJson)
}

function Invoke-SglSampleFetch {
    param(
        [string]$Name,
        [string]$Url,
        [string]$Mode,
        [string]$Directory
    )

    $rawPath = Join-Path $Directory "$Name.$Mode.json"
    $prettyPath = Join-Path $Directory "$Name.$Mode.pretty.json"

    if ($Mode -eq 'public') {
        $saved = Save-EnvValues -Names $authEnvNames
        try {
            foreach ($name in $authEnvNames) {
                Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
            }
            $meta = & $fetchScript -Url $Url -OutputPath $rawPath -SessionJsonPath '' | ConvertFrom-Json
        } finally {
            Restore-EnvValues -Saved $saved -Names $authEnvNames
        }
    } else {
        $meta = & $fetchScript -Url $Url -OutputPath $rawPath | ConvertFrom-Json
    }

    $json = $null
    $jsonError = $null
    try {
        $jsonText = Get-Content -LiteralPath $rawPath -Raw
        $json = $jsonText | ConvertFrom-Json
        $json | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $prettyPath -Encoding UTF8
    } catch {
        $jsonError = [string]$_.Exception.Message
    }

    $keys = if ($json) { @($json.PSObject.Properties.Name) } else { @() }
    $payloadError = if ($json -and $json.PSObject.Properties.Name -contains 'error') { [string]$json.error } else { $null }
    $unauthorizedText = ($payloadError -match '(?i)unauthor|forbidden|token|jwt|bearer|session|login|expired')

    return [pscustomobject]@{
        mode = $Mode
        ok = [bool]$meta.ok
        status_code = $meta.status_code
        body_length = $meta.body_length
        raw_content_length = $meta.raw_content_length
        empty_object = ($keys.Count -eq 0)
        payload_error = $payloadError
        unauthorized_text = [bool]$unauthorizedText
        session_json_used = [bool]$meta.session_json_used
        cookie_header_used = [bool]$meta.cookie_header_used
        authorization_used = [bool]$meta.authorization_used
        json_error = $jsonError
        json = $json
        raw_file = $rawPath
        pretty_file = $prettyPath
    }
}

function Test-SglPayloadHealthy {
    param(
        [object]$Probe,
        [string]$Family,
        [string]$ExpectedPeopleId,
        [string]$ExpectedShowId
    )

    if (-not $Probe.ok) { return $false }
    if ($Probe.status_code -eq 401 -or $Probe.status_code -eq 403) { return $false }
    if ($Probe.empty_object) { return $false }
    if ($Probe.unauthorized_text) { return $false }
    if ($Probe.payload_error) { return $false }

    $json = $Probe.json
    if (-not $json) { return $false }

    if ($Family -eq 'people') {
        $returnedPeopleId = $json.people.people_id
        if ([string]$returnedPeopleId -ne [string]$ExpectedPeopleId) { return $false }
        if (-not ($json.PSObject.Properties.Name -contains 'trips')) { return $false }
        return $true
    }

    if ($Family -eq 'auth_only') {
        return $Probe.body_length -gt 100
    }

    return $Probe.body_length -gt 100
}

function Get-SglAuthHealthStatus {
    param(
        [object]$PublicProbe,
        [object]$AuthProbe,
        [bool]$AuthConfigured,
        [bool]$PublicHealthy,
        [bool]$AuthHealthy,
        [string]$Family
    )

    if (-not $AuthConfigured -and $Family -ne 'auth_only') {
        if ($PublicHealthy) { return 'AUTH_NOT_CONFIGURED' }
        return 'ENDPOINT_CHANGED_OR_SCOPE_BAD'
    }

    if ($AuthProbe.status_code -eq 401 -or $AuthProbe.status_code -eq 403 -or $AuthProbe.unauthorized_text) {
        return 'AUTH_STALE_OR_FORBIDDEN'
    }

    if ($PublicProbe -and $PublicHealthy -and -not $AuthHealthy) {
        return 'PUBLIC_OK_AUTH_BAD'
    }

    if ((($PublicProbe -and $PublicProbe.empty_object) -or ($PublicProbe -and $PublicProbe.body_length -le 2)) -and
        (($AuthProbe -and $AuthProbe.empty_object) -or ($AuthProbe -and $AuthProbe.body_length -le 2))) {
        return 'SOFT_THROTTLE'
    }

    if ($AuthHealthy) {
        return 'AUTH_OK'
    }

    return 'ENDPOINT_CHANGED_OR_SCOPE_BAD'
}

$authConfigured = Test-AuthConfigured
$checks = @()

foreach ($showId in $ShowIds) {
    $checks += [pscustomobject]@{
        name = "people_$PeopleId`_$showId"
        family = 'people'
        url = "https://sglapi.wellingtoninternational.com/people/$PeopleId`?pid=$PeopleId&show_id=$showId&customer_id=$CustomerId"
        expected_people_id = $PeopleId
        expected_show_id = $showId
        compare_public = $true
    }
}

$envAuthOnlyUrls = [Environment]::GetEnvironmentVariable('SGL_AUTH_CHECK_URLS', 'Process')
if (-not [string]::IsNullOrWhiteSpace($envAuthOnlyUrls)) {
    $AuthOnlyUrls += @($envAuthOnlyUrls -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
}

$index = 0
foreach ($url in $AuthOnlyUrls) {
    $index += 1
    $checks += [pscustomobject]@{
        name = "auth_only_$index"
        family = 'auth_only'
        url = $url
        expected_people_id = ''
        expected_show_id = ''
        compare_public = $false
    }
}

$results = foreach ($check in $checks) {
    Write-Host ""
    Write-Host "CHECK $($check.name)"

    $publicProbe = $null
    $publicHealthy = $false
    if ($check.compare_public) {
        $publicProbe = Invoke-SglSampleFetch -Name $check.name -Url $check.url -Mode 'public' -Directory $outDir
        $publicHealthy = Test-SglPayloadHealthy `
            -Probe $publicProbe `
            -Family $check.family `
            -ExpectedPeopleId $check.expected_people_id `
            -ExpectedShowId $check.expected_show_id
    }

    $authProbe = Invoke-SglSampleFetch -Name $check.name -Url $check.url -Mode 'auth' -Directory $outDir
    $authHealthy = Test-SglPayloadHealthy `
        -Probe $authProbe `
        -Family $check.family `
        -ExpectedPeopleId $check.expected_people_id `
        -ExpectedShowId $check.expected_show_id

    $status = Get-SglAuthHealthStatus `
        -PublicProbe $publicProbe `
        -AuthProbe $authProbe `
        -AuthConfigured $authConfigured `
        -PublicHealthy $publicHealthy `
        -AuthHealthy $authHealthy `
        -Family $check.family

    [pscustomobject]@{
        name = $check.name
        status = $status
        auth_configured = $authConfigured
        public_ok = if ($publicProbe) { $publicProbe.ok } else { $null }
        public_healthy = if ($publicProbe) { $publicHealthy } else { $null }
        public_body = if ($publicProbe) { $publicProbe.body_length } else { $null }
        auth_ok = $authProbe.ok
        auth_healthy = $authHealthy
        auth_body = $authProbe.body_length
        auth_status_code = $authProbe.status_code
        auth_session_json = $authProbe.session_json_used
        auth_cookie_header = $authProbe.cookie_header_used
        auth_authorization = $authProbe.authorization_used
        payload_error = $authProbe.payload_error
        raw_public = if ($publicProbe) { $publicProbe.raw_file } else { $null }
        raw_auth = $authProbe.raw_file
    }
}

Write-Host ""
Write-Host "SGL auth/session health"
$results | Format-Table -AutoSize

Write-Host ""
Write-Host "Status guide:"
Write-Host "  AUTH_OK                  Auth/session probe returned a healthy payload."
Write-Host "  AUTH_NOT_CONFIGURED      No bearer/cookie/session was detected; public probe is healthy."
Write-Host "  PUBLIC_OK_AUTH_BAD       Public probe is healthy but auth/session probe is not; refresh or remove auth values."
Write-Host "  AUTH_STALE_OR_FORBIDDEN  Auth/session probe returned 401/403 or unauthorized-style payload."
Write-Host "  SOFT_THROTTLE            Both probes returned an empty/tiny payload."
Write-Host "  ENDPOINT_CHANGED_OR_SCOPE_BAD Expected payload shape did not match."

Write-Host ""
Write-Host "Files written to:"
Write-Host $outDir

if ($OpenFolder) {
    Invoke-Item -LiteralPath $outDir
}
