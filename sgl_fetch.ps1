param(
    [Parameter(Mandatory = $true)]
    [string]$Url,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath,

    [string]$SessionJsonPath = $(if ($env:SGL_FETCH_SESSION_JSON) { $env:SGL_FETCH_SESSION_JSON } else { 'C:\actions-runner\ringstatus\sgl_fetch_session.json' }),

    [int]$TimeoutSec = $(if ($env:SGL_FETCH_TIMEOUT_SEC) { [int]$env:SGL_FETCH_TIMEOUT_SEC } else { 30 })
)

$ErrorActionPreference = 'Stop'

function Get-ConfigValue {
    param([string]$Name)

    foreach ($scope in @('Process', 'User', 'Machine')) {
        $value = [Environment]::GetEnvironmentVariable($Name, $scope)
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $value
        }
    }

    return $null
}

function ConvertTo-PlainHashtable {
    param([object]$Value)

    $out = @{}
    if (-not $Value) {
        return $out
    }

    foreach ($property in $Value.PSObject.Properties) {
        $out[$property.Name] = $property.Value
    }
    return $out
}

function Add-SessionCookie {
    param(
        [Microsoft.PowerShell.Commands.WebRequestSession]$Session,
        [string]$Name,
        [string]$Value,
        [string]$Domain,
        [string]$Path = '/'
    )

    if ([string]::IsNullOrWhiteSpace($Name)) {
        return
    }

    if ($null -eq $Value) {
        $Value = ''
    }

    if ([string]::IsNullOrWhiteSpace($Domain)) {
        $Domain = 'sglapi.wellingtoninternational.com'
    }

    if ([string]::IsNullOrWhiteSpace($Path)) {
        $Path = '/'
    }

    $Session.Cookies.Add((New-Object System.Net.Cookie($Name, $Value, $Path, $Domain)))
}

function Add-CookieHeaderPairs {
    param(
        [Microsoft.PowerShell.Commands.WebRequestSession]$Session,
        [string]$CookieHeader
    )

    if ([string]::IsNullOrWhiteSpace($CookieHeader)) {
        return
    }

    foreach ($part in ($CookieHeader -split ';')) {
        $pair = $part.Trim()
        if (-not $pair -or -not $pair.Contains('=')) {
            continue
        }

        $name, $value = $pair.Split('=', 2)
        $name = $name.Trim()
        $value = $value.Trim()
        if (-not $name) {
            continue
        }

        Add-SessionCookie -Session $Session -Name $name -Value $value -Domain 'sglapi.wellingtoninternational.com'
        Add-SessionCookie -Session $Session -Name $name -Value $value -Domain '.wellingtoninternational.com'
    }
}

function Add-HeaderIfValue {
    param(
        [hashtable]$Headers,
        [string]$Name,
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Name) -or [string]::IsNullOrWhiteSpace($Value)) {
        return
    }

    $blocked = @(
        'authority',
        'method',
        'path',
        'scheme',
        'host',
        'content-length',
        'connection'
    )

    if ($Name.StartsWith(':') -or $blocked -contains $Name.ToLowerInvariant()) {
        return
    }

    $Headers[$Name] = $Value
}

$sessionJsonUsed = $false
$cookieHeaderUsed = $false
$authorizationUsed = $false
$userAgentOverrideUsed = $false
$clientHintsOverrideUsed = $false
$recaptchaUsed = $false
$config = $null

try {
    $resolvedOutputPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputPath)

    $sessionJsonOverride = Get-ConfigValue -Name 'SGL_FETCH_SESSION_JSON'
    if (-not [string]::IsNullOrWhiteSpace($sessionJsonOverride)) {
        $SessionJsonPath = $sessionJsonOverride
    }

    if (-not [string]::IsNullOrWhiteSpace($SessionJsonPath) -and (Test-Path -LiteralPath $SessionJsonPath)) {
        $config = Get-Content -LiteralPath $SessionJsonPath -Raw | ConvertFrom-Json
        $sessionJsonUsed = $true
    }

    $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
    $session.UserAgent = Get-ConfigValue -Name 'SGL_USER_AGENT'
    if (-not [string]::IsNullOrWhiteSpace($session.UserAgent)) {
        $userAgentOverrideUsed = $true
    }
    if ([string]::IsNullOrWhiteSpace($session.UserAgent) -and $config -and $config.userAgent) {
        $session.UserAgent = [string]$config.userAgent
        $userAgentOverrideUsed = $true
    }
    if ([string]::IsNullOrWhiteSpace($session.UserAgent)) {
        $session.UserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0'
    }

    if ($config -and $config.cookies) {
        foreach ($cookie in @($config.cookies)) {
            Add-SessionCookie `
                -Session $session `
                -Name ([string]$cookie.name) `
                -Value ([string]$cookie.value) `
                -Domain ([string]$cookie.domain) `
                -Path ([string]$cookie.path)
        }
    }

    $cookieHeader = Get-ConfigValue -Name 'SGL_COOKIE_HEADER'
    if (-not [string]::IsNullOrWhiteSpace($cookieHeader)) {
        Add-CookieHeaderPairs -Session $session -CookieHeader $cookieHeader
        $cookieHeaderUsed = $true
    }

    $headers = @{
        'accept' = 'application/json, text/plain, */*'
        'accept-language' = 'en-US,en;q=0.9'
        'cache-control' = 'no-cache'
        'expires' = '0'
        'origin' = 'https://www.wellingtoninternational.com'
        'pragma' = 'no-cache'
        'referer' = 'https://www.wellingtoninternational.com/'
        'sec-ch-ua' = '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"'
        'sec-ch-ua-mobile' = '?0'
        'sec-ch-ua-platform' = '"Windows"'
        'sec-fetch-dest' = 'empty'
        'sec-fetch-mode' = 'cors'
        'sec-fetch-site' = 'same-site'
        'sgl-request-origin' = 'SGL-API'
    }

    $secChUa = Get-ConfigValue -Name 'SGL_SEC_CH_UA'
    if (-not [string]::IsNullOrWhiteSpace($secChUa)) {
        $headers['sec-ch-ua'] = $secChUa
        $clientHintsOverrideUsed = $true
    }

    $secChUaMobile = Get-ConfigValue -Name 'SGL_SEC_CH_UA_MOBILE'
    if (-not [string]::IsNullOrWhiteSpace($secChUaMobile)) {
        $headers['sec-ch-ua-mobile'] = $secChUaMobile
        $clientHintsOverrideUsed = $true
    }

    $secChUaPlatform = Get-ConfigValue -Name 'SGL_SEC_CH_UA_PLATFORM'
    if (-not [string]::IsNullOrWhiteSpace($secChUaPlatform)) {
        $headers['sec-ch-ua-platform'] = $secChUaPlatform
        $clientHintsOverrideUsed = $true
    }

    $recaptchaToken = Get-ConfigValue -Name 'SGL_RECAPTCHA_TOKEN'
    if ([string]::IsNullOrWhiteSpace($recaptchaToken)) {
        $recaptchaToken = Get-ConfigValue -Name 'SGL_X_RECAPTCHA_TOKEN'
    }
    if (-not [string]::IsNullOrWhiteSpace($recaptchaToken)) {
        $headers['x-recaptcha-token'] = $recaptchaToken
        $recaptchaUsed = $true
    }

    $authorization = Get-ConfigValue -Name 'SGL_AUTHORIZATION'
    $bearerToken = Get-ConfigValue -Name 'SGL_BEARER_TOKEN'
    if ([string]::IsNullOrWhiteSpace($authorization) -and -not [string]::IsNullOrWhiteSpace($bearerToken)) {
        $authorization = "Bearer $bearerToken"
    }
    if ([string]::IsNullOrWhiteSpace($authorization) -and $config -and $config.authorization) {
        $authorization = [string]$config.authorization
    }
    if (-not [string]::IsNullOrWhiteSpace($authorization)) {
        if ($authorization -notmatch '^\s*Bearer\s+') {
            $authorization = "Bearer $authorization"
        }
        $headers['authorization'] = $authorization
        $authorizationUsed = $true
    }

    if ($config -and $config.headers) {
        foreach ($item in (ConvertTo-PlainHashtable -Value $config.headers).GetEnumerator()) {
            Add-HeaderIfValue -Headers $headers -Name ([string]$item.Key) -Value ([string]$item.Value)
        }
    }

    $outputDirectory = Split-Path -Parent $resolvedOutputPath
    if (-not [string]::IsNullOrWhiteSpace($outputDirectory) -and -not (Test-Path -LiteralPath $outputDirectory)) {
        New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
    }

    $response = Invoke-WebRequest `
        -UseBasicParsing `
        -Uri $Url `
        -WebSession $session `
        -Headers $headers `
        -TimeoutSec $TimeoutSec

    $content = [string]$response.Content
    [System.IO.File]::WriteAllText(
        $resolvedOutputPath,
        $content,
        [System.Text.UTF8Encoding]::new($false)
    )

    $contentLength = $null
    if ($response.Headers.ContainsKey('Content-Length')) {
        $contentLength = [int64]([string]$response.Headers['Content-Length'])
    }

    $meta = [ordered]@{
        ok = $true
        transport = 'powershell_iwr'
        url = $Url
        output_path = $resolvedOutputPath
        status_code = [int]$response.StatusCode
        content_length = $contentLength
        raw_content_length = [int64]$response.RawContentLength
        body_length = [System.Text.Encoding]::UTF8.GetByteCount($content)
        session_json_used = $sessionJsonUsed
        cookie_header_used = $cookieHeaderUsed
        authorization_used = $authorizationUsed
        user_agent_override_used = $userAgentOverrideUsed
        client_hints_override_used = $clientHintsOverrideUsed
        recaptcha_used = $recaptchaUsed
    }

    $meta | ConvertTo-Json -Compress -Depth 5
}
catch {
    $meta = [ordered]@{
        ok = $false
        transport = 'powershell_iwr'
        url = $Url
        output_path = $OutputPath
        error = [string]$_.Exception.Message
        session_json_used = $sessionJsonUsed
        cookie_header_used = $cookieHeaderUsed
        authorization_used = $authorizationUsed
        user_agent_override_used = $userAgentOverrideUsed
        client_hints_override_used = $clientHintsOverrideUsed
        recaptcha_used = $recaptchaUsed
    }
    $meta | ConvertTo-Json -Compress -Depth 5
    exit 1
}
