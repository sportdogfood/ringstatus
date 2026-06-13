param(
  [string]$SiteId = "6982268b7543ac3c80151266",
  [string]$RedirectUri = "http://localhost:8789/callback",
  [string]$TokenOut = "$env:APPDATA\webflow\custom-code-token.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Web

$clientId = Read-Host "Webflow Data Client App client_id"
$clientSecretSecure = Read-Host "Webflow Data Client App client_secret" -AsSecureString
$clientSecret = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
  [Runtime.InteropServices.Marshal]::SecureStringToBSTR($clientSecretSecure)
)

if (!$clientId -or !$clientSecret) {
  throw "client_id and client_secret are required"
}

$scopes = @(
  "authorized_user:read",
  "sites:read",
  "sites:write",
  "pages:read",
  "pages:write",
  "custom_code:read",
  "custom_code:write"
) -join " "

$stateBytes = New-Object byte[] 16
[Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($stateBytes)
$state = [Convert]::ToBase64String($stateBytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")

$authUrl = "https://webflow.com/oauth/authorize" +
  "?response_type=code" +
  "&client_id=$([System.Web.HttpUtility]::UrlEncode($clientId))" +
  "&redirect_uri=$([System.Web.HttpUtility]::UrlEncode($RedirectUri))" +
  "&scope=$([System.Web.HttpUtility]::UrlEncode($scopes))" +
  "&state=$([System.Web.HttpUtility]::UrlEncode($state))"

$listener = [System.Net.HttpListener]::new()
$prefix = $RedirectUri
if (!$prefix.EndsWith("/")) { $prefix = "$prefix/" }
$listener.Prefixes.Add($prefix)

Write-Host ""
Write-Host "Required Webflow App redirect URI:"
Write-Host "  $RedirectUri"
Write-Host ""
Write-Host "Opening OAuth approval page..."
Write-Host "Approve the app for RingStatus, then return here."

$listener.Start()
Start-Process $authUrl

$context = $listener.GetContext()
$request = $context.Request
$response = $context.Response

$code = $request.QueryString["code"]
$returnedState = $request.QueryString["state"]
$errorValue = $request.QueryString["error"]
$errorDescription = $request.QueryString["error_description"]

$html = "<html><body><h2>Webflow OAuth received.</h2><p>You can return to PowerShell.</p></body></html>"
$buffer = [Text.Encoding]::UTF8.GetBytes($html)
$response.ContentType = "text/html"
$response.ContentLength64 = $buffer.Length
$response.OutputStream.Write($buffer, 0, $buffer.Length)
$response.OutputStream.Close()
$listener.Stop()

if ($errorValue) {
  throw "Webflow OAuth failed: $errorValue $errorDescription"
}
if (!$code) {
  throw "No authorization code returned"
}
if ($returnedState -ne $state) {
  throw "OAuth state mismatch"
}

$tokenBody = @{
  grant_type = "authorization_code"
  client_id = $clientId
  client_secret = $clientSecret
  code = $code
  redirect_uri = $RedirectUri
}

try {
  $token = Invoke-RestMethod `
    -Method Post `
    -Uri "https://api.webflow.com/oauth/access_token" `
    -ContentType "application/json" `
    -Body ($tokenBody | ConvertTo-Json -Compress) `
    -TimeoutSec 60
} catch {
  $form = "client_id=$([System.Web.HttpUtility]::UrlEncode($clientId))" +
    "&client_secret=$([System.Web.HttpUtility]::UrlEncode($clientSecret))" +
    "&grant_type=authorization_code" +
    "&code=$([System.Web.HttpUtility]::UrlEncode($code))" +
    "&redirect_uri=$([System.Web.HttpUtility]::UrlEncode($RedirectUri))"

  $token = Invoke-RestMethod `
    -Method Post `
    -Uri "https://api.webflow.com/oauth/access_token" `
    -ContentType "application/x-www-form-urlencoded" `
    -Body $form `
    -TimeoutSec 60
}

$accessToken = $token.access_token
if (!$accessToken) {
  throw "No access_token returned from Webflow"
}

$tokenDir = Split-Path -Parent $TokenOut
if (!(Test-Path $tokenDir)) {
  New-Item -ItemType Directory -Force -Path $tokenDir | Out-Null
}

[pscustomobject]@{
  created_at = (Get-Date).ToString("o")
  site_id = $SiteId
  redirect_uri = $RedirectUri
  scopes_requested = $scopes
  access_token = $accessToken
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $TokenOut -Encoding UTF8

Write-Host ""
Write-Host "Token saved:"
Write-Host "  $TokenOut"
Write-Host ""
Write-Host "Testing custom_code:read..."

try {
  $customCode = Invoke-RestMethod `
    -Uri "https://api.webflow.com/v2/sites/$SiteId/custom_code" `
    -Headers @{ Authorization = "Bearer $accessToken" } `
    -TimeoutSec 60

  $count = @($customCode.scripts).Count
  Write-Host "PASS: custom_code:read works. Applied script count: $count"
  Write-Host "Next: Codex can use this token file for Webflow custom-code API writes."
} catch {
  $status = $null
  if ($_.Exception.Response) { $status = [int]$_.Exception.Response.StatusCode }
  $errorBody = ""
  if ($_.ErrorDetails.Message) { $errorBody = $_.ErrorDetails.Message }
  if ($status -eq 404 -and $errorBody -match "Custom code block not found") {
    Write-Host "PASS: custom_code:read reached Webflow. No custom-code block exists yet."
    Write-Host "Next: Codex can use this token file for Webflow custom-code API writes."
    exit 0
  }
  Write-Host "FAIL: custom_code:read failed. status=$status"
  Write-Host $_.Exception.Message
  if ($errorBody) { Write-Host $errorBody }
  Write-Host "Check that the Webflow App is a Data Client app and includes custom_code:read and custom_code:write."
  exit 1
}
