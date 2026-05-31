param(
    [string]$Target = "payload-folder"
)

$ErrorActionPreference = "Stop"

$paths = @{
    "payload-folder" = "C:\actions-runner\ringstatus\manual_sgl_payloads"
}

$key = [string]$Target
if ($key -match '^ringstatus://') {
    $key = $key -replace '^ringstatus://', ''
}
$key = $key.Trim().TrimEnd('/')

if (-not $paths.ContainsKey($key)) {
    throw "Unknown ringstatus path target: $Target"
}

$path = $paths[$key]
New-Item -ItemType Directory -Path $path -Force | Out-Null
Start-Process explorer.exe -ArgumentList @($path)
