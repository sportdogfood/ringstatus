$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$opener = Join-Path $repo "open_ringstatus_path.ps1"

if (-not (Test-Path -LiteralPath $opener)) {
    throw "Missing opener script: $opener"
}

$protocolRoot = "HKCU:\Software\Classes\ringstatus"
$commandKey = Join-Path $protocolRoot "shell\open\command"

New-Item -Path $protocolRoot -Force | Out-Null
New-ItemProperty -Path $protocolRoot -Name "(default)" -Value "URL:ringstatus" -PropertyType String -Force | Out-Null
New-ItemProperty -Path $protocolRoot -Name "URL Protocol" -Value "" -PropertyType String -Force | Out-Null

New-Item -Path $commandKey -Force | Out-Null
$command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$opener`" -Target `"%1`""
New-ItemProperty -Path $commandKey -Name "(default)" -Value $command -PropertyType String -Force | Out-Null

[pscustomobject]@{
    ok = $true
    protocol = "ringstatus://payload-folder"
    command = $command
} | ConvertTo-Json -Depth 3
