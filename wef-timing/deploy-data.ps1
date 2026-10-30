$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$paceName = 'wef-timing-data'
if (Get-ScheduledTask -TaskName $paceName -ErrorAction SilentlyContinue) { throw 'Existing Timing task must be reviewed; refusing replacement' }
if (-not (Test-Path -LiteralPath 'runtime/data/runner.env')) { throw 'Missing private Timing credential' }
if (Test-Path -LiteralPath 'runtime/data/runner.lock') { throw 'Existing Timing lock requires inspection' }
$paceRoot = Split-Path -Parent $PSScriptRoot
$paceProtection = @{}
foreach ($paceDir in @('wef-schedules','wef-live','wef-entries','wef-data','wef-results','wef-messages','wef-publish')) {
    $paceFull = Join-Path $paceRoot $paceDir
    if (Test-Path -LiteralPath $paceFull) {
        Get-ChildItem -LiteralPath $paceFull -File | Where-Object {$_.Extension -in @('.js','.py','.ps1','.vbs')} | ForEach-Object {
            $paceProtection[$_.FullName] = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        }
    }
}
$paceTasks = Get-ScheduledTask | Where-Object {$_.TaskName -like 'wef-*'} | ForEach-Object {
    @{name=$_.TaskName; path=$_.TaskPath; xml=(Export-ScheduledTask -TaskName $_.TaskName -TaskPath $_.TaskPath)}
}
$paceAt = (Get-Date).AddMinutes(2)
$paceAt = Get-Date -Year $paceAt.Year -Month $paceAt.Month -Day $paceAt.Day -Hour $paceAt.Hour -Minute $paceAt.Minute -Second 0
$paceAction = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\wscript.exe" -Argument ('//B //Nologo "' + (Join-Path $PSScriptRoot 'run-hidden-data.vbs') + '"') -WorkingDirectory $PSScriptRoot
$paceTrigger = New-ScheduledTaskTrigger -Once -At $paceAt -RepetitionInterval (New-TimeSpan -Minutes 3)
$pacePrincipal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$paceSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Seconds 175) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -Hidden
$paceTask = New-ScheduledTask -Action $paceAction -Trigger $paceTrigger -Principal $pacePrincipal -Settings $paceSettings -Description 'Ring pace only. Reads existing Data handoff/Live; writes snapshots and checkpoints in wef-timing. Stops itself on failure.'
Register-ScheduledTask -TaskName $paceName -InputObject $paceTask | Out-Null
Export-ScheduledTask -TaskName $paceName | Set-Content -LiteralPath 'data-task.xml' -Encoding utf8
$paceHashes = @{}
Get-ChildItem -LiteralPath $PSScriptRoot -File | Where-Object {$_.Extension -in @('.js','.ps1','.vbs')} | ForEach-Object {$paceHashes[$_.Name]=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash}
@{deployed_at=(Get-Date).ToUniversalTime().ToString('o');first_normal_trigger=$paceAt.ToString('o');files=$paceHashes;protected_files=$paceProtection;existing_tasks=$paceTasks;manual_start=$false} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath 'data-deployment.json' -Encoding utf8
Get-ScheduledTaskInfo -TaskName $paceName | Select-Object NextRunTime,LastTaskResult | ConvertTo-Json
