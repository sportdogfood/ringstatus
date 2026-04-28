param(
    [string]$TaskName = 'epoch-tagger-local',
    [switch]$DoNotEnable,
    [string]$RunnerPath = 'C:\ringstatus-task\run_tagger_task.cmd'
)

$ErrorActionPreference = 'Stop'

<#
Run this file as a script, not by pasting it line-by-line into an interactive
PowerShell prompt. If you need notes, use PowerShell comment syntax only, not
pasted console output.
#>

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Restart-Elevated {
    param(
        [string]$ScriptPath,
        [string]$TaskNameArg,
        [bool]$DoNotEnableArg,
        [string]$RunnerPathArg
    )

    $argList = @(
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', ('"{0}"' -f $ScriptPath),
        '-TaskName', ('"{0}"' -f $TaskNameArg),
        '-RunnerPath', ('"{0}"' -f $RunnerPathArg)
    )

    if ($DoNotEnableArg) {
        $argList += '-DoNotEnable'
    }

    Start-Process powershell -Verb RunAs -ArgumentList $argList
}

function Show-TaskSummary {
    param($Task)

    $task | Select-Object TaskName, TaskPath, State | Format-Table -AutoSize
    $Task.Settings | Format-List MultipleInstances, Enabled, ExecutionTimeLimit, Hidden, Priority
    $Task.Triggers | Format-List Enabled, StartBoundary, Repetition
    $Task.Actions | Format-List Execute, Arguments, WorkingDirectory
}

function ConvertTo-TaskTimeSpan {
    param([object]$Value)

    if ($null -eq $Value) {
        return $null
    }

    if ($Value -is [TimeSpan]) {
        return $Value
    }

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    try {
        return [System.Xml.XmlConvert]::ToTimeSpan($text)
    } catch {
        try {
            return [TimeSpan]::Parse($text)
        } catch {
            throw "Unable to parse task duration '$text' as TimeSpan."
        }
    }
}

function ConvertTo-NormalizedPathText {
    param([object]$Value)

    if ($null -eq $Value) {
        return ''
    }

    return ([string]$Value).Trim().Trim('"')
}

function Resolve-RunnerPathValue {
    param([string]$PathText)

    if ([string]::IsNullOrWhiteSpace($PathText)) {
        throw "RunnerPath cannot be blank."
    }

    return [System.IO.Path]::GetFullPath($PathText)
}

function New-DesiredTaskAction {
    param([string]$ResolvedRunnerPath)

    if (-not (Test-Path $ResolvedRunnerPath)) {
        throw "Runner path not found: $ResolvedRunnerPath"
    }

    $runnerDirectory = Split-Path -Parent $ResolvedRunnerPath
    $arguments = "/d /c `"$ResolvedRunnerPath`""

    return New-ScheduledTaskAction `
        -Execute 'C:\Windows\System32\cmd.exe' `
        -Argument $arguments `
        -WorkingDirectory $runnerDirectory
}

function Test-TaskActionMatchesRunner {
    param(
        $Task,
        [string]$ResolvedRunnerPath
    )

    $actions = @($Task.Actions)
    if ($actions.Count -ne 1) {
        return $false
    }

    $action = $actions[0]
    $expectedExecute = 'C:\Windows\System32\cmd.exe'
    $expectedArguments = "/d /c `"$ResolvedRunnerPath`""
    $expectedWorkingDirectory = Split-Path -Parent $ResolvedRunnerPath

    $actualExecute = ConvertTo-NormalizedPathText $action.Execute
    $actualArguments = ([string]$action.Arguments).Trim()
    $actualWorkingDirectory = ConvertTo-NormalizedPathText $action.WorkingDirectory

    return (
        [string]::Equals($actualExecute, $expectedExecute, [System.StringComparison]::OrdinalIgnoreCase) -and
        [string]::Equals($actualArguments, $expectedArguments, [System.StringComparison]::OrdinalIgnoreCase) -and
        [string]::Equals($actualWorkingDirectory, $expectedWorkingDirectory, [System.StringComparison]::OrdinalIgnoreCase)
    )
}

function Get-TaskCredentialArgs {
    param($Task)

    $logonType = [string]$Task.Principal.LogonType
    $userId = [string]$Task.Principal.UserId

    if ($logonType -ne 'Password') {
        return @{}
    }

    if ([string]::IsNullOrWhiteSpace($userId)) {
        throw "Task uses Password logon but no task user could be read from the task definition."
    }

    Write-Host "Task '$($Task.TaskName)' runs as '$userId' and requires the Windows account password to update."
    $securePassword = Read-Host -AsSecureString "Enter the Windows password for '$userId'"
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
    try {
        $plainPassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    } finally {
        if ($bstr -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
        }
    }

    if ([string]::IsNullOrWhiteSpace($plainPassword)) {
        throw "Task password cannot be blank."
    }

    return @{
        User = $userId
        Password = $plainPassword
    }
}

if (-not (Test-IsAdministrator)) {
    Write-Host "Restarting elevated to modify scheduled task '$TaskName'..."
    Restart-Elevated `
        -ScriptPath $PSCommandPath `
        -TaskNameArg $TaskName `
        -DoNotEnableArg $DoNotEnable.IsPresent `
        -RunnerPathArg $RunnerPath
    exit 0
}

$resolvedRunnerPath = Resolve-RunnerPathValue $RunnerPath
$desiredAction = New-DesiredTaskAction -ResolvedRunnerPath $resolvedRunnerPath
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop

Write-Host "Current task settings:"
Show-TaskSummary -Task $task

$queueAlreadySet = [string]$task.Settings.MultipleInstances -eq 'Queue'
$enabledAlreadySet = $DoNotEnable -or [bool]$task.Settings.Enabled
$actionAlreadySet = Test-TaskActionMatchesRunner -Task $task -ResolvedRunnerPath $resolvedRunnerPath

if ($queueAlreadySet -and $enabledAlreadySet -and $actionAlreadySet) {
    Write-Host ""
    Write-Host "Task already matches the requested settings. No changes needed."
    exit 0
}

$executionTimeLimit = ConvertTo-TaskTimeSpan $task.Settings.ExecutionTimeLimit
$credentialArgs = Get-TaskCredentialArgs -Task $task

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances Queue `
    -ExecutionTimeLimit $executionTimeLimit `
    -Hidden:([bool]$task.Settings.Hidden) `
    -Priority ([int]$task.Settings.Priority)

try {
    Set-ScheduledTask `
        -TaskName $TaskName `
        -Action $desiredAction `
        -Settings $settings `
        @credentialArgs | Out-Null
} catch {
    Write-Warning "Set-ScheduledTask failed. Trying re-register path. Error: $($_.Exception.Message)"

    if ($credentialArgs.Count -gt 0) {
        Register-ScheduledTask `
            -TaskName $task.TaskName `
            -TaskPath $task.TaskPath `
            -Action $desiredAction `
            -Trigger $task.Triggers `
            -Settings $settings `
            -User $credentialArgs.User `
            -Password $credentialArgs.Password `
            -RunLevel $task.Principal.RunLevel `
            -Description $task.Description `
            -Force | Out-Null
    } else {
        Register-ScheduledTask `
            -TaskName $task.TaskName `
            -TaskPath $task.TaskPath `
            -Action $desiredAction `
            -Trigger $task.Triggers `
            -Principal $task.Principal `
            -Settings $settings `
            -Description $task.Description `
            -Force | Out-Null
    }
}

if (-not $DoNotEnable) {
    Enable-ScheduledTask -TaskName $TaskName -ErrorAction Stop | Out-Null
}

$updated = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop

Write-Host ""
Write-Host "Updated task settings:"
Show-TaskSummary -Task $updated

Write-Host ""
Write-Host "Expected:"
Write-Host "  MultipleInstances : Queue"
Write-Host "  Enabled           : True"
Write-Host "  Arguments         : /d /c `"$resolvedRunnerPath`""
