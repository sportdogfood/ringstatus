$ErrorActionPreference = 'Stop'

param(
    [string]$TaskName = 'epoch-tagger-local',
    [switch]$DoNotEnable
)

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Restart-Elevated {
    param([string]$ScriptPath, [string]$TaskNameArg, [bool]$DoNotEnableArg)

    $argList = @(
        '-NoProfile'
        '-ExecutionPolicy', 'Bypass'
        '-File', ('"{0}"' -f $ScriptPath)
        '-TaskName', ('"{0}"' -f $TaskNameArg)
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

function Convert-ToTaskTimeSpan {
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

if (-not (Test-IsAdministrator)) {
    Write-Host "Restarting elevated to modify scheduled task '$TaskName'..."
    Restart-Elevated -ScriptPath $PSCommandPath -TaskNameArg $TaskName -DoNotEnableArg $DoNotEnable.IsPresent
    exit 0
}

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop

Write-Host "Current task settings:"
Show-TaskSummary -Task $task

$executionTimeLimit = Convert-ToTaskTimeSpan $task.Settings.ExecutionTimeLimit

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances Queue `
    -ExecutionTimeLimit $executionTimeLimit `
    -Hidden:([bool]$task.Settings.Hidden) `
    -Priority ([int]$task.Settings.Priority)

try {
    Set-ScheduledTask -TaskName $TaskName -Settings $settings | Out-Null
} catch {
    Write-Warning "Set-ScheduledTask failed. Trying re-register path. Error: $($_.Exception.Message)"

    Register-ScheduledTask `
        -TaskName $task.TaskName `
        -TaskPath $task.TaskPath `
        -Action $task.Actions `
        -Trigger $task.Triggers `
        -Principal $task.Principal `
        -Settings $settings `
        -Force | Out-Null
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
