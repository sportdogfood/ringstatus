param(
    [string]$TaskName = 'epoch-tagger-local',
    [switch]$DoNotEnable
)

$ErrorActionPreference = 'Stop'

<#
Run this file as a script, not by pasting it line-by-line into an interactive
PowerShell prompt. The `param(...)` block must stay at the top of the script.
#>

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


//
PS C:\WINDOWS\system32> robocopy "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" "C:\ringstatus-task" fix_epoch_tagger_task.ps1

-------------------------------------------------------------------------------
   ROBOCOPY     ::     Robust File Copy for Windows
-------------------------------------------------------------------------------

  Started : Monday, April 27, 2026 8:27:49 PM
   Source : C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\
     Dest : C:\ringstatus-task\

    Files : fix_epoch_tagger_task.ps1

  Options : /DCOPY:DA /COPY:DAT /R:1000000 /W:30

------------------------------------------------------------------------------

                           1    C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\
100%        Newer                   3665        fix_epoch_tagger_task.ps1

------------------------------------------------------------------------------

               Total    Copied   Skipped  Mismatch    FAILED    Extras
    Dirs :         1         0         1         0         0         0
   Files :         1         1         0         0         0         0
   Bytes :     3.5 k     3.5 k         0         0         0         0
   Times :   0:00:00   0:00:00                       0:00:00   0:00:00
   Ended : Monday, April 27, 2026 8:27:49 PM

PS C:\WINDOWS\system32> powershell -ExecutionPolicy Bypass -File "C:\ringstatus-task\fix_epoch_tagger_task.ps1"
Current task settings:

TaskName           TaskPath   State
--------           --------   -----
epoch-tagger-local \        Running




MultipleInstances  : Queue
Enabled            : True
ExecutionTimeLimit : PT72H
Hidden             : False
Priority           : 7





Enabled       : True
StartBoundary : 2026-04-12T18:55:12
Repetition    : MSFT_TaskRepetitionPattern





Execute          : C:\Windows\System32\cmd.exe
Arguments        : /d /c "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"
WorkingDirectory :



WARNING: Set-ScheduledTask failed. Trying re-register path. Error: The user name or password is incorrect.
Register-ScheduledTask : The user name or password is incorrect.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:96 char:5
+     Register-ScheduledTask `
+     ~~~~~~~~~~~~~~~~~~~~~~~~
    + CategoryInfo          : AuthenticationError: (PS_ScheduledTask:Root/Microsoft/...S_ScheduledTask) [Register-Sche
   duledTask], CimException
    + FullyQualifiedErrorId : HRESULT 0x8007052e,Register-ScheduledTask
