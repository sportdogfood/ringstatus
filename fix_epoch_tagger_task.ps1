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
    Restart-Elevated -ScriptPath $PSCommandPath -TaskNameArg $TaskName -DoNotEnableArg $DoNotEnable.IsPresent
    exit 0
}

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop

Write-Host "Current task settings:"
Show-TaskSummary -Task $task

$queueAlreadySet = [string]$task.Settings.MultipleInstances -eq 'Queue'
$enabledAlreadySet = $DoNotEnable -or [bool]$task.Settings.Enabled

if ($queueAlreadySet -and $enabledAlreadySet) {
    Write-Host ""
    Write-Host "Task already matches the requested settings. No changes needed."
    exit 0
}

$executionTimeLimit = Convert-ToTaskTimeSpan $task.Settings.ExecutionTimeLimit
$credentialArgs = Get-TaskCredentialArgs -Task $task

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances Queue `
    -ExecutionTimeLimit $executionTimeLimit `
    -Hidden:([bool]$task.Settings.Hidden) `
    -Priority ([int]$task.Settings.Priority)

try {
    Set-ScheduledTask -TaskName $TaskName -Settings $settings @credentialArgs | Out-Null
} catch {
    Write-Warning "Set-ScheduledTask failed. Trying re-register path. Error: $($_.Exception.Message)"

    if ($credentialArgs.Count -gt 0) {
        Register-ScheduledTask `
            -TaskName $task.TaskName `
            -TaskPath $task.TaskPath `
            -Action $task.Actions `
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
            -Action $task.Actions `
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

//
//PS C:\WINDOWS\system32> robocopy "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" "C:\ringstatus-task" fix_epoch_tagger_task.ps1

-------------------------------------------------------------------------------
   ROBOCOPY     ::     Robust File Copy for Windows
-------------------------------------------------------------------------------

  Started : Monday, April 27, 2026 8:39:40 PM
   Source : C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\
     Dest : C:\ringstatus-task\

    Files : fix_epoch_tagger_task.ps1

  Options : /DCOPY:DA /COPY:DAT /R:1000000 /W:30

------------------------------------------------------------------------------

                           1    C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\
100%        Newer                   6043        fix_epoch_tagger_task.ps1

------------------------------------------------------------------------------

               Total    Copied   Skipped  Mismatch    FAILED    Extras
    Dirs :         1         0         1         0         0         0
   Files :         1         1         0         0         0         0
   Bytes :     5.9 k     5.9 k         0         0         0         0
   Times :   0:00:00   0:00:00                       0:00:00   0:00:00
   Ended : Monday, April 27, 2026 8:39:40 PM

PS C:\WINDOWS\system32> Get-Item "C:\ringstatus-task\fix_epoch_tagger_task.ps1" | Select-Object FullName,Length,LastWriteTime

FullName                                     Length LastWriteTime
--------                                     ------ -------------
C:\ringstatus-task\fix_epoch_tagger_task.ps1   6043 4/27/2026 8:29:54 PM


PS C:\WINDOWS\system32> powershell -ExecutionPolicy Bypass -File "C:\ringstatus-task\fix_epoch_tagger_task.ps1"
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:125 char:80
+ ... ---------------------------------------------------------------------
+                                                                          ~
Missing expression after unary operator '-'.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:126 char:4
+    ROBOCOPY     ::     Robust File Copy for Windows
+    ~~~~~~~~
Unexpected token 'ROBOCOPY' in expression or statement.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:127 char:80
+ ... ---------------------------------------------------------------------
+                                                                          ~
Missing expression after unary operator '-'.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:129 char:3
+   Started : Monday, April 27, 2026 8:27:49 PM
+   ~~~~~~~
Unexpected token 'Started' in expression or statement.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:139 char:33
+                            1    C:\Users\gombc\OneDrive - Sport Dog F ...
+                                 ~~~~~~~~~~~~~~~~~~~~~~~
Unexpected token 'C:\Users\gombc\OneDrive' in expression or statement.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:140 char:5
+ 100%        Newer                   3665        fix_epoch_tagger_task ...
+     ~
You must provide a value expression following the '%' operator.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:140 char:13
+ 100%        Newer                   3665        fix_epoch_tagger_task ...
+             ~~~~~
Unexpected token 'Newer' in expression or statement.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:142 char:79
+ ... ---------------------------------------------------------------------
+                                                                          ~
Missing expression after unary operator '--'.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:144 char:16
+                Total    Copied   Skipped  Mismatch    FAILED    Extra ...
+                ~~~~~
Unexpected token 'Total' in expression or statement.
At C:\ringstatus-task\fix_epoch_tagger_task.ps1:155 char:36
+ --------           --------   -----
+                                    ~
Missing expression after unary operator '-'.
Not all parse errors were reported.  Correct the reported errors and try again.
    + CategoryInfo          : ParserError: (:) [], ParentContainsErrorRecordException
    + FullyQualifiedErrorId : MissingExpressionAfterOperator
