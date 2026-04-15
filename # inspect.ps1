# inspect
Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  ForEach-Object { $_.Settings } |
  Format-List MultipleInstances, Enabled, ExecutionTimeLimit, Hidden


  # fix repeat behavior
$settings = New-ScheduledTaskSettingsSet `
  -MultipleInstances Queue `
  -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
  -Hidden `
  -Priority 7

Set-ScheduledTask -TaskName 'epoch-tagger-local' -Settings $settings
Enable-ScheduledTask -TaskName 'epoch-tagger-local'


# confirm
Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  ForEach-Object { $_.Settings } |
  Format-List MultipleInstances, Enabled


  Start-Process powershell -Verb RunAs


  $settings = New-ScheduledTaskSettingsSet `
  -MultipleInstances Queue `
  -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
  -Hidden `
  -Priority 7

Set-ScheduledTask -TaskName 'epoch-tagger-local' -Settings $settings
Enable-ScheduledTask -TaskName 'epoch-tagger-local'


$task = Get-ScheduledTask -TaskName 'epoch-tagger-local'

$action    = $task.Actions
$trigger   = $task.Triggers
$principal = $task.Principal
$settings  = New-ScheduledTaskSettingsSet `
  -MultipleInstances Queue `
  -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
  -Hidden `
  -Priority 7

Register-ScheduledTask `
  -TaskName 'epoch-tagger-local' `
  -Action $action `
  -Trigger $trigger `
  -Principal $principal `
  -Settings $settings `
  -Force

  Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  ForEach-Object { $_.Settings } |
  Format-List MultipleInstances, Enabled

notepad "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"
dir "C:\actions-runner\ringstatus\schedules-calculatorv2.log"

  cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"

Get-Content .\run_tagger_task.cmd
Get-Content .\run_tagger_task.ps1
Get-Content .\run_tagger_task_cmd.ps1



  cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"

Select-String -Path .\run_tagger_task.cmd, .\run_tagger_task.ps1, .\run_tagger_task_cmd.ps1 `
  -Pattern 'heartbeat|schedules_dailyv2|schedules_calculatorv2|trips_dailyv2|trips_calculatorv2'



cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
git status
git pull


  powershell -ExecutionPolicy Bypass -File "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\fix_epoch_tagger_task.ps1"



  Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  Select-Object -ExpandProperty Actions |
  Format-List Execute,Arguments,WorkingDirectory


  Fastest fix:

Open Task Scheduler
epoch-tagger-local
Properties
Actions
Set it to:
Program/script: C:\Windows\System32\cmd.exe
Add arguments: /d /c "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"


<<<<<<< HEAD
dir "C:\actions-runner\ringstatus\schedules-dailyv2.log"
dir "C:\actions-runner\ringstatus\schedules-calculatorv2.log"
=======
Execute          : powershell.exe
Arguments        : -ExecutionPolicy Bypass -File "C:\Users\gombc\OneDrive - Sport Dog
                   Food\github\repos\ringstatus\run_tagger_task.ps1"
WorkingDirectory : C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus

>>>>>>> 4412de39cbfdf4ca3faf3c1c521cdc54101fa296


Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  Select-Object -ExpandProperty Actions |
  Format-List Execute,Arguments,WorkingDirectory


  "C:\Users\gombc\OneDrive - Sport Dog Food"

  
Execute          : C:\Windows\System32\cmd.exe
Arguments        : /d /c "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"
WorkingDirectory :

Error: UNKNOWN: unknown error, open 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\schedules_dailyv2.js'
    at Object.readFileSync (node:fs:440:20)
    at defaultLoadImpl (node:internal/modules/cjs/loader:1148:17)
    at loadSource (node:internal/modules/cjs/loader:1838:20)
    at Object..js (node:internal/modules/cjs/loader:1936:44)
    at Module.load (node:internal/modules/cjs/loader:1533:32)
    at Module._load (node:internal/modules/cjs/loader:1335:12)
    at wrapModuleLoad (node:internal/modules/cjs/loader:255:19)
    at Module.executeUserEntryPoint [as runMain] (node:internal/modules/run_main:154:5)
    at node:internal/main/run_main_module:33:47 {
  errno: -4094,
  code: 'UNKNOWN',
  syscall: 'open',
  path: 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_dailyv2.js'
}

Node.js v24.14.1
[Wed 04/15/2026 14:15:23.35] SCHEDULES_DAILYV2 RUN 
node:fs:440
    return binding.readFileUtf8(path, stringToFlags(options.flag));
                   ^

Error: UNKNOWN: unknown error, open 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\schedules_dailyv2.js'
    at Object.readFileSync (node:fs:440:20)
    at defaultLoadImpl (node:internal/modules/cjs/loader:1148:17)
    at loadSource (node:internal/modules/cjs/loader:1838:20)
    at Object..js (node:internal/modules/cjs/loader:1936:44)
    at Module.load (node:internal/modules/cjs/loader:1533:32)
    at Module._load (node:internal/modules/cjs/loader:1335:12)
    at wrapModuleLoad (node:internal/modules/cjs/loader:255:19)
    at Module.executeUserEntryPoint [as runMain] (node:internal/modules/run_main:154:5)
    at node:internal/main/run_main_module:33:47 {
  errno: -4094,
  code: 'UNKNOWN',
  syscall: 'open',
  path: 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_dailyv2.js'
}

Node.js v24.14.1

