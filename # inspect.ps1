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


dir "C:\actions-runner\ringstatus\schedules-dailyv2.log"
dir "C:\actions-runner\ringstatus\schedules-calculatorv2.log"
