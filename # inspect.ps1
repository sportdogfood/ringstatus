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
