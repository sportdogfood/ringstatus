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
