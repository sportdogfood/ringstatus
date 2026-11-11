$ErrorActionPreference = 'Stop'
Disable-ScheduledTask -TaskPath '\' -TaskName 'epoch-tagger-local' | Out-Null
$task = Get-ScheduledTask -TaskPath '\' -TaskName 'epoch-tagger-local'
if ($task.Settings.Enabled) { throw 'Legacy task still enabled' }
