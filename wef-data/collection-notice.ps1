param([guid]$Attempt,[ValidateSet('desktop','records')][string]$Channel)
$ErrorActionPreference='Stop'
$message=[Console]::In.ReadToEnd()
$receipt=Join-Path $PSScriptRoot ("runtime/collection-notice-"+$Channel+".json")
$result=[ordered]@{attempt="$Attempt";ok=$false;detail=''}
try {
    if($Channel -eq 'desktop'){
        [Windows.UI.Notifications.ToastNotificationManager,Windows.UI.Notifications,ContentType=WindowsRuntime] | Out-Null
        $doc=[Windows.Data.Xml.Dom.XmlDocument,Windows.Data.Xml.Dom.XmlDocument,ContentType=WindowsRuntime]::new()
        $escaped=[Security.SecurityElement]::Escape($message)
        $doc.LoadXml("<toast><visual><binding template='ToastGeneric'><text>wef-data collection</text><text>$escaped</text></binding></visual></toast>")
        $notifier=[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('OpenAI.Codex_2p2nqsd0c76g0!App')
        if ([string]$notifier.Setting -ne 'Enabled') {throw "Windows notification setting: $($notifier.Setting)"}
        $toast=[Windows.UI.Notifications.ToastNotification]::new($doc)
        $toast.Tag="$Attempt".Substring(0,16)
        $notifier.Show($toast)
        $result.detail='Submitted to Windows'
    }else{
        $cli=Join-Path $env:LOCALAPPDATA 'Programs/OpenAI/Codex/bin/codex.exe'
        $notice="Scheduled wef-data notification. Acknowledge receipt only. No actions. Run $Attempt.`n$message"
        $ErrorActionPreference="Continue"
        $notice | & $cli exec --sandbox read-only resume '01a169bc-e58a-7de0-94c6-5d3931fcf745' - 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {throw "Codex Records delivery exited $LASTEXITCODE"}
        $result.detail='Delivered to Records'
    }
    $result.ok=$true
} catch {$result.detail=$_.Exception.Message}
[IO.File]::WriteAllText($receipt+'.tmp',($result | ConvertTo-Json))
if([IO.File]::Exists($receipt)){[IO.File]::Replace($receipt+'.tmp',$receipt,[System.Management.Automation.Language.NullString]::Value)}else{[IO.File]::Move($receipt+'.tmp',$receipt)}
