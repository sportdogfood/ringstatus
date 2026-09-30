param(
  [string]$RepoPath = "$env:USERPROFILE\OneDrive - Sport Dog Food\github\repos\ringstatus"
)

$ErrorActionPreference = "Stop"
$PayloadCommit = "26a8c4f95ab49950a7b6fa5d09cc9a1f44c4730d"
$ApiRoot = "https://api.github.com/repos/sportdogfood/ringstatus/contents"

$PayloadPaths = @(
  ".codex/config.toml",
  ".codex/hooks.json",
  ".codex/hooks/common.mjs",
  ".codex/hooks/pre_tool_use.mjs",
  ".codex/hooks/stop_guard.mjs",
  ".codex/hooks/user_prompt_submit.mjs",
  ".codex/hooks/session_start.mjs",
  ".codex/control/policy.txt",
  ".codex/control/task-contract.json",
  ".codex/control/README.md",
  ".codex/control/USER-FAILURE-RECORD.md",
  ".codex/control/FAILURE-SOLUTION-MAP.md",
  ".codex/control/EVIDENCE-INDEX.md",
  ".codex/control/ENTRY-STATUS-SIDECAR.md",
  ".codex/control/history/AGENT-HIERARCHY-REVIEW-2026-09-29.txt",
  ".codex/control/history/CURRENT-SESSION-EVIDENCE-2026-09-29.md",
  ".codex/control/history/FULL-SESSION-REVIEW-2026-09-29.txt",
  ".codex/control/history/FAILURE-24-2026-09-29.txt",
  ".codex/control/archive/ringstatus-ai-control-foundation-v2.1.0/README.md",
  ".agents/skills/ringstatus-control/SKILL.md",
  "tests/codex-control-hooks.test.mjs",
  "tests/codex-session-start-hook.test.mjs"
)

function Get-GitBlobSha([byte[]]$Bytes) {
  $prefix = [System.Text.Encoding]::ASCII.GetBytes("blob $($Bytes.Length)" + [char]0)
  $all = New-Object byte[] ($prefix.Length + $Bytes.Length)
  [Array]::Copy($prefix, 0, $all, 0, $prefix.Length)
  [Array]::Copy($Bytes, 0, $all, $prefix.Length, $Bytes.Length)
  $sha = [System.Security.Cryptography.SHA1]::Create()
  try {
    return -join (($sha.ComputeHash($all)) | ForEach-Object { $_.ToString("x2") })
  } finally {
    $sha.Dispose()
  }
}

function Encode-GitHubPath([string]$Path) {
  return (($Path -split "/") | ForEach-Object { [uri]::EscapeDataString($_) }) -join "/"
}

if (-not (Test-Path -LiteralPath (Join-Path $RepoPath ".git"))) {
  throw "RingStatus repository not found at: $RepoPath"
}

$backupRoot = Join-Path $env:TEMP ("ringstatus-control-backup-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null

$headers = @{
  "Accept" = "application/vnd.github+json"
  "User-Agent" = "RingStatus-Control-Installer"
}

foreach ($rel in $PayloadPaths) {
  $encoded = Encode-GitHubPath $rel
  $uri = "$ApiRoot/$encoded" + "?ref=$PayloadCommit"
  $item = Invoke-RestMethod -Uri $uri -Headers $headers -Method Get
  if (-not $item.content) {
    throw "GitHub payload missing content for $rel"
  }

  $bytes = [Convert]::FromBase64String(($item.content -replace "\s", ""))
  $actualSha = Get-GitBlobSha $bytes
  if ($actualSha -ne $item.sha) {
    throw "GitHub payload hash mismatch before install: $rel"
  }

  $winRel = $rel.Replace("/", "\")
  $target = Join-Path $RepoPath $winRel
  $targetDir = Split-Path -Parent $target
  New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

  if (Test-Path -LiteralPath $target) {
    $backup = Join-Path $backupRoot $winRel
    New-Item -ItemType Directory -Path (Split-Path -Parent $backup) -Force | Out-Null
    Copy-Item -LiteralPath $target -Destination $backup -Force
  }

  [System.IO.File]::WriteAllBytes($target, $bytes)

  $written = [System.IO.File]::ReadAllBytes($target)
  if ((Get-GitBlobSha $written) -ne $item.sha) {
    throw "Installed file verification failed: $rel"
  }
}

function Find-Node {
  $cmd = Get-Command node -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }

  $candidates = Get-ChildItem -Path (Join-Path $env:LOCALAPPDATA "OpenAI\Codex\runtimes\cua_node\*\bin\node.exe") -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending
  if ($candidates) { return $candidates[0].FullName }
  throw "Node executable not found."
}

function Find-Codex {
  $cmd = Get-Command codex -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }

  $candidates = Get-ChildItem -Path (Join-Path $env:LOCALAPPDATA "OpenAI\Codex\bin\*\codex.exe") -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending
  if ($candidates) { return $candidates[0].FullName }
  throw "Codex executable not found."
}

$node = Find-Node
$test1 = & $node (Join-Path $RepoPath "tests\codex-control-hooks.test.mjs") 2>&1
if ($LASTEXITCODE -ne 0) {
  throw "Control hook tests failed: $($test1 -join [Environment]::NewLine)"
}
$test2 = & $node (Join-Path $RepoPath "tests\codex-session-start-hook.test.mjs") 2>&1
if ($LASTEXITCODE -ne 0) {
  throw "Session-start hook test failed: $($test2 -join [Environment]::NewLine)"
}

$codex = Find-Codex
$receipt = Join-Path $env:TEMP "ringstatus-codex-hook-receipt.json"
Remove-Item -LiteralPath $receipt -Force -ErrorAction SilentlyContinue
$lastMessage = Join-Path $env:TEMP "ringstatus-codex-hook-proof-message.txt"
Remove-Item -LiteralPath $lastMessage -Force -ErrorAction SilentlyContinue

$codexOutput = & $codex exec --dangerously-bypass-hook-trust --ephemeral --sandbox read-only --cd $RepoPath --output-last-message $lastMessage "Return exactly RINGSTATUS_HOOK_PROOF. Do not use tools." 2>&1
$codexExit = $LASTEXITCODE

if ($codexExit -ne 0) {
  throw "Codex live hook proof failed to launch: $($codexOutput -join [Environment]::NewLine)"
}
if (-not (Test-Path -LiteralPath $receipt)) {
  throw "Codex ran but the RingStatus SessionStart hook did not create its proof receipt."
}

$hookReceipt = Get-Content -LiteralPath $receipt -Raw | ConvertFrom-Json
$expectedCwd = [System.IO.Path]::GetFullPath($RepoPath).TrimEnd("\")
$actualCwd = [System.IO.Path]::GetFullPath([string]$hookReceipt.cwd).TrimEnd("\")
if ($hookReceipt.hook_event_name -ne "SessionStart" -or $actualCwd -ne $expectedCwd) {
  throw "Codex hook receipt did not match the RingStatus project."
}

$result = [ordered]@{
  status = "PASS"
  payload_commit = $PayloadCommit
  installed_repo = $RepoPath
  deterministic_control_tests = "PASS"
  session_start_test = "PASS"
  live_codex_hook_execution = "PASS"
  persistent_hook_trust = "PENDING_REVIEW"
  verified_at = (Get-Date).ToString("o")
  backup_directory = $backupRoot
}

$resultPath = Join-Path $RepoPath ".codex\control\LOCAL-PROOF-RESULT.json"
$result | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $resultPath -Encoding UTF8

Remove-Item -LiteralPath $receipt -Force -ErrorAction SilentlyContinue

Write-Host "RINGSTATUS_CONTROL_PROOF_PASS"
Write-Host "Persistent Codex hook trust remains PENDING_REVIEW."
