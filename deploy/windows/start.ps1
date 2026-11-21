# Start one serialhub per config (com-*.yaml) in this folder.
# Usage: .\start.ps1 [-ListenHost 0.0.0.0]
param([string]$ListenHost = "")
$Root = (Resolve-Path "$PSScriptRoot\..\..").Path
$Run = Join-Path $PSScriptRoot "run"; New-Item -ItemType Directory -Force $Run | Out-Null
foreach ($cfg in Get-ChildItem "$PSScriptRoot\com-*.yaml") {
  $n = $cfg.BaseName; $pidFile = Join-Path $Run "$n.pid"
  if ((Test-Path $pidFile) -and (Get-Process -Id (Get-Content $pidFile) -ErrorAction SilentlyContinue)) {
    Write-Host "$n already running (pid $(Get-Content $pidFile))"; continue }
  $argList = @("serve", "--config", $cfg.FullName); if ($ListenHost) { $argList += @("--host", $ListenHost) }
  $p = Start-Process -FilePath "$Root\.venv\Scripts\serialhub.exe" -ArgumentList $argList -WorkingDirectory $Root `
       -WindowStyle Hidden -PassThru -RedirectStandardOutput "$Run\$n.out.log" -RedirectStandardError "$Run\$n.log"
  $p.Id | Set-Content $pidFile
  $port = (Select-String -Path $cfg.FullName -Pattern '^\s*port:\s*(\d+)' | Select-Object -First 1).Matches.Groups[1].Value
  $h = if ($ListenHost) { $ListenHost } else { "127.0.0.1" }
  Write-Host "$n started (pid $($p.Id)) -> http://${h}:$port/   log: $Run\$n.log"
}
