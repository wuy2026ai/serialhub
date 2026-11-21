foreach ($f in Get-ChildItem "$PSScriptRoot\run\*.pid" -ErrorAction SilentlyContinue) {
  Stop-Process -Id (Get-Content $f) -Force -ErrorAction SilentlyContinue
  Write-Host "stopped $($f.BaseName)"; Remove-Item $f
}
