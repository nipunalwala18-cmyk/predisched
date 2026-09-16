# PrediSched stop-cluster
# Each pid file holds the cmd.exe that start-cluster launched; taskkill /T also ends its java child.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
foreach ($pidFile in Get-ChildItem "$root\logs\*.pid" -ErrorAction SilentlyContinue) {
  # Not $pid: that is PowerShell's read-only automatic variable for the current process.
  $nodePid = Get-Content $pidFile.FullName -ErrorAction SilentlyContinue
  if ($nodePid) {
    cmd.exe /c "taskkill /T /F /PID $nodePid >nul 2>&1"
    if ($LASTEXITCODE -eq 0) {
      Write-Output "stopped pid=$nodePid ($($pidFile.BaseName))"
    } else {
      Write-Output "already stopped pid=$nodePid ($($pidFile.BaseName))"
    }
  }
  Remove-Item $pidFile.FullName -Force -ErrorAction SilentlyContinue
}
