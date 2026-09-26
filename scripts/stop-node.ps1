<#
  Kills one node started by start-cluster.ps1, the way a crash would (no graceful shutdown).

    scripts\stop-node.ps1 scheduler 5
    scripts\stop-node.ps1 worker 2
    scripts\stop-node.ps1 all            # every node with a PID file
#>
param(
    [Parameter(Mandatory)][ValidateSet('scheduler', 'worker', 'all')][string]$Role,
    [int]$Id
)
Set-Location (Split-Path $PSScriptRoot -Parent)
$files = if ($Role -eq 'all') { Get-ChildItem logs\pids\*.pid -ErrorAction SilentlyContinue }
         else { Get-Item "logs\pids\$Role-$Id.pid" -ErrorAction SilentlyContinue }
if (-not $files) { Write-Host "no PID file for $Role $Id"; exit 1 }
foreach ($file in $files) {
    $nodePid = Get-Content $file
    # The PID is the cmd.exe wrapper start-cluster.ps1 created; /T takes its java child too.
    taskkill /T /F /PID $nodePid 2>$null | Out-Null
    Remove-Item $file
    Write-Host "stopped $($file.BaseName) (pid $nodePid)"
}
