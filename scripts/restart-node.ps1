<#
  Starts one node again after a crash, as start-cluster.ps1 started it (Exp 8). A scheduler comes
  back empty, rejoins as a backup and catches up with SyncFrom; a worker registers as new.

    scripts\restart-node.ps1 scheduler 5
    scripts\restart-node.ps1 worker 2
    scripts\restart-node.ps1 scheduler 3 -Config configs/other.yaml -Algorithm ring
#>
param(
    [Parameter(Mandatory)][ValidateSet('scheduler', 'worker')][string]$Role,
    [Parameter(Mandatory)][int]$Id,
    [string]$Config = 'configs/cluster.yaml',
    [ValidateSet('bully', 'ring')][string]$Algorithm = 'bully'
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force logs\pids | Out-Null
$name = "$Role-$Id"
# The java this shell runs: a WMI-created process does not inherit this session's PATH.
$java = (Get-Command java -ErrorAction Stop).Source
$pidFile = "logs\pids\$name.pid"
if ((Test-Path $pidFile) -and (Get-Process -Id (Get-Content $pidFile) -ErrorAction SilentlyContinue)) {
    Write-Host "$name already running (pid $(Get-Content $pidFile))"
    exit 0
}
$javaArgs = if ($Role -eq 'scheduler') {
    @('-jar', 'predisched-scheduler/target/predisched-scheduler.jar', '--config', $Config,
      '--id', $name, '--election-algorithm', $Algorithm)
} else {
    @('-jar', 'predisched-worker/target/predisched-worker.jar', '--config', $Config,
      '--id', $name, '--port', (51060 + $Id))
}
# As in start-cluster.ps1: WMI starts it detached, output appended to logs\<node>.out.
$hidden = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]0 }
$log = Join-Path (Get-Location) "logs\$name.out"
$created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine               = "cmd /c `"`"$java`" $($javaArgs -join ' ') >> `"$log`" 2>&1`""
    CurrentDirectory          = (Get-Location).Path
    ProcessStartupInformation = $hidden
}
if ($created.ReturnValue -ne 0) { throw "could not start $name (WMI code $($created.ReturnValue))" }
Set-Content $pidFile $created.ProcessId
Write-Host "restarted $name (pid $($created.ProcessId))"
