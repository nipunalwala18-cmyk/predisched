<#
  Starts the local cluster: 5 schedulers (ports 51051-51055, election ids 1-5) and 3 workers
  (51061-51063), each in the background with its output in logs\<node>.out and its PID in
  logs\pids\<node>.pid. Stop one node with scripts\stop-node.ps1, or all with -Stop.

    scripts\start-cluster.ps1                      # Bully (the default in configs\cluster.yaml)
    scripts\start-cluster.ps1 -Algorithm ring
    scripts\start-cluster.ps1 -Schedulers 3 -Workers 1
#>
param(
    [ValidateSet('bully', 'ring')][string]$Algorithm = 'bully',
    [string]$Config = 'configs/cluster.yaml',
    [ValidateRange(1, 5)][int]$Schedulers = 5,
    [ValidateRange(0, 3)][int]$Workers = 3
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force logs\pids | Out-Null

function Start-Node([string]$Name, [string[]]$JavaArgs) {
    $pidFile = "logs\pids\$Name.pid"
    if (Test-Path $pidFile) {
        $old = Get-Content $pidFile
        if (Get-Process -Id $old -ErrorAction SilentlyContinue) {
            Write-Host "$Name already running (pid $old)"
            return
        }
    }
    # WMI creates the process with no inherited handles, so this script returns at once even
    # when its own output is piped; Start-Process would keep the caller's pipe open.
    $hidden = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]0 }
    $log = Join-Path (Get-Location) "logs\$Name.out"
    $created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
        CommandLine               = "cmd /c java $($JavaArgs -join ' ') > `"$log`" 2>&1"
        CurrentDirectory          = (Get-Location).Path
        ProcessStartupInformation = $hidden
    }
    if ($created.ReturnValue -ne 0) { throw "could not start $Name (WMI code $($created.ReturnValue))" }
    Set-Content $pidFile $created.ProcessId
    Write-Host "started $Name (pid $($created.ProcessId))"
}

for ($n = 1; $n -le $Schedulers; $n++) {
    Start-Node "scheduler-$n" @('-jar', 'predisched-scheduler/target/predisched-scheduler.jar',
        '--config', $Config, '--id', "scheduler-$n", '--election-algorithm', $Algorithm)
}
for ($n = 1; $n -le $Workers; $n++) {
    Start-Node "worker-$n" @('-jar', 'predisched-worker/target/predisched-worker.jar',
        '--config', $Config, '--id', "worker-$n", '--port', (51060 + $n))
}
Write-Host "cluster starting ($Algorithm); check it with:"
Write-Host "  java -jar predisched-client/target/predisched-client.jar cluster leader"
