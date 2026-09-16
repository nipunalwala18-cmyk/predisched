# PrediSched start-cluster
# Starts scheduler-1 and worker-1..3 as detached background processes, logs in logs/.
#
# Nodes are launched through Win32_Process.Create rather than Start-Process. Start-Process hands the
# child the caller's console handles, so a coding agent that runs this script waits for the long-lived
# java process to exit and hangs forever. A process created by WMI shares nothing with the caller.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root
New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null

Write-Output "Building jars (skip tests)..."
& mvn -q -DskipTests package
if ($LASTEXITCODE -ne 0) { throw "mvn package failed" }

function Get-ConfigPort($config) {
  $line = Select-String -Path $config -Pattern '^port:\s*(\d+)' | Select-Object -First 1
  if (-not $line) { throw "no top-level port in $config" }
  [int]$line.Matches[0].Groups[1].Value
}

function Stop-Node($name, $port) {
  $pidFile = "$root\logs\$name.pid"
  if (Test-Path $pidFile) {
    $old = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($old) { cmd.exe /c "taskkill /T /F /PID $old >nul 2>&1" }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
  }
  # A java process left on this port by an earlier run without a pid file.
  Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
    ForEach-Object { Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue } |
    Where-Object { $_.ProcessName -eq "java" } |
    ForEach-Object {
      Write-Output "stopping leftover java pid=$($_.Id) on port $port"
      Stop-Process -Id $_.Id -Force
    }
  # Wait for the port to be released so the listen check below cannot see the old process.
  $deadline = (Get-Date).AddSeconds(10)
  while ((Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) -and (Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 250
  }
}

function Start-Node($jar, $config, $name) {
  $port = Get-ConfigPort $config
  Stop-Node $name $port

  $logOut = "$root\logs\$name.out.log"
  $logErr = "$root\logs\$name.err.log"
  $cmd = "cmd.exe /c java -jar `"$jar`" `"$config`" 1>`"$logOut`" 2>`"$logErr`""
  $r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create `
         -Arguments @{ CommandLine = $cmd; CurrentDirectory = $root }
  if ($r.ReturnValue -ne 0) { throw "failed to launch $name (Win32_Process.Create returned $($r.ReturnValue))" }
  # This is the cmd.exe pid; stop-cluster kills its whole tree, which includes java.
  Set-Content -Path "$root\logs\$name.pid" -Value $r.ProcessId

  # Wait until the node is actually listening instead of assuming it started.
  $deadline = (Get-Date).AddSeconds(30)
  while ((Get-Date) -lt $deadline) {
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
      Write-Output "started $name pid=$($r.ProcessId) port=$port"
      return
    }
    if (-not (Get-Process -Id $r.ProcessId -ErrorAction SilentlyContinue)) { break }
    Start-Sleep -Milliseconds 500
  }
  Write-Output "FAILED to start $name on port $port. Last lines of logs\$name.err.log:"
  Get-Content $logErr -Tail 15 -ErrorAction SilentlyContinue
  exit 1
}

Start-Node "$root\predisched-scheduler\target\predisched-scheduler.jar" "$root\configs\local\scheduler-1.yaml" "scheduler-1"
Start-Node "$root\predisched-worker\target\predisched-worker.jar" "$root\configs\local\worker-1.yaml" "worker-1"
Start-Node "$root\predisched-worker\target\predisched-worker.jar" "$root\configs\local\worker-2.yaml" "worker-2"
Start-Node "$root\predisched-worker\target\predisched-worker.jar" "$root\configs\local\worker-3.yaml" "worker-3"
Write-Output "Cluster up. Logs in logs/. Stop with scripts\stop-cluster.ps1"
