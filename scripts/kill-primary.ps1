<#
  Kills the primary scheduler the way a crash would (Exp 8): asks the cluster who leads
  (`cluster leader`), then kills that node's process with stop-node.ps1.

    scripts\kill-primary.ps1
    scripts\kill-primary.ps1 -Config configs/other.yaml
#>
param([string]$Config = 'configs/cluster.yaml')
Set-Location (Split-Path $PSScriptRoot -Parent)
$answers = & java -jar predisched-client/target/predisched-client.jar --config $Config cluster leader 2>$null
$answers | ForEach-Object { Write-Host $_ }
# The leader most live nodes name.
$votes = $answers | Select-String -Pattern 'leader=(\d+)' -AllMatches |
    ForEach-Object { $_.Matches } | ForEach-Object { $_.Groups[1].Value } |
    Group-Object | Sort-Object Count -Descending
if (-not $votes) { Write-Host 'no leader known: nothing killed'; exit 1 }
$leader = $votes[0].Name
Write-Host "primary is scheduler ${leader}: killing it"
& (Join-Path $PSScriptRoot 'stop-node.ps1') scheduler $leader
exit $LASTEXITCODE
