# Runs an mpi/predisched_mpi program on N ranks: natively when mpiexec is found (PATH, e.g.
# MS-MPI, or the impi_rt runtime in the repo's .venv), else in docker/mpi.Dockerfile (OpenMPI).
#
#   scripts/run-mpi.ps1 4 predisched_mpi.collectives --generate 20 --seed 42
#   scripts/run-mpi.ps1 matmul --sizes 200,400,800 --repeat 3   # no rank count: each of
#                                                               # $env:MPI_RANKS (default "1 2 4")
# A module without a dot is short for predisched_mpi.<name>.
# Plain $args, not param(): an advanced script would read --out as -OutVariable/-OutBuffer.
if ($args.Count -lt 1) { Write-Error 'usage: run-mpi.ps1 [<ranks>] <module> [args]' }
if ("$($args[0])" -match '^[0-9]+$') {
    $RankList = @([int]$args[0])
    $Module = $args[1]
    $ModuleArgs = @($args | Select-Object -Skip 2)
} else {
    $spec = if ($env:MPI_RANKS) { $env:MPI_RANKS } else { '1 2 4' }
    $RankList = @($spec -split '\s+' | Where-Object { $_ } | ForEach-Object { [int]$_ })
    $Module = $args[0]
    $ModuleArgs = @($args | Select-Object -Skip 1)
}
if ($Module -notmatch '\.') { $Module = "predisched_mpi.$Module" }
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = 'python'
if (Test-Path '.venv\Scripts\python.exe') { $python = Join-Path $root '.venv\Scripts\python.exe' }

$mpiexec = $null
if (Get-Command mpiexec -ErrorAction SilentlyContinue) {
    $mpiexec = (Get-Command mpiexec).Source
} elseif (Test-Path '.venv\Library\bin\mpiexec.exe') {
    $mpiexec = Join-Path $root '.venv\Library\bin\mpiexec.exe'
    $env:PATH = (Join-Path $root '.venv\Library\bin') + ';' + $env:PATH
}

if ($mpiexec) {
    Write-Host "run-mpi: native ($mpiexec)"
    Set-Location (Join-Path $root 'mpi')
    foreach ($Ranks in $RankList) {
        Write-Host "run-mpi: mpiexec -n $Ranks python -m $Module $ModuleArgs"
        & $mpiexec -n $Ranks $python -m $Module @ModuleArgs
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    exit 0
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error 'run-mpi: neither mpiexec nor docker found; see docs/components/mpi.md'
}
Write-Host 'run-mpi: docker (predisched-mpi, OpenMPI)'
docker build -q -f docker/mpi.Dockerfile -t predisched-mpi . | Out-Null
foreach ($Ranks in $RankList) {
    docker run --rm -v "${root}:/work" predisched-mpi $Ranks $Module @ModuleArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
exit 0
