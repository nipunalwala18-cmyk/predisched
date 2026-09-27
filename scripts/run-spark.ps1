# Runs one of the spark/predisched_spark jobs with spark-submit --master local[*]:
# natively when spark-submit is found (SPARK_HOME, PATH or the repo's .venv), else in
# docker/spark.Dockerfile.
#
#   scripts/run-spark.ps1 exec_stats.py --input spark/data/execution_history.csv --output spark/out
# Plain $args, not param(): an advanced script would read --output as -OutVariable/-OutBuffer.
if ($args.Count -lt 1) { Write-Error 'usage: run-spark.ps1 <job.py> [args]' }
$Job = $args[0]
$JobArgs = @($args | Select-Object -Skip 1)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$submit = $null
if ($env:SPARK_HOME -and (Test-Path (Join-Path $env:SPARK_HOME 'bin\spark-submit.cmd'))) {
    $submit = Join-Path $env:SPARK_HOME 'bin\spark-submit.cmd'
} elseif (Get-Command spark-submit.cmd -ErrorAction SilentlyContinue) {
    $submit = (Get-Command spark-submit.cmd).Source
} elseif (Test-Path '.venv\Scripts\spark-submit.cmd') {
    $submit = Join-Path $root '.venv\Scripts\spark-submit.cmd'
    $env:PYSPARK_PYTHON = Join-Path $root '.venv\Scripts\python.exe'
}

if ($submit) {
    Write-Host "run-spark: native ($submit)"
    & $submit --master 'local[*]' "spark/predisched_spark/$Job" @JobArgs
    exit $LASTEXITCODE
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error 'run-spark: neither spark-submit nor docker found; see docs/components/spark.md'
}
Write-Host 'run-spark: docker (predisched-spark)'
docker build -q -f docker/spark.Dockerfile -t predisched-spark . | Out-Null
docker run --rm -v "${root}:/work" predisched-spark $Job @JobArgs
exit $LASTEXITCODE
