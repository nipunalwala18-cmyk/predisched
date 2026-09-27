# The lab demo (prompt 24) from PowerShell: runs scripts/demo.sh in Git for Windows' bash, so
# there is one demo script, not two that drift apart.
#
#   scripts/demo.ps1
#   scripts/demo.ps1 --no-pause
#   scripts/demo.ps1 --docker | --native
# Plain $args, not param(), so --no-pause reaches demo.sh unchanged.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$bash = $null
$git = Get-Command git.exe -ErrorAction SilentlyContinue
if ($git) {
    # git.exe lives in <Git>\cmd or <Git>\mingw64\bin; bash.exe in <Git>\bin.
    foreach ($up in @('..', '..\..')) {
        $candidate = Join-Path (Split-Path -Parent $git.Source) "$up\bin\bash.exe"
        if (Test-Path $candidate) { $bash = (Resolve-Path $candidate).Path; break }
    }
}
if (-not $bash) {
    $found = Get-Command bash.exe -ErrorAction SilentlyContinue |
        Where-Object { $_.Source -notlike '*\System32\*' } | Select-Object -First 1
    if ($found) { $bash = $found.Source }
}
if (-not $bash) {
    Write-Error 'scripts/demo.ps1 needs bash: install Git for Windows (docs/TROUBLESHOOTING.md)'
}
# The Java tools the demo calls must be on PATH for bash too.
if ($env:JAVA_HOME) { $env:PATH = (Join-Path $env:JAVA_HOME 'bin') + ';' + $env:PATH }
& $bash scripts/demo.sh @args
exit $LASTEXITCODE
