#!/usr/bin/env bash
# Runs an mpi/predisched_mpi program on N ranks: natively when mpiexec is found (PATH, or the
# impi_rt runtime in the repo's .venv), else in docker/mpi.Dockerfile (OpenMPI).
#
#   scripts/run-mpi.sh 4 predisched_mpi.collectives --generate 20 --seed 42
set -euo pipefail
cd "$(dirname "$0")/.."
ranks="${1:?usage: run-mpi.sh <ranks> <module> [args]}"
module="${2:?usage: run-mpi.sh <ranks> <module> [args]}"
shift 2

python="python3"
[ -x .venv/bin/python ] && python="$PWD/.venv/bin/python"
[ -x .venv/Scripts/python.exe ] && python="$PWD/.venv/Scripts/python.exe"

mpiexec=""
if command -v mpiexec >/dev/null 2>&1; then
  mpiexec="$(command -v mpiexec)"
elif [ -x .venv/Library/bin/mpiexec.exe ]; then
  mpiexec="$PWD/.venv/Library/bin/mpiexec.exe"
  export PATH="$PWD/.venv/Library/bin:$PATH"
fi

if [ -n "$mpiexec" ]; then
  echo "run-mpi: native ($mpiexec)"
  cd mpi
  exec "$mpiexec" -n "$ranks" "$python" -m "$module" "$@"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "run-mpi: neither mpiexec nor docker found; see docs/components/mpi.md" >&2
  exit 1
fi
echo "run-mpi: docker (predisched-mpi, OpenMPI)"
docker build -q -f docker/mpi.Dockerfile -t predisched-mpi . >/dev/null
exec docker run --rm -v "$PWD:/work" predisched-mpi "$ranks" "$module" "$@"
