#!/usr/bin/env bash
# Runs an mpi/predisched_mpi program on N ranks: natively when mpiexec is found (PATH, or the
# impi_rt runtime in the repo's .venv), else in docker/mpi.Dockerfile (OpenMPI).
#
#   scripts/run-mpi.sh 4 predisched_mpi.collectives --generate 20 --seed 42
#   scripts/run-mpi.sh matmul --sizes 200,400,800 --repeat 3     # no rank count: runs on each of
#                                                                # $MPI_RANKS (default "1 2 4")
# A module without a dot is short for predisched_mpi.<name>.
set -euo pipefail
cd "$(dirname "$0")/.."
usage="usage: run-mpi.sh [<ranks>] <module> [args]"
if [[ "${1:?$usage}" =~ ^[0-9]+$ ]]; then
  rank_list="$1"
  shift
else
  rank_list="${MPI_RANKS:-1 2 4}"
fi
module="${1:?$usage}"
shift
[[ "$module" == *.* ]] || module="predisched_mpi.$module"

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
  for ranks in $rank_list; do
    echo "run-mpi: mpiexec -n $ranks python -m $module $*"
    "$mpiexec" -n "$ranks" "$python" -m "$module" "$@"
  done
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "run-mpi: neither mpiexec nor docker found; see docs/components/mpi.md" >&2
  exit 1
fi
echo "run-mpi: docker (predisched-mpi, OpenMPI)"
docker build -q -f docker/mpi.Dockerfile -t predisched-mpi . >/dev/null
for ranks in $rank_list; do
  docker run --rm -v "$PWD:/work" predisched-mpi "$ranks" "$module" "$@"
done
