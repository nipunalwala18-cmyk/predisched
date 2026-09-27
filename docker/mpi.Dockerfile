# MPI programs of prompts 13-14 on OpenMPI: the Linux route, and the fallback
# scripts/run-mpi.sh / run-mpi.ps1 pick when no native mpiexec is found.
#
#   docker build -f docker/mpi.Dockerfile -t predisched-mpi .
#   docker run --rm -v "$PWD:/work" predisched-mpi 4 predisched_mpi.collectives --generate 20 --seed 42
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openmpi-bin libopenmpi-dev build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir mpi4py==4.1.2 numpy==2.1.3 pytest==8.3.3

# OpenMPI refuses to run as root and to start more ranks than cores without these.
ENV OMPI_ALLOW_RUN_AS_ROOT=1 \
    OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 \
    OMPI_MCA_rmaps_base_oversubscribe=1 \
    PYTHONPATH=/work/mpi

WORKDIR /work/mpi
# First argument: number of ranks; the rest: the module and its arguments.
ENTRYPOINT ["/bin/bash", "-c", "n=\"$0\"; exec mpiexec -n \"$n\" python -m \"$@\""]
