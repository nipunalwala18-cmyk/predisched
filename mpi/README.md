# mpi — MPI programs (Exp 9-10), added in prompts 13-14.

```bash
python -m pip install -r mpi/requirements.txt     # Windows: brings Intel MPI's mpiexec via impi_rt
cd mpi && mpiexec -n 4 python -m predisched_mpi.collectives --generate 20 --seed 42
python -m pytest                                  # from mpi/
```

Setup on Windows and Linux, the collectives and real output: `docs/components/mpi.md`.
