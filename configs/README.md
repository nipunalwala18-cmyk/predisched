# configs — cluster and benchmark YAML configs, starting in prompt 01.

- `campaign.yaml`, `campaign-node.yaml`: the prompt 15 dataset campaign (runs, heterogeneous workers, label thresholds, hold-out) and the node config it runs with (result cache off).
- `ml.yaml`: `configs/mpi.yaml` plus `ml.python`, so a worker runs `ML_INFER_TASK` (prompt 16, `docs/components/ml-models.md`).
- `predictive.yaml`: `campaign-node.yaml` with the predictive strategy and its prediction-server client on (prompt 18, `docs/components/predictive-strategy.md`).
- `chaos.yaml`: `cluster.yaml` with the chaos API and speculative execution on, least-loaded placement (prompt 19, `docs/components/speculation-and-chaos.md`).
- `benchmark.yaml`, `benchmark-node.yaml`: the prompt 20 benchmark suite (scenarios, strategies, worker sets, controls) and the node config of every run.
