# configs — cluster and benchmark YAML configs, starting in prompt 01.

- `campaign.yaml`, `campaign-node.yaml`: the prompt 15 dataset campaign (runs, heterogeneous workers, label thresholds, hold-out) and the node config it runs with (result cache off).
- `ml.yaml`: `configs/mpi.yaml` plus `ml.python`, so a worker runs `ML_INFER_TASK` (prompt 16, `docs/components/ml-models.md`).
