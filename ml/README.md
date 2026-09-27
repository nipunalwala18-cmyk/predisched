# ml — Python prediction pipeline and gRPC prediction server, added in prompts 15–17.

- `build_dataset.py`: the labelled dataset from a campaign (prompt 15, `docs/components/dataset.md`).
- `predisched_ml/`: features, training, evaluation and batch inference (prompt 16,
  `docs/components/ml-models.md`). Run from this folder:

      python -m predisched_ml.train --model all
      python -m predisched_ml.evaluate --model all
      python -m predisched_ml.infer --model exec_time --batch 1000
      python -m predisched_ml.prediction_server --port 50070     # prompt 17
      python -m predisched_ml.bench_server --workers 3 --requests 2000
      python -m predisched_ml.client < testdata/predict-request.json

- `generated/`: Python gRPC stubs for `proto/*.proto` (regenerate with the command in RULES).
