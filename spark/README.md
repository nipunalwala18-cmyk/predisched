# spark — PySpark MapReduce jobs (Exp 7), added in prompt 12.

```bash
python -m pip install -r spark/requirements.txt
python spark/export_history.py
spark-submit --master local[*] spark/predisched_spark/exec_stats.py --input spark/data/execution_history.csv --output spark/out
python -m pytest            # from spark/
```

Map and reduce phases, Windows notes and real output: `docs/components/spark.md`.
