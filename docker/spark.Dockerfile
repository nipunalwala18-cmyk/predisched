# Spark jobs of prompt 12 in a container: the fallback when there is no native Spark
# (scripts/run-spark.sh / run-spark.ps1 pick it automatically).
#
# The spec names apache/spark-py; that image stopped at v3.4.0 and continues as
# apache/spark:<version>-python3, used here so the container matches PySpark 3.5.3 in
# spark/requirements.txt.
#
#   docker build -f docker/spark.Dockerfile -t predisched-spark .
#   docker run --rm -v "$PWD:/work" predisched-spark exec_stats.py \
#       --input spark/data/execution_history.csv --output spark/out
FROM apache/spark:3.5.3-python3

USER root
RUN pip3 install --no-cache-dir pandas==2.2.3 numpy==2.1.3 pyarrow==17.0.0
USER spark

WORKDIR /work
ENV PYTHONPATH=/work/spark
ENTRYPOINT ["/bin/bash", "-c", "exec /opt/spark/bin/spark-submit --master 'local[*]' /work/spark/predisched_spark/\"$0\" \"$@\""]
