# The prediction server of prompt 17 (gRPC, port 50070).
#
#   docker build -f docker/prediction.Dockerfile -t predisched-prediction .
#   docker run -p 50070:50070 predisched-prediction
#
# Model binaries are not committed (.gitignore), so the image trains m1-m3 from the committed
# dataset (ml/data/dataset.parquet) at build time: about a minute, the same models as
# `python -m predisched_ml.train --model all` on a laptop (seeded, ml/config.yaml).
FROM python:3.12-slim

RUN useradd --create-home --uid 10001 predisched
WORKDIR /app
COPY ml/requirements.txt ml/requirements.txt
RUN pip install --no-cache-dir -r ml/requirements.txt
COPY proto proto
COPY ml ml
COPY scripts/merge-events.py scripts/merge-events.py
# A fresh registry, so the image's models are v1 like the committed metadata.
RUN rm -rf ml/models/m1 ml/models/m2 ml/models/m3 ml/models/registry.json \
    && cd ml && python -m predisched_ml.train --model all \
    && mkdir -p /app/logs \
    && chown -R predisched:predisched /app
USER predisched
WORKDIR /app/ml
ENV PYTHONUNBUFFERED=1 \
    PREDISCHED_DSN="postgresql://postgres:predisched@postgres:5432/predisched"
EXPOSE 50070
HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=12 \
    CMD python -c "import socket; socket.create_connection(('127.0.0.1', 50070), 2)"
ENTRYPOINT ["python", "-m", "predisched_ml.prediction_server"]
CMD ["--port", "50070"]
