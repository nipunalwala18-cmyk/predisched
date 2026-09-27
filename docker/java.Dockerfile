# The Java services (prompt 24): one image, role chosen by the first argument.
#
#   docker build -f docker/java.Dockerfile -t predisched-java .
#   docker run predisched-java scheduler --config configs/docker.yaml --id scheduler-1
#   docker run predisched-java worker --config configs/docker.yaml --id worker-1 --host worker-1
#   docker run predisched-java dashboard-api --dashboard.cluster-config=configs/docker.yaml
#   docker run predisched-java client --host scheduler-5 cluster leader
#   docker run predisched-java benchmark ...
#
# Tests run in CI's build job (mvn verify), so the image build only packages.

FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
# Dependencies first, so a source change does not re-download them.
COPY pom.xml .
COPY predisched-common/pom.xml predisched-common/
COPY predisched-client/pom.xml predisched-client/
COPY predisched-scheduler/pom.xml predisched-scheduler/
COPY predisched-worker/pom.xml predisched-worker/
COPY predisched-election/pom.xml predisched-election/
COPY predisched-replication/pom.xml predisched-replication/
COPY predisched-fault/pom.xml predisched-fault/
COPY predisched-benchmark/pom.xml predisched-benchmark/
COPY predisched-dashboard-api/pom.xml predisched-dashboard-api/
RUN mvn -B -q dependency:go-offline -DexcludeReactor=true || true
COPY proto proto
COPY predisched-common predisched-common
COPY predisched-client predisched-client
COPY predisched-scheduler predisched-scheduler
COPY predisched-worker predisched-worker
COPY predisched-election predisched-election
COPY predisched-replication predisched-replication
COPY predisched-fault predisched-fault
COPY predisched-benchmark predisched-benchmark
COPY predisched-dashboard-api predisched-dashboard-api
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:17-jre
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 predisched
WORKDIR /app
COPY --from=build /src/predisched-scheduler/target/predisched-scheduler.jar jars/
COPY --from=build /src/predisched-worker/target/predisched-worker.jar jars/
COPY --from=build /src/predisched-client/target/predisched-client.jar jars/
COPY --from=build /src/predisched-benchmark/target/predisched-benchmark.jar jars/
COPY --from=build /src/predisched-dashboard-api/target/predisched-dashboard-api.jar jars/
COPY configs configs
COPY workloads workloads
COPY testdata testdata
COPY docker/entrypoint.sh /usr/local/bin/predisched
RUN chmod 755 /usr/local/bin/predisched \
    && mkdir -p logs results \
    && chown -R predisched:predisched /app
USER predisched
# Containers get a memory limit; let the heap follow it.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75" \
    PREDISCHED_PORT=""
# The role's gRPC or HTTP port answers (PREDISCHED_PORT, set per service in compose).
HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=12 \
    CMD [ -z "$PREDISCHED_PORT" ] || bash -c "</dev/tcp/127.0.0.1/$PREDISCHED_PORT"
ENTRYPOINT ["predisched"]
CMD ["client", "--help"]
