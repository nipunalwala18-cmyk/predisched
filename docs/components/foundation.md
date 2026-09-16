# Foundation

Skeleton the whole product grows into (Prompt 00). No feature code yet.

## What exists

- Parent `pom.xml` (`com.predisched:predisched-parent`, Java 17) pinning gRPC 1.66.0,
  Protobuf 3.25.5, SLF4J 2.0.13, Logback 1.5.8, JUnit 5.10.3, Mockito 5.12.0,
  SnakeYAML 2.2, PostgreSQL JDBC 42.7.3, HikariCP 5.1.0.
- Modules: `predisched-common`, `predisched-client`, `predisched-scheduler`,
  `predisched-worker`, `predisched-election`, `predisched-replication`,
  `predisched-fault`, `predisched-benchmark`.
- `proto/common.proto` with `Ack`; `predisched-common` compiles every `proto/*.proto`.
- `common/config/NodeConfig`: YAML from first CLI arg or `PREDISCHED_CONFIG`
  (`nodeId`, `role`, `host`, `port`, `peers`, `seed`, `settings`).
- `common/grpc/Channels`: one cached `ManagedChannel` per `host:port`.
- `logback.xml` pattern `%d{HH:mm:ss.SSS} [%X{node}] [L=%X{lamport}] %-5level %logger{20} - %msg%n`
  (MDC filled from Prompt 04).
- `configs/local/`: 3 schedulers (50051-50053) + 3 workers (50061-50063).
- `requirements.txt`, `scripts/gen-python-protos.(ps1|sh)`, CI workflow.

## How to verify

```bash
mvn -q verify
```

Green, all 9 reactor entries (parent + 8 modules).
`predisched-common/target/generated-sources/protobuf` contains `Ack.java`.

## Design choices

- Common owns generated protos; other modules depend on common, never vice versa.
- `nodeId` is a String so scheduler ids (`scheduler-1`) and numeric election ids
  can coexist later.
- `Channels` registers a shutdown hook so tests and CLIs never leak gRPC threads.
