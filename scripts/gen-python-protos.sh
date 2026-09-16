#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROTO_DIR="$ROOT/proto"
for target in ml/generated mpi/generated spark/generated; do
  mkdir -p "$ROOT/$target"
  touch "$ROOT/$target/__init__.py"
  python -m grpc_tools.protoc -I "$PROTO_DIR" --python_out="$ROOT/$target" --grpc_python_out="$ROOT/$target" "$PROTO_DIR"/*.proto
done
echo "Python stubs generated."
