#!/usr/bin/env bash
# Generate Python gRPC stubs from the .proto files.
#
# Run from the project root:
#   bash scripts/gen_proto.sh
#
# The generated files land in dfs/proto/ so they are importable as
# dfs.proto.master_pb2, dfs.proto.datanode_pb2, etc.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

python -m grpc_tools.protoc \
    -I proto \
    --python_out=dfs/proto \
    --grpc_python_out=dfs/proto \
    proto/master.proto \
    proto/datanode.proto

echo "Generated files in dfs/proto/:"
ls dfs/proto/*.py
