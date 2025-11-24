#!/usr/bin/env bash
set -euo pipefail

# Determine repo root (parent of this script directory)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Archives will be stored under prompt/
ARCHIVE_DIR="${ROOT_DIR}/prompt"
mkdir -p "$ARCHIVE_DIR"

CODE_ARCHIVE_NAME="1-schemas.tar.gz"
CODE_ARCHIVE_PATH="${ARCHIVE_DIR}/${CODE_ARCHIVE_NAME}"

BENCH_ARCHIVE_NAME="2-benchmark.tar.gz"
BENCH_ARCHIVE_PATH="${ARCHIVE_DIR}/${BENCH_ARCHIVE_NAME}"

# Go to repo root so tar paths are clean
cd "$ROOT_DIR"

# Create tar.gz for project code:
# - docker/docker-compose.base.yaml
# - docker/docker-compose.yaml
# - cmd/* (the whole cmd directory)
tar -czf "$CODE_ARCHIVE_PATH" \
  docker/docker-compose.base.yaml \
  docker/docker-compose.yaml \
  cmd

echo "Code archive created: $CODE_ARCHIVE_PATH"

# Create tar.gz for benchmark artifacts:
# Includes all logs and env file under benchmark/
if [[ -d benchmark ]]; then
  tar -czf "$BENCH_ARCHIVE_PATH" benchmark
  echo "Benchmark archive created: $BENCH_ARCHIVE_PATH"
else
  echo "Directory 'benchmark' not found, skipping benchmark archive." >&2
fi
