#!/usr/bin/env bash
set -euo pipefail

# Fast (~30-60s) MinIO docker-backed test runner for Litestream integration tests.
# Builds required binaries and runs the shortest MinIO integration test.

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is not installed or not in PATH" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not running" >&2
  exit 1
fi

echo "Building litestream binaries..."
go build -o "$ROOT/bin/litestream" ./cmd/litestream
go build -o "$ROOT/bin/litestream-test" ./cmd/litestream-test

echo ""
echo "Starting MinIO integration tests..."
echo "=================================="

echo ""
echo "Test 1: MinIO without SSL (TestS3AccessPointLocalStack)..."
go test -v -tags=integration -run '^TestS3AccessPointLocalStack$' -timeout=5m ./tests/integration

echo ""
echo "Test 2: MinIO with SSL (TestS3AccessPointLocalStackSSL)..."
go test -v -tags=integration -run '^TestS3AccessPointLocalStackSSL$' -timeout=5m ./tests/integration

echo ""
echo "=================================="
echo "All tests completed successfully!"
