#!/usr/bin/env bash
# Placeholder entrypoint. Proves the image builds, loads into minikube and runs.
# Replaced by the real deid command in M4 (DATA-709/710/711/712).
set -euo pipefail
echo "[deid] placeholder image -- no work to do until M4 (DATA-709/710/711/712)"
echo "[deid] uv: $(uv --version)"
echo "[deid] python: $(python --version)"
exec sleep infinity
