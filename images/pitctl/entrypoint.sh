#!/usr/bin/env bash
# Placeholder entrypoint. Proves the image builds, loads into minikube and runs.
# Replaced by the real pitctl command in M5-M7 (DATA-714 onward).
set -euo pipefail
echo "[pitctl] placeholder image -- no work to do until M5-M7 (DATA-714 onward)"
echo "[pitctl] uv: $(uv --version)"
echo "[pitctl] python: $(python --version)"
exec sleep infinity
