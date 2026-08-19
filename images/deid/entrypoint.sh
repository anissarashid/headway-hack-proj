#!/usr/bin/env bash
# The de-identification transformer. Arguments are passed through, so the same
# image runs the acceptance check's bounded modes:
#
#   kubectl run ... -- --dry-run          # derive, create topics, register, stop
#   kubectl run ... -- --idle-timeout 20  # drain what is there and stop
#
# Every setting other than those comes from the environment; see Config.from_env
# in deid/runner.py and the deid chart's Deployment.
set -euo pipefail

# A hard kill here is safe by design rather than by luck: offsets are committed
# only for records the broker has acknowledged, so whatever was in flight is
# re-read on the next start and re-produced. The applier upserts, so a duplicate
# is a no-op. That is what makes `uv run` acceptable as PID 1.
# --no-sync: the venv was built at image build time from the lock file, so there
# is nothing to resolve at startup. Without it uv re-installs the project on
# every container start, which is slower and writes to the image's venv.
exec uv run --no-sync python -m deid.runner "$@"
