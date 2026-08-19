# De-identification transformer: raw.* -> clean.*
#
# Built from the repo root, not from this directory: the image needs the `deid`
# package, and Docker cannot COPY above its context.
#
#     docker build -f images/deid/Dockerfile -t pit/deid:dev .   # `make build-deid`
#
# The policy is deliberately NOT in the image. It is mounted from a ConfigMap at
# /etc/deid/clinic.yml, so the auditable artifact can be reviewed and changed
# without rebuilding anything -- and so there is only one copy of it.
#
# All Python runs through `uv run` -- never bare python3 or pytest.
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /bin/

# HOME and UV_CACHE_DIR under /app because the container runs as a non-root user
# with no home directory: uv wants a writable cache, and the default
# ~/.cache/uv is not writable for it.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR=/app/.cache/uv \
    HOME=/app \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first and on their own layer, so editing the transformer does not
# re-resolve confluent-kafka and fastavro on every build. --frozen: the lock file
# is the source of truth, and a build that silently re-resolved it could ship a
# different fastavro than the tests ran against.
COPY deid/pyproject.toml deid/uv.lock /app/
RUN uv sync --frozen --no-dev --no-install-project

COPY deid/src /app/src
RUN uv sync --frozen --no-dev

COPY images/deid/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin deid \
 && mkdir -p /app/.cache/uv \
 && chown -R deid:deid /app
USER deid

LABEL org.opencontainers.image.title="pit-deid" \
      org.opencontainers.image.description="De-identification transformer: raw.* -> clean.*"

ENTRYPOINT ["/app/entrypoint.sh"]
