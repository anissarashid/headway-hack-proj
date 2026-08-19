# pit CLI: initdb, tail, and later replay, snapshot, restore.
#
# One image for all of them. `pit-tail` runs it as a Deployment, M7's snapshot
# CronJob and M6's restore Job run it as one-shot Jobs, and they want the same
# code -- a second image would be a second thing to keep in step.
#
# **Built from the repo root**, not from this directory:
#
#     docker build -f images/pitctl/Dockerfile .
#
# A Dockerfile cannot COPY from outside its build context, and the code it has to
# package lives in pit/. `make build-pitctl` does this; the root .dockerignore is
# what keeps the resulting context small.
#
# All Python runs through `uv`, never bare python3 or pytest.
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# kubectl, for one specific job: `CREATE DATABASE ... TEMPLATE pit_base` fails
# while any connection to pit_base is open, so M7's snapshot CronJob scales
# pit-tail to 0, cuts the clone, and scales it back. That is also what the Role in
# the pitctl chart grants, and nothing more.
#
# Pinned to the minor version of the minikube profile's control plane. A skew of
# one minor either way is supported, so this does not need to track exactly.
ARG KUBECTL_VERSION=v1.34.0
ARG TARGETARCH
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    curl -fsSLo /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH:-amd64}/kubectl"; \
    chmod +x /usr/local/bin/kubectl; \
    kubectl version --client=true; \
    apt-get purge -y --auto-remove curl; \
    rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first, from the lockfile alone, so editing the source does not
# invalidate the layer that installs psycopg. --frozen fails rather than
# re-resolving: the image gets the versions the tests ran against or it gets
# nothing.
COPY pit/pyproject.toml pit/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY pit/src ./src
RUN uv sync --frozen --no-dev

# Not root. The CLI needs a Postgres connection, a registry connection and its own
# ServiceAccount token -- none of which need a privileged user.
RUN useradd --create-home --uid 1000 pit && chown -R pit:pit /app
USER pit

LABEL org.opencontainers.image.title="pit-pitctl" \
      org.opencontainers.image.description="pit CLI: initdb, tail, replay, snapshot, restore"

ENTRYPOINT ["pit"]
CMD ["--help"]
