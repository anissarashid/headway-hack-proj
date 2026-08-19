# De-identification transformer: raw.* -> clean.*
#
# M1 builds a runnable base so the build/load/reload loop is exercised now.
# Application code and dependencies land in M4 (DATA-709/710/711/712).
#
# All Python runs through `uv run` -- never bare python3 or pytest.
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# M4 (DATA-709/710/711/712): COPY pyproject.toml uv.lock, then `uv sync --frozen --no-dev`, then the
# package source. Kept out of M1 so the image builds before the code exists.
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

LABEL org.opencontainers.image.title="pit-deid" \
      org.opencontainers.image.description="De-identification transformer: raw.* -> clean.*"

ENTRYPOINT ["/app/entrypoint.sh"]
