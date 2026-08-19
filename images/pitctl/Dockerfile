# pit CLI: initdb, tail, replay, snapshot, restore
#
# M1 builds a runnable base so the build/load/reload loop is exercised now.
# Application code and dependencies land in M5-M7 (DATA-714 onward).
#
# All Python runs through `uv run` -- never bare python3 or pytest.
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# M5-M7 (DATA-714 onward): COPY pyproject.toml uv.lock, then `uv sync --frozen --no-dev`, then the
# package source. Kept out of M1 so the image builds before the code exists.
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

LABEL org.opencontainers.image.title="pit-pitctl" \
      org.opencontainers.image.description="pit CLI: initdb, tail, replay, snapshot, restore"

ENTRYPOINT ["/app/entrypoint.sh"]
