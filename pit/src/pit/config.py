"""Where the sink and the registry are, and nothing else.

Two endpoints, resolved the same way whether the caller is a laptop behind
``make forward`` or a pod inside the cluster. The defaults are the laptop, which
is what makes ``make initdb`` a command you can run without arguments; the
in-cluster values arrive as environment variables from the chart.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from psycopg.conninfo import make_conninfo

# The database the applier lands in. Named here rather than defaulted at each
# call site: `pit_base` is what M7 cuts snapshots from and what M6 clones, so it
# is a fact about the system, not a CLI convenience.
BASE_DATABASE = "pit_base"

# The database `pit initdb` connects to in order to create another one. You
# cannot create or drop the database you are connected to, and it must never be
# a payload database for the same reason -- see the sink-pg chart's values.yaml.
MAINTENANCE_DATABASE = "pit_sink"


def sink_dsn(database: str | None = None) -> str:
    """Connection string for the sink Postgres.

    Defaults assume ``make forward`` against the dev release, whose credentials
    live in ``charts/pit/charts/sink-pg/values.yaml``. Note port 5433: the
    forward maps the sink there so it can coexist with the source on 5432.

    Set ``PIT_SINK_DSN`` to point somewhere else. The individual ``PG*``
    variables are honoured too, so this behaves like any other libpq client --
    except that ``database``, when given, always wins, because the caller asking
    for a specific database means it.

    ``PIT_SINK_DSN`` is merged rather than concatenated: it may be a URI
    (``postgresql://...``) as well as a keyword string, and appending
    ``dbname=...`` to a URI produces something libpq cannot parse.
    """
    dsn = os.environ.get("PIT_SINK_DSN")
    if dsn:
        return make_conninfo(dsn, dbname=database) if database else dsn
    host = os.environ.get("PGHOST", "127.0.0.1")
    port = os.environ.get("PGPORT", "5433")
    user = os.environ.get("PGUSER", "pit")
    password = os.environ.get("PGPASSWORD", "pit-dev-password")
    dbname = database or os.environ.get("PGDATABASE", MAINTENANCE_DATABASE)
    return f"host={host} port={port} user={user} password={password} dbname={dbname}"


def registry_url() -> str:
    """Base URL of the schema registry.

    In-cluster it is the broker itself -- Redpanda serves the registry on 8081
    rather than as a separate service. ``make forward`` maps the same port to
    localhost.
    """
    return os.environ.get("PIT_REGISTRY_URL", "http://localhost:8081").rstrip("/")


@dataclass(frozen=True)
class Settings:
    """Resolved endpoints for one invocation."""

    database: str = BASE_DATABASE
    registry: str = ""

    @classmethod
    def resolve(cls, *, database: str | None = None, registry: str | None = None) -> Settings:
        return cls(
            database=database or BASE_DATABASE,
            registry=(registry or registry_url()).rstrip("/"),
        )
