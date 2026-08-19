"""Shared fixtures.

The clean schemas are loaded from ``tests/fixtures/clean``, which stands in for
what M4 will register. See that directory's README for how they are derived and
why they are trustworthy.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
CLEAN = FIXTURES / "clean"
RAW = FIXTURES / "raw"

TABLES = ("patients", "providers", "appointments", "claims", "notes")


def load(directory: Path, table: str, kind: str) -> dict:
    return json.loads((directory / f"public.{table}-{kind}.json").read_text())


@pytest.fixture(scope="session")
def clean_schemas() -> dict[str, tuple[dict, dict]]:
    """``table -> (key_schema, value_schema)`` for every clean fixture."""
    return {table: (load(CLEAN, table, "key"), load(CLEAN, table, "value")) for table in TABLES}


@pytest.fixture(scope="session")
def raw_schemas() -> dict[str, tuple[dict, dict]]:
    """The same, for the raw schemas Debezium registered."""
    return {table: (load(RAW, table, "key"), load(RAW, table, "value")) for table in TABLES}


@pytest.fixture(scope="session")
def clean_dir() -> Path:
    return CLEAN


@pytest.fixture
def sink_dsn() -> str:
    """DSN for a live sink, or skip.

    Same shape as loadgen's ``PIT_TEST_DSN``: the tests that need a database say
    so, and skip cleanly on a laptop with no cluster running rather than failing
    and hiding the tests that do not.
    """
    dsn = os.environ.get("PIT_TEST_SINK_DSN")
    if not dsn:
        pytest.skip("set PIT_TEST_SINK_DSN to run the tests that need a live sink")
    return dsn


def reset_offsets(conn) -> None:
    """Empty ``pit_meta.applied_offsets``.

    Every test in this suite shares one database, and unlike the payload tables
    the bookkeeping table is not namespaced per test -- there is deliberately one
    per database, because it describes the database. So a test that writes an
    offset leaks into any later test that asserts on the whole table.

    Call this at both ends of a fixture that touches offsets. Truncate rather than
    drop: the table is what `ensure_schema` created, and recreating it would test
    the fixture instead of the code.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            "select 1 from information_schema.tables "
            "where table_schema = 'pit_meta' and table_name = 'applied_offsets'"
        )
        if cursor.fetchone():
            cursor.execute('truncate table pit_meta."applied_offsets"')
    conn.commit()
