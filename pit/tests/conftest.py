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
