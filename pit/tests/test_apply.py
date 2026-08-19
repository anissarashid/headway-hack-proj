"""Statements -> SQL, and the properties that make a replay safe to re-run.

The SQL-shape tests need nothing. The rest run against a scratch database in a
live sink and skip without ``PIT_TEST_SINK_DSN`` -- `make pit-check` sets it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pit import applier, ddl, envelope

psycopg = pytest.importorskip("psycopg")


@pytest.fixture
def claims(clean_schemas) -> ddl.Table:
    return ddl.read_table(*clean_schemas["claims"])


# ---------------------------------------------------------------------------
# the SQL, without a database
# ---------------------------------------------------------------------------


def test_upsert_conflicts_on_the_primary_key(claims):
    sql = applier.upsert_sql(claims, ["claim_id", "patient_id", "billed_amount"])
    assert 'on conflict ("claim_id")' in sql
    assert 'do update set "patient_id" = excluded."patient_id"' in sql
    # The key is never in the update list: assigning it to itself is noise, and
    # on a compound key it would let one column of the key be rewritten.
    assert '"claim_id" = excluded' not in sql


def test_upsert_writes_only_the_columns_the_record_carried(claims):
    """A record predating an added column must not null it out.

    Listing every table column would set the missing ones to NULL on every
    update, which is a slow way to lose data that was correct.
    """
    sql = applier.upsert_sql(claims, ["claim_id", "billed_amount"])
    assert "procedure_code" not in sql


def test_upsert_on_an_all_key_table_does_nothing_on_conflict():
    """Nothing to update a row to when every column identifies it."""
    table = ddl.Table(
        schema="public",
        name="link",
        columns=(ddl.Column("a", "text", False), ddl.Column("b", "text", False)),
        primary_key=("a", "b"),
    )
    sql = applier.upsert_sql(table, ["a", "b"])
    assert "do nothing" in sql


def test_delete_matches_every_key_column():
    table = ddl.Table(
        schema="public",
        name="link",
        columns=(ddl.Column("a", "text", False), ddl.Column("b", "text", False)),
        primary_key=("a", "b"),
    )
    sql = applier.delete_sql(table)
    assert sql.count("%s") == 2
    assert '"a" = %s and "b" = %s' in sql


def test_identifiers_are_quoted():
    """A column called `end` or `order` is legal Avro and reserved SQL."""
    table = ddl.Table(
        schema="public",
        name="order",
        columns=(ddl.Column("end", "text", True), ddl.Column("id", "text", False)),
        primary_key=("id",),
    )
    assert '"public"."order"' in applier.upsert_sql(table, ["id", "end"])
    assert '"end"' in applier.upsert_sql(table, ["id", "end"])


# ---------------------------------------------------------------------------
# against a live sink
# ---------------------------------------------------------------------------


@pytest.fixture
def conn(sink_dsn, clean_dir):
    """A connection to a scratch schema with the sink tables in it.

    A schema rather than a database, so the test needs no CREATEDB and leaves
    nothing behind if it dies.
    """
    with psycopg.connect(sink_dsn) as connection:
        tables = ddl.tables_from_dir(clean_dir)
        # Rebase every table into a scratch schema so a live pit_base is untouched.
        scratch = tuple(
            ddl.Table(
                schema="pit_test",
                name=table.name,
                columns=table.columns,
                primary_key=table.primary_key,
            )
            for table in tables
        )
        with connection.cursor() as cursor:
            cursor.execute("drop schema if exists pit_test cascade")
        connection.commit()
        ddl.ensure_schema(connection, scratch)
        connection.scratch_tables = {table.name: table for table in scratch}
        try:
            yield connection
        finally:
            with connection.cursor() as cursor:
                cursor.execute("drop schema if exists pit_test cascade")
            connection.commit()


def claim_row(claim_id="cl_0001", amount="1234.56", codes=("E11", "I10")):
    return {
        "claim_id": claim_id,
        "patient_id": "pt_9f2a",
        "appointment_id": None,
        "billed_amount": Decimal(amount),
        "allowed_amount": None,
        "paid_amount": None,
        "patient_responsibility": None,
        "diagnosis_codes": list(codes),
        "procedure_code": "99213",
        "claim_status": "submitted",
        "submitted_at": "2026-08-19T14:00:00.000000Z",
        "adjudicated_at": None,
        "created_at": "2026-08-19T14:00:00.000000Z",
        "updated_at": "2026-08-19T14:00:00.000000Z",
    }


def fetch(conn, table, claim_id="cl_0001"):
    with conn.cursor() as cursor:
        cursor.execute(f"select * from {table.qualified} where claim_id = %s", (claim_id,))
        columns = [description.name for description in cursor.description]
        found = cursor.fetchone()
    return dict(zip(columns, found)) if found else None


def test_avro_logical_types_land_as_real_postgres_types(conn):
    """No manual coercion anywhere in the applier.

    A Decimal goes into numeric, a list of str into text[], and the
    ZonedTimestamp *string* into timestamptz because Postgres parses ISO-8601
    with an offset natively. If any of those needed hand-conversion, the applier
    would be interpreting values rather than moving them.
    """
    table = conn.scratch_tables["claims"]
    applier.apply(conn, [envelope.Upsert(table, {"claim_id": "cl_0001"}, claim_row())])

    stored = fetch(conn, table)
    assert stored["billed_amount"] == Decimal("1234.56")
    assert stored["diagnosis_codes"] == ["E11", "I10"]
    assert stored["submitted_at"] == datetime(2026, 8, 19, 14, 0, tzinfo=timezone.utc)


def test_applying_the_same_batch_twice_converges(conn):
    """The property that makes a crash between two commits harmless.

    The data and the offsets commit together; Kafka's offsets do not. So a run
    that dies in between replays the tail of a batch on restart, and that has to
    be a no-op rather than a duplicate-key failure.
    """
    table = conn.scratch_tables["claims"]
    batch = [
        envelope.Upsert(table, {"claim_id": "cl_0001"}, claim_row()),
        envelope.Upsert(table, {"claim_id": "cl_0002"}, claim_row(claim_id="cl_0002")),
    ]
    applier.apply(conn, batch)
    first = [fetch(conn, table, "cl_0001"), fetch(conn, table, "cl_0002")]

    applier.apply(conn, batch)
    second = [fetch(conn, table, "cl_0001"), fetch(conn, table, "cl_0002")]

    assert first == second
    assert applier.row_counts(conn, [table])["pit_test.claims"] == 2


def test_update_then_delete_of_the_same_key_leaves_the_row_deleted(conn):
    """Order within a batch is the correctness argument, so it is preserved.

    Grouping same-shaped statements into an executemany would be faster and would
    reorder them; an upsert followed by a delete is a deleted row and the reverse
    is a live one.
    """
    table = conn.scratch_tables["claims"]
    applier.apply(
        conn,
        [
            envelope.Upsert(table, {"claim_id": "cl_0001"}, claim_row()),
            envelope.Delete(table, {"claim_id": "cl_0001"}),
        ],
    )
    assert fetch(conn, table) is None


def test_deleting_a_row_that_is_not_there_is_not_an_error(conn):
    """Replaying a range that starts after the insert still has to converge."""
    table = conn.scratch_tables["claims"]
    result = applier.apply(conn, [envelope.Delete(table, {"claim_id": "nonexistent"})])
    assert result.deletes == 1


def test_a_source_delete_removes_the_sink_row(conn, clean_schemas):
    """DATA-715's third acceptance criterion, through the whole translation."""
    table = conn.scratch_tables["claims"]
    key = {"claim_id": "cl_0001"}
    applier.apply(conn, [envelope.Upsert(table, key, claim_row())])
    assert fetch(conn, table) is not None

    delete_event = {
        "op": "d",
        "before": claim_row(),
        "after": None,
        "source": {"ts_ms": 1755612000000},
    }
    applier.apply(conn, [envelope.translate(table, key, delete_event)])
    assert fetch(conn, table) is None


def test_offsets_commit_with_the_data(conn):
    """The manifest lives inside the payload database, in the same transaction.

    That is what makes `CREATE DATABASE ... TEMPLATE pit_base` produce a clone
    carrying the position it was cut at, with no window for the two to disagree.
    """
    table = conn.scratch_tables["claims"]
    applier.apply(
        conn,
        [envelope.Upsert(table, {"claim_id": "cl_0001"}, claim_row())],
        offsets=[applier.Offset("clean.public.claims", 0, 42)],
    )
    assert applier.applied_offsets(conn) == {("clean.public.claims", 0): 42}


def test_offsets_advance_rather_than_duplicate(conn):
    applier.apply(conn, [], offsets=[applier.Offset("clean.public.claims", 0, 42)])
    applier.apply(conn, [], offsets=[applier.Offset("clean.public.claims", 0, 99)])
    assert applier.applied_offsets(conn) == {("clean.public.claims", 0): 99}


def test_skipped_statements_are_counted_not_applied(conn):
    result = applier.apply(conn, [None, None])
    assert result == applier.Applied(upserts=0, deletes=0, skipped=2)


def test_ensure_schema_is_idempotent(conn, clean_dir):
    """`pit initdb` twice in a row, and `pit tail` calling it at every startup."""
    tables = tuple(conn.scratch_tables.values())
    assert ddl.ensure_schema(conn, tables) == ddl.ensure_schema(conn, tables)


def test_ensure_schema_adds_a_column_to_a_populated_table(conn):
    """The M4-adds-a-column path, end to end against a real table with rows in it."""
    table = conn.scratch_tables["providers"]
    applier.apply(
        conn,
        [
            envelope.Upsert(
                table,
                {"provider_id": "pv_0001"},
                {"provider_id": "pv_0001", "npi": "npi_x", "full_name": "A. Provider"},
            )
        ],
    )

    grown = ddl.Table(
        schema=table.schema,
        name=table.name,
        columns=table.columns + (ddl.Column("newly_covered", "text", True),),
        primary_key=table.primary_key,
    )
    ddl.ensure_schema(conn, [grown])

    live = ddl.live_columns(conn, grown)
    assert "newly_covered" in live
    with conn.cursor() as cursor:
        cursor.execute(f"select count(*) from {grown.qualified}")
        assert cursor.fetchone()[0] == 1


def test_ensure_schema_refuses_a_type_change_against_a_live_table(conn):
    table = conn.scratch_tables["claims"]
    broken = ddl.Table(
        schema=table.schema,
        name=table.name,
        columns=tuple(
            ddl.Column(c.name, "text" if c.name == "billed_amount" else c.sql_type, c.nullable)
            for c in table.columns
        ),
        primary_key=table.primary_key,
    )
    with pytest.raises(ddl.IncompatibleSinkSchema, match="billed_amount"):
        ddl.ensure_schema(conn, [broken])
