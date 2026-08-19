"""The type map, the envelope walk, and additive reconciliation.

These are the tests that make DATA-714's claim checkable: that the sink's shape
is the policy's shape, and that a column type nobody thought about is a loud
failure rather than a `text` column returning wrong answers.
"""

from __future__ import annotations

import pytest

from pit import ddl

# ---------------------------------------------------------------------------
# the type map
# ---------------------------------------------------------------------------

ZONED = {"type": "string", "connect.version": 1, "connect.name": ddl.ZONED_TIMESTAMP}
ENUM = {
    "type": "string",
    "connect.version": 1,
    "connect.parameters": {"allowed": "scheduled,completed"},
    "connect.name": ddl.ENUM,
}
DECIMAL_12_2 = {
    "type": "bytes",
    "scale": 2,
    "precision": 12,
    "connect.version": 1,
    "connect.parameters": {"scale": "2", "connect.decimal.precision": "12"},
    "connect.name": ddl.DECIMAL,
    "logicalType": "decimal",
}
CODE_ARRAY = {"type": "array", "items": ["null", "string"]}


@pytest.mark.parametrize(
    "avro_type, expected",
    [
        ("string", "text"),
        (["null", "string"], "text"),
        ("int", "integer"),
        (["null", "int"], "integer"),
        ("long", "bigint"),
        ("boolean", "boolean"),
        ("double", "double precision"),
        ("bytes", "bytea"),
        # The one every timestamp in this schema actually is. `timestamptz`, not
        # `text` -- a ZonedTimestamp is a string on the wire and an instant in
        # meaning, and storing the wire type would break every date query.
        (ZONED, "timestamptz"),
        (["null", ZONED], "timestamptz"),
        # Enum is a string with an `allowed` parameter. The source constraint
        # does not survive CDC, so the sink takes plain text.
        (ENUM, "text"),
        # Money. `bytea` here is DATA-714's worked example of getting it wrong.
        (DECIMAL_12_2, "numeric(12,2)"),
        (["null", DECIMAL_12_2], "numeric(12,2)"),
        (CODE_ARRAY, "text[]"),
        # Both spellings of the date/time logical names appear on the wire:
        # `time.precision.mode=connect` uses Connect's for date and timestamp,
        # while timestamptz stays Debezium's ZonedTimestamp either way.
        ({"type": "int", "connect.name": ddl.CONNECT_DATE, "logicalType": "date"}, "date"),
        ({"type": "int", "connect.name": ddl.DEBEZIUM_DATE}, "date"),
        ({"type": "long", "connect.name": ddl.CONNECT_TIMESTAMP}, "timestamptz"),
        ({"type": "long", "connect.name": ddl.MICRO_TIMESTAMP}, "timestamptz"),
        ({"type": "string", "connect.name": ddl.JSON_NAME}, "jsonb"),
    ],
)
def test_pg_type(avro_type, expected):
    assert ddl.pg_type(avro_type) == expected


def test_decimal_falls_back_to_connect_parameters():
    """Avro's own precision/scale win, but the Connect parameters are enough."""
    only_parameters = {
        "type": "bytes",
        "logicalType": "decimal",
        "connect.name": ddl.DECIMAL,
        "connect.parameters": {"scale": "4", "connect.decimal.precision": "18"},
    }
    assert ddl.pg_type(only_parameters) == "numeric(18,4)"


def test_decimal_without_precision_is_unconstrained():
    """Wider than the source column, but never wrong."""
    assert ddl.pg_type({"type": "bytes", "logicalType": "decimal"}) == "numeric"


@pytest.mark.parametrize(
    "avro_type, because",
    [
        (["string", "long"], "two non-null branches have no single column type"),
        ({"type": "array", "items": {"type": "array", "items": "string"}}, "nested array"),
        ({"type": "record", "name": "Nested", "fields": []}, "a nested record"),
        ("unheard-of", "an unknown primitive"),
    ],
)
def test_unmapped_types_raise(avro_type, because):
    """Refused, not defaulted to text.

    A column silently stored as text is a column whose queries quietly return
    the wrong answer, which is the failure this whole module exists to prevent.
    """
    with pytest.raises(ddl.UnmappedAvroType):
        ddl.pg_type(avro_type)


@pytest.mark.parametrize(
    "avro_type, nullable",
    [("string", False), (["null", "string"], True), (["string", "null"], True), (ZONED, False)],
)
def test_nullability_comes_from_the_union(avro_type, nullable):
    assert ddl.is_nullable(avro_type) is nullable


# ---------------------------------------------------------------------------
# reading the envelope
# ---------------------------------------------------------------------------


def test_record_is_read_from_before_not_after(clean_schemas):
    """`before` defines the record; `after` is a bare name reference to it.

    Verified against all five registered schemas. The project description's
    "Avro named-type reuse" guardrail states this backwards, so a walker that
    trusted `after` would find a string where it wanted fields.
    """
    for table, (_key, value) in clean_schemas.items():
        after = next(f for f in value["fields"] if f["name"] == "after")
        assert "Value" in after["type"], f"{table}: after should reference Value by name"
        assert not any(isinstance(b, dict) for b in after["type"]), (
            f"{table}: after should be a reference, not a second definition"
        )
        record = ddl.value_record(value)
        assert record["fields"], f"{table}: no fields found"


def test_envelope_without_a_record_definition_raises():
    with pytest.raises(ddl.UnmappedAvroType):
        ddl.value_record({"fields": [{"name": "after", "type": ["null", "Value"]}]})


def test_source_block_survives_untouched(clean_schemas):
    """PIT resolves a point in time from ``source.ts_ms``.

    A policy rule against the source block, or a derivation that renamed it,
    would take the timestamp index with it. This asserts the block is still there
    and still Debezium's own, in every clean schema.
    """
    for table, (_key, value) in clean_schemas.items():
        source = next(f for f in value["fields"] if f["name"] == "source")
        branches = [b for b in ddl.branches(source["type"]) if b != "null"]
        assert branches, f"{table}: source has no record"
        block = branches[0]
        assert block["namespace"] == "io.debezium.connector.postgresql"
        assert "ts_ms" in [f["name"] for f in block["fields"]], f"{table}: source.ts_ms is gone"


def test_table_name_from_namespace():
    assert ddl.table_name({"namespace": "clean.public.patients"}) == ("public", "patients")


def test_table_name_needs_a_schema_and_table():
    with pytest.raises(ddl.UnmappedAvroType):
        ddl.table_name({"namespace": "clean"})


def test_primary_key_comes_from_the_key_schema(clean_schemas):
    key, value = clean_schemas["patients"]
    table = ddl.read_table(key, value)
    assert table.primary_key == ("patient_id",)
    # Hashed by the policy, so text -- not the source's bigint. This is the
    # argument for generating DDL from the registry rather than the source.
    assert table.column("patient_id").sql_type == "text"


def test_primary_key_is_not_null_even_when_the_value_says_otherwise():
    """`date_shift` and friends widen a payload column; a key column may not widen.

    Nullability in the value schema is about whether a *value* can be read. For
    the column a row is identified by, null is not a value it can take.
    """
    key = {"namespace": "clean.public.t", "fields": [{"name": "id", "type": "string"}]}
    value = {
        "namespace": "clean.public.t",
        "fields": [
            {
                "name": "before",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Value",
                        "fields": [
                            {"name": "id", "type": ["null", "string"]},
                            {"name": "payload", "type": ["null", "string"]},
                        ],
                    },
                ],
            }
        ],
    }
    table = ddl.read_table(key, value)
    assert table.column("id").nullable is False
    assert table.column("payload").nullable is True


def test_key_naming_a_field_the_value_lacks_raises():
    key = {"namespace": "clean.public.t", "fields": [{"name": "missing", "type": "string"}]}
    value = {
        "namespace": "clean.public.t",
        "fields": [
            {
                "name": "before",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Value",
                        "fields": [{"name": "id", "type": "string"}],
                    },
                ],
            }
        ],
    }
    with pytest.raises(ddl.UnmappedAvroType, match="no such field"):
        ddl.read_table(key, value)


# ---------------------------------------------------------------------------
# the tables this policy actually produces
# ---------------------------------------------------------------------------


def test_dropped_columns_have_no_sink_column(clean_schemas):
    """What the policy drops, the sink never has a column for.

    Not nulled, not redacted -- absent. A column that exists and is empty is a
    column someone will later try to backfill.
    """
    dropped = {
        "patients": ["ssn", "address_line1", "address_line2", "city"],
        "appointments": ["location", "intake_answers"],
        "notes": ["body"],
    }
    for table_name, columns in dropped.items():
        table = ddl.read_table(*clean_schemas[table_name])
        for column in columns:
            assert table.column(column) is None, f"{table_name}.{column} should not exist"


def test_claims_amounts_are_numeric_not_bytea(clean_schemas):
    table = ddl.read_table(*clean_schemas["claims"])
    for column in ("billed_amount", "allowed_amount", "paid_amount", "patient_responsibility"):
        assert table.column(column).sql_type == "numeric(12,2)"


def test_diagnosis_codes_is_a_text_array(clean_schemas):
    table = ddl.read_table(*clean_schemas["claims"])
    assert table.column("diagnosis_codes").sql_type == "text[]"


def test_timestamps_are_timestamptz_and_nullable(clean_schemas):
    """Nullable even where the source column is NOT NULL.

    `date_shift` widens a ZonedTimestamp because a string is not necessarily an
    instant (deid/src/deid/ops.py). The sink follows the registered schema rather
    than second-guessing it.
    """
    table = ddl.read_table(*clean_schemas["appointments"])
    for column in ("scheduled_at", "checked_in_at", "completed_at", "created_at", "updated_at"):
        assert table.column(column).sql_type == "timestamptz"
        assert table.column(column).nullable is True


def test_every_primary_key_is_text(clean_schemas):
    for table_name in clean_schemas:
        table = ddl.read_table(*clean_schemas[table_name])
        for key in table.primary_key:
            assert table.column(key).sql_type == "text", f"{table_name}.{key}"


def test_create_table_has_a_primary_key_and_no_foreign_keys(clean_schemas):
    """No FKs: per-table topics replay independently, so order is not guaranteed."""
    for table_name in clean_schemas:
        statement = ddl.create_table(ddl.read_table(*clean_schemas[table_name]))
        assert "primary key" in statement
        assert "references" not in statement.lower()
        assert "foreign key" not in statement.lower()


# ---------------------------------------------------------------------------
# reconciliation
# ---------------------------------------------------------------------------


def table_of(clean_schemas, name):
    return ddl.read_table(*clean_schemas[name])


def test_reconcile_creates_a_missing_table(clean_schemas):
    statements = ddl.reconcile(table_of(clean_schemas, "providers"), existing={})
    assert len(statements) == 1
    assert statements[0].startswith("create table if not exists")


def test_reconcile_is_a_no_op_when_the_table_matches(clean_schemas):
    table = table_of(clean_schemas, "providers")
    live = {column.name: column.sql_type for column in table.columns}
    assert ddl.reconcile(table, live) == []


def test_reconcile_adds_a_new_column(clean_schemas):
    """The case M4's headline demo produces.

    Cover a new source column in the policy, M4 un-halts the topic, and the next
    record carries a field the sink has no column for. Additive is the only
    correct answer, and it has to be automatic or the demo stalls.
    """
    table = table_of(clean_schemas, "providers")
    live = {c.name: c.sql_type for c in table.columns if c.name != "specialty"}
    statements = ddl.reconcile(table, live)
    assert len(statements) == 1
    assert statements[0].startswith("alter table")
    assert '"specialty" text' in statements[0]


def test_added_columns_are_nullable(clean_schemas):
    """A column added to a populated table cannot be NOT NULL without a default.

    Inventing a default for a de-identified column would be inventing data.
    """
    table = table_of(clean_schemas, "providers")
    live = {c.name: c.sql_type for c in table.columns if c.name != "full_name"}
    assert table.column("full_name").nullable is False
    statement = ddl.reconcile(table, live)[0]
    assert "not null" not in statement


def test_reconcile_refuses_a_type_change(clean_schemas):
    """A safe failure, not a solved case.

    There is no rule for what the existing values should become, so guessing one
    would rewrite data on the strength of an assumption.
    """
    table = table_of(clean_schemas, "claims")
    live = {c.name: c.sql_type for c in table.columns}
    live["billed_amount"] = "text"
    with pytest.raises(ddl.IncompatibleSinkSchema, match="billed_amount"):
        ddl.reconcile(table, live)


def test_reconcile_refuses_a_vanished_column(clean_schemas):
    """A policy that starts dropping a column has to take the old values with it.

    Leaving them keeps exactly the PHI the change was meant to remove, so this
    stops rather than quietly preserving it.
    """
    table = table_of(clean_schemas, "providers")
    live = {c.name: c.sql_type for c in table.columns}
    live["ssn"] = "text"
    with pytest.raises(ddl.IncompatibleSinkSchema, match="ssn"):
        ddl.reconcile(table, live)


@pytest.mark.parametrize(
    "declared, live",
    [
        ("timestamptz", "timestamp with time zone"),
        ("numeric(12,2)", "numeric"),
        ("integer", "int4"),
        ("text[]", "text[]"),
        ("boolean", "bool"),
        ("double precision", "float8"),
    ],
)
def test_types_agree_across_information_schema_spellings(declared, live):
    """`information_schema` spells types differently from the DDL that made them.

    Without this, every reconcile would report a type change on every run.
    """
    assert ddl.types_agree(declared, live)


def test_types_disagree_when_they_really_differ():
    assert not ddl.types_agree("timestamptz", "text")
    assert not ddl.types_agree("text", "text[]")


# ---------------------------------------------------------------------------
# loading from disk
# ---------------------------------------------------------------------------


def test_tables_from_dir_reads_every_fixture(clean_dir):
    tables = ddl.tables_from_dir(clean_dir)
    assert {f"{t.schema}.{t.name}" for t in tables} == {
        "public.patients",
        "public.providers",
        "public.appointments",
        "public.claims",
        "public.notes",
    }


def test_tables_from_dir_needs_both_halves(tmp_path, clean_dir):
    """The primary key comes from the key schema, so a value alone is not enough."""
    (tmp_path / "public.patients-value.json").write_text(
        (clean_dir / "public.patients-value.json").read_text()
    )
    with pytest.raises(FileNotFoundError, match="key schema"):
        ddl.tables_from_dir(tmp_path)


def test_tables_from_dir_on_an_empty_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        ddl.tables_from_dir(tmp_path)
