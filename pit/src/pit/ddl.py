"""Sink DDL, derived from the registered clean Avro schemas.

The sink schema is the post-policy schema. ``ssn`` does not exist, ``ssn``'s
table still does; ``date_of_birth`` is an integer year; every primary key is text
because the policy hashes the ids. Reading the source's ``information_schema``
and re-applying the policy would duplicate logic that already lives in the
registry, and the two copies would disagree the first time a rule changed.

So the registry is the single versioned source of truth for what the sink looks
like, and this module is the translation. Everything above :func:`ensure_schema`
is pure: it takes schema dicts and returns strings, which is why the type map can
be tested exhaustively without a database.

Two rules that are load-bearing rather than stylistic:

**No foreign keys.** Per-table topics replay independently, so referential order
is not guaranteed and any FK would reject a legal replay. Standard for a CDC
sink; M8's join-integrity test is how the relationships get checked instead.

**Schema changes are additive, or they fail.** A new column appears in the clean
schema when someone covers a new source column in the policy, and the sink has to
grow to accept it. A column that changed type or vanished is not a migration this
can reason about, so it raises. That is the same fail-closed shape M4 uses when it
halts a single topic: contained, loud, and never a silent wrong answer.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from . import registry

# Debezium's logical type names, as they arrive in `connect.name`. The unit is
# not guessable from the primitive, so the name is what decides the column type.
#
# Note the spellings. `time.precision.mode=connect` makes Debezium use Kafka
# Connect's names for date/time (`org.apache.kafka.connect.data.*`), but a
# `timestamptz` column is a Debezium ZonedTimestamp either way. Both families
# appear below because both appear on the wire.
ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp"
DEBEZIUM_DATE = "io.debezium.time.Date"
DEBEZIUM_TIMESTAMP = "io.debezium.time.Timestamp"
MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp"
NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp"
CONNECT_DATE = "org.apache.kafka.connect.data.Date"
CONNECT_TIME = "org.apache.kafka.connect.data.Time"
CONNECT_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp"
DECIMAL = "org.apache.kafka.connect.data.Decimal"
JSON_NAME = "io.debezium.data.Json"
ENUM = "io.debezium.data.Enum"

# Logical name -> Postgres type, for the names whose primitive alone would be
# ambiguous. A ZonedTimestamp is a string and a Timestamp is a long, and storing
# either as its primitive would make every date query in the replica wrong.
BY_LOGICAL_NAME: Mapping[str, str] = {
    ZONED_TIMESTAMP: "timestamptz",
    DEBEZIUM_TIMESTAMP: "timestamptz",
    MICRO_TIMESTAMP: "timestamptz",
    NANO_TIMESTAMP: "timestamptz",
    CONNECT_TIMESTAMP: "timestamptz",
    DEBEZIUM_DATE: "date",
    CONNECT_DATE: "date",
    CONNECT_TIME: "time",
    JSON_NAME: "jsonb",
    ENUM: "text",
}

# Avro primitive -> Postgres type, for everything with no logical annotation.
BY_PRIMITIVE: Mapping[str, str] = {
    "boolean": "boolean",
    "int": "integer",
    "long": "bigint",
    "float": "real",
    "double": "double precision",
    "string": "text",
    "bytes": "bytea",
}

# The schema `ensure_schema` keeps the applier's bookkeeping in. It lives inside
# the payload database on purpose -- see applier.py -- so M8's oracle compare and
# leak scan have to exclude it.
META_SCHEMA = "pit_meta"


class UnmappedAvroType(RuntimeError):
    """No Postgres type for this Avro type.

    Raised rather than defaulting to ``text``. A column silently stored as text
    is a column whose queries quietly return the wrong answer, and the whole
    point of deriving DDL from the registry is that the sink's types are the
    policy's types.
    """


class IncompatibleSinkSchema(RuntimeError):
    """The live table cannot be reconciled with the registered schema additively."""


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    nullable: bool

    def definition(self) -> str:
        return f"{quote(self.name)} {self.sql_type}{'' if self.nullable else ' not null'}"


@dataclass(frozen=True)
class Table:
    """Everything needed to create one sink table."""

    schema: str
    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]

    @property
    def qualified(self) -> str:
        return f"{quote(self.schema)}.{quote(self.name)}"

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)


def quote(identifier: str) -> str:
    """Quote an identifier for Postgres.

    Every identifier is quoted, always. The connector's
    `field.name.adjustment.mode=avro` guarantees Avro-legal field names, which is
    a narrower set than Postgres needs quoting for, but a column called `end` or
    `order` is legal Avro and reserved SQL.
    """
    return '"' + identifier.replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# reading an Avro schema
# ---------------------------------------------------------------------------


def is_union(avro_type: Any) -> bool:
    return isinstance(avro_type, (list, tuple))


def branches(avro_type: Any) -> tuple[Any, ...]:
    return tuple(avro_type) if is_union(avro_type) else (avro_type,)


def is_nullable(avro_type: Any) -> bool:
    return any(branch == "null" for branch in branches(avro_type))


def non_null(avro_type: Any) -> Any:
    """The type with its ``null`` branch removed.

    A union of two real branches is returned as-is and will fail the type
    lookup, which is the right outcome: two branches means two possible column
    types, and nothing here should pick one.
    """
    rest = [branch for branch in branches(avro_type) if branch != "null"]
    if not rest:
        return "null"
    return rest[0] if len(rest) == 1 else rest


def primitive_of(avro_type: Any) -> str | None:
    """The underlying Avro kind, with annotations peeled off."""
    if is_union(avro_type):
        return None
    if isinstance(avro_type, str):
        return avro_type
    inner = avro_type.get("type")
    return primitive_of(inner) if inner is not None else None


def logical_of(avro_type: Any) -> str | None:
    """``connect.name`` if present, else Avro's own ``logicalType``."""
    if is_union(avro_type) or not isinstance(avro_type, Mapping):
        return None
    name = avro_type.get("connect.name") or avro_type.get("logicalType")
    return name if isinstance(name, str) else None


def pg_type(avro_type: Any) -> str:
    """The Postgres type for one clean Avro field type.

    Nullability is not part of the answer -- see :func:`is_nullable` -- because a
    column's type and whether it accepts null are separate facts and conflating
    them makes the union handling harder to follow.
    """
    inner = non_null(avro_type)
    if is_union(inner):
        raise UnmappedAvroType(
            f"a union of more than one non-null branch has no single Postgres type: "
            f"{json.dumps(inner, default=str)}"
        )

    kind = primitive_of(inner)
    name = logical_of(inner)

    # Decimal first: it is the one type whose parameters change the column, and
    # storing it by primitive would give bytea holding an unscaled big-endian
    # integer -- which compares equal to nothing and is DATA-714's worked example
    # of the failure.
    if name == DECIMAL or (kind == "bytes" and name == "decimal"):
        return decimal_type(inner)

    if name is not None and name in BY_LOGICAL_NAME:
        return BY_LOGICAL_NAME[name]

    if kind == "array":
        items = inner.get("items", "null") if isinstance(inner, Mapping) else "null"
        element = pg_type(items)
        if element.endswith("[]"):
            raise UnmappedAvroType("nested arrays have no faithful Postgres column type")
        return f"{element}[]"

    if kind == "map":
        return "jsonb"

    if kind in ("record", "enum", "fixed"):
        # A nested record in a clean schema would mean the policy descended into
        # a structure, which no op does yet. jsonb would accept it and quietly
        # change what the column means, so refuse instead.
        raise UnmappedAvroType(
            f"{kind} has no column type here; the policy has no op that produces one"
        )

    if kind in BY_PRIMITIVE:
        return BY_PRIMITIVE[kind]

    raise UnmappedAvroType(
        f"no Postgres type for {json.dumps(inner, default=str)}. Add it to the map in "
        f"ddl.py rather than letting it fall through to text."
    )


def decimal_type(avro_type: Any) -> str:
    """``numeric(p,s)`` from an Avro decimal's precision and scale.

    Debezium's `precise` mode writes precision and scale twice: as Avro's own
    top-level keys, and as strings inside `connect.parameters`. Avro's spelling
    wins; the Connect parameters are the fallback. With neither, an unconstrained
    `numeric` is still correct, just wider than the source column.
    """
    if not isinstance(avro_type, Mapping):
        return "numeric"
    parameters = avro_type.get("connect.parameters") or {}
    precision = avro_type.get("precision", parameters.get("connect.decimal.precision"))
    scale = avro_type.get("scale", parameters.get("scale"))
    if precision is None:
        return "numeric"
    if scale is None:
        return f"numeric({int(precision)})"
    return f"numeric({int(precision)},{int(scale)})"


def value_record(value_schema: Mapping[str, Any]) -> Mapping[str, Any]:
    """The row record the Debezium envelope carries.

    ``before`` holds the record *definition* and ``after`` is a name reference to
    it -- verified against all five registered schemas. The project
    description's "Avro named-type reuse" guardrail states this the other way
    round, so this reads the definition from whichever field actually has it
    instead of trusting either spelling.
    """
    for field_name in ("before", "after"):
        field = next(
            (f for f in value_schema.get("fields", ()) if f.get("name") == field_name), None
        )
        if field is None:
            continue
        for branch in branches(field.get("type")):
            if isinstance(branch, Mapping) and branch.get("type") == "record":
                return branch
    raise UnmappedAvroType(
        "neither before nor after carries a record definition; this does not look "
        "like a Debezium envelope"
    )


def table_name(value_schema: Mapping[str, Any]) -> tuple[str, str]:
    """``(schema, table)`` from the Avro namespace.

    ``clean.public.patients`` -> ``("public", "patients")``. Taken from the
    schema rather than the topic name so this module stays pure and never has to
    know that a topic exists.
    """
    namespace = value_schema.get("namespace") or ""
    parts = [part for part in namespace.split(".") if part]
    if parts and parts[0] == registry.CLEAN_PREFIX.rstrip("."):
        parts = parts[1:]
    if len(parts) < 2:
        raise UnmappedAvroType(
            f"cannot read a schema and table from namespace {namespace!r}; "
            f"expected something like clean.public.patients"
        )
    return parts[-2], parts[-1]


def primary_key(key_schema: Mapping[str, Any]) -> tuple[str, ...]:
    """The primary key columns, from the *key* schema.

    Never from the value. The key is what an upsert conflicts on and what a
    delete matches, and it is why M4 de-identifies the message key as well as
    the payload -- if the two disagreed, upserts would write duplicate rows.
    """
    fields = tuple(f["name"] for f in key_schema.get("fields", ()) if "name" in f)
    if not fields:
        raise UnmappedAvroType("the key schema has no fields, so there is no primary key")
    return fields


def read_table(key_schema: Mapping[str, Any], value_schema: Mapping[str, Any]) -> Table:
    """One sink table, from one topic's pair of clean schemas."""
    schema_name, name = table_name(value_schema)
    keys = primary_key(key_schema)
    record = value_record(value_schema)

    columns = []
    for field in record.get("fields", ()):
        avro_type = field["type"]
        # A primary key column is never null, whatever the value schema says.
        # `date_shift` and friends widen a type to nullable when a *value* might
        # be unreadable, and that widening is right for a payload column and
        # wrong for the one the row is identified by.
        nullable = is_nullable(avro_type) and field["name"] not in keys
        columns.append(
            Column(name=field["name"], sql_type=pg_type(avro_type), nullable=nullable)
        )

    missing = [key for key in keys if not any(c.name == key for c in columns)]
    if missing:
        raise UnmappedAvroType(
            f"{schema_name}.{name}: the key names {', '.join(missing)} but the value has "
            f"no such field. A primary key the rows do not carry cannot be upserted on."
        )
    return Table(schema=schema_name, name=name, columns=tuple(columns), primary_key=keys)


# ---------------------------------------------------------------------------
# emitting DDL
# ---------------------------------------------------------------------------


def create_table(table: Table) -> str:
    """``CREATE TABLE IF NOT EXISTS``, with a primary key and no foreign keys."""
    body = [column.definition() for column in table.columns]
    body.append(f"primary key ({', '.join(quote(key) for key in table.primary_key)})")
    lines = ",\n  ".join(body)
    return f"create table if not exists {table.qualified} (\n  {lines}\n)"


def create_schema(name: str) -> str:
    return f"create schema if not exists {quote(name)}"


def add_column(table: Table, column: Column) -> str:
    """``ALTER TABLE ADD COLUMN``, always nullable.

    A column added to a table that already has rows cannot be `not null` without
    a default, and inventing a default for a de-identified column would be
    inventing data. The registered schema still says what it says; the sink is
    just more permissive than it about rows that predate the column.
    """
    return (
        f"alter table {table.qualified} add column if not exists "
        f"{quote(column.name)} {column.sql_type}"
    )


def reconcile(table: Table, existing: Mapping[str, str]) -> list[str]:
    """Statements to bring a live table in line with the registered schema.

    ``existing`` maps column name to Postgres type name, as
    ``information_schema`` reports it.

    Additive only. A new column is an `ALTER TABLE ADD COLUMN`, because that is
    what happens when someone covers a new source column in the policy and M4
    un-halts the topic. Anything else raises:

    * a column that changed type would need the existing rows rewritten, and
      there is no rule for what the old values should become;
    * a column the schema no longer has is a policy that started dropping
      something, and silently keeping the old data in the sink would leave
      exactly the PHI the change was meant to remove.

    Both are safe failures rather than solved cases, which is what the project's
    "known limitations" section already says about drops and type changes.
    """
    if not existing:
        return [create_table(table)]

    statements = []
    for column in table.columns:
        live = existing.get(column.name)
        if live is None:
            statements.append(add_column(table, column))
            continue
        if not types_agree(column.sql_type, live):
            raise IncompatibleSinkSchema(
                f"{table.qualified}.{quote(column.name)} is {live} in the sink but the "
                f"registered schema now says {column.sql_type}. A type change needs the "
                f"existing rows rewritten and there is no rule here for what the old "
                f"values should become -- resolve it by hand, or drop {table.qualified} "
                f"and replay."
            )

    vanished = sorted(set(existing) - {c.name for c in table.columns})
    if vanished:
        raise IncompatibleSinkSchema(
            f"{table.qualified} has {', '.join(vanished)}, which the registered schema no "
            f"longer does. If the policy started dropping a column, leaving the old values "
            f"in the sink keeps exactly what the change was meant to remove -- drop the "
            f"table and replay instead."
        )
    return statements


# `information_schema.columns.data_type` spells types differently from the DDL
# that created them, so a string compare would report a change on every run.
CANONICAL_TYPES: Mapping[str, str] = {
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
    "time without time zone": "time",
    "character varying": "text",
    "int4": "integer",
    "int8": "bigint",
    "int2": "smallint",
    "float4": "real",
    "float8": "double precision",
    "bool": "boolean",
    "decimal": "numeric",
    "array": "[]",
}


def canonical_type(sql_type: str) -> str:
    """A comparable spelling of a Postgres type name.

    Parameters are dropped: `numeric(12,2)` and `numeric` compare equal, because
    `information_schema.data_type` reports the latter for both and the precision
    lives in separate columns. Losing that means a widened precision does not
    raise -- an acceptable trade against raising on every single run.
    """
    lowered = sql_type.strip().lower()
    array = lowered.endswith("[]")
    if array:
        lowered = lowered[:-2].strip()
    lowered = CANONICAL_TYPES.get(lowered, lowered)
    lowered = lowered.split("(")[0].strip()
    return f"{lowered}[]" if array else lowered


def types_agree(declared: str, live: str) -> bool:
    return canonical_type(declared) == canonical_type(live)


# ---------------------------------------------------------------------------
# the edge
# ---------------------------------------------------------------------------


def live_columns(conn, table: Table) -> dict[str, str]:
    """What the sink currently has for ``table``: column name -> type name.

    An array column reports `data_type = 'ARRAY'` with the element type in
    `udt_name` as `_text`, so this reassembles `text[]` rather than comparing
    against the useless spelling.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            select column_name, data_type, udt_name
              from information_schema.columns
             where table_schema = %s and table_name = %s
            """,
            (table.schema, table.name),
        )
        rows = cursor.fetchall()
    columns = {}
    for name, data_type, udt_name in rows:
        if data_type.lower() == "array" and udt_name.startswith("_"):
            columns[name] = f"{udt_name[1:]}[]"
        else:
            columns[name] = data_type
    return columns


def ensure_schema(conn, tables: Iterable[Table], *, dry_run: bool = False) -> list[str]:
    """Create or additively reconcile every table, and the bookkeeping schema.

    Returns the statements it ran, so a caller can print them -- `initdb` does,
    because "what did it do to my database" should be answerable without turning
    on statement logging.

    ``pit tail`` calls this at startup too, so it never matters whether `initdb`
    ran first.
    """
    tables = list(tables)
    statements: list[str] = [create_schema(META_SCHEMA), create_applied_offsets()]
    for table in tables:
        statements.append(create_schema(table.schema))
        statements.extend(reconcile(table, live_columns(conn, table)))

    if dry_run:
        return statements

    with conn.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    conn.commit()
    return statements


def create_applied_offsets() -> str:
    """The applier's bookkeeping table.

    Inside the payload database, deliberately. A snapshot cut with
    ``CREATE DATABASE ... TEMPLATE pit_base`` then carries the manifest it was cut
    at, so there is no window between reading the offsets and cloning the
    database in which the two could disagree. See applier.py.
    """
    return f"""create table if not exists {quote(META_SCHEMA)}."applied_offsets" (
  "topic"      text   not null,
  "partition"  int    not null,
  "next_offset" bigint not null,
  "updated_at" timestamptz not null default now(),
  primary key ("topic", "partition")
)"""


def tables_from_registry(client: registry.Registry) -> list[Table]:
    """Every sink table the registry currently describes."""
    return [read_table(*client.schemas_for(topic)) for topic in client.clean_topics()]


def tables_from_dir(path) -> list[Table]:
    """Every sink table described by a directory of schema fixtures.

    Files are named ``<schema>.<table>-key.json`` and ``-value.json``. This is
    what makes `pit initdb` runnable before M4 exists: the same code path, fed
    from disk instead of over HTTP.
    """
    from pathlib import Path

    directory = Path(path)
    tables = []
    for value_file in sorted(directory.glob("*-value.json")):
        key_file = value_file.with_name(value_file.name.replace("-value.json", "-key.json"))
        if not key_file.exists():
            raise FileNotFoundError(
                f"{value_file.name} has no matching {key_file.name}; the primary key comes "
                f"from the key schema, so both halves are required"
            )
        tables.append(
            read_table(
                json.loads(key_file.read_text()),
                json.loads(value_file.read_text()),
            )
        )
    if not tables:
        raise FileNotFoundError(f"no *-value.json schemas in {directory}")
    return tables


def describe(tables: Sequence[Table]) -> str:
    """A short rendering, for `initdb`'s output."""
    lines = []
    for table in tables:
        keys = ", ".join(table.primary_key)
        lines.append(f"  {table.schema}.{table.name}  ({len(table.columns)} columns, pk {keys})")
    return "\n".join(lines)
