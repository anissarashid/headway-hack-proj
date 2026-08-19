"""Statements -> SQL, in one transaction, idempotently.

This is the applier that replaces a JDBC sink connector. It exists because Kafka
Connect sinks can start a consumer group at a timestamp but cannot bound the *end*
of a read: a controller that watches lag and pauses the connector overshoots by up
to one batch, writing events past ``T``. For a point-in-time tool that is a
correctness bug rather than a rough edge. An applier we own stops at an exact
offset by construction -- and stopping is a line of code.

Two decisions here that M6 and M7 depend on.

**The batch is one transaction, and the offsets are in it.** Data and the position
it was applied from commit together, so the pair cannot disagree. If the process
dies after that commit but before the Kafka offsets are committed, the next run
replays the tail of the batch -- which is harmless, because every statement here
is idempotent.

**The offsets live inside the payload database.** ``pit_meta.applied_offsets`` is
a table in ``pit_base``, not in a control database beside it. So
``CREATE DATABASE snap_x TEMPLATE pit_base`` produces a clone that carries the
manifest it was cut at, with no window in which the recorded offsets and the
cloned data could drift apart. A snapshot is then not a special artifact -- it is
a database plus a position in the log, which is exactly what M7 needs and what
lets M6's replay be reused unchanged.

The cost of that choice: ``pit_meta`` is in every clone, so M8's oracle compare
and PHI leak scan have to exclude the schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .ddl import META_SCHEMA, Table, quote
from .envelope import Delete, Statement, Upsert


@dataclass(frozen=True)
class Offset:
    """The next offset to read for one topic partition.

    "Next", not "last applied": it is what a consumer would seek to in order to
    resume, which is how Kafka spells a position and therefore the only spelling
    that does not need converting at the point of use.
    """

    topic: str
    partition: int
    next_offset: int


@dataclass(frozen=True)
class Applied:
    """What one :func:`apply` call did. Returned so callers can log it."""

    upserts: int = 0
    deletes: int = 0
    skipped: int = 0

    @property
    def total(self) -> int:
        return self.upserts + self.deletes


def upsert_sql(table: Table, columns: Sequence[str]) -> str:
    """``INSERT ... ON CONFLICT DO UPDATE``, restricted to the columns present.

    Only the columns the record carried are written. A record that predates an
    added column would otherwise null it out on every update, which is a slow
    way to lose data that was correct.
    """
    keys = list(table.primary_key)
    updatable = [column for column in columns if column not in keys]
    target = ", ".join(quote(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    conflict = ", ".join(quote(key) for key in keys)

    if not updatable:
        # A table whose every column is part of the primary key. The row either
        # exists or does not; there is nothing to update it to.
        action = "do nothing"
    else:
        assignments = ", ".join(f"{quote(c)} = excluded.{quote(c)}" for c in updatable)
        action = f"do update set {assignments}"

    return (
        f"insert into {table.qualified} ({target}) values ({placeholders}) "
        f"on conflict ({conflict}) {action}"
    )


def delete_sql(table: Table) -> str:
    predicate = " and ".join(f"{quote(key)} = %s" for key in table.primary_key)
    return f"delete from {table.qualified} where {predicate}"


def offsets_sql() -> str:
    return (
        f"insert into {quote(META_SCHEMA)}.\"applied_offsets\" "
        f'("topic", "partition", "next_offset", "updated_at") values (%s, %s, %s, now()) '
        f'on conflict ("topic", "partition") do update set '
        f'"next_offset" = excluded."next_offset", "updated_at" = excluded."updated_at"'
    )


def apply(
    conn,
    statements: Iterable[Statement | None],
    offsets: Iterable[Offset] = (),
) -> Applied:
    """Apply a batch and record where it got to, in one transaction.

    Statements run in the order given, one at a time. Grouping same-shaped
    statements into an ``executemany`` would be faster and would also reorder
    them, and order is the whole correctness argument within a partition: an
    upsert followed by a delete of the same key is a deleted row, and the reverse
    is a live one.
    """
    counts = {"upserts": 0, "deletes": 0, "skipped": 0}
    with conn.cursor() as cursor:
        for statement in statements:
            if statement is None:
                counts["skipped"] += 1
            elif isinstance(statement, Upsert):
                columns = list(statement.values)
                cursor.execute(
                    upsert_sql(statement.table, columns),
                    [statement.values[column] for column in columns],
                )
                counts["upserts"] += 1
            elif isinstance(statement, Delete):
                cursor.execute(
                    delete_sql(statement.table),
                    [statement.key[key] for key in statement.table.primary_key],
                )
                counts["deletes"] += 1
            else:  # pragma: no cover - the union is closed
                raise TypeError(f"not a statement: {statement!r}")

        for offset in offsets:
            cursor.execute(offsets_sql(), (offset.topic, offset.partition, offset.next_offset))

    conn.commit()
    return Applied(**counts)


def applied_offsets(conn) -> dict[tuple[str, int], int]:
    """Where the sink has got to, per topic partition.

    This is the manifest a snapshot was cut at, and what a restarted tail seeks
    to. Read from the sink rather than from Kafka's consumer group because the
    sink is where the data is: a group offset that disagreed with the data would
    be a replay gap or a re-replay, and this way the two cannot disagree.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            f'select "topic", "partition", "next_offset" '
            f'from {quote(META_SCHEMA)}."applied_offsets"'
        )
        return {(topic, partition): offset for topic, partition, offset in cursor.fetchall()}


def row_counts(conn, tables: Sequence[Table]) -> dict[str, int]:
    """``schema.table -> count``. The M5 milestone check compares this to the source."""
    counts = {}
    with conn.cursor() as cursor:
        for table in tables:
            cursor.execute(f"select count(*) from {table.qualified}")
            counts[f"{table.schema}.{table.name}"] = cursor.fetchone()[0]
    return counts


def statements_for(
    table: Table,
    records: Iterable[tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]],
) -> list[Statement | None]:
    """Translate a run of ``(key, value)`` records for one table.

    A thin convenience over :func:`pit.envelope.translate`, kept here so the
    consumer that arrives with the Kafka half has one obvious call to make per
    partition batch.
    """
    from .envelope import translate

    return [translate(table, key, value) for key, value in records]
