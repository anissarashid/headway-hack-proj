"""Debezium envelope -> a statement to run against the sink.

Pure. No connection, no cursor, no SQL text -- just the decision about what a
CDC event means. That is what lets M6 reuse it for bounded replay: replay and
tail differ only in where they stop, never in what they do with a record.

The mapping is small enough to state completely:

| ``op``                  | statement          |
| ----------------------- | ------------------ |
| ``c`` create            | upsert             |
| ``r`` snapshot read     | upsert             |
| ``u`` update            | upsert             |
| ``d`` delete            | delete             |
| null value (tombstone)  | skip               |

Upsert rather than insert for ``c``, and upsert rather than update for ``u``,
because every operation has to be idempotent: replaying a range twice has to
converge rather than fail on a duplicate key or no-op on a missing row. That
property is what makes a crash between a database commit and an offset commit
harmless, and it is what M8's determinism test checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .ddl import Table

# The operations Debezium spells in `op`. Truncate (`t`) and logical message
# (`m`) are deliberately absent -- see `UnsupportedOperation`.
UPSERT_OPS = frozenset({"c", "r", "u"})
DELETE_OP = "d"


class MalformedEnvelope(RuntimeError):
    """The record does not carry what its ``op`` says it should."""


class UnsupportedOperation(RuntimeError):
    """An operation this applier has no correct handling for.

    Raised rather than skipped. A truncate that gets skipped leaves the sink
    holding every row the source dropped, which is a silently wrong replica --
    the failure mode this project cares most about avoiding. Halting is the safe
    outcome even though it stops the tail.
    """


class UnknownField(RuntimeError):
    """The record carries a field the sink table does not have.

    Expected, and recoverable: it is what happens when someone covers a new
    source column in the policy, M4 registers a new clean schema version, and the
    first record carrying the new field arrives while the tail is already
    running. The caller's move is to re-run :func:`pit.ddl.ensure_schema` -- which
    adds the column -- and retry the batch. It is an error here rather than a
    silent drop because dropping it would mean the sink quietly stops tracking a
    column the policy has approved.
    """


@dataclass(frozen=True)
class Upsert:
    """Insert the row, or update it if the primary key is already there."""

    table: Table
    key: Mapping[str, Any]
    values: Mapping[str, Any]


@dataclass(frozen=True)
class Delete:
    """Remove the row with this primary key, if it is there."""

    table: Table
    key: Mapping[str, Any]


Statement = Upsert | Delete


def key_of(table: Table, key_record: Mapping[str, Any] | None) -> dict[str, Any]:
    """The primary key, read from the message key.

    From the key and never from the value. The key is what an upsert conflicts on
    and what a delete matches, and M4 de-identifies it with the same ops as the
    payload precisely so the two agree -- if they disagreed, the same logical row
    would upsert under two different surrogates and the sink would grow duplicates
    that no join could reconcile.
    """
    if not key_record:
        raise MalformedEnvelope(
            f"{table.schema}.{table.name}: no message key, so there is no primary key to "
            f"apply this record under"
        )
    missing = [column for column in table.primary_key if column not in key_record]
    if missing:
        raise MalformedEnvelope(
            f"{table.schema}.{table.name}: the message key is missing "
            f"{', '.join(missing)}, which the key schema says it carries"
        )
    return {column: key_record[column] for column in table.primary_key}


def row_of(table: Table, after: Mapping[str, Any]) -> dict[str, Any]:
    """The payload, checked against the columns the sink actually has."""
    known = {column.name for column in table.columns}
    unknown = sorted(set(after) - known)
    if unknown:
        raise UnknownField(
            f"{table.schema}.{table.name}: the record carries {', '.join(unknown)}, which "
            f"the sink has no column for. If the policy just started covering a new source "
            f"column, re-run ensure_schema to add it and retry."
        )
    return dict(after)


def translate(
    table: Table,
    key_record: Mapping[str, Any] | None,
    value_record: Mapping[str, Any] | None,
) -> Statement | None:
    """One record -> one statement, or ``None`` to skip it.

    ``value_record`` is ``None`` for a tombstone. The connector runs
    ``tombstones.on.delete=false`` so one should not arrive, but a compacted or
    hand-produced topic can still hold them and skipping is the correct handling
    either way: a tombstone says nothing about row state that the preceding
    delete has not already said.
    """
    if value_record is None:
        return None

    op = value_record.get("op")
    if not isinstance(op, str):
        raise MalformedEnvelope(f"{table.schema}.{table.name}: record has no op field")

    if op in UPSERT_OPS:
        after = value_record.get("after")
        if after is None:
            raise MalformedEnvelope(
                f"{table.schema}.{table.name}: op {op!r} with no after image. A create or "
                f"update has to carry the row it produced."
            )
        return Upsert(table=table, key=key_of(table, key_record), values=row_of(table, after))

    if op == DELETE_OP:
        return Delete(table=table, key=key_of(table, key_record))

    raise UnsupportedOperation(
        f"{table.schema}.{table.name}: op {op!r} has no handling here. Truncate and logical "
        f"message events change or fail to change the sink in ways this applier cannot "
        f"reproduce, and skipping one would leave a replica that looks right and is not."
    )


def commit_timestamp_ms(value_record: Mapping[str, Any]) -> int | None:
    """``source.ts_ms`` -- the database commit time, not the transformer's clock.

    The value the whole point-in-time model rests on. M4 sets each cleaned
    record's Kafka timestamp from it, which is what makes ``offsets_for_times(T)``
    resolve "the database as of T" rather than "whenever the transformer happened
    to run". Read here so the applier can report how far behind the source it is
    without having to understand the envelope twice.
    """
    source = value_record.get("source")
    if not isinstance(source, Mapping):
        return None
    ts_ms = source.get("ts_ms")
    return ts_ms if isinstance(ts_ms, int) else None
