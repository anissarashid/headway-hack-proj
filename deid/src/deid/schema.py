"""The clean Avro schema, derived from ``(raw schema, policy)``.

This is where the schema registry becomes a policy enforcement mechanism rather
than a serialization detail. The transformer does not decide per record what to
emit: it derives one clean schema at startup, registers it, and from then on the
registry rejects anything that does not fit. A source column nobody wrote a rule
for cannot reach a clean topic, because there is no field for it in the schema
the topic is registered with -- and under ``halt_topic`` the derivation itself
refuses to produce one, so a new PHI column added to the source is a startup
failure on one topic instead of a leak nobody notices.

:func:`derive_clean_schema` is pure. It reaches no registry, reads no file and
opens no socket: raw schema and policy in, clean schema out. That is what makes
the enforcement testable -- every claim this module makes is checkable without a
cluster, which is the only reason anyone will believe it.

Two things about the Debezium envelope shape make the walk less obvious than it
sounds.

*The row image is defined once and referenced once.* Debezium's value schema
carries ``before`` and ``after``, and both are ``["null",
"<topic>.Value"]`` -- the same named record. Avro allows a fullname to be
*defined* exactly once and referenced by name thereafter, so the derived record
is emitted in full at whichever of the two fields carried the definition and as
a bare name at the other. Emitting the full definition twice produces a schema
that looks right in a diff, passes a unit test that checks one field at a time,
and is rejected by the registry as a duplicate name at registration -- which is
after the first record has already been read. :func:`definitions` and
:func:`references` exist so that invariant is asserted rather than assumed, and
:func:`derive_clean_schema` checks it on its own output before returning.

*The envelope is not the policy's business.* ``source``, ``op``, ``ts_ms`` and
``transaction`` pass through byte-identically. ``source.ts_ms`` is the database
commit time, and point-in-time replay resolves T against it, so a derivation
that rewrote or reordered the source block could destroy the timeline while
every record still looked de-identified. :mod:`deid.policy` already refuses to
address those fields; this module refuses to touch them.

    python -m deid.schema                       # derive and print public.patients
    python -m deid.schema --table public.claims
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import avro, ops, policy
from .avro import AvroType

# The two envelope fields that carry a row image, and the only two this module
# rewrites. Debezium emits `before` first, so `before` is normally the field
# that defines the record and `after` the one that references it -- but the
# walk below reads which is which off the schema instead of assuming, because
# the order is the connector's choice and not ours to depend on.
ROW_IMAGE_FIELDS = ("before", "after")

# Avro's named types: the kinds that introduce a fullname, and can therefore be
# defined once and referenced afterwards.
NAMED_KINDS = frozenset({"record", "error", "enum", "fixed"})

DEFINITION = "definition"
REFERENCE = "reference"

# Distinguishes "this field has no default" from "this field defaults to null".
_MISSING = object()


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class SchemaError(Exception):
    """The raw schema is not a shape this module can derive from.

    Not a :class:`~deid.policy.PolicyError`: nothing in the policy file can
    cause it and no edit to the policy will fix it. Either the connector emitted
    something unexpected or the wrong subject was fetched.
    """


class MalformedEnvelopeError(SchemaError):
    """The raw schema is not a Debezium envelope with a row image in it."""


class DuplicateDefinitionError(SchemaError):
    """A named type defined more than once: the registry would reject it."""


class UndefinedReferenceError(SchemaError):
    """A named type referenced before -- or without -- being defined."""


class UncoveredColumnError(policy.PolicyError):
    """A source column the policy says nothing about, under ``halt_topic``.

    The whole point. This is the failure the design trades for: one topic
    halted, with a message naming the column, instead of a clean topic that
    carries a column nobody reviewed.
    """


class UnknownColumnError(policy.PolicyError):
    """A rule for a column the raw schema does not have.

    Not a leak on its own -- but a rule that protects nothing is a rule a
    reviewer will read as protection. It usually means a column was renamed at
    the source, in which case its replacement is uncovered and leaking, so the
    stale rule is the more legible half of the same mistake.
    """


# ---------------------------------------------------------------------------
# names
# ---------------------------------------------------------------------------


def _resolve(name: str, namespace: str | None) -> str:
    """A name as written, resolved against the namespace enclosing it."""
    if "." in name or not namespace:
        return name
    return f"{namespace}.{name}"


def _fullname(named: Mapping[str, Any], namespace: str | None) -> str:
    """The fullname of a named type, given the namespace it appears in."""
    name = named.get("name")
    if not isinstance(name, str) or not name:
        raise MalformedEnvelopeError(
            f"a {named.get('type', 'named')} with no name: {json.dumps(named, default=str)[:120]}"
        )
    if "." in name:
        return name
    return _resolve(name, named.get("namespace") or namespace)


def _walk(schema: AvroType, namespace: str | None) -> Iterator[tuple[str, str]]:
    """Every named type in document order, as ``(DEFINITION|REFERENCE, fullname)``.

    Document order matters: Avro resolves a reference against the definitions
    that precede it, so "defined somewhere in the schema" is not the same claim
    as "defined before it is used".
    """
    if avro.is_union(schema):
        for branch in schema:
            yield from _walk(branch, namespace)
        return
    if isinstance(schema, str):
        # A bare string is either a primitive or a reference to a named type.
        if schema not in avro.PRIMITIVES:
            yield REFERENCE, _resolve(schema, namespace)
        return
    if not isinstance(schema, Mapping):
        return

    kind = schema.get("type")
    if isinstance(kind, str) and kind in NAMED_KINDS:
        full = _fullname(schema, namespace)
        yield DEFINITION, full
        # Names inside a named type resolve against *its* namespace, not the
        # one it was found in.
        inner = full.rsplit(".", 1)[0] if "." in full else None
        for field in schema.get("fields", ()):
            if isinstance(field, Mapping):
                yield from _walk(field.get("type", "null"), inner)
        return
    if kind == "array":
        yield from _walk(schema.get("items", "null"), namespace)
        return
    if kind == "map":
        yield from _walk(schema.get("values", "null"), namespace)
        return
    if kind is not None and not isinstance(kind, str):
        # An annotated wrapper: {"type": {...}, "connect.name": ...}.
        yield from _walk(kind, namespace)


def definitions(schema: AvroType) -> tuple[str, ...]:
    """Every named type *defined* in ``schema``, fully qualified, in order.

    Repeats are kept, because a repeat is exactly the bug worth finding.
    """
    return tuple(name for kind, name in _walk(schema, None) if kind == DEFINITION)


def references(schema: AvroType) -> tuple[str, ...]:
    """Every named type *referenced* by name in ``schema``, fully qualified, in order.

    Relative references are resolved against their enclosing namespace, so
    ``"Value"`` inside ``raw.public.patients`` and the fully-qualified spelling
    come back the same. The registry canonicalizes to the relative form on a
    round trip (see the DATA-703 spike), so a caller comparing schemas has to
    compare resolved names rather than JSON.
    """
    return tuple(name for kind, name in _walk(schema, None) if kind == REFERENCE)


def check_names(schema: AvroType) -> None:
    """Raise unless every fullname is defined once and referenced after that.

    The two ways a derived schema gets rejected at registration rather than
    here. Cheap, so :func:`derive_clean_schema` runs it on its own output: a
    duplicate ``Value`` record is a mistake this module could plausibly make,
    and discovering it in a unit test costs nothing while discovering it at
    registration costs a halted topic.
    """
    defined: set[str] = set()
    for kind, name in _walk(schema, None):
        if kind == DEFINITION:
            if name in defined:
                raise DuplicateDefinitionError(
                    f"{name!r} is defined more than once. Avro allows a named type "
                    "exactly one definition; define it at the first field that "
                    "carries it and reference it by name at the rest"
                )
            defined.add(name)
        elif name not in defined:
            raise UndefinedReferenceError(
                f"{name!r} is referenced before it is defined"
                + (f" (defined: {', '.join(sorted(defined)) or 'nothing'})")
            )


# ---------------------------------------------------------------------------
# the envelope
# ---------------------------------------------------------------------------


def _as_record(schema: Any, what: str) -> Mapping[str, Any]:
    """``schema``, checked to be a record whose fields are all named and typed.

    Checked up front rather than at each ``field["type"]``, so a schema that is
    not the shape this module walks says so once instead of raising a
    ``KeyError`` from somewhere in the middle of the walk.
    """
    if not isinstance(schema, Mapping):
        raise MalformedEnvelopeError(
            f"{what} must be an Avro record, got {type(schema).__name__}"
        )
    if schema.get("type") != "record":
        raise MalformedEnvelopeError(
            f"{what} must be an Avro record, got type {schema.get('type')!r}"
        )
    fields = schema.get("fields")
    if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)):
        raise MalformedEnvelopeError(f"{what} has no 'fields' list")
    for field in fields:
        if not isinstance(field, Mapping) or not isinstance(field.get("name"), str):
            raise MalformedEnvelopeError(
                f"{what} has a field that is not a named mapping: {field!r}"
            )
        if "type" not in field:
            raise MalformedEnvelopeError(f"{what} field {field['name']!r} has no type")
    return schema


def _row_image_branch(field: Mapping[str, Any], namespace: str | None) -> AvroType:
    """The row-image branch of a ``before``/``after`` field type.

    Debezium writes ``["null", <record>]``; a bare record is accepted too. A
    union with more than one non-null branch is refused rather than guessed at,
    for the same reason the ops refuse one: nothing here can tell which branch a
    row image arrived on.
    """
    inner = avro.non_null(field["type"])
    if avro.is_union(inner) or inner == "null":
        raise MalformedEnvelopeError(
            f"envelope field {field['name']!r} is {avro.describe(field['type'])}, which "
            "is not a nullable row-image record"
        )
    if not isinstance(inner, (str, Mapping)):
        raise MalformedEnvelopeError(
            f"envelope field {field['name']!r} carries no row-image record"
        )
    return inner


def _replace_branch(
    field_type: AvroType, matches: Callable[[AvroType], bool], replacement: AvroType
) -> AvroType:
    """``field_type`` with the branch ``matches`` selects swapped out.

    Branch order and any other branches survive, so a nullable field stays
    nullable with ``null`` where it was -- which is what keeps the field's
    ``default: null`` valid.
    """
    if avro.is_union(field_type):
        return [replacement if matches(branch) else branch for branch in field_type]
    return replacement if matches(field_type) else field_type


# ---------------------------------------------------------------------------
# the row image
# ---------------------------------------------------------------------------


def _default_for(clean_type: AvroType, raw_default: Any) -> Any:
    """The default the derived field can carry, or ``_MISSING`` for none.

    Avro checks a default against the *first* branch of a union. An op that can
    fail to read its input widens the column to nullable and :func:`avro.nullable`
    puts ``null`` first, so a nullable clean column can always default to null --
    and should, because that is what Debezium writes for an optional field and
    what lets the clean subject gain a field later without breaking readers.

    A default that no longer fits is dropped rather than replaced. Inventing a
    default for a column whose type just changed would be inventing data, and a
    default that does not fit its type is rejected at registration -- the exact
    failure this module exists to move earlier.
    """
    if avro.branches(clean_type)[0] == "null":
        return None
    if raw_default is _MISSING:
        return _MISSING
    return raw_default if avro.conforms(raw_default, clean_type) else _MISSING


def _clean_field(field: Mapping[str, Any], clean_type: AvroType) -> dict[str, Any]:
    """The derived field: the raw one with its type, and only its type, replaced.

    ``doc``, ``aliases`` and the connector's own properties are carried over
    verbatim. They describe the column, and the column is still the same column
    -- it is its representation that changed.
    """
    default = _default_for(clean_type, field.get("default", _MISSING))
    clean: dict[str, Any] = {}
    for key, value in field.items():
        if key == "type":
            clean["type"] = clean_type
        elif key == "default":
            if default is not _MISSING:
                clean["default"] = default
        else:
            clean[key] = value
    if default is not _MISSING and "default" not in clean:
        clean["default"] = default
    return clean


def _renamed(named: Mapping[str, Any], namespace: str, enclosing: str | None) -> dict[str, Any]:
    """``named``, moved into ``namespace``.

    The clean topic is ``clean.public.patients`` and the raw one is
    ``raw.public.patients``. Leaving the derived records named after the raw
    topic registers two different schemas under one fullname, which is fine for
    the registry and wrong for anything that resolves types by name -- a code
    generator, a cache, ``fastavro``'s named-type table. ``connect.name`` is
    moved with the name when it spelled the old one, so the two do not disagree.
    """
    old_full = _fullname(named, enclosing)
    simple = old_full.rsplit(".", 1)[-1]
    new_full = f"{namespace}.{simple}"

    clean: dict[str, Any] = {}
    for key, value in named.items():
        if key == "name":
            clean["name"] = simple
            clean["namespace"] = namespace
        elif key == "namespace":
            clean["namespace"] = namespace
        elif key == "connect.name" and value == old_full:
            clean["connect.name"] = new_full
        else:
            clean[key] = value
    clean.setdefault("namespace", namespace)
    return clean


def _derive_row_image(
    raw_record: Mapping[str, Any],
    table_policy: policy.TablePolicy,
    *,
    keys: ops.Keys,
    on_uncovered: policy.UncoveredColumn,
    namespace: str | None,
    enclosing: str | None,
    source: str | None,
) -> dict[str, Any]:
    """The clean row-image record: one field per covered column that survives."""
    _as_record(raw_record, f"the row image of {table_policy.name}")
    raw_fields = raw_record["fields"]
    columns = [field["name"] for field in raw_fields]

    uncovered = [column for column in columns if not table_policy.covers(column)]
    if uncovered and on_uncovered is policy.UncoveredColumn.HALT_TOPIC:
        raise UncoveredColumnError(
            f"{len(uncovered)} column(s) in the raw schema have no rule: "
            f"{', '.join(uncovered)}. A column nobody wrote a rule for is a column "
            "nobody reviewed, so this topic halts instead of deriving a clean schema "
            "that carries it. Add a rule for each, or set "
            f"on_uncovered_column: {policy.UncoveredColumn.DROP_COLUMN.value}",
            source=source,
            table=table_policy.name,
            column=uncovered[0] if len(uncovered) == 1 else None,
        )

    known = set(columns)
    stale = sorted(column for column in table_policy.rules if column not in known)
    if stale:
        raise UnknownColumnError(
            f"the policy has rule(s) for column(s) the table does not have: "
            f"{', '.join(stale)}. A rule that protects nothing still reads as "
            f"protection; the table's columns are: {', '.join(columns)}",
            source=source,
            table=table_policy.name,
            column=stale[0] if len(stale) == 1 else None,
        )

    clean_fields: list[dict[str, Any]] = []
    for field in raw_fields:
        rule = table_policy.rule_for(field["name"])
        if rule is None:
            continue  # drop_column; halt_topic already raised above
        raw_type = field["type"]
        # `build` is what refuses a rule the column's type cannot support, and
        # it refuses here -- at startup, with no record in hand -- which is the
        # entire reason the type half of an op is a function.
        clean_type = ops.build(rule, raw_type, keys=keys).derive_type(raw_type)
        if clean_type is ops.DROPPED:
            continue
        clean_fields.append(_clean_field(field, clean_type))

    clean_record = _renamed(raw_record, namespace, enclosing) if namespace else dict(raw_record)
    clean_record["fields"] = clean_fields
    return clean_record


# ---------------------------------------------------------------------------
# the derivation
# ---------------------------------------------------------------------------


def derive_clean_schema(
    raw_schema: AvroType,
    table_policy: policy.TablePolicy,
    *,
    keys: ops.Keys,
    on_uncovered: policy.UncoveredColumn = policy.UncoveredColumn.HALT_TOPIC,
    namespace: str | None = None,
    source: str | None = None,
) -> AvroType:
    """The clean value schema for one table, from its raw schema and its policy.

    Pure: no registry, no file, no clock. The returned schema shares no mutable
    state with ``raw_schema``.

    ``before`` and ``after`` get the derived row image -- defined at whichever
    of them the raw schema defined it at, referenced by name at the other.
    ``source``, ``op``, ``ts_ms``, ``transaction`` and any other envelope field
    are copied through unchanged; ``source.ts_ms`` is what point-in-time replay
    resolves against, so it is not the policy's to touch and not this function's
    to reformat.

    ``keys`` is required because an op is built once and both halves come out of
    that build; the type half does not read the salt, but there is deliberately
    no way to build one half alone. ``on_uncovered`` defaults to halting, which
    is the fail-closed answer and the one the policy file defaults to.

    ``namespace`` moves the derived envelope and row-image records into another
    namespace -- pass the clean topic name so the clean schema does not claim
    the raw schema's fullnames. The ``source`` block keeps its own namespace
    either way: it is Debezium's type, not this topic's.

    Raises :class:`UncoveredColumnError` for a column with no rule under
    ``halt_topic``, :class:`UnknownColumnError` for a rule with no column,
    :class:`~deid.ops.IncompatibleColumnError` for a rule the column's type
    cannot support, and :class:`SchemaError` if the raw schema is not an
    envelope. All of them at startup, all of them naming the column.
    """
    # Checked before anything is derived, so an invalid raw schema is reported as
    # an invalid raw schema rather than surfacing later as the same complaint
    # about this function's output.
    try:
        check_names(raw_schema)
    except SchemaError as exc:
        raise type(exc)(f"the raw value schema is not valid Avro: {exc}") from None

    envelope = _as_record(raw_schema, "the raw value schema")
    fields: Sequence[Mapping[str, Any]] = envelope["fields"]
    # Names written inside the envelope resolve against the envelope's own
    # namespace, which is the raw topic name.
    envelope_full = _fullname(envelope, None)
    enclosing = envelope_full.rsplit(".", 1)[0] if "." in envelope_full else None

    row_fields = [
        (index, field)
        for index, field in enumerate(fields)
        if field["name"] in ROW_IMAGE_FIELDS
    ]
    if not row_fields:
        raise MalformedEnvelopeError(
            "the raw value schema has no 'before' or 'after' field, so it is not a "
            f"Debezium change envelope (its fields are: {', '.join(f['name'] for f in fields)})"
        )

    # Which field carries the definition is the connector's choice: Debezium
    # emits `before` first, so `before` defines and `after` references, but the
    # schema says so and there is no reason to assume it.
    raw_record: Mapping[str, Any] | None = None
    definer: int | None = None
    for index, field in row_fields:
        branch = _row_image_branch(field, enclosing)
        if isinstance(branch, Mapping):
            if raw_record is None:
                raw_record, definer = branch, index
            elif branch != raw_record:
                raise MalformedEnvelopeError(
                    f"{', '.join(f['name'] for _, f in row_fields)} define different "
                    "row-image records; before and after are the same table"
                )
    if raw_record is None:
        raise MalformedEnvelopeError(
            "neither 'before' nor 'after' defines the row-image record -- both are "
            "references, so there is no field list to derive from"
        )

    raw_full = _fullname(raw_record, enclosing)
    for index, field in row_fields:
        branch = _row_image_branch(field, enclosing)
        if isinstance(branch, str) and _resolve(branch, enclosing) != raw_full:
            raise MalformedEnvelopeError(
                f"envelope field {field['name']!r} references {branch!r}, which is not "
                f"the row-image record {raw_full!r}"
            )

    clean_record = _derive_row_image(
        raw_record,
        table_policy,
        keys=keys,
        on_uncovered=on_uncovered,
        namespace=namespace,
        enclosing=enclosing,
        source=source,
    )
    clean_full = _fullname(clean_record, namespace or enclosing)

    def is_row_image(branch: AvroType) -> bool:
        if isinstance(branch, Mapping):
            return branch == raw_record
        return isinstance(branch, str) and _resolve(branch, enclosing) == raw_full

    clean_fields: list[Any] = []
    for index, field in enumerate(fields):
        if field["name"] not in ROW_IMAGE_FIELDS:
            clean_fields.append(field)
            continue
        # The definition at one field, the name at the rest. Both would be a
        # duplicate fullname, which the registry rejects at registration.
        replacement: AvroType = clean_record if index == definer else clean_full
        clean_fields.append(
            {**field, "type": _replace_branch(field["type"], is_row_image, replacement)}
        )

    clean_envelope = _renamed(envelope, namespace, None) if namespace else dict(envelope)
    clean_envelope["fields"] = clean_fields

    # Nothing above aliases the input once this copy is taken, so a caller
    # mutating the derived schema cannot reach back into the raw one.
    clean_envelope = copy.deepcopy(clean_envelope)
    check_names(clean_envelope)
    return clean_envelope


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# A hand-written stand-in for the raw value schema Debezium registers for
# public.patients, under the connector settings in
# charts/pit/charts/connect/connectors/source-pg.json:
# time.precision.mode=connect (so a date arrives as Kafka Connect's Date, not
# Debezium's), decimal.handling.mode=precise, enhanced.avro.schema.support=true.
# It exists so `python -m deid.schema` and the registry acceptance check run
# with no cluster and no connector; both prefer the live subject when there is
# one. Field order, the named-type reuse and the source block mirror what the
# DATA-703 spike registered and confirmed.
_RAW_TOPIC = "raw.public.patients"

_CONNECT_DATE_TYPE = {
    "type": "int",
    "connect.version": 1,
    "connect.name": avro.CONNECT_DATE,
    "logicalType": "date",
}
_ZONED_TYPE = {
    "type": "string",
    "connect.version": 1,
    "connect.name": avro.ZONED_TIMESTAMP,
}

_SOURCE_BLOCK = {
    "type": "record",
    "name": "Source",
    "namespace": "io.debezium.connector.postgresql",
    "fields": [
        {"name": "version", "type": "string"},
        {"name": "connector", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "ts_ms", "type": "long"},
        {
            "name": "snapshot",
            "type": ["null", {"type": "string", "connect.name": "io.debezium.data.Enum",
                              "connect.parameters": {"allowed": "true,last,false,incremental"}}],
            "default": None,
        },
        {"name": "db", "type": "string"},
        {"name": "sequence", "type": ["null", "string"], "default": None},
        {"name": "schema", "type": "string"},
        {"name": "table", "type": "string"},
        {"name": "txId", "type": ["null", "long"], "default": None},
        {"name": "lsn", "type": ["null", "long"], "default": None},
        {"name": "xmin", "type": ["null", "long"], "default": None},
    ],
    "connect.name": "io.debezium.connector.postgresql.Source",
}

DEMO_RAW_VALUE_SCHEMA: Mapping[str, Any] = {
    "type": "record",
    "name": "Envelope",
    "namespace": _RAW_TOPIC,
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Value",
                    "namespace": _RAW_TOPIC,
                    "fields": [
                        {"name": "patient_id", "type": "long"},
                        {"name": "mrn", "type": "string"},
                        {"name": "first_name", "type": "string"},
                        {"name": "middle_name", "type": ["null", "string"], "default": None},
                        {"name": "last_name", "type": "string"},
                        {"name": "date_of_birth", "type": _CONNECT_DATE_TYPE},
                        {"name": "ssn", "type": ["null", "string"], "default": None},
                        {"name": "email", "type": ["null", "string"], "default": None},
                        {"name": "phone", "type": ["null", "string"], "default": None},
                        {"name": "address_line1", "type": ["null", "string"], "default": None},
                        {"name": "address_line2", "type": ["null", "string"], "default": None},
                        {"name": "city", "type": ["null", "string"], "default": None},
                        {"name": "state", "type": ["null", "string"], "default": None},
                        {"name": "postal_code", "type": ["null", "string"], "default": None},
                        {"name": "created_at", "type": _ZONED_TYPE},
                        {"name": "updated_at", "type": _ZONED_TYPE},
                    ],
                    "connect.name": f"{_RAW_TOPIC}.Value",
                },
            ],
            "default": None,
        },
        # The load-bearing reference: the name `before` just defined.
        {"name": "after", "type": ["null", f"{_RAW_TOPIC}.Value"], "default": None},
        {"name": "source", "type": _SOURCE_BLOCK},
        {"name": "op", "type": "string"},
        {"name": "ts_ms", "type": ["null", "long"], "default": None},
        {
            "name": "transaction",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "block",
                    "namespace": "event",
                    "fields": [
                        {"name": "id", "type": "string"},
                        {"name": "total_order", "type": "long"},
                        {"name": "data_collection_order", "type": "long"},
                    ],
                    "connect.version": 1,
                    "connect.name": "event.block",
                },
            ],
            "default": None,
        },
    ],
    "connect.name": f"{_RAW_TOPIC}.Envelope",
}

DEMO_TABLE = "public.patients"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m deid.schema",
        description="Derive a clean Avro schema from a raw Debezium schema and the policy.",
    )
    parser.add_argument(
        "policy",
        nargs="?",
        default=policy.policy_path_from_env(),
        help="policy file (default: $PIT_POLICY_PATH, else the mounted path)",
    )
    parser.add_argument(
        "--table",
        default=DEMO_TABLE,
        help=f"schema-qualified table to derive (default: {DEMO_TABLE})",
    )
    parser.add_argument(
        "--raw",
        default=None,
        metavar="FILE",
        help="raw value schema as JSON ('-' for stdin); default: the built-in "
        f"stand-in for {DEMO_TABLE}, which is the only table it covers",
    )
    parser.add_argument(
        "--namespace",
        default=None,
        help="move the derived records into this namespace, e.g. clean.public.patients",
    )
    args = parser.parse_args(argv)

    try:
        parsed = policy.load_policy(args.policy)
    except policy.PolicyError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1

    table_policy = parsed.table(args.table)
    if table_policy is None:
        print(
            f"INVALID: {parsed.source}: no policy for table {args.table!r} "
            f"(it covers: {', '.join(sorted(parsed.tables))})",
            file=sys.stderr,
        )
        return 1

    if args.raw is None:
        if args.table != DEMO_TABLE:
            print(
                f"INVALID: the built-in raw schema is only for {DEMO_TABLE}; pass "
                f"--raw with the raw value schema for {args.table}",
                file=sys.stderr,
            )
            return 1
        raw_schema: Any = DEMO_RAW_VALUE_SCHEMA
    else:
        text = sys.stdin.read() if args.raw == "-" else open(args.raw, encoding="utf-8").read()
        try:
            raw_schema = json.loads(text)
        except json.JSONDecodeError as exc:
            print(f"INVALID: --raw is not JSON: {exc}", file=sys.stderr)
            return 1

    keys = ops.Keys(salt=ops.DEMO_SALT, reference_date=ops.DEMO_REFERENCE_DATE)
    try:
        clean = derive_clean_schema(
            raw_schema,
            table_policy,
            keys=keys,
            on_uncovered=parsed.on_uncovered_column,
            namespace=args.namespace,
            source=parsed.source,
        )
    except (policy.PolicyError, SchemaError) as exc:
        print(f"HALT {args.table}: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(clean, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
