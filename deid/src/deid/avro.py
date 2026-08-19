"""The Avro type model the two halves of an op have to agree on.

An op changes a value and a type at the same time, and the clean schema is
derived from ``(raw schema, policy)`` before a single record is transformed. So
"what type does this op produce" is a question asked at startup, against a
schema, and "does this value fit that type" is a question asked of every record
afterwards. Both are answered here, by the same code, so that a disagreement
between them is a test failure rather than a rejected produce at 3am.

Types are Avro's own JSON shapes -- a string for a primitive, a list for a
union, a mapping for everything else -- rather than a parallel class hierarchy.
That is deliberate: the schema registry speaks JSON, Debezium's schemas arrive
as JSON, and a translation layer in between would be one more thing that can be
wrong in a way nothing catches. The cost is that a "type" is an untyped blob,
which is what the helpers below exist to make bearable.

:func:`conforms` checks the *wire* type and nothing else. It asks the question
the registry asks -- is this value writable against this schema -- and not
whether an int is a plausible date. Logical types are annotations on a
primitive; the ops are what give them meaning.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence, Union

# An Avro schema as it appears in JSON: "long", ["null", "long"], or
# {"type": "long", "connect.name": "io.debezium.time.MicroTimestamp"}.
AvroType = Union[str, Mapping[str, Any], Sequence[Any]]

PRIMITIVES = frozenset(
    {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
)

# Avro's int is 32-bit and its long is 64-bit. A value outside the range is not
# a large number, it is an unwritable record, which is why the ops clamp.
INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1
INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1

# Debezium's logical types, spelled as ``connect.name`` in the registered
# schema. The unit is not guessable from the primitive -- Timestamp and
# MicroTimestamp are both ``long`` -- so an op that shifts time has to read the
# name, and has to refuse a bare long rather than assume one.
DATE = "io.debezium.time.Date"  # int, days since 1970-01-01
TIMESTAMP = "io.debezium.time.Timestamp"  # long, ms since epoch, UTC, no zone
MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp"  # long, µs
NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp"  # long, ns
ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp"  # string, ISO-8601 + offset
# The same instants, spelled as Kafka Connect's own logical types. The
# connector runs with ``time.precision.mode=connect``, which is what makes a
# date arrive as ``org.apache.kafka.connect.data.Date`` rather than
# ``io.debezium.time.Date`` -- so both spellings reach this module, and an op
# that knew only Debezium's would refuse date_of_birth at startup against the
# schema the connector actually registers.
CONNECT_DATE = "org.apache.kafka.connect.data.Date"  # int, days since 1970-01-01
CONNECT_TIME = "org.apache.kafka.connect.data.Time"  # int, ms since midnight
CONNECT_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp"  # long, ms since epoch
JSON = "io.debezium.data.Json"  # string holding a json document
ENUM = "io.debezium.data.Enum"  # string, with an "allowed" property
# numeric/decimal, under the connector's three decimal.handling.mode settings:
# `precise` gives bytes + scale, `string` a plain string, `double` a double.
DECIMAL = "org.apache.kafka.connect.data.Decimal"
VARIABLE_SCALE_DECIMAL = "io.debezium.data.VariableScaleDecimal"


def is_union(avro_type: AvroType) -> bool:
    """True for ``["null", "string"]`` and friends."""
    return not isinstance(avro_type, (str, Mapping)) and isinstance(avro_type, Sequence)


def branches(avro_type: AvroType) -> tuple[AvroType, ...]:
    """The union's branches, or the type itself as a single branch."""
    return tuple(avro_type) if is_union(avro_type) else (avro_type,)


def is_nullable(avro_type: AvroType) -> bool:
    return any(branch == "null" for branch in branches(avro_type))


def non_null(avro_type: AvroType) -> AvroType:
    """The type with its ``null`` branch removed.

    Returns a union if there was more than one other branch; the ops refuse
    those rather than guess which branch a value is.
    """
    rest = [branch for branch in branches(avro_type) if branch != "null"]
    if not rest:
        return "null"
    return rest[0] if len(rest) == 1 else rest


def nullable(avro_type: AvroType) -> AvroType:
    """``["null", t]``, idempotently. Avro forbids a union inside a union."""
    if is_nullable(avro_type) or avro_type == "null":
        return avro_type
    return ["null", *branches(avro_type)]


def like(source: AvroType, target: AvroType) -> AvroType:
    """``target``, made nullable if ``source`` was.

    Most ops replace a value without inventing one: a null column stays a null
    column, so the derived type has to stay nullable. Spelling that as one
    function keeps the rule in one place instead of in nine.
    """
    return nullable(target) if is_nullable(source) else target


def base(avro_type: AvroType) -> str | None:
    """The underlying kind: ``"long"``, ``"string"``, ``"array"``, ...

    ``None`` for a union, because a union has no single kind. Annotations are
    peeled off -- a Debezium Date is an ``int`` wearing a name.
    """
    if is_union(avro_type):
        return None
    if isinstance(avro_type, str):
        return avro_type
    inner = avro_type.get("type")
    return base(inner) if inner is not None else None


def logical(avro_type: AvroType) -> str | None:
    """``connect.name`` if there is one, else Avro's own ``logicalType``.

    Debezium writes both for some types and only ``connect.name`` for others,
    and its name is the more specific of the two, so it wins.
    """
    if is_union(avro_type) or not isinstance(avro_type, Mapping):
        return None
    name = avro_type.get("connect.name") or avro_type.get("logicalType")
    return name if isinstance(name, str) else None


def describe(avro_type: AvroType) -> str:
    """A short rendering for error messages."""
    if is_union(avro_type):
        return "[" + ", ".join(describe(branch) for branch in branches(avro_type)) + "]"
    if isinstance(avro_type, str):
        return avro_type
    kind, name = base(avro_type), logical(avro_type)
    if kind and name:
        return f"{kind} ({name})"
    if kind in ("array", "map"):
        item = avro_type.get("items", avro_type.get("values"))
        return f"{kind}<{describe(item)}>"
    return kind or json.dumps(avro_type, sort_keys=True, default=str)


def conforms(value: Any, avro_type: AvroType) -> bool:
    """Could this value be written against this schema?

    The question the registry asks, and therefore the question an op's two
    halves have to agree on. Physical shape only: an ``int`` annotated as a
    date conforms for any int, because that is exactly what the registry
    accepts. Range is checked, because Avro's int is 32-bit and a Python int is
    not, and that is the one way a plausible-looking value gets rejected at
    produce time.
    """
    if is_union(avro_type):
        return any(conforms(value, branch) for branch in branches(avro_type))

    if isinstance(avro_type, Mapping):
        kind = avro_type.get("type")
        if kind == "array":
            items = avro_type.get("items", "null")
            return isinstance(value, (list, tuple)) and all(
                conforms(item, items) for item in value
            )
        if kind == "map":
            values = avro_type.get("values", "null")
            return isinstance(value, Mapping) and all(
                isinstance(key, str) and conforms(item, values)
                for key, item in value.items()
            )
        if kind == "record":
            fields = avro_type.get("fields", ())
            if not isinstance(value, Mapping):
                return False
            names = {field["name"] for field in fields}
            if set(value) - names:
                return False
            return all(
                field["name"] in value and conforms(value[field["name"]], field["type"])
                for field in fields
            )
        if kind == "enum":
            return isinstance(value, str) and value in avro_type.get("symbols", ())
        if kind == "fixed":
            return isinstance(value, (bytes, bytearray)) and len(value) == avro_type.get(
                "size", -1
            )
        return conforms(value, kind) if kind is not None else False

    if avro_type == "null":
        return value is None
    if avro_type == "boolean":
        return isinstance(value, bool)
    # bool is a subclass of int, and `true` is not a number in any column here.
    if avro_type == "int":
        return isinstance(value, int) and not isinstance(value, bool) and INT32_MIN <= value <= INT32_MAX
    if avro_type == "long":
        return isinstance(value, int) and not isinstance(value, bool) and INT64_MIN <= value <= INT64_MAX
    if avro_type in ("float", "double"):
        # Avro promotes int to float on write, so an unjittered integral amount
        # in a double column is legal.
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if avro_type == "bytes":
        return isinstance(value, (bytes, bytearray))
    if avro_type == "string":
        return isinstance(value, str)
    # A named reference to a record defined elsewhere in the schema. Nothing
    # here can resolve it, and quietly returning True would make the
    # conformance test vacuous for that field.
    return False
