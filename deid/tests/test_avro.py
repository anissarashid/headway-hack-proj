"""The type model, checked before anything is built on top of it.

``conforms`` is the referee for the conformance property in ``test_ops.py``: if
it said yes to everything, every op would pass and the property would be worth
nothing. So the tests that matter most here are the ones where it says no.
"""

from __future__ import annotations

import pytest

from deid import avro

DEBEZIUM_DATE = {"type": "int", "connect.name": avro.DATE}
MICRO_TIMESTAMP = {"type": "long", "connect.name": avro.MICRO_TIMESTAMP}
ARRAY_OF_STRING = {"type": "array", "items": "string"}


# ---------------------------------------------------------------------------
# reading a type
# ---------------------------------------------------------------------------


def test_unions_are_recognised_and_flattened():
    assert avro.is_union(["null", "string"])
    assert not avro.is_union("string")
    assert not avro.is_union(MICRO_TIMESTAMP)
    assert avro.branches(["null", "string"]) == ("null", "string")
    assert avro.branches("string") == ("string",)


def test_nullability():
    assert avro.is_nullable(["null", "string"])
    assert not avro.is_nullable("string")
    assert avro.non_null(["null", "string"]) == "string"
    assert avro.non_null("string") == "string"
    assert avro.non_null(["null"]) == "null"


def test_non_null_keeps_a_multi_branch_union_a_union():
    """Which is what lets the ops refuse it rather than pick a branch."""
    assert avro.is_union(avro.non_null(["null", "string", "long"]))


def test_nullable_is_idempotent_and_never_nests():
    """Avro forbids a union inside a union, so this cannot just wrap."""
    assert avro.nullable("string") == ["null", "string"]
    assert avro.nullable(["null", "string"]) == ["null", "string"]
    assert avro.nullable(["string", "long"]) == ["null", "string", "long"]
    assert avro.nullable("null") == "null"


def test_like_carries_nullability_across():
    assert avro.like(["null", "long"], "string") == ["null", "string"]
    assert avro.like("long", "string") == "string"


def test_base_peels_annotations_off():
    assert avro.base("long") == "long"
    assert avro.base(MICRO_TIMESTAMP) == "long"
    assert avro.base(ARRAY_OF_STRING) == "array"
    # A union has no single kind, and pretending otherwise is how an op ends up
    # applied to the branch it did not mean.
    assert avro.base(["null", "long"]) is None


def test_logical_prefers_the_debezium_name():
    assert avro.logical(MICRO_TIMESTAMP) == avro.MICRO_TIMESTAMP
    assert avro.logical({"type": "int", "logicalType": "date"}) == "date"
    assert (
        avro.logical({"type": "int", "connect.name": avro.DATE, "logicalType": "date"})
        == avro.DATE
    )
    assert avro.logical("int") is None


def test_describe_is_readable_enough_for_an_error_message():
    assert avro.describe("string") == "string"
    assert avro.describe(MICRO_TIMESTAMP) == "long (io.debezium.time.MicroTimestamp)"
    assert avro.describe(["null", "string"]) == "[null, string]"
    assert avro.describe(ARRAY_OF_STRING) == "array<string>"


# ---------------------------------------------------------------------------
# conformance: the yeses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,avro_type",
    [
        (None, "null"),
        (True, "boolean"),
        (0, "int"),
        (-1, "long"),
        (2**31 - 1, "int"),
        (2**63 - 1, "long"),
        (1.5, "double"),
        (2, "double"),  # avro promotes int to double on write
        (b"\x00", "bytes"),
        ("", "string"),
        (None, ["null", "string"]),
        ("x", ["null", "string"]),
        (19_000, DEBEZIUM_DATE),
        ([], ARRAY_OF_STRING),
        (["E11"], ARRAY_OF_STRING),
        ({"a": 1}, {"type": "map", "values": "int"}),
        ("paid", {"type": "enum", "name": "status", "symbols": ["paid", "denied"]}),
        (b"1234", {"type": "fixed", "name": "f", "size": 4}),
        (
            {"scale": 2, "value": b"\x01"},
            {
                "type": "record",
                "name": "VariableScaleDecimal",
                "fields": [{"name": "scale", "type": "int"}, {"name": "value", "type": "bytes"}],
            },
        ),
    ],
)
def test_conforming_values(value, avro_type):
    assert avro.conforms(value, avro_type)


# ---------------------------------------------------------------------------
# conformance: the noes, which are the ones that matter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,avro_type",
    [
        ("x", "null"),
        (None, "string"),
        (None, "long"),
        (1, "boolean"),
        (True, "int"),  # bool is an int subclass, and `true` is not a number
        (True, "double"),
        (2**31, "int"),  # the whole reason the ops clamp
        (-(2**31) - 1, "int"),
        (2**63, "long"),
        (1.5, "int"),
        ("2", "long"),
        (b"x", "string"),
        ("x", "bytes"),
        (0, ARRAY_OF_STRING),
        ([1], ARRAY_OF_STRING),
        ([None], ARRAY_OF_STRING),
        ({"a": "x"}, {"type": "map", "values": "int"}),
        ({1: 2}, {"type": "map", "values": "int"}),
        ("void", {"type": "enum", "name": "status", "symbols": ["paid"]}),
        (b"12345", {"type": "fixed", "name": "f", "size": 4}),
        (1.5, ["null", "long"]),
        ("x", {"type": "record", "name": "r", "fields": []}),
        ({"extra": 1}, {"type": "record", "name": "r", "fields": []}),
        (
            {"scale": 2},
            {
                "type": "record",
                "name": "r",
                "fields": [{"name": "scale", "type": "int"}, {"name": "value", "type": "bytes"}],
            },
        ),
    ],
)
def test_non_conforming_values(value, avro_type):
    assert not avro.conforms(value, avro_type)


def test_an_unresolvable_named_reference_conforms_to_nothing():
    """Saying yes here would make the property test vacuous for that field."""
    assert not avro.conforms("anything", "com.example.SomeRecord")
    assert not avro.conforms(None, "com.example.SomeRecord")


def test_a_logical_type_is_checked_at_the_wire_level_only():
    """The registry checks the primitive; giving it meaning is the op's job."""
    assert avro.conforms(0, DEBEZIUM_DATE)
    assert avro.conforms(-40_000, DEBEZIUM_DATE)
    assert not avro.conforms("1948-03-14", DEBEZIUM_DATE)
