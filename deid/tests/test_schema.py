"""What the derived clean schema promises, asserted.

The claim under test is the one the whole design rests on: the clean schema is a
function of ``(raw schema, policy)``, and a source column nobody wrote a rule
for cannot reach a clean topic because the derivation refuses to build one.

Four kinds of test here.

*Per op, through the schema.* A dropped column is absent, a retyped column has
the type its op derives, a passthrough column is untouched. These are the
claims a reviewer reads the policy file expecting.

*The named-type invariant.* Debezium's ``before`` and ``after`` are the same
Avro record, and Avro allows a fullname exactly one definition. So the derived
schema has to define the row image once and reference it once, and a test that
checks one field at a time would pass on a schema the registry rejects at
registration. :func:`deid.schema.definitions` and
:func:`deid.schema.references` are what make that assertable.

*The envelope.* ``source`` byte-identical, because ``source.ts_ms`` is what
point-in-time replay resolves T against, and a derivation that reformatted it
would still look de-identified while the timeline was gone.

*Enforcement.* An uncovered column raises under ``halt_topic`` and disappears
under ``drop_column``, and there is no third behaviour.
"""

from __future__ import annotations

import copy
import json
from datetime import date

import pytest

from deid import avro, ops, policy, schema

SALT = b"a-fixed-test-salt-not-a-real-one"
KEYS = ops.Keys(salt=SALT, reference_date=date(2026, 8, 1))

RAW_TOPIC = "raw.public.patients"
CLEAN_TOPIC = "clean.public.patients"
VALUE_NAME = f"{RAW_TOPIC}.Value"
ENVELOPE_NAME = f"{RAW_TOPIC}.Envelope"

PATIENTS_POLICY = """
on_uncovered_column: halt_topic
tables:
  public.patients:
    patient_id:    { op: hmac, domain: patient }
    mrn:           { op: hmac, domain: mrn }
    first_name:    { op: fake, kind: first_name }
    middle_name:   { op: fake, kind: middle_name }
    last_name:     { op: fake, kind: last_name }
    date_of_birth: { op: generalize, to: birth_year, cap_age: 89 }
    ssn:           { op: drop }
    email:         { op: fake, kind: email }
    phone:         { op: fake, kind: phone }
    address_line1: { op: drop }
    address_line2: { op: drop }
    city:          { op: drop }
    state:         { op: passthrough }
    postal_code:   { op: generalize, to: zip3 }
    created_at:    { op: date_shift, anchor: patient_id }
    updated_at:    { op: date_shift, anchor: patient_id }
"""


def table_policy(text: str = PATIENTS_POLICY, table: str = "public.patients"):
    import yaml

    parsed = policy.parse_policy(yaml.safe_load(text), source="test.yml")
    return parsed, parsed.table(table)


def derive(raw=None, text: str = PATIENTS_POLICY, **kwargs):
    parsed, table = table_policy(text)
    kwargs.setdefault("on_uncovered", parsed.on_uncovered_column)
    return schema.derive_clean_schema(
        schema.DEMO_RAW_VALUE_SCHEMA if raw is None else raw,
        table,
        keys=KEYS,
        source=parsed.source,
        **kwargs,
    )


def field(record, name):
    """One field of an Avro record, by name; ``None`` if it is not there."""
    for candidate in record["fields"]:
        if candidate["name"] == name:
            return candidate
    return None


def envelope_field(derived, name):
    return field(derived, name)


def row_image(derived, which: str = "before"):
    """The record definition carried by ``before``/``after``, or the reference."""
    return avro.non_null(envelope_field(derived, which)["type"])


# ---------------------------------------------------------------------------
# the fixture is the shape it claims to be
# ---------------------------------------------------------------------------


def test_demo_raw_schema_is_a_valid_envelope():
    """The stand-in has to be the thing under test, not a simplification of it.

    Specifically: it defines the row image once and references it once, which is
    the property the derivation has to preserve. If the fixture did not have it,
    every assertion about it downstream would be vacuous.
    """
    raw = schema.DEMO_RAW_VALUE_SCHEMA
    assert schema.definitions(raw) == (
        ENVELOPE_NAME,
        VALUE_NAME,
        "io.debezium.connector.postgresql.Source",
        "event.block",
    )
    assert schema.references(raw) == (VALUE_NAME,)
    schema.check_names(raw)


# ---------------------------------------------------------------------------
# one definition, one reference
# ---------------------------------------------------------------------------


def test_row_image_is_defined_once_and_referenced_once():
    """The subtle one, and the one that only fails at registration.

    Avro permits a fullname exactly one definition. Emitting the derived record
    in full at both ``before`` and ``after`` produces a schema that passes any
    per-field assertion and is rejected by the registry as a duplicate name --
    after the transformer has already started.
    """
    derived = derive()

    assert schema.definitions(derived).count(VALUE_NAME) == 1
    assert schema.references(derived).count(VALUE_NAME) == 1

    # And in the right places: Debezium defines at `before`, references at
    # `after`, so the derivation does too.
    assert isinstance(row_image(derived, "before"), dict)
    assert row_image(derived, "after") == VALUE_NAME

    # The invariant the registry checks, checked here.
    schema.check_names(derived)


def test_before_and_after_agree_on_the_derived_row_image():
    """Both fields describe one record, so there is nothing to disagree about."""
    derived = derive()
    definition = row_image(derived, "before")
    reference = row_image(derived, "after")
    assert schema._resolve(reference, RAW_TOPIC) == schema._fullname(definition, RAW_TOPIC)


def test_definition_follows_whichever_field_the_raw_schema_defined_at():
    """Field order is the connector's choice, not something to depend on.

    Debezium emits ``before`` first, so ``before`` defines. Avro requires the
    definition to precede the reference, so a connector that emitted ``after``
    first would define there -- and the derivation reads which is which off the
    schema rather than hard-coding today's order.
    """
    raw = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    before, after = raw["fields"][0], raw["fields"][1]
    assert (before["name"], after["name"]) == ("before", "after")
    raw["fields"][0] = {**after, "type": before["type"]}
    raw["fields"][1] = {**before, "type": ["null", VALUE_NAME]}

    derived = derive(raw)
    assert [f["name"] for f in derived["fields"][:2]] == ["after", "before"]
    assert isinstance(row_image(derived, "after"), dict)
    assert row_image(derived, "before") == VALUE_NAME
    schema.check_names(derived)


def test_an_invalid_raw_schema_is_reported_as_one():
    """A forward reference in the input is the input's problem, said so."""
    raw = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    before, after = raw["fields"][0], raw["fields"][1]
    # Reference at `before`, definition at `after`: not legal Avro.
    raw["fields"][0] = {**before, "type": ["null", VALUE_NAME]}
    raw["fields"][1] = {**after, "type": before["type"]}
    with pytest.raises(schema.UndefinedReferenceError, match="the raw value schema"):
        derive(raw)


def test_a_duplicated_definition_is_caught_not_registered():
    """The failure mode this module exists to move earlier."""
    doubled = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    definition = doubled["fields"][0]["type"]
    doubled["fields"][1] = {**doubled["fields"][1], "type": copy.deepcopy(definition)}
    with pytest.raises(schema.DuplicateDefinitionError, match=VALUE_NAME):
        schema.check_names(doubled)


def test_a_forward_reference_is_caught():
    """Avro resolves a name against the definitions before it, not after."""
    reordered = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    before, after = reordered["fields"][0], reordered["fields"][1]
    reordered["fields"][0] = {**before, "type": ["null", VALUE_NAME]}
    reordered["fields"][1] = {**after, "type": ["null", VALUE_NAME]}
    with pytest.raises(schema.UndefinedReferenceError, match=VALUE_NAME):
        schema.check_names(reordered)


def test_relative_and_qualified_references_resolve_the_same():
    """The registry canonicalizes to the relative form; DATA-703 found that."""
    relative = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    relative["fields"][1] = {**relative["fields"][1], "type": ["null", "Value"]}
    assert schema.references(relative) == (VALUE_NAME,)
    schema.check_names(relative)
    # And it derives to the same schema as the fully-qualified spelling.
    assert derive(relative) == derive()


# ---------------------------------------------------------------------------
# per op, through the schema
# ---------------------------------------------------------------------------


def test_a_dropped_column_disappears():
    """`drop` removes the field, in both before and after, because they are one."""
    derived = derive()
    definition = row_image(derived, "before")
    for column in ("ssn", "address_line1", "address_line2", "city"):
        assert field(definition, column) is None, column
    # It really is gone from the whole schema, not just from one field.
    assert "ssn" not in json.dumps(derived)


def test_a_retyped_column_changes_type():
    """`generalize to birth_year` turns a date into a nullable year."""
    definition = row_image(derive(), "before")
    raw_dob = field(row_image(schema.DEMO_RAW_VALUE_SCHEMA, "before"), "date_of_birth")
    assert avro.logical(raw_dob["type"]) == avro.CONNECT_DATE

    clean_dob = field(definition, "date_of_birth")
    assert clean_dob["type"] == ["null", "int"]
    # The op widened a NOT NULL column, so the field gains the default that
    # keeps the nullable union legal and the subject evolvable.
    assert clean_dob["default"] is None


def test_a_passthrough_column_keeps_its_type_exactly():
    raw_state = field(row_image(schema.DEMO_RAW_VALUE_SCHEMA, "before"), "state")
    clean_state = field(row_image(derive(), "before"), "state")
    assert clean_state == raw_state


def test_hmac_keeps_the_column_writable_as_a_string():
    definition = row_image(derive(), "before")
    # patient_id is a bigint at the source; a token is not.
    assert field(definition, "patient_id")["type"] == "string"
    assert field(definition, "mrn")["type"] == "string"


def test_zip3_and_zoned_date_shift_widen_to_nullable():
    """An op that can fail to read its input says so in the type."""
    definition = row_image(derive(), "before")
    assert field(definition, "postal_code")["type"] == ["null", "string"]

    created_at = field(definition, "created_at")
    assert avro.is_nullable(created_at["type"])
    # A shifted ZonedTimestamp is still a ZonedTimestamp.
    assert avro.logical(avro.non_null(created_at["type"])) == avro.ZONED_TIMESTAMP
    assert created_at["default"] is None


def test_every_derived_column_matches_the_op_that_derived_it():
    """The schema is the ops' answers, assembled -- not a second opinion."""
    parsed, table = table_policy()
    derived = derive()
    definition = row_image(derived, "before")
    raw_definition = row_image(schema.DEMO_RAW_VALUE_SCHEMA, "before")

    for raw_field in raw_definition["fields"]:
        rule = table.rule_for(raw_field["name"])
        expected = ops.build(rule, raw_field["type"], keys=KEYS).derive_type(raw_field["type"])
        clean_field = field(definition, raw_field["name"])
        if expected is ops.DROPPED:
            assert clean_field is None, raw_field["name"]
        else:
            assert clean_field["type"] == expected, raw_field["name"]


def test_column_order_survives():
    """A reviewer diffing raw against clean should see removals, not a shuffle."""
    raw_columns = [f["name"] for f in row_image(schema.DEMO_RAW_VALUE_SCHEMA, "before")["fields"]]
    clean_columns = [f["name"] for f in row_image(derive(), "before")["fields"]]
    assert clean_columns == [c for c in raw_columns if c in clean_columns]


# ---------------------------------------------------------------------------
# the envelope is not the policy's business
# ---------------------------------------------------------------------------


def test_source_block_is_byte_identical():
    """The point-in-time timeline lives in source.ts_ms.

    Byte-identical rather than merely equal: the derivation must not reorder or
    reformat the block M6 resolves T against, because a diff that shows a change
    there is a diff someone has to reason about.
    """
    derived = derive()
    raw_source = envelope_field(schema.DEMO_RAW_VALUE_SCHEMA, "source")
    clean_source = envelope_field(derived, "source")
    assert json.dumps(clean_source) == json.dumps(raw_source)
    assert field(clean_source["type"], "ts_ms")["type"] == "long"


def test_the_rest_of_the_envelope_passes_through():
    derived = derive()
    for name in ("source", "op", "ts_ms", "transaction"):
        assert json.dumps(envelope_field(derived, name)) == json.dumps(
            envelope_field(schema.DEMO_RAW_VALUE_SCHEMA, name)
        ), name


def test_envelope_field_order_survives():
    assert [f["name"] for f in derive()["fields"]] == [
        f["name"] for f in schema.DEMO_RAW_VALUE_SCHEMA["fields"]
    ]


def test_the_policy_cannot_reach_the_envelope():
    """Belt and braces: policy.py refuses the rule, so no rule can arrive here."""
    with pytest.raises(policy.ReservedFieldError):
        table_policy(PATIENTS_POLICY + "    source: { op: drop }\n")


# ---------------------------------------------------------------------------
# purity
# ---------------------------------------------------------------------------


def test_the_raw_schema_is_not_mutated_and_is_not_aliased():
    raw = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    before = json.dumps(raw)
    derived = derive(raw)
    assert json.dumps(raw) == before

    # Mutating the result must not reach back into the input.
    row_image(derived, "before")["fields"].clear()
    envelope_field(derived, "source")["type"]["fields"].clear()
    assert json.dumps(raw) == before


def test_derivation_is_deterministic():
    assert json.dumps(derive()) == json.dumps(derive())


# ---------------------------------------------------------------------------
# enforcement
# ---------------------------------------------------------------------------

WITHOUT_SSN = PATIENTS_POLICY.replace("    ssn:           { op: drop }\n", "")


def test_an_uncovered_column_halts_and_names_itself():
    with pytest.raises(schema.UncoveredColumnError) as caught:
        derive(text=WITHOUT_SSN)
    assert caught.value.column == "ssn"
    assert caught.value.table == "public.patients"
    assert "ssn" in str(caught.value)
    # A PolicyError, so the same startup `except` that catches a bad policy
    # catches this too.
    assert isinstance(caught.value, policy.PolicyError)


def test_every_uncovered_column_is_reported_at_once():
    """One restart should tell you about all of them, not the first one."""
    stripped = WITHOUT_SSN.replace("    phone:         { op: fake, kind: phone }\n", "")
    with pytest.raises(schema.UncoveredColumnError) as caught:
        derive(text=stripped)
    assert "ssn" in str(caught.value) and "phone" in str(caught.value)
    assert caught.value.column is None


def test_drop_column_drops_the_uncovered_column_instead():
    dropping = WITHOUT_SSN.replace(
        "on_uncovered_column: halt_topic", "on_uncovered_column: drop_column"
    )
    derived = derive(text=dropping)
    assert field(row_image(derived, "before"), "ssn") is None
    # ...and nothing else changed.
    assert json.dumps(derived) == json.dumps(derive())


def test_there_is_no_passthrough_for_an_uncovered_column():
    """The two options are halt and drop; a leak is not one of them."""
    assert {member.value for member in policy.UncoveredColumn} == {
        "halt_topic",
        "drop_column",
    }


def test_a_rule_for_a_column_that_does_not_exist_is_an_error():
    """Usually the legible half of a rename, whose other half is a leak."""
    renamed = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    definition = renamed["fields"][0]["type"][1]
    definition["fields"] = [f for f in definition["fields"] if f["name"] != "ssn"]
    with pytest.raises(schema.UnknownColumnError) as caught:
        derive(renamed)
    assert caught.value.column == "ssn"


def test_a_rule_the_column_type_cannot_support_halts_at_derivation():
    """The reason the type half of an op is a function and not a comment."""
    bad = PATIENTS_POLICY.replace(
        "    postal_code:   { op: generalize, to: zip3 }",
        "    postal_code:   { op: date_shift, anchor: patient_id }",
    )
    with pytest.raises(ops.IncompatibleColumnError) as caught:
        derive(text=bad)
    assert caught.value.column == "postal_code"


# ---------------------------------------------------------------------------
# malformed input
# ---------------------------------------------------------------------------


def test_a_schema_with_no_row_image_is_refused():
    key_schema = {
        "type": "record",
        "name": "Key",
        "namespace": RAW_TOPIC,
        "fields": [{"name": "patient_id", "type": "long"}],
    }
    with pytest.raises(schema.MalformedEnvelopeError, match="before"):
        derive(key_schema)


def test_a_row_image_that_is_only_a_reference_is_refused():
    """Both fields referencing means there is no field list to derive from.

    Which is also a dangling name, so it fails as the invalid raw schema it is
    -- one diagnosis, not two.
    """
    unresolvable = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    for index in (0, 1):
        unresolvable["fields"][index] = {
            **unresolvable["fields"][index],
            "type": ["null", VALUE_NAME],
        }
    with pytest.raises(schema.UndefinedReferenceError, match="the raw value schema"):
        derive(unresolvable)


def test_before_and_after_defining_different_records_is_refused():
    """One table, one row image. Two means the wrong subject was fetched."""
    inconsistent = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    other = copy.deepcopy(inconsistent["fields"][0]["type"][1])
    # A different fullname, so this is legal Avro and only the envelope's own
    # invariant is violated.
    other["name"] = "Other"
    other["connect.name"] = f"{RAW_TOPIC}.Other"
    other["fields"] = other["fields"][:2]
    inconsistent["fields"][1] = {**inconsistent["fields"][1], "type": ["null", other]}
    with pytest.raises(schema.MalformedEnvelopeError, match="different"):
        derive(inconsistent)


def test_a_non_record_is_refused():
    with pytest.raises(schema.MalformedEnvelopeError, match="must be an Avro record"):
        derive(["null", "string"])


def test_an_ambiguous_row_image_union_is_refused():
    ambiguous = copy.deepcopy(dict(schema.DEMO_RAW_VALUE_SCHEMA))
    ambiguous["fields"][0] = {
        **ambiguous["fields"][0],
        "type": ["null", "string", ambiguous["fields"][0]["type"][1]],
    }
    with pytest.raises(schema.MalformedEnvelopeError, match="row-image record"):
        derive(ambiguous)


# ---------------------------------------------------------------------------
# namespacing
# ---------------------------------------------------------------------------


def test_namespace_moves_the_derived_records_and_nothing_else():
    """The clean topic should not claim the raw topic's fullnames.

    Two schemas under one fullname is legal in the registry and wrong for
    anything that resolves types by name. The source block keeps its own
    namespace: it is Debezium's type, not this topic's.
    """
    derived = derive(namespace=CLEAN_TOPIC)
    assert schema.definitions(derived) == (
        f"{CLEAN_TOPIC}.Envelope",
        f"{CLEAN_TOPIC}.Value",
        "io.debezium.connector.postgresql.Source",
        "event.block",
    )
    assert schema.references(derived) == (f"{CLEAN_TOPIC}.Value",)
    schema.check_names(derived)

    # connect.name moves with the name rather than contradicting it.
    assert derived["connect.name"] == f"{CLEAN_TOPIC}.Envelope"
    assert row_image(derived, "before")["connect.name"] == f"{CLEAN_TOPIC}.Value"

    # The source block is still byte-identical.
    assert json.dumps(envelope_field(derived, "source")) == json.dumps(
        envelope_field(schema.DEMO_RAW_VALUE_SCHEMA, "source")
    )


def test_namespacing_changes_only_names():
    """Same columns, same types; a rename is not a re-derivation."""
    plain = row_image(derive(), "before")
    moved = row_image(derive(namespace=CLEAN_TOPIC), "before")
    assert moved["fields"] == plain["fields"]


# ---------------------------------------------------------------------------
# the real policy, against the raw schema the connector registers
# ---------------------------------------------------------------------------


def test_the_shipped_policy_derives_public_patients():
    """The acceptance case, minus the registry.

    ``policy/clinic.yml`` is the artifact under review, and the raw schema is the
    one the connector settings produce. If this raises, the policy and the source
    schema disagree and one topic would halt at startup -- which is the design
    working, and a test failure either way.
    """
    from pathlib import Path

    here = Path(__file__).resolve().parents[1]
    parsed = policy.load_policy(here / "policy" / "clinic.yml")
    derived = schema.derive_clean_schema(
        schema.DEMO_RAW_VALUE_SCHEMA,
        parsed.table("public.patients"),
        keys=KEYS,
        on_uncovered=parsed.on_uncovered_column,
        namespace=CLEAN_TOPIC,
        source=parsed.source,
    )
    schema.check_names(derived)
    columns = {f["name"] for f in row_image(derived, "before")["fields"]}
    assert "ssn" not in columns
    assert "state" in columns
    assert field(row_image(derived, "before"), "date_of_birth")["type"] == ["null", "int"]


# The Avro types the clinic DDL produces through the connector settings in
# charts/pit/charts/connect/connectors/source-pg.json. Only the column list and
# the types matter here, so the envelope is assembled around them rather than
# written out five times.
LONG = "long"
NULLABLE_LONG = ["null", "long"]
STRING = "string"
NULLABLE_STRING = ["null", "string"]
INT = "int"
BOOLEAN = "boolean"
CONNECT_DATE = {"type": "int", "connect.name": avro.CONNECT_DATE, "logicalType": "date"}
ZONED = {"type": "string", "connect.name": avro.ZONED_TIMESTAMP}
NULLABLE_ZONED = ["null", ZONED]
DECIMAL = {
    "type": "bytes",
    "connect.name": avro.DECIMAL,
    "logicalType": "decimal",
    "precision": 12,
    "scale": 2,
    "connect.parameters": {"scale": "2", "connect.decimal.precision": "12"},
}
NULLABLE_DECIMAL = ["null", DECIMAL]
CODES = {"type": "array", "items": ["null", "string"]}
JSONB = ["null", {"type": "string", "connect.name": avro.JSON}]
STATUS = {
    "type": "string",
    "connect.name": avro.ENUM,
    "connect.parameters": {"allowed": "scheduled,checked_in,completed,cancelled,no_show"},
}

CLINIC_COLUMNS: dict[str, list[tuple[str, object]]] = {
    "public.patients": [
        ("patient_id", LONG), ("mrn", STRING), ("first_name", STRING),
        ("middle_name", NULLABLE_STRING), ("last_name", STRING),
        ("date_of_birth", CONNECT_DATE), ("ssn", NULLABLE_STRING),
        ("email", NULLABLE_STRING), ("phone", NULLABLE_STRING),
        ("address_line1", NULLABLE_STRING), ("address_line2", NULLABLE_STRING),
        ("city", NULLABLE_STRING), ("state", NULLABLE_STRING),
        ("postal_code", NULLABLE_STRING), ("created_at", ZONED), ("updated_at", ZONED),
    ],
    "public.providers": [
        ("provider_id", LONG), ("npi", STRING), ("full_name", STRING),
        ("specialty", NULLABLE_STRING), ("email", NULLABLE_STRING),
        ("created_at", ZONED), ("updated_at", ZONED),
    ],
    "public.appointments": [
        ("appointment_id", LONG), ("patient_id", LONG), ("provider_id", LONG),
        ("scheduled_at", ZONED), ("checked_in_at", NULLABLE_ZONED),
        ("completed_at", NULLABLE_ZONED), ("duration_minutes", INT),
        ("status", STATUS), ("location", NULLABLE_STRING), ("intake_answers", JSONB),
        ("created_at", ZONED), ("updated_at", ZONED),
    ],
    "public.claims": [
        ("claim_id", LONG), ("patient_id", LONG), ("appointment_id", NULLABLE_LONG),
        ("billed_amount", DECIMAL), ("allowed_amount", NULLABLE_DECIMAL),
        ("paid_amount", NULLABLE_DECIMAL), ("patient_responsibility", NULLABLE_DECIMAL),
        ("diagnosis_codes", CODES), ("procedure_code", NULLABLE_STRING),
        ("claim_status", STRING), ("submitted_at", ZONED),
        ("adjudicated_at", NULLABLE_ZONED), ("created_at", ZONED), ("updated_at", ZONED),
    ],
    "public.notes": [
        ("note_id", LONG), ("patient_id", LONG), ("provider_id", LONG),
        ("appointment_id", NULLABLE_LONG), ("amends_note_id", NULLABLE_LONG),
        ("note_type", STRING), ("body", STRING), ("authored_at", ZONED),
        ("signed_at", NULLABLE_ZONED), ("is_amended", BOOLEAN),
        ("created_at", ZONED), ("updated_at", ZONED),
    ],
}


def raw_envelope(table: str, columns: list[tuple[str, object]]) -> dict:
    """A Debezium value schema for a table: definition at `before`, reference at `after`."""
    topic = f"raw.{table}"
    value = {
        "type": "record",
        "name": "Value",
        "namespace": topic,
        "fields": [
            {"name": name, "type": avro_type}
            | ({"default": None} if avro.is_nullable(avro_type) else {})
            for name, avro_type in columns
        ],
        "connect.name": f"{topic}.Value",
    }
    return {
        "type": "record",
        "name": "Envelope",
        "namespace": topic,
        "fields": [
            {"name": "before", "type": ["null", value], "default": None},
            {"name": "after", "type": ["null", f"{topic}.Value"], "default": None},
            {"name": "source", "type": copy.deepcopy(dict(schema._SOURCE_BLOCK))},
            {"name": "op", "type": "string"},
            {"name": "ts_ms", "type": ["null", "long"], "default": None},
        ],
        "connect.name": f"{topic}.Envelope",
    }


def test_the_two_patients_fixtures_do_not_drift():
    """One is a full envelope, one is a column list; they describe one table.

    Without this, adding a column to the table below and not to
    ``DEMO_RAW_VALUE_SCHEMA`` (which the CLI and the registry check use) would
    leave the registry check quietly testing an older table than the tests do.
    """
    from_envelope = [
        (f["name"], f["type"])
        for f in row_image(schema.DEMO_RAW_VALUE_SCHEMA, "before")["fields"]
    ]
    assert [name for name, _ in from_envelope] == [
        name for name, _ in CLINIC_COLUMNS["public.patients"]
    ]
    for (name, envelope_type), (_, listed_type) in zip(
        from_envelope, CLINIC_COLUMNS["public.patients"]
    ):
        # The envelope carries the connector's `connect.version` noise; what has
        # to agree is the kind, the logical name and the nullability.
        assert avro.base(avro.non_null(envelope_type)) == avro.base(
            avro.non_null(listed_type)
        ), name
        assert avro.logical(avro.non_null(envelope_type)) == avro.logical(
            avro.non_null(listed_type)
        ), name
        assert avro.is_nullable(envelope_type) == avro.is_nullable(listed_type), name


@pytest.mark.parametrize("table", sorted(CLINIC_COLUMNS))
def test_the_shipped_policy_derives_every_clinic_table(table):
    """Every table, against the types the DDL and the connector settings produce.

    public.patients is the acceptance case, but it is also the easy one: it has
    no array, no decimal, no jsonb and no enum. The policy's hardest claims are
    on claims.diagnosis_codes (generalize an array), claims.*_amount (pass a
    precise decimal through untouched) and appointments.intake_answers (drop a
    document a column-level policy cannot see into). If any of those cannot be
    derived, that topic halts at startup -- correct behaviour and a broken
    pipeline, so it is a test failure here.
    """
    from pathlib import Path

    here = Path(__file__).resolve().parents[1]
    parsed = policy.load_policy(here / "policy" / "clinic.yml")
    raw = raw_envelope(table, CLINIC_COLUMNS[table])

    derived = schema.derive_clean_schema(
        raw,
        parsed.table(table),
        keys=KEYS,
        on_uncovered=parsed.on_uncovered_column,
        namespace=f"clean.{table}",
        source=parsed.source,
    )
    schema.check_names(derived)

    definition = row_image(derived, "before")
    assert schema.definitions(derived).count(f"clean.{table}.Value") == 1
    assert schema.references(derived).count(f"clean.{table}.Value") == 1
    # Nothing survives that the policy drops, and nothing the policy keeps is lost.
    rules = parsed.table(table).rules
    kept = {f["name"] for f in definition["fields"]}
    dropped = {c for c, rule in rules.items() if isinstance(rule.op, policy.Drop)}
    assert set(rules) - kept == dropped
    # And the timeline is intact.
    assert json.dumps(envelope_field(derived, "source")) == json.dumps(
        envelope_field(raw, "source")
    )


def test_the_hard_columns_derive_to_the_types_the_policy_intends():
    """The three columns the policy file argues hardest about."""
    from pathlib import Path

    here = Path(__file__).resolve().parents[1]
    parsed = policy.load_policy(here / "policy" / "clinic.yml")

    claims = row_image(
        schema.derive_clean_schema(
            raw_envelope("public.claims", CLINIC_COLUMNS["public.claims"]),
            parsed.table("public.claims"),
            keys=KEYS,
            on_uncovered=parsed.on_uncovered_column,
        ),
        "before",
    )
    # An array of codes generalized to categories is still an array of strings.
    assert field(claims, "diagnosis_codes")["type"] == CODES
    # Money is the payload: passed through as the precise decimal it arrived as.
    assert field(claims, "billed_amount")["type"] == DECIMAL

    appointments = row_image(
        schema.derive_clean_schema(
            raw_envelope("public.appointments", CLINIC_COLUMNS["public.appointments"]),
            parsed.table("public.appointments"),
            keys=KEYS,
            on_uncovered=parsed.on_uncovered_column,
        ),
        "before",
    )
    # Free text inside a document a column-level policy cannot see into.
    assert field(appointments, "intake_answers") is None
    # A Postgres enum is a string with an `allowed` property, passed through.
    assert field(appointments, "status")["type"] == STATUS


def test_cli_prints_a_schema_the_name_check_accepts(capsys):
    from pathlib import Path

    here = Path(__file__).resolve().parents[1]
    exit_code = schema.main(
        [str(here / "policy" / "clinic.yml"), "--namespace", CLEAN_TOPIC]
    )
    assert exit_code == 0
    printed = json.loads(capsys.readouterr().out)
    schema.check_names(printed)
    assert printed["namespace"] == CLEAN_TOPIC
