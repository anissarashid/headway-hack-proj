"""What a cleaned change event promises, asserted without a broker.

Four claims, each of which the pipeline can break while every dashboard stays
green.

*The key and the value say the same thing.* The applier upserts on the message
key, so a key that still holds ``patient_id = 4711`` next to a row image holding
a token is one duplicate row per change event, forever, with no error anywhere.
The tests here check it for an insert, an update and a delete, because the delete
is the case where the key comes from ``before`` and a naive implementation reads
``after``.

*A key column is injective or the topic halts.* ``redact`` on a primary key does
not leak -- it maps every patient onto one token, and the applier then *merges*
every patient into one row. So :data:`deid.envelope.KEY_OPS` is a closed set of
two, and everything else is refused at startup, naming the column.

*The commit time is ``source.ts_ms`` and nothing else.* It becomes the cleaned
record's Kafka timestamp, which is the entire point-in-time mechanism, so a
record that cannot answer for it is refused rather than stamped with wall clock.

*A record that does not match the schema it was derived from is refused.* This is
the runtime half of the enforcement: ``ALTER TABLE`` on a running system, and the
new column has to be a halt rather than a field nobody looks at.
"""

from __future__ import annotations

import copy
import io
from datetime import date

import fastavro
import pytest
import yaml

from deid import avro, envelope, ops, policy, schema

SALT = b"a-fixed-test-salt-not-a-real-one"
KEYS = ops.Keys(salt=SALT, reference_date=date(2026, 8, 1))

TABLE = "public.patients"
RAW_TOPIC = "raw.public.patients"
CLEAN_TOPIC = "clean.public.patients"
EPOCH = date(1970, 1, 1)

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

# One row of the shape the demo raw schema describes. Deliberately dirty in the
# places the load generator plants dirt: a padded mrn, a zip with a +4.
RAW_ROW = {
    "patient_id": 4711,
    "mrn": "  MRN-000482 ",
    "first_name": "Rosalind",
    "middle_name": None,
    "last_name": "Chen",
    "date_of_birth": (date(1948, 3, 14) - EPOCH).days,
    "ssn": "078-05-1120",
    "email": "r.chen@example.org",
    "phone": "617-555-0100",
    "address_line1": "12 Elm St",
    "address_line2": None,
    "city": "Cambridge",
    "state": "MA",
    "postal_code": "02139-1234",
    "created_at": "2026-03-02T09:15:00Z",
    "updated_at": "2026-03-02T09:15:00Z",
}

SOURCE_BLOCK = {
    "version": "3.0.0.Final",
    "connector": "postgresql",
    "name": "raw",
    "ts_ms": 1_771_000_000_000,
    "snapshot": None,
    "db": "pit",
    "sequence": None,
    "schema": "public",
    "table": "patients",
    "txId": 99,
    "lsn": 1000,
    "xmin": None,
}


def parse(text: str = PATIENTS_POLICY, table: str = TABLE):
    parsed = policy.parse_policy(yaml.safe_load(text), source="test.yml")
    return parsed, parsed.table(table)


def transformer(
    text: str = PATIENTS_POLICY,
    *,
    raw_value=None,
    raw_key=None,
    table: str = TABLE,
) -> envelope.TableTransformer:
    parsed, table_policy = parse(text, table)
    return envelope.TableTransformer.for_table(
        table,
        schema.DEMO_RAW_VALUE_SCHEMA if raw_value is None else raw_value,
        schema.DEMO_RAW_KEY_SCHEMA if raw_key is None else raw_key,
        table_policy,
        keys=KEYS,
        on_uncovered=parsed.on_uncovered_column,
        clean_namespace=CLEAN_TOPIC,
        source=parsed.source,
    )


def event(before=None, after=None, op="c", **source):
    block = {**SOURCE_BLOCK, **source}
    return {
        "before": copy.deepcopy(before),
        "after": copy.deepcopy(after),
        "source": block,
        "op": op,
        "ts_ms": block["ts_ms"] + 123,
        "transaction": None,
    }


def writable(record, avro_schema) -> bool:
    """Could this record be written against this schema, and read back as itself?

    ``avro.conforms`` deliberately refuses to resolve a named reference, and a
    Debezium envelope has one -- ``after`` is the name ``before`` defined. So the
    whole-envelope claim is checked with fastavro, which is the same library the
    runner serializes with, against the same parsed schema.
    """
    buffer = io.BytesIO()
    parsed = fastavro.parse_schema(avro_schema)
    fastavro.schemaless_writer(buffer, parsed, record)
    buffer.seek(0)
    return fastavro.schemaless_reader(buffer, parsed) == record


def key_schema(column_type, column: str = "patient_id") -> dict:
    return {
        "type": "record",
        "name": "Key",
        "namespace": RAW_TOPIC,
        "fields": [{"name": column, "type": column_type}],
        "connect.name": f"{RAW_TOPIC}.Key",
    }


# ---------------------------------------------------------------------------
# the key agrees with the value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "op, before, after",
    [
        ("c", None, RAW_ROW),
        ("u", RAW_ROW, {**RAW_ROW, "email": "new@example.org"}),
        ("d", RAW_ROW, None),
        ("r", None, RAW_ROW),  # a snapshot read
    ],
)
def test_key_is_the_cleaned_row_image(op, before, after):
    """Whichever image the key comes from, it is the one that was cleaned.

    Not "the two computations happen to agree" -- the key is assembled out of
    the cleaned row image, so there is no second computation to disagree.
    """
    built = transformer()
    cleaned = built.clean(event(before=before, after=after, op=op))

    image = cleaned.value["after"] if after is not None else cleaned.value["before"]
    assert cleaned.key == {"patient_id": image["patient_id"]}
    # And it is a surrogate, not the source value that came in.
    assert cleaned.key["patient_id"] != RAW_ROW["patient_id"]


def test_key_of_a_delete_comes_from_the_before_image():
    built = transformer()
    deleted = built.clean(event(before=RAW_ROW, after=None, op="d"))
    inserted = built.clean(event(before=None, after=RAW_ROW, op="c"))
    # Same row, so the same key -- which is what lets the applier delete the row
    # its own insert created.
    assert deleted.key == inserted.key


def test_key_columns_survive_into_the_clean_row_image():
    built = transformer()
    for column in built.key_columns:
        assert column in built.clean_columns


def test_the_cleaned_key_fits_the_clean_key_schema():
    built = transformer()
    cleaned = built.clean(event(after=RAW_ROW))
    assert avro.conforms(cleaned.key, built.clean_key_schema)


def test_the_cleaned_value_fits_the_clean_value_schema():
    """The record is writable against the schema that was registered for it.

    This is the produce-time failure the whole two-halves-of-an-op design exists
    to move to startup: if it were false, the registry would reject the record
    and the topic would stop with a half-written stream behind it.
    """
    built = transformer()
    cleaned = built.clean(event(before=RAW_ROW, after=RAW_ROW, op="u"))
    assert writable(cleaned.value, built.clean_value_schema)


# ---------------------------------------------------------------------------
# a key column is injective, or the topic halts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rule", ["{ op: drop }", '{ op: "null" }', "{ op: redact }"])
def test_a_key_column_takes_only_passthrough_or_hmac(rule):
    """Every other op is many-to-one, and a many-to-one key merges rows.

    Worth being explicit about the failure this prevents: it is not a leak. The
    applier upserts on the key, so two patients that land on the same token
    become one row in the sink, silently, and the replica under-reports its own
    population.
    """
    text = PATIENTS_POLICY.replace(
        "patient_id:    { op: hmac, domain: patient }", f"patient_id:    {rule}"
    )
    with pytest.raises(envelope.KeyColumnError) as raised:
        transformer(text)
    assert "patient_id" in str(raised.value)
    assert raised.value.table == TABLE


@pytest.mark.parametrize(
    "rule", ["{ op: fake, kind: full_name }", "{ op: generalize, to: zip3 }"]
)
def test_a_key_op_the_column_type_allows_is_still_refused_on_the_key(rule):
    """The refusal is about the key, not about the column type.

    ``fake`` and ``generalize to: zip3`` both apply perfectly well to a plain
    string column -- the value derivation accepts them. They are refused here
    only because this column is the message key, and neither op is injective:
    two mrns can fake to the same name, and every mrn generalizes to the same
    three characters.
    """
    text = PATIENTS_POLICY.replace("mrn:           { op: hmac, domain: mrn }", f"mrn:           {rule}")
    with pytest.raises(envelope.KeyColumnError) as raised:
        transformer(text, raw_key=key_schema("string", column="mrn"))
    assert "mrn" in str(raised.value)


@pytest.mark.parametrize("rule", ["{ op: passthrough }", "{ op: hmac, domain: patient }"])
def test_the_two_injective_ops_are_accepted_on_a_key(rule):
    text = PATIENTS_POLICY.replace(
        "patient_id:    { op: hmac, domain: patient }", f"patient_id:    {rule}"
    )
    built = transformer(text)
    assert built.key_columns == ("patient_id",)


def test_a_nullable_key_type_is_refused():
    """An op widens to nullable when it can fail to read its input.

    On the record where it does fail, the applier gets a null key -- so the
    widening has to be refused for a key column at startup, not discovered on
    the one row that trips it.
    """
    with pytest.raises(envelope.KeyColumnError) as raised:
        transformer(raw_key=key_schema(["null", "string"]))
    assert "nullable" in str(raised.value)


def test_an_uncovered_key_column_halts_even_under_drop_column():
    """``drop_column`` cannot apply to a primary key: dropping it leaves no key.

    So this is the one place where the two settings of ``on_uncovered_column``
    behave the same, and it is worth a test because the alternative -- a key
    record with no fields -- would register happily.
    """
    text = PATIENTS_POLICY.replace(
        "on_uncovered_column: halt_topic", "on_uncovered_column: drop_column"
    )
    # The anchors move to mrn first: a date_shift anchored on a column with no
    # rule is refused by the policy parser, for its own good reasons.
    text = text.replace("anchor: patient_id", "anchor: mrn")
    text = text.replace("    patient_id:    { op: hmac, domain: patient }\n", "")
    with pytest.raises(envelope.KeyColumnError) as raised:
        transformer(text)
    assert "patient_id" in str(raised.value)
    assert "primary key" in str(raised.value)


def test_a_key_record_with_no_fields_is_refused():
    """A table with no primary key: Debezium writes no usable message key."""
    empty = {**key_schema("long"), "fields": []}
    with pytest.raises(schema.MalformedEnvelopeError):
        transformer(raw_key=empty)


def test_derive_clean_key_schema_moves_the_namespace():
    _parsed, table_policy = parse()
    derived = envelope.derive_clean_key_schema(
        schema.DEMO_RAW_KEY_SCHEMA, table_policy, keys=KEYS, namespace=CLEAN_TOPIC
    )
    assert derived["namespace"] == CLEAN_TOPIC
    assert derived["connect.name"] == f"{CLEAN_TOPIC}.Key"
    # The raw schema is not touched.
    assert schema.DEMO_RAW_KEY_SCHEMA["namespace"] == RAW_TOPIC


# ---------------------------------------------------------------------------
# the commit time
# ---------------------------------------------------------------------------


def test_the_timestamp_is_source_ts_ms():
    """Not the envelope's own ts_ms, which is when the connector processed it.

    ``source.ts_ms`` is the database commit time. The two differ by however long
    the connector took, and using the wrong one puts every record slightly in
    the future of where it belongs.
    """
    built = transformer()
    cleaned = built.clean(event(after=RAW_ROW))
    assert cleaned.timestamp_ms == SOURCE_BLOCK["ts_ms"]
    assert cleaned.value["ts_ms"] != cleaned.timestamp_ms


@pytest.mark.parametrize(
    "block",
    [None, {}, {"ts_ms": None}, {"ts_ms": "1771000000000"}, {"ts_ms": True}],
)
def test_a_record_with_no_readable_commit_time_is_refused(block):
    built = transformer()
    broken = event(after=RAW_ROW)
    if block is None:
        del broken["source"]
    else:
        broken["source"] = block
    with pytest.raises(envelope.MissingCommitTimeError):
        built.clean(broken)


def test_the_envelope_passes_through_unchanged():
    """``source``, ``op``, ``ts_ms`` and ``transaction`` are not the policy's.

    A derivation or a transform that reformatted the source block would leave
    every record looking de-identified with the timeline destroyed.
    """
    built = transformer()
    raw = event(before=RAW_ROW, after=RAW_ROW, op="u")
    cleaned = built.clean(copy.deepcopy(raw))
    for field in ("source", "op", "ts_ms", "transaction"):
        assert cleaned.value[field] == raw[field]


# ---------------------------------------------------------------------------
# the row image
# ---------------------------------------------------------------------------


def test_dropped_columns_are_absent_and_the_rest_are_present():
    built = transformer()
    after = built.clean(event(after=RAW_ROW)).value["after"]
    assert set(after) == set(built.clean_columns)
    for dropped in ("ssn", "address_line1", "address_line2", "city"):
        assert dropped not in after


def test_null_in_null_out():
    built = transformer()
    sparse = {**RAW_ROW, "email": None, "phone": None, "postal_code": None}
    after = built.clean(event(after=sparse)).value["after"]
    assert after["email"] is None
    assert after["phone"] is None
    assert after["postal_code"] is None


def test_a_null_row_image_stays_null():
    built = transformer()
    assert built.clean_row(None) is None


def test_before_and_after_shift_by_the_same_offset():
    """The anchor is the whole point of ``date_shift``: intervals must survive.

    An update that moves ``updated_at`` by an hour has to still be an hour on
    the clean side, or every duration the replica exists to measure is noise.
    """
    built = transformer()
    later = "2026-03-02T10:15:00Z"
    cleaned = built.clean(event(before=RAW_ROW, after={**RAW_ROW, "updated_at": later}, op="u"))
    before, after = cleaned.value["before"], cleaned.value["after"]
    assert before["created_at"] == after["created_at"]
    # One hour in, one hour out.
    assert after["updated_at"][11:16] == "10:15"
    assert before["updated_at"][11:16] == "09:15"
    assert after["updated_at"][:10] == before["updated_at"][:10]


def test_the_same_row_cleans_the_same_way_every_time():
    """Determinism, from the outside: replay has to reproduce the clean topic."""
    first = transformer().clean(event(after=RAW_ROW))
    second = transformer().clean(event(after=RAW_ROW))
    assert first == second


# ---------------------------------------------------------------------------
# the runtime half of the enforcement
# ---------------------------------------------------------------------------


def test_a_record_with_a_new_column_is_refused():
    """``ALTER TABLE patients ADD COLUMN insurance_id text`` on a running system.

    The alternative is the one outcome the design exists to prevent: the extra
    key is ignored, the record is cleaned as if nothing had changed, and a
    column nobody reviewed is simply not in the clean topic -- until someone
    adds it to the policy for an unrelated reason and it appears.
    """
    built = transformer()
    with pytest.raises(envelope.UnexpectedColumnsError) as raised:
        built.clean(event(after={**RAW_ROW, "insurance_id": "AETNA-99"}))
    assert "insurance_id" in str(raised.value)


def test_a_record_missing_a_column_is_refused():
    """The other direction: a missing column would be cleaned as a null.

    Which writes a plausible wrong row rather than an error.
    """
    built = transformer()
    short = {key: value for key, value in RAW_ROW.items() if key != "state"}
    with pytest.raises(envelope.UnexpectedColumnsError) as raised:
        built.clean(event(after=short))
    assert "state" in str(raised.value)


def test_a_change_event_with_no_row_image_is_refused():
    built = transformer()
    with pytest.raises(envelope.NoRowImageError):
        built.clean(event(before=None, after=None, op="t"))


def test_a_policy_approved_new_column_derives_a_backward_compatible_field():
    """Why the clean subjects are BACKWARD rather than NONE.

    A column added to the source and to the policy has to be addable to the
    clean subject without breaking a reader written against the old version.
    Avro's rule is that the new field is optional, so the assertion is that the
    derived field is nullable *and* defaults to null -- which is what
    :func:`deid.schema.clean_field` produces for an op that can fail to read its
    input, and what the registry checks at registration.
    """
    raw = copy.deepcopy(schema.DEMO_RAW_VALUE_SCHEMA)
    row_image = envelope.row_image_record(raw)
    row_image["fields"].append(
        {"name": "insurance_id", "type": ["null", "string"], "default": None}
    )
    text = PATIENTS_POLICY + "    insurance_id:  { op: hmac, domain: insurance }\n"

    built = transformer(text, raw_value=raw)
    field = next(
        f
        for f in envelope.row_image_record(built.clean_value_schema)["fields"]
        if f["name"] == "insurance_id"
    )
    assert avro.is_nullable(field["type"])
    assert field["default"] is None
    assert built.clean(event(after={**RAW_ROW, "insurance_id": "AETNA-99"}))


def test_an_uncovered_new_column_halts_the_derivation():
    """The same ALTER without the policy edit: one topic halts, by name."""
    raw = copy.deepcopy(schema.DEMO_RAW_VALUE_SCHEMA)
    envelope.row_image_record(raw)["fields"].append(
        {"name": "insurance_id", "type": ["null", "string"], "default": None}
    )
    with pytest.raises(schema.UncoveredColumnError) as raised:
        transformer(raw_value=raw)
    assert "insurance_id" in str(raised.value)


# ---------------------------------------------------------------------------
# reading the schemas
# ---------------------------------------------------------------------------


def test_row_image_record_finds_the_definition_wherever_it_is():
    """``before`` or ``after`` -- whichever the connector defined it at."""
    raw = copy.deepcopy(schema.DEMO_RAW_VALUE_SCHEMA)
    fields = {field["name"]: field for field in raw["fields"]}
    # Swap the definition onto `after` and make `before` the reference.
    definition = avro.non_null(fields["before"]["type"])
    fields["before"]["type"] = ["null", f"{RAW_TOPIC}.Value"]
    fields["after"]["type"] = ["null", definition]
    assert envelope.row_image_record(raw)["name"] == "Value"


def test_row_image_record_refuses_a_schema_that_is_not_an_envelope():
    with pytest.raises(schema.MalformedEnvelopeError):
        envelope.row_image_record({"type": "record", "name": "Nope", "fields": []})


def test_column_types_reads_the_raw_types():
    types = envelope.column_types(envelope.row_image_record(schema.DEMO_RAW_VALUE_SCHEMA))
    assert types["patient_id"] == "long"
    assert avro.logical(types["date_of_birth"]) == avro.CONNECT_DATE


def test_the_transformer_covers_every_table_in_the_shipped_policy():
    """The shipped policy, against the one raw schema there is a stand-in for.

    The other four tables are covered by tests/test_schema.py and by
    `make schema-check` against the live registry; this asserts the wiring the
    runner does -- both schemas, all ops, key included -- works on the real file
    rather than on a fixture written to suit it.
    """
    parsed = policy.load_policy("policy/clinic.yml")
    built = envelope.TableTransformer.for_table(
        TABLE,
        schema.DEMO_RAW_VALUE_SCHEMA,
        schema.DEMO_RAW_KEY_SCHEMA,
        parsed.table(TABLE),
        keys=KEYS,
        on_uncovered=parsed.on_uncovered_column,
        clean_namespace=CLEAN_TOPIC,
        source=parsed.source,
    )
    assert "ssn" not in built.clean_columns
    cleaned = built.clean(event(after=RAW_ROW))
    assert writable(cleaned.value, built.clean_value_schema)
    assert writable(cleaned.key, built.clean_key_schema)
