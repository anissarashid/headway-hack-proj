"""The runner's edges, driven without a broker.

The runner is the one module with sockets in it, so most of what it does can only
be checked against a cluster -- that is what ``make deid-check`` is for. Three
things can be checked here, and they are the three that fail silently:

*The Kafka timestamp is ``source.ts_ms``.* Everything about this is invisible if
it is wrong. librdkafka stamps wall-clock time when no timestamp is passed, the
clean topic looks completely healthy, and every point-in-time query resolves to
the present. So the produce call is driven with a fake producer and the timestamp
it receives is asserted against the commit time in the record.

*A halt is per topic.* A record the policy cannot cover has to stop its own topic
and leave the others flowing, and it has to leave its own offsets uncommitted so
a restart resumes from it rather than past it. Two streams, one poisoned record,
and both halves of that are assertable here.

*The schema id in a cleaned record is the id that was registered.* A record
framed with an id whose schema it does not fit is a record no consumer can read,
and the failure surfaces in the applier rather than here.

The fakes are deliberately thin -- they record what they were asked to do and
answer from a dict. Anything that needed more fidelity than that would be
testing librdkafka, which is what the acceptance check does against the real one.
"""

from __future__ import annotations

import copy
import json
from datetime import date

import fastavro
import pytest
import yaml

from deid import avro, envelope, policy, runner

SALT = b"a-fixed-test-salt-not-a-real-one"
REFERENCE_DATE = date(2026, 8, 1)

COMMIT_MS = 1_771_000_000_000

# A source block with the one field anything reads. Kept minimal on purpose: the
# envelope is copied through verbatim, so the test only has to agree with itself.
SOURCE_SCHEMA = {
    "type": "record",
    "name": "Source",
    "namespace": "io.debezium.connector.postgresql",
    "fields": [
        {"name": "ts_ms", "type": "long"},
        {"name": "table", "type": "string"},
    ],
}

POLICY = """
on_uncovered_column: halt_topic
tables:
  public.patients:
    patient_id:  { op: hmac, domain: patient }
    first_name:  { op: fake, kind: first_name }
    ssn:         { op: drop }
    visit_count: { op: passthrough }
  public.providers:
    provider_id: { op: hmac, domain: provider }
    npi:         { op: hmac, domain: npi }
"""

PATIENT_ROW = {"patient_id": 4711, "first_name": "Rosalind", "ssn": "078-05-1120", "visit_count": 3}
PROVIDER_ROW = {"provider_id": 88, "npi": "1234567893"}

TABLE_COLUMNS = {
    "public.patients": [
        {"name": "patient_id", "type": "long"},
        {"name": "first_name", "type": "string"},
        {"name": "ssn", "type": ["null", "string"], "default": None},
        {"name": "visit_count", "type": "int"},
    ],
    "public.providers": [
        {"name": "provider_id", "type": "long"},
        {"name": "npi", "type": "string"},
    ],
}
TABLE_KEYS = {"public.patients": "patient_id", "public.providers": "provider_id"}


# ---------------------------------------------------------------------------
# raw schema fixtures
# ---------------------------------------------------------------------------


def raw_value_schema(table: str, columns=None) -> dict:
    """A Debezium change envelope for one table, in the shape M3 registers.

    ``before`` defines the row image and ``after`` references it by name, which
    is the arrangement the derivation has to preserve -- see deid/schema.py.
    """
    topic = f"raw.{table}"
    return {
        "type": "record",
        "name": "Envelope",
        "namespace": topic,
        "fields": [
            {
                "name": "before",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Value",
                        "namespace": topic,
                        "fields": copy.deepcopy(columns or TABLE_COLUMNS[table]),
                    },
                ],
                "default": None,
            },
            {"name": "after", "type": ["null", f"{topic}.Value"], "default": None},
            {"name": "source", "type": copy.deepcopy(SOURCE_SCHEMA)},
            {"name": "op", "type": "string"},
            {"name": "ts_ms", "type": ["null", "long"], "default": None},
        ],
    }


def raw_key_schema(table: str) -> dict:
    topic = f"raw.{table}"
    column = TABLE_KEYS[table]
    raw_type = next(
        field["type"] for field in TABLE_COLUMNS[table] if field["name"] == column
    )
    return {
        "type": "record",
        "name": "Key",
        "namespace": topic,
        "fields": [{"name": column, "type": raw_type}],
    }


def change_event(table: str, *, after=None, before=None, op="c", ts_ms=COMMIT_MS) -> dict:
    return {
        "before": copy.deepcopy(before),
        "after": copy.deepcopy(after),
        "source": {"ts_ms": ts_ms, "table": table.split(".", 1)[1]},
        "op": op,
        "ts_ms": ts_ms + 7,
    }


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------


class FakeRegistry:
    """A registry that answers from a dict and records what it was asked to do."""

    def __init__(self, subjects: dict[str, dict]) -> None:
        self.pinged = False
        self.registrations: list[tuple[str, dict]] = []
        self.compatibility: dict[str, str] = {}
        self._latest: dict[str, dict] = dict(subjects)
        self._by_id: dict[int, runner.Registered] = {}
        self._ids: dict[str, int] = {}
        self._next_id = 100

    def ping(self) -> None:
        self.pinged = True

    def latest(self, subject: str):
        found = self._latest.get(subject)
        return None if found is None else self.entry(found)

    def by_id(self, schema_id: int) -> runner.Registered:
        return self._by_id[schema_id]

    def register(self, subject: str, avro_schema) -> runner.Registered:
        self.registrations.append((subject, avro_schema))
        self._latest[subject] = avro_schema
        return self.entry(avro_schema)

    def set_compatibility(self, subject: str, level: str) -> str:
        self.compatibility[subject] = level
        return level

    def entry(self, avro_schema) -> runner.Registered:
        """The id for a schema, allocated once and stable thereafter."""
        fingerprint = json.dumps(avro_schema, sort_keys=True, default=str)
        schema_id = self._ids.get(fingerprint)
        if schema_id is None:
            schema_id = self._next_id
            self._next_id += 1
            self._ids[fingerprint] = schema_id
        registered = runner.Registered.of(schema_id, avro_schema)
        self._by_id[schema_id] = registered
        return registered


class FakeTopics:
    def __init__(self) -> None:
        self.created: list[tuple[str, int, int]] = []

    def ensure(self, topic: str, *, partitions: int, replication_factor: int) -> str:
        self.created.append((topic, partitions, replication_factor))
        return "created (fake)"


class FakeMessage:
    def __init__(self, topic, partition, offset, key, value, error=None) -> None:
        self._topic, self._partition, self._offset = topic, partition, offset
        self._key, self._value, self._error = key, value, error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeProducer:
    def __init__(self) -> None:
        self.produced: list[dict] = []
        self.flushes = 0

    def produce(self, topic, key=None, value=None, timestamp=None, on_delivery=None):
        self.produced.append(
            {"topic": topic, "key": key, "value": value, "timestamp": timestamp}
        )
        self._callbacks = getattr(self, "_callbacks", [])
        self._callbacks.append(on_delivery)

    def flush(self, *args):
        self.flushes += 1
        for callback in getattr(self, "_callbacks", []):
            if callback is not None:
                callback(None, None)
        self._callbacks = []
        return 0

    def poll(self, timeout=0):
        return 0


class FailingProducer(FakeProducer):
    """A producer whose deliveries all fail, to drive the delivery-failure path."""

    def flush(self, *args):
        self.flushes += 1
        for callback in getattr(self, "_callbacks", []):
            if callback is not None:
                callback("Broker: Message too large", None)
        self._callbacks = []
        return 0


class FakeConsumer:
    def __init__(self) -> None:
        self.commits: list = []
        self.paused: list = []
        self.subscribed: list[str] = []
        self.closed = False

    def subscribe(self, topics, on_assign=None):
        self.subscribed = list(topics)

    def assignment(self):
        return []

    def pause(self, partitions):
        self.paused.extend(partitions)

    def commit(self, offsets=None, asynchronous=True):
        self.commits.append(list(offsets or []))

    def close(self):
        self.closed = True

    def consume(self, num_messages=1, timeout=None):
        return []


# ---------------------------------------------------------------------------
# assembling a runner
# ---------------------------------------------------------------------------


def config(**changes) -> runner.Config:
    base = dict(
        bootstrap_servers="broker.invalid:9093",
        registry_url="http://registry.invalid",
        group_id="pit-deid-test",
        policy_path="test.yml",
        salt=SALT,
        reference_date=REFERENCE_DATE,
    )
    return runner.Config(**{**base, **changes})


def build(
    policy_text: str = POLICY,
    *,
    subjects: dict | None = None,
    producer=None,
    **settings,
) -> tuple[runner.Runner, FakeRegistry, FakeProducer, FakeConsumer]:
    parsed = policy.parse_policy(yaml.safe_load(policy_text), source="test.yml")
    if subjects is None:
        subjects = {}
        for table in parsed.tables:
            subjects[f"raw.{table}-value"] = raw_value_schema(table)
            subjects[f"raw.{table}-key"] = raw_key_schema(table)
    registry = FakeRegistry(subjects)
    fake_producer = producer or FakeProducer()
    consumer = FakeConsumer()
    built = runner.Runner(
        config(**settings),
        parsed,
        registry=registry,
        admin=object(),
        consumer=consumer,
        producer=fake_producer,
    )
    built.topics = FakeTopics()
    return built, registry, fake_producer, consumer


def raw_message(
    registry: FakeRegistry,
    table: str,
    event: dict,
    *,
    offset: int = 0,
    value_schema=None,
    key_schema=None,
) -> FakeMessage:
    """A raw Kafka message, framed exactly the way Debezium's converter frames one."""
    value_schema = value_schema or raw_value_schema(table)
    key_schema = key_schema or raw_key_schema(table)
    key_column = TABLE_KEYS[table]
    image = event.get("after") or event.get("before") or {}
    return FakeMessage(
        f"raw.{table}",
        0,
        offset,
        runner.encode({key_column: image[key_column]}, registry.entry(key_schema)),
        runner.encode(event, registry.entry(value_schema)),
    )


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


def test_the_salt_has_no_default(monkeypatch):
    """A salt with a default is a configuration that de-identifies with a known key."""
    monkeypatch.delenv("DEID_SALT", raising=False)
    monkeypatch.setenv("DEID_REFERENCE_DATE", "2026-08-01")
    with pytest.raises(runner.ConfigError) as raised:
        runner.Config.from_env()
    assert "DEID_SALT" in str(raised.value)


def test_a_short_salt_is_refused(monkeypatch):
    monkeypatch.setenv("DEID_SALT", "too-short")
    monkeypatch.setenv("DEID_REFERENCE_DATE", "2026-08-01")
    with pytest.raises(runner.ConfigError):
        runner.Config.from_env().keys()


def test_the_reference_date_has_no_default(monkeypatch):
    """Read from the clock it would make the same record clean differently tomorrow.

    Which would mean a clean topic that cannot be regenerated from its raw
    topic, and an offset manifest pointing at something that has moved.
    """
    monkeypatch.setenv("DEID_SALT", "a-salt-long-enough-to-be-accepted")
    monkeypatch.delenv("DEID_REFERENCE_DATE", raising=False)
    with pytest.raises(runner.ConfigError) as raised:
        runner.Config.from_env()
    assert "DEID_REFERENCE_DATE" in str(raised.value)


def test_a_non_date_reference_date_is_refused(monkeypatch):
    monkeypatch.setenv("DEID_SALT", "a-salt-long-enough-to-be-accepted")
    monkeypatch.setenv("DEID_REFERENCE_DATE", "last tuesday")
    with pytest.raises(runner.ConfigError):
        runner.Config.from_env()


def test_from_env_reads_the_rest(monkeypatch):
    monkeypatch.setenv("DEID_SALT", "a-salt-long-enough-to-be-accepted")
    monkeypatch.setenv("DEID_REFERENCE_DATE", "2026-08-01")
    monkeypatch.setenv("DEID_BOOTSTRAP_SERVERS", "somewhere:9093")
    monkeypatch.setenv("DEID_REGISTRY_URL", "http://somewhere:8081/")
    monkeypatch.setenv("DEID_CLEAN_PARTITIONS", "3")
    parsed = runner.Config.from_env()
    assert parsed.bootstrap_servers == "somewhere:9093"
    # Trailing slash stripped, or every path becomes a double slash.
    assert parsed.registry_url == "http://somewhere:8081"
    assert parsed.clean_partitions == 3
    assert parsed.raw_topic("public.patients") == "raw.public.patients"
    assert parsed.clean_topic("public.patients") == "clean.public.patients"


def test_a_non_numeric_setting_is_refused(monkeypatch):
    monkeypatch.setenv("DEID_SALT", "a-salt-long-enough-to-be-accepted")
    monkeypatch.setenv("DEID_REFERENCE_DATE", "2026-08-01")
    monkeypatch.setenv("DEID_CLEAN_PARTITIONS", "lots")
    with pytest.raises(runner.ConfigError):
        runner.Config.from_env()


# ---------------------------------------------------------------------------
# the wire format
# ---------------------------------------------------------------------------


def test_encode_and_decode_round_trip():
    registry = FakeRegistry({})
    registered = registry.entry(raw_key_schema("public.patients"))
    payload = runner.encode({"patient_id": 4711}, registered)
    assert runner.schema_id_of(payload) == registered.schema_id
    assert runner.decode(payload, registry) == (registered.schema_id, {"patient_id": 4711})


def test_the_codec_works_in_wire_types_not_python_objects():
    """fastavro converts logical types; the ops are defined against the wire.

    A ``date`` comes back from fastavro as a ``datetime.date`` unless the
    annotation is stripped, and ``deid.ops`` expects days-since-epoch -- so
    without this the first record with a date column is a TypeError halfway
    through a topic. Symmetric on the way out, so a passthrough column is
    byte-identical.
    """
    dated = {
        "type": "record",
        "name": "Key",
        "namespace": "raw.public.patients",
        "fields": [
            {
                "name": "day",
                "type": {"type": "int", "logicalType": "date", "connect.name": avro.CONNECT_DATE},
            }
        ],
    }
    registry = FakeRegistry({})
    registered = registry.entry(dated)
    payload = runner.encode({"day": 20514}, registered)
    _schema_id, decoded = runner.decode(payload, registry)
    assert decoded == {"day": 20514}
    # The annotation is still on the schema the registry holds, for consumers.
    assert avro.logical(registered.raw["fields"][0]["type"]) == avro.CONNECT_DATE


def test_a_message_that_is_not_confluent_framed_is_refused():
    registry = FakeRegistry({})
    with pytest.raises(runner.WireFormatError):
        runner.decode(b"", registry)
    with pytest.raises(runner.WireFormatError) as raised:
        runner.decode(b"\x01\x00\x00\x00\x01body", registry)
    assert "magic byte" in str(raised.value)


def test_subject_names_follow_the_topic_name_strategy():
    assert runner.subject("clean.public.patients", "value") == "clean.public.patients-value"


# ---------------------------------------------------------------------------
# startup
# ---------------------------------------------------------------------------


def test_prepare_registers_both_subjects_and_pins_compatibility():
    built, registry, _producer, _consumer = build()
    live = built.prepare()

    assert {stream.table for stream in live} == {"public.patients", "public.providers"}
    for table in ("public.patients", "public.providers"):
        for part in ("key", "value"):
            subject = f"clean.{table}-{part}"
            assert subject in dict(registry.registrations)
            # BACKWARD is what lets a policy-approved nullable column be added later.
            assert registry.compatibility[subject] == runner.CLEAN_COMPATIBILITY


def test_prepare_creates_the_clean_topics():
    built, _registry, _producer, _consumer = build()
    built.prepare()
    assert {topic for topic, _p, _rf in built.topics.created} == {
        "clean.public.patients",
        "clean.public.providers",
    }


def test_the_clean_topic_config_is_the_one_replay_needs():
    """Three settings, each of which fails silently if it is wrong.

    Compaction discards the history replay reads; finite retention expires it;
    ``LogAppendTime`` makes the broker overwrite the commit time this pipeline
    exists to preserve. A regression in any of them leaves a topic that answers
    every query plausibly and wrongly, so the constant is asserted directly.
    """
    assert runner.CLEAN_TOPIC_CONFIG["cleanup.policy"] == "delete"
    assert runner.CLEAN_TOPIC_CONFIG["retention.ms"] == "-1"
    assert runner.CLEAN_TOPIC_CONFIG["message.timestamp.type"] == "CreateTime"


def test_a_table_the_policy_does_not_cover_halts_only_itself():
    """An uncovered column at startup: one topic halted, the rest prepared."""
    text = POLICY.replace("    ssn:         { op: drop }\n", "")
    built, _registry, _producer, _consumer = build(text)
    live = built.prepare()

    assert [stream.table for stream in live] == ["public.providers"]
    halted = built.streams["public.patients"]
    assert halted.halted is not None
    assert "ssn" in halted.halted


def test_a_missing_raw_subject_halts_only_that_table():
    """The connector has not emitted for this table yet, or is not capturing it."""
    subjects = {
        "raw.public.providers-value": raw_value_schema("public.providers"),
        "raw.public.providers-key": raw_key_schema("public.providers"),
    }
    # No wait: on a real cluster the runner loses this race on purpose, for a
    # bounded time, because Debezium registers a subject on its first record.
    built, _registry, _producer, _consumer = build(subjects=subjects, schema_wait_seconds=0.0)
    live = built.prepare()
    assert [stream.table for stream in live] == ["public.providers"]
    assert "raw.public.patients-value" in built.streams["public.patients"].halted


# ---------------------------------------------------------------------------
# the produce path
# ---------------------------------------------------------------------------


def test_the_kafka_timestamp_is_the_database_commit_time():
    """The whole point-in-time mechanism, in one assertion.

    If this is wrong nothing complains: the record is produced with wall-clock
    time, the topic looks healthy, and ``offsets_for_times(T)`` answers with the
    present for every T.
    """
    built, registry, producer, _consumer = build()
    built.prepare()
    event = change_event("public.patients", after=PATIENT_ROW, ts_ms=COMMIT_MS)

    built._process([raw_message(registry, "public.patients", event)])

    assert len(producer.produced) == 1
    produced = producer.produced[0]
    assert produced["timestamp"] == COMMIT_MS
    # Not the envelope's own ts_ms, which is when the connector saw it.
    assert produced["timestamp"] != event["ts_ms"]
    assert produced["topic"] == "clean.public.patients"


def test_the_produced_record_carries_the_registered_schema_ids():
    built, registry, producer, _consumer = build()
    built.prepare()
    stream = built.streams["public.patients"]
    built._process(
        [raw_message(registry, "public.patients", change_event("public.patients", after=PATIENT_ROW))]
    )

    produced = producer.produced[0]
    assert runner.schema_id_of(produced["value"]) == stream.clean_value.schema_id
    assert runner.schema_id_of(produced["key"]) == stream.clean_key.schema_id
    # And the bodies decode against those very schemas.
    assert runner.decode(produced["value"], registry)[1]["after"]["patient_id"] == (
        runner.decode(produced["key"], registry)[1]["patient_id"]
    )


def test_the_cleaned_record_is_de_identified():
    built, registry, producer, _consumer = build()
    built.prepare()
    built._process(
        [raw_message(registry, "public.patients", change_event("public.patients", after=PATIENT_ROW))]
    )
    after = runner.decode(producer.produced[0]["value"], registry)[1]["after"]
    assert "ssn" not in after
    assert after["patient_id"] != PATIENT_ROW["patient_id"]
    assert after["first_name"] != PATIENT_ROW["first_name"]


def test_offsets_are_committed_past_the_records_that_were_delivered():
    built, registry, producer, consumer = build()
    built.prepare()
    messages = [
        raw_message(registry, "public.patients", change_event("public.patients", after=PATIENT_ROW), offset=offset)
        for offset in (0, 1, 2)
    ]
    built._process(messages)

    assert len(consumer.commits) == 1
    committed = consumer.commits[0]
    assert [(part.topic, part.partition, part.offset) for part in committed] == [
        ("raw.public.patients", 0, 3)
    ]


def test_nothing_is_committed_when_delivery_fails():
    """At-least-once, in the direction that matters.

    A committed offset for a record the broker never took is a hole in the clean
    topic, and a hole is a row the replica never gets.
    """
    built, registry, _producer, consumer = build(producer=FailingProducer())
    built.prepare()
    built._process(
        [raw_message(registry, "public.patients", change_event("public.patients", after=PATIENT_ROW))]
    )
    assert consumer.commits == []
    assert built.streams["public.patients"].halted is not None


# ---------------------------------------------------------------------------
# halting, at runtime, per topic
# ---------------------------------------------------------------------------


def test_a_new_uncovered_column_halts_one_topic_and_leaves_the_others_flowing():
    """``ALTER TABLE patients ADD COLUMN insurance_id text``, mid-run.

    Debezium registers a new raw schema and starts emitting records that carry
    the column. The runner re-derives from the record's own writer schema, the
    policy has no rule for it, and this topic stops. The claim under test is the
    second half: ``providers`` in the same batch is produced and committed.
    """
    built, registry, producer, consumer = build()
    built.prepare()

    altered = raw_value_schema(
        "public.patients",
        columns=TABLE_COLUMNS["public.patients"]
        + [{"name": "insurance_id", "type": ["null", "string"], "default": None}],
    )
    poisoned = raw_message(
        registry,
        "public.patients",
        change_event("public.patients", after={**PATIENT_ROW, "insurance_id": "AETNA-99"}),
        offset=5,
        value_schema=altered,
    )
    healthy = raw_message(
        registry, "public.providers", change_event("public.providers", after=PROVIDER_ROW), offset=9
    )

    built._process([poisoned, healthy])

    patients, providers = built.streams["public.patients"], built.streams["public.providers"]
    assert patients.halted is not None and "insurance_id" in patients.halted
    assert providers.halted is None

    # The other topic kept flowing...
    assert [record["topic"] for record in producer.produced] == ["clean.public.providers"]
    # ...and only its offsets were committed, so a restart with the policy fixed
    # resumes at the patients record that halted it rather than past it.
    committed = {(part.topic, part.offset) for part in consumer.commits[0]}
    assert committed == {("raw.public.providers", 10)}


def test_a_halted_topic_stays_halted_for_later_batches():
    built, registry, producer, _consumer = build()
    built.prepare()
    built.streams["public.patients"].halted = "halted earlier"
    built._process(
        [raw_message(registry, "public.patients", change_event("public.patients", after=PATIENT_ROW))]
    )
    assert producer.produced == []


def test_a_column_type_change_registers_a_new_clean_version_without_a_restart():
    """``ALTER COLUMN visit_count TYPE bigint``: same columns, a different type.

    This is the schema change the runtime re-derivation handles on its own. The
    column set has not moved, so the policy still covers exactly the table, and
    the derived clean schema is a new version that BACKWARD accepts -- Avro
    promotes int to long for a reader, so consumers written against the old
    version keep working. No restart, no halt.
    """
    built, registry, producer, _consumer = build()
    built.prepare()
    before = len(registry.registrations)

    widened = raw_value_schema(
        "public.patients",
        columns=[
            {**field, "type": "long"} if field["name"] == "visit_count" else field
            for field in TABLE_COLUMNS["public.patients"]
        ],
    )
    built._process(
        [
            raw_message(
                registry,
                "public.patients",
                change_event("public.patients", after=PATIENT_ROW),
                value_schema=widened,
            )
        ]
    )

    stream = built.streams["public.patients"]
    assert stream.halted is None
    # A new version of both clean subjects, derived from the record's own writer
    # schema rather than from whatever the subject's latest version happens to be.
    assert len(registry.registrations) == before + 2
    row_image = envelope.row_image_record(stream.clean_value.raw)
    assert {"name": "visit_count", "type": "long"} in row_image["fields"]
    assert runner.decode(producer.produced[0]["value"], registry)[1]["after"]["visit_count"] == 3


def test_a_new_column_flows_once_the_policy_covers_it_and_the_runner_restarts():
    """The remediation path the halt message names, end to end.

    Order matters here and it is worth being explicit about: the source is
    altered first, so by the time anything halts, Debezium has already registered
    the raw schema that carries the new column. Restarting with the rule added
    then derives against that schema and the topic resumes. Adding the rule
    *before* the DDL is the case the derivation refuses -- a rule for a column
    the table does not have reads as protection and protects nothing.
    """
    text = POLICY.replace(
        "    visit_count: { op: passthrough }\n",
        "    visit_count: { op: passthrough }\n    insurance_id: { op: hmac, domain: insurance }\n",
    )
    altered = raw_value_schema(
        "public.patients",
        columns=TABLE_COLUMNS["public.patients"]
        + [{"name": "insurance_id", "type": ["null", "string"], "default": None}],
    )
    subjects = {
        "raw.public.patients-value": altered,
        "raw.public.patients-key": raw_key_schema("public.patients"),
        "raw.public.providers-value": raw_value_schema("public.providers"),
        "raw.public.providers-key": raw_key_schema("public.providers"),
    }
    built, registry, producer, _consumer = build(text, subjects=subjects)
    live = built.prepare()

    assert {stream.table for stream in live} == {"public.patients", "public.providers"}
    built._process(
        [
            raw_message(
                registry,
                "public.patients",
                change_event("public.patients", after={**PATIENT_ROW, "insurance_id": "AETNA-99"}),
                value_schema=altered,
            )
        ]
    )
    after = runner.decode(producer.produced[0]["value"], registry)[1]["after"]
    assert after["insurance_id"] not in (None, "AETNA-99")


def test_a_record_with_no_commit_time_halts_its_topic():
    built, registry, producer, consumer = build()
    built.prepare()
    event = change_event("public.patients", after=PATIENT_ROW)
    event["source"] = {"ts_ms": 0, "table": "patients"}
    # A zero commit time is readable, so build the unreadable case by hand: a
    # schema whose source block has no ts_ms at all.
    sourceless = raw_value_schema("public.patients")
    for field in sourceless["fields"]:
        if field["name"] == "source":
            field["type"] = {
                "type": "record",
                "name": "Source",
                "namespace": "io.debezium.connector.postgresql.other",
                "fields": [{"name": "table", "type": "string"}],
            }
    event["source"] = {"table": "patients"}
    built._process(
        [raw_message(registry, "public.patients", event, value_schema=sourceless)]
    )
    assert built.streams["public.patients"].halted is not None
    assert producer.produced == []
    assert consumer.commits == []


def test_a_tombstone_halts_rather_than_being_dropped():
    """The connector sets ``tombstones.on.delete=false``, so one means it changed.

    A tombstone carries a key and no row, and there is nothing to de-identify a
    key from. Dropping it silently would leave the applier's delete unapplied.
    """
    built, registry, _producer, _consumer = build()
    built.prepare()
    tombstone = FakeMessage(
        "raw.public.patients",
        0,
        3,
        runner.encode({"patient_id": 4711}, registry.entry(raw_key_schema("public.patients"))),
        None,
    )
    built._process([tombstone])
    assert "tombstone" in built.streams["public.patients"].halted


def test_a_consumer_error_is_logged_and_does_not_halt_the_topic():
    built, registry, producer, _consumer = build()
    built.prepare()
    built._process([FakeMessage("raw.public.patients", 0, 1, None, None, error="broker down")])
    assert built.streams["public.patients"].halted is None
    assert producer.produced == []


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------


def test_report_names_the_halted_topics(capsys):
    text = POLICY.replace("    ssn:         { op: drop }\n", "")
    built, _registry, _producer, _consumer = build(text)
    built.prepare()
    halted = built.report()
    printed = capsys.readouterr().out
    assert halted == 1
    assert "HALT public.patients" in printed
    assert "1/2 topic(s) ready, 1 halted" in printed
