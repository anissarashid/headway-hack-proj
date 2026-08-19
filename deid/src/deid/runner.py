"""The edges: the only module that touches Kafka or the schema registry.

Everything else in :mod:`deid` is a pure function of its arguments, which is why
the policy, the ops, the derivation and the envelope transform are all testable
without a broker. That property is not an accident of how the code grew; it is
maintained by keeping every socket in this one file. If a test needs a cluster,
what it is testing lives here.

Startup: schemas first, records second
--------------------------------------

The order is the design. For each captured table, before a single record is
consumed:

1. Fetch the latest raw key and value schemas Debezium registered.
2. Derive the clean schemas from ``(raw schema, policy)``. A source column with
   no rule raises here, and that halts *this one topic* -- the other tables keep
   flowing, because a new PHI column on ``patients`` is not a reason to stop
   replicating ``claims``.
3. Create the cleaned topic with ``retention.ms=-1`` and
   ``cleanup.policy=delete``.
4. Register the clean key and value schemas and set the clean subjects to
   ``BACKWARD`` compatibility.
5. Only then subscribe.

Doing it in this order is what makes "the registry enforces the policy" true
rather than aspirational: at the instant the first record is consumed, the clean
subject already exists and already describes exactly the columns the policy
allows. A transformer that registered on first record would spend its first
moments in a state where the enforcement had not happened yet.

``BACKWARD`` on the clean subjects is the setting that lets a policy-approved
column be added later: a nullable field with a default can be appended, and a
reader written against the old schema keeps working. It is also what makes an
*unapproved* schema change loud -- retyping a column that consumers read is
rejected at registration rather than quietly written.

Three runtime details, each of which the pipeline can get wrong while looking
healthy
-----------------------------------------------------------------------------

*``produce(..., timestamp=source.ts_ms)``.* The cleaned record's Kafka timestamp
is the database commit time, so ``offsets_for_times(T)`` resolves "the database
as of T" to an exact offset per partition. That offset set is the point-in-time
manifest, and this one keyword argument is the whole mechanism. Everything about
it is silent if it is wrong: librdkafka stamps wall-clock time by default, the
topic looks fine, and every point-in-time query returns the present. The
cleaned topics are therefore created with ``message.timestamp.type=CreateTime``
as well -- under ``LogAppendTime`` the broker overwrites what we send, and
nothing downstream can tell.

*``retention.ms=-1`` and ``cleanup.policy=delete``.* Compaction keeps the latest
value per key and discards the history, which is precisely what replay reads. A
compacted cleaned topic answers every query, plausibly, and wrongly. So the
topics are created explicitly rather than auto-created from broker defaults, and
an existing topic whose config disagrees is corrected loudly rather than used.

*Halting is per topic, at runtime as well as at startup.* ``ALTER TABLE patients
ADD COLUMN insurance_id text`` on a running system makes Debezium register a new
raw schema and start emitting records carrying a column the policy has never
seen. The transform refuses such a record, the runner re-derives against the
record's own writer schema, and if the new column has no rule that topic halts:
its partitions are paused, its offsets are not committed, and the other topics
are untouched. Add the column to the policy, restart, and it resumes from the
record it stopped on.

    python -m deid.runner --dry-run    # startup only: derive, create, register
    python -m deid.runner              # ... and then consume
    python -m deid.runner --verify     # check the cleaned topics against all of it
"""

from __future__ import annotations

import argparse
import dataclasses
import io
import json
import logging
import os
import signal
import struct
import sys
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Sequence

import fastavro
import requests
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)

from . import avro, envelope, ops, policy, schema
from .avro import AvroType

LOG = logging.getLogger("deid")

# The Confluent wire format, which is what Debezium's AvroConverter writes and
# therefore what this has to read: one magic byte, a big-endian 4-byte schema id,
# then the Avro binary body with no embedded schema.
MAGIC_BYTE = 0
WIRE_HEADER = struct.Struct(">bI")

# Topic name prefixes. `raw.` is the connector's `topic.prefix`; `clean.` is
# ours. Both are configurable only so a second pipeline can coexist -- the names
# are load-bearing enough that M5's applier hard-codes the clean one.
DEFAULT_RAW_PREFIX = "raw."
DEFAULT_CLEAN_PREFIX = "clean."

# The three settings a cleaned topic cannot be wrong about. See the module
# docstring: each of them fails silently.
CLEAN_TOPIC_CONFIG: Mapping[str, str] = {
    "cleanup.policy": "delete",
    "retention.ms": "-1",
    "message.timestamp.type": "CreateTime",
}

# So a policy-approved nullable column can be added to a clean subject later.
CLEAN_COMPATIBILITY = "BACKWARD"

# Debezium registers a table's subjects when it first emits for that table, so a
# transformer that starts with the connector finds nothing and would halt every
# topic. Waiting is not optional; the length of the wait is.
DEFAULT_SCHEMA_WAIT_SECONDS = 300.0
SCHEMA_POLL_SECONDS = 5.0


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class RunnerError(Exception):
    """A failure at one of this module's edges."""


class ConfigError(RunnerError):
    """The runner was not given what it needs to start.

    Raised for the whole process, not one topic: no salt means no
    de-identification, and there is no partial answer to that.
    """


class RegistryError(RunnerError):
    """The schema registry refused something, or could not be reached."""


class TopicError(RunnerError):
    """A cleaned topic could not be created with the config replay needs."""


class WireFormatError(RunnerError):
    """A message that is not Confluent-framed Avro."""


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from None


@dataclass(frozen=True)
class Config:
    """Everything the runner reads from its environment, read once.

    Two of these are required and have no default, for the same reason
    :class:`deid.ops.Keys` takes them by injection rather than reading them
    itself. A salt with a default would mean a configuration in which the
    transformer quietly de-identifies with a key anyone can guess. A reference
    date read from the clock would make the HIPAA age cap answer differently on
    different days, so replaying a raw topic a year from now would produce
    different clean records and the offset manifest would point at something
    that had moved.
    """

    bootstrap_servers: str
    registry_url: str
    group_id: str
    policy_path: str
    salt: bytes
    reference_date: date
    raw_prefix: str = DEFAULT_RAW_PREFIX
    clean_prefix: str = DEFAULT_CLEAN_PREFIX
    clean_partitions: int = 1
    clean_replication_factor: int = 1
    batch_size: int = 500
    poll_timeout: float = 1.0
    schema_wait_seconds: float = DEFAULT_SCHEMA_WAIT_SECONDS

    @classmethod
    def from_env(cls) -> "Config":
        salt = os.environ.get("DEID_SALT", "")
        if not salt.strip():
            raise ConfigError(
                "DEID_SALT is empty or unset. Every surrogate in every clean topic "
                "descends from it, so there is no default that is not a leak; the deid "
                "chart mounts it from a Secret. Note that a Secret is base64, not "
                "encryption -- anywhere shared, this comes from a real secret manager"
            )
        raw_reference = os.environ.get("DEID_REFERENCE_DATE", "").strip()
        if not raw_reference:
            raise ConfigError(
                "DEID_REFERENCE_DATE is empty or unset. It is the date HIPAA's age cap "
                "is measured against, and it is configured rather than read from the "
                "clock so that replaying a raw topic produces byte-identical clean "
                "records whenever it runs. Set it to an ISO date, e.g. 2026-08-01"
            )
        try:
            reference_date = date.fromisoformat(raw_reference)
        except ValueError:
            raise ConfigError(
                f"DEID_REFERENCE_DATE must be an ISO date like 2026-08-01, got {raw_reference!r}"
            ) from None

        return cls(
            bootstrap_servers=os.environ.get("DEID_BOOTSTRAP_SERVERS", "pit-redpanda:9093"),
            registry_url=os.environ.get(
                "DEID_REGISTRY_URL", "http://pit-redpanda:8081"
            ).rstrip("/"),
            group_id=os.environ.get("DEID_GROUP_ID", "pit-deid"),
            policy_path=policy.policy_path_from_env(),
            salt=salt.encode("utf-8"),
            reference_date=reference_date,
            raw_prefix=os.environ.get("DEID_RAW_PREFIX", DEFAULT_RAW_PREFIX),
            clean_prefix=os.environ.get("DEID_CLEAN_PREFIX", DEFAULT_CLEAN_PREFIX),
            clean_partitions=_env_int("DEID_CLEAN_PARTITIONS", 1),
            clean_replication_factor=_env_int("DEID_CLEAN_REPLICATION_FACTOR", 1),
            batch_size=_env_int("DEID_BATCH_SIZE", 500),
            poll_timeout=_env_float("DEID_POLL_TIMEOUT", 1.0),
            schema_wait_seconds=_env_float(
                "DEID_SCHEMA_WAIT_SECONDS", DEFAULT_SCHEMA_WAIT_SECONDS
            ),
        )

    def keys(self) -> ops.Keys:
        """The injected key material, validated by :class:`deid.ops.Keys` itself."""
        try:
            return ops.Keys(salt=self.salt, reference_date=self.reference_date)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"DEID_SALT: {exc}") from None

    def raw_topic(self, table: str) -> str:
        return f"{self.raw_prefix}{table}"

    def clean_topic(self, table: str) -> str:
        return f"{self.clean_prefix}{table}"


# ---------------------------------------------------------------------------
# the schema registry
# ---------------------------------------------------------------------------


def wire_schema(avro_schema: AvroType) -> AvroType:
    """``avro_schema`` with every ``logicalType`` annotation removed.

    Not a detail. fastavro reads and writes logical types as Python objects: an
    ``int`` annotated ``logicalType: date`` comes back as a
    :class:`datetime.date`, a ``bytes`` annotated ``logicalType: decimal`` as a
    :class:`~decimal.Decimal`. The DATA-703 spike confirmed exactly that, and it
    is the right behaviour for an application reading records.

    It is the wrong behaviour here. :mod:`deid.ops` is defined against the *wire*
    representation, deliberately and for a stated reason: Debezium spells the
    unit of a timestamp in ``connect.name`` and nothing else, so an op has to
    read the annotation itself rather than trust a converted value, and
    :func:`deid.avro.conforms` answers the question the registry asks about wire
    types. Handing those ops a ``datetime.date`` where they expect days-since-
    epoch is a ``TypeError`` on the first record.

    So the codec works in wire types at both ends, symmetrically: a value read as
    an int is written back as an int, and a passthrough column comes out
    byte-identical. The annotation is only dropped from the schema fastavro is
    handed -- the schema *registered* for the clean subject keeps it, so the
    applier downstream still gets a ``date`` where the source had one.

    Stripping is safe because a logical type is an annotation over a primitive:
    Avro's binary encoding of ``int`` does not depend on whether it is called a
    date.
    """
    if avro.is_union(avro_schema):
        return [wire_schema(branch) for branch in avro_schema]
    if not isinstance(avro_schema, Mapping):
        return avro_schema
    stripped = {
        key: value for key, value in avro_schema.items() if key != "logicalType"
    }
    for key in ("type", "items", "values"):
        if key in stripped:
            stripped[key] = wire_schema(stripped[key])
    if "fields" in stripped:
        stripped["fields"] = [
            {**field, "type": wire_schema(field["type"])}
            if isinstance(field, Mapping) and "type" in field
            else field
            for field in stripped["fields"]
        ]
    return stripped


@dataclass(frozen=True)
class Registered:
    """A schema, the id the registry knows it by, and the form fastavro is given.

    ``raw`` is the schema as the registry holds it, annotations included, because
    that is what is registered and what consumers read. ``parsed`` is the same
    schema with the logical-type annotations stripped -- see :func:`wire_schema`
    for why the two have to differ.
    """

    schema_id: int
    parsed: Mapping[str, Any]
    raw: AvroType

    @classmethod
    def of(cls, schema_id: int, avro_schema: AvroType) -> "Registered":
        return cls(
            schema_id=schema_id,
            parsed=fastavro.parse_schema(wire_schema(avro_schema)),
            raw=avro_schema,
        )


class Registry:
    """The Confluent-API subset this needs, over ``requests``.

    A hand-written client rather than ``confluent_kafka.schema_registry``,
    because the wire framing and the schema id are load-bearing enough to want
    in plain sight: the id in the header of a cleaned record is the id of the
    schema this registered, and the record is serialized against that exact
    schema. There is no path where a serializer registers something on our
    behalf, which is the property "the registry enforces the policy" rests on.
    """

    def __init__(self, url: str, *, timeout: float = 15.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._by_id: dict[int, Registered] = {}

    # -- reading ------------------------------------------------------------

    def _get(self, path: str) -> Any | None:
        """GET, returning ``None`` for a 404 rather than raising."""
        try:
            response = self._session.get(f"{self.url}{path}", timeout=self.timeout)
        except requests.RequestException as exc:
            raise RegistryError(f"registry unreachable at {self.url}{path}: {exc}") from None
        if response.status_code == 404:
            return None
        if not response.ok:
            raise RegistryError(f"GET {path} -> {response.status_code} {response.text}")
        return response.json()

    def latest(self, subject: str) -> Registered | None:
        """The latest version of a subject, or ``None`` if it has none."""
        body = self._get(f"/subjects/{subject}/versions/latest")
        if body is None:
            return None
        return self._remember(int(body["id"]), json.loads(body["schema"]))

    def by_id(self, schema_id: int) -> Registered:
        """The schema an id names, cached.

        Cached because every record carries an id and a decode needs the parsed
        form; the mapping from id to schema is immutable in the registry, so a
        cache cannot go stale.
        """
        cached = self._by_id.get(schema_id)
        if cached is not None:
            return cached
        body = self._get(f"/schemas/ids/{schema_id}")
        if body is None:
            raise RegistryError(
                f"the registry has no schema with id {schema_id}, but a record was "
                "written with it. Either the record came from another registry or the "
                "subject was hard-deleted"
            )
        return self._remember(schema_id, json.loads(body["schema"]))

    def _remember(self, schema_id: int, raw: AvroType) -> Registered:
        entry = Registered.of(schema_id, raw)
        self._by_id[schema_id] = entry
        return entry

    # -- writing ------------------------------------------------------------

    def register(self, subject: str, avro_schema: AvroType) -> Registered:
        """Register a schema under a subject and return it with its id.

        The registry canonicalizes named-type references to their relative
        spelling (see spikes/data-703), so the schema it stores may not be
        byte-identical to the one sent. That is why the parsed form kept here is
        the one *sent*: it is what cleaned records are serialized against, and
        the two are the same Avro schema by construction even when the JSON
        differs.
        """
        payload = json.dumps({"schema": json.dumps(avro_schema), "schemaType": "AVRO"})
        try:
            response = self._session.post(
                f"{self.url}/subjects/{subject}/versions",
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                data=payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise RegistryError(
                f"registry unreachable while registering {subject}: {exc}"
            ) from None
        if response.status_code == 409:
            raise RegistryError(
                f"{subject}: the registry rejected the derived schema as incompatible with "
                f"the version already registered ({CLEAN_COMPATIBILITY} compatibility). "
                "A policy change that retypes or removes a column consumers already read "
                "is meant to fail here rather than be written silently. Either evolve it "
                "compatibly, or -- in this PoC -- delete the subject and let the clean "
                f"topic be rebuilt: {response.text}"
            )
        if not response.ok:
            raise RegistryError(
                f"POST /subjects/{subject}/versions -> "
                f"{response.status_code} {response.text}"
            )
        return self._remember(int(response.json()["id"]), avro_schema)

    def set_compatibility(self, subject: str, level: str) -> str:
        """Set a subject's compatibility level and return what it reads back as.

        Read back rather than assumed: this is the setting that decides whether
        an approved column can be added later and an unapproved change is
        refused, and a registry that ignored the request would look identical
        until the day it mattered.
        """
        try:
            response = self._session.put(
                f"{self.url}/config/{subject}",
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                data=json.dumps({"compatibility": level}),
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise RegistryError(
                f"registry unreachable while setting {subject} config: {exc}"
            ) from None
        if not response.ok:
            raise RegistryError(
                f"PUT /config/{subject} -> {response.status_code} {response.text}"
            )
        effective = self.compatibility(subject)
        if effective != level:
            raise RegistryError(
                f"{subject}: asked for {level} compatibility, the registry reports "
                f"{effective}. A clean subject that is not {level} cannot gain a "
                "policy-approved nullable column later"
            )
        return effective

    def compatibility(self, subject: str) -> str | None:
        """A subject's own compatibility level, or ``None`` if it has none set."""
        body = self._get(f"/config/{subject}")
        if body is None:
            return None
        level = body.get("compatibilityLevel") or body.get("compatibility")
        return level if isinstance(level, str) else None

    def ping(self) -> None:
        """Fail now, with a sentence, rather than per subject with a stack trace."""
        self._get("/subjects")


# ---------------------------------------------------------------------------
# the wire format
# ---------------------------------------------------------------------------


def schema_id_of(payload: bytes) -> int:
    """The schema id in a Confluent-framed message, without decoding the body.

    Cheap on purpose: every record is checked against the id its stream was
    derived from, and only a record whose id has moved is worth doing anything
    more with.
    """
    if not isinstance(payload, (bytes, bytearray)) or len(payload) < WIRE_HEADER.size:
        raise WireFormatError(
            f"a message of {len(payload) if payload else 0} bytes is too short to be "
            "Confluent-framed Avro (1 magic byte + 4 id bytes + body)"
        )
    magic, schema_id = WIRE_HEADER.unpack_from(payload)
    if magic != MAGIC_BYTE:
        raise WireFormatError(
            f"message magic byte is {magic}, not {MAGIC_BYTE}: this is not Avro written "
            "by the Confluent converter. Check the connector's key/value.converter"
        )
    return schema_id


def decode(payload: bytes, registry: Registry) -> tuple[int, Any]:
    """A Confluent-framed Avro message, as ``(schema id, record)``.

    The writer schema comes from the id in the message rather than from the
    subject's latest version, which matters the moment someone runs ``ALTER
    TABLE``: for a while the topic holds records written against two schemas,
    and decoding an old one against the new schema would silently mis-read it.
    """
    schema_id = schema_id_of(payload)
    writer = registry.by_id(schema_id)
    return schema_id, fastavro.schemaless_reader(
        io.BytesIO(payload[WIRE_HEADER.size :]), writer.parsed
    )


def encode(record: Mapping[str, Any], registered: Registered) -> bytes:
    """A record, framed with the id of the schema it was written against.

    The id and the schema are the same :class:`Registered`, so a cleaned record
    can never claim an id whose schema it does not fit.
    """
    buffer = io.BytesIO()
    buffer.write(WIRE_HEADER.pack(MAGIC_BYTE, registered.schema_id))
    fastavro.schemaless_writer(buffer, registered.parsed, record)
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# topics
# ---------------------------------------------------------------------------


class Topics:
    """Cleaned-topic creation, and the config assertions that go with it."""

    def __init__(self, admin: AdminClient, *, timeout: float = 30.0) -> None:
        self.admin = admin
        self.timeout = timeout

    def ensure(self, topic: str, *, partitions: int, replication_factor: int) -> str:
        """Create ``topic`` with the replay config, or correct it if it exists.

        Returns a one-line description of what happened, for the startup log.

        The existing-topic path is the one that matters. A cleaned topic
        auto-created by a produce inherits the broker defaults, and on a broker
        whose default is compaction that topic looks completely healthy while
        having thrown away the history replay reads. So the config is described
        and corrected rather than trusted.
        """
        new_topic = NewTopic(
            topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=dict(CLEAN_TOPIC_CONFIG),
        )
        futures = self.admin.create_topics([new_topic], request_timeout=self.timeout)
        try:
            futures[topic].result(timeout=self.timeout)
            return f"created ({', '.join(f'{k}={v}' for k, v in CLEAN_TOPIC_CONFIG.items())})"
        except KafkaException as exc:
            error = exc.args[0]
            already = (
                isinstance(error, KafkaError)
                and error.code() == KafkaError.TOPIC_ALREADY_EXISTS
            )
            if not already:
                raise TopicError(f"could not create {topic}: {exc}") from None
        except Exception as exc:  # pragma: no cover - transport-level failure
            raise TopicError(f"could not create {topic}: {exc}") from None

        wrong = self._disagreements(topic)
        if not wrong:
            return "already existed with the right config"

        LOG.error(
            "%s already existed with the wrong config (%s). Correcting it -- but note "
            "that a topic that was compacted has already discarded history, and no "
            "config change brings it back: rebuild the clean topic if a "
            "point-in-time query looks wrong.",
            topic,
            ", ".join(f"{name}={found!r} (want {want!r})" for name, found, want in wrong),
        )
        self._correct(topic, [name for name, _found, _want in wrong])
        still_wrong = self._disagreements(topic)
        if still_wrong:
            raise TopicError(
                f"{topic} keeps "
                + ", ".join(
                    f"{name}={found!r} instead of {want!r}"
                    for name, found, want in still_wrong
                )
                + ". Compaction discards the history replay reads and finite retention "
                "expires it, so this topic cannot be used for point-in-time replay"
            )
        return "existed with the wrong config; corrected"

    def _describe(self, topic: str) -> Mapping[str, str]:
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        futures = self.admin.describe_configs([resource], request_timeout=self.timeout)
        try:
            described = list(futures.values())[0].result(timeout=self.timeout)
        except Exception as exc:
            raise TopicError(f"could not read the config of {topic}: {exc}") from None
        return {name: entry.value for name, entry in described.items()}

    def _disagreements(self, topic: str) -> list[tuple[str, str | None, str]]:
        """``(setting, what it is, what it must be)`` for each one that is wrong."""
        found = self._describe(topic)
        return [
            (name, found.get(name), want)
            for name, want in CLEAN_TOPIC_CONFIG.items()
            # A broker that does not know a setting reports nothing for it. Redpanda
            # accepts all three, but not asserting the absence of a key keeps this
            # from failing against a broker that spells one differently.
            if name in found and (found.get(name) or "").lower() != want.lower()
        ]

    def _correct(self, topic: str, names: Sequence[str]) -> None:
        entries = [
            ConfigEntry(
                name, CLEAN_TOPIC_CONFIG[name], incremental_operation=AlterConfigOpType.SET
            )
            for name in names
        ]
        resource = ConfigResource(
            ConfigResource.Type.TOPIC, topic, incremental_configs=entries
        )
        futures = self.admin.incremental_alter_configs([resource], request_timeout=self.timeout)
        try:
            list(futures.values())[0].result(timeout=self.timeout)
        except Exception as exc:
            raise TopicError(f"could not correct the config of {topic}: {exc}") from None


# ---------------------------------------------------------------------------
# one table
# ---------------------------------------------------------------------------


def subject(topic: str, part: str) -> str:
    """The registry subject for a topic's key or value: ``TopicNameStrategy``.

    Debezium's converter uses it for the raw subjects, so the clean ones use it
    too -- a reader that can find ``raw.public.patients-value`` can find
    ``clean.public.patients-value`` by the same rule.
    """
    return f"{topic}-{part}"


@dataclass
class Stream:
    """One table's end-to-end state: two topics, four subjects, one transformer.

    Mutable, and the only mutable state in :mod:`deid`. ``halted`` is why: a
    topic that halts has to stay halted for the life of the process while its
    neighbours keep flowing, and that is a fact about this stream rather than
    about the run.
    """

    table: str
    raw_topic: str
    clean_topic: str
    raw_value_id: int | None = None
    raw_key_id: int | None = None
    transformer: envelope.TableTransformer | None = None
    clean_value: Registered | None = None
    clean_key: Registered | None = None
    halted: str | None = None
    records: int = 0

    @property
    def live(self) -> bool:
        return self.halted is None and self.transformer is not None


# ---------------------------------------------------------------------------
# the runner
# ---------------------------------------------------------------------------


class Runner:
    """Startup, then the consume/transform/produce loop.

    One consumer, one producer, one process. A second replica would join the
    consumer group and take some partitions, which works, but the cleaned topics
    have one partition each here and per-key ordering is what the applier needs,
    so the chart runs one.
    """

    def __init__(
        self,
        config: Config,
        parsed_policy: policy.Policy,
        *,
        registry: Registry | None = None,
        admin: AdminClient | None = None,
        consumer: Consumer | None = None,
        producer: Producer | None = None,
    ) -> None:
        self.config = config
        self.policy = parsed_policy
        self.keys = config.keys()
        self.registry = registry or Registry(config.registry_url)
        self.admin = admin or AdminClient({"bootstrap.servers": config.bootstrap_servers})
        self.topics = Topics(self.admin)
        self._consumer = consumer
        self._producer = producer
        self.streams: dict[str, Stream] = {}
        self._by_raw_topic: dict[str, Stream] = {}
        self._stopping = False
        self._delivery_failures: list[tuple[Stream, str]] = []
        # One deadline for the whole startup wait, not one per subject: five
        # tables times a five-minute wait is twenty-five minutes of a pod that
        # looks hung. What is being waited for is a single event -- the connector
        # coming up -- so one clock is the honest model of it.
        self._schema_deadline = 0.0

    # -- clients ------------------------------------------------------------

    @property
    def consumer(self) -> Consumer:
        if self._consumer is None:
            self._consumer = Consumer(
                {
                    "bootstrap.servers": self.config.bootstrap_servers,
                    "group.id": self.config.group_id,
                    # Earliest, always. The clean topics have to carry the whole
                    # history the raw topics carry, or a point in time near the
                    # start of the window resolves to an offset that holds
                    # nothing.
                    "auto.offset.reset": "earliest",
                    # Offsets are committed after the corresponding cleaned
                    # records are acknowledged, never before. Auto-commit would
                    # commit on a timer and lose records on a crash.
                    "enable.auto.commit": False,
                    "enable.partition.eof": False,
                }
            )
        return self._consumer

    @property
    def producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": self.config.bootstrap_servers,
                    # Idempotence gives ordering per partition and no duplicates
                    # from a retry. Duplicates from a *restart* are still
                    # possible -- offsets are committed after delivery, so a
                    # crash in between replays a batch -- and the applier's
                    # upsert absorbs those.
                    "enable.idempotence": True,
                    "acks": "all",
                    "linger.ms": 20,
                    "compression.type": "lz4",
                }
            )
        return self._producer

    # -- startup ------------------------------------------------------------

    def prepare(self) -> list[Stream]:
        """Schemas first: derive, create and register for every table.

        Never raises for one table's sake. A table whose derivation or
        registration fails is marked halted and named in the summary; the rest
        are prepared and returned. That is the whole point of the design -- one
        new PHI column halts one topic.
        """
        self.registry.ping()
        self._schema_deadline = time.monotonic() + self.config.schema_wait_seconds
        tables = sorted(self.policy.tables)
        LOG.info(
            "policy %s: %d table(s), on_uncovered_column=%s",
            self.policy.source,
            len(tables),
            self.policy.on_uncovered_column.value,
        )

        for table in tables:
            stream = Stream(
                table=table,
                raw_topic=self.config.raw_topic(table),
                clean_topic=self.config.clean_topic(table),
            )
            self.streams[table] = stream
            self._by_raw_topic[stream.raw_topic] = stream
            try:
                self._prepare(stream)
            except (policy.PolicyError, schema.SchemaError, envelope.EnvelopeError) as exc:
                # The design working. A policy that does not cover the source
                # schema is a halted topic, named, with the column in the message.
                self._halt(stream, f"{type(exc).__name__}: {exc}")
            except (RegistryError, TopicError) as exc:
                self._halt(stream, str(exc))

        live = [stream for stream in self.streams.values() if stream.live]
        halted = [stream for stream in self.streams.values() if stream.halted]
        for stream in live:
            LOG.info(
                "%s -> %s ready: value schema id %d, key schema id %d, %d clean column(s)",
                stream.raw_topic,
                stream.clean_topic,
                stream.clean_value.schema_id,
                stream.clean_key.schema_id,
                len(stream.transformer.clean_columns),
            )
        if halted:
            LOG.error(
                "%d of %d topic(s) HALTED and will not be consumed: %s",
                len(halted),
                len(self.streams),
                ", ".join(stream.table for stream in halted),
            )
        return live

    def _prepare(self, stream: Stream) -> None:
        raw_value = self._await_subject(subject(stream.raw_topic, "value"))
        raw_key = self._await_subject(subject(stream.raw_topic, "key"))
        self._derive(stream, raw_value, raw_key)

        detail = self.topics.ensure(
            stream.clean_topic,
            partitions=self.config.clean_partitions,
            replication_factor=self.config.clean_replication_factor,
        )
        LOG.info("%s %s", stream.clean_topic, detail)
        self._register(stream)

    def _derive(self, stream: Stream, raw_value: Registered, raw_key: Registered) -> None:
        """Build the transformer for the raw schemas in hand. Raises on refusal."""
        table_policy = self.policy.table(stream.table)
        if table_policy is None:  # pragma: no cover - streams come from the policy
            raise policy.MalformedPolicyError(
                "no rules for this table", source=self.policy.source, table=stream.table
            )
        stream.transformer = envelope.TableTransformer.for_table(
            stream.table,
            raw_value.raw,
            raw_key.raw,
            table_policy,
            keys=self.keys,
            on_uncovered=self.policy.on_uncovered_column,
            clean_namespace=stream.clean_topic,
            source=self.policy.source,
        )
        stream.raw_value_id = raw_value.schema_id
        stream.raw_key_id = raw_key.schema_id

    def _register(self, stream: Stream) -> None:
        """Register both clean subjects and pin them to BACKWARD compatibility."""
        transformer = stream.transformer
        for part, avro_schema in (
            ("value", transformer.clean_value_schema),
            ("key", transformer.clean_key_schema),
        ):
            clean_subject = subject(stream.clean_topic, part)
            registered = self.registry.register(clean_subject, avro_schema)
            level = self.registry.set_compatibility(clean_subject, CLEAN_COMPATIBILITY)
            LOG.info("%s: id %d, compatibility %s", clean_subject, registered.schema_id, level)
            if part == "value":
                stream.clean_value = registered
            else:
                stream.clean_key = registered

    def _await_subject(self, name: str) -> Registered:
        """The latest version of a raw subject, waiting for the connector to write it.

        Debezium registers a table's subjects the first time it emits for that
        table, so on a fresh install the transformer and the connector race. This
        loses that race deliberately, for a bounded time, rather than halting
        every topic because it started first.
        """
        deadline = self._schema_deadline
        announced = False
        while True:
            latest = self.registry.latest(name)
            if latest is not None:
                return latest
            if time.monotonic() >= deadline or self._stopping:
                raise RegistryError(
                    f"no {name} in the registry within the "
                    f"{self.config.schema_wait_seconds:.0f}s startup budget. Debezium "
                    "registers it when "
                    "it first emits for the table, so either the connector is not running, "
                    "or it is not capturing this table (check table.include.list), or the "
                    "table has never changed"
                )
            if not announced:
                LOG.info("waiting for %s -- Debezium registers it on its first record", name)
                announced = True
            time.sleep(min(SCHEMA_POLL_SECONDS, max(0.0, deadline - time.monotonic())))

    # -- halting ------------------------------------------------------------

    def _halt(self, stream: Stream, reason: str) -> None:
        """Stop one topic, loudly, and leave every other one flowing.

        Paused rather than unsubscribed, and its offsets are not committed, so a
        restart with the policy fixed resumes from the record that halted it
        instead of skipping the ones behind it.
        """
        if stream.halted is not None:
            return
        stream.halted = reason
        LOG.error(
            "HALT %s: %s\n"
            "    %s stops here; every other topic keeps flowing. Nothing is committed "
            "for it, so fixing %s and restarting resumes from this record.",
            stream.table,
            reason,
            stream.clean_topic,
            self.policy.source,
        )
        self._pause_halted()

    def _pause_halted(self) -> None:
        paused = [
            partition
            for partition in (self.consumer.assignment() if self._consumer else [])
            if (stream := self._by_raw_topic.get(partition.topic)) is not None and stream.halted
        ]
        if paused:
            self.consumer.pause(paused)

    # -- the loop -----------------------------------------------------------

    def run(self, *, max_records: int | None = None, idle_timeout: float | None = None) -> int:
        """Consume, transform, produce, commit. Returns the number of halted topics.

        ``max_records`` and ``idle_timeout`` exist for the acceptance check,
        which needs a run that ends. Left unset, this runs until it is signalled.
        """
        live = [stream.raw_topic for stream in self.streams.values() if stream.live]
        if not live:
            LOG.error("no topic has a clean schema; there is nothing to consume")
            return len(self.streams)

        self.consumer.subscribe(live, on_assign=self._on_assign)
        LOG.info("consuming %s", ", ".join(sorted(live)))

        produced = 0
        last_record = time.monotonic()
        while not self._stopping:
            messages = self.consumer.consume(
                num_messages=self.config.batch_size, timeout=self.config.poll_timeout
            )
            if messages:
                produced += self._process(messages)
                last_record = time.monotonic()
            else:
                self.producer.poll(0)
                if idle_timeout is not None and time.monotonic() - last_record >= idle_timeout:
                    LOG.info("no records for %.0fs; stopping", idle_timeout)
                    break
            if max_records is not None and produced >= max_records:
                LOG.info("produced %d record(s); stopping", produced)
                break
            if not any(stream.live for stream in self.streams.values()):
                LOG.error("every topic has halted; nothing left to consume")
                break

        self.close()
        halted = [stream for stream in self.streams.values() if stream.halted]
        LOG.info(
            "produced %d cleaned record(s); %d topic(s) halted%s",
            produced,
            len(halted),
            f": {', '.join(stream.table for stream in halted)}" if halted else "",
        )
        return len(halted)

    def _on_assign(self, consumer: Consumer, partitions: Sequence[TopicPartition]) -> None:
        consumer.assign(partitions)
        # A rebalance resumes everything, so a topic halted before it would
        # quietly start flowing again.
        self._pause_halted()

    def _process(self, messages: Sequence[Message]) -> int:
        """One batch: transform and produce, then commit what was acknowledged.

        Offsets are committed only for streams whose deliveries all succeeded,
        and only up to the last record handed to the producer -- so the record
        that halts a topic is never committed.
        """
        self._delivery_failures.clear()
        pending: dict[tuple[str, int], int] = {}
        produced = 0

        for message in messages:
            if message.error() is not None:
                LOG.error("consumer error on %s: %s", message.topic(), message.error())
                continue
            stream = self._by_raw_topic.get(message.topic())
            if stream is None or not stream.live:
                # Records already fetched before the pause took effect.
                continue
            try:
                record = self._transform(stream, message)
            except (policy.PolicyError, schema.SchemaError, envelope.EnvelopeError) as exc:
                self._halt(stream, f"{type(exc).__name__}: {exc}")
                continue
            except (RegistryError, WireFormatError) as exc:
                self._halt(stream, str(exc))
                continue

            try:
                self._produce(stream, record)
            except BufferError:
                # The queue is full: drain it and try once more. Dropping the
                # record is not an option -- the clean topic would have a hole in
                # it, and a hole is a row the replica never gets.
                self.producer.flush()
                self._produce(stream, record)
            except (ValueError, KafkaException) as exc:
                self._halt(stream, f"could not produce to {stream.clean_topic}: {exc}")
                continue

            stream.records += 1
            produced += 1
            key = (message.topic(), message.partition())
            pending[key] = max(pending.get(key, -1), message.offset())

        self.producer.flush()
        for stream, detail in self._delivery_failures:
            self._halt(stream, f"delivery to {stream.clean_topic} failed: {detail}")

        commits = [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in pending.items()
            if (stream := self._by_raw_topic.get(topic)) is not None and not stream.halted
        ]
        if commits:
            self.consumer.commit(offsets=commits, asynchronous=False)
        return produced

    def _produce(self, stream: Stream, record: envelope.CleanRecord) -> None:
        self.producer.produce(
            stream.clean_topic,
            key=encode(record.key, stream.clean_key),
            value=encode(record.value, stream.clean_value),
            # The point-in-time mechanism, in one keyword argument: the cleaned
            # record's Kafka timestamp is the database commit time, so
            # offsets_for_times(T) is an exact answer to "as of T".
            timestamp=record.timestamp_ms,
            on_delivery=self._delivered(stream),
        )

    def _delivered(self, stream: Stream) -> Callable[[Any, Message], None]:
        def on_delivery(error: Any, message: Message) -> None:
            if error is not None:
                self._delivery_failures.append((stream, str(error)))

        return on_delivery

    def _transform(self, stream: Stream, message: Message) -> envelope.CleanRecord:
        """One raw message, de-identified, re-deriving if the source schema moved."""
        payload = message.value()
        if payload is None:
            raise envelope.NoRowImageError(
                f"{stream.table}: a message with a null value is a tombstone, and a "
                "tombstone carries no row to de-identify. The connector is configured "
                "with tombstones.on.delete=false; set it back, or teach the applier to "
                "read a cleaned tombstone key"
            )
        # The ids are read from the message headers rather than the subjects'
        # latest versions: while a schema change is rolling out the topic holds
        # records written against two schemas, and each has to be transformed by
        # the derivation that matches the one it was written with.
        value_id = schema_id_of(payload)
        key_id = schema_id_of(message.key()) if message.key() is not None else stream.raw_key_id
        if value_id != stream.raw_value_id or key_id != stream.raw_key_id:
            self._rederive(stream, value_id, key_id)
        return stream.transformer.clean(decode(payload, self.registry)[1])

    def _rederive(self, stream: Stream, value_id: int, key_id: int) -> None:
        """The source schema changed under a running transformer.

        Re-derive against the record's own writer schemas -- not the subject's
        latest, which may already have moved again -- and register the result. A
        new column with a policy rule becomes a new clean schema version, which
        ``BACKWARD`` accepts when the field is nullable with a default. A new
        column with no rule raises, and the caller halts this one topic.
        """
        LOG.warning(
            "%s: the raw schema changed (value id %s -> %s, key id %s -> %s). "
            "Re-deriving the clean schema from the policy.",
            stream.table,
            stream.raw_value_id,
            value_id,
            stream.raw_key_id,
            key_id,
        )
        self._derive(stream, self.registry.by_id(value_id), self.registry.by_id(key_id))
        self._register(stream)

    # -- shutdown -----------------------------------------------------------

    def stop(self) -> None:
        """Ask the loop to finish its batch and exit. Safe from a signal handler."""
        self._stopping = True

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
        if self._consumer is not None:
            self._consumer.close()

    # -- reporting ----------------------------------------------------------

    def report(self) -> int:
        """Print what was derived, for ``--dry-run``. Returns the halted count."""
        print()
        for stream in self.streams.values():
            if stream.halted:
                print(f"HALT {stream.table}: {stream.halted}")
                continue
            transformer = stream.transformer
            print(f"{stream.table}: {stream.raw_topic} -> {stream.clean_topic}")
            print(
                f"  value subject {stream.clean_topic}-value id={stream.clean_value.schema_id} "
                f"compatibility={CLEAN_COMPATIBILITY}"
            )
            print(
                f"  key   subject {stream.clean_topic}-key   id={stream.clean_key.schema_id} "
                f"({', '.join(transformer.key_columns)})"
            )
            removed = sorted(set(transformer.raw_columns) - set(transformer.clean_columns))
            print(f"  columns kept    {', '.join(transformer.clean_columns)}")
            print(f"  columns removed {', '.join(removed) or 'none'}")
        halted = sum(1 for stream in self.streams.values() if stream.halted)
        ready = len(self.streams) - halted
        print(f"\n{ready}/{len(self.streams)} topic(s) ready, {halted} halted")
        return halted


# ---------------------------------------------------------------------------
# verification
# ---------------------------------------------------------------------------
#
# `--verify` asserts, against the real broker and the real registry, the claims
# this module makes that a unit test cannot reach. It lives here rather than in
# scripts/ for two reasons: everything it needs -- the registry client, the wire
# codec, the topic descriptions -- is already here, and the Kafka API is not
# port-forwarded, so an acceptance check has to run inside the cluster. It runs
# in the transformer's own pod, which is where the policy is mounted and the salt
# already is:
#
#     kubectl exec deploy/pit-deid -- /app/entrypoint.sh --verify
#
# It consumes with `assign` and its own group id, so it neither joins the
# transformer's consumer group nor commits anything.


VERIFY_SAMPLE = 25

# What each generalization target leaves in the registered clean schema. The
# ticket's acceptance criterion for this milestone is one row of this table --
# `date_of_birth` has to come out as an int and not a date -- and the rest are
# here because a generalization that quietly left the column's type alone would
# be indistinguishable from one that worked. `icd10_category` covers both of its
# shapes: a code column becomes a string, an array of codes stays an array.
GENERALIZED_WIRE_TYPES: Mapping[str, frozenset[str]] = {
    "birth_year": frozenset({"int"}),
    "year": frozenset({"int"}),
    "month": frozenset({"string"}),
    "age_band": frozenset({"string"}),
    "zip3": frozenset({"string"}),
    "icd10_category": frozenset({"string", "array"}),
}


class Verifier:
    """Checks the cleaned topics against what the design promises them to be."""

    def __init__(self, config: Config, parsed_policy: policy.Policy) -> None:
        self.config = config
        self.policy = parsed_policy
        self.registry = Registry(config.registry_url)
        self.admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})
        self.results: list[bool] = []
        self.notes: list[str] = []

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.results.append(bool(ok))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
        return bool(ok)

    def run(self) -> int:
        print(f"registry: {self.config.registry_url}")
        print(f"broker:   {self.config.bootstrap_servers}")
        print(f"policy:   {self.policy.source}")

        consumer = Consumer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                # Its own group, and it never commits: the transformer's offsets
                # are not this check's to move.
                "group.id": f"{self.config.group_id}-verify",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        try:
            for table in sorted(self.policy.tables):
                print(f"\n{table}:")
                self._verify_table(table, consumer)
        finally:
            consumer.close()

        passed, total = sum(self.results), len(self.results)
        print(f"\nRESULT: {passed}/{total} checks passed")
        for note in self.notes:
            print(f"NOT CHECKED: {note}")
        return 0 if total and passed == total else 1

    # -- per table ----------------------------------------------------------

    def _verify_table(self, table: str, consumer: Consumer) -> None:
        clean_topic = self.config.clean_topic(table)
        table_policy = self.policy.table(table)

        value_subject = subject(clean_topic, "value")
        registered = self.registry.latest(value_subject)
        if registered is None:
            self.notes.append(
                f"{table}: no {value_subject} in the registry. The transformer either "
                "has not reached this table yet or halted it -- check its logs for HALT"
            )
            print(f"  [SKIP] {value_subject} does not exist yet")
            return
        self.check("clean value subject registered", True, f"id={registered.schema_id}")

        key_subject = subject(clean_topic, "key")
        key_registered = self.registry.latest(key_subject)
        self.check("clean key subject registered", key_registered is not None, key_subject)

        self._verify_columns(table, table_policy, registered)
        self._verify_compatibility(value_subject)
        self._verify_compatibility(key_subject)
        self._verify_topic_config(clean_topic)
        self._verify_records(table, table_policy, clean_topic, consumer)

    def _verify_columns(self, table: str, table_policy, registered: Registered) -> None:
        """Exactly the columns the policy keeps, and nothing the policy drops.

        The general form of "``clean.public.patients-value`` has no ``ssn``": the
        registry is the enforcement point, so what it holds has to be checkable
        against the policy file without reading the transformer's logs.
        """
        row_image = envelope.row_image_record(registered.raw)
        present = {field["name"]: field["type"] for field in row_image["fields"]}
        dropped = {
            column
            for column, rule in table_policy.rules.items()
            if isinstance(rule.op, policy.Drop)
        }
        leaked = sorted(dropped & set(present))
        self.check(
            "every column the policy drops is absent",
            not leaked,
            f"dropped: {', '.join(sorted(dropped)) or 'none'}"
            if not leaked
            else f"STILL PRESENT: {', '.join(leaked)}",
        )
        unreviewed = sorted(set(present) - set(table_policy.rules))
        self.check(
            "every column present has a policy rule",
            not unreviewed,
            f"{len(present)} column(s)" if not unreviewed else f"no rule: {', '.join(unreviewed)}",
        )
        # Generalized columns are checked against what their target promises: the
        # registry is where "date_of_birth is an int holding a birth year, not a
        # date" becomes a fact anything downstream can read, so it is worth
        # asserting there rather than trusting the derivation that put it there.
        for column, clean_type in present.items():
            rule = table_policy.rule_for(column)
            if not isinstance(rule.op, policy.Generalize):
                continue
            wanted = GENERALIZED_WIRE_TYPES.get(rule.op.to)
            kind = avro.base(avro.non_null(clean_type))
            self.check(
                f"{column} generalized to {rule.op.to} is {'/'.join(sorted(wanted))}"
                if wanted
                else f"{column} generalized to {rule.op.to}",
                wanted is None or kind in wanted,
                avro.describe(clean_type),
            )

    def _verify_compatibility(self, name: str) -> None:
        level = self.registry.compatibility(name)
        self.check(
            f"{name} is {CLEAN_COMPATIBILITY}",
            level == CLEAN_COMPATIBILITY,
            f"compatibility={level}",
        )

    def _verify_topic_config(self, topic: str) -> None:
        """The three settings that fail silently. See CLEAN_TOPIC_CONFIG."""
        try:
            found = Topics(self.admin)._describe(topic)
        except TopicError as exc:
            self.check("cleaned topic config", False, str(exc))
            return
        for name, want in CLEAN_TOPIC_CONFIG.items():
            if name not in found:
                self.notes.append(f"{topic}: the broker reports no {name}")
                continue
            self.check(
                f"{topic} {name}={want}",
                (found[name] or "").lower() == want.lower(),
                f"{name}={found[name]!r}",
            )

    def _verify_records(self, table: str, table_policy, topic: str, consumer: Consumer) -> None:
        """The point-in-time mechanism, on records the transformer actually wrote.

        Three claims, and the first is the one the whole model rests on:

        * every cleaned record's Kafka timestamp is its own ``source.ts_ms``, and
          the broker recorded it as a CreateTime rather than stamping its own;
        * ``offsets_for_times(T)`` lands on the first record at or after T, and
          the record before it is strictly earlier -- which is what makes an
          offset manifest an exact answer to "as of T";
        * the message key is the row image's key columns, so the applier cannot
          write a row it will never find again.
        """
        partition = TopicPartition(topic, 0)
        try:
            low, high = consumer.get_watermark_offsets(partition, timeout=15)
        except KafkaException as exc:
            self.check(f"{topic} readable", False, str(exc))
            return
        if high <= low:
            self.notes.append(
                f"{topic} is empty ({low}..{high}), so nothing about the record "
                "timestamps could be checked. Change some rows in the source and "
                "try again"
            )
            print(f"  [SKIP] {topic} is empty")
            return

        start = max(low, high - VERIFY_SAMPLE)
        consumer.assign([TopicPartition(topic, 0, start)])
        records: list[tuple[int, int, Any, Any]] = []
        deadline = time.monotonic() + 30
        while len(records) < high - start and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error() is not None:
                continue
            kind, timestamp = message.timestamp()
            records.append(
                (
                    message.offset(),
                    timestamp,
                    kind,
                    (decode(message.value(), self.registry)[1], decode(message.key(), self.registry)[1]),
                )
            )
        consumer.unassign()

        if not records:
            self.check(f"{topic}: read back {high - start} record(s)", False, "read none")
            return

        mismatched = [
            (offset, timestamp, value["source"]["ts_ms"])
            for offset, timestamp, _kind, (value, _key) in records
            if timestamp != value["source"]["ts_ms"]
        ]
        self.check(
            "every record's Kafka timestamp is its source.ts_ms",
            not mismatched,
            f"{len(records)} record(s) checked"
            if not mismatched
            else f"offset {mismatched[0][0]}: timestamp {mismatched[0][1]} != "
            f"source.ts_ms {mismatched[0][2]}",
        )

        # The broker's own answer to "whose timestamp is this". Under
        # LogAppendTime it would report its own and the check above would still
        # pass on a topic whose timestamps had been overwritten.
        wrong_kind = [offset for offset, _ts, kind, _r in records if kind != TIMESTAMP_CREATE_TIME]
        self.check(
            "the broker kept the producer's timestamp (CreateTime)",
            not wrong_kind,
            "" if not wrong_kind else f"offset {wrong_kind[0]} is not a CreateTime",
        )

        key_columns = self._key_columns(table, records[0][3][1])
        disagreeing = [
            offset
            for offset, _ts, _kind, (value, key) in records
            if key != {
                column: (value["after"] or value["before"])[column] for column in key_columns
            }
        ]
        self.check(
            "the message key is the cleaned row image's key columns",
            not disagreeing,
            f"key columns: {', '.join(key_columns)}"
            if not disagreeing
            else f"offset {disagreeing[0]} disagrees, so the applier would duplicate rows",
        )

        self._verify_offsets_for_times(topic, records, consumer, low)

    def _key_columns(self, table: str, key_record: Mapping[str, Any]) -> tuple[str, ...]:
        return tuple(key_record)

    def _timestamp_at(self, consumer: Consumer, topic: str, offset: int) -> int | None:
        """The Kafka timestamp of the record at one offset, read directly.

        Read rather than looked up in the sample. The sample is only the tail of
        the topic, and the correctness of ``offsets_for_times`` is a statement
        about the whole partition: on a topic where every record shares one
        commit time -- which is what an initial snapshot produces, see the PIT
        window lower bound in the README -- the right answer is offset zero, and
        a check that compared against the sample would call that a failure.
        """
        consumer.assign([TopicPartition(topic, 0, offset)])
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                message = consumer.poll(1.0)
                if message is None or message.error() is not None:
                    continue
                return message.timestamp()[1]
            return None
        finally:
            consumer.unassign()

    def _verify_offsets_for_times(
        self,
        topic: str,
        records: Sequence[tuple[int, int, Any, Any]],
        consumer: Consumer,
        low: int,
    ) -> None:
        """``offsets_for_times(T)`` is the query the whole design exists to answer.

        Checked against its definition rather than against the sample: the record
        *at* the resolved offset is at or after T, and the record before it --
        if there is one -- is strictly before T. Those two together are what
        "the database as of T" means, and they are what M6's restore will rely on.
        """
        # The newest commit time in the sample, rather than one from the middle of
        # it. On a topic whose bulk is an initial snapshot, every snapshot record
        # carries the same wall clock, so a T taken from the middle resolves to
        # offset zero and the "what came before" half of the check has nothing to
        # look at. The newest timestamp is the one most likely to have something
        # strictly earlier behind it.
        ordered = sorted(records, key=lambda record: record[0])
        wanted = max(record[1] for record in ordered)
        try:
            resolved = consumer.offsets_for_times(
                [TopicPartition(topic, 0, wanted)], timeout=15
            )
        except KafkaException as exc:
            self.check("offsets_for_times resolves a commit time", False, str(exc))
            return
        offset = resolved[0].offset
        if offset < 0:
            self.check(
                "offsets_for_times resolves a commit time that is in the topic",
                False,
                f"T={wanted} -> {offset} (no record at or after a timestamp read off a record)",
            )
            return

        at = self._timestamp_at(consumer, topic, offset)
        self.check(
            "the record offsets_for_times(T) points at is at or after T",
            at is not None and at >= wanted,
            f"T={wanted} -> offset {offset}, whose timestamp is {at}",
        )
        if offset <= low:
            self.notes.append(
                f"{topic}: offsets_for_times(T) resolved to the start of the topic, so "
                "'the record before it is earlier than T' had nothing to check. That is "
                "what an initial snapshot looks like -- every pre-existing row carries "
                "the snapshot's wall clock, so there is only one point in time in the "
                "topic. Change some rows in the source for a timeline with "
                "distinguishable points in it"
            )
            return
        before = self._timestamp_at(consumer, topic, offset - 1)
        self.check(
            "the record before it is strictly earlier than T",
            before is not None and before < wanted,
            f"offset {offset - 1} has timestamp {before}",
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m deid.runner",
        description="De-identify raw.* Debezium topics into clean.* topics.",
    )
    parser.add_argument(
        "--policy",
        default=None,
        help="policy file (default: $PIT_POLICY_PATH, else the mounted path)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="do the whole startup -- derive, create topics, register, set compatibility "
        "-- print what it produced, and stop before consuming",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="check the cleaned topics against what the design promises -- schemas, "
        "compatibility, topic config, and that every record's Kafka timestamp is its "
        "own source.ts_ms -- then stop. Consumes nothing from the transformer's group",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="stop after producing this many cleaned records (for acceptance checks)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="stop after this many seconds with no new records (for acceptance checks)",
    )
    parser.add_argument("--log-level", default=os.environ.get("DEID_LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stdout,
    )

    try:
        config = Config.from_env()
        if args.policy:
            config = dataclasses.replace(config, policy_path=args.policy)
        parsed = policy.load_policy(config.policy_path)
    except (ConfigError, policy.PolicyError) as exc:
        LOG.error("cannot start: %s", exc)
        return 2

    if args.verify:
        return Verifier(config, parsed).run()

    LOG.info(
        "deid: %s -> %s via %s, registry %s",
        f"{config.raw_prefix}*",
        f"{config.clean_prefix}*",
        config.bootstrap_servers,
        config.registry_url,
    )
    runner = Runner(config, parsed)
    try:
        runner.prepare()
    except (RunnerError, policy.PolicyError) as exc:
        LOG.error("cannot start: %s", exc)
        return 2

    if args.dry_run:
        halted = runner.report()
        runner.close()
        return 1 if halted else 0

    for received in (signal.SIGINT, signal.SIGTERM):
        signal.signal(received, lambda *_: runner.stop())

    halted = runner.run(max_records=args.max_records, idle_timeout=args.idle_timeout)
    return 1 if halted else 0


if __name__ == "__main__":
    raise SystemExit(main())
