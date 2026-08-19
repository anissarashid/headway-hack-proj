"""One Debezium change event, de-identified: the same decision on key and value.

:mod:`deid.schema` derives the clean *value* schema and stops there, because that
is where the enforcement argument lives. A record needs three more things before
it can be produced, and each of them is a way the pipeline can look correct and
be wrong:

*The key.* Debezium writes the primary key as the message key, in its own subject
and its own Avro record. The applier in M5 upserts on that key, so if the key
says ``patient_id = 4711`` and the value's row image says the token
``KZ4W...``, the applier is inserting a row it will never find again -- one
duplicate per change event, growing forever, in a sink that reports no error. So
the key is not de-identified *as well as* the value; it is de-identified *by* the
value. :meth:`TableTransformer.clean` reads each key column straight out of the
row image it already cleaned, which makes agreement a property of the code shape
rather than of two computations that have to be kept in step.

That is also why :data:`KEY_OPS` is a closed set of two. A key column may be
passed through or HMAC'd, and nothing else, because every other op is
many-to-one: ``redact`` maps every patient onto one token, ``generalize`` maps a
cohort onto one, and the applier does not write duplicate rows for those -- it
*merges* rows, silently, and the sink ends up with one patient where the source
had four hundred. A nullable key type is refused for the same reason.

*The commit time.* ``source.ts_ms`` is the database commit time, and the runner
produces each cleaned record with it as the Kafka message timestamp; that is what
makes ``offsets_for_times(T)`` an exact answer to "the database as of T". A
record with no readable ``source.ts_ms`` therefore cannot be placed in the
timeline at all. It is refused rather than produced, because the fallback --
librdkafka stamping wall-clock time -- produces a topic that replays to a
plausible wrong answer instead of an error.

*The columns the record actually has.* Startup derivation proves the policy
covers the schema Debezium had registered at startup. Nothing stops someone
running ``ALTER TABLE`` an hour later, at which point records start arriving with
a column the policy has never seen. :meth:`TableTransformer.clean` refuses a
record whose columns are not exactly the ones it was built for, so the runner can
re-derive against the new raw schema -- and halt that one topic if the new column
has no rule. Without that check the new column would simply be ignored, which is
the leak this whole design exists to make impossible, arriving by the back door.

Everything here is pure: values and schemas in, values and schemas out. No
broker, no registry, no clock. :mod:`deid.runner` is the only module that has
edges, which is what keeps the transform itself testable without a cluster.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from . import avro, ops, policy, schema
from .avro import AvroType

# What a primary key column may be. Both are injective -- passthrough trivially,
# hmac up to a 120-bit collision -- and injectivity is the property the applier
# depends on: the clean key has to identify exactly the rows the raw key did.
# Every other op in the policy is many-to-one, and a many-to-one key does not
# leak, it merges, which is worse for being invisible in the data.
KEY_OPS = frozenset({policy.Passthrough, policy.Hmac})

# The Debezium envelope fields this module reads. `source.ts_ms` is the commit
# time the whole point-in-time model resolves against; `op` is carried for logs
# and error messages only. Neither is ever rewritten -- see deid.policy.
SOURCE_FIELD = "source"
COMMIT_TIME_FIELD = "ts_ms"
OP_FIELD = "op"


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class EnvelopeError(Exception):
    """A record or key schema this module will not transform.

    Deliberately not a :class:`~deid.policy.PolicyError`: no edit to the policy
    file fixes any of these. They mean the connector emitted something other
    than the change event this module was built against.
    """


class MissingCommitTimeError(EnvelopeError):
    """A record with no readable ``source.ts_ms``.

    Fatal for the record rather than survivable, because the alternative is
    producing it with the wrong timestamp. See the module docstring.
    """


class NoRowImageError(EnvelopeError):
    """A change event with neither a ``before`` nor an ``after`` image.

    Debezium emits one for a truncate (``op: "t"``) and for its own message
    events. There is no row to de-identify and no key to write, so guessing
    would mean putting a keyless record on a topic the applier reads by key.
    Set the connector's ``skipped.operations`` instead.
    """


class UnexpectedColumnsError(EnvelopeError):
    """A record whose columns are not the ones this transformer was built for.

    The runtime half of the enforcement: the source schema changed under a
    running transformer. The runner re-derives against the new raw schema, which
    either produces a new clean schema version or halts the topic.
    """


class KeyColumnError(policy.PolicyError):
    """The policy cannot be applied to a primary key column.

    A :class:`~deid.policy.PolicyError` because it *is* fixable in the policy
    file, and because it is caught at the same startup boundary by the same
    ``except``: one topic halts, naming the column and the op.
    """


# ---------------------------------------------------------------------------
# reading the raw schemas
# ---------------------------------------------------------------------------


def row_image_record(value_schema: AvroType) -> Mapping[str, Any]:
    """The row-image record a Debezium value schema defines.

    ``before`` and ``after`` are the same Avro named type, so exactly one of
    them carries the definition and the other is a bare name (see
    :mod:`deid.schema`). This returns the definition, wherever it is, and works
    on a derived clean schema as well as a raw one -- which is how the
    transformer reads the surviving columns off the schema it registered rather
    than recomputing them.
    """
    if not isinstance(value_schema, Mapping):
        raise schema.MalformedEnvelopeError(
            f"a value schema must be an Avro record, got {type(value_schema).__name__}"
        )
    for field in value_schema.get("fields", ()):
        if not isinstance(field, Mapping) or field.get("name") not in schema.ROW_IMAGE_FIELDS:
            continue
        for branch in avro.branches(field.get("type", "null")):
            if isinstance(branch, Mapping) and branch.get("type") == "record":
                return branch
    raise schema.MalformedEnvelopeError(
        "the value schema defines no row-image record: none of "
        f"{', '.join(schema.ROW_IMAGE_FIELDS)} carries a record definition"
    )


def column_types(record: Mapping[str, Any]) -> dict[str, AvroType]:
    """``{column: raw Avro type}`` for a row-image or key record."""
    fields = record.get("fields")
    if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)):
        raise schema.MalformedEnvelopeError(
            f"{record.get('name', 'the record')} has no 'fields' list"
        )
    types: dict[str, AvroType] = {}
    for field in fields:
        if not isinstance(field, Mapping) or not isinstance(field.get("name"), str):
            raise schema.MalformedEnvelopeError(f"a field that is not a named mapping: {field!r}")
        if "type" not in field:
            raise schema.MalformedEnvelopeError(f"field {field['name']!r} has no type")
        types[field["name"]] = field["type"]
    return types


# ---------------------------------------------------------------------------
# the clean key schema
# ---------------------------------------------------------------------------


def derive_clean_key_schema(
    raw_key_schema: AvroType,
    table_policy: policy.TablePolicy,
    *,
    keys: ops.Keys,
    namespace: str | None = None,
    source: str | None = None,
) -> AvroType:
    """The clean key schema for one table, from its raw key schema and its policy.

    Pure, like :func:`deid.schema.derive_clean_schema`, and checked at the same
    startup boundary. A key record is a flat record of the primary key columns,
    so the walk is simpler than the envelope's -- but the refusals are stricter,
    because a key that has lost a column, gained a null, or stopped being
    injective breaks the applier rather than the schema.

    ``on_uncovered_column`` is deliberately not a parameter. ``drop_column``
    cannot apply to a primary key: dropping it leaves a record with no key in
    it. So an uncovered key column halts the topic under either setting.
    """
    if not isinstance(raw_key_schema, Mapping) or raw_key_schema.get("type") != "record":
        raise schema.MalformedEnvelopeError(
            f"the raw key schema must be an Avro record, got {avro.describe(raw_key_schema)}"
        )
    raw_types = column_types(raw_key_schema)
    if not raw_types:
        raise schema.MalformedEnvelopeError(
            f"the raw key schema for {table_policy.name} has no fields, so the table has "
            "no primary key Debezium can write a message key from"
        )

    clean_fields: list[dict[str, Any]] = []
    for field in raw_key_schema["fields"]:
        column = field["name"]
        rule = table_policy.rule_for(column)
        if rule is None:
            raise KeyColumnError(
                f"{column!r} is part of the primary key and has no rule. An uncovered "
                "key column halts the topic whatever on_uncovered_column says, because "
                "a key cannot be dropped: the applier upserts on it",
                source=source,
                table=table_policy.name,
                column=column,
            )
        if type(rule.op) not in KEY_OPS:
            allowed = ", ".join(sorted(op_cls.name for op_cls in KEY_OPS))
            raise KeyColumnError(
                f"op {rule.op.name!r} cannot be applied to a primary key column "
                f"(a key column takes {allowed}). Every other op is many-to-one, and a "
                "many-to-one key does not leak -- it merges: the applier upserts on this "
                "value, so every row that lands on the same token becomes one row in the "
                "sink, with no error anywhere",
                source=source,
                table=table_policy.name,
                column=column,
            )
        clean_type = ops.build(rule, raw_types[column], keys=keys).derive_type(raw_types[column])
        if clean_type is ops.DROPPED:  # pragma: no cover - KEY_OPS excludes drop
            raise KeyColumnError(
                f"op {rule.op.name!r} removes {column!r}, which is part of the primary key",
                source=source,
                table=table_policy.name,
                column=column,
            )
        if avro.is_nullable(clean_type):
            raise KeyColumnError(
                f"op {rule.op.name!r} derives {avro.describe(clean_type)} for key column "
                f"{column!r}, and a nullable primary key is not one. An op widens to "
                "nullable when it can fail to read its input, so on the record where it "
                "does, the applier gets a null key",
                source=source,
                table=table_policy.name,
                column=column,
            )
        clean_fields.append(schema.clean_field(field, clean_type))

    clean_key = (
        schema.renamed(raw_key_schema, namespace, None) if namespace else dict(raw_key_schema)
    )
    clean_key["fields"] = clean_fields
    schema.check_names(clean_key)
    return clean_key


# ---------------------------------------------------------------------------
# one cleaned record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CleanRecord:
    """One cleaned change event, ready to produce.

    ``timestamp_ms`` is ``source.ts_ms`` verbatim -- the database commit time,
    not the time this record was transformed. The runner passes it to
    ``produce(..., timestamp=...)`` and that is the entire point-in-time
    mechanism; see the module docstring.
    """

    key: dict[str, Any]
    value: dict[str, Any]
    timestamp_ms: int
    op: str | None = None


# ---------------------------------------------------------------------------
# the transformer
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableTransformer:
    """Everything one table needs, derived once, applied to every record.

    Built by :meth:`for_table` at startup, which is where every failure the
    policy can cause surfaces: an uncovered column, a rule for a column that
    does not exist, an op the column's type cannot support, a key column that is
    not injective. After that the per-record path has no policy decisions left
    in it -- only the ops, already built and type-checked against this exact raw
    schema.

    Ops for anchored columns are the one exception: ``date_shift`` and
    ``numeric_jitter`` draw an offset from the record's anchor value, so they are
    built per record. The expensive half of that (the keyed draw) is memoized
    inside :mod:`deid.ops`, so the same patient across ten thousand records
    hashes once.
    """

    table: str
    clean_value_schema: AvroType
    clean_key_schema: AvroType
    raw_value_schema: AvroType
    raw_key_schema: AvroType

    # Columns, in schema order. `raw_columns` is what a record must carry
    # exactly; `clean_columns` is what one gets, read off the derived schema so
    # the record cannot disagree with the schema it is written against.
    raw_columns: frozenset[str]
    clean_columns: tuple[str, ...]
    key_columns: tuple[str, ...]

    # column -> the op, for columns whose op is the same for every record.
    static_ops: Mapping[str, ops.Op]
    # column -> (rule, raw type, anchor column), for the ops built per record.
    anchored: Mapping[str, tuple[policy.Rule, AvroType, str]]
    keys: ops.Keys

    @classmethod
    def for_table(
        cls,
        table: str,
        raw_value_schema: AvroType,
        raw_key_schema: AvroType,
        table_policy: policy.TablePolicy,
        *,
        keys: ops.Keys,
        on_uncovered: policy.UncoveredColumn = policy.UncoveredColumn.HALT_TOPIC,
        clean_namespace: str | None = None,
        source: str | None = None,
    ) -> "TableTransformer":
        """Derive both clean schemas and build every op, or raise.

        Raises :class:`~deid.policy.PolicyError` (including
        :class:`~deid.schema.UncoveredColumnError`, :class:`KeyColumnError` and
        :class:`~deid.ops.IncompatibleColumnError`) for anything the policy got
        wrong, and :class:`~deid.schema.SchemaError` for a raw schema that is
        not a Debezium change event. The runner catches both and halts this one
        topic.
        """
        clean_value_schema = schema.derive_clean_schema(
            raw_value_schema,
            table_policy,
            keys=keys,
            on_uncovered=on_uncovered,
            namespace=clean_namespace,
            source=source,
        )
        clean_key_schema = derive_clean_key_schema(
            raw_key_schema,
            table_policy,
            keys=keys,
            namespace=clean_namespace,
            source=source,
        )

        raw_types = column_types(row_image_record(raw_value_schema))
        clean_columns = tuple(column_types(row_image_record(clean_value_schema)))
        key_columns = tuple(column_types(raw_key_schema))

        static: dict[str, ops.Op] = {}
        anchored: dict[str, tuple[policy.Rule, AvroType, str]] = {}
        for column in clean_columns:
            rule = table_policy.rule_for(column)
            if rule is None:  # pragma: no cover - the derivation already refused
                raise KeyColumnError(
                    f"{column!r} survived into the clean schema with no rule",
                    source=source,
                    table=table,
                    column=column,
                )
            raw_type = raw_types[column]
            anchor = ops.anchor_column(rule.op)
            if anchor is None:
                static[column] = ops.build(rule, raw_type, keys=keys)
            else:
                # Built here once anyway, with no anchor, so an anchored op whose
                # column type it cannot support is refused at startup like every
                # other one rather than on the first record.
                ops.build(rule, raw_type, keys=keys)
                anchored[column] = (rule, raw_type, anchor)

        # A key column is always a clean column: KEY_OPS excludes drop, and
        # derive_clean_key_schema has already refused anything else.
        missing = [column for column in key_columns if column not in clean_columns]
        if missing:  # pragma: no cover - unreachable via derive_clean_key_schema
            raise KeyColumnError(
                f"key column(s) {', '.join(missing)} are absent from the clean row image",
                source=source,
                table=table,
                column=missing[0] if len(missing) == 1 else None,
            )

        return cls(
            table=table,
            clean_value_schema=clean_value_schema,
            clean_key_schema=clean_key_schema,
            raw_value_schema=raw_value_schema,
            raw_key_schema=raw_key_schema,
            raw_columns=frozenset(raw_types),
            clean_columns=clean_columns,
            key_columns=key_columns,
            static_ops=static,
            anchored=anchored,
            keys=keys,
        )

    # -- per record ---------------------------------------------------------

    def clean_row(self, row: Mapping[str, Any] | None) -> dict[str, Any] | None:
        """One row image, de-identified. ``None`` in, ``None`` out.

        The returned dict has exactly the fields of the clean schema's row
        image, so it is writable against the schema this transformer registered
        by construction rather than by inspection.
        """
        if row is None:
            return None
        if not isinstance(row, Mapping):
            raise UnexpectedColumnsError(
                f"{self.table}: a row image must be a mapping, got {type(row).__name__}"
            )
        self._check_columns(row, self.raw_columns, "row image")

        clean: dict[str, Any] = {}
        for column in self.clean_columns:
            op = self.static_ops.get(column)
            if op is None:
                rule, raw_type, anchor = self.anchored[column]
                # The anchor is read from the raw row, before any op has touched
                # it, so a policy that hashes or drops the anchor column still
                # gets the entity's identity -- and rules need no dependency
                # order.
                op = ops.build(rule, raw_type, keys=self.keys, anchor=row[anchor])
            clean[column] = op.apply(row[column])
        return clean

    def commit_time_ms(self, value: Mapping[str, Any]) -> int:
        """``source.ts_ms``: the database commit time, in milliseconds.

        Read, never written. The runner produces with it, so a record this
        cannot answer for is a record that cannot be placed in the timeline.
        """
        block = value.get(SOURCE_FIELD) if isinstance(value, Mapping) else None
        commit_time = block.get(COMMIT_TIME_FIELD) if isinstance(block, Mapping) else None
        if not isinstance(commit_time, int) or isinstance(commit_time, bool):
            raise MissingCommitTimeError(
                f"{self.table}: {SOURCE_FIELD}.{COMMIT_TIME_FIELD} is "
                f"{commit_time!r}, not a millisecond timestamp. It is the database "
                "commit time and the cleaned record's Kafka timestamp is set from it, "
                "so producing this record would stamp it with wall-clock time and put "
                "it in the wrong place in the timeline"
            )
        return commit_time

    def clean(self, value: Mapping[str, Any]) -> CleanRecord:
        """One raw change event, de-identified: key, value and commit time.

        The message key is not read from the raw key at all -- it is assembled
        out of the row image this call just cleaned, so key and value cannot
        disagree. Which image it comes from follows Debezium: ``after`` for an
        insert or update, ``before`` for a delete, because that is the row the
        key identifies.
        """
        if not isinstance(value, Mapping):
            raise UnexpectedColumnsError(
                f"{self.table}: a change event must be a mapping, got {type(value).__name__}"
            )

        timestamp_ms = self.commit_time_ms(value)
        clean_before = self.clean_row(value.get("before"))
        clean_after = self.clean_row(value.get("after"))

        key_row = clean_after if clean_after is not None else clean_before
        if key_row is None:
            raise NoRowImageError(
                f"{self.table}: change event with op={value.get(OP_FIELD)!r} carries "
                "neither a before nor an after image, so there is no row to "
                "de-identify and no key to write. Debezium emits these for truncate "
                "and message events; add the operation to the connector's "
                "skipped.operations"
            )

        clean_value = dict(value)
        clean_value["before"] = clean_before
        clean_value["after"] = clean_after
        return CleanRecord(
            key={column: key_row[column] for column in self.key_columns},
            value=clean_value,
            timestamp_ms=timestamp_ms,
            op=value.get(OP_FIELD) if isinstance(value.get(OP_FIELD), str) else None,
        )

    def _check_columns(self, record: Mapping[str, Any], expected: Iterable[str], what: str) -> None:
        """Refuse a record whose columns are not the ones this was built for.

        Both directions matter. An extra column is a source column the policy
        has never seen, and ignoring it is the leak the design exists to
        prevent. A missing one would be silently de-identified as null, which
        writes a plausible wrong row.
        """
        present, wanted = set(record), set(expected)
        if present == wanted:
            return
        added, removed = sorted(present - wanted), sorted(wanted - present)
        detail = ", ".join(
            part
            for part in (
                f"not in the schema it was built from: {', '.join(added)}" if added else "",
                f"absent from the record: {', '.join(removed)}" if removed else "",
            )
            if part
        )
        raise UnexpectedColumnsError(
            f"{self.table}: the {what} does not match the raw schema this transformer "
            f"was derived from ({detail}). The source schema changed under a running "
            "transformer; re-derive against the new raw schema, which either registers "
            "a new clean schema version or halts this topic"
        )
