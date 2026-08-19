"""The de-identification policy: a YAML file, parsed once, typed thereafter.

The policy file is the auditable artifact. When someone asks "what did we do to
``ssn``", the answer has to be a line in a file under version control, not a
branch buried in a transformer. That only holds if the file is the *only* place
a rule can live, which is why this module exposes no way to construct a rule
from anything but a parsed policy, and why nothing downstream of
:func:`load_policy` ever sees a raw dict again.

Everything is checked at the edge. A policy that references an op that does not
exist, an ``hmac`` with no domain, or a ``date_shift`` with no anchor is a
mistake made once, at startup, in a file a human is reading -- not a
``KeyError`` raised on record forty thousand of a replay, halfway through a
topic, with a partially-written clean stream behind it. Every failure here
raises a :class:`PolicyError` naming the table, the column and the problem.

Three rules are load-bearing rather than merely tidy:

*Nothing may address the ``source`` block.* Point-in-time replay works because
each cleaned record's Kafka timestamp is set to ``source.ts_ms``, which makes
``offsets_for_times(T)`` an exact answer to "the database as of T". A policy
that could rewrite, shift or drop ``source.ts_ms`` could silently destroy the
timeline while every record still looked de-identified. So the envelope is not
addressable at all: see :data:`RESERVED_FIELDS`.

*An uncovered column is not a passthrough.* ``on_uncovered_column`` exists so
that a column nobody wrote a rule for is a loud failure of one topic rather
than a quiet leak of every row in it. There is deliberately no ``passthrough``
setting for it -- the two options are halt and drop.

*A duplicate key is an error.* YAML lets the second ``ssn:`` win silently, which
is precisely the audit failure this file exists to prevent, so the loader
rejects it.

    python -m deid.policy                  # parse the default policy and print it
    python -m deid.policy path/to/x.yml    # ... or another one
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import re
import sys
from dataclasses import dataclass, fields
from enum import Enum
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType, UnionType
from typing import Any, ClassVar, Iterator, Mapping, Sequence, Union, get_args, get_origin, get_type_hints

import yaml

# The Debezium envelope. None of these are source columns, and ``source`` in
# particular carries ``ts_ms``, which is the point-in-time key -- see the module
# docstring. Naming any of them, at the top level or as a column, is an error
# rather than an unknown key, so the message can say why.
RESERVED_FIELDS = frozenset({"source", "op", "ts_ms", "transaction"})

# Postgres identifiers as the source actually spells them. Deliberately no
# quoting and no dots: a dotted column name would be an attempt to reach into
# the envelope or a jsonb document, and neither is supported yet.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

DEFAULT_POLICY_PATH = "/app/policy/clinic.yml"


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class PolicyError(Exception):
    """A policy that cannot be trusted to mean one thing.

    Every instance carries where it happened, so the message reads
    ``clinic.yml: public.patients.ssn: <problem>`` and a reader can go straight
    to the line.
    """

    def __init__(
        self,
        problem: str,
        *,
        source: str | None = None,
        table: str | None = None,
        column: str | None = None,
    ) -> None:
        self.problem = problem
        self.source = source
        self.table = table
        self.column = column
        super().__init__(self._render())

    def _render(self) -> str:
        where = ".".join(part for part in (self.table, self.column) if part)
        prefix = ": ".join(part for part in (self.source, where) if part)
        return f"{prefix}: {self.problem}" if prefix else self.problem


class MalformedPolicyError(PolicyError):
    """The file is not a policy: bad YAML, wrong shape, missing sections."""


class DuplicateKeyError(PolicyError):
    """The same key twice, where the second would silently win."""


class UnknownOpError(PolicyError):
    """A rule names an op that does not exist."""


class MissingArgumentError(PolicyError):
    """An op is missing an argument it cannot work without."""


class UnknownArgumentError(PolicyError):
    """A rule passes an argument its op does not take."""


class InvalidArgumentError(PolicyError):
    """An argument is present but is the wrong type or an impossible value."""


class ReservedFieldError(PolicyError):
    """A rule tries to address the Debezium envelope."""


# ---------------------------------------------------------------------------
# ops
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Op:
    """One transformation, with its arguments already validated.

    Subclasses declare their arguments as dataclass fields; a field with no
    default is a required argument. That is the whole argument schema -- the
    parser reflects on it rather than being told twice, so a new op cannot be
    added with a schema that disagrees with its constructor.
    """

    name: ClassVar[str]

    def validate(self, *, source: str | None, table: str, column: str) -> None:
        """Check what types alone cannot. Called once, at parse time."""

    def validate_in_table(
        self,
        rules: Mapping[str, "Rule"],
        *,
        source: str | None,
        table: str,
        column: str,
    ) -> None:
        """Check this rule against its siblings, once the table is fully parsed."""


@dataclass(frozen=True)
class Passthrough(Op):
    """Copy the value through unchanged.

    Saying so explicitly is the point: an uncovered column halts the topic, so
    every column that survives de-identification does it because someone wrote
    this down and someone else reviewed it.
    """

    name = "passthrough"


@dataclass(frozen=True)
class Drop(Op):
    """Remove the column from the clean schema entirely.

    Not null it -- remove it. A nulled column still tells you the source had
    one, and still has to be reasoned about by everything downstream.
    """

    name = "drop"


@dataclass(frozen=True)
class Null(Op):
    """Keep the column, always emit null.

    The narrow case between ``drop`` and everything else: something downstream
    -- a view, an ORM, a report -- needs the column to exist, and nothing may
    see what was in it. Weaker than ``drop``, because a nulled column still
    tells you the source had one, so ``drop`` is the default answer and this is
    the exception that has to be argued for in review.

    Spell it ``op: "null"`` in YAML. Unquoted, ``op: null`` is YAML's null and
    the rule has no op at all; the parser says so rather than letting it read
    as this.
    """

    name = "null"


@dataclass(frozen=True)
class Redact(Op):
    """Replace the value with one fixed constant.

    Every row lands on the same string, so unlike ``hmac`` this destroys
    equality: nothing joins, nothing groups, nothing counts distinct. That is
    the point of choosing it -- it is what to use when the column has to stay
    readable as a column ("[redacted]" in a UI) and must carry no information
    at all.
    """

    name = "redact"
    value: str = "[redacted]"

    def validate(self, *, source: str | None, table: str, column: str) -> None:
        if not self.value:
            raise InvalidArgumentError(
                "argument 'value' must not be empty (use op: drop, or op: \"null\")",
                source=source,
                table=table,
                column=column,
            )


@dataclass(frozen=True)
class Hmac(Op):
    """Replace the value with a keyed hash, stable within a domain.

    The domain is what makes joins survive de-identification: two columns
    hashed under ``patient`` land on the same token for the same patient, so
    ``appointments.patient_id`` still joins to ``patients.patient_id``, while a
    column under a different domain cannot be joined against them even though
    the underlying values may be equal. Getting the domain wrong is therefore
    either a broken join or a re-identification channel, and there is no
    default that is right more often than it is wrong.
    """

    name = "hmac"
    domain: str

    def validate(self, *, source: str | None, table: str, column: str) -> None:
        if not self.domain.strip():
            raise InvalidArgumentError(
                "argument 'domain' must not be empty",
                source=source,
                table=table,
                column=column,
            )


# What ``fake`` can be asked for. A closed set, because a typo'd kind that
# resolved at record time would be a crash mid-topic, and a kind silently
# falling back to something generic would put a plausible-looking wrong value
# into a column a reviewer signed off on.
FAKE_KINDS = frozenset(
    {
        "first_name",
        "middle_name",
        "last_name",
        "full_name",
        "email",
        "phone",
        "street_address",
        "city",
        "postal_code",
        "company",
    }
)


@dataclass(frozen=True)
class Fake(Op):
    """Replace the value with a plausible synthetic one of the same kind.

    Used where the shape of the data matters to whoever reads the replica and
    the value does not: a name column full of nulls breaks every UI and every
    join-on-non-empty downstream, and it is not any safer than a fake name.
    """

    name = "fake"
    kind: str

    def validate(self, *, source: str | None, table: str, column: str) -> None:
        if self.kind not in FAKE_KINDS:
            raise InvalidArgumentError(
                f"unknown fake kind {self.kind!r} "
                f"(known kinds: {', '.join(sorted(FAKE_KINDS))})",
                source=source,
                table=table,
                column=column,
            )


# Generalization targets, mapped to the extra arguments each one accepts.
# ``cap_age`` is only meaningful where the output is an age or a birth year;
# accepting it on ``zip3`` would let a policy claim a Safe Harbor age cap it
# does not actually apply.
GENERALIZE_TARGETS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "birth_year": frozenset({"cap_age"}),
        "age_band": frozenset({"cap_age"}),
        "year": frozenset(),
        "month": frozenset(),
        "zip3": frozenset(),
        "icd10_category": frozenset(),
    }
)


@dataclass(frozen=True)
class Generalize(Op):
    """Coarsen the value instead of destroying it.

    This is the op that decides whether the replica is worth having. Masking
    ``date_of_birth`` to NULL is trivially safe and takes every age cohort with
    it; generalizing it to a birth year, capped at the HIPAA Safe Harbor age of
    89, keeps the cohorts and collapses the handful of people whose age is
    itself an identifier.
    """

    name = "generalize"
    to: str
    cap_age: int | None = None

    def validate(self, *, source: str | None, table: str, column: str) -> None:
        allowed = GENERALIZE_TARGETS.get(self.to)
        if allowed is None:
            raise InvalidArgumentError(
                f"unknown generalize target {self.to!r} "
                f"(known targets: {', '.join(sorted(GENERALIZE_TARGETS))})",
                source=source,
                table=table,
                column=column,
            )
        if self.cap_age is None:
            return
        if "cap_age" not in allowed:
            accepts = sorted(t for t, args in GENERALIZE_TARGETS.items() if "cap_age" in args)
            raise UnknownArgumentError(
                f"generalize to {self.to!r} does not take 'cap_age' "
                f"(only {', '.join(accepts)} do)",
                source=source,
                table=table,
                column=column,
            )
        if not 1 <= self.cap_age <= 120:
            raise InvalidArgumentError(
                f"argument 'cap_age' must be between 1 and 120, got {self.cap_age}",
                source=source,
                table=table,
                column=column,
            )


@dataclass(frozen=True)
class Anchored(Op):
    """An op whose parameter is constant per entity rather than per record.

    Two ops need this and they need exactly the same guarantees, so the checks
    live once. The anchor names a sibling column whose value identifies the
    entity the offset belongs to: same entity, same offset, every record, every
    table, every restart.

    The anchor is read from the *raw* record, before any op has touched it, so
    it may name a column the policy drops or hashes -- what matters is that the
    value is the entity's identity, not that it survives to the clean side.
    That also means the transformer does not have to apply rules in dependency
    order, which is one class of bug that cannot then happen.
    """

    anchor: str

    def validate(self, *, source: str | None, table: str, column: str) -> None:
        if not self.anchor.strip():
            raise InvalidArgumentError(
                "argument 'anchor' must not be empty",
                source=source,
                table=table,
                column=column,
            )

    def validate_in_table(
        self,
        rules: Mapping[str, "Rule"],
        *,
        source: str | None,
        table: str,
        column: str,
    ) -> None:
        if self.anchor == column:
            raise InvalidArgumentError(
                f"a {self.name} cannot be anchored on itself",
                source=source,
                table=table,
                column=column,
            )
        anchored_on = rules.get(self.anchor)
        if anchored_on is None:
            raise InvalidArgumentError(
                f"anchor {self.anchor!r} has no rule in {table} "
                "(the anchor must be a column this policy covers)",
                source=source,
                table=table,
                column=column,
            )
        if isinstance(anchored_on.op, Anchored):
            raise InvalidArgumentError(
                f"anchor {self.anchor!r} is itself {anchored_on.op.name}'d, so it is a "
                "measurement rather than an identity: the offset would vary per record "
                "and every interval and ratio the anchor exists to preserve would be "
                "destroyed",
                source=source,
                table=table,
                column=column,
            )


@dataclass(frozen=True)
class DateShift(Anchored):
    """Move the timestamp by an offset that is constant per anchor entity.

    The anchor is the whole design. Shifting every timestamp by the same amount
    is a caesar cipher on the calendar; shifting each one independently
    destroys every interval -- length of stay, time to adjudication, the gap
    between two visits -- which is usually the reason anyone wanted the data.
    Anchoring the offset on the patient keeps every interval *within* a patient
    exact while decoupling patients from each other and from the real calendar,
    so there is no default anchor that is safe to guess.

    Known limitation, recorded here because the policy file is where someone
    will look for it: the shift is not hidden from an attacker who can also see
    the Kafka record timestamp, which is the unshifted ``source.ts_ms``. That
    is a deliberate trade -- the unshifted commit time is what makes
    point-in-time replay exact -- and it means date shift protects against
    re-identification from the replica's contents, not against someone who
    already has read access to the clean topics.
    """

    name = "date_shift"


# The widest jitter that is still a jitter. Past this the column is not a
# perturbed amount any more, it is a different amount wearing the same name,
# and a policy that wanted that should say `redact` or `null` and be read as
# saying it.
MAX_JITTER_PCT = 25


@dataclass(frozen=True)
class NumericJitter(Anchored):
    """Perturb a number by a percentage that is constant per anchor entity.

    For amounts, and the same argument as :class:`DateShift` one dimension
    over. A per-record factor destroys every relationship inside a row --
    billed >= allowed >= paid, the parts summing to the whole -- which is most
    of what makes claims data worth replicating. A per-entity factor keeps all
    of them exactly and moves the entity off its real numbers.

    The trade that buys, recorded here because the policy file is where someone
    will look for it: within one anchor entity the jitter is a single scalar
    multiple, so anyone who knows one true amount for that entity can recover
    the rest. It defends against a reader of the replica, not against someone
    who already has a bill.

    Note what it does *not* do to the join graph: amounts are payload, not
    keys, so nothing here has to agree with anything in another table.
    """

    name = "numeric_jitter"
    pct: int

    def validate(self, *, source: str | None, table: str, column: str) -> None:
        super().validate(source=source, table=table, column=column)
        if not 1 <= self.pct <= MAX_JITTER_PCT:
            raise InvalidArgumentError(
                f"argument 'pct' must be between 1 and {MAX_JITTER_PCT}, got {self.pct} "
                "(a wider spread is not a jitter; say redact or \"null\" and mean it)",
                source=source,
                table=table,
                column=column,
            )


OPS: Mapping[str, type[Op]] = MappingProxyType(
    {
        cls.name: cls
        for cls in (
            Passthrough,
            Drop,
            Null,
            Redact,
            Hmac,
            Fake,
            Generalize,
            DateShift,
            NumericJitter,
        )
    }
)


# ---------------------------------------------------------------------------
# the policy
# ---------------------------------------------------------------------------


class UncoveredColumn(str, Enum):
    """What to do with a source column the policy says nothing about.

    There is no ``passthrough`` member on purpose. The registry derives the
    clean schema from ``(raw schema, policy)``; a column with no rule is a
    column nobody has thought about, and the only two defensible answers are
    "stop this topic" and "do not emit it".
    """

    HALT_TOPIC = "halt_topic"
    DROP_COLUMN = "drop_column"


@dataclass(frozen=True)
class Rule:
    """One column, one op. The unit the transformer actually executes."""

    table: str
    column: str
    op: Op

    def __str__(self) -> str:
        args = ", ".join(
            f"{f.name}={getattr(self.op, f.name)!r}"
            for f in fields(self.op)
            if getattr(self.op, f.name) is not None
        )
        return f"{self.table}.{self.column}: {self.op.name}" + (f"({args})" if args else "")


@dataclass(frozen=True)
class TablePolicy:
    """Every rule for one schema-qualified table."""

    name: str
    rules: Mapping[str, Rule]

    def rule_for(self, column: str) -> Rule | None:
        return self.rules.get(column)

    def covers(self, column: str) -> bool:
        return column in self.rules


@dataclass(frozen=True)
class Policy:
    """A parsed, validated policy. The only shape the rest of the deid sees."""

    on_uncovered_column: UncoveredColumn
    tables: Mapping[str, TablePolicy]
    source: str = "<memory>"

    def table(self, table: str) -> TablePolicy | None:
        return self.tables.get(table)

    def rule_for(self, table: str, column: str) -> Rule | None:
        table_policy = self.tables.get(table)
        return table_policy.rule_for(column) if table_policy else None

    def covers(self, table: str, column: str) -> bool:
        return self.rule_for(table, column) is not None

    def all_rules(self) -> Iterator[Rule]:
        for table_policy in self.tables.values():
            yield from table_policy.rules.values()

    @property
    def hmac_domains(self) -> frozenset[str]:
        """Every domain the policy hashes under; the key material M4 must hold."""
        return frozenset(
            rule.op.domain for rule in self.all_rules() if isinstance(rule.op, Hmac)
        )


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses duplicate mapping keys."""


def _no_duplicate_keys(loader: _StrictLoader, node: yaml.MappingNode) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            mark = key_node.start_mark
            raise DuplicateKeyError(
                f"duplicate key {key!r} at line {mark.line + 1} "
                "(the later rule would silently win)"
            )
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys
)


@dataclass(frozen=True)
class _ArgSpec:
    name: str
    annotation: Any
    required: bool


@lru_cache(maxsize=None)
def _arg_specs(op_cls: type[Op]) -> tuple[_ArgSpec, ...]:
    hints = get_type_hints(op_cls)
    return tuple(
        _ArgSpec(
            name=f.name,
            annotation=hints[f.name],
            required=f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING,
        )
        for f in fields(op_cls)
    )


def _allowed_types(annotation: Any) -> tuple[type, ...]:
    # `int | None` resolves to types.UnionType, `Optional[int]` to typing.Union;
    # both are legal in an op's field annotations, so both have to be unwrapped.
    if get_origin(annotation) in (Union, UnionType):
        return tuple(get_args(annotation))
    return (annotation,)


def _describe(types: Sequence[type]) -> str:
    words = {str: "a string", int: "an integer", bool: "a boolean", type(None): "null"}
    return " or ".join(words.get(t, getattr(t, "__name__", str(t))) for t in types)


def _check_type(
    value: Any,
    annotation: Any,
    *,
    arg: str,
    source: str | None,
    table: str,
    column: str,
) -> Any:
    types = _allowed_types(annotation)
    for expected in types:
        if expected is type(None):
            if value is None:
                return None
        elif expected is bool:
            if isinstance(value, bool):
                return value
        elif expected is int:
            # bool is a subclass of int; `cap_age: true` is not an age.
            if isinstance(value, int) and not isinstance(value, bool):
                return value
        elif isinstance(value, expected):
            return value
    got = "null" if value is None else type(value).__name__
    raise InvalidArgumentError(
        f"argument {arg!r} must be {_describe(types)}, got {got} ({value!r})",
        source=source,
        table=table,
        column=column,
    )


def _build_op(spec: Mapping[str, Any], *, source: str | None, table: str, column: str) -> Op:
    op_name = spec.get("op")
    if op_name is None:
        # `op: null` is YAML's null, not the op named "null". Unhelpfully, the
        # two are indistinguishable by the time the document is parsed, so the
        # message covers both readings rather than guessing.
        hint = ' (for the null op, quote it: op: "null")' if "op" in spec else ""
        raise MissingArgumentError(
            f"rule has no 'op'{hint}", source=source, table=table, column=column
        )
    if not isinstance(op_name, str):
        raise InvalidArgumentError(
            f"'op' must be a string, got {type(op_name).__name__} ({op_name!r})",
            source=source,
            table=table,
            column=column,
        )
    op_cls = OPS.get(op_name)
    if op_cls is None:
        raise UnknownOpError(
            f"unknown op {op_name!r} (known ops: {', '.join(sorted(OPS))})",
            source=source,
            table=table,
            column=column,
        )

    specs = _arg_specs(op_cls)
    known = {arg.name for arg in specs}
    for key in spec:
        if key != "op" and key not in known:
            takes = ", ".join(sorted(known)) if known else "no arguments"
            raise UnknownArgumentError(
                f"op {op_name!r} does not take argument {key!r} (it takes {takes})",
                source=source,
                table=table,
                column=column,
            )

    kwargs: dict[str, Any] = {}
    for arg in specs:
        if arg.name not in spec:
            if arg.required:
                raise MissingArgumentError(
                    f"op {op_name!r} requires argument {arg.name!r}",
                    source=source,
                    table=table,
                    column=column,
                )
            continue
        kwargs[arg.name] = _check_type(
            spec[arg.name],
            arg.annotation,
            arg=arg.name,
            source=source,
            table=table,
            column=column,
        )

    op = op_cls(**kwargs)
    op.validate(source=source, table=table, column=column)
    return op


def _check_column_name(column: Any, *, source: str | None, table: str) -> str:
    if not isinstance(column, str):
        raise MalformedPolicyError(
            f"column names must be strings, got {type(column).__name__} ({column!r})",
            source=source,
            table=table,
        )
    head = column.split(".", 1)[0]
    if head in RESERVED_FIELDS:
        raise ReservedFieldError(
            f"{head!r} is part of the Debezium envelope, not a source column, and the "
            "policy may not address it: point-in-time replay resolves T against "
            "source.ts_ms, so a rule that could rewrite or drop it would destroy the "
            "timeline while every record still looked de-identified",
            source=source,
            table=table,
            column=column,
        )
    if not _IDENTIFIER.match(column):
        raise MalformedPolicyError(
            f"{column!r} is not a plain column name",
            source=source,
            table=table,
            column=column,
        )
    return column


def _check_table_name(table: Any, *, source: str | None) -> str:
    if not isinstance(table, str):
        raise MalformedPolicyError(
            f"table names must be strings, got {type(table).__name__} ({table!r})",
            source=source,
        )
    parts = table.split(".")
    if len(parts) != 2 or not all(_IDENTIFIER.match(part) for part in parts):
        raise MalformedPolicyError(
            f"table {table!r} must be schema-qualified, as in 'public.patients' "
            "(the topic name is derived from it, so an unqualified name is ambiguous)",
            source=source,
        )
    return table


def _parse_table(
    table: str, spec: Any, *, source: str | None
) -> TablePolicy:
    if not isinstance(spec, Mapping):
        raise MalformedPolicyError(
            f"table {table!r} must map columns to rules, got {type(spec).__name__}",
            source=source,
        )
    if not spec:
        raise MalformedPolicyError(
            f"table {table!r} has no rules (remove it, or the topic halts on every column)",
            source=source,
        )

    rules: dict[str, Rule] = {}
    for raw_column, raw_rule in spec.items():
        column = _check_column_name(raw_column, source=source, table=table)
        if not isinstance(raw_rule, Mapping):
            raise MalformedPolicyError(
                f"rule must be a mapping like {{ op: drop }}, got "
                f"{type(raw_rule).__name__} ({raw_rule!r})",
                source=source,
                table=table,
                column=column,
            )
        op = _build_op(raw_rule, source=source, table=table, column=column)
        rules[column] = Rule(table=table, column=column, op=op)

    # Second pass: rules that refer to their siblings can only be checked once
    # every sibling exists.
    for column, rule in rules.items():
        rule.op.validate_in_table(rules, source=source, table=table, column=column)

    return TablePolicy(name=table, rules=MappingProxyType(rules))


def parse_policy(document: Any, *, source: str | None = None) -> Policy:
    """Validate an already-loaded YAML document into a :class:`Policy`."""
    if not isinstance(document, Mapping):
        raise MalformedPolicyError(
            f"policy must be a mapping, got {type(document).__name__}", source=source
        )

    for key in document:
        if isinstance(key, str) and key in RESERVED_FIELDS:
            raise ReservedFieldError(
                f"{key!r} is part of the Debezium envelope and cannot be given a rule; "
                "the policy addresses source columns under 'tables' only",
                source=source,
            )

    known_keys = {"on_uncovered_column", "tables"}
    unknown = sorted(str(key) for key in document if key not in known_keys)
    if unknown:
        raise MalformedPolicyError(
            f"unknown top-level key(s): {', '.join(unknown)} "
            f"(the policy takes {', '.join(sorted(known_keys))})",
            source=source,
        )

    raw_uncovered = document.get("on_uncovered_column", UncoveredColumn.HALT_TOPIC.value)
    try:
        on_uncovered_column = UncoveredColumn(raw_uncovered)
    except ValueError:
        choices = ", ".join(member.value for member in UncoveredColumn)
        raise InvalidArgumentError(
            f"on_uncovered_column must be one of {choices}, got {raw_uncovered!r}",
            source=source,
        ) from None

    raw_tables = document.get("tables")
    if raw_tables is None:
        raise MalformedPolicyError("policy has no 'tables' section", source=source)
    if not isinstance(raw_tables, Mapping):
        raise MalformedPolicyError(
            f"'tables' must be a mapping, got {type(raw_tables).__name__}", source=source
        )
    if not raw_tables:
        raise MalformedPolicyError("policy covers no tables", source=source)

    tables: dict[str, TablePolicy] = {}
    for raw_table, table_spec in raw_tables.items():
        table = _check_table_name(raw_table, source=source)
        tables[table] = _parse_table(table, table_spec, source=source)

    return Policy(
        on_uncovered_column=on_uncovered_column,
        tables=MappingProxyType(tables),
        source=source or "<memory>",
    )


def load_policy(path: str | os.PathLike[str]) -> Policy:
    """Read and validate a policy file. The only way to get a :class:`Policy`."""
    path = Path(path)
    label = path.name
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise MalformedPolicyError(
            f"cannot read policy file {path}: {exc.strerror or exc}", source=label
        ) from exc
    try:
        document = yaml.load(text, Loader=_StrictLoader)
    except DuplicateKeyError as exc:
        raise DuplicateKeyError(exc.problem, source=label) from None
    except yaml.YAMLError as exc:
        raise MalformedPolicyError(f"invalid YAML: {exc}", source=label) from exc
    if document is None:
        raise MalformedPolicyError("policy file is empty", source=label)
    return parse_policy(document, source=label)


def policy_path_from_env() -> str:
    """Where the policy lives at runtime.

    ``PIT_POLICY_PATH`` overrides; the default is where the deid chart mounts
    ``deid/policy/clinic.yml`` into the container.
    """
    return os.environ.get("PIT_POLICY_PATH", DEFAULT_POLICY_PATH)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    """Parse a policy and print what it says, so review is not a YAML diff."""
    parser = argparse.ArgumentParser(
        prog="python -m deid.policy",
        description="Validate a de-identification policy and print its rules.",
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=policy_path_from_env(),
        help=f"policy file (default: $PIT_POLICY_PATH or {DEFAULT_POLICY_PATH})",
    )
    args = parser.parse_args(argv)

    try:
        policy = load_policy(args.path)
    except PolicyError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1

    print(f"{policy.source}: on_uncovered_column={policy.on_uncovered_column.value}")
    for table_policy in policy.tables.values():
        print(f"  {table_policy.name} ({len(table_policy.rules)} columns)")
        for rule in table_policy.rules.values():
            print(f"    {rule}")
    domains = ", ".join(sorted(policy.hmac_domains)) or "none"
    print(f"  hmac domains: {domains}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
