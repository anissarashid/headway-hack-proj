"""What each policy op does to a value, and to the value's Avro type.

Every op changes both halves at once. ``date_of_birth`` stops being a Debezium
date and becomes an int holding a birth year; ``ssn`` stops existing. The two
changes are not independent -- if the schema says the clean column is an int
and the transform hands back a string, the registry rejects the record at
produce time and the topic stops with a half-written stream behind it. So the
halves are built together, by one function per op, and handed back as one
:class:`Op`:

    op = build(rule, raw_type, keys=keys)
    clean_type = op.derive_type(raw_type)   # asked once, at startup
    clean_value = op.apply(raw_value)       # asked of every record after that

``derive_type`` is the whole enforcement mechanism. The clean Avro schema is
derived from ``(raw schema, policy)`` before the first record moves, so a rule
that cannot work against the column it names -- ``generalize to: zip3`` on a
bigint, ``date_shift`` on a bare long whose unit nobody wrote down -- raises
:class:`IncompatibleColumnError` at startup and halts that one topic. There is
no path where a mismatch is discovered by trying it.

The raw type is an input to *both* halves, so it is an input to :func:`build`.
It has to be: Debezium spells the unit of a timestamp in ``connect.name`` and
nowhere else, so ``io.debezium.time.Timestamp`` and
``io.debezium.time.MicroTimestamp`` are both ``long`` and a value alone cannot
say which it is. An op that guessed would be wrong by a factor of a thousand
and still produce conforming records.

Four rules hold across every op, and the property test in ``tests/test_ops.py``
is what keeps them true:

*Null in, null out.* No op invents a value where the source had none, and only
``null`` removes one. A column that was nullable stays nullable; a column that
was not, is not -- except for the one case below.

*An op that can fail to read its input widens its type to nullable.* A zip that
is not a zip, an ISO timestamp that will not parse, an amount that is not a
number: the op cannot truncate, shift or perturb what it cannot read, and
passing the value through would leak exactly the value the rule exists to
remove. So it emits null, and says so in the type it derives. The widening is
visible in the registry, which is the point.

*Deterministic, always.* No clock, no ``random``, no environment. Every keyed
value descends from the injected salt, so replaying a raw topic a year later
produces byte-identical clean records -- which is what makes a point-in-time
manifest mean anything. The one date the module needs (the reference date for
HIPAA's age cap) is injected for the same reason: read from the clock, it would
make the same input produce different output on a different day.

*The envelope is untouchable.* Nothing here reads or writes ``source.ts_ms``;
see the module docstring in :mod:`deid.policy`.

The salt arrives by dependency injection -- :class:`Keys`, constructed by the
transformer's entrypoint from a mounted Secret. This module never reads the
environment, because a module that can find its own key material is a module
that can be tested with the wrong one and pass.

    python -m deid.ops          # every op, its derived type, and a worked example
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import lru_cache
from typing import Any, Callable, ClassVar, Mapping, NoReturn, Sequence

from . import avro, policy, vocab
from .avro import AvroType

# The widest a date_shift will move a record: +/- two years, in whole days.
#
# Whole days rather than whole seconds so that a time of day survives -- an
# appointment at 09:15 stays at 09:15, and "clinic hours" remains a true
# statement about the replica. Whole days rather than whole weeks because the
# extra factor of seven in the offset space is worth more than preserving
# day-of-week; anyone who needs weekday effects should change this constant to
# step in sevens and accept the smaller space, deliberately.
MAX_SHIFT_DAYS = 730

# The three-digit ZIP prefixes HIPAA Safe Harbor requires be reported as 000,
# because the 2000 census put fewer than 20,000 people in each of them. A zip3
# is only de-identifying where the population behind it is large.
RESTRICTED_ZIP3 = frozenset(
    {
        "036", "059", "063", "102", "203", "556", "692", "790", "821",
        "823", "830", "831", "878", "879", "884", "890", "893",
    }
)

# Debezium's temporal logical types, mapped to how many of the column's units
# make a day. Anything not in here is not a timestamp this module will touch.
UNITS_PER_DAY: Mapping[str, int] = {
    avro.DATE: 1,
    "date": 1,
    avro.TIMESTAMP: 86_400_000,
    "timestamp-millis": 86_400_000,
    avro.MICRO_TIMESTAMP: 86_400_000_000,
    "timestamp-micros": 86_400_000_000,
    avro.NANO_TIMESTAMP: 86_400_000_000_000,
}

# ZonedTimestamp is temporal too, but it is a string and is handled apart.
TEMPORAL_NAMES = frozenset(UNITS_PER_DAY) | {avro.ZONED_TIMESTAMP}

DECIMAL_NAMES = frozenset({avro.DECIMAL, avro.VARIABLE_SCALE_DECIMAL})

EPOCH = date(1970, 1, 1)

# Demo key material for `python -m deid.ops`. Named so that it cannot be
# mistaken for the real thing in a log, a screenshot or a code search.
DEMO_SALT = b"demo-salt-not-for-real-data-0000"
DEMO_REFERENCE_DATE = date(2026, 8, 1)


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class IncompatibleColumnError(policy.PolicyError):
    """The policy asks for an op the column's raw type cannot support.

    A :class:`~deid.policy.PolicyError` because it is the same class of
    mistake, caught at the same startup boundary, by the same ``except``: a
    rule that is wrong about the world. It just cannot be seen until the raw
    schema is in hand.
    """


class AnchorRequired(RuntimeError):
    """An anchored op was built without an anchor value and then applied.

    A caller bug, not a policy one: ``date_shift`` and ``numeric_jitter`` are
    built per record because their offset depends on the record's anchor
    column. Deriving the clean type without one is fine and expected -- that
    happens once, at startup, with no record in hand -- so the failure lands
    here rather than at build time.
    """


# ---------------------------------------------------------------------------
# injected inputs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Keys:
    """The two things this module refuses to source for itself.

    ``salt`` is key material: HMAC under it is what makes a surrogate stable
    and unguessable. It comes from a mounted Secret, read by the transformer's
    entrypoint and passed in. Nothing here goes looking for it, so there is no
    configuration under which this module quietly de-identifies with a
    default.

    ``reference_date`` is the date HIPAA's age cap is measured against. It is
    injected for the same reason and a different one: an op that read the clock
    would return a different answer for the same record on a different day, and
    a clean topic that cannot be regenerated from its raw topic is not a
    replica of anything.
    """

    salt: bytes
    reference_date: date

    MIN_SALT_BYTES: ClassVar[int] = 16

    def __post_init__(self) -> None:
        if not isinstance(self.salt, (bytes, bytearray)):
            raise TypeError(f"salt must be bytes, got {type(self.salt).__name__}")
        if len(self.salt) < self.MIN_SALT_BYTES:
            raise ValueError(
                f"salt must be at least {self.MIN_SALT_BYTES} bytes, got {len(self.salt)} "
                "(a short salt is a guessable one, and every surrogate in every topic "
                "descends from it)"
            )
        object.__setattr__(self, "salt", bytes(self.salt))
        # datetime is a date subclass and only the year is ever read, but
        # normalising here keeps two Keys with the same day equal.
        if isinstance(self.reference_date, datetime):
            object.__setattr__(self, "reference_date", self.reference_date.date())
        elif not isinstance(self.reference_date, date):
            raise TypeError(
                f"reference_date must be a date, got {type(self.reference_date).__name__}"
            )


# ---------------------------------------------------------------------------
# the op
# ---------------------------------------------------------------------------


class _Dropped:
    """The clean side of a column that does not have one.

    A distinct singleton rather than ``None``, because ``None`` is a value a
    column can legitimately hold and "this field is absent from the record"
    is not the same statement as "this field is null". Returned by both halves
    of ``drop``, so a caller that forgets to check gets something obviously
    wrong rather than a plausible null.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "DROPPED"

    def __bool__(self) -> bool:
        return False


DROPPED = _Dropped()

Apply = Callable[[Any], Any]
DeriveType = Callable[[AvroType], AvroType]

# What one op builder hands back: the clean type, and the transform that
# produces values fitting it. Never one without the other.
Built = tuple[AvroType, Apply]


@dataclass(frozen=True)
class Op:
    """One transformation: what it does to the type, and what it does to the value.

    Both halves come out of the same builder call, so they cannot be edited
    apart. ``derive_type`` re-runs that builder, so it stays a pure function of
    the raw type; ``apply`` is specialised to the type (and, for anchored ops,
    the anchor) this instance was built for, and applying it to a value of some
    other type is a caller error.
    """

    derive_type: DeriveType  # raw field type -> clean field type
    apply: Apply  # raw value      -> clean value


# ---------------------------------------------------------------------------
# keyed derivation
# ---------------------------------------------------------------------------


def _canonical(value: object) -> bytes:
    """The bytes a keyed derivation actually hashes.

    Identifiers arrive dirty. The load generator plants the case deliberately:
    an mrn padded with spaces in one row and not the next is one person, and a
    tokenizer keying on the exact string hands back two surrogates and breaks
    the join the whole domain exists to preserve. So strings are normalised --
    NFKC, trimmed, internal whitespace collapsed, casefolded -- before hashing.

    The limit, stated because it is a real one: punctuation is left alone, so
    ``A-100`` and ``A100`` are still two people. Canonicalising punctuation
    would merge identifiers that genuinely differ by it, which is the worse
    failure of the two -- it invents joins rather than missing them.
    """
    if value is None:
        # Only reachable for a null anchor: every op that hashes a column value
        # returns null for a null before it gets here. See _anchored_draw.
        return b"\x00none"
    if isinstance(value, bool):
        return b"true" if value else b"false"
    if isinstance(value, int):
        return str(value).encode("utf-8")
    if isinstance(value, str):
        text = unicodedata.normalize("NFKC", value).strip().casefold()
        return " ".join(text.split()).encode("utf-8")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    # Floats and everything else an anchor column could hold. repr round-trips
    # a float exactly, which is what stability needs; it is not a shape anyone
    # should be anchoring on, and no op hashes a column value of this type.
    return repr(value).encode("utf-8")


@lru_cache(maxsize=16384)
def _keyed(salt: bytes, purpose: str, canonical: bytes) -> bytes:
    """HMAC-SHA256 over a purpose-separated message.

    Cached because the same patient id appears in thousands of records and this
    is a pure function of its arguments: the cache can change how fast an
    answer arrives, never what it is. The separator is 0x1f, which no purpose
    string contains, so ``("patient", "x")`` and ``("patient" + "x", "")``
    cannot collide.
    """
    return hmac.new(salt, purpose.encode("utf-8") + b"\x1f" + canonical, hashlib.sha256).digest()


def _digest(keys: Keys, purpose: str, value: object) -> bytes:
    return _keyed(keys.salt, purpose, _canonical(value))


def _token(digest: bytes) -> str:
    """120 bits of the digest, base32, no padding: 24 stable ASCII characters."""
    return base64.b32encode(digest[:15]).decode("ascii")


def _draw(digest: bytes, slot: int, n: int) -> int:
    """A number in ``[0, n)`` from one 4-byte window of the digest.

    Eight independent slots per digest, which is more than any op needs. The
    modulo bias is on the order of ``n / 2**32`` and irrelevant at these sizes.
    """
    window = digest[slot * 4 : slot * 4 + 4]
    return int.from_bytes(window, "big") % n


def _signed_draw(digest: bytes, slot: int, span: int) -> int:
    """A number in ``[-span, span]``, zero included."""
    return _draw(digest, slot, 2 * span + 1) - span


def _pick(digest: bytes, slot: int, choices: Sequence[str]) -> str:
    return choices[_draw(digest, slot, len(choices))]


# ---------------------------------------------------------------------------
# temporal conversion
# ---------------------------------------------------------------------------


def _to_date(value: int, units_per_day: int) -> date | None:
    """The calendar date of an epoch-relative integer, or None if it is absurd.

    Floor division, so a negative timestamp lands on the day it belongs to
    rather than the one after. ``date`` covers years 1 through 9999; a long can
    hold instants far outside that, and there is no year 300,000,000 to
    generalize to.
    """
    try:
        return EPOCH + timedelta(days=value // units_per_day)
    except (OverflowError, ValueError):
        return None


def _parse_zoned(text: str) -> datetime | None:
    if not isinstance(text, str):
        return None
    normalised = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return datetime.fromisoformat(normalised)
    except ValueError:
        return None


def _render_zoned(moment: datetime, like: str) -> str:
    """ISO-8601 again, keeping the ``Z`` spelling if that is what came in."""
    rendered = moment.isoformat()
    if like.endswith("Z") and rendered.endswith("+00:00"):
        return rendered[:-6] + "Z"
    return rendered


def _clamp(value: int, kind: str) -> int:
    """Keep an integer inside its Avro range.

    A shift that pushes a value out of int64 would produce a record the
    registry rejects, which stops the topic. Clamping produces an absurd date
    at an absurd input, which does not.
    """
    if kind == "int":
        return max(avro.INT32_MIN, min(avro.INT32_MAX, value))
    return max(avro.INT64_MIN, min(avro.INT64_MAX, value))


# ---------------------------------------------------------------------------
# type inspection, and the refusal
# ---------------------------------------------------------------------------


def _refuse(rule: policy.Rule, raw: AvroType, why: str) -> NoReturn:
    raise IncompatibleColumnError(
        f"op {rule.op.name!r} cannot be applied to a {avro.describe(raw)} column: {why}",
        table=rule.table,
        column=rule.column,
    )


def _inspect(rule: policy.Rule, raw: AvroType) -> tuple[AvroType, str, str | None]:
    """The non-null branch of a column type, its kind, and its logical name."""
    inner = avro.non_null(raw)
    if avro.is_union(inner):
        _refuse(
            rule,
            raw,
            "a union of more than one non-null branch is ambiguous -- the op cannot "
            "know which branch a value arrived on",
        )
    kind = avro.base(inner)
    if kind is None:
        _refuse(rule, raw, "the column type is not one this op can read")
    return inner, kind, avro.logical(inner)


# ---------------------------------------------------------------------------
# the ops
# ---------------------------------------------------------------------------
#
# One function per op, returning both halves. Each is called with the rule (for
# its arguments and for error messages), the raw Avro type of the column, the
# injected keys, and -- for anchored ops -- the record's anchor value.


def _passthrough(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    return raw, lambda value: value


def _drop(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    return DROPPED, lambda value: DROPPED


def _null(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    # The one op that widens a non-nullable column on purpose rather than
    # because it might fail to read something.
    return avro.nullable(raw), lambda value: None


def _redact(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    _inspect(rule, raw)  # unions are still ambiguous, even for a constant
    constant = rule.op.value
    return avro.like(raw, "string"), lambda value: None if value is None else constant


def _hmac(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    inner, kind, name = _inspect(rule, raw)
    if kind not in ("string", "int", "long", "bytes"):
        _refuse(rule, raw, "only a string, an integer or bytes can be tokenized")
    if name in TEMPORAL_NAMES:
        _refuse(
            rule,
            raw,
            "a timestamp is not an identifier -- date_shift or generalize it instead, "
            "or the column stops being a time and every interval in the table with it",
        )
    if name in DECIMAL_NAMES:
        _refuse(rule, raw, "an amount is not an identifier")

    purpose = f"hmac:{rule.op.domain}"

    def apply(value: object) -> object:
        if value is None:
            return None
        return _token(_digest(keys, purpose, value))

    return avro.like(raw, "string"), apply


def _fake(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    inner, kind, name = _inspect(rule, raw)
    if kind != "string":
        _refuse(rule, raw, f"a {rule.op.kind} can only replace a string")
    if name is not None:
        _refuse(
            rule,
            raw,
            f"the column is a string that means something specific ({name}); a fake "
            f"{rule.op.kind} would still conform to the schema and be nonsense in it",
        )

    purpose = f"fake:{rule.op.kind}"
    maker = _FAKERS[rule.op.kind]

    def apply(value: object) -> object:
        if value is None:
            return None
        return maker(_digest(keys, purpose, value))

    return raw, apply


def _fake_first(digest: bytes) -> str:
    return _pick(digest, 0, vocab.FIRST_NAMES)


def _fake_last(digest: bytes) -> str:
    return _pick(digest, 0, vocab.LAST_NAMES)


def _fake_full_name(digest: bytes) -> str:
    return f"{_pick(digest, 0, vocab.FIRST_NAMES)} {_pick(digest, 1, vocab.LAST_NAMES)}"


def _fake_email(digest: bytes) -> str:
    first = _pick(digest, 0, vocab.FIRST_NAMES).casefold()
    last = _pick(digest, 1, vocab.LAST_NAMES).casefold()
    # Strip anything a mail address cannot carry: the vocab has accents and an
    # apostrophe in it, both on purpose.
    local = "".join(ch for ch in f"{first}.{last}" if ch.isascii() and (ch.isalnum() or ch == "."))
    return f"{local}{_draw(digest, 2, 100):02d}@{_pick(digest, 3, vocab.EMAIL_DOMAINS)}"


def _fake_phone(digest: bytes) -> str:
    # 555-0100 through 555-0199 is reserved for fiction, so no fake value here
    # can ring a real telephone.
    return f"({_pick(digest, 0, vocab.AREA_CODES)}) 555-01{_draw(digest, 1, 100):02d}"


def _fake_street(digest: bytes) -> str:
    number = 1 + _draw(digest, 0, 9999)
    return (
        f"{number} {_pick(digest, 1, vocab.STREET_NAMES)} "
        f"{_pick(digest, 2, vocab.STREET_TYPES)}"
    )


def _fake_postal_code(digest: bytes) -> str:
    return f"{_draw(digest, 0, 100_000):05d}"


def _fake_company(digest: bytes) -> str:
    return f"{_pick(digest, 0, vocab.COMPANY_HEADS)} {_pick(digest, 1, vocab.COMPANY_TAILS)}"


_FAKERS: Mapping[str, Callable[[bytes], str]] = {
    "first_name": _fake_first,
    "middle_name": _fake_first,
    "last_name": _fake_last,
    "full_name": _fake_full_name,
    "email": _fake_email,
    "phone": _fake_phone,
    "street_address": _fake_street,
    "city": lambda digest: _pick(digest, 0, vocab.CITIES),
    "postal_code": _fake_postal_code,
    "company": _fake_company,
}


def _generalize(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    target = rule.op.to
    inner, kind, name = _inspect(rule, raw)

    if target == "zip3":
        if kind != "string" or name is not None:
            _refuse(rule, raw, "zip3 truncates a postal code, which has to be a plain string")
        return avro.nullable("string"), _zip3

    if target == "icd10_category":
        return _icd10(rule, raw, inner, kind, name)

    if target == "age_band" and kind in ("int", "long") and name is None:
        cap = rule.op.cap_age
        return avro.nullable("string"), lambda value: _band(value, cap)

    units = UNITS_PER_DAY.get(name or "")
    if units is None and name != avro.ZONED_TIMESTAMP:
        expected = "a date or a timestamp"
        if target == "age_band":
            expected = "a date, a timestamp, or a plain integer age"
        _refuse(rule, raw, f"{target} needs {expected}, and this column carries neither")

    if units is not None:

        def as_date(value: Any) -> date | None:
            return _to_date(value, units)

    else:

        def as_date(value: Any) -> date | None:
            moment = _parse_zoned(value)
            return None if moment is None else moment.date()


    if target == "year":
        return avro.nullable("int"), lambda value: _year_of(as_date, value)
    if target == "month":
        return avro.nullable("string"), lambda value: _month_of(as_date, value)
    if target == "birth_year":
        floor = _cap_floor_year(keys, rule.op.cap_age)
        return avro.nullable("int"), lambda value: _birth_year(as_date, value, floor)
    # age_band, from a date rather than an integer age.
    cap = rule.op.cap_age
    reference_year = keys.reference_date.year
    return (
        avro.nullable("string"),
        lambda value: _band_of_date(as_date, value, reference_year, cap),
    )


def _zip3(value: object) -> object:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    # A US zip is five digits, or nine with the +4. Anything shorter is not one
    # -- a four-digit string could be a truncated zip or a foreign postcode,
    # and guessing which would either leak or invent a location.
    if len(digits) < 5:
        return None
    prefix = digits[:3]
    return "000" if prefix in RESTRICTED_ZIP3 else prefix


def _icd10(
    rule: policy.Rule, raw: AvroType, inner: AvroType, kind: str, name: str | None
) -> Built:
    if kind == "string" and name is None:
        return avro.nullable("string"), lambda value: _icd10_category(value)
    if kind == "array":
        items = inner.get("items", "null")
        if avro.base(avro.non_null(items)) != "string":
            _refuse(rule, raw, "icd10_category reads codes, so the array has to hold strings")

        def apply_many(value: object) -> object:
            if value is None:
                return None
            # Deduplicated, order preserved: E11.9 and E11.65 are two codes in
            # one category, and emitting ["E11", "E11"] would say something
            # about the original that the generalization is meant to remove.
            seen: dict[str, None] = {}
            for code in value:
                category = _icd10_category(code)
                if category is not None:
                    seen[category] = None
            return list(seen)

        return raw, apply_many
    _refuse(rule, raw, "icd10_category needs a code string or an array of them")


def _icd10_category(code: object) -> str | None:
    if not isinstance(code, str):
        return None
    text = unicodedata.normalize("NFKC", code).strip().upper()
    head = text[:3]
    # A category is a letter, a digit, then one more character: E11, C4A, M54.
    if len(head) < 3 or not head[0].isalpha() or not head[1].isdigit() or not head[2].isalnum():
        return None
    return head


def _cap_floor_year(keys: Keys, cap_age: int | None) -> int | None:
    """The year everyone over the age cap collapses into.

    HIPAA Safe Harbor: ages over 89 go into one bucket, because a 97-year-old
    in a clinic this size is identified by their age alone. Expressed as a
    birth year rather than an age, the bucket is "this year or earlier", and
    the year is the last one nobody in it can be younger than.

    Deliberately computed from the year alone, so the op never needs a birth
    month or day. The cost is that the oldest un-capped cohort -- people who
    are exactly the cap age and have not had this year's birthday -- lands in
    the bucket too. That errs towards generalizing, which is the safe
    direction; the property the tests assert is that nobody *over* the cap ever
    escapes it.
    """
    return None if cap_age is None else keys.reference_date.year - cap_age - 1


def _year_of(as_date: Callable[[Any], date | None], value: object) -> object:
    if value is None:
        return None
    moment = as_date(value)
    return None if moment is None else moment.year


def _month_of(as_date: Callable[[Any], date | None], value: object) -> object:
    if value is None:
        return None
    moment = as_date(value)
    return None if moment is None else f"{moment.year:04d}-{moment.month:02d}"


def _birth_year(
    as_date: Callable[[Any], date | None], value: object, floor: int | None
) -> object:
    if value is None:
        return None
    moment = as_date(value)
    if moment is None:
        return None
    return moment.year if floor is None else max(moment.year, floor)


def _band(age: object, cap: int | None) -> object:
    """A decade band from an integer age."""
    if age is None:
        return None
    if not isinstance(age, int) or isinstance(age, bool) or age < 0:
        # A negative age is corrupt data, not a cohort.
        return None
    if cap is not None and age > cap:
        return f"{cap + 1}+"
    low = (age // 10) * 10
    return f"{low}-{low + 9}"


def _band_of_date(
    as_date: Callable[[Any], date | None],
    value: object,
    reference_year: int,
    cap: int | None,
) -> object:
    if value is None:
        return None
    moment = as_date(value)
    if moment is None:
        return None
    # Year arithmetic, like the birth_year cap: it over-states an age by up to
    # a year for anyone whose birthday has not passed, which can only move
    # someone into an older band. Safe direction.
    return _band(reference_year - moment.year, cap)


def _date_shift(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    inner, kind, name = _inspect(rule, raw)
    offset = _anchored_draw(rule, keys, anchor, "date_shift", MAX_SHIFT_DAYS)

    units = UNITS_PER_DAY.get(name or "")
    if units is not None:
        if kind not in ("int", "long"):
            _refuse(rule, raw, f"{name} should be an integer, and this column is not")

        def apply(value: object) -> object:
            if value is None:
                return None
            return _clamp(value + offset() * units, kind)

        # Every integer in range is a valid instant, so this half cannot fail
        # to read its input and the type is preserved exactly.
        return raw, apply

    if name == avro.ZONED_TIMESTAMP:

        def apply_zoned(value: object) -> object:
            if value is None:
                return None
            moment = _parse_zoned(value)
            if moment is None:
                return None
            try:
                shifted = moment + timedelta(days=offset())
            except OverflowError:
                return None
            return _render_zoned(shifted, like=value)

        # A ZonedTimestamp is a string, and a string is not necessarily an
        # instant. This is the one temporal shape that can be unreadable, so
        # it is the one that widens.
        return avro.nullable(raw), apply_zoned

    _refuse(
        rule,
        raw,
        "there is no unit here to shift by. Debezium spells the unit in connect.name "
        f"({avro.MICRO_TIMESTAMP} and friends); a bare {kind} could be milliseconds, "
        "microseconds or a row version, and shifting the wrong one is off by a factor "
        "of a thousand in a value that still looks like a date",
    )


def _numeric_jitter(rule: policy.Rule, raw: AvroType, keys: Keys, anchor: object) -> Built:
    inner, kind, name = _inspect(rule, raw)
    pct = rule.op.pct
    # One draw in [-10000, 10000] scaled by pct, rather than a draw sized to
    # pct: two columns jittered by the same percentage under the same anchor
    # then get the same factor, which is what keeps billed >= allowed >= paid
    # true in the replica.
    spread = _anchored_draw(rule, keys, anchor, "numeric_jitter", 10_000)

    def factor() -> Decimal:
        return Decimal(1_000_000 + spread() * pct) / Decimal(1_000_000)

    if name in DECIMAL_NAMES:
        _refuse(
            rule,
            raw,
            "this is a base64 Decimal, which is not a number until something decodes "
            "it. Set the connector's decimal.handling.mode to `string` or `double` "
            "(see the note on claims in the clinic DDL) -- under `precise` the sink "
            "cannot compare these amounts either",
        )

    if kind in ("int", "long") and name is None:

        def apply_int(value: object) -> object:
            if value is None:
                return None
            moved = (Decimal(value) * factor()).to_integral_value(rounding=ROUND_HALF_UP)
            return _clamp(int(moved), kind)

        return raw, apply_int

    if kind in ("float", "double") and name is None:

        def apply_float(value: object) -> object:
            if value is None:
                return None
            return float(value) * float(factor())

        return raw, apply_float

    if kind == "string" and name is None:

        def apply_decimal_string(value: object) -> object:
            if value is None:
                return None
            return _jitter_decimal_string(value, factor())

        # An amount the connector emitted as a string is the recommended shape,
        # and a string column can hold something that is not a number.
        return avro.nullable(raw), apply_decimal_string

    _refuse(rule, raw, "only a number can be jittered")


def _jitter_decimal_string(value: object, factor: Decimal) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        amount = Decimal(value.strip())
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite():
        return None
    exponent = amount.as_tuple().exponent
    scale = -exponent if isinstance(exponent, int) and exponent < 0 else 0
    try:
        # Same scale in as out: numeric(12,2) has to come back with two decimal
        # places or the sink's own column rounds it a second time.
        moved = (amount * factor).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None
    return f"{moved:f}"


# ---------------------------------------------------------------------------
# building
# ---------------------------------------------------------------------------

_UNSET = object()


def _anchored_draw(
    rule: policy.Rule, keys: Keys, anchor: object, purpose: str, span: int
) -> Callable[[], int]:
    """The per-entity draw for an anchored op, deferred until it is needed.

    Deferred so that deriving the clean type works with no record in hand:
    schema derivation happens once at startup, and the anchor is a value from a
    record that does not exist yet. Only ``apply`` needs the number.
    """
    if anchor is _UNSET:

        def unbound() -> int:
            raise AnchorRequired(
                f"{rule.table}.{rule.column}: {rule.op.name} is anchored on "
                f"{rule.op.anchor!r}, so it has to be built per record with "
                f"anchor=<the record's {rule.op.anchor}>"
            )

        return unbound

    # A null anchor has no entity to be constant within. Every such record
    # shares one offset, which is the honest answer: passing the values through
    # unshifted would hand back the real calendar.
    value = _signed_draw(_digest(keys, purpose, anchor), 0, span)
    return lambda: value


_Builder = Callable[[policy.Rule, AvroType, Keys, object], Built]

_BUILDERS: Mapping[type, _Builder] = {
    policy.Passthrough: _passthrough,
    policy.Drop: _drop,
    policy.Null: _null,
    policy.Redact: _redact,
    policy.Hmac: _hmac,
    policy.Fake: _fake,
    policy.Generalize: _generalize,
    policy.DateShift: _date_shift,
    policy.NumericJitter: _numeric_jitter,
}

# An op the policy can express and this module cannot execute is a topic that
# halts on its first record with a KeyError. Checked at import, so it halts at
# startup with a sentence instead.
_unimplemented = sorted(
    [name for name, op_cls in policy.OPS.items() if op_cls not in _BUILDERS]
    + [f"fake kind {kind}" for kind in policy.FAKE_KINDS - set(_FAKERS)]
)
if _unimplemented:  # pragma: no cover - a wiring mistake, caught at import
    raise ImportError(
        f"deid.policy accepts {', '.join(_unimplemented)}, which deid.ops cannot "
        "apply; every op the policy can express needs both halves here"
    )


def needs_anchor(op: policy.Op) -> bool:
    """True for ops built per record because their parameter is per entity."""
    return isinstance(op, policy.Anchored)


def anchor_column(op: policy.Op) -> str | None:
    """The column an op is anchored on, if it is anchored on one."""
    return op.anchor if isinstance(op, policy.Anchored) else None


def build(
    rule: policy.Rule, raw_type: AvroType, *, keys: Keys, anchor: object = _UNSET
) -> Op:
    """Both halves of ``rule``, for a column of ``raw_type``.

    Raises :class:`IncompatibleColumnError` if the rule cannot work against
    that type. That happens here, at startup, while the clean schema is being
    derived -- which is the entire reason the type derivation is a function and
    not a comment.

    ``anchor`` is the record's value for :attr:`~deid.policy.Anchored.anchor`,
    read from the raw record before any op has touched it. Omit it to derive
    the type; supply it to transform a record. Ops that are not anchored ignore
    it.
    """
    builder = _BUILDERS.get(type(rule.op))
    if builder is None:  # pragma: no cover - _unimplemented catches this at import
        raise IncompatibleColumnError(
            f"no implementation for op {rule.op.name!r}",
            table=rule.table,
            column=rule.column,
        )
    _clean_type, apply = builder(rule, raw_type, keys, anchor)
    return Op(
        derive_type=lambda other: builder(rule, other, keys, anchor)[0],
        apply=apply,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# One worked example per op, against the column types the clinic schema
# actually produces. `python -m deid.ops` prints it, so "what does this op do"
# is answered by running it rather than by reading the implementation.
_TIMESTAMP_TYPE = {"type": "long", "connect.name": avro.MICRO_TIMESTAMP}
_DATE_TYPE = {"type": "int", "connect.name": avro.DATE}

_DEMOS: tuple[tuple[str, policy.Op, AvroType, object], ...] = (
    ("state", policy.Passthrough(), ["null", "string"], "MA"),
    ("ssn", policy.Drop(), ["null", "string"], "078-05-1120"),
    ("location", policy.Null(), ["null", "string"], "Riverside Clinic - Suite 200"),
    ("body", policy.Redact(), "string", "Patient reports..."),
    ("patient_id", policy.Hmac(domain="patient"), "long", 4711),
    ("mrn", policy.Hmac(domain="mrn"), "string", "  MRN-000482 "),
    ("first_name", policy.Fake(kind="first_name"), "string", "Rosalind"),
    ("email", policy.Fake(kind="email"), ["null", "string"], "r.chen@example.org"),
    (
        "date_of_birth",
        policy.Generalize(to="birth_year", cap_age=89),
        _DATE_TYPE,
        (date(1948, 3, 14) - EPOCH).days,
    ),
    (
        "date_of_birth",
        policy.Generalize(to="birth_year", cap_age=89),
        _DATE_TYPE,
        (date(1929, 11, 2) - EPOCH).days,  # 96 years old: over the cap
    ),
    ("postal_code", policy.Generalize(to="zip3"), ["null", "string"], "02139-1234"),
    ("postal_code", policy.Generalize(to="zip3"), ["null", "string"], "03601"),
    (
        "diagnosis_codes",
        policy.Generalize(to="icd10_category"),
        {"type": "array", "items": "string"},
        ["E11.9", "E11.65", "M54.5"],
    ),
    (
        "scheduled_at",
        policy.DateShift(anchor="patient_id"),
        _TIMESTAMP_TYPE,
        int(datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc).timestamp() * 1_000_000),
    ),
    (
        "billed_amount",
        policy.NumericJitter(anchor="patient_id", pct=5),
        "string",
        "1420.00",
    ),
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m deid.ops",
        description="Show what each de-identification op does to a value and its type.",
    )
    parser.add_argument(
        "--salt",
        default=None,
        help="salt to key the demo with (default: a fixed demo salt, printed below)",
    )
    args = parser.parse_args(argv)

    salt = args.salt.encode("utf-8") if args.salt else DEMO_SALT
    try:
        keys = Keys(salt=salt, reference_date=DEMO_REFERENCE_DATE)
    except (TypeError, ValueError) as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1

    print(f"salt: {salt.decode('utf-8', 'replace')!r}")
    print(f"reference date: {keys.reference_date} (the age cap is measured against it)")
    print()
    for column, op, raw_type, value in _DEMOS:
        rule = policy.Rule(table="public.demo", column=column, op=op)
        built = build(rule, raw_type, keys=keys, anchor=4711)
        clean_type = built.derive_type(raw_type)
        print(f"  {rule}")
        print(f"    type  {avro.describe(raw_type)} -> "
              f"{'(removed)' if clean_type is DROPPED else avro.describe(clean_type)}")
        clean = built.apply(value)
        print(f"    value {value!r} -> "
              f"{'(removed)' if clean is DROPPED else repr(clean)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
