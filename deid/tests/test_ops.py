"""What the ops promise, asserted.

Three kinds of test here.

The first kind is per op: what it does to an ordinary value, what it does to a
null, and what it does at the boundary that decides whether the op is correct
-- the age exactly at the HIPAA cap, the zip3 on the restricted list, the
timestamp one day from overflowing its int64.

The second kind is the conformance property, and it is the one this module
exists for. For every op and every column type, either the op refuses the
column at startup or the value it produces fits the type it derived. There is
no third outcome. A pair that disagrees is a record the schema registry rejects
at produce time, halfway through a replay, with a partially-written clean topic
behind it -- so the disagreement has to be a test failure instead. Both halves
come out of one builder call, which is what makes the property tractable; this
is what proves the builders actually do it.

The third kind is determinism. A clean topic that cannot be regenerated
byte-for-byte from its raw topic is not a replica of anything, so the same
input under the same salt has to give the same output -- in the same process,
in the next one, and under a different PYTHONHASHSEED.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from datetime import date, datetime, timedelta, timezone

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from deid import avro, ops, policy

SALT = b"a-fixed-test-salt-not-a-real-one"
OTHER_SALT = b"a-different-test-salt-of-its-own"
REFERENCE_DATE = date(2026, 8, 1)

KEYS = ops.Keys(salt=SALT, reference_date=REFERENCE_DATE)
OTHER_KEYS = ops.Keys(salt=OTHER_SALT, reference_date=REFERENCE_DATE)

ANCHOR = 4711

# The column types the clinic schema actually produces through Debezium, plus
# the shapes it produces under the connector settings the DDL warns about.
DATE_TYPE = {"type": "int", "connect.name": avro.DATE}
TIMESTAMP_TYPE = {"type": "long", "connect.name": avro.TIMESTAMP}
MICRO_TYPE = {"type": "long", "connect.name": avro.MICRO_TIMESTAMP}
NANO_TYPE = {"type": "long", "connect.name": avro.NANO_TIMESTAMP}
ZONED_TYPE = {"type": "string", "connect.name": avro.ZONED_TIMESTAMP}
JSON_TYPE = {"type": "string", "connect.name": avro.JSON}
ENUM_TYPE = {"type": "string", "connect.name": avro.ENUM}
DECIMAL_TYPE = {"type": "bytes", "connect.name": avro.DECIMAL, "scale": 2}
ARRAY_TYPE = {"type": "array", "items": "string"}
NULLABLE_STRING = ["null", "string"]


def build(op: policy.Op, raw_type, *, keys: ops.Keys = KEYS, column: str = "c", **kwargs):
    rule = policy.Rule(table="public.patients", column=column, op=op)
    return ops.build(rule, raw_type, keys=keys, **kwargs)


def days(day: date) -> int:
    """A date as Debezium writes it: days since the epoch."""
    return (day - ops.EPOCH).days


def micros(moment: datetime) -> int:
    return int(moment.timestamp() * 1_000_000)


def age_on(dob: date, reference: date) -> int:
    """Full years, the way a person counts them."""
    return (
        reference.year
        - dob.year
        - ((reference.month, reference.day) < (dob.month, dob.day))
    )


# ---------------------------------------------------------------------------
# passthrough, drop, null, redact
# ---------------------------------------------------------------------------


def test_passthrough_changes_neither_half():
    op = build(policy.Passthrough(), NULLABLE_STRING)
    assert op.derive_type(NULLABLE_STRING) == NULLABLE_STRING
    assert op.apply("MA") == "MA"
    assert op.apply(None) is None


def test_drop_removes_the_field_from_both_halves():
    op = build(policy.Drop(), NULLABLE_STRING)
    assert op.derive_type(NULLABLE_STRING) is ops.DROPPED
    assert op.apply("078-05-1120") is ops.DROPPED
    assert op.apply(None) is ops.DROPPED


def test_dropped_is_not_none_and_is_not_true():
    """A caller that forgets to check gets something obviously wrong."""
    assert ops.DROPPED is not None
    assert not ops.DROPPED
    assert repr(ops.DROPPED) == "DROPPED"


def test_null_keeps_the_column_and_widens_it():
    op = build(policy.Null(), "string")
    assert op.derive_type("string") == ["null", "string"]
    assert op.apply("Riverside Clinic") is None


def test_null_does_not_double_wrap_an_already_nullable_column():
    op = build(policy.Null(), NULLABLE_STRING)
    assert op.derive_type(NULLABLE_STRING) == NULLABLE_STRING


def test_redact_lands_every_row_on_one_constant():
    op = build(policy.Redact(), "string")
    assert op.derive_type("string") == "string"
    assert op.apply("Patient reports chest pain") == "[redacted]"
    assert op.apply("something else entirely") == "[redacted]"


def test_redact_takes_the_constant_from_the_policy():
    op = build(policy.Redact(value="XXX"), "string")
    assert op.apply("anything") == "XXX"


def test_redact_turns_a_non_string_column_into_a_string():
    op = build(policy.Redact(), "long")
    assert op.derive_type("long") == "string"
    assert op.apply(4711) == "[redacted]"


def test_redact_does_not_invent_a_value_where_there_was_none():
    op = build(policy.Redact(), NULLABLE_STRING)
    assert op.derive_type(NULLABLE_STRING) == NULLABLE_STRING
    assert op.apply(None) is None


# ---------------------------------------------------------------------------
# hmac
# ---------------------------------------------------------------------------


def test_hmac_gives_the_same_surrogate_for_the_same_input():
    op = build(policy.Hmac(domain="patient"), "long")
    assert op.derive_type("long") == "string"
    assert op.apply(4711) == op.apply(4711)


def test_hmac_tokens_are_fixed_width_and_boring():
    """They end up in a database column and in a person's terminal."""
    token = build(policy.Hmac(domain="patient"), "long").apply(4711)
    assert len(token) == 24
    assert token.isalnum() and token.isupper()


def test_hmac_joins_survive_across_tables_in_one_domain():
    """The whole reason the domain exists."""
    patients = build(policy.Hmac(domain="patient"), "long", column="patient_id")
    appointments = build(policy.Hmac(domain="patient"), "long", column="patient_id")
    assert patients.apply(4711) == appointments.apply(4711)


def test_a_different_domain_cannot_be_joined_against():
    patient = build(policy.Hmac(domain="patient"), "long")
    mrn = build(policy.Hmac(domain="mrn"), "long")
    assert patient.apply(4711) != mrn.apply(4711)


def test_a_different_salt_gives_a_different_surrogate():
    assert build(policy.Hmac(domain="patient"), "long").apply(4711) != build(
        policy.Hmac(domain="patient"), "long", keys=OTHER_KEYS
    ).apply(4711)


def test_hmac_normalises_the_dirt_the_source_actually_contains():
    """A padded mrn is one person, and two surrogates would break the join."""
    op = build(policy.Hmac(domain="mrn"), "string")
    assert op.apply("  MRN-000482 ") == op.apply("MRN-000482")
    assert op.apply("MRN-000482") == op.apply("mrn-000482")
    assert op.apply("MRN  000482") == op.apply("MRN 000482")


def test_hmac_does_not_normalise_punctuation_away():
    """The documented limit: merging these would invent a join, not fix one."""
    op = build(policy.Hmac(domain="mrn"), "string")
    assert op.apply("A-100") != op.apply("A100")


def test_hmac_passes_a_null_through():
    assert build(policy.Hmac(domain="patient"), NULLABLE_STRING).apply(None) is None


def test_hmac_keeps_nullability():
    op = build(policy.Hmac(domain="patient"), NULLABLE_STRING)
    assert op.derive_type(NULLABLE_STRING) == NULLABLE_STRING


# ---------------------------------------------------------------------------
# fake
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", sorted(policy.FAKE_KINDS))
def test_every_fake_kind_produces_a_string_and_keeps_the_type(kind):
    op = build(policy.Fake(kind=kind), "string")
    assert op.derive_type("string") == "string"
    value = op.apply("Rosalind Whitfield")
    assert isinstance(value, str) and value


@pytest.mark.parametrize("kind", sorted(policy.FAKE_KINDS))
def test_fake_is_stable_for_one_input(kind):
    op = build(policy.Fake(kind=kind), "string")
    assert op.apply("Rosalind") == op.apply("Rosalind")


def test_fake_differs_by_kind_so_a_first_name_is_not_a_last_name():
    first = build(policy.Fake(kind="first_name"), "string").apply("Rosalind")
    last = build(policy.Fake(kind="last_name"), "string").apply("Rosalind")
    assert first != last


def test_fake_differs_by_salt():
    assert build(policy.Fake(kind="first_name"), "string").apply("Rosalind") != build(
        policy.Fake(kind="first_name"), "string", keys=OTHER_KEYS
    ).apply("Rosalind")


def test_fake_passes_a_null_through():
    """A null email is a null email; inventing one would be a new fact."""
    assert build(policy.Fake(kind="email"), NULLABLE_STRING).apply(None) is None


def test_fake_emails_cannot_reach_anybody():
    op = build(policy.Fake(kind="email"), "string")
    for original in ("r.chen@example.org", "someone@real-domain.test", "x"):
        value = op.apply(original)
        assert value.count("@") == 1
        local, domain = value.split("@")
        assert domain in vocab_domains()
        assert local.isascii()


def vocab_domains():
    from deid import vocab

    return set(vocab.EMAIL_DOMAINS)


def test_fake_phones_are_in_the_range_reserved_for_fiction():
    op = build(policy.Fake(kind="phone"), "string")
    for original in ("(617) 555-0142", "413-555-0100", "nonsense"):
        assert "555-01" in op.apply(original)


def test_fake_preserves_equality_which_is_the_trade_it_makes():
    """Two rows with the same name get the same fake name. Documented, tested."""
    op = build(policy.Fake(kind="last_name"), "string")
    assert op.apply("Whitfield") == op.apply("Whitfield")
    assert op.apply("Whitfield") != op.apply("Sandoval")


# ---------------------------------------------------------------------------
# generalize: zip3
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "postal,expected",
    [
        ("02139", "021"),
        ("02139-1234", "021"),  # zip+4
        ("021391234", "021"),
        (" 02139 ", "021"),
        ("94110", "941"),
        ("03601", "000"),  # 036 is on the Safe Harbor restricted list
        ("05901", "000"),  # ... and so is 059
        ("0213", None),  # not a zip; guessing would leak or invent
        ("", None),
        ("SW1A 1AA", None),
        (None, None),
    ],
)
def test_zip3(postal, expected):
    op = build(policy.Generalize(to="zip3"), NULLABLE_STRING)
    assert op.apply(postal) == expected


def test_zip3_widens_to_nullable_because_it_can_fail_to_read_a_zip():
    op = build(policy.Generalize(to="zip3"), "string")
    assert op.derive_type("string") == ["null", "string"]


def test_the_restricted_zip3_list_is_the_safe_harbor_one():
    """Seventeen prefixes, each with under 20,000 people behind it."""
    assert len(ops.RESTRICTED_ZIP3) == 17
    assert "036" in ops.RESTRICTED_ZIP3
    assert "021" not in ops.RESTRICTED_ZIP3


# ---------------------------------------------------------------------------
# generalize: dates
# ---------------------------------------------------------------------------


def test_birth_year_keeps_the_year_under_the_cap():
    op = build(policy.Generalize(to="birth_year", cap_age=89), DATE_TYPE)
    assert op.derive_type(DATE_TYPE) == ["null", "int"]
    assert op.apply(days(date(1990, 5, 2))) == 1990


def test_nobody_over_the_safe_harbor_cap_escapes_the_bucket():
    """The property the cap exists for, checked across a century of birthdays."""
    op = build(policy.Generalize(to="birth_year", cap_age=89), DATE_TYPE)
    floor = REFERENCE_DATE.year - 89 - 1
    for year in range(1890, 2027):
        for month in (1, 6, 12):
            dob = date(year, month, 15)
            emitted = op.apply(days(dob))
            if age_on(dob, REFERENCE_DATE) > 89:
                assert emitted == floor, f"{dob} is {age_on(dob, REFERENCE_DATE)} and leaked"
            assert emitted == max(dob.year, floor)


def test_birth_year_without_a_cap_does_not_bucket_anyone():
    op = build(policy.Generalize(to="birth_year"), DATE_TYPE)
    assert op.apply(days(date(1912, 4, 15))) == 1912


def test_birth_year_of_an_unreadable_date_is_null_not_a_guess():
    op = build(policy.Generalize(to="birth_year", cap_age=89), DATE_TYPE)
    assert op.apply(2**31 - 1) is None  # ~5.8 million AD
    assert op.apply(None) is None


def test_year_and_month_from_a_microsecond_timestamp():
    moment = datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc)
    assert build(policy.Generalize(to="year"), MICRO_TYPE).apply(micros(moment)) == 2026
    assert build(policy.Generalize(to="month"), MICRO_TYPE).apply(micros(moment)) == "2026-03"


def test_year_from_a_zoned_timestamp_string():
    op = build(policy.Generalize(to="year"), ZONED_TYPE)
    assert op.apply("2026-03-02T09:15:00Z") == 2026
    assert op.apply("not a timestamp") is None


def test_a_timestamp_before_the_epoch_lands_on_the_right_day():
    """Floor division, or a negative instant reports the day after itself."""
    moment = datetime(1969, 7, 20, 20, 17, tzinfo=timezone.utc)
    assert build(policy.Generalize(to="month"), MICRO_TYPE).apply(micros(moment)) == "1969-07"


@pytest.mark.parametrize(
    "age,expected",
    [(0, "0-9"), (9, "0-9"), (10, "10-19"), (35, "30-39"), (89, "80-89"), (90, "90+"), (104, "90+")],
)
def test_age_band_from_an_integer_age(age, expected):
    op = build(policy.Generalize(to="age_band", cap_age=89), "int")
    assert op.apply(age) == expected


def test_age_band_from_a_date_of_birth():
    op = build(policy.Generalize(to="age_band", cap_age=89), DATE_TYPE)
    assert op.apply(days(date(1990, 5, 2))) == "30-39"
    assert op.apply(days(date(1929, 11, 2))) == "90+"


def test_a_negative_age_is_corrupt_data_not_a_cohort():
    assert build(policy.Generalize(to="age_band"), "int").apply(-1) is None


# ---------------------------------------------------------------------------
# generalize: icd10
# ---------------------------------------------------------------------------


def test_icd10_category_truncates_a_code():
    op = build(policy.Generalize(to="icd10_category"), NULLABLE_STRING)
    assert op.apply("E11.9") == "E11"
    assert op.apply("m54.5") == "M54"
    assert op.apply("C4A.70") == "C4A"
    assert op.apply("nope") is None
    assert op.apply(None) is None


def test_icd10_category_over_the_array_the_clinic_actually_has():
    op = build(policy.Generalize(to="icd10_category"), ARRAY_TYPE)
    assert op.derive_type(ARRAY_TYPE) == ARRAY_TYPE
    # Deduplicated: two codes in one category would say something about the
    # original that the generalization is there to remove.
    assert op.apply(["E11.9", "E11.65", "M54.5"]) == ["E11", "M54"]
    assert op.apply([]) == []
    assert op.apply(["junk"]) == []


# ---------------------------------------------------------------------------
# date_shift
# ---------------------------------------------------------------------------


def test_date_shift_keeps_the_type_it_was_given():
    op = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=ANCHOR)
    assert op.derive_type(MICRO_TYPE) == MICRO_TYPE


def test_intervals_inside_one_entity_survive_exactly():
    """The reason the offset is per anchor and not per record."""
    op = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=ANCHOR)
    first = micros(datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc))
    second = micros(datetime(2026, 3, 2, 9, 47, tzinfo=timezone.utc))
    assert op.apply(second) - op.apply(first) == second - first


def test_the_offset_is_the_same_across_columns_and_across_tables():
    appointments = build(
        policy.DateShift(anchor="patient_id"), MICRO_TYPE, column="scheduled_at", anchor=ANCHOR
    )
    notes = build(
        policy.DateShift(anchor="patient_id"), MICRO_TYPE, column="authored_at", anchor=ANCHOR
    )
    moment = micros(datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc))
    assert appointments.apply(moment) == notes.apply(moment)


def test_the_offset_is_the_same_whatever_unit_the_column_uses():
    """A date column and a timestamp column in one row must move together."""
    day = date(2026, 3, 2)
    moment = datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc)
    shifted_day = build(policy.DateShift(anchor="patient_id"), DATE_TYPE, anchor=ANCHOR).apply(
        days(day)
    )
    shifted_moment = build(
        policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=ANCHOR
    ).apply(micros(moment))
    assert shifted_day - days(day) == (shifted_moment - micros(moment)) // 86_400_000_000


def test_different_entities_get_different_offsets():
    op_type = MICRO_TYPE
    moment = micros(datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc))
    shifted = {
        build(policy.DateShift(anchor="patient_id"), op_type, anchor=patient).apply(moment)
        for patient in range(200)
    }
    # 200 patients over 1461 possible offsets: a handful of collisions is
    # expected, an offset that ignores the anchor is not.
    assert len(shifted) > 150


def test_the_shift_stays_inside_the_documented_range():
    moment = micros(datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc))
    for patient in range(200):
        op = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=patient)
        offset_days = (op.apply(moment) - moment) / 86_400_000_000
        assert abs(offset_days) <= ops.MAX_SHIFT_DAYS


def test_the_time_of_day_survives_the_shift():
    """Whole days, so "the clinic opens at nine" stays true in the replica."""
    moment = micros(datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc))
    for patient in range(20):
        op = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=patient)
        assert op.apply(moment) % 86_400_000_000 == moment % 86_400_000_000


def test_date_shift_passes_a_null_through():
    op = build(policy.DateShift(anchor="patient_id"), ["null", MICRO_TYPE], anchor=ANCHOR)
    assert op.apply(None) is None


def test_records_with_no_anchor_entity_share_one_offset():
    """They have no entity to preserve intervals within, and passing them
    through unshifted would hand back the real calendar."""
    moment = micros(datetime(2026, 3, 2, 9, 15, tzinfo=timezone.utc))
    first = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=None).apply(moment)
    second = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=None).apply(moment)
    assert first == second != moment


def test_a_shift_that_would_leave_the_range_is_clamped_not_wrapped():
    """Wrapping produces a conforming record that is off by 584,000 years."""
    op = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE, anchor=ANCHOR)
    for extreme in (avro.INT64_MAX, avro.INT64_MIN):
        assert avro.conforms(op.apply(extreme), MICRO_TYPE)


def test_date_shift_on_a_zoned_timestamp_string():
    op = build(policy.DateShift(anchor="patient_id"), ZONED_TYPE, anchor=ANCHOR)
    shifted = op.apply("2026-03-02T09:15:00Z")
    assert shifted.endswith("Z")  # the spelling that came in
    assert shifted.startswith(("2024-", "2025-", "2026-", "2027-", "2028-"))
    assert "T09:15:00" in shifted


def test_an_offset_zoned_timestamp_keeps_its_offset():
    op = build(policy.DateShift(anchor="patient_id"), ZONED_TYPE, anchor=ANCHOR)
    assert op.apply("2026-03-02T09:15:00-05:00").endswith("-05:00")


def test_a_zoned_timestamp_that_is_not_a_timestamp_becomes_null():
    op = build(policy.DateShift(anchor="patient_id"), ZONED_TYPE, anchor=ANCHOR)
    assert op.derive_type(ZONED_TYPE) == ["null", ZONED_TYPE]
    assert op.apply("last Tuesday") is None


def test_a_caller_can_ask_which_rules_have_to_be_built_per_record():
    assert ops.needs_anchor(policy.DateShift(anchor="patient_id"))
    assert ops.needs_anchor(policy.NumericJitter(anchor="patient_id", pct=5))
    assert not ops.needs_anchor(policy.Hmac(domain="patient"))
    assert ops.anchor_column(policy.DateShift(anchor="patient_id")) == "patient_id"
    assert ops.anchor_column(policy.Passthrough()) is None


def test_applying_an_unanchored_shift_says_what_the_caller_forgot():
    """Deriving the type without a record is fine; transforming one is not."""
    op = build(policy.DateShift(anchor="patient_id"), MICRO_TYPE)
    assert op.derive_type(MICRO_TYPE) == MICRO_TYPE
    with pytest.raises(ops.AnchorRequired) as exc:
        op.apply(0)
    assert "patient_id" in str(exc.value)


# ---------------------------------------------------------------------------
# numeric_jitter
# ---------------------------------------------------------------------------


def test_jitter_moves_an_amount_by_no_more_than_the_policy_says():
    for patient in range(200):
        op = build(policy.NumericJitter(anchor="patient_id", pct=5), "string", anchor=patient)
        moved = float(op.apply("1420.00"))
        assert 1420.00 * 0.95 <= moved <= 1420.00 * 1.05


def test_jitter_actually_moves_something():
    moved = {
        build(policy.NumericJitter(anchor="patient_id", pct=5), "string", anchor=p).apply("1420.00")
        for p in range(50)
    }
    assert len(moved) > 25


def test_amounts_in_one_row_keep_their_relationships():
    """Billed >= allowed >= paid has to survive, or the replica is useless."""
    op = policy.NumericJitter(anchor="patient_id", pct=10)
    billed = build(op, "string", column="billed_amount", anchor=ANCHOR).apply("1420.00")
    allowed = build(op, "string", column="allowed_amount", anchor=ANCHOR).apply("980.00")
    paid = build(op, "string", column="paid_amount", anchor=ANCHOR).apply("742.35")
    assert float(billed) > float(allowed) > float(paid)
    # One factor for the whole entity, so the ratios come back too -- up to the
    # half-cent each amount was rounded by to keep its scale.
    assert float(billed) / float(allowed) == pytest.approx(1420.00 / 980.00, abs=1e-4)
    assert float(allowed) / float(paid) == pytest.approx(980.00 / 742.35, abs=1e-4)


def test_a_different_entity_gets_a_different_factor():
    op = policy.NumericJitter(anchor="patient_id", pct=10)
    assert build(op, "string", anchor=1).apply("1420.00") != build(
        op, "string", anchor=2
    ).apply("1420.00")


def test_jitter_keeps_the_scale_the_column_was_written_with():
    op = build(policy.NumericJitter(anchor="patient_id", pct=10), "string", anchor=ANCHOR)
    assert op.apply("1420.00").count(".") == 1
    assert len(op.apply("1420.00").split(".")[1]) == 2
    assert "." not in op.apply("1420")


def test_jitter_cannot_manufacture_money():
    """Zero is zero: a multiplicative factor leaves it alone, on purpose."""
    op = build(policy.NumericJitter(anchor="patient_id", pct=25), "string", anchor=ANCHOR)
    assert float(op.apply("0.00")) == 0.0


def test_jitter_keeps_the_sign_of_a_credit():
    op = build(policy.NumericJitter(anchor="patient_id", pct=10), "string", anchor=ANCHOR)
    assert float(op.apply("-120.00")) < 0


def test_an_amount_that_is_not_a_number_becomes_null():
    op = build(policy.NumericJitter(anchor="patient_id", pct=10), "string", anchor=ANCHOR)
    assert op.derive_type("string") == ["null", "string"]
    assert op.apply("not money") is None
    assert op.apply("nan") is None
    assert op.apply(None) is None


def test_jitter_on_an_integer_column_stays_an_integer():
    op = build(policy.NumericJitter(anchor="patient_id", pct=10), "int", anchor=ANCHOR)
    assert op.derive_type("int") == "int"
    moved = op.apply(30)
    assert isinstance(moved, int) and 27 <= moved <= 33


def test_jitter_on_an_integer_column_clamps_rather_than_overflows():
    op = build(policy.NumericJitter(anchor="patient_id", pct=25), "int", anchor=ANCHOR)
    assert avro.conforms(op.apply(avro.INT32_MAX), "int")
    assert avro.conforms(op.apply(avro.INT32_MIN), "int")


def test_jitter_on_a_double_column():
    op = build(policy.NumericJitter(anchor="patient_id", pct=10), "double", anchor=ANCHOR)
    assert 1278.0 <= op.apply(1420.0) <= 1562.0


def test_applying_an_unanchored_jitter_says_what_the_caller_forgot():
    op = build(policy.NumericJitter(anchor="patient_id", pct=10), "string")
    assert op.derive_type("string") == ["null", "string"]
    with pytest.raises(ops.AnchorRequired):
        op.apply("1420.00")


# ---------------------------------------------------------------------------
# refusals: the mismatches that have to halt at startup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "op,raw_type,expected",
    [
        (policy.Generalize(to="zip3"), "long", "postal code"),
        (policy.Generalize(to="zip3"), ZONED_TYPE, "postal code"),
        (policy.Generalize(to="birth_year"), "string", "needs a date"),
        (policy.Generalize(to="year"), "long", "needs a date"),
        (policy.Generalize(to="icd10_category"), "long", "code string"),
        (policy.Generalize(to="icd10_category"), {"type": "array", "items": "long"}, "strings"),
        (policy.Generalize(to="age_band"), "double", "integer age"),
        (policy.Fake(kind="first_name"), "long", "only replace a string"),
        (policy.Fake(kind="first_name"), ZONED_TYPE, "means something specific"),
        (policy.Fake(kind="first_name"), JSON_TYPE, "means something specific"),
        (policy.Hmac(domain="patient"), "double", "tokenized"),
        (policy.Hmac(domain="patient"), "boolean", "tokenized"),
        (policy.Hmac(domain="patient"), ARRAY_TYPE, "tokenized"),
        (policy.Hmac(domain="patient"), MICRO_TYPE, "not an identifier"),
        (policy.Hmac(domain="patient"), DECIMAL_TYPE, "not an identifier"),
        (policy.DateShift(anchor="patient_id"), "long", "no unit here"),
        (policy.DateShift(anchor="patient_id"), "string", "no unit here"),
        (policy.NumericJitter(anchor="patient_id", pct=5), DECIMAL_TYPE, "base64 Decimal"),
        (policy.NumericJitter(anchor="patient_id", pct=5), "boolean", "only a number"),
        (policy.NumericJitter(anchor="patient_id", pct=5), MICRO_TYPE, "only a number"),
    ],
)
def test_a_rule_that_cannot_work_against_its_column_halts_at_startup(op, raw_type, expected):
    with pytest.raises(ops.IncompatibleColumnError) as exc:
        build(op, raw_type, anchor=ANCHOR, column="the_column")
    assert expected in str(exc.value)
    # The message has to say where, like every other policy failure.
    assert exc.value.table == "public.patients"
    assert exc.value.column == "the_column"
    assert str(exc.value).startswith("public.patients.the_column: ")


def test_an_incompatible_column_is_a_policy_error():
    """One except clause at the startup boundary has to be enough."""
    with pytest.raises(policy.PolicyError):
        build(policy.Generalize(to="zip3"), "long")


@pytest.mark.parametrize(
    "op",
    [
        policy.Hmac(domain="patient"),
        policy.Fake(kind="first_name"),
        policy.Generalize(to="zip3"),
        policy.Redact(),
    ],
)
def test_a_multi_branch_union_is_refused_rather_than_guessed_at(op):
    with pytest.raises(ops.IncompatibleColumnError) as exc:
        build(op, ["null", "string", "long"])
    assert "ambiguous" in str(exc.value)


def test_passthrough_and_drop_accept_anything():
    """They are the two ops that never have to read a value."""
    for raw_type in (["null", "string", "long"], DECIMAL_TYPE, ARRAY_TYPE, "com.example.Rec"):
        assert build(policy.Passthrough(), raw_type).apply(None) is None
        assert build(policy.Drop(), raw_type).apply(None) is ops.DROPPED


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------


def test_a_short_salt_is_refused_at_construction():
    with pytest.raises(ValueError) as exc:
        ops.Keys(salt=b"too-short", reference_date=REFERENCE_DATE)
    assert "at least 16 bytes" in str(exc.value)


def test_a_salt_that_is_not_bytes_is_refused():
    with pytest.raises(TypeError):
        ops.Keys(salt="a-string-of-the-right-length", reference_date=REFERENCE_DATE)


def test_a_datetime_reference_is_normalised_to_its_day():
    keys = ops.Keys(salt=SALT, reference_date=datetime(2026, 8, 1, 13, 30))
    assert keys.reference_date == date(2026, 8, 1)
    assert keys == KEYS


def test_the_module_does_not_read_the_environment_for_its_salt():
    """Injected, or there is no key: a module that can find its own key
    material can be tested with the wrong one and pass."""
    source = (
        __import__("pathlib").Path(ops.__file__).read_text(encoding="utf-8")
    )
    assert "os.environ" not in source
    assert "getenv" not in source


# ---------------------------------------------------------------------------
# determinism, including across processes
# ---------------------------------------------------------------------------

_CROSS_PROCESS = textwrap.dedent(
    """
    from datetime import date
    from deid import ops, policy

    keys = ops.Keys(salt=%r, reference_date=date(2026, 8, 1))
    for op, raw_type, value in (
        (policy.Hmac(domain="patient"), "long", 4711),
        (policy.Hmac(domain="mrn"), "string", "  MRN-000482 "),
        (policy.Fake(kind="full_name"), "string", "Rosalind Whitfield"),
    ):
        rule = policy.Rule(table="public.patients", column="c", op=op)
        print(ops.build(rule, raw_type, keys=keys).apply(value))
    """
)


def _in_another_process(hash_seed: str) -> str:
    result = subprocess.run(
        [sys.executable, "-c", _CROSS_PROCESS % SALT],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONHASHSEED": hash_seed},
    )
    return result.stdout


def test_the_same_domain_gives_the_same_surrogate_in_a_different_process():
    """Two runs, two hash seeds, one answer.

    ``hash()`` is randomised per process, so anything that reached for it --
    directly, or through a dict iteration order, or through a set -- would show
    up here and nowhere else. It has to hold across restarts because a replay
    six months from now has to join to what was written today.
    """
    first = _in_another_process("0")
    second = _in_another_process("12345")
    assert first == second

    in_process = "".join(
        f"{value}\n"
        for value in (
            build(policy.Hmac(domain="patient"), "long").apply(4711),
            build(policy.Hmac(domain="mrn"), "string").apply("  MRN-000482 "),
            build(policy.Fake(kind="full_name"), "string").apply("Rosalind Whitfield"),
        )
    )
    assert first == in_process


# ---------------------------------------------------------------------------
# the conformance property
# ---------------------------------------------------------------------------
#
# The column types the raw topics carry, each with one representative value and
# a strategy for the rest. The pair *is* `schema_of`: an Avro type cannot be
# recovered from a Python value (an int is an int, a date and a row version all
# at once), so the samples carry the type they were drawn for.

MONEYISH = [
    "1420.00", "0.00", "-12.50", "1420", "1e400", "nan", "inf", "not money", "",
]
POSTALISH = ["02139", "02139-1234", "03601", "0213", "SW1A 1AA"]
CODEISH = ["E11.9", "C4A.70", "m54.5", "junk"]
INSTANTISH = ["2026-03-02T09:15:00Z", "2026-03-02T09:15:00.123456+00:00", "last Tuesday"]

TEXT = st.one_of(
    st.text(max_size=24),
    st.sampled_from(MONEYISH + POSTALISH + CODEISH + INSTANTISH + ["  MRN-000482 "]),
)

COLUMN_TYPES: tuple[tuple[str, object, object, object], ...] = (
    # name, avro type, representative value, strategy
    ("boolean", "boolean", True, st.booleans()),
    ("int", "int", 30, st.integers(avro.INT32_MIN, avro.INT32_MAX)),
    ("long", "long", 4711, st.integers(avro.INT64_MIN, avro.INT64_MAX)),
    ("float", "float", 1.5, st.floats(width=32)),
    ("double", "double", 1420.0, st.floats()),
    ("bytes", "bytes", b"\x01\x02", st.binary(max_size=8)),
    ("string", "string", "1420.00", TEXT),
    ("date", DATE_TYPE, 19_000, st.integers(avro.INT32_MIN, avro.INT32_MAX)),
    ("timestamp", TIMESTAMP_TYPE, 1_772_442_900_000, st.integers(avro.INT64_MIN, avro.INT64_MAX)),
    ("micro", MICRO_TYPE, 1_772_442_900_000_000, st.integers(avro.INT64_MIN, avro.INT64_MAX)),
    ("nano", NANO_TYPE, 1_772_442_900_000_000_000, st.integers(avro.INT64_MIN, avro.INT64_MAX)),
    ("zoned", ZONED_TYPE, "2026-03-02T09:15:00Z", TEXT),
    ("json", JSON_TYPE, '{"a": 1}', TEXT),
    ("enum", ENUM_TYPE, "scheduled", st.sampled_from(["scheduled", "no_show"])),
    ("decimal", DECIMAL_TYPE, b"\x00\x01", st.binary(max_size=8)),
    ("array", ARRAY_TYPE, ["E11.9", "M54.5"], st.lists(TEXT, max_size=4)),
    ("nullable string", NULLABLE_STRING, None, st.none() | TEXT),
    ("nullable long", ["null", "long"], None, st.none() | st.integers(avro.INT64_MIN, avro.INT64_MAX)),
    ("nullable date", ["null", DATE_TYPE], None, st.none() | st.integers(-50_000, 50_000)),
    ("nullable micro", ["null", MICRO_TYPE], None, st.none() | st.integers(avro.INT64_MIN, avro.INT64_MAX)),
    ("nullable array", ["null", ARRAY_TYPE], None, st.none() | st.lists(TEXT, max_size=4)),
)

OP_CASES: tuple[policy.Op, ...] = (
    policy.Passthrough(),
    policy.Drop(),
    policy.Null(),
    policy.Redact(),
    policy.Redact(value="XXX"),
    policy.Hmac(domain="patient"),
    *(policy.Fake(kind=kind) for kind in sorted(policy.FAKE_KINDS)),
    *(policy.Generalize(to=target) for target in sorted(policy.GENERALIZE_TARGETS)),
    policy.Generalize(to="birth_year", cap_age=89),
    policy.Generalize(to="age_band", cap_age=89),
    policy.DateShift(anchor="patient_id"),
    policy.NumericJitter(anchor="patient_id", pct=1),
    policy.NumericJitter(anchor="patient_id", pct=25),
)


def _same(first, second) -> bool:
    if isinstance(first, float) and isinstance(second, float):
        return repr(first) == repr(second)  # nan is not equal to itself
    return first == second


def check_conformance(op: policy.Op, raw_type, value, anchor=ANCHOR) -> bool:
    """The property, in one place: refused at startup, or the halves agree.

    Returns whether the column was accepted, so the caller can also assert that
    an op is not passing by refusing everything.
    """
    rule = policy.Rule(table="public.patients", column="c", op=op)
    try:
        built = ops.build(rule, raw_type, keys=KEYS, anchor=anchor)
    except ops.IncompatibleColumnError:
        return False

    clean_type = built.derive_type(raw_type)
    clean = built.apply(value)

    if clean_type is ops.DROPPED:
        assert clean is ops.DROPPED, f"{op} derived a dropped field but produced {clean!r}"
        return True

    assert avro.conforms(clean, clean_type), (
        f"{op} on {avro.describe(raw_type)}: {value!r} -> {clean!r}, "
        f"which does not fit the derived {avro.describe(clean_type)}"
    )
    assert _same(clean, built.apply(value)), f"{op} is not deterministic on {value!r}"
    if value is None:
        assert clean is None, f"{op} invented {clean!r} where the source had null"
    return True


@pytest.mark.parametrize("name,raw_type,value,_strategy", COLUMN_TYPES, ids=lambda x: str(x)[:20])
def test_every_op_against_every_column_type(name, raw_type, value, _strategy):
    """Exhaustive over the cross product, with one value each.

    The hypothesis test below varies the values; this one guarantees that every
    (op, type) pair is visited at least once, which a random sampler does not.
    """
    for op in OP_CASES:
        check_conformance(op, raw_type, value)


@settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    op=st.sampled_from(OP_CASES),
    column=st.sampled_from(COLUMN_TYPES).flatmap(
        lambda spec: st.tuples(st.just(spec[1]), spec[3])
    ),
    anchor=st.one_of(st.integers(), st.text(max_size=8), st.none()),
)
def test_apply_conforms_to_the_type_derive_type_promised(op, column, anchor):
    raw_type, value = column
    check_conformance(op, raw_type, value, anchor=anchor)


@pytest.mark.parametrize("op", OP_CASES, ids=str)
def test_no_op_passes_the_property_by_refusing_everything(op):
    accepted = [
        name
        for name, raw_type, value, _ in COLUMN_TYPES
        if check_conformance(op, raw_type, value)
    ]
    assert accepted, f"{op} refuses every column type there is"


def test_every_op_the_policy_can_express_can_be_applied():
    """Adding an op to the policy without both halves here is an import error.

    Asserted as well as raised, because the import-time check only fires when
    something imports the module, and a policy op with no implementation is a
    topic that halts on its first record.
    """
    for op_cls in policy.OPS.values():
        assert op_cls in ops._BUILDERS, f"{op_cls.__name__} has no implementation"
    assert set(policy.FAKE_KINDS) == set(ops._FAKERS)


def test_the_shipped_policy_can_be_applied_to_the_clinic_schema():
    """Every rule in clinic.yml, against the type its column actually has.

    The policy tests check the rules against the DDL's column *names*; this
    checks them against the column *types*, which is the half that only exists
    once there is something to derive a clean schema with.
    """
    clinic = policy.load_policy(CLINIC_POLICY)
    for rule in clinic.all_rules():
        raw_type = CLINIC_COLUMN_TYPES[f"{rule.table}.{rule.column}"]
        built = ops.build(rule, raw_type, keys=KEYS, anchor=ANCHOR)
        assert built.derive_type(raw_type) is not None


CLINIC_POLICY = __import__("pathlib").Path(__file__).resolve().parents[1] / "policy" / "clinic.yml"

# The Avro types Debezium emits for the clinic schema, with
# decimal.handling.mode=string as the DDL's comment on claims requires.
_TEXT = ["null", "string"]
_REQUIRED_TEXT = "string"
_TS = ["null", MICRO_TYPE]
_REQUIRED_TS = MICRO_TYPE

CLINIC_COLUMN_TYPES = {
    "public.patients.patient_id": "long",
    "public.patients.mrn": _REQUIRED_TEXT,
    "public.patients.first_name": _REQUIRED_TEXT,
    "public.patients.middle_name": _TEXT,
    "public.patients.last_name": _REQUIRED_TEXT,
    "public.patients.date_of_birth": DATE_TYPE,
    "public.patients.ssn": _TEXT,
    "public.patients.email": _TEXT,
    "public.patients.phone": _TEXT,
    "public.patients.address_line1": _TEXT,
    "public.patients.address_line2": _TEXT,
    "public.patients.city": _TEXT,
    "public.patients.state": _TEXT,
    "public.patients.postal_code": _TEXT,
    "public.patients.created_at": _REQUIRED_TS,
    "public.patients.updated_at": _REQUIRED_TS,
    "public.providers.provider_id": "long",
    "public.providers.npi": _REQUIRED_TEXT,
    "public.providers.full_name": _REQUIRED_TEXT,
    "public.providers.specialty": _TEXT,
    "public.providers.email": _TEXT,
    "public.providers.created_at": _REQUIRED_TS,
    "public.providers.updated_at": _REQUIRED_TS,
    "public.appointments.appointment_id": "long",
    "public.appointments.patient_id": "long",
    "public.appointments.provider_id": "long",
    "public.appointments.scheduled_at": _REQUIRED_TS,
    "public.appointments.checked_in_at": _TS,
    "public.appointments.completed_at": _TS,
    "public.appointments.duration_minutes": "int",
    "public.appointments.status": ENUM_TYPE,
    "public.appointments.location": _TEXT,
    "public.appointments.intake_answers": ["null", JSON_TYPE],
    "public.appointments.created_at": _REQUIRED_TS,
    "public.appointments.updated_at": _REQUIRED_TS,
    "public.claims.claim_id": "long",
    "public.claims.patient_id": "long",
    "public.claims.appointment_id": ["null", "long"],
    "public.claims.billed_amount": _REQUIRED_TEXT,
    "public.claims.allowed_amount": _TEXT,
    "public.claims.paid_amount": _TEXT,
    "public.claims.patient_responsibility": _TEXT,
    "public.claims.diagnosis_codes": ARRAY_TYPE,
    "public.claims.procedure_code": _TEXT,
    "public.claims.claim_status": _REQUIRED_TEXT,
    "public.claims.submitted_at": _REQUIRED_TS,
    "public.claims.adjudicated_at": _TS,
    "public.claims.created_at": _REQUIRED_TS,
    "public.claims.updated_at": _REQUIRED_TS,
    "public.notes.note_id": "long",
    "public.notes.patient_id": "long",
    "public.notes.provider_id": "long",
    "public.notes.appointment_id": ["null", "long"],
    "public.notes.amends_note_id": ["null", "long"],
    "public.notes.note_type": _REQUIRED_TEXT,
    "public.notes.body": _REQUIRED_TEXT,
    "public.notes.authored_at": _REQUIRED_TS,
    "public.notes.signed_at": _TS,
    "public.notes.is_amended": "boolean",
    "public.notes.created_at": _REQUIRED_TS,
    "public.notes.updated_at": _REQUIRED_TS,
}
