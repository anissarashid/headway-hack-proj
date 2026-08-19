"""Envelope -> statement, for every operation the pipeline can produce."""

from __future__ import annotations

import pytest

from pit import ddl, envelope


@pytest.fixture
def patients(clean_schemas) -> ddl.Table:
    return ddl.read_table(*clean_schemas["patients"])


def row(**overrides):
    base = {
        "patient_id": "pt_9f2a",
        "mrn": "mrn_41bd",
        "first_name": "Rosalind",
        "middle_name": None,
        "last_name": "Chen",
        "date_of_birth": 1948,
        "email": "r.chen@example.org",
        "phone": "555-0100",
        "state": "MA",
        "postal_code": "021",
        "created_at": "2026-08-19T14:00:00.000000Z",
        "updated_at": "2026-08-19T14:00:00.000000Z",
    }
    base.update(overrides)
    return base


def value(op, after=None, before=None, ts_ms=1755612000000):
    return {
        "op": op,
        "before": before,
        "after": after,
        "source": {"ts_ms": ts_ms, "table": "patients", "schema": "public"},
        "ts_ms": ts_ms + 5,
    }


@pytest.mark.parametrize("op", ["c", "r", "u"])
def test_create_read_and_update_all_upsert(patients, op):
    """Upsert for all three, so replaying a range twice converges.

    An insert would fail the second time and an update would no-op the first,
    and either makes a re-run of the same offsets change the answer.
    """
    statement = envelope.translate(patients, {"patient_id": "pt_9f2a"}, value(op, after=row()))
    assert isinstance(statement, envelope.Upsert)
    assert statement.key == {"patient_id": "pt_9f2a"}
    assert statement.values["last_name"] == "Chen"


def test_delete_matches_on_the_key(patients):
    statement = envelope.translate(
        patients, {"patient_id": "pt_9f2a"}, value("d", before=row(), after=None)
    )
    assert isinstance(statement, envelope.Delete)
    assert statement.key == {"patient_id": "pt_9f2a"}


def test_tombstone_is_skipped(patients):
    """A null value says nothing the preceding delete has not already said."""
    assert envelope.translate(patients, {"patient_id": "pt_9f2a"}, None) is None


def test_key_comes_from_the_message_key_not_the_value(patients):
    """If the two disagreed, upserts would write duplicate rows.

    M4 de-identifies the key with the same ops as the payload precisely so they
    agree. This asserts that the key is what is used, by making them differ.
    """
    statement = envelope.translate(
        patients,
        {"patient_id": "from_the_key"},
        value("u", after=row(patient_id="from_the_value")),
    )
    assert statement.key == {"patient_id": "from_the_key"}


def test_missing_message_key_raises(patients):
    with pytest.raises(envelope.MalformedEnvelope, match="no message key"):
        envelope.translate(patients, None, value("c", after=row()))


def test_key_missing_a_primary_key_column_raises(patients):
    with pytest.raises(envelope.MalformedEnvelope, match="missing patient_id"):
        envelope.translate(patients, {"something_else": 1}, value("c", after=row()))


def test_create_without_an_after_image_raises(patients):
    with pytest.raises(envelope.MalformedEnvelope, match="no after image"):
        envelope.translate(patients, {"patient_id": "pt_9f2a"}, value("c", after=None))


def test_record_without_an_op_raises(patients):
    with pytest.raises(envelope.MalformedEnvelope, match="no op"):
        envelope.translate(patients, {"patient_id": "pt_9f2a"}, {"after": row()})


def test_truncate_halts_rather_than_being_skipped(patients):
    """Skipping a truncate leaves the sink holding every row the source dropped.

    A replica that looks right and is not is the worst outcome available here, so
    an operation with no correct handling stops the applier instead.
    """
    with pytest.raises(envelope.UnsupportedOperation, match="'t'"):
        envelope.translate(patients, {"patient_id": "pt_9f2a"}, value("t"))


def test_unknown_field_is_an_error_not_a_silent_drop(patients):
    """The mid-run schema evolution seam.

    Someone covers a new source column in the policy, M4 registers a new clean
    schema version, and a record arrives carrying a field the sink has no column
    for. Dropping it would mean the sink quietly stops tracking a column the
    policy has approved, so the applier is told and re-runs ensure_schema.
    """
    with pytest.raises(envelope.UnknownField, match="ensure_schema"):
        envelope.translate(
            patients,
            {"patient_id": "pt_9f2a"},
            value("u", after=row(newly_covered_column="whatever")),
        )


def test_commit_timestamp_is_read_from_the_source_block(patients):
    """`source.ts_ms` is the database commit time, not the transformer's clock."""
    assert envelope.commit_timestamp_ms(value("c", after=row(), ts_ms=1234567890123)) == 1234567890123


def test_commit_timestamp_is_none_without_a_source_block():
    assert envelope.commit_timestamp_ms({"op": "c"}) is None
