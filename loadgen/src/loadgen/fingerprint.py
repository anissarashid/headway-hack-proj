"""Turn a dataset -- generated or stored -- into a digest you can compare.

"Two runs with the same seed produce identical rows" is only a claim until
something checks it, and comparing two databases row by row is not a check
anyone runs twice. This module reduces a population to one hex digest, plus one
per table so that a mismatch says which table drifted.

Two things have to be neutralised before the comparison means anything:

*Surrogate keys.* ``patient_id`` and friends come from identity sequences, and a
sequence that has been used once does not go back on its own. Reseeding into the
same database gives identical rows with different ids. So the canonical form
drops the surrogate keys and expresses every foreign key as the parent's natural
key -- ``mrn`` for a patient, ``npi`` for a provider, and patient-plus-timestamp
for the tables that have no natural key of their own. ``seed.reset`` does rewind
the sequences, but the fingerprint must not depend on that having happened.

*Type round-tripping.* The generated dataset holds Python objects and the stored
one holds whatever the driver hands back. Both sides go through the same
normalisation -- UTC ISO-8601 for instants, fixed-scale strings for money -- so
a digest computed from memory can be compared against one computed from the
database, which is what proves the load itself did not mangle anything.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

UTC = timezone.utc

from .seed import CENTS, TABLES, Dataset

# Columns compared, per table: everything except the surrogate primary key and
# the foreign key ids, which are replaced by the reference columns below.
_CONTENT = {
    "providers": ("npi", "full_name", "specialty", "email", "created_at", "updated_at"),
    "patients": ("mrn", "first_name", "middle_name", "last_name", "date_of_birth", "ssn",
                 "email", "phone", "address_line1", "address_line2", "city", "state",
                 "postal_code", "created_at", "updated_at"),
    "appointments": ("scheduled_at", "checked_in_at", "completed_at", "duration_minutes",
                     "status", "location", "intake_answers", "created_at", "updated_at"),
    "claims": ("billed_amount", "allowed_amount", "paid_amount", "patient_responsibility",
               "diagnosis_codes", "procedure_code", "claim_status", "submitted_at",
               "adjudicated_at", "created_at", "updated_at"),
    "notes": ("note_type", "body", "authored_at", "signed_at", "is_amended",
              "created_at", "updated_at"),
}

_SORT_KEYS = {
    "providers": ("npi",),
    "patients": ("mrn",),
    "appointments": ("patient", "scheduled_at"),
    "claims": ("patient", "submitted_at"),
    "notes": ("patient", "authored_at"),
}


def _norm(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        # Fixed scale on both sides: numeric(12,2) comes back as 0.00, and an
        # unquantized 0 in memory would otherwise serialize differently.
        return str(value.quantize(CENTS))
    if isinstance(value, (list, tuple)):
        return [_norm(v) for v in value]
    if isinstance(value, dict):
        return {k: _norm(v) for k, v in value.items()}
    return value


def _appointment_key(patient_mrn: str, scheduled_at: Any) -> str:
    return f"{patient_mrn}@{_norm(scheduled_at)}"


def _note_key(patient_mrn: str, authored_at: Any) -> str:
    return f"{patient_mrn}@{_norm(authored_at)}"


def _sorted(table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = _SORT_KEYS[table]
    return sorted(rows, key=lambda r: tuple(str(r[k]) for k in keys))


# --------------------------------------------------------------------------
# from a generated dataset
# --------------------------------------------------------------------------


def canonical_dataset(ds: Dataset) -> dict[str, list[dict[str, Any]]]:
    mrn = [p["mrn"] for p in ds.patients]
    npi = [p["npi"] for p in ds.providers]
    appt_key = [_appointment_key(mrn[a["patient_ix"]], a["scheduled_at"]) for a in ds.appointments]
    note_key = [_note_key(mrn[n["patient_ix"]], n["authored_at"]) for n in ds.notes]

    out: dict[str, list[dict[str, Any]]] = {}
    out["providers"] = [{c: _norm(r[c]) for c in _CONTENT["providers"]} for r in ds.providers]
    out["patients"] = [{c: _norm(r[c]) for c in _CONTENT["patients"]} for r in ds.patients]
    out["appointments"] = [
        {"patient": mrn[r["patient_ix"]], "provider": npi[r["provider_ix"]],
         **{c: _norm(r[c]) for c in _CONTENT["appointments"]}}
        for r in ds.appointments
    ]
    out["claims"] = [
        {"patient": mrn[r["patient_ix"]],
         "appointment": None if r["appointment_ix"] is None else appt_key[r["appointment_ix"]],
         **{c: _norm(r[c]) for c in _CONTENT["claims"]}}
        for r in ds.claims
    ]
    out["notes"] = [
        {"patient": mrn[r["patient_ix"]], "provider": npi[r["provider_ix"]],
         "appointment": None if r["appointment_ix"] is None else appt_key[r["appointment_ix"]],
         "amends": None if r["amends_ix"] is None else note_key[r["amends_ix"]],
         **{c: _norm(r[c]) for c in _CONTENT["notes"]}}
        for r in ds.notes
    ]
    return {t: _sorted(t, out[t]) for t in TABLES}


# --------------------------------------------------------------------------
# from a live database
# --------------------------------------------------------------------------


def canonical_database(conn) -> dict[str, list[dict[str, Any]]]:
    """The same canonical form, read back out of Postgres.

    Read with plain SELECTs and joined in Python rather than in SQL: the join
    that resolves a foreign key to a natural key has to produce exactly what
    ``canonical_dataset`` produces, and one implementation of that mapping is
    easier to keep honest than two.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT patient_id, mrn FROM patients")
        mrn = dict(cur.fetchall())
        cur.execute("SELECT provider_id, npi FROM providers")
        npi = dict(cur.fetchall())
        cur.execute("SELECT appointment_id, patient_id, scheduled_at FROM appointments")
        appt_key = {a: _appointment_key(mrn[p], s) for a, p, s in cur.fetchall()}
        cur.execute("SELECT note_id, patient_id, authored_at FROM notes")
        note_key = {n: _note_key(mrn[p], t) for n, p, t in cur.fetchall()}

        out: dict[str, list[dict[str, Any]]] = {}
        for table in ("providers", "patients"):
            columns = _CONTENT[table]
            cur.execute(f"SELECT {', '.join(columns)} FROM {table}")
            out[table] = [dict(zip(columns, (_norm(v) for v in row))) for row in cur.fetchall()]

        columns = _CONTENT["appointments"]
        cur.execute(f"SELECT patient_id, provider_id, {', '.join(columns)} FROM appointments")
        out["appointments"] = [
            {"patient": mrn[row[0]], "provider": npi[row[1]],
             **dict(zip(columns, (_norm(v) for v in row[2:])))}
            for row in cur.fetchall()
        ]

        columns = _CONTENT["claims"]
        cur.execute(f"SELECT patient_id, appointment_id, {', '.join(columns)} FROM claims")
        out["claims"] = [
            {"patient": mrn[row[0]],
             "appointment": None if row[1] is None else appt_key[row[1]],
             **dict(zip(columns, (_norm(v) for v in row[2:])))}
            for row in cur.fetchall()
        ]

        columns = _CONTENT["notes"]
        cur.execute(
            f"SELECT patient_id, provider_id, appointment_id, amends_note_id, "
            f"{', '.join(columns)} FROM notes")
        out["notes"] = [
            {"patient": mrn[row[0]], "provider": npi[row[1]],
             "appointment": None if row[2] is None else appt_key[row[2]],
             "amends": None if row[3] is None else note_key[row[3]],
             **dict(zip(columns, (_norm(v) for v in row[4:])))}
            for row in cur.fetchall()
        ]

    return {t: _sorted(t, out[t]) for t in TABLES}


# --------------------------------------------------------------------------
# digests
# --------------------------------------------------------------------------


def _dumps(value: Any) -> str:
    # sort_keys because jsonb does not preserve the key order it was given, and
    # ensure_ascii=False so a unicode name hashes as the text it is.
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest(canonical: dict[str, list[dict[str, Any]]]) -> str:
    return hashlib.sha256(_dumps(canonical).encode("utf-8")).hexdigest()


def per_table_digests(canonical: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    """Short per-table digests, so a mismatch names the table that moved."""
    return {
        table: hashlib.sha256(_dumps(rows).encode("utf-8")).hexdigest()[:12]
        for table, rows in canonical.items()
    }
