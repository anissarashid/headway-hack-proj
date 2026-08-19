"""Ongoing churn against the clinic schema: the timeline a replay replays.

``seed.py`` gives one state. Point-in-time replay needs a *history* -- a stretch
of wall clock with distinguishable instants in it -- so this module keeps
mutating a seeded database: inserts, updates and deletes, at a configurable
rate, each in its own explicit transaction.

Three properties are the point.

*Every mutation lands in the ledger.* The ``<table>_history`` tables written by
the ``pit_audit`` trigger are the correctness oracle for M8 and the thing M6
reads to choose a meaningful ``T``. This module does not write that ledger --
the trigger does, inside the same transaction as the change, which is the only
way an oracle can be trusted -- but it does read back, per transaction, exactly
what the ledger recorded, compares that against what it meant to do, and reports
the difference. A ``DELETE`` of one appointment showing up as three tables' worth
of history entries is the cascade fan-out that makes replay hard; this is where
you can see it.

*Some transactions touch several tables at once.* A transaction that inserts a
patient, their first appointment and their intake note lands at a single
``tx_at``. A replay that stops halfway through it would show an appointment
belonging to a patient who does not exist, which is what ``--snap-to-txn`` is
for; without multi-table transactions in the data, that flag has nothing to be
right or wrong about. Every shape below declares which tables it touches, and
the summary counts how many transactions touched more than one.

*The volume is bounded.* Cleaned topics run at infinite retention on a laptop
PVC, so an unbounded generator fills the disk and takes the cluster with it.
Three independent ceilings apply and whichever is reached first stops the run:
wall-clock duration, transaction count, and total ledger rows produced. The
population is bounded too -- insert and delete weights are biased by how full
each table is relative to a band around its seeded size, so a long run churns
rather than grows.

Determinism is weaker here than in the seed, deliberately. Which mutation
happens next, and every value inside it, descends from ``config.SEED`` exactly as
the seed's rows do. *When* it happens does not: the rate is wall-clock paced and
``tx_at`` is real time, because a fake timeline cannot be resolved by
``offsets_for_times``. Two churn runs against the same seeded database therefore
apply the same plan at different instants.

    python -m loadgen                      # 5 minutes at 2 transactions/second
    python -m loadgen --duration 30s       # a short run that still hits every shape
    python -m loadgen --rate 8 --max-txns 500
    python -m loadgen --verify             # check the ledger, change nothing
"""

from __future__ import annotations

import argparse
import dataclasses
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Sequence

from psycopg.types.json import Json

from . import config, vocab
from .config import SeedConfig
from .seed import (
    APPOINTMENT_COLUMNS,
    CLAIM_COLUMNS,
    NOTE_COLUMNS,
    PATIENT_COLUMNS,
    PROVIDER_COLUMNS,
    TABLES,
    SeedError,
    _business_slot,
    _claim_row,
    _insert,
    _intake_answers,
    _note_body,
    _npi,
    _patients,
    _phone,
    _reprice,
    _stream,
    _weighted,
    table_counts,
)

# The row builders above are imported from the seed on purpose. Churn rows and
# seeded rows have to be shaped the same way -- the same SSN formats, the same
# awkward jsonb, the same quantized money -- or the de-identification policy
# would be exercised by the initial population and not by anything after it.
# There is one place where a column's value distribution is defined, and it is
# seed.py.

# (table, operation) pairs a shape says it issued itself. The ledger is the
# authority on what actually happened; this is what it gets checked against.
Direct = list[tuple[str, str]]

_IDENT = re.compile(r"[a-z_][a-z0-9_]*\Z")


class NoCandidate(Exception):
    """This shape has nothing to work on right now, so the transaction is dropped.

    Raised from inside the transaction block so psycopg rolls back: a shape that
    found its candidate row deleted by an earlier cascade must leave no trace at
    all, rather than commit a partial transaction whose ledger entries do not
    match what it claimed to do.
    """


# --------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ChurnConfig:
    """Rate, ceilings, and the band the population is kept inside.

    The ceilings are not advisory. ``max_ledger_rows`` in particular is the one
    that matters on a laptop: every ledger row becomes a CDC event, and cleaned
    topics are configured for infinite retention, so the disk cost of a run is
    proportional to this number and nothing else caps it.
    """

    seed: int = config.SEED

    # Transactions per second. Low by default: the interesting thing about the
    # timeline is that its instants are distinguishable, not that they are dense,
    # and two transactions sharing a `tx_at` to the microsecond would be two
    # points in time a replay cannot tell apart.
    rate: float = 2.0

    # Whichever of the three is reached first ends the run.
    duration: float = 300.0
    max_txns: int = 2000
    max_ledger_rows: int = 50_000

    # Multiples of the configured row counts. Above the top of the band inserts
    # back off and deletes are favoured; below the bottom, the reverse. The band
    # is anchored to `config.Counts`, not to the row counts observed at startup,
    # so running churn ten times in a row does not ratchet the database upward.
    band_low: float = 0.7
    band_high: float = 1.4

    # Cascading deletes invalidate cached ids, so the pool is reloaded after any
    # transaction that deleted anything, and unconditionally this often.
    refresh_every: int = 50

    # How many pre-generated patients churn draws new registrations from. Drawing
    # from a generated population rather than building a patient inline means the
    # guaranteed awkward cases -- null email, over-89, leading-zero zip, unicode
    # name -- turn up in churn-inserted rows too.
    patient_pool: int = 128

    # Run every shape once, in order, before going weighted-random, so a
    # thirty-second run still produces a cascade delete and a multi-table insert.
    warmup: bool = True

    def bands(self) -> dict[str, tuple[int, int]]:
        counts = dataclasses.asdict(config.DEFAULT.counts)
        # providers is left ungoverned: only `hire_provider` grows it, at a low
        # weight, and the transaction ceilings bound it well below anything that
        # matters.
        return {
            table: (round(counts[table] * self.band_low), round(counts[table] * self.band_high))
            for table in ("patients", "appointments", "claims", "notes")
        }


# --------------------------------------------------------------------------
# what the ledger said
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class LedgerEntry:
    table: str
    op: str
    pk: dict[str, Any]


@dataclass(frozen=True)
class TxnResult:
    shape: str
    txid: int
    tx_at: datetime
    direct: Direct
    entries: list[LedgerEntry]

    @property
    def cascaded(self) -> int:
        """Ledger rows this transaction caused but did not issue.

        Referential actions, not bookkeeping: deleting an appointment deletes its
        claims and sets ``notes.appointment_id`` to NULL, so one statement becomes
        history in three tables. A pipeline that only watches for deletes gets the
        notes wrong, which is the reason this number is reported rather than
        summed away.
        """
        return max(len(self.entries) - len(self.direct), 0)

    @property
    def tables(self) -> list[str]:
        seen: list[str] = []
        for entry in self.entries:
            if entry.table not in seen:
                seen.append(entry.table)
        return seen


# --------------------------------------------------------------------------
# candidate pool
# --------------------------------------------------------------------------


@dataclass
class Pool:
    """Ids worth mutating, cached so shape selection does not query per pick.

    Held in insertion (id) order rather than in a set, because the shape that
    picks from it does so with a seeded ``Random`` and set iteration order is not
    something a deterministic plan can depend on.
    """

    patients: list[int] = field(default_factory=list)
    providers: dict[int, str] = field(default_factory=dict)   # id -> full_name
    npis: set[str] = field(default_factory=set)
    appointments: dict[int, str] = field(default_factory=dict)  # id -> status
    claims: dict[int, str] = field(default_factory=dict)        # id -> claim_status
    notes: list[int] = field(default_factory=list)              # ids not yet amended
    next_mrn: int = 0

    def load(self, cur) -> None:
        cur.execute("SELECT patient_id FROM patients ORDER BY patient_id")
        self.patients = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT provider_id, full_name, npi FROM providers ORDER BY provider_id")
        rows = cur.fetchall()
        self.providers = {r[0]: r[1] for r in rows}
        self.npis = {r[2] for r in rows}

        cur.execute("SELECT appointment_id, status::text FROM appointments ORDER BY appointment_id")
        self.appointments = dict(cur.fetchall())

        cur.execute("SELECT claim_id, claim_status FROM claims ORDER BY claim_id")
        self.claims = dict(cur.fetchall())

        cur.execute("SELECT note_id FROM notes WHERE NOT is_amended ORDER BY note_id")
        self.notes = [r[0] for r in cur.fetchall()]

        # MRNs continue above the highest one already present instead of coming
        # from a namespace of their own. The unique index is the only thing that
        # cares, and continuing the sequence means a churned database is not
        # visibly divided into "seeded" and "churned" patients -- which it should
        # not be, because the pipeline must not be able to tell either.
        cur.execute("SELECT coalesce(max(substring(mrn from 5)::bigint), 0) "
                    "FROM patients WHERE mrn ~ '^MRN-[0-9]+$'")
        self.next_mrn = cur.fetchone()[0] + 1

    def take_mrn(self) -> str:
        mrn = f"MRN-{self.next_mrn:07d}"
        self.next_mrn += 1
        return mrn

    def with_status(self, kind: str, *wanted: str) -> list[int]:
        source = self.appointments if kind == "appointments" else self.claims
        return [i for i, status in source.items() if status in wanted]


def _ids(cur, sql: str, params: Sequence[Any] | None = None) -> list[int]:
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]


# --------------------------------------------------------------------------
# one transaction's working set
# --------------------------------------------------------------------------


class Streams:
    """One ``(Random, Faker)`` pair per shape, all derived from the seed.

    Per-shape rather than one shared stream, for the same reason the seed uses
    one per table: changing how a note is worded should not reshuffle every
    demographic correction that follows it.
    """

    def __init__(self, base: SeedConfig) -> None:
        self._base = base
        self._cache: dict[str, tuple[Any, Any]] = {}

    def __call__(self, name: str) -> tuple[Any, Any]:
        if name not in self._cache:
            self._cache[name] = _stream(self._base, f"churn:{name}")
        return self._cache[name]


@dataclass
class Txn:
    """Everything a shape is allowed to touch."""

    cur: Any
    cfg: SeedConfig          # the base config with as_of moved to this tx_at
    pool: Pool
    streams: Streams
    patient_pool: list[dict[str, Any]]
    txid: int
    tx_at: datetime


# --------------------------------------------------------------------------
# statement helpers
# --------------------------------------------------------------------------


def _pick(rng, candidates: Sequence[int], what: str) -> int:
    if not candidates:
        raise NoCandidate(f"no {what}")
    return rng.choice(list(candidates))


def _fetch(cur, table: str, pk_col: str, pk: int, columns: Sequence[str]) -> dict[str, Any]:
    cur.execute(f"SELECT {', '.join(columns)} FROM {table} WHERE {pk_col} = %s", (pk,))
    row = cur.fetchone()
    if row is None:
        raise NoCandidate(f"{table}.{pk_col}={pk} is gone")
    return dict(zip(columns, row))


def _update(cur, table: str, pk_col: str, pk: int, values: dict[str, Any]) -> None:
    """Update exactly one row, or drop the transaction.

    ``updated_at`` is deliberately absent from every caller: the
    ``pit_touch_updated_at`` trigger maintains it, and the audit trigger fires
    after that one, so the recorded ``after_row`` carries the new value. A writer
    that set it by hand would be asserting something the database already
    guarantees.

    The ``RETURNING`` is not decoration. An ``UPDATE`` that matches nothing is a
    silent no-op, and a transaction that claims to have updated a row but wrote
    no ledger entry is exactly the discrepancy this module exists to notice.
    """
    assignments = ", ".join(f"{c} = %({c})s" for c in values)
    cur.execute(
        f"UPDATE {table} SET {assignments} WHERE {pk_col} = %(__pk)s RETURNING {pk_col}",
        {**values, "__pk": pk},
    )
    if cur.fetchone() is None:
        raise NoCandidate(f"{table}.{pk_col}={pk} vanished before it could be updated")


def _delete(cur, table: str, pk_col: str, pk: int) -> None:
    cur.execute(f"DELETE FROM {table} WHERE {pk_col} = %s RETURNING {pk_col}", (pk,))
    if cur.fetchone() is None:
        raise NoCandidate(f"{table}.{pk_col}={pk} was already gone")


def _json(doc: dict[str, Any] | None) -> Json | None:
    return None if doc is None else Json(doc)


_PATIENT_PHI = ("first_name", "middle_name", "last_name", "date_of_birth", "mrn",
                "address_line1", "city", "state", "postal_code", "phone")


def _write_note(t: Txn, rng, fake, *, patient_id: int, provider_id: int,
                appointment_id: int | None, note_type: str,
                amends_note_id: int | None = None,
                patient: dict[str, Any] | None = None) -> int:
    """Insert one note, with PHI in the prose the way the seed writes it.

    The body needs the patient's real demographics, so an existing patient's row
    is read back rather than invented: a note whose names and dates do not match
    the columns they came from would let a de-identification policy pass by
    coincidence.
    """
    patient = patient or _fetch(t.cur, "patients", "patient_id", patient_id, _PATIENT_PHI)
    full_name = t.pool.providers.get(provider_id)
    if full_name is None:
        full_name = _fetch(t.cur, "providers", "provider_id", provider_id, ("full_name",))["full_name"]
    shell = {"note_type": note_type, "authored_at": t.tx_at}
    body = _note_body(rng, fake, t.cfg, shell, patient, {"full_name": full_name})
    row = {
        "patient_id": patient_id,
        "provider_id": provider_id,
        "appointment_id": appointment_id,
        "amends_note_id": amends_note_id,
        "note_type": note_type,
        "body": body,
        "authored_at": t.tx_at,
        # Signed in the same transaction or left unsigned. A signed_at drawn
        # hours ahead of tx_at would be a timestamp from the future, and the
        # whole point of the timeline is that nothing in it is.
        "signed_at": t.tx_at if rng.random() < 0.72 else None,
        "is_amended": False,
        "created_at": t.tx_at,
        "updated_at": t.tx_at,
    }
    note_id = _insert(t.cur, "notes", NOTE_COLUMNS, [row], "note_id")[0]
    t.pool.notes.append(note_id)
    return note_id


def _write_claim(t: Txn, rng, *, patient_id: int, appointment_id: int | None) -> int:
    """A freshly submitted claim: unadjudicated, so a later transaction can adjudicate it.

    That two-step arc is the reason this is not just an insert. A claim whose
    money is filled in on arrival has one version and tells a point-in-time query
    nothing; a claim that is submitted now and paid four transactions later has
    two, and the answer at ``T`` depends on which side of the adjudication ``T``
    falls.
    """
    row = _claim_row(rng, t.cfg, 0, None, t.tx_at)
    row["claim_status"] = _weighted(rng, [("submitted", 0.78), ("pending", 0.22)])
    row["adjudicated_at"] = None
    _reprice(row, rng)
    row.pop("patient_ix")
    row.pop("appointment_ix")
    row["patient_id"] = patient_id
    row["appointment_id"] = appointment_id
    claim_id = _insert(t.cur, "claims", CLAIM_COLUMNS, [row], "claim_id")[0]
    t.pool.claims[claim_id] = row["claim_status"]
    return claim_id


# --------------------------------------------------------------------------
# the shapes
# --------------------------------------------------------------------------


def _register_patient(t: Txn) -> Direct:
    """A new patient, their first appointment and the intake note: three tables, one tx_at.

    The canonical ``--snap-to-txn`` case. There is no instant at which this
    patient exists without their appointment, so a replay that produces one has
    stopped inside a transaction rather than between two.
    """
    rng, fake = t.streams("register_patient")
    row = dict(rng.choice(t.patient_pool))
    row["mrn"] = t.pool.take_mrn()
    row["created_at"] = row["updated_at"] = t.tx_at
    patient_id = _insert(t.cur, "patients", PATIENT_COLUMNS, [row], "patient_id")[0]
    t.pool.patients.append(patient_id)

    provider_id = _pick(rng, list(t.pool.providers), "providers")
    appointment_id = _book(t, rng, fake, patient_id, provider_id,
                           phone=row["phone"], first_visit=True)
    _write_note(t, rng, fake, patient_id=patient_id, provider_id=provider_id,
                appointment_id=appointment_id, note_type="intake", patient=row)
    return [("patients", "I"), ("appointments", "I"), ("notes", "I")]


def _book(t: Txn, rng, fake, patient_id: int, provider_id: int,
          phone: str | None, first_visit: bool) -> int:
    lead = timedelta(days=1 if first_visit else rng.randrange(1, 60))
    row = {
        "patient_id": patient_id,
        "provider_id": provider_id,
        "scheduled_at": _business_slot(rng, t.tx_at + lead, t.tx_at + lead + timedelta(days=30)),
        "checked_in_at": None,
        "completed_at": None,
        "duration_minutes": _weighted(rng, [(15, 0.10), (20, 0.14), (30, 0.46), (45, 0.20), (60, 0.10)]),
        "status": "scheduled",
        "location": None if rng.random() < 0.08 else rng.choice(vocab.CLINIC_LOCATIONS),
        "intake_answers": _json(_intake_answers(rng, fake, phone)),
        "created_at": t.tx_at,
        "updated_at": t.tx_at,
    }
    appointment_id = _insert(t.cur, "appointments", APPOINTMENT_COLUMNS, [row], "appointment_id")[0]
    t.pool.appointments[appointment_id] = "scheduled"
    return appointment_id


def _book_appointment(t: Txn) -> Direct:
    """An existing patient books a visit. One table, one row: the common case."""
    rng, fake = t.streams("book_appointment")
    patient_id = _pick(rng, t.pool.patients, "patients")
    patient = _fetch(t.cur, "patients", "patient_id", patient_id, ("phone",))
    provider_id = _pick(rng, list(t.pool.providers), "providers")
    _book(t, rng, fake, patient_id, provider_id, patient["phone"], first_visit=False)
    return [("appointments", "I")]


def _check_in(t: Txn) -> Direct:
    """scheduled -> checked_in. The first of two updates to the same row."""
    rng, _ = t.streams("check_in")
    appointment_id = _pick(rng, t.pool.with_status("appointments", "scheduled"),
                           "scheduled appointments")
    _update(t.cur, "appointments", "appointment_id", appointment_id,
            {"status": "checked_in", "checked_in_at": t.tx_at})
    t.pool.appointments[appointment_id] = "checked_in"
    return [("appointments", "U")]


def _complete_visit(t: Txn) -> Direct:
    """The visit happens: the appointment closes, a note is written, a claim goes out.

    Three tables in one transaction, and the one that produces the most versions
    per row over a run -- an appointment that was scheduled, then checked in,
    then completed has three ledger entries and three different answers depending
    on ``T``.
    """
    rng, fake = t.streams("complete_visit")
    appointment_id = _pick(rng, t.pool.with_status("appointments", "checked_in"),
                           "checked-in appointments")
    appt = _fetch(t.cur, "appointments", "appointment_id", appointment_id,
                  ("patient_id", "provider_id", "checked_in_at", "duration_minutes"))

    completed = appt["checked_in_at"] + timedelta(minutes=appt["duration_minutes"] + rng.randrange(-5, 16))
    # Nothing in the timeline may be stamped ahead of the transaction writing it,
    # and completed_at has to stay strictly after checked_in_at for the row to
    # mean anything.
    completed = min(completed, t.tx_at)
    completed = max(completed, appt["checked_in_at"] + timedelta(seconds=1))

    _update(t.cur, "appointments", "appointment_id", appointment_id,
            {"status": "completed", "completed_at": completed})
    t.pool.appointments[appointment_id] = "completed"

    _write_note(t, rng, fake, patient_id=appt["patient_id"], provider_id=appt["provider_id"],
                appointment_id=appointment_id, note_type="progress")
    _write_claim(t, rng, patient_id=appt["patient_id"], appointment_id=appointment_id)
    return [("appointments", "U"), ("notes", "I"), ("claims", "I")]


def _adjudicate_claim(t: Txn) -> Direct:
    """A claim moves along: money appears, or it is denied. One row, one table."""
    rng, _ = t.streams("adjudicate_claim")
    claim_id = _pick(rng, t.pool.with_status("claims", "submitted", "pending"),
                     "open claims")
    claim = _fetch(t.cur, "claims", "claim_id", claim_id, ("billed_amount", "claim_status"))
    if claim["claim_status"] == "submitted":
        status = _weighted(rng, [("pending", 0.34), ("paid", 0.38), ("denied", 0.19), ("appealed", 0.09)])
    else:
        status = _weighted(rng, [("paid", 0.60), ("denied", 0.28), ("appealed", 0.12)])

    row = {"billed_amount": claim["billed_amount"], "claim_status": status}
    _reprice(row, rng)
    row["adjudicated_at"] = None if status == "pending" else t.tx_at
    row.pop("billed_amount")
    _update(t.cur, "claims", "claim_id", claim_id, row)
    t.pool.claims[claim_id] = status
    return [("claims", "U")]


def _batch_adjudication(t: Txn) -> Direct:
    """One statement, many rows: the clearinghouse acknowledges a batch.

    Every row lands at the same ``tx_at``, so a replay either sees the whole
    batch or none of it. A per-row applier that is not transaction-aware will
    happily stop in the middle of this and be wrong about a dozen claims at once,
    which single-row updates never expose.
    """
    rng, _ = t.streams("batch_adjudication")
    open_claims = t.pool.with_status("claims", "submitted")
    if len(open_claims) < 4:
        raise NoCandidate("not enough submitted claims for a batch")
    size = min(len(open_claims), rng.randrange(5, 21))
    batch = sorted(rng.sample(open_claims, size))
    t.cur.execute(
        "UPDATE claims SET claim_status = 'pending' WHERE claim_id = ANY(%s) RETURNING claim_id",
        (batch,),
    )
    touched = [r[0] for r in t.cur.fetchall()]
    if not touched:
        raise NoCandidate("the batch was gone by the time it was updated")
    for claim_id in touched:
        t.pool.claims[claim_id] = "pending"
    return [("claims", "U")] * len(touched)


def _amend_note(t: Txn) -> Direct:
    """An addendum, plus the note it supersedes flipped to amended.

    Two rows in one table, and a self-referencing foreign key resolved inside the
    transaction: the addendum cannot point at the note it amends until that note
    has an id, and the flag on the earlier note cannot be set until the addendum
    exists to justify it.
    """
    rng, fake = t.streams("amend_note")
    note_id = _pick(rng, t.pool.notes, "unamended notes")
    note = _fetch(t.cur, "notes", "note_id", note_id,
                  ("patient_id", "provider_id", "appointment_id", "is_amended"))
    if note["is_amended"]:
        raise NoCandidate(f"notes.note_id={note_id} was already amended")

    _write_note(t, rng, fake, patient_id=note["patient_id"], provider_id=note["provider_id"],
                appointment_id=note["appointment_id"], note_type="addendum",
                amends_note_id=note_id)
    _update(t.cur, "notes", "note_id", note_id, {"is_amended": True})
    if note_id in t.pool.notes:
        t.pool.notes.remove(note_id)
    return [("notes", "I"), ("notes", "U")]


def _correct_demographics(t: Txn) -> Direct:
    """A patient's details change. The old value still has to be the answer at an earlier T.

    The single most load-bearing shape for the de-identification policy. If a
    phone number is tokenized, the same patient now has two tokens and the
    pipeline has to be right about which one a query at ``T`` should see.
    """
    rng, fake = t.streams("correct_demographics")
    patient_id = _pick(rng, t.pool.patients, "patients")
    kind = _weighted(rng, [("contact", 0.48), ("address", 0.32), ("name", 0.20)])

    if kind == "contact":
        values: dict[str, Any] = {"phone": _phone(rng)}
        if rng.random() < 0.45:
            handle = fake.user_name()
            values["email"] = f"{handle}@{rng.choice(vocab.EMAIL_DOMAINS)}"
    elif kind == "address":
        # Moved. city, state and postal_code change together or the zip stops
        # belonging to its state, and zip3 is the only geography Safe Harbor
        # leaves behind -- a zip that does not match its state makes that
        # generalization meaningless.
        state = fake.state_abbr(include_territories=False, include_freely_associated_states=False)
        values = {
            "address_line1": fake.street_address(),
            "address_line2": fake.secondary_address() if rng.random() < 0.2 else None,
            "city": fake.city(),
            "state": state,
            "postal_code": fake.postcode_in_state(state),
        }
    else:
        # A surname changes and the email does not follow it. Stale derived data
        # is normal in a real primary and is a quasi-identifier a policy that
        # only looks at one column will miss.
        values = {"last_name": fake.last_name()}

    _update(t.cur, "patients", "patient_id", patient_id, values)
    return [("patients", "U")]


def _reschedule(t: Txn) -> Direct:
    """A booked visit moves, or is cancelled."""
    rng, _ = t.streams("reschedule")
    appointment_id = _pick(rng, t.pool.with_status("appointments", "scheduled"),
                           "scheduled appointments")
    if rng.random() < 0.68:
        shift = timedelta(days=rng.randrange(1, 21))
        values = {"scheduled_at": _business_slot(rng, t.tx_at + shift,
                                                 t.tx_at + shift + timedelta(days=14))}
    else:
        values = {"status": "cancelled"}
        t.pool.appointments[appointment_id] = "cancelled"
    _update(t.cur, "appointments", "appointment_id", appointment_id, values)
    return [("appointments", "U")]


def _update_provider(t: Txn) -> Direct:
    """The provider directory changes. Small table, still captured, still a quasi-identifier."""
    rng, fake = t.streams("update_provider")
    provider_id = _pick(rng, list(t.pool.providers), "providers")
    if rng.random() < 0.5:
        values: dict[str, Any] = {"specialty": rng.choice(vocab.SPECIALTIES)}
    else:
        values = {"email": f"{fake.user_name()}@clinic.example.org"}
    _update(t.cur, "providers", "provider_id", provider_id, values)
    return [("providers", "U")]


def _hire_provider(t: Txn) -> Direct:
    """A new provider. Rare, and it never fans out: every FK to providers is RESTRICT."""
    rng, fake = t.streams("hire_provider")
    npi = _npi(rng)
    attempts = 0
    while npi in t.pool.npis:
        npi = _npi(rng)
        attempts += 1
        if attempts > 50:
            raise NoCandidate("could not draw an unused NPI")
    first, last = fake.first_name(), fake.last_name()
    full_name = f"Dr. {first} {last}, {rng.choice(vocab.CREDENTIALS)}"
    row = {
        "npi": npi,
        "full_name": full_name,
        "specialty": rng.choice(vocab.SPECIALTIES),
        "email": f"{fake.user_name()}@clinic.example.org",
        "created_at": t.tx_at,
        "updated_at": t.tx_at,
    }
    provider_id = _insert(t.cur, "providers", PROVIDER_COLUMNS, [row], "provider_id")[0]
    t.pool.providers[provider_id] = full_name
    t.pool.npis.add(npi)
    return [("providers", "I")]


def _detach_appointment(t: Txn) -> Direct:
    """Delete one appointment; watch it become three tables in the ledger.

    ``claims.appointment_id`` is ON DELETE CASCADE, so the claims disappear.
    ``notes.appointment_id`` is ON DELETE SET NULL, so the notes are *updated*
    instead -- a pipeline that only watches for deletes silently keeps notes
    pointing at an appointment that no longer exists. The candidate is chosen to
    guarantee both happen rather than hoping for it.
    """
    rng, _ = t.streams("detach_appointment")
    candidates = _ids(t.cur, """
        SELECT a.appointment_id
          FROM appointments a
         WHERE EXISTS (SELECT 1 FROM claims c WHERE c.appointment_id = a.appointment_id)
           AND EXISTS (SELECT 1 FROM notes  n WHERE n.appointment_id = a.appointment_id)
         ORDER BY a.appointment_id
    """)
    appointment_id = _pick(rng, candidates, "appointments with both a claim and a note")
    _delete(t.cur, "appointments", "appointment_id", appointment_id)
    return [("appointments", "D")]


def _purge_patient(t: Txn) -> Direct:
    """One DELETE, mutations in four tables. The erasure request.

    Bounded to a patient with a small footprint: the fan-out is the point, but a
    heavy utilizer with sixty visits would take a visible slice of the population
    with them and skew the rest of the run.
    """
    rng, _ = t.streams("purge_patient")
    candidates = _ids(t.cur, """
        SELECT p.patient_id
          FROM patients p
          LEFT JOIN appointments a ON a.patient_id = p.patient_id
         GROUP BY p.patient_id
        HAVING count(a.appointment_id) BETWEEN 1 AND 5
         ORDER BY p.patient_id
    """)
    patient_id = _pick(rng, candidates, "patients with a small footprint")
    _delete(t.cur, "patients", "patient_id", patient_id)
    return [("patients", "D")]


@dataclass(frozen=True)
class Shape:
    name: str
    weight: float
    # "insert" grows the population, "delete" shrinks it, "update" leaves the row
    # count alone. Only used to bias the weights against the population band.
    kind: str
    apply: Callable[[Txn], Direct]


# Declaration order is also the warmup order, and it is a clinical arc: a patient
# registers, books, arrives, is seen, is billed, is paid. Every shape after that
# needs something an earlier one produced, so a short run reaches all of them.
SHAPES: tuple[Shape, ...] = (
    Shape("register_patient", 4.0, "insert", _register_patient),
    Shape("book_appointment", 14.0, "insert", _book_appointment),
    Shape("check_in", 12.0, "update", _check_in),
    Shape("complete_visit", 12.0, "insert", _complete_visit),
    Shape("adjudicate_claim", 10.0, "update", _adjudicate_claim),
    Shape("batch_adjudication", 2.0, "update", _batch_adjudication),
    Shape("amend_note", 5.0, "insert", _amend_note),
    Shape("correct_demographics", 10.0, "update", _correct_demographics),
    Shape("reschedule", 8.0, "update", _reschedule),
    Shape("update_provider", 2.0, "update", _update_provider),
    Shape("hire_provider", 0.5, "insert", _hire_provider),
    Shape("detach_appointment", 4.0, "delete", _detach_appointment),
    Shape("purge_patient", 1.5, "delete", _purge_patient),
)

def _fill(counts: dict[str, int], bands: dict[str, tuple[int, int]]) -> float:
    """How full the population is: 0.0 at the bottom of the band, 1.0 at the top.

    The fullest governed table decides, not the average. One table running away
    is the failure mode worth stopping, and an average lets it hide behind four
    others that are fine.
    """
    return max(
        (counts.get(table, 0) - low) / max(high - low, 1)
        for table, (low, high) in bands.items()
    )


def _weights(fill: float) -> dict[str, float]:
    """Multipliers per shape kind, so a long run churns instead of growing.

    Hard-clamped outside the band rather than smoothly damped: the point of the
    band is that the database cannot run away in either direction, and a soft
    bias that only mostly holds is the one that fills the PVC overnight.
    """
    if fill > 1.0:
        return {"insert": 0.1, "update": 1.0, "delete": 3.0}
    if fill < 0.0:
        return {"insert": 3.0, "update": 1.0, "delete": 0.0}
    return {"insert": 1.5 - fill, "update": 1.0, "delete": 0.25 + fill}


# --------------------------------------------------------------------------
# the ledger read-back
# --------------------------------------------------------------------------


def captured_tables(conn) -> list[str]:
    """Which tables are captured, according to the database rather than this file.

    Read from ``pit_captured_tables`` so a table added to the schema shows up in
    the ledger read-back without anybody remembering to edit a list here.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM pit_captured_tables ORDER BY table_name")
        tables = [r[0] for r in cur.fetchall()]
    bad = [t for t in tables if not _IDENT.match(t)]
    if bad:
        raise SeedError(f"refusing to interpolate table names that are not plain identifiers: {bad}")
    if not tables:
        raise SeedError("no captured tables: the history triggers are not installed")
    return tables


def _ledger_sql(tables: Sequence[str]) -> str:
    parts = [
        f"SELECT '{t}' AS table_name, history_id, op, pk, stmt_at "
        f"FROM {t}_history WHERE txid = %(txid)s"
        for t in tables
    ]
    # Ordered by statement, then table, then insertion, so the read-back reads
    # in the order the transaction actually happened rather than in table order.
    return " UNION ALL ".join(parts) + " ORDER BY stmt_at, table_name, history_id"


def _read_ledger(cur, sql: str, txid: int) -> list[LedgerEntry]:
    cur.execute(sql, {"txid": txid})
    return [LedgerEntry(table=r[0], op=r[2], pk=r[3]) for r in cur.fetchall()]


# --------------------------------------------------------------------------
# the loop
# --------------------------------------------------------------------------


@dataclass
class Totals:
    txns: int = 0
    ledger_rows: int = 0
    cascaded: int = 0
    multi_table: int = 0
    skipped: int = 0
    by_shape: dict[str, int] = field(default_factory=dict)
    by_table_op: dict[tuple[str, str], int] = field(default_factory=dict)
    instants: set[datetime] = field(default_factory=set)
    first_tx_at: datetime | None = None
    last_tx_at: datetime | None = None
    # A single writer starting each transaction only after the previous one
    # committed makes tx_at strictly increasing. That is a property of *one*
    # writer, not of the ledger, so it is checked rather than assumed: a second
    # churn process against the same database would break it, and a T chosen
    # between two out-of-order transactions would resolve to the wrong state.
    out_of_order: int = 0

    def record(self, result: TxnResult) -> None:
        self.txns += 1
        self.ledger_rows += len(result.entries)
        self.cascaded += result.cascaded
        if len(result.tables) > 1:
            self.multi_table += 1
        self.by_shape[result.shape] = self.by_shape.get(result.shape, 0) + 1
        for entry in result.entries:
            key = (entry.table, entry.op)
            self.by_table_op[key] = self.by_table_op.get(key, 0) + 1
        if self.last_tx_at is not None and result.tx_at <= self.last_tx_at:
            self.out_of_order += 1
        if self.first_tx_at is None:
            self.first_tx_at = result.tx_at
        self.last_tx_at = result.tx_at
        self.instants.add(result.tx_at)


def _one_txn(conn, shape: Shape, base: SeedConfig, pool: Pool, streams: Streams,
             patient_pool: list[dict], ledger_sql: str) -> TxnResult | None:
    """Apply one shape in one explicit transaction, then read back what the ledger got.

    ``pg_current_xact_id()`` is called first so the transaction's identity and
    its ``transaction_timestamp()`` are known before anything is written: every
    timestamp the shape stores is derived from that one instant, which is the
    same instant the audit trigger stamps into ``tx_at``. Nothing here reads the
    clock independently, so no row can carry a timestamp the ledger disagrees
    with.

    Returns ``None`` when the shape had nothing to work on. The transaction rolls
    back in that case and leaves no ledger entries, because a dropped
    transaction has to be indistinguishable from one that never started.
    """
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("SELECT pg_current_xact_id()::text::bigint, transaction_timestamp()")
            txid, tx_at = cur.fetchone()
            t = Txn(
                cur=cur,
                # as_of moves to this transaction's instant so the row builders
                # borrowed from the seed treat "now" as now rather than as the
                # seed's fixed 2026-08-01.
                cfg=dataclasses.replace(base, as_of=tx_at),
                pool=pool,
                streams=streams,
                patient_pool=patient_pool,
                txid=txid,
                tx_at=tx_at,
            )
            direct = shape.apply(t)
            entries = _read_ledger(cur, ledger_sql, txid)
            if not entries:
                # The shape reported work but the trigger recorded nothing, which
                # means the ledger is not capturing a table it is supposed to.
                # Rolling back is the only safe response: committing would leave
                # a mutation the oracle cannot see.
                raise SeedError(
                    f"{shape.name} mutated {len(direct)} row(s) but wrote no ledger entries; "
                    "the pit_audit trigger is missing on a table it claims to touch"
                )
    except NoCandidate:
        return None
    return TxnResult(shape=shape.name, txid=txid, tx_at=tx_at, direct=direct, entries=entries)


def run(conn, cfg: ChurnConfig, out=sys.stdout, quiet: bool = False) -> Totals:
    """Churn until one of the ceilings is reached, reporting as it goes."""
    # Each mutation must be its own transaction, and psycopg only turns a
    # `conn.transaction()` block into a real BEGIN/COMMIT when the connection is
    # in autocommit mode. Without it the first read opens an implicit
    # transaction, every transaction block after it degrades to a savepoint
    # inside that one, and the whole run commits as a single transaction at a
    # single tx_at -- a timeline with exactly one point in it. That failure is
    # silent and it invalidates everything downstream, so it is checked rather
    # than trusted to the caller.
    if not conn.autocommit:
        raise SeedError(
            "churn needs a connection in autocommit mode, so that each transaction block "
            "is a real BEGIN/COMMIT and not a savepoint inside one long-running transaction"
        )

    base = dataclasses.replace(config.DEFAULT, seed=cfg.seed)
    streams = Streams(base)

    before = table_counts(conn)
    if not before.get("patients"):
        raise SeedError("nothing to churn: the clinic tables are empty. Run the seed first.")

    tables = captured_tables(conn)
    ledger_sql = _ledger_sql(tables)
    bands = cfg.bands()

    # A generated population, not a hand-rolled patient: the guaranteed awkward
    # cases come with it, so churn-inserted patients are as hard to de-identify
    # as seeded ones.
    pool_cfg = dataclasses.replace(
        base,
        seed=cfg.seed + 1,
        counts=dataclasses.replace(base.counts, patients=cfg.patient_pool),
    )
    patient_pool = _patients(pool_cfg)

    pool = Pool()
    with conn.cursor() as cur:
        pool.load(cur)

    plan_rng, _ = _stream(base, "churn:plan")
    warmup = list(SHAPES) if cfg.warmup else []
    totals = Totals()

    if not quiet:
        print(f"churn: seed {cfg.seed}, {cfg.rate:g} txn/s, stopping at whichever comes first of "
              f"{_fmt(cfg.duration)}, {cfg.max_txns} transactions, {cfg.max_ledger_rows} ledger rows",
              file=out)
        print("  before   " + _counts_line(before), file=out)
        print(file=out)

    started = time.monotonic()
    stop = "duration reached"
    counts = dict(before)
    stale = True
    attempt = 0

    try:
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= cfg.duration:
                stop = f"duration reached after {_fmt(elapsed)}"
                break
            if totals.txns >= cfg.max_txns:
                stop = f"transaction ceiling reached ({cfg.max_txns})"
                break
            if totals.ledger_rows >= cfg.max_ledger_rows:
                stop = f"ledger ceiling reached ({totals.ledger_rows} rows)"
                break

            if stale or totals.txns % cfg.refresh_every == 0:
                with conn.cursor() as cur:
                    pool.load(cur)
                counts = table_counts(conn)
                stale = False

            if warmup:
                shape = warmup.pop(0)
            else:
                fill = _fill(counts, bands)
                multiplier = _weights(fill)
                shape = _weighted(plan_rng, [(s, s.weight * multiplier[s.kind]) for s in SHAPES])

            result = _one_txn(conn, shape, base, pool, streams, patient_pool, ledger_sql)
            attempt += 1
            if result is None:
                totals.skipped += 1
                # A shape with no candidate does not consume a slot in the paced
                # schedule; it also must not spin, so the pool is reloaded before
                # the next pick.
                stale = True
                if totals.skipped > 50 + attempt // 2:
                    stop = f"too many shapes had nothing to work on ({totals.skipped} skipped)"
                    break
                continue

            totals.record(result)
            counts = _apply_delta(counts, result)
            if any(e.op == "D" for e in result.entries):
                stale = True
            if not quiet:
                print(_txn_line(totals.txns, result), file=out)

            # Pace against the schedule rather than sleeping a fixed interval, so
            # a slow transaction is absorbed instead of compounding.
            deadline = started + totals.txns / cfg.rate
            slack = deadline - time.monotonic()
            if slack > 0:
                time.sleep(slack)
    except KeyboardInterrupt:
        stop = f"interrupted after {_fmt(time.monotonic() - started)}"

    after = table_counts(conn)
    if not quiet:
        print(file=out)
    _report(out, stop, before, after, totals, tables)
    return totals


def _apply_delta(counts: dict[str, int], result: TxnResult) -> dict[str, int]:
    """Keep the population estimate current between full recounts."""
    out = dict(counts)
    for entry in result.entries:
        if entry.table not in out:
            continue
        if entry.op == "I":
            out[entry.table] += 1
        elif entry.op == "D":
            out[entry.table] -= 1
    return out


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{int(seconds) // 60}m{int(seconds) % 60:02d}s"


def _counts_line(counts: dict[str, int]) -> str:
    return "  ".join(f"{t} {counts.get(t, 0)}" for t in TABLES)


def _txn_line(n: int, result: TxnResult) -> str:
    stamp = result.tx_at.strftime("%H:%M:%S.%f")
    rows = len(result.entries)
    detail = " ".join(f"{table}·{''.join(sorted({e.op for e in result.entries if e.table == table}))}"
                      for table in result.tables)
    extra = f"  +{result.cascaded} cascaded" if result.cascaded else ""
    return (f"  #{n:<5} txid {result.txid:<9} {stamp}  {result.shape:<22}"
            f"{rows:>4} row{'s' if rows != 1 else ' '}  {detail}{extra}")


def _report(out, stop: str, before: dict[str, int], after: dict[str, int],
            totals: Totals, tables: Sequence[str]) -> None:
    print(f"  stopped  {stop}", file=out)
    print("  before   " + _counts_line(before), file=out)
    print("  after    " + _counts_line(after), file=out)
    delta = "  ".join(f"{t} {after.get(t, 0) - before.get(t, 0):+d}" for t in TABLES)
    print("  delta    " + delta, file=out)
    print(f"  txns     {totals.txns}  ({totals.multi_table} multi-table, "
          f"{totals.skipped} dropped for want of a candidate)", file=out)
    print(f"  ledger   {totals.ledger_rows} rows, {totals.cascaded} of them cascaded", file=out)
    for table in tables:
        ops = {op: n for (t, op), n in sorted(totals.by_table_op.items()) if t == table}
        if ops:
            print(f"    {table:<14} " + "  ".join(f"{op} {n}" for op, n in sorted(ops.items())),
                  file=out)
    if totals.first_tx_at is not None:
        print(f"  timeline {totals.first_tx_at.isoformat()} .. {totals.last_tx_at.isoformat()}",
              file=out)
        print(f"           {len(totals.instants)} distinct instants for {totals.txns} transactions, "
              f"{totals.out_of_order} out of order", file=out)
    if totals.by_shape:
        print("  shapes   " + "  ".join(f"{name} {n}" for name, n in sorted(totals.by_shape.items())),
              file=out)


# --------------------------------------------------------------------------
# verification: is the ledger actually an oracle?
# --------------------------------------------------------------------------


@dataclass
class TableCheck:
    table: str
    entries: int
    txns: int
    instants: int
    inversions: int
    split_txns: int
    expected_rows: int
    actual_rows: int
    only_expected: int
    only_actual: int

    @property
    def ok(self) -> bool:
        return (self.inversions == 0 and self.split_txns == 0
                and self.only_expected == 0 and self.only_actual == 0
                and self.expected_rows == self.actual_rows)

    def problems(self) -> list[str]:
        out = []
        if self.inversions:
            out.append(f"{self.inversions} entries whose tx_at goes backwards against history_id")
        if self.split_txns:
            out.append(f"{self.split_txns} transactions recorded at more than one tx_at")
        if self.expected_rows != self.actual_rows:
            out.append(f"replay yields {self.expected_rows} rows but the table holds {self.actual_rows}")
        if self.only_expected or self.only_actual:
            out.append(f"{self.only_expected} rows only in the replay, {self.only_actual} only in the table")
        return out


def check_table(cur, table: str) -> TableCheck:
    """Everything the ledger has to be true of, for one table.

    The last check is the whole reason the ledger exists: replay it to its own
    newest ``tx_at`` -- for each key, the ``after_row`` of the newest entry at or
    before ``T``, absent if that entry was a delete -- and the result has to be
    the live table, row for row. That is precisely the query M8 will run at an
    arbitrary ``T``; running it at ``T = now`` is the one case where the answer
    can be checked against something independent.
    """
    hist = f"{table}_history"

    cur.execute(f"SELECT count(*), count(DISTINCT txid), count(DISTINCT tx_at) FROM {hist}")
    entries, txns, instants = cur.fetchone()

    # Append-only means history_id order and tx_at order agree. They can be equal
    # -- every row of one transaction shares a tx_at -- but never inverted.
    cur.execute(f"""
        SELECT count(*) FROM (
          SELECT tx_at, lag(tx_at) OVER (ORDER BY history_id) AS prev FROM {hist}
        ) s WHERE prev IS NOT NULL AND tx_at < prev
    """)
    inversions = cur.fetchone()[0]

    cur.execute(f"""
        SELECT count(*) FROM (
          SELECT txid FROM {hist} GROUP BY txid HAVING count(DISTINCT tx_at) > 1
        ) s
    """)
    split_txns = cur.fetchone()[0]

    # Both sides of the comparison are jsonb built by to_jsonb: after_row when
    # the trigger fired, and the live row now. jsonb renders a timestamptz using
    # the session's TimeZone, so a session that writes under one setting and
    # verifies under another shows a spurious diff. Neither the seed nor churn
    # changes it, and the row counts are reported separately, so a mismatch of
    # content-but-not-count points straight at that.
    cur.execute(f"""
        WITH latest AS (
          SELECT DISTINCT ON (pk) pk, op, after_row
            FROM {hist}
           ORDER BY pk, tx_at DESC, history_id DESC
        ),
        expected AS (SELECT after_row AS row FROM latest WHERE op <> 'D'),
        actual   AS (SELECT to_jsonb(x) AS row FROM {table} x)
        SELECT (SELECT count(*) FROM expected),
               (SELECT count(*) FROM actual),
               (SELECT count(*) FROM (SELECT row FROM expected EXCEPT ALL SELECT row FROM actual) d),
               (SELECT count(*) FROM (SELECT row FROM actual EXCEPT ALL SELECT row FROM expected) d)
    """)
    expected_rows, actual_rows, only_expected, only_actual = cur.fetchone()

    return TableCheck(table=table, entries=entries, txns=txns, instants=instants,
                      inversions=inversions, split_txns=split_txns,
                      expected_rows=expected_rows, actual_rows=actual_rows,
                      only_expected=only_expected, only_actual=only_actual)


def verify(conn, out=sys.stdout) -> bool:
    """Assert the ledger is append-only, monotonic, and a faithful oracle."""
    tables = captured_tables(conn)
    failures: list[str] = []

    print("verify: the ledger replays to the live tables", file=out)
    with conn.cursor() as cur:
        for table in tables:
            check = check_table(cur, table)
            status = "ok" if check.ok else "FAIL"
            print(f"  {table:<14} {check.entries:>6} entries  {check.txns:>5} txns  "
                  f"{check.instants:>5} instants  replay {check.expected_rows:>6} vs "
                  f"{check.actual_rows:>6} live  {status}", file=out)
            for problem in check.problems():
                print(f"    - {problem}", file=out)
                failures.append(f"{table}: {problem}")

        # The whole ledger at once. Three things are true of it or the timeline
        # is not usable:
        #
        #   * a transaction is one point in time -- one tx_at, even when it wrote
        #     to four tables, or a replay could see half of it;
        #   * two transactions are two points -- if they shared a tx_at, no T
        #     could separate them and the timeline has fewer usable points in it
        #     than it appears to;
        #   * some transactions touched more than one table, or --snap-to-txn has
        #     nothing to be right or wrong about.
        union = " UNION ALL ".join(
            f"SELECT txid, tx_at, '{t}' AS table_name FROM {t}_history" for t in tables
        )
        cur.execute(f"""
            WITH entries AS ({union}),
            per_txn AS (
              SELECT txid,
                     count(DISTINCT tx_at)     AS instants,
                     count(DISTINCT table_name) AS tables
                FROM entries
               GROUP BY txid
            )
            SELECT (SELECT count(*) FROM per_txn),
                   (SELECT count(DISTINCT tx_at) FROM entries),
                   (SELECT count(*) FROM per_txn WHERE instants > 1),
                   (SELECT count(*) FROM per_txn WHERE tables > 1)
        """)
        txids, instants, split, multi = cur.fetchone()
        print(f"  {'timeline':<14} {txids:>6} txns    {instants:>5} instants  "
              f"{multi:>5} touching more than one table", file=out)
        if split:
            failures.append(f"{split} transactions are recorded at more than one tx_at across tables")
        if txids != instants:
            failures.append(f"{txids} transactions share only {instants} distinct tx_at values; "
                            "some points on the timeline are indistinguishable")
        if multi == 0:
            failures.append("no transaction touched more than one table; --snap-to-txn has "
                            "nothing to be right or wrong about")

    if failures:
        print(file=out)
        print("FAIL: the ledger is not a usable oracle:", file=out)
        for failure in failures:
            print(f"  - {failure}", file=out)
        return False
    print(file=out)
    print("PASS: every mutation is in the ledger, commit timestamps increase, and replaying "
          "the ledger reproduces the live tables", file=out)
    return True


# --------------------------------------------------------------------------
# cli
# --------------------------------------------------------------------------


def _seconds(text: str) -> float:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([smh]?)", text.strip())
    if not match:
        raise argparse.ArgumentTypeError(f"expected a duration like 90, 30s, 5m or 1h, got {text!r}")
    return float(match.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[match.group(2)]


def _parser() -> argparse.ArgumentParser:
    d = ChurnConfig()
    p = argparse.ArgumentParser(
        prog="python -m loadgen",
        description="Apply continuous inserts, updates and deletes to the seeded clinic schema.",
        epilog="The run stops at whichever ceiling is reached first. There is no unbounded mode: "
               "cleaned topics run at infinite retention on a laptop PVC.",
    )
    p.add_argument("--dsn", default=None, help="libpq connection string (default: $PIT_DSN or the dev release)")
    p.add_argument("--seed", type=int, default=d.seed, help=f"seed for the mutation plan (default: {d.seed})")
    p.add_argument("--rate", type=float, default=d.rate, help=f"transactions per second (default: {d.rate:g})")
    p.add_argument("--duration", type=_seconds, default=d.duration,
                   help=f"wall-clock ceiling, e.g. 30s / 5m / 1h (default: {_fmt(d.duration)})")
    p.add_argument("--max-txns", type=int, default=d.max_txns,
                   help=f"transaction ceiling (default: {d.max_txns})")
    p.add_argument("--max-ledger-rows", type=int, default=d.max_ledger_rows,
                   help=f"ceiling on ledger rows written (default: {d.max_ledger_rows})")
    p.add_argument("--refresh-every", type=int, default=d.refresh_every,
                   help=f"reload candidate ids this often (default: {d.refresh_every})")
    p.add_argument("--no-warmup", action="store_true",
                   help="skip the pass that runs every shape once before going random")
    p.add_argument("--verify", action="store_true", help="check the ledger and exit without churning")
    p.add_argument("--no-verify", action="store_true", help="churn without checking the ledger afterwards")
    p.add_argument("--quiet", action="store_true", help="suppress the per-transaction lines")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    import psycopg

    args = _parser().parse_args(argv)
    if args.rate <= 0:
        print("--rate must be greater than zero", file=sys.stderr)
        return 2

    cfg = ChurnConfig(
        seed=args.seed,
        rate=args.rate,
        duration=args.duration,
        max_txns=args.max_txns,
        max_ledger_rows=args.max_ledger_rows,
        refresh_every=max(args.refresh_every, 1),
        warmup=not args.no_warmup,
    )

    dsn = args.dsn or config.dsn_from_env()
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            if args.verify:
                return 0 if verify(conn) else 1
            run(conn, cfg, quiet=args.quiet)
            if args.no_verify:
                return 0
            print(file=sys.stdout)
            return 0 if verify(conn) else 1
    except psycopg.OperationalError as exc:
        print(f"churn: cannot reach the source database at {dsn!r}: {exc}\n"
              "Is `make forward` running?", file=sys.stderr)
        return 1
    except SeedError as exc:
        print(f"churn: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
