"""Every decision the seed makes that is not code, in one place.

The seed constant is the reason this module exists. Two people debugging the
same failure have to be looking at the same rows, and M8's determinism test
needs a baseline that does not move underneath it, so nothing in the generator
may read the clock, the OS entropy pool, or the environment. Every value in a
generated row descends from ``SEED`` and the constants below.

Changing anything here changes the dataset, and therefore changes the
fingerprint. That is intended -- the fingerprint is a claim about a specific
configuration, not about the generator in general.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

UTC = timezone.utc

# Arbitrary, and fixed forever. The value carries no meaning; the fact that it
# never changes is the whole point.
SEED = 20260819


@dataclass(frozen=True)
class Counts:
    """Exact row counts. The generator hits these numbers or raises.

    Volume is not what makes this dataset useful -- shape is -- so these are
    sized to seed a laptop-scale Postgres in a couple of seconds.
    """

    providers: int = 12
    patients: int = 250
    appointments: int = 900
    claims: int = 500
    notes: int = 750


@dataclass(frozen=True)
class Mix:
    """Distribution knobs.

    Real clinic data is not uniform: a handful of patients account for a large
    share of the appointments, most have one or two, and some have none at all.
    A uniform generator hides every bug that only shows up in the tail -- the
    patient with forty appointments is the one whose replay is slow and whose
    de-identification leaks through sheer volume of quasi-identifiers, and the
    patient with none is the one that a join-based policy silently drops.
    """

    # Patients carried on the books with no appointment yet.
    patients_without_appointments: float = 0.12
    # A few patients who account for a disproportionate share of the visits.
    heavy_utilizers: int = 6
    heavy_utilizer_weight: float = 14.0
    # Spread of the lognormal weights behind everyone else's visit count.
    utilization_sigma: float = 1.05

    # Claims raised against a specific completed visit; the rest are
    # patient-level and carry a NULL appointment_id.
    claims_attached_to_appointment: float = 0.70
    # Notes written against a visit; the rest are standalone (telephone
    # encounters, letters, care-coordination notes).
    notes_attached_to_appointment: float = 0.80
    # Notes that are superseded by a later addendum.
    notes_amended: float = 0.06


@dataclass(frozen=True)
class Edges:
    """Awkward cases the de-identification policy has to survive.

    These are guaranteed, not sampled. A case that shows up "usually" is a case
    that is absent from the one run where it mattered, so the generator plants a
    fixed number of each and refuses to return a dataset that is missing any of
    them (see ``_assert_edges``). The random tail produces more of them
    incidentally; these are the floor.
    """

    # A policy that assumes every patient is reachable by email.
    null_emails: int = 8
    # HIPAA Safe Harbor caps age at 89: everyone older has to collapse into a
    # single "90+" bucket or the outlier re-identifies themselves.
    over_89: int = 3
    # Zips are text, not integers. Somewhere downstream something will parse
    # 02134 as 2134 and truncate it to a zip3 of "213".
    leading_zero_zips: int = 4
    # Non-ASCII names, which break naive regex redaction, byte-length
    # truncation, and anything that round-trips through latin-1.
    unicode_names: int = 6
    # No SSN on file at all, versus an SSN in an unexpected format.
    null_ssns: int = 10
    # numeric(12,2) values chosen to be annoying: zero, sub-dollar, a value
    # whose cents are lost to float, and one near the top of the precision.
    claim_amounts: tuple[Decimal, ...] = (
        Decimal("0.00"),
        Decimal("0.07"),
        Decimal("1234.56"),
        Decimal("0.10"),
        Decimal("99999999.99"),
    )
    # NOT NULL DEFAULT '{}' -- an empty array is not a null array, and a policy
    # that generalizes diagnosis codes has to say something about both.
    empty_diagnosis_claims: int = 3


@dataclass(frozen=True)
class SeedConfig:
    seed: int = SEED

    # The dataset's "now". A fixed instant rather than the wall clock, because
    # a dataset that drifts with the calendar is not reproducible. Appointments
    # after this are in the future and therefore still 'scheduled'. Bump it
    # deliberately when the data starts to feel stale; expect the fingerprint
    # to change when you do.
    as_of: datetime = datetime(2026, 8, 1, tzinfo=UTC)
    # Oldest patient enrollment.
    history_start: datetime = datetime(2023, 1, 2, tzinfo=UTC)
    # Furthest-out scheduled appointment.
    future_horizon: datetime = datetime(2026, 11, 30, tzinfo=UTC)

    locale: str = "en_US"

    counts: Counts = field(default_factory=Counts)
    mix: Mix = field(default_factory=Mix)
    edges: Edges = field(default_factory=Edges)

    @property
    def enrollment_end(self) -> datetime:
        """Nobody enrolls in the last month; everyone has some history."""
        return self.as_of - timedelta(days=30)


DEFAULT = SeedConfig()


def dsn_from_env() -> str:
    """Connection string for the source Postgres.

    Defaults assume ``make forward`` is running against the dev release,
    whose credentials live in ``charts/pit/charts/source-pg/values.yaml``. Set
    ``PIT_DSN`` to point somewhere else; the individual ``PG*`` variables are
    honoured too so this behaves like any other libpq client.
    """
    dsn = os.environ.get("PIT_DSN")
    if dsn:
        return dsn
    host = os.environ.get("PGHOST", "127.0.0.1")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "pit")
    password = os.environ.get("PGPASSWORD", "pit-dev-password")
    database = os.environ.get("PGDATABASE", "pit")
    return f"host={host} port={port} user={user} password={password} dbname={database}"
