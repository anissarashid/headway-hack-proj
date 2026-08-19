"""Populate the clinic schema with synthetic patients, providers and history.

Everything here is fake. No real patient data goes anywhere near this repo, and
the generator has no way to reach any: it takes an integer seed and produces
rows, with no network, no filesystem and no clock involved.

Two properties matter more than volume:

*Reproducibility.* Given the same config, two runs produce byte-identical row
contents. Nothing calls ``datetime.now()`` or the global ``random`` module --
every draw comes from a ``random.Random`` seeded from ``config.SEED``, and each
table gets its own stream so that changing how notes are generated does not
reshuffle the patients. ``fingerprint.py`` turns a dataset into a digest so this
can be asserted rather than hoped for.

*Shape.* A uniform dataset hides every bug that only appears in the tail, so the
distributions are deliberately lumpy -- a few patients with dozens of
appointments, most with one or two, some with none -- and the awkward cases the
de-identification policy has to survive (null emails, a patient over the HIPAA
Safe Harbor age cap, a zip starting with a zero, decimal money, unicode names)
are planted rather than sampled. ``_assert_edges`` refuses to return a dataset
that is missing any of them.

The whole seed lands in one transaction, so the mutation ledger records it at a
single ``tx_at``: a point-in-time query before that instant sees an empty
database, and one after it sees the whole population. Ongoing churn is a
separate concern and belongs in its own generator.

    python -m loadgen.seed --reset        # wipe and repopulate
    python -m loadgen.seed --dry-run      # generate, print the digest, no DB
    python -m loadgen.seed --fingerprint  # digest whatever is in the DB now
"""

from __future__ import annotations

import argparse
import dataclasses
import random
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable, Sequence

from faker import Faker

from . import config, vocab
from .config import UTC, SeedConfig

CENTS = Decimal("0.01")

# Order matters: parents before children on insert, children before parents on
# delete. providers is last because notes and appointments reference it with
# ON DELETE RESTRICT.
TABLES = ("providers", "patients", "appointments", "claims", "notes")

PROVIDER_COLUMNS = ("npi", "full_name", "specialty", "email", "created_at", "updated_at")
PATIENT_COLUMNS = (
    "mrn", "first_name", "middle_name", "last_name", "date_of_birth", "ssn",
    "email", "phone", "address_line1", "address_line2", "city", "state",
    "postal_code", "created_at", "updated_at",
)
APPOINTMENT_COLUMNS = (
    "patient_id", "provider_id", "scheduled_at", "checked_in_at", "completed_at",
    "duration_minutes", "status", "location", "intake_answers", "created_at", "updated_at",
)
CLAIM_COLUMNS = (
    "patient_id", "appointment_id", "billed_amount", "allowed_amount", "paid_amount",
    "patient_responsibility", "diagnosis_codes", "procedure_code", "claim_status",
    "submitted_at", "adjudicated_at", "created_at", "updated_at",
)
NOTE_COLUMNS = (
    "patient_id", "provider_id", "appointment_id", "amends_note_id", "note_type",
    "body", "authored_at", "signed_at", "is_amended", "created_at", "updated_at",
)


class SeedError(RuntimeError):
    """The generator produced something it was told not to produce."""


@dataclass
class Dataset:
    """Generated rows, before they have database identities.

    Foreign keys are carried as ``*_ix`` indexes into the sibling lists rather
    than as ids, because ids do not exist until the insert. ``load`` resolves
    them; ``fingerprint`` resolves them to natural keys instead, which is what
    makes two runs comparable even when the identity sequences have moved on.
    """

    providers: list[dict[str, Any]] = field(default_factory=list)
    patients: list[dict[str, Any]] = field(default_factory=list)
    appointments: list[dict[str, Any]] = field(default_factory=list)
    claims: list[dict[str, Any]] = field(default_factory=list)
    notes: list[dict[str, Any]] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        return {t: len(getattr(self, t)) for t in TABLES}


# --------------------------------------------------------------------------
# seeded streams
# --------------------------------------------------------------------------


def _stream(cfg: SeedConfig, name: str) -> tuple[random.Random, Faker]:
    """A random source and a Faker, both seeded from ``(cfg.seed, name)``.

    Per-table streams rather than one shared generator: with a single stream,
    adding a column to ``patients`` would consume a different number of draws
    and silently change every provider, appointment and note downstream. Seeding
    from a string is stable across processes -- ``random.Random`` hashes it with
    SHA-512, so unlike ``hash()`` it is not affected by PYTHONHASHSEED.
    """
    key = f"{cfg.seed}:{name}"
    rng = random.Random(key)
    fake = Faker(cfg.locale)
    fake.seed_instance(key)
    return rng, fake


def _disjoint_picks(rng: random.Random, n: int, sizes: dict[str, int]) -> dict[str, set[int]]:
    """Carve non-overlapping index sets out of ``range(n)``.

    The guaranteed edge cases are kept apart so that a failure points at one
    thing: a patient who is both 94 and has a null email tells you less than two
    patients who each have one problem. The random tail still produces overlaps
    on its own.
    """
    total = sum(sizes.values())
    if total > n:
        raise SeedError(f"edge cases need {total} patients but only {n} were asked for")
    pool = list(range(n))
    rng.shuffle(pool)
    out: dict[str, set[int]] = {}
    cursor = 0
    for name, size in sizes.items():
        out[name] = set(pool[cursor:cursor + size])
        cursor += size
    return out


def _weighted(rng: random.Random, choices: Sequence[tuple[Any, float]]) -> Any:
    values = [c[0] for c in choices]
    weights = [c[1] for c in choices]
    return rng.choices(values, weights=weights, k=1)[0]


def _between(rng: random.Random, lo: datetime, hi: datetime) -> datetime:
    """A uniform instant in [lo, hi), to the second."""
    span = max(int((hi - lo).total_seconds()), 1)
    return (lo + timedelta(seconds=rng.randrange(span))).replace(microsecond=0)


def _business_slot(rng: random.Random, lo: datetime, hi: datetime) -> datetime:
    """A weekday appointment slot on the quarter hour."""
    when = _between(rng, lo, hi)
    while when.weekday() >= 5:
        when += timedelta(days=1)
    hour = rng.choice([8, 9, 10, 11, 13, 14, 15, 16])
    minute = rng.choice([0, 15, 30, 45])
    return when.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _dedupe_times(rows: list[dict[str, Any]], key: str, group: str = "patient_ix") -> None:
    """Force (group, timestamp) to be unique, in place.

    Not cosmetic. Neither appointments nor notes nor claims have a natural key
    in the schema, so the fingerprint identifies them by patient plus timestamp;
    a collision there would make two genuinely different rows compare equal and
    quietly weaken the determinism check. A patient not being double-booked to
    the second is also true of real clinics.
    """
    seen: set[tuple[Any, datetime]] = set()
    for row in sorted(rows, key=lambda r: (r[group], r[key])):
        stamp = row[key]
        while (row[group], stamp) in seen:
            stamp = stamp + timedelta(microseconds=1)
        seen.add((row[group], stamp))
        row[key] = stamp


def _age_on(dob: date, when: datetime) -> int:
    d = when.date()
    return d.year - dob.year - ((d.month, d.day) < (dob.month, dob.day))


def _has_non_ascii(*values: str | None) -> bool:
    return any(v is not None and any(ord(c) > 127 for c in v) for v in values)


def _ascii_handle(*parts: str) -> str:
    """A mailbox-safe handle from a possibly non-ASCII name.

    Returns "" when nothing survives folding -- NFKD decomposes Latin
    diacritics but has nothing to say about Cyrillic, Han or Arabic. The caller
    decides what to do with that; silently emailing everyone whose name is not
    Latin at the same address would be a bug hiding in a default.
    """
    folded = unicodedata.normalize("NFKD", ".".join(parts))
    kept = [c.lower() for c in folded if c.isascii() and (c.isalnum() or c == ".")]
    return "".join(kept).strip(".")


# --------------------------------------------------------------------------
# providers
# --------------------------------------------------------------------------


def _npi(rng: random.Random) -> str:
    """A 10-digit NPI with a real check digit.

    The Luhn check runs over the 80840 prefix the standard prescribes, so these
    pass validation anywhere an NPI is validated -- a number that fails a
    checksum gets rejected before it can exercise anything downstream.
    """
    body = "".join(rng.choice("0123456789") for _ in range(9))
    total = 0
    for i, ch in enumerate(reversed("80840" + body)):
        d = int(ch)
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return body + str((10 - total % 10) % 10)


def _providers(cfg: SeedConfig) -> list[dict[str, Any]]:
    rng, fake = _stream(cfg, "providers")
    rows: list[dict[str, Any]] = []
    seen_npi: set[str] = set()
    for _ in range(cfg.counts.providers):
        npi = _npi(rng)
        while npi in seen_npi:
            npi = _npi(rng)
        seen_npi.add(npi)

        first, last = fake.first_name(), fake.last_name()
        credential = rng.choice(vocab.CREDENTIALS)
        handle = _ascii_handle(first, last) or f"provider{len(rows) + 1}"
        # One column, not three: providers carry their name the way a directory
        # does, which is a different parsing problem from patients.
        full_name = f"Dr. {first} {last}, {credential}"
        created = _between(rng, cfg.history_start - timedelta(days=900), cfg.history_start)
        rows.append({
            "npi": npi,
            "full_name": full_name,
            "specialty": rng.choice(vocab.SPECIALTIES),
            "email": f"{handle}@clinic.example.org",
            "created_at": created,
            "updated_at": created,
        })
    return rows


# --------------------------------------------------------------------------
# patients
# --------------------------------------------------------------------------


def _ssn(rng: random.Random) -> str:
    """An SSN in one of the formats the source actually contains.

    The schema comment says the format varies deliberately and warns against a
    CHECK that assumes one. Dirty input is the point: the padded variants below
    are not a bug in this generator, they are the case where a tokenizer that
    keys on the exact string produces two tokens for one person.
    """
    area = rng.randrange(1, 900)
    while area in (666,):
        area = rng.randrange(1, 900)
    group = rng.randrange(1, 100)
    serial = rng.randrange(1, 10000)
    style = _weighted(rng, [
        ("dashed", 0.66),
        ("bare", 0.16),
        ("spaced", 0.08),
        ("dotted", 0.04),
        ("padded", 0.06),
    ])
    if style == "dashed":
        return f"{area:03d}-{group:02d}-{serial:04d}"
    if style == "bare":
        return f"{area:03d}{group:02d}{serial:04d}"
    if style == "spaced":
        return f"{area:03d} {group:02d} {serial:04d}"
    if style == "dotted":
        return f"{area:03d}.{group:02d}.{serial:04d}"
    return f"  {area:03d}-{group:02d}-{serial:04d} "


def _phone(rng: random.Random) -> str:
    # 555-0100 through 555-0199 is the range reserved for fiction, so none of
    # these can ring a real telephone.
    area = rng.choice(vocab.AREA_CODES)
    line = f"555-01{rng.randrange(0, 100):02d}"
    style = _weighted(rng, [
        ("paren", 0.34), ("dashed", 0.30), ("bare", 0.14),
        ("intl", 0.10), ("dotted", 0.06), ("ext", 0.06),
    ])
    if style == "paren":
        return f"({area}) {line}"
    if style == "dashed":
        return f"{area}-{line}"
    if style == "bare":
        return f"{area}{line.replace('-', '')}"
    if style == "intl":
        return f"+1 {area} {line.replace('-', ' ')}"
    if style == "dotted":
        return f"{area}.{line.replace('-', '.')}"
    return f"{area}-{line} x{rng.randrange(100, 999)}"


def _date_of_birth(rng: random.Random, cfg: SeedConfig, very_old: bool) -> date:
    if very_old:
        # Past the Safe Harbor cap on purpose: 90+ has to collapse into one
        # bucket, and an outlier that survives generalization identifies itself.
        age = rng.randrange(90, 105)
    else:
        low, high = _weighted(rng, [
            ((0, 17), 0.14), ((18, 34), 0.24), ((35, 49), 0.22),
            ((50, 64), 0.22), ((65, 79), 0.14), ((80, 89), 0.04),
        ])
        age = rng.randrange(low, high + 1)
    birthday = cfg.as_of.date().replace(year=cfg.as_of.year - age)
    return birthday - timedelta(days=rng.randrange(0, 365))


def _patients(cfg: SeedConfig) -> list[dict[str, Any]]:
    rng, fake = _stream(cfg, "patients")
    n = cfg.counts.patients
    edges = cfg.edges
    picks = _disjoint_picks(rng, n, {
        "unicode": edges.unicode_names,
        "null_email": edges.null_emails,
        "over_89": edges.over_89,
        "zero_zip": edges.leading_zero_zips,
        "null_ssn": edges.null_ssns,
    })

    # Handed out in list order rather than sampled, so the guaranteed unicode
    # patients cover as many different scripts as there are of them.
    unicode_names = {i: vocab.UNICODE_NAMES[k % len(vocab.UNICODE_NAMES)]
                     for k, i in enumerate(sorted(picks["unicode"]))}

    # Sequential with a seed-derived base, so MRNs are unique within a run and
    # differ between seeds -- two datasets loaded side by side never collide on
    # the unique index.
    mrn_base = 3_000_000 + rng.randrange(0, 900_000)

    rows: list[dict[str, Any]] = []
    for i in range(n):
        if i in unicode_names:
            first, last = unicode_names[i]
            # An ASCII middle name alongside a non-Latin given name is common
            # and mixes scripts within one person's record.
            middle = fake.first_name() if rng.random() < 0.4 else None
        else:
            first, last = fake.first_name(), fake.last_name()
            middle = fake.first_name() if rng.random() < 0.42 else None

        dob = _date_of_birth(rng, cfg, very_old=i in picks["over_89"])

        if i in picks["null_email"] or rng.random() < 0.05:
            email = None
        else:
            # Nothing survives folding for a name in a non-Latin script, so
            # those patients get a handle the way a real registration desk
            # would issue one, rather than an address built from nothing.
            handle = _ascii_handle(first, last) or fake.user_name()
            domain = rng.choice(vocab.EMAIL_DOMAINS)
            flavour = _weighted(rng, [("plain", 0.6), ("plus", 0.12), ("numeric", 0.2), ("initial", 0.08)])
            if flavour == "plus":
                email = f"{handle}+clinic@{domain}"
            elif flavour == "numeric":
                email = f"{handle}{rng.randrange(1, 99)}@{domain}"
            elif flavour == "initial":
                email = f"{handle.replace('.', '')[:9]}@{domain}"
            else:
                email = f"{handle}@{domain}"

        if i in picks["zero_zip"]:
            city, state, postal = rng.choice(vocab.LEADING_ZERO_PLACES)
        else:
            # postcode_in_state, not postcode: a zip that does not belong to
            # its state makes a zip3 generalization meaningless, and zip3 is
            # the only geography Safe Harbor leaves behind.
            city = fake.city()
            state = fake.state_abbr(include_territories=False, include_freely_associated_states=False)
            postal = fake.postcode_in_state(state)
            if rng.random() < 0.12:
                postal = f"{postal}-{rng.randrange(1000, 9999)}"

        # Enrollment cannot predate birth, and a newborn is registered within
        # a few days. Everything else in the dataset hangs off created_at, so
        # getting this wrong would put appointments before the patient existed.
        earliest = max(cfg.history_start, datetime(dob.year, dob.month, dob.day, tzinfo=UTC))
        enrolled = _between(rng, earliest, max(earliest + timedelta(days=3), cfg.enrollment_end))
        rows.append({
            "mrn": f"MRN-{mrn_base + i * 3:07d}",
            "first_name": first,
            "middle_name": middle,
            "last_name": last,
            "date_of_birth": dob,
            "ssn": None if i in picks["null_ssn"] else _ssn(rng),
            "email": email,
            "phone": None if rng.random() < 0.06 else _phone(rng),
            "address_line1": fake.street_address(),
            "address_line2": fake.secondary_address() if rng.random() < 0.18 else None,
            "city": city,
            "state": state,
            "postal_code": postal,
            "created_at": enrolled,
            "updated_at": enrolled,
        })
    return rows


# --------------------------------------------------------------------------
# appointments
# --------------------------------------------------------------------------


def _allocate(rng: random.Random, cfg: SeedConfig, patients: int) -> list[int]:
    """How many appointments each patient gets.

    Largest-remainder apportionment over lognormal weights: the total is exactly
    what the config asked for, the shape is heavy-tailed, and a few designated
    heavy utilizers sit well out in the tail where they can be relied on rather
    than hoped for.
    """
    mix = cfg.mix
    total = cfg.counts.appointments
    zero_count = round(patients * mix.patients_without_appointments)
    order = list(range(patients))
    rng.shuffle(order)
    zero = set(order[:zero_count])
    eligible = [i for i in order[zero_count:]]

    if total < len(eligible):
        raise SeedError(
            f"{total} appointments cannot cover {len(eligible)} patients with at least one each; "
            "raise counts.appointments or mix.patients_without_appointments"
        )

    weights = [rng.lognormvariate(0.0, mix.utilization_sigma) for _ in eligible]
    for slot in range(min(mix.heavy_utilizers, len(eligible))):
        weights[slot] *= mix.heavy_utilizer_weight

    remaining = total - len(eligible)
    swt = sum(weights)
    quota = [remaining * w / swt for w in weights]
    extra = [int(q) for q in quota]
    leftover = remaining - sum(extra)
    # Ties broken by index, never by dict or set ordering, so this is stable.
    ranked = sorted(range(len(eligible)), key=lambda k: (-(quota[k] - extra[k]), k))
    for k in ranked[:leftover]:
        extra[k] += 1

    counts = [0] * patients
    for k, patient_ix in enumerate(eligible):
        counts[patient_ix] = 1 + extra[k]
    for patient_ix in zero:
        counts[patient_ix] = 0
    return counts


def _intake_answers(rng: random.Random, fake: Faker, phone: str | None) -> dict[str, Any] | None:
    """Free text nested in a jsonb document, with a shape that varies per row.

    An intake form changes over time, so the keys are not a fixed set and a
    policy that hardcoded one would already be wrong. Some values are nested
    objects and some are arrays, because a policy that only descends one level
    is a policy that leaks the emergency contact.
    """
    roll = rng.random()
    if roll < 0.24:
        return None
    if roll < 0.30:
        return {}

    keys = rng.sample(vocab.INTAKE_KEYS, k=rng.randrange(2, 6))
    relative = fake.name()
    employer = fake.company()
    contact = phone or _phone(rng)
    doc: dict[str, Any] = {}
    for key in sorted(keys):
        if key in ("reason_for_visit", "notes_for_provider"):
            doc[key] = rng.choice(vocab.INTAKE_FREE_TEXT).format(
                relative=relative, employer=employer, phone=contact)
        elif key == "current_medications":
            doc[key] = rng.sample(
                ["metformin 500mg", "lisinopril 10mg", "sertraline 50mg",
                 "albuterol PRN", "atorvastatin 20mg", "none"],
                k=rng.randrange(1, 4))
        elif key == "allergies":
            doc[key] = rng.choice(["penicillin", "none known", "sulfa drugs", "latex, shellfish"])
        elif key == "emergency_contact":
            if rng.random() < 0.5:
                doc[key] = {
                    "name": relative,
                    "relation": rng.choice(vocab.RELATIONS),
                    "phone": _phone(rng),
                }
            else:
                doc[key] = f"{relative} ({rng.choice(vocab.RELATIONS)}), {_phone(rng)}"
        elif key == "preferred_pharmacy":
            doc[key] = rng.choice(vocab.PHARMACIES)
        elif key == "interpreter_needed":
            doc[key] = rng.random() < 0.15
        elif key == "transport_needed":
            doc[key] = rng.random() < 0.1
        elif key == "employer":
            doc[key] = employer
        elif key == "referred_by":
            doc[key] = f"Dr. {fake.last_name()}"
    return doc


def _appointments(cfg: SeedConfig, patients: list[dict], providers: list[dict]) -> list[dict[str, Any]]:
    rng, fake = _stream(cfg, "appointments")
    per_patient = _allocate(rng, cfg, len(patients))

    rows: list[dict[str, Any]] = []
    for patient_ix, count in enumerate(per_patient):
        if count == 0:
            continue
        enrolled = patients[patient_ix]["created_at"]
        # Most patients stay with the provider who took them on; a minority get
        # passed around, which is what makes provider a re-identification handle.
        usual = rng.randrange(len(providers))
        slots: set[datetime] = set()
        while len(slots) < count:
            slots.add(_business_slot(rng, enrolled, cfg.future_horizon))

        for scheduled in sorted(slots):
            provider_ix = usual if rng.random() < 0.78 else rng.randrange(len(providers))
            duration = _weighted(rng, [(15, 0.12), (20, 0.14), (30, 0.44), (45, 0.20), (60, 0.10)])

            if scheduled > cfg.as_of:
                status = _weighted(rng, [("scheduled", 0.94), ("cancelled", 0.06)])
            elif scheduled > cfg.as_of - timedelta(days=1):
                status = "checked_in"
            else:
                status = _weighted(rng, [
                    ("completed", 0.70), ("cancelled", 0.16),
                    ("no_show", 0.10), ("checked_in", 0.04),
                ])

            checked_in = completed = None
            if status in ("checked_in", "completed"):
                checked_in = scheduled + timedelta(minutes=rng.randrange(-10, 26))
            if status == "completed":
                completed = checked_in + timedelta(minutes=duration + rng.randrange(-5, 16))

            created = min(scheduled - timedelta(days=rng.randrange(1, 45)), cfg.as_of)
            created = max(created, enrolled)
            rows.append({
                "patient_ix": patient_ix,
                "provider_ix": provider_ix,
                "scheduled_at": scheduled,
                "checked_in_at": checked_in,
                "completed_at": completed,
                "duration_minutes": duration,
                "status": status,
                "location": None if rng.random() < 0.08 else rng.choice(vocab.CLINIC_LOCATIONS),
                "intake_answers": _intake_answers(rng, fake, patients[patient_ix]["phone"]),
                "created_at": created,
                "updated_at": created,
            })

    _dedupe_times(rows, "scheduled_at")
    rows.sort(key=lambda r: (r["patient_ix"], r["scheduled_at"]))
    return rows


# --------------------------------------------------------------------------
# claims
# --------------------------------------------------------------------------


def _money(rng: random.Random, low: float, high: float) -> Decimal:
    """A numeric(12,2), quantized here rather than left to the server.

    Rounding in Postgres instead would mean the in-memory dataset and the stored
    row disagree in the last cent, and the fingerprint compares the two.
    """
    return (Decimal(str(rng.uniform(low, high)))).quantize(CENTS)


def _claims(cfg: SeedConfig, patients: list[dict], appointments: list[dict]) -> list[dict[str, Any]]:
    rng, _fake = _stream(cfg, "claims")
    total = cfg.counts.claims
    edges = cfg.edges

    eligible = [i for i, a in enumerate(appointments) if a["status"] == "completed"]
    attached_target = round(total * cfg.mix.claims_attached_to_appointment)
    if attached_target > len(eligible):
        attached_target = len(eligible)
    attached = sorted(rng.sample(eligible, attached_target))

    rows: list[dict[str, Any]] = []
    for appointment_ix in attached:
        appt = appointments[appointment_ix]
        rows.append(_claim_row(rng, cfg, appt["patient_ix"], appointment_ix,
                               appt["completed_at"] + timedelta(days=rng.randrange(0, 15))))

    # The rest carry a NULL appointment_id: labs, supplies and referrals bill
    # against the patient, not the visit. Patients with no appointments at all
    # can still appear here, which is the case a join-only policy loses.
    if total > len(rows):
        weights = [0.0] * len(patients)
        for appt in appointments:
            weights[appt["patient_ix"]] += 1.0
        weights = [w + 0.35 for w in weights]
        picks = rng.choices(range(len(patients)), weights=weights, k=total - len(rows))
        for patient_ix in picks:
            submitted = _between(rng, patients[patient_ix]["created_at"], cfg.as_of)
            rows.append(_claim_row(rng, cfg, patient_ix, None, submitted))

    # Planted amounts and empty arrays, applied by position so they are present
    # in every run rather than in most of them.
    for offset, amount in enumerate(edges.claim_amounts):
        row = rows[offset]
        row["billed_amount"] = amount
        _reprice(row, rng)
    for offset in range(len(edges.claim_amounts), len(edges.claim_amounts) + edges.empty_diagnosis_claims):
        rows[offset]["diagnosis_codes"] = []

    _dedupe_times(rows, "submitted_at")
    rows.sort(key=lambda r: (r["patient_ix"], r["submitted_at"]))
    return rows


def _claim_row(rng: random.Random, cfg: SeedConfig, patient_ix: int,
               appointment_ix: int | None, submitted: datetime) -> dict[str, Any]:
    code, _label, low, high = rng.choice(vocab.PROCEDURES)
    conditions = rng.sample(vocab.CONDITIONS, k=rng.randrange(1, 5))
    status = _weighted(rng, [
        ("paid", 0.56), ("denied", 0.12), ("pending", 0.14),
        ("submitted", 0.13), ("appealed", 0.05),
    ])
    submitted = min(submitted, cfg.as_of).replace(microsecond=0)
    row: dict[str, Any] = {
        "patient_ix": patient_ix,
        "appointment_ix": appointment_ix,
        "billed_amount": _money(rng, low, high),
        "allowed_amount": None,
        "paid_amount": None,
        "patient_responsibility": None,
        "diagnosis_codes": [c.code for c in conditions],
        "procedure_code": code if rng.random() < 0.94 else None,
        "claim_status": status,
        "submitted_at": submitted,
        "adjudicated_at": None,
        "created_at": submitted,
        "updated_at": submitted,
    }
    if status in ("paid", "denied", "appealed"):
        row["adjudicated_at"] = submitted + timedelta(days=rng.randrange(3, 60))
    _reprice(row, rng)
    return row


def _reprice(row: dict[str, Any], rng: random.Random) -> None:
    """Derive the money columns from billed_amount and the claim status.

    Kept separate so a planted edge amount can be substituted and the rest of
    the row made consistent with it -- every CHECK on the table is >= 0, and
    allowed >= paid + responsibility has to hold to be worth reading.
    """
    billed: Decimal = row["billed_amount"]
    status = row["claim_status"]
    if status in ("submitted", "pending"):
        row["allowed_amount"] = None
        row["paid_amount"] = None
        row["patient_responsibility"] = None
        return
    if status == "denied":
        row["allowed_amount"] = None
        row["paid_amount"] = Decimal("0.00")
        row["patient_responsibility"] = billed
        return
    allowed = (billed * Decimal(str(round(rng.uniform(0.45, 0.92), 4)))).quantize(CENTS)
    if status == "appealed":
        row["allowed_amount"] = allowed
        row["paid_amount"] = Decimal("0.00")
        row["patient_responsibility"] = allowed
        return
    paid = (allowed * Decimal(str(round(rng.uniform(0.55, 1.0), 4)))).quantize(CENTS)
    row["allowed_amount"] = allowed
    row["paid_amount"] = paid
    row["patient_responsibility"] = allowed - paid


# --------------------------------------------------------------------------
# notes
# --------------------------------------------------------------------------


def _notes(cfg: SeedConfig, patients: list[dict], providers: list[dict],
           appointments: list[dict]) -> list[dict[str, Any]]:
    rng, fake = _stream(cfg, "notes")
    total = cfg.counts.notes

    eligible = [i for i, a in enumerate(appointments) if a["status"] in ("completed", "checked_in")]
    attached_target = min(round(total * cfg.mix.notes_attached_to_appointment), total)
    if not eligible:
        attached_target = 0

    rows: list[dict[str, Any]] = []
    # Repetition is allowed: one visit can carry a progress note and a letter.
    for appointment_ix in (rng.choices(eligible, k=attached_target) if attached_target else []):
        appt = appointments[appointment_ix]
        anchor = appt["completed_at"] or appt["checked_in_at"] or appt["scheduled_at"]
        rows.append({
            "patient_ix": appt["patient_ix"],
            "provider_ix": appt["provider_ix"],
            "appointment_ix": appointment_ix,
            "amends_ix": None,
            "note_type": _weighted(rng, [("progress", 0.68), ("intake", 0.16), ("discharge", 0.16)]),
            "authored_at": anchor + timedelta(minutes=rng.randrange(5, 900)),
        })

    # Standalone notes: telephone encounters and care coordination, with no
    # appointment to hang from. notes.appointment_id is nullable for exactly
    # this reason, and it is also the column that goes to NULL on cascade.
    for _ in range(total - len(rows)):
        patient_ix = rng.randrange(len(patients))
        rows.append({
            "patient_ix": patient_ix,
            "provider_ix": rng.randrange(len(providers)),
            "appointment_ix": None,
            "amends_ix": None,
            "note_type": _weighted(rng, [("telephone", 0.72), ("progress", 0.28)]),
            "authored_at": _between(rng, patients[patient_ix]["created_at"], cfg.as_of),
        })

    _dedupe_times(rows, "authored_at")
    rows.sort(key=lambda r: (r["patient_ix"], r["authored_at"]))

    _apply_amendments(rng, cfg, rows)

    for i, row in enumerate(rows):
        patient = patients[row["patient_ix"]]
        provider = providers[row["provider_ix"]]
        row["body"] = _note_body(rng, fake, cfg, row, patient, provider)
        row["signed_at"] = (
            None if rng.random() < 0.12
            else row["authored_at"] + timedelta(minutes=rng.randrange(2, 2880))
        )
        row["created_at"] = row["authored_at"]
        row["updated_at"] = row["authored_at"]
    return rows


def _apply_amendments(rng: random.Random, cfg: SeedConfig, rows: list[dict[str, Any]]) -> None:
    """Pair up notes so some are superseded by a later addendum.

    Marked at insert time rather than by updating the earlier note afterwards:
    the seed is insert-only on purpose, so the ledger records the initial
    population as one transaction of nothing but 'I' rows. Amendments arriving
    as updates over time is what the churn generator is for.
    """
    target = round(len(rows) * cfg.mix.notes_amended)
    if target == 0:
        return
    by_patient: dict[int, list[int]] = {}
    for i, row in enumerate(rows):
        by_patient.setdefault(row["patient_ix"], []).append(i)

    candidates = [
        (indexes[k], indexes[k + 1])
        for _patient, indexes in sorted(by_patient.items())
        for k in range(0, len(indexes) - 1, 2)
    ]
    if not candidates:
        return
    for earlier, later in rng.sample(candidates, k=min(target, len(candidates))):
        rows[earlier]["is_amended"] = True
        rows[later]["note_type"] = "addendum"
        rows[later]["amends_ix"] = earlier
        # An addendum written before the note it amends would be nonsense.
        if rows[later]["authored_at"] <= rows[earlier]["authored_at"]:
            rows[later]["authored_at"] = rows[earlier]["authored_at"] + timedelta(hours=6)
    for row in rows:
        row.setdefault("is_amended", False)


def _note_body(rng: random.Random, fake: Faker, cfg: SeedConfig, row: dict,
               patient: dict, provider: dict) -> str:
    """Unstructured PHI mixed into clinical prose.

    The hardest column in the schema. Names, dates of birth, phone numbers,
    employers, relatives, street addresses and the MRN itself appear here in
    running text, alongside the clinical content that has to survive
    de-identification for the row to be worth keeping.
    """
    condition = rng.choice(vocab.CONDITIONS)
    relation = rng.choice(vocab.RELATIONS)
    template = rng.choice(vocab.NOTE_TEMPLATES[row["note_type"]])
    onset = (row["authored_at"] - timedelta(days=rng.randrange(14, 400))).date()
    fields = {
        "patient": " ".join(p for p in (patient["first_name"], patient["middle_name"], patient["last_name"]) if p),
        "age": _age_on(patient["date_of_birth"], row["authored_at"]),
        "dob": patient["date_of_birth"].isoformat(),
        "mrn": patient["mrn"],
        "street": patient["address_line1"],
        "city": patient["city"],
        "state": patient["state"],
        "postal": patient["postal_code"],
        "phone": patient["phone"] or _phone(rng),
        "provider": provider["full_name"],
        "label": condition.label,
        "code": condition.code,
        "complaint": condition.complaint,
        "plan": condition.plan,
        "relative": fake.name(),
        "relation": relation,
        "relative_title": relation.capitalize(),
        "employer": fake.company(),
        "referrer": f"Dr. {fake.last_name()}",
        "pharmacy": rng.choice(vocab.PHARMACIES),
        "onset": onset.isoformat(),
        "weeks": rng.choice([2, 4, 6, 8, 12]),
        "minutes": rng.choice([4, 7, 11, 15, 22]),
    }
    return template.format(**fields)


# --------------------------------------------------------------------------
# generation
# --------------------------------------------------------------------------


def generate(cfg: SeedConfig = config.DEFAULT) -> Dataset:
    """Build the whole dataset in memory. Pure: no clock, no DB, no env."""
    providers = _providers(cfg)
    patients = _patients(cfg)
    appointments = _appointments(cfg, patients, providers)
    claims = _claims(cfg, patients, appointments)
    notes = _notes(cfg, patients, providers, appointments)
    ds = Dataset(providers=providers, patients=patients, appointments=appointments,
                 claims=claims, notes=notes)
    _assert_counts(ds, cfg)
    _assert_edges(ds, cfg)
    return ds


def _assert_counts(ds: Dataset, cfg: SeedConfig) -> None:
    wanted = dataclasses.asdict(cfg.counts)
    got = ds.counts()
    off = {t: (wanted[t], got[t]) for t in wanted if wanted[t] != got[t]}
    if off:
        detail = ", ".join(f"{t}: asked {w}, got {g}" for t, (w, g) in sorted(off.items()))
        raise SeedError(f"row counts do not match the config ({detail})")


def _assert_edges(ds: Dataset, cfg: SeedConfig) -> None:
    """Refuse to hand back a dataset that is missing an awkward case.

    Without this the guarantees in ``config.Edges`` are a comment. A case that
    is merely likely is a case that is absent from the one run where it would
    have caught something.
    """
    edges = cfg.edges
    failures: list[str] = []

    def want(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    emails = sum(1 for p in ds.patients if p["email"] is None)
    want(emails >= edges.null_emails, f"null emails: wanted {edges.null_emails}, found {emails}")

    ssns = sum(1 for p in ds.patients if p["ssn"] is None)
    want(ssns >= edges.null_ssns, f"null ssns: wanted {edges.null_ssns}, found {ssns}")

    old = sum(1 for p in ds.patients if _age_on(p["date_of_birth"], cfg.as_of) > 89)
    want(old >= edges.over_89, f"patients over 89: wanted {edges.over_89}, found {old}")

    zips = sum(1 for p in ds.patients if (p["postal_code"] or "").startswith("0"))
    want(zips >= edges.leading_zero_zips,
         f"leading-zero zips: wanted {edges.leading_zero_zips}, found {zips}")

    uni = sum(1 for p in ds.patients
              if _has_non_ascii(p["first_name"], p["middle_name"], p["last_name"]))
    want(uni >= edges.unicode_names, f"unicode names: wanted {edges.unicode_names}, found {uni}")

    billed = {c["billed_amount"] for c in ds.claims}
    for amount in edges.claim_amounts:
        want(amount in billed, f"claim amount {amount} was not planted")
    want(any(c["billed_amount"].as_tuple().exponent == -2 and c["billed_amount"] % 1 != 0
             for c in ds.claims), "no claim has a fractional amount")

    empty = sum(1 for c in ds.claims if c["diagnosis_codes"] == [])
    want(empty >= edges.empty_diagnosis_claims,
         f"empty diagnosis arrays: wanted {edges.empty_diagnosis_claims}, found {empty}")

    per_patient: dict[int, int] = {}
    for appt in ds.appointments:
        per_patient[appt["patient_ix"]] = per_patient.get(appt["patient_ix"], 0) + 1
    want(len(per_patient) < len(ds.patients), "every patient has an appointment; nobody has none")
    want(max(per_patient.values(), default=0) >= 10, "no patient has a long appointment history")
    want(sum(1 for v in per_patient.values() if v <= 2) > len(per_patient) / 2,
         "the appointment distribution is not tail-heavy")

    want(any(c["appointment_ix"] is None for c in ds.claims), "no claim has a NULL appointment_id")
    want(any(n["amends_ix"] is not None for n in ds.notes), "no note amends another")
    want(any(a["intake_answers"] is None for a in ds.appointments), "no appointment has NULL intake")
    want(any(isinstance(v, dict)
             for a in ds.appointments if a["intake_answers"]
             for v in a["intake_answers"].values()),
         "no intake document nests an object")

    if failures:
        raise SeedError("the dataset is missing guaranteed cases:\n  - " + "\n  - ".join(failures))


# --------------------------------------------------------------------------
# loading
# --------------------------------------------------------------------------


def _insert(cur, table: str, columns: Sequence[str], rows: Iterable[dict[str, Any]],
            returning: str) -> list[int]:
    """Insert rows and return the new ids in input order.

    ``executemany(..., returning=True)`` gives one result set per parameter set,
    in the order supplied. A single multi-row VALUES would be faster, but the
    order of its RETURNING is not guaranteed, and every foreign key below is
    resolved by position.
    """
    rows = list(rows)
    if not rows:
        return []
    placeholders = ", ".join(f"%({c})s" for c in columns)
    sql = (f"INSERT INTO {table} ({', '.join(columns)}) "
           f"VALUES ({placeholders}) RETURNING {returning}")
    cur.executemany(sql, rows, returning=True)
    ids: list[int] = []
    while True:
        ids.append(cur.fetchone()[0])
        if not cur.nextset():
            break
    if len(ids) != len(rows):
        raise SeedError(f"{table}: inserted {len(rows)} rows but got {len(ids)} ids back")
    return ids


def load(conn, ds: Dataset) -> dict[str, int]:
    """Insert the dataset in a single transaction.

    One transaction on purpose. The ledger stamps every row with
    ``transaction_timestamp()``, so the entire initial population lands at one
    point on the timeline: a point-in-time query before it sees an empty
    database and one after it sees all of it, with no half-loaded state in
    between that a replay could legitimately land on.
    """
    from psycopg.types.json import Json

    with conn.transaction(), conn.cursor() as cur:
        provider_ids = _insert(cur, "providers", PROVIDER_COLUMNS, ds.providers, "provider_id")
        patient_ids = _insert(cur, "patients", PATIENT_COLUMNS, ds.patients, "patient_id")

        appt_rows = [
            dict(r,
                 patient_id=patient_ids[r["patient_ix"]],
                 provider_id=provider_ids[r["provider_ix"]],
                 intake_answers=None if r["intake_answers"] is None else Json(r["intake_answers"]))
            for r in ds.appointments
        ]
        appointment_ids = _insert(cur, "appointments", APPOINTMENT_COLUMNS, appt_rows, "appointment_id")

        claim_rows = [
            dict(r,
                 patient_id=patient_ids[r["patient_ix"]],
                 appointment_id=None if r["appointment_ix"] is None else appointment_ids[r["appointment_ix"]])
            for r in ds.claims
        ]
        _insert(cur, "claims", CLAIM_COLUMNS, claim_rows, "claim_id")

        # Notes are inserted one at a time in authored order because
        # amends_note_id points at a note inserted earlier in this same loop --
        # the id has to exist before it can be referenced.
        note_ids: list[int] = []
        for r in ds.notes:
            row = dict(
                r,
                patient_id=patient_ids[r["patient_ix"]],
                provider_id=provider_ids[r["provider_ix"]],
                appointment_id=None if r["appointment_ix"] is None else appointment_ids[r["appointment_ix"]],
                amends_note_id=None if r["amends_ix"] is None else note_ids[r["amends_ix"]],
            )
            note_ids.append(_insert(cur, "notes", NOTE_COLUMNS, [row], "note_id")[0])

    return ds.counts()


def table_counts(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        out = {}
        for table in TABLES:
            cur.execute(f"SELECT count(*) FROM {table}")
            out[table] = cur.fetchone()[0]
        return out


def reset(conn) -> dict[str, int]:
    """Empty the captured tables and the ledger, and rewind the identities.

    DELETE, never TRUNCATE. TRUNCATE does not fire row-level triggers, so it
    would empty a captured table without the ledger noticing -- the README calls
    this out as the one known gap, and a reset that used it would silently
    corrupt the oracle for every run afterwards.

    Deleting the history rows is a separate step because the history tables are
    not themselves captured. Leaving them would mix the previous population's
    ledger into this one's, and the point-in-time answer at any T would then
    depend on how many times the seed had been run.
    """
    before = table_counts(conn)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT table_name FROM pit_captured_tables ORDER BY table_name")
        captured = {r[0] for r in cur.fetchall()}
        unknown = captured - set(TABLES)
        if unknown:
            # Better to stop than to leave rows behind in a table this module
            # has never heard of.
            raise SeedError(
                f"the database captures tables this seeder does not know about: {sorted(unknown)}. "
                "Add them to TABLES (children first) before resetting."
            )
        for table in reversed(TABLES):
            cur.execute(f"DELETE FROM {table}")
        for table in sorted(captured):
            cur.execute(f"DELETE FROM {table}_history")
        # Identity sequences are rewound so a reseeded database has the same
        # ids as a fresh one. The fingerprint deliberately does not depend on
        # this -- it keys on natural keys -- but identical ids make two runs
        # easier to diff by eye.
        for table, column in (("providers", "provider_id"), ("patients", "patient_id"),
                              ("appointments", "appointment_id"), ("claims", "claim_id"),
                              ("notes", "note_id")):
            cur.execute(f"ALTER TABLE {table} ALTER COLUMN {column} RESTART WITH 1")
        for table in sorted(captured):
            cur.execute(f"ALTER TABLE {table}_history ALTER COLUMN history_id RESTART WITH 1")
    return before


# --------------------------------------------------------------------------
# cli
# --------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m loadgen.seed",
        description="Populate the clinic schema with deterministic synthetic data.",
    )
    p.add_argument("--dsn", default=None, help="libpq connection string (default: $PIT_DSN or the dev release)")
    p.add_argument("--seed", type=int, default=None, help=f"override the seed (default: {config.SEED})")
    p.add_argument("--reset", action="store_true", help="delete existing rows and ledger history first")
    p.add_argument("--dry-run", action="store_true", help="generate and fingerprint without touching a database")
    p.add_argument("--fingerprint", action="store_true", help="digest what is already in the database and exit")
    p.add_argument("--quiet", action="store_true", help="suppress the summary; errors still print")
    return p


def _summary(out, title: str, counts: dict[str, int], digest: str, per_table: dict[str, str]) -> None:
    print(title, file=out)
    for table in TABLES:
        print(f"  {table:<14} {counts.get(table, 0):>6}  {per_table.get(table, '')}", file=out)
    print(f"  {'fingerprint':<14} {digest}", file=out)


def main(argv: Sequence[str] | None = None) -> int:
    from . import fingerprint as fp

    args = _parser().parse_args(argv)
    cfg = config.DEFAULT if args.seed is None else dataclasses.replace(config.DEFAULT, seed=args.seed)
    # The modes that emit a digest for a script to capture keep stdout clean and
    # put the human summary on stderr.
    out = sys.stderr if (args.fingerprint or args.dry_run) else sys.stdout

    if args.dry_run:
        ds = generate(cfg)
        canonical = fp.canonical_dataset(ds)
        if not args.quiet:
            _summary(out, f"generated (seed {cfg.seed}, not written)", ds.counts(),
                     fp.digest(canonical), fp.per_table_digests(canonical))
        print(fp.digest(canonical))
        return 0

    import psycopg

    dsn = args.dsn or config.dsn_from_env()
    with psycopg.connect(dsn) as conn:
        if args.fingerprint:
            canonical = fp.canonical_database(conn)
            if not args.quiet:
                _summary(out, "database contents", table_counts(conn),
                         fp.digest(canonical), fp.per_table_digests(canonical))
            print(fp.digest(canonical))
            return 0

        if args.reset:
            removed = reset(conn)
            if not args.quiet and any(removed.values()):
                print("reset: " + ", ".join(f"{t} -{n}" for t, n in removed.items() if n), file=out)
        else:
            existing = table_counts(conn)
            if any(existing.values()):
                print("refusing to seed: the clinic tables are not empty "
                      f"({', '.join(f'{t}={n}' for t, n in existing.items() if n)}). "
                      "Re-run with --reset.", file=sys.stderr)
                return 1

        ds = generate(cfg)
        load(conn, ds)
        canonical = fp.canonical_database(conn)
        stored = fp.digest(canonical)
        expected = fp.digest(fp.canonical_dataset(ds))
        if stored != expected:
            # The rows we generated and the rows Postgres kept disagree, which
            # means a type round-tripped badly. Worth failing on: the
            # fingerprint is only useful if both sides compute it the same way.
            print(f"stored data does not match what was generated: {stored} != {expected}",
                  file=sys.stderr)
            return 1
        if not args.quiet:
            _summary(out, f"seeded (seed {cfg.seed})", ds.counts(), stored,
                     fp.per_table_digests(canonical))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
