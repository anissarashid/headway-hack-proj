"""What the seed promises, asserted.

The determinism tests are the reason this file exists: they are M8's baseline,
and they run without a database so they can fail fast in CI. The distribution
and edge-case tests are here because a generator that is reproducibly useless is
still useless -- the shape of the data is part of the contract.
"""

from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
from datetime import timedelta

import pytest

from loadgen import config, fingerprint as fp
from loadgen.seed import Dataset, SeedError, _age_on, _has_non_ascii, generate


@pytest.fixture(scope="module")
def ds() -> Dataset:
    return generate(config.DEFAULT)


# --------------------------------------------------------------------------
# determinism
# --------------------------------------------------------------------------


def test_same_seed_same_rows():
    a, b = generate(config.DEFAULT), generate(config.DEFAULT)
    assert fp.canonical_dataset(a) == fp.canonical_dataset(b)
    assert fp.digest(fp.canonical_dataset(a)) == fp.digest(fp.canonical_dataset(b))


def test_same_seed_across_processes():
    """Two fresh interpreters, two different hash seeds, one digest.

    In-process comparison cannot catch this class of bug: a set or a dict keyed
    on strings iterates consistently within one process, so generation order
    that leaked from a set would still look deterministic. Randomising
    PYTHONHASHSEED between the two runs is what makes the check real.
    """
    def run(hash_seed: str) -> str:
        env = {**os.environ, "PYTHONHASHSEED": hash_seed}
        out = subprocess.run(
            [sys.executable, "-m", "loadgen.seed", "--dry-run", "--quiet"],
            capture_output=True, text=True, check=True, env=env,
        )
        return out.stdout.strip()

    assert run("0") == run("12345")


def test_different_seed_different_rows():
    other = dataclasses.replace(config.DEFAULT, seed=config.SEED + 1)
    assert fp.digest(fp.canonical_dataset(generate(config.DEFAULT))) != \
        fp.digest(fp.canonical_dataset(generate(other)))


def test_per_table_digests_localise_a_change(ds: Dataset):
    """A drifting table should be named, not just detected."""
    before = fp.per_table_digests(fp.canonical_dataset(ds))
    mutated = dataclasses.replace(ds)
    mutated.patients = [dict(p) for p in ds.patients]
    mutated.patients[0]["city"] = "Somewhere Else"
    after = fp.per_table_digests(fp.canonical_dataset(mutated))
    assert after["patients"] != before["patients"]
    assert after["providers"] == before["providers"]


# --------------------------------------------------------------------------
# counts
# --------------------------------------------------------------------------


def test_row_counts_match_config(ds: Dataset):
    assert ds.counts() == dataclasses.asdict(config.DEFAULT.counts)


def test_counts_are_enforced_not_approximated():
    cfg = dataclasses.replace(
        config.DEFAULT,
        counts=dataclasses.replace(config.DEFAULT.counts, appointments=613, claims=97, notes=311),
    )
    got = generate(cfg).counts()
    assert (got["appointments"], got["claims"], got["notes"]) == (613, 97, 311)


def test_impossible_config_fails_loudly():
    """Fewer appointments than patients who need one is a config error."""
    cfg = dataclasses.replace(
        config.DEFAULT,
        counts=dataclasses.replace(config.DEFAULT.counts, patients=100, appointments=5),
    )
    with pytest.raises(SeedError, match="cannot cover"):
        generate(cfg)


# --------------------------------------------------------------------------
# shape
# --------------------------------------------------------------------------


def _per_patient(ds: Dataset) -> list[int]:
    counts = [0] * len(ds.patients)
    for appt in ds.appointments:
        counts[appt["patient_ix"]] += 1
    return counts


def test_appointment_distribution_is_lumpy(ds: Dataset):
    counts = sorted(_per_patient(ds))
    assert counts[0] == 0, "no patient has zero appointments"
    assert counts[-1] >= 10, "no patient has a long history"
    # Most patients are light users, and the top few carry a real share of the
    # total. A uniform generator fails both halves of this.
    light = sum(1 for c in counts if 0 < c <= 2)
    assert light > len([c for c in counts if c > 0]) / 2
    assert sum(counts[-5:]) / sum(counts) > 0.08


def test_every_provider_is_used(ds: Dataset):
    used = {a["provider_ix"] for a in ds.appointments}
    assert used == set(range(len(ds.providers)))


# --------------------------------------------------------------------------
# the awkward cases
# --------------------------------------------------------------------------


def test_null_emails(ds: Dataset):
    assert sum(1 for p in ds.patients if p["email"] is None) >= config.DEFAULT.edges.null_emails


def test_patient_over_the_safe_harbor_age_cap(ds: Dataset):
    as_of = config.DEFAULT.as_of
    old = [p for p in ds.patients if _age_on(p["date_of_birth"], as_of) > 89]
    assert len(old) >= config.DEFAULT.edges.over_89


def test_zip_starting_with_zero_survives_as_text(ds: Dataset):
    zips = [p["postal_code"] for p in ds.patients if p["postal_code"].startswith("0")]
    assert len(zips) >= config.DEFAULT.edges.leading_zero_zips
    assert all(isinstance(z, str) and len(z.split("-")[0]) == 5 for z in zips)


def test_unicode_in_names(ds: Dataset):
    hits = [p for p in ds.patients
            if _has_non_ascii(p["first_name"], p["middle_name"], p["last_name"])]
    assert len(hits) >= config.DEFAULT.edges.unicode_names
    # More than one script, or a policy that only knows accented Latin passes.
    scripts = {max(ord(c) for c in p["first_name"] + p["last_name"]) > 0x0500 for p in hits}
    assert scripts == {True, False}


def test_decimal_claim_amounts(ds: Dataset):
    amounts = {c["billed_amount"] for c in ds.claims}
    for planted in config.DEFAULT.edges.claim_amounts:
        assert planted in amounts, f"{planted} was not planted"
    assert all(c["billed_amount"].as_tuple().exponent == -2 for c in ds.claims), \
        "an amount is not quantized to cents; Postgres would round it and the digests would diverge"


def test_ssn_formats_vary(ds: Dataset):
    present = [p["ssn"] for p in ds.patients if p["ssn"]]
    assert any("-" in s for s in present)
    assert any(s.isdigit() for s in present)
    assert any(s != s.strip() for s in present), "no SSN has stray whitespace"
    assert sum(1 for p in ds.patients if p["ssn"] is None) >= config.DEFAULT.edges.null_ssns


def test_nested_free_text_in_jsonb(ds: Dataset):
    docs = [a["intake_answers"] for a in ds.appointments]
    assert any(d is None for d in docs)
    assert any(d == {} for d in docs)
    assert any(isinstance(v, dict) for d in docs if d for v in d.values())
    assert any(isinstance(v, list) for d in docs if d for v in d.values())


def test_empty_and_populated_diagnosis_arrays(ds: Dataset):
    empty = sum(1 for c in ds.claims if c["diagnosis_codes"] == [])
    assert empty >= config.DEFAULT.edges.empty_diagnosis_claims
    assert any(len(c["diagnosis_codes"]) > 2 for c in ds.claims)


def test_notes_carry_identifiers_in_prose(ds: Dataset):
    """The hardest column: PHI in running text, not in a column of its own."""
    bodies = "\n".join(n["body"] for n in ds.notes)
    assert "MRN-" in bodies
    assert "555-" in bodies
    assert any(p["last_name"] in bodies for p in ds.patients)
    assert any(p["date_of_birth"].isoformat() in bodies for p in ds.patients)


# --------------------------------------------------------------------------
# internal consistency
# --------------------------------------------------------------------------


def test_foreign_keys_resolve(ds: Dataset):
    for appt in ds.appointments:
        assert 0 <= appt["patient_ix"] < len(ds.patients)
        assert 0 <= appt["provider_ix"] < len(ds.providers)
    for claim in ds.claims:
        assert 0 <= claim["patient_ix"] < len(ds.patients)
        if claim["appointment_ix"] is not None:
            appt = ds.appointments[claim["appointment_ix"]]
            assert appt["patient_ix"] == claim["patient_ix"], "claim billed against another patient's visit"
    for note in ds.notes:
        if note["appointment_ix"] is not None:
            assert ds.appointments[note["appointment_ix"]]["patient_ix"] == note["patient_ix"]


def test_amendments_point_backwards(ds: Dataset):
    amended = [n for n in ds.notes if n["amends_ix"] is not None]
    assert amended, "no note amends another"
    for note in amended:
        target = ds.notes[note["amends_ix"]]
        assert target["patient_ix"] == note["patient_ix"]
        assert target["authored_at"] < note["authored_at"]
        assert target["is_amended"] is True
        assert note["note_type"] == "addendum"
        # The referenced note must already exist when this one is inserted.
        assert note["amends_ix"] < ds.notes.index(note)


def test_appointment_timestamps_agree_with_status(ds: Dataset):
    for appt in ds.appointments:
        status = appt["status"]
        if status in ("scheduled", "cancelled", "no_show"):
            assert appt["checked_in_at"] is None
            assert appt["completed_at"] is None
        if status == "checked_in":
            assert appt["checked_in_at"] is not None
            assert appt["completed_at"] is None
        if status == "completed":
            assert appt["checked_in_at"] is not None
            assert appt["completed_at"] > appt["checked_in_at"]
        if status == "scheduled":
            assert appt["scheduled_at"] > config.DEFAULT.as_of


def test_claim_money_respects_the_table_checks(ds: Dataset):
    for claim in ds.claims:
        for column in ("billed_amount", "allowed_amount", "paid_amount", "patient_responsibility"):
            if claim[column] is not None:
                assert claim[column] >= 0, f"{column} violates the CHECK on claims"
        if claim["allowed_amount"] is not None and claim["paid_amount"] is not None:
            assert claim["paid_amount"] <= claim["allowed_amount"]
        if claim["claim_status"] in ("submitted", "pending"):
            assert claim["adjudicated_at"] is None
        else:
            assert claim["adjudicated_at"] >= claim["submitted_at"]


def test_nothing_predates_enrollment(ds: Dataset):
    for appt in ds.appointments:
        assert appt["scheduled_at"] >= ds.patients[appt["patient_ix"]]["created_at"]
    for note in ds.notes:
        assert note["authored_at"] >= ds.patients[note["patient_ix"]]["created_at"] - timedelta(seconds=1)


def test_natural_keys_are_unique(ds: Dataset):
    """The fingerprint identifies rows by these; a collision would hide a diff."""
    assert len({p["mrn"] for p in ds.patients}) == len(ds.patients)
    assert len({p["npi"] for p in ds.providers}) == len(ds.providers)
    for table, stamp in (("appointments", "scheduled_at"), ("claims", "submitted_at"),
                         ("notes", "authored_at")):
        rows = getattr(ds, table)
        keys = {(r["patient_ix"], r[stamp]) for r in rows}
        assert len(keys) == len(rows), f"{table}: two rows share a natural key"


# --------------------------------------------------------------------------
# against a real database
# --------------------------------------------------------------------------

pg = pytest.mark.skipif(
    not os.environ.get("PIT_TEST_DSN"),
    reason="set PIT_TEST_DSN to run against a live source-pg (make forward)",
)


@pg
def test_round_trip_through_postgres():
    """Seed twice and check the stored rows are identical both times.

    The in-memory tests prove the generator is deterministic. This proves the
    load is too -- that nothing is lost or rounded on the way into Postgres, and
    that a reseeded database is the same database, even though the identity
    sequences have moved on between the two runs.
    """
    import psycopg

    from loadgen.seed import load, reset

    dsn = os.environ["PIT_TEST_DSN"]
    digests = []
    for _ in range(2):
        with psycopg.connect(dsn) as conn:
            reset(conn)
            data = generate(config.DEFAULT)
            load(conn, data)
            stored = fp.canonical_database(conn)
            assert fp.digest(stored) == fp.digest(fp.canonical_dataset(data)), \
                "what Postgres kept differs from what was generated"
            digests.append(fp.digest(stored))
    assert digests[0] == digests[1]


@pg
def test_seed_is_one_transaction():
    """The whole population lands in a single transaction.

    This is what makes the seed a clean baseline for a point-in-time query:
    there is no instant at which the database is half-populated. Since the
    mutation ledger went away, xmin is the evidence -- every row carries the
    inserting transaction id, so one distinct xmin per table means one commit.

    Reading xmin only works before a freeze rewrites it to FrozenTransactionId,
    which will not happen to rows this young.
    """
    import psycopg

    from loadgen.seed import TABLES, load, reset

    with psycopg.connect(os.environ["PIT_TEST_DSN"]) as conn:
        reset(conn)
        data = generate(config.DEFAULT)
        load(conn, data)
        with conn.cursor() as cur:
            seen = set()
            for table in TABLES:
                # xmin is type xid, which has no ordering operator, so DISTINCT
                # cannot sort it. Cast to bigint.
                cur.execute(
                    f"SELECT count(*), count(DISTINCT xmin::text::bigint) FROM {table}")
                total, instants = cur.fetchone()
                assert total == len(getattr(data, table)), f"{table} is missing rows"
                assert instants == 1, f"{table} was written across {instants} transactions"

                cur.execute(f"SELECT DISTINCT xmin::text::bigint FROM {table}")
                seen.add(cur.fetchone()[0])

            # Not just one transaction per table -- one transaction for all of
            # them. Five separate commits would each be a point on the timeline
            # at which the database was half-populated.
            assert len(seen) == 1, f"the population spans {len(seen)} transactions, not 1"
