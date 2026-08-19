"""What the churn loop promises, asserted.

The pure tests cover the two things that are cheap to get wrong and expensive to
notice: the ceilings that keep the run bounded, and the bias that keeps the
population inside its band. Everything else about churn is only true against a
real database -- a mutation that is not in the ledger, or a ledger that does not
replay, cannot be caught in memory -- so those tests are gated on
``PIT_TEST_DSN``.
"""

from __future__ import annotations

import os

import pytest

from loadgen import __main__ as churn
from loadgen import config


# --------------------------------------------------------------------------
# the shape registry
# --------------------------------------------------------------------------


def test_shape_names_are_unique():
    names = [s.name for s in churn.SHAPES]
    assert len(set(names)) == len(names)


def test_every_shape_declares_a_known_kind():
    """The kind is what the population bias keys on; a typo would silently unbias it."""
    assert {s.kind for s in churn.SHAPES} <= {"insert", "update", "delete"}
    for kind in ("insert", "update", "delete"):
        assert any(s.kind == kind for s in churn.SHAPES), f"no {kind} shape"


def test_warmup_covers_every_shape():
    """A short run has to reach all of them, including the cascade deletes.

    The warmup pass is the whole reason a thirty-second run is worth anything:
    without it, purge_patient at weight 1.5 out of ~85 shows up in maybe one run
    in three.
    """
    cfg = churn.ChurnConfig()
    assert cfg.warmup
    assert len(churn.SHAPES) <= cfg.max_txns


def test_the_multi_table_shapes_are_still_registered():
    """--snap-to-txn needs transactions that touch several tables at one tx_at.

    Named rather than counted, because these two are the ones that produce them:
    a patient with their first appointment and note, and a visit closing into a
    note and a claim. Losing either quietly removes the only data that flag can
    be tested against.
    """
    names = {s.name for s in churn.SHAPES}
    assert {"register_patient", "complete_visit"} <= names


# --------------------------------------------------------------------------
# staying bounded
# --------------------------------------------------------------------------


def test_every_ceiling_is_finite_by_default():
    """There is no unbounded mode. Cleaned topics keep everything this produces."""
    cfg = churn.ChurnConfig()
    for ceiling in (cfg.duration, cfg.max_txns, cfg.max_ledger_rows):
        assert 0 < ceiling < float("inf")


def test_bands_bracket_the_configured_counts():
    counts = config.DEFAULT.counts
    bands = churn.ChurnConfig().bands()
    assert set(bands) == {"patients", "appointments", "claims", "notes"}
    for table, (low, high) in bands.items():
        target = getattr(counts, table)
        assert low < target < high


def test_bands_do_not_depend_on_the_current_population():
    """Anchored to the config, not to what is in the database.

    A band derived from the observed row counts would ratchet upward every time
    churn ran, and ten runs would leave the database at 1.4**10 times its size.
    """
    assert churn.ChurnConfig().bands() == churn.ChurnConfig().bands()


@pytest.mark.parametrize(
    "counts, expected",
    [
        ({"patients": 175, "appointments": 630, "claims": 350, "notes": 525}, 0.0),
        ({"patients": 350, "appointments": 630, "claims": 350, "notes": 525}, 1.0),
    ],
)
def test_fill_is_zero_at_the_floor_and_one_at_the_ceiling(counts, expected):
    assert churn._fill(counts, churn.ChurnConfig().bands()) == pytest.approx(expected)


def test_the_fullest_table_governs():
    """One table running away is the failure worth stopping; an average hides it."""
    bands = churn.ChurnConfig().bands()
    low = {t: lo for t, (lo, _hi) in bands.items()}
    assert churn._fill(low, bands) == pytest.approx(0.0)
    assert churn._fill({**low, "notes": 2000}, bands) > 1.0


def test_weights_clamp_hard_outside_the_band():
    """Above the band inserts nearly stop and deletes are favoured; below it, no deletes at all."""
    over = churn._weights(1.4)
    assert over["delete"] > over["insert"]
    under = churn._weights(-0.3)
    assert under["delete"] == 0.0
    assert under["insert"] > under["update"]


def test_weights_cross_over_inside_the_band():
    """The bias has to actually change direction, not just tilt."""
    empty, full = churn._weights(0.0), churn._weights(1.0)
    assert empty["insert"] > empty["delete"]
    assert full["delete"] > full["insert"]


# --------------------------------------------------------------------------
# cli
# --------------------------------------------------------------------------


@pytest.mark.parametrize("text, seconds", [("90", 90), ("30s", 30), ("5m", 300), ("1h", 3600)])
def test_duration_suffixes(text, seconds):
    assert churn._seconds(text) == seconds


def test_a_duration_that_is_not_one_is_rejected():
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        churn._seconds("a while")


def test_ledger_sql_only_interpolates_validated_identifiers():
    """Table names come from the database, so they are checked before interpolation."""
    sql = churn._ledger_sql(["patients", "notes"])
    assert "patients_history" in sql and "notes_history" in sql
    assert sql.count("%(txid)s") == 2
    # Ordered by the transaction's own progress, not by table.
    assert sql.rstrip().endswith("ORDER BY stmt_at, table_name, history_id")


# --------------------------------------------------------------------------
# ledger accounting
# --------------------------------------------------------------------------


def _result(shape: str, entries: list[tuple[str, str]], direct: list[tuple[str, str]]):
    from datetime import datetime, timezone

    return churn.TxnResult(
        shape=shape,
        txid=1,
        tx_at=datetime(2026, 8, 19, tzinfo=timezone.utc),
        direct=direct,
        entries=[churn.LedgerEntry(table=t, op=op, pk={"id": i})
                 for i, (t, op) in enumerate(entries)],
    )


def test_cascaded_counts_ledger_rows_the_shape_did_not_issue():
    """Deleting an appointment is one statement and three tables of history."""
    result = _result(
        "detach_appointment",
        entries=[("appointments", "D"), ("claims", "D"), ("notes", "U")],
        direct=[("appointments", "D")],
    )
    assert result.cascaded == 2
    assert result.tables == ["appointments", "claims", "notes"]


def test_a_shape_that_fans_out_nowhere_reports_no_cascade():
    result = _result("check_in", entries=[("appointments", "U")], direct=[("appointments", "U")])
    assert result.cascaded == 0
    assert result.tables == ["appointments"]


def test_out_of_order_commit_timestamps_are_counted_not_ignored():
    """Monotonic tx_at is a property of one writer, so it is checked, not assumed."""
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 8, 19, tzinfo=timezone.utc)
    totals = churn.Totals()
    for offset in (0, 1, -1, 2):
        result = churn.TxnResult(
            shape="check_in", txid=offset, tx_at=base + timedelta(seconds=offset),
            direct=[("appointments", "U")],
            entries=[churn.LedgerEntry("appointments", "U", {"appointment_id": 1})],
        )
        totals.record(result)
    assert totals.txns == 4
    assert totals.out_of_order == 1
    assert len(totals.instants) == 4


def test_delta_tracking_follows_inserts_and_deletes():
    counts = {"patients": 10, "appointments": 20}
    after = churn._apply_delta(counts, _result(
        "register_patient",
        entries=[("patients", "I"), ("appointments", "I")],
        direct=[("patients", "I"), ("appointments", "I")],
    ))
    assert after == {"patients": 11, "appointments": 21}

    after = churn._apply_delta(after, _result(
        "purge_patient",
        entries=[("patients", "D"), ("appointments", "D"), ("appointments", "D")],
        direct=[("patients", "D")],
    ))
    assert after == {"patients": 10, "appointments": 19}


# --------------------------------------------------------------------------
# against a real database
# --------------------------------------------------------------------------

pg = pytest.mark.skipif(
    not os.environ.get("PIT_TEST_DSN"),
    reason="set PIT_TEST_DSN to run against a live source-pg (make forward)",
)


@pytest.fixture
def seeded():
    """A freshly seeded database and an autocommit connection onto it."""
    import psycopg

    from loadgen.seed import generate, load, reset

    with psycopg.connect(os.environ["PIT_TEST_DSN"]) as setup:
        reset(setup)
        load(setup, generate(config.DEFAULT))
        setup.commit()

    with psycopg.connect(os.environ["PIT_TEST_DSN"], autocommit=True) as conn:
        yield conn


@pg
def test_a_short_run_moves_the_rows_and_fills_the_ledger(seeded):
    cfg = churn.ChurnConfig(max_txns=40, duration=120.0, rate=60.0)
    before = churn.table_counts(seeded)
    totals = churn.run(seeded, cfg, quiet=True)
    after = churn.table_counts(seeded)

    assert totals.txns == 40
    assert totals.ledger_rows >= totals.txns, "a transaction wrote nothing to the ledger"
    assert after != before, "40 transactions left every row count unchanged"
    # The warmup pass runs every shape once, so a run this short still produces
    # the cascade deletes and the multi-table inserts.
    assert set(totals.by_shape) == {s.name for s in churn.SHAPES}
    assert totals.multi_table > 0
    assert totals.cascaded > 0


@pg
def test_commit_timestamps_strictly_increase(seeded):
    """One writer, one transaction at a time: every tx_at is a distinct point.

    This is what makes M6 able to pick a T at all. Two transactions sharing a
    tx_at would be two states no point-in-time query could tell apart.
    """
    totals = churn.run(seeded, churn.ChurnConfig(max_txns=30, rate=60.0), quiet=True)
    assert totals.out_of_order == 0
    assert len(totals.instants) == totals.txns


@pg
def test_each_transaction_is_its_own_transaction(seeded):
    """The savepoint trap: without autocommit the whole run lands at one tx_at.

    psycopg only turns a transaction block into a real BEGIN/COMMIT when the
    connection is in autocommit mode. Otherwise the first read opens an implicit
    transaction, every block after it becomes a savepoint inside it, and the run
    produces a timeline with exactly one point in it.
    """
    import psycopg

    from loadgen.seed import SeedError

    totals = churn.run(seeded, churn.ChurnConfig(max_txns=20, rate=60.0), quiet=True)
    with seeded.cursor() as cur:
        cur.execute("SELECT count(DISTINCT txid) FROM appointments_history")
        assert cur.fetchone()[0] > 1

    with psycopg.connect(os.environ["PIT_TEST_DSN"]) as blocking:
        with pytest.raises(SeedError, match="autocommit"):
            churn.run(blocking, churn.ChurnConfig(max_txns=1), quiet=True)
    assert totals.txns == 20


@pg
def test_the_ledger_replays_to_the_live_tables(seeded):
    """The oracle check, at the one T where the answer can be verified independently.

    Replaying the ledger to its own newest tx_at -- newest entry per key, absent
    if it was a delete -- has to reproduce the live table exactly. That is the
    query M8 will run at an arbitrary T; running it at T = now is the only case
    with something to compare against.
    """
    churn.run(seeded, churn.ChurnConfig(max_txns=60, rate=60.0), quiet=True)
    with seeded.cursor() as cur:
        for table in churn.captured_tables(seeded):
            check = churn.check_table(cur, table)
            assert check.ok, f"{table}: " + "; ".join(check.problems())


@pg
def test_cascades_reach_the_ledger(seeded):
    """One DELETE, three tables of history.

    claims.appointment_id cascades and notes.appointment_id goes to NULL, so the
    notes show up as updates. A pipeline that only watches for deletes gets those
    wrong, and this is the assertion that the evidence is in the ledger to catch
    it with.
    """
    churn.run(seeded, churn.ChurnConfig(max_txns=len(churn.SHAPES), rate=60.0), quiet=True)
    with seeded.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM notes_history n
             WHERE n.op = 'U'
               AND n.before_row->>'appointment_id' IS NOT NULL
               AND n.after_row->>'appointment_id' IS NULL
        """)
        assert cur.fetchone()[0] > 0, "no note was orphaned by a deleted appointment"
        cur.execute("SELECT count(*) FROM claims_history WHERE op = 'D'")
        assert cur.fetchone()[0] > 0, "no claim was cascaded away"


@pg
def test_refusing_to_churn_an_empty_database(seeded):
    """There is nothing to mutate before the seed has run, and saying so beats a stack trace."""
    from loadgen.seed import SeedError, reset

    reset(seeded)
    with pytest.raises(SeedError, match="empty"):
        churn.run(seeded, churn.ChurnConfig(max_txns=1), quiet=True)


@pg
def test_verify_passes_after_a_run(seeded):
    churn.run(seeded, churn.ChurnConfig(max_txns=40, rate=60.0), quiet=True)
    assert churn.verify(seeded) is True
