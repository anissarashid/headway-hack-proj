"""``pit tail`` -- keep ``pit_base`` current.

What a finished tail does is three things in a loop: reconcile the sink schema
against the registry, consume a batch of cleaned records, apply it with the
offsets in the same transaction. The middle step is the one that needs a Kafka
consumer, and that needs DATA-712 to register real ``clean.*`` subjects and put
records behind them. The other two work now, so this runs them now.

That is not a placeholder standing in for the loop; it *is* the loop, with one
step not yet plugged in. :func:`consume_and_apply` is the seam, and it says what
it is rather than pretending: while it returns nothing, the reconcile step still
does real work, because the sink's schema has to track the registry as M4
registers new schema versions and the applier will need the tables to exist
before it writes to them.

Two behaviours matter more than they look:

**No clean subjects is a wait, not a crash.** Until the transformer runs there is
nothing to tail. A Deployment that exits non-zero on that would CrashLoopBackOff
and bury the reason in a restart count; one that waits and says so is readable in
``kubectl logs``. This mirrors M4's own shape -- halt the affected topic, keep
everything else running -- rather than treating a not-yet state as a failure.

**Scaling to zero and back has to resume, not restart.** M7's snapshot CronJob
scales this Deployment to 0 so ``CREATE DATABASE ... TEMPLATE pit_base`` can take
its clone, then back to 1. Position lives in ``pit_meta.applied_offsets`` inside
``pit_base``, so a restarted tail reads where it got to out of the database it
writes to and the two cannot disagree.
"""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass, field
from typing import Sequence

import psycopg

from . import applier, config, ddl, registry

log = logging.getLogger("pit.tail")

# How long to wait between passes. Short enough that a newly registered schema
# reaches the sink promptly, long enough not to hammer the registry: the reconcile
# is a handful of catalog reads and a registry GET per topic.
DEFAULT_INTERVAL_SECONDS = 15.0


@dataclass
class Stopping:
    """Set by SIGTERM/SIGINT so a pass finishes before the process exits.

    Kubernetes sends SIGTERM and waits out the grace period, so the useful
    behaviour is to stop *between* passes rather than half way through one. A
    partially applied batch would still be correct -- the transaction is all or
    nothing -- but an interrupted reconcile is a confusing thing to find in a log.
    """

    requested: bool = False
    signals: tuple[int, ...] = (signal.SIGTERM, signal.SIGINT)

    def install(self) -> Stopping:
        for number in self.signals:
            try:
                signal.signal(number, self._handle)
            except ValueError:
                # Not the main thread. Tests call the loop directly.
                pass
        return self

    def _handle(self, number: int, _frame: object) -> None:
        log.info("signal %s received; finishing this pass and stopping", signal.Signals(number).name)
        self.requested = True


@dataclass
class Pass:
    """What one iteration of the loop found and did."""

    topics: tuple[str, ...] = ()
    statements: tuple[str, ...] = ()
    applied: applier.Applied = field(default_factory=applier.Applied)
    offsets: dict[tuple[str, int], int] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)

    @property
    def waiting(self) -> bool:
        return not self.topics


def consume_and_apply(conn, tables: Sequence[ddl.Table]) -> applier.Applied:
    """Consume a batch of cleaned records and apply it. **Not yet implemented.**

    The seam. When DATA-715's consumer lands it goes here, and the shape it has to
    fit is already fixed by what exists around it:

    * seek each partition to ``pit_meta.applied_offsets``, or to the beginning
      when there is no row for it -- :func:`pit.applier.applied_offsets` reads it;
    * translate each record with :func:`pit.envelope.translate`, which needs the
      message key as well as the value;
    * hand the batch and the offsets it ends at to :func:`pit.applier.apply`,
      which commits both together;
    * commit the Kafka offsets *after* that returns. A crash in between replays
      the tail of the batch, which is harmless because every statement is
      idempotent;
    * on :class:`pit.envelope.UnknownField`, re-run :func:`pit.ddl.ensure_schema`
      and retry the batch -- that is a policy that just started covering a new
      source column, not an error.

    Returns an empty result rather than raising, so the reconcile half of the loop
    keeps working in the meantime.
    """
    return applier.Applied()


def one_pass(conn, client: registry.Registry) -> Pass:
    """Reconcile the schema, then apply whatever is waiting.

    Schema first, always. A record cannot be applied to a table that does not
    exist, and the whole design has the registry decide what the tables are.
    """
    topics = tuple(client.clean_topics())
    if not topics:
        return Pass()

    tables = [ddl.read_table(*client.schemas_for(topic)) for topic in topics]
    statements = ddl.ensure_schema(conn, tables)
    applied = consume_and_apply(conn, tables)

    return Pass(
        topics=topics,
        statements=tuple(statements),
        applied=applied,
        offsets=applier.applied_offsets(conn),
        rows=applier.row_counts(conn, tables),
    )


def describe(result: Pass) -> str:
    """One log line per pass. Readable in `kubectl logs`, which is where it lands."""
    if result.waiting:
        return "no clean.* subjects registered yet; waiting"

    # ddl.changes filters out the bookkeeping statements, which ensure_schema
    # emits unconditionally -- counting those would report a schema change on
    # every pass forever.
    structural = ddl.changes(result.statements)
    parts = [f"{len(result.topics)} topics"]
    if structural:
        parts.append(f"{len(structural)} schema changes")
    if result.applied.total:
        parts.append(f"{result.applied.upserts} upserts, {result.applied.deletes} deletes")
    if result.rows:
        parts.append(f"{sum(result.rows.values())} rows")
    return ", ".join(parts)


def run(
    database: str,
    client: registry.Registry,
    *,
    interval: float = DEFAULT_INTERVAL_SECONDS,
    once: bool = False,
    stopping: Stopping | None = None,
) -> int:
    """The loop. Returns an exit code.

    Exits 0 on a signal, so a scale-to-zero is a clean shutdown rather than
    something that looks like a failure in the pod's status.
    """
    stopping = stopping or Stopping().install()
    log.info("tailing into %s, registry %s, every %.0fs", database, client.base_url, interval)

    waiting_logged = False
    while True:
        try:
            with psycopg.connect(config.sink_dsn(database)) as conn:
                result = one_pass(conn, client)
        except (psycopg.OperationalError, registry.RegistryError) as failure:
            # The sink or the registry is briefly unreachable -- a rolling
            # restart, a not-yet-ready broker. Retrying is right; exiting would
            # turn a blip into a CrashLoopBackOff.
            log.warning("%s; retrying in %.0fs", failure, interval)
        else:
            # Say "waiting" once, then stay quiet about it. Repeating it every
            # interval buries the line that matters when the topics do appear.
            if result.waiting:
                if not waiting_logged:
                    log.info("%s", describe(result))
                    waiting_logged = True
            else:
                waiting_logged = False
                log.info("%s", describe(result))
                for statement in ddl.changes(result.statements):
                    log.info("  %s", statement.split("\n")[0])

        if once or stopping.requested:
            return 0
        time.sleep(interval)
