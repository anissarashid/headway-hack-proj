"""The pit CLI: build a point-in-time de-identified Postgres replica.

Two ideas hold this package together.

**The registry decides what the sink looks like.** The sink's schema is the
post-policy schema -- ``ssn`` does not exist, ``date_of_birth`` is an integer,
every id is text -- and the only place that shape is written down is the clean
Avro schema M4 registered. :mod:`pit.ddl` reads it and emits DDL, so the sink
cannot drift from the policy without the registry saying so first.

**A point in time is an offset manifest, not a timestamp.** :mod:`pit.applier`
records the offsets it applied inside the database it applied them to, which is
what lets M6 replay a bounded range and M7 clone a snapshot that knows where it
was cut.

:mod:`pit.envelope` and the pure half of :mod:`pit.ddl` do the translating and
touch nothing, so both are testable without a broker or a database. The edges are
:mod:`pit.registry` (HTTP), the connection-taking functions in
:mod:`pit.applier` and :mod:`pit.ddl`, and :mod:`pit.tail`, which is the loop that
drives them.
"""

from __future__ import annotations

__all__ = ["applier", "config", "ddl", "envelope", "registry", "tail"]
