"""De-identification transformer: raw.* topics -> clean.* topics.

The policy is the only place a de-identification decision is written down; see
:mod:`deid.policy`. :mod:`deid.ops` is what each decision does -- to the value
and to its Avro type, together, because the schema registry checks the second
against the first on every record. :mod:`deid.schema` is where that becomes
enforcement: the clean schema is derived from ``(raw schema, policy)`` and
registered before a record moves, so a column with no rule halts one topic at
startup instead of leaking through it.

:mod:`deid.envelope` applies all of that to one Debezium change event -- key
included, because the applier upserts on the key and a key that disagrees with
its row image writes duplicate rows. :mod:`deid.runner` is the only module with
edges: it is the one that touches Kafka and the registry, which is what keeps
everything above it testable without a broker.
"""

from __future__ import annotations

__all__ = ["avro", "envelope", "ops", "policy", "runner", "schema", "vocab"]
