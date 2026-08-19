"""De-identification transformer: raw.* topics -> clean.* topics.

The policy is the only place a de-identification decision is written down; see
:mod:`deid.policy`. :mod:`deid.ops` is what each decision does -- to the value
and to its Avro type, together, because the schema registry checks the second
against the first on every record. :mod:`deid.schema` is where that becomes
enforcement: the clean schema is derived from ``(raw schema, policy)`` and
registered before a record moves, so a column with no rule halts one topic at
startup instead of leaking through it.
"""

from __future__ import annotations

__all__ = ["avro", "ops", "policy", "schema", "vocab"]
