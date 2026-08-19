"""De-identification transformer: raw.* topics -> clean.* topics.

The policy is the only place a de-identification decision is written down; see
:mod:`deid.policy`.
"""

from __future__ import annotations

__all__ = ["policy"]
