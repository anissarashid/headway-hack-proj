"""Deterministic synthetic load for the PIT clinic schema.

Nothing produced by this package is real. See ``seed.py`` for the generator that
builds the initial population, ``__main__.py`` for the churn loop that gives it a
timeline, and ``fingerprint.py`` for the digest that proves two seed runs agree.
"""

__all__ = ["config", "fingerprint", "seed", "vocab"]
