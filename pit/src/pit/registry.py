"""Read-only client for the schema registry.

The registry is the single versioned source of truth for what the sink looks
like, so this module is how the sink's shape is discovered -- not the source's
``information_schema``, which describes the pre-policy schema and would have to
re-apply the policy to be useful.

Read-only on purpose. M4 registers schemas; M5 only ever reads them. A client
that cannot write is a client that cannot corrupt the artifact the whole de-id
argument rests on.

Stdlib ``urllib`` rather than ``requests``: the entire protocol used here is one
GET returning a JSON envelope whose ``schema`` field is itself a JSON string.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass

# Registry subjects for a topic `clean.public.patients` are
# `clean.public.patients-key` and `-value`. Confluent's default naming strategy,
# which Redpanda's Confluent-compatible registry follows.
KEY_SUFFIX = "-key"
VALUE_SUFFIX = "-value"

# The prefix M4's cleaned topics carry. `clean.public.patients` -> the sink's
# `public.patients`.
CLEAN_PREFIX = "clean."


class RegistryError(RuntimeError):
    """The registry could not be reached, or answered with something unusable."""


@dataclass(frozen=True)
class Registry:
    """The subset of the registry API this project reads."""

    base_url: str
    timeout: float = 10.0

    def _get(self, path: str) -> object:
        url = f"{self.base_url}{path}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as failure:
            raise RegistryError(f"{url} returned HTTP {failure.code}") from failure
        except (urllib.error.URLError, TimeoutError) as failure:
            raise RegistryError(
                f"could not reach the registry at {url}: {failure}. "
                f"If you are on a laptop, `make forward` maps it to localhost:8081."
            ) from failure

    def subjects(self) -> list[str]:
        subjects = self._get("/subjects")
        if not isinstance(subjects, list):
            raise RegistryError(f"/subjects returned {type(subjects).__name__}, expected a list")
        return [s for s in subjects if isinstance(s, str)]

    def latest(self, subject: str) -> dict:
        """The latest registered version of ``subject``, parsed.

        The registry wraps the schema as a JSON *string* inside a JSON object,
        so this unwraps twice. Compare schemas by parsed form, never by string:
        the registry canonicalizes a fully-qualified named-type reference to its
        relative form, so the JSON it returns is not the JSON that was sent.
        See spikes/data-703-debezium-avro-registry/FINDINGS.md.
        """
        payload = self._get(f"/subjects/{subject}/versions/latest")
        if not isinstance(payload, dict) or "schema" not in payload:
            raise RegistryError(f"{subject} returned no schema field")
        try:
            schema = json.loads(payload["schema"])
        except (TypeError, json.JSONDecodeError) as failure:
            raise RegistryError(f"{subject} carries an unparseable schema") from failure
        if not isinstance(schema, dict):
            raise RegistryError(f"{subject} is not a record schema")
        return schema

    def clean_topics(self) -> list[str]:
        """Every `clean.*` topic with both a key and a value schema registered.

        Both halves are required. A topic with only one is M4 mid-startup, or M4
        having halted that topic -- either way there is not enough to build a
        table from, and inventing the missing half is how a sink ends up with the
        wrong primary key.
        """
        subjects = set(self.subjects())
        topics = sorted(
            subject[: -len(VALUE_SUFFIX)]
            for subject in subjects
            if subject.startswith(CLEAN_PREFIX) and subject.endswith(VALUE_SUFFIX)
        )
        return [topic for topic in topics if f"{topic}{KEY_SUFFIX}" in subjects]

    def schemas_for(self, topic: str) -> tuple[dict, dict]:
        """``(key_schema, value_schema)`` for one topic."""
        return self.latest(f"{topic}{KEY_SUFFIX}"), self.latest(f"{topic}{VALUE_SUFFIX}")
