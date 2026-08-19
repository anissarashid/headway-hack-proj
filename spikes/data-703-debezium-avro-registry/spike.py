"""DATA-703 spike: does Redpanda's schema registry accept Debezium Avro schemas?

Gates M4–M8. Redpanda's registry is Confluent-API-compatible but has had
reported friction with Debezium's namespaced Avro type names
(redpanda-data/redpanda#4970). This registers a faithful Debezium envelope
schema against the live registry and round-trips a record through fastavro,
checking the five things the ticket calls out:

  1. Nested before/after records with namespaced names survive registration.
  2. `["null", "raw.public.patients.Value"]` unions where `after` REFERENCES the
     named record that `before` DEFINES (Avro named-type reuse).
  3. Logical types from time.precision.mode=connect decode as datetimes/dates,
     not bare longs/ints.
  4. decimal.handling.mode=precise decodes to Decimal.
  5. The io.debezium.connector.postgresql.Source block round-trips with ts_ms.

Run against a port-forwarded registry:
    kubectl -n pit-poc port-forward svc/pit-redpanda 18081:8081 &
    uv run --with fastavro --with requests python spike.py --registry http://localhost:18081
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import sys
from decimal import Decimal

import fastavro
import requests

TOPIC = "raw.public.patients"

# A faithful slice of a Debezium Postgres Avro *value* schema. Field order and
# the named-type reuse mirror what Debezium actually emits: `before` DEFINES the
# `<topic>.Value` record, `after` REFERENCES it by fully-qualified name.
VALUE_SCHEMA = {
    "type": "record",
    "name": "Envelope",
    "namespace": TOPIC,
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Value",
                    "namespace": TOPIC,
                    "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "full_name", "type": ["null", "string"], "default": None},
                        {"name": "ssn", "type": ["null", "string"], "default": None},
                        # time.precision.mode=connect -> real Avro logical types
                        {
                            "name": "date_of_birth",
                            "type": ["null", {"type": "int", "logicalType": "date"}],
                            "default": None,
                        },
                        {
                            "name": "created_at",
                            "type": [
                                "null",
                                {"type": "long", "logicalType": "timestamp-millis"},
                            ],
                            "default": None,
                        },
                        # decimal.handling.mode=precise -> bytes + logicalType decimal
                        {
                            "name": "balance",
                            "type": [
                                "null",
                                {
                                    "type": "bytes",
                                    "logicalType": "decimal",
                                    "precision": 10,
                                    "scale": 2,
                                },
                            ],
                            "default": None,
                        },
                    ],
                },
            ],
            "default": None,
        },
        # The load-bearing reference: same name `before` just defined.
        {"name": "after", "type": ["null", f"{TOPIC}.Value"], "default": None},
        {
            "name": "source",
            "type": {
                "type": "record",
                "name": "Source",
                "namespace": "io.debezium.connector.postgresql",
                "fields": [
                    {"name": "version", "type": "string"},
                    {"name": "connector", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "ts_ms", "type": "long"},
                    {"name": "snapshot", "type": ["null", "string"], "default": None},
                    {"name": "db", "type": "string"},
                    {"name": "schema", "type": "string"},
                    {"name": "table", "type": "string"},
                    {"name": "lsn", "type": ["null", "long"], "default": None},
                    {"name": "txId", "type": ["null", "long"], "default": None},
                ],
            },
        },
        {"name": "op", "type": "string"},
        {"name": "ts_ms", "type": ["null", "long"], "default": None},
    ],
}

# Debezium key schema: the PK, namespaced under the topic.
KEY_SCHEMA = {
    "type": "record",
    "name": "Key",
    "namespace": TOPIC,
    "fields": [{"name": "id", "type": "int"}],
}

COMMIT_TS = 1_755_621_600_000  # a fixed backdated commit time, ms


def register(registry: str, subject: str, schema: dict) -> int:
    """Register a schema under a subject via the Confluent API. Returns id."""
    r = requests.post(
        f"{registry}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": json.dumps(schema), "schemaType": "AVRO"}),
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["id"]


def fetch(registry: str, schema_id: int) -> dict:
    r = requests.get(f"{registry}/schemas/ids/{schema_id}", timeout=15)
    r.raise_for_status()
    return json.loads(r.json()["schema"])


def check(name: str, ok: bool, detail: str = "") -> bool:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""))
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="http://localhost:18081")
    args = ap.parse_args()
    reg = args.registry.rstrip("/")

    # Prefixed so the spike never collides with the real raw.public.patients
    # subjects Debezium registers later. The schema's record names still use the
    # real topic namespace — that is the thing under test, not the subject name.
    subj_v = f"_spike.{TOPIC}-value"
    subj_k = f"_spike.{TOPIC}-key"

    results: list[bool] = []
    print(f"registry: {reg}\n")

    # --- 1/2. Registration of namespaced nested records + named-type reuse ---
    print("registration:")
    try:
        vid = register(reg, subj_v, VALUE_SCHEMA)
        kid = register(reg, subj_k, KEY_SCHEMA)
        results.append(check("value schema registered", True, f"id={vid}"))
        results.append(check("key schema registered", True, f"id={kid}"))
    except requests.HTTPError as e:
        body = e.response.text if e.response is not None else str(e)
        results.append(check("value schema registered", False, body))
        print("\nregistry rejected the schema — this is the gating failure.")
        return 1

    subjects = requests.get(f"{reg}/subjects", timeout=15).json()
    results.append(
        check(
            "both subjects listed",
            subj_v in subjects and subj_k in subjects,
            ", ".join(s for s in subjects if s.startswith("_spike")),
        )
    )

    # Fetch the value schema back and confirm the reference survived. The
    # registry canonicalizes the reference to the *relative* name "Value", which
    # inside namespace raw.public.patients resolves to raw.public.patients.Value
    # — same type. Accept either spelling; the real proof is that fastavro parses
    # the fetched schema and round-trips a populated `after` below.
    fetched = fetch(reg, vid)
    after_type = next(f for f in fetched["fields"] if f["name"] == "after")["type"]
    results.append(
        check(
            "named-type reuse survived (after references before's Value)",
            after_type in (["null", "Value"], ["null", f"{TOPIC}.Value"]),
            json.dumps(after_type),
        )
    )

    # --- 3/4/5. fastavro round-trip with logical types ---------------------
    print("\nfastavro round-trip:")
    parsed = fastavro.parse_schema(fetched)  # must handle the named reference
    results.append(check("fastavro parsed the fetched schema", True))

    record = {
        "before": None,
        "after": {
            "id": 42,
            "full_name": "Jane Doe",
            "ssn": "123-45-6789",
            "date_of_birth": dt.date(1990, 5, 17),
            "created_at": dt.datetime(2026, 8, 19, 14, 0, tzinfo=dt.timezone.utc),
            "balance": Decimal("1234.56"),
        },
        "source": {
            "version": "3.0.0.Final",
            "connector": "postgresql",
            "name": "raw",
            "ts_ms": COMMIT_TS,
            "snapshot": None,
            "db": "pit",
            "schema": "public",
            "table": "patients",
            "lsn": 987654321,
            "txId": 555,
        },
        "op": "c",
        "ts_ms": COMMIT_TS + 5,
    }

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    buf.seek(0)
    out = fastavro.schemaless_reader(buf, parsed)

    after = out["after"]
    results.append(
        check(
            "date_of_birth decodes as datetime.date",
            isinstance(after["date_of_birth"], dt.date)
            and after["date_of_birth"] == dt.date(1990, 5, 17),
            f"{after['date_of_birth']!r}",
        )
    )
    results.append(
        check(
            "created_at decodes as datetime (timestamp-millis)",
            isinstance(after["created_at"], dt.datetime),
            f"{after['created_at']!r}",
        )
    )
    results.append(
        check(
            "balance decodes as Decimal (precise)",
            isinstance(after["balance"], Decimal) and after["balance"] == Decimal("1234.56"),
            f"{after['balance']!r}",
        )
    )
    results.append(
        check(
            "source.ts_ms intact (the PIT timestamp)",
            out["source"]["ts_ms"] == COMMIT_TS,
            str(out["source"]["ts_ms"]),
        )
    )
    results.append(check("op present", out["op"] == "c", out["op"]))

    # Best-effort cleanup so the registry is left as we found it.
    for subj in (subj_v, subj_k):
        try:
            requests.delete(f"{reg}/subjects/{subj}", timeout=15)
            requests.delete(f"{reg}/subjects/{subj}?permanent=true", timeout=15)
        except requests.RequestException:
            pass

    print()
    passed, total = sum(results), len(results)
    print(f"RESULT: {passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
