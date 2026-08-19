"""DATA-711 acceptance check: does the derived clean schema register?

`deid.schema.derive_clean_schema` is a pure function and its unit tests prove
what it does. What they cannot prove is that a real Confluent-API registry
accepts the result -- and the one way this derivation fails in production is the
one a per-field unit test cannot see: `before` and `after` are the same Avro
named type, so emitting the derived record in full at both is a duplicate
fullname that only the registry complains about, at registration, after the
transformer has started.

So this registers the derived schema for every table in the policy against a
live registry, fetches it back, and parses the fetched form with fastavro --
which is the check that the named reference still resolves after the registry
canonicalizes it to the relative spelling (see spikes/data-703, "the one
caveat").

Where the raw schema comes from:

  * the live `raw.<table>-value` subject, if Debezium has registered one. That
    is the real input, and this check gets stronger the moment M3 is deployed.
  * otherwise `deid.schema.DEMO_RAW_VALUE_SCHEMA`, the hand-written stand-in for
    public.patients. Which one was used is printed per table, because "passed
    against a fixture" and "passed against what the connector emits" are
    different claims.

Subjects are registered under a `_check.` prefix and deleted afterwards, so the
registry is left as it was found and no premature `clean.*` subject blocks the
transformer from registering the real one later. The record *names* inside the
schema are the real clean-topic names -- those are the thing under test.

    make forward                 # or port-forward the registry yourself
    make schema-check
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "deid" / "src"))

import fastavro  # noqa: E402
import requests  # noqa: E402

from deid import ops, policy, schema  # noqa: E402

DEFAULT_REGISTRY = os.environ.get("PIT_REGISTRY_URL", "http://localhost:8081")
DEFAULT_POLICY = REPO / "deid" / "policy" / "clinic.yml"

# Prefixed so this never creates the subject the transformer will own.
CHECK_PREFIX = "_check."

# Key material for a schema derivation. The salt does not reach the derived
# type -- an op's two halves come out of one build and only the value half reads
# it -- but there is deliberately no way to build one half alone, so a salt has
# to be supplied. Named so it cannot be mistaken for the real one.
CHECK_KEYS = ops.Keys(
    salt=b"data-711-schema-check-salt-not-a-real-one",
    reference_date=date(2026, 8, 1),
)


def raw_subject(table: str) -> str:
    return f"raw.{table}-value"


def clean_topic(table: str) -> str:
    return f"clean.{table}"


def get_json(registry: str, path: str) -> object | None:
    """GET, returning None for a 404 rather than raising."""
    response = requests.get(f"{registry}{path}", timeout=15)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def fetch_raw_schema(registry: str, table: str) -> dict | None:
    """The raw value schema Debezium registered for a table, if it has."""
    latest = get_json(registry, f"/subjects/{raw_subject(table)}/versions/latest")
    return None if latest is None else json.loads(latest["schema"])


def register(registry: str, subject: str, avro_schema: object) -> int:
    response = requests.post(
        f"{registry}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": json.dumps(avro_schema), "schemaType": "AVRO"}),
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["id"]


def delete(registry: str, subject: str) -> None:
    for suffix in ("", "?permanent=true"):
        try:
            requests.delete(f"{registry}/subjects/{subject}{suffix}", timeout=15)
        except requests.RequestException:
            pass


def row_image(value_schema: dict) -> dict:
    """The row-image record a value schema defines, wherever it defines it."""
    for field in value_schema["fields"]:
        if field["name"] not in schema.ROW_IMAGE_FIELDS:
            continue
        branches = field["type"] if isinstance(field["type"], list) else [field["type"]]
        for branch in branches:
            if isinstance(branch, dict) and branch.get("type") == "record":
                return branch
    raise AssertionError("the fetched schema defines no row image")


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
    return ok


def check_table(registry: str, parsed: policy.Policy, table: str) -> list[bool] | None:
    """The checks for one table, or None if there was no raw schema to check."""
    print(f"\n{table}:")
    results: list[bool] = []

    live = fetch_raw_schema(registry, table)
    if live is not None:
        raw_schema, origin = live, f"live {raw_subject(table)}"
    elif table == schema.DEMO_TABLE:
        raw_schema, origin = schema.DEMO_RAW_VALUE_SCHEMA, "built-in stand-in (M3 not deployed)"
    else:
        print(f"  [SKIP] no {raw_subject(table)} in the registry and no stand-in for it")
        return None
    print(f"  raw schema: {origin}")

    topic = clean_topic(table)
    try:
        derived = schema.derive_clean_schema(
            raw_schema,
            parsed.table(table),
            keys=CHECK_KEYS,
            on_uncovered=parsed.on_uncovered_column,
            namespace=topic,
            source=parsed.source,
        )
    except (policy.PolicyError, schema.SchemaError) as exc:
        # The design working, and a failure of this check either way: a topic
        # that would halt at startup is a topic with no clean schema.
        results.append(check("derived", False, f"HALT: {exc}"))
        return results

    value_name = f"{topic}.Value"
    results.append(
        check(
            "row image defined once, referenced once",
            schema.definitions(derived).count(value_name) == 1
            and schema.references(derived).count(value_name) == 1,
            f"definitions={schema.definitions(derived)}",
        )
    )

    subject = f"{CHECK_PREFIX}{topic}-value"
    try:
        schema_id = register(registry, subject, derived)
        results.append(check("registered", True, f"subject={subject} id={schema_id}"))
    except requests.HTTPError as exc:
        body = exc.response.text if exc.response is not None else str(exc)
        results.append(check("registered", False, body))
        return results

    try:
        fetched = json.loads(get_json(registry, f"/schemas/ids/{schema_id}")["schema"])
        # The DATA-703 caveat: the registry canonicalizes the reference to its
        # relative spelling. Compare resolved names, never JSON.
        results.append(
            check(
                "named reference survived the round trip",
                schema.references(fetched) == (value_name,),
                json.dumps(
                    next(f["type"] for f in fetched["fields"] if f["name"] == "after")
                ),
            )
        )
        fastavro.parse_schema(fetched)
        results.append(check("fastavro parsed the fetched schema", True))

        rules = parsed.table(table).rules
        clean_columns = {field["name"] for field in row_image(fetched)["fields"]}
        absent = set(rules) - clean_columns
        should_be_absent = {
            column for column, rule in rules.items() if isinstance(rule.op, policy.Drop)
        }
        results.append(
            check(
                "exactly the columns the policy drops are absent",
                absent == should_be_absent,
                f"absent={', '.join(sorted(absent)) or 'nothing'}",
            )
        )
    finally:
        delete(registry, subject)

    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="scripts/register-clean-schema.py", description=__doc__.splitlines()[0]
    )
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--policy", default=str(DEFAULT_POLICY))
    parser.add_argument(
        "--table",
        action="append",
        default=None,
        help="check only this table (repeatable); default: every table in the policy",
    )
    args = parser.parse_args(argv)
    registry = args.registry.rstrip("/")

    try:
        parsed = policy.load_policy(args.policy)
    except policy.PolicyError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1

    tables = args.table or sorted(parsed.tables)
    unknown = [table for table in tables if parsed.table(table) is None]
    if unknown:
        print(f"INVALID: {parsed.source} has no rules for {', '.join(unknown)}", file=sys.stderr)
        return 1

    print(f"registry: {registry}")
    print(f"policy:   {args.policy} ({parsed.on_uncovered_column.value})")
    try:
        requests.get(f"{registry}/subjects", timeout=5).raise_for_status()
    except requests.RequestException as exc:
        print(f"\nFAIL: registry unreachable at {registry}: {exc}", file=sys.stderr)
        print("Run `make forward` first, or pass --registry.", file=sys.stderr)
        return 1

    results: list[bool] = []
    skipped: list[str] = []
    for table in tables:
        table_results = check_table(registry, parsed, table)
        if table_results is None:
            skipped.append(table)
        else:
            results.extend(table_results)

    passed, total = sum(results), len(results)
    print(f"\nRESULT: {passed}/{total} checks passed")
    if skipped:
        # Said out loud, because "5/5 passed" next to four silently skipped
        # tables reads as coverage it is not. Deploy M3 and this list empties.
        print(
            f"NOT CHECKED against the registry: {', '.join(skipped)} — Debezium has "
            "registered no raw schema for them. Their derivations are covered by "
            "deid/tests/test_schema.py; rerun this after `make install` with connect up."
        )
    return 0 if results and passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
