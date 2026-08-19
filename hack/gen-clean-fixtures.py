#!/usr/bin/env python3
"""Derive the clean Avro schema fixtures that M5's tests are written against.

M5 reads `clean.public.*` schemas from the registry, and M4 is what registers
them. Until M4 lands there is nothing to read, so the fixtures stand in for it.

They are *derived*, not hand-authored, because every input needed to derive them
faithfully is already merged: the raw schemas Debezium actually registered (in
pit/tests/fixtures/raw/, fetched from the live registry), the policy
(deid/policy/clinic.yml), and the two halves of each op (deid.ops.build). So
rather than transcribe what we think M4 will emit, this runs the same derivation
M4 will run and writes the answer down.

That makes the fixtures a faithful stand-in for DATA-711's output, not a
specification of it. ddl.py and envelope.py take schema dicts, so if M4 emits a
shape this does not predict, the fixtures change and the code does not.

Usage:
    ./deid/.venv/bin/python hack/gen-clean-fixtures.py            # from committed raw
    ./deid/.venv/bin/python hack/gen-clean-fixtures.py --refresh-raw   # re-fetch raw first

`make fixtures` wraps the first form. The second needs `make forward` running,
and rewrites pit/tests/fixtures/raw/ from whatever the registry currently holds.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
import urllib.request
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "deid" / "src"))

from deid import ops, policy  # noqa: E402  (after sys.path)

TABLES = ("patients", "providers", "appointments", "claims", "notes")
RAW = REPO / "pit" / "tests" / "fixtures" / "raw"
CLEAN = REPO / "pit" / "tests" / "fixtures" / "clean"

# Fixed inputs. The salt and reference date only affect *values*, never the
# derived types, so any constant does here -- and a constant keeps the output
# byte-identical between runs.
KEYS = ops.Keys(salt=b"fixture-generation-not-a-real-salt", reference_date=date(2026, 8, 19))

# Columns whose derivation fails today, with the reason and the type M4 will
# produce once it is fixed. Every entry is a known defect being worked around,
# not a shrug: a column that fails without an entry here stops this script.
#
#   patients.date_of_birth -- deid/src/deid/avro.py knows the logical-type name
#   `io.debezium.time.Date`, but the connector runs `time.precision.mode=connect`
#   (charts/pit/charts/connect/connectors/source-pg.json), so Debezium emits
#   `org.apache.kafka.connect.data.Date` instead and `generalize` refuses the
#   column. avro.py already uses the Connect spelling for DECIMAL, so the names
#   were half-adopted. Against the name it expects, `generalize to: birth_year`
#   derives `["null", "int"]` -- verified -- which is what this substitutes.
PATCHES: dict[tuple[str, str], tuple[object, str]] = {
    ("public.patients", "date_of_birth"): (
        ["null", "int"],
        "avro.py knows io.debezium.time.Date; the connector emits "
        "org.apache.kafka.connect.data.Date. See pit/tests/fixtures/README.md.",
    ),
}


def fetch_raw(registry: str) -> None:
    """Re-fetch the raw fixtures from a live registry."""
    RAW.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        for kind in ("key", "value"):
            subject = f"raw.public.{table}-{kind}"
            url = f"{registry}/subjects/{subject}/versions/latest"
            with urllib.request.urlopen(url) as response:
                schema = json.loads(json.load(response)["schema"])
            write(RAW / f"public.{table}-{kind}.json", schema)
            print(f"  raw   {subject}")


def write(path: Path, schema: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")


def rename_namespaces(node: object) -> object:
    """Rewrite `raw.*` Avro names to `clean.*`, in place, recursively.

    Only `namespace` and `connect.name` carry them. Scoped to names that start
    with `raw.` so the `source` block's own namespace
    (`io.debezium.connector.postgresql`) is left exactly as it is -- PIT depends
    on that block, and a policy rule may never be written against it.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("namespace", "connect.name") and isinstance(value, str):
                if value.startswith("raw."):
                    node[key] = "clean." + value[len("raw.") :]
            else:
                rename_namespaces(value)
    elif isinstance(node, list):
        for item in node:
            rename_namespaces(item)
    return node


def value_record(envelope: dict) -> dict:
    """The record definition the envelope carries.

    `before` holds the definition and `after` is a name reference to it. That is
    the shape the registry actually returned for all five tables -- the project
    description's "Avro named-type reuse" guardrail states it the other way
    round, so this reads the definition wherever it is rather than trusting
    either spelling.
    """
    for name in ("before", "after"):
        field = next(f for f in envelope["fields"] if f["name"] == name)
        for branch in field["type"] if isinstance(field["type"], list) else [field["type"]]:
            if isinstance(branch, dict) and branch.get("type") == "record":
                return branch
    raise SystemExit("no record definition in before/after -- the envelope shape changed")


def derive_field(table: str, column: str, raw_type: object) -> object | None:
    """The clean type for one column, or None if the policy drops it."""
    rule = policy.load_policy(REPO / "deid" / "policy" / "clinic.yml").rule_for(table, column)
    if rule is None:
        raise SystemExit(
            f"{table}.{column} has no policy rule. The policy says "
            f"on_uncovered_column: halt_topic, so M4 would halt this topic and there "
            f"would be no clean schema to derive. Add a rule before regenerating."
        )
    try:
        clean = ops.build(rule, raw_type, keys=KEYS).derive_type(raw_type)
    except ops.IncompatibleColumnError as failure:
        patch = PATCHES.get((table, column))
        if patch is None:
            raise SystemExit(
                f"{table}.{column}: {failure}\n\n"
                f"No patch is recorded for this column. Either fix the op, or add an "
                f"entry to PATCHES in {Path(__file__).name} explaining why the "
                f"substitute type is right. Silently guessing a type here would make "
                f"every M5 test that depends on it meaningless."
            ) from failure
        clean, reason = patch
        print(f"  patch {table}.{column} -> {json.dumps(clean)}  ({reason.splitlines()[0]})")
    if clean is ops.DROPPED:
        return None
    return clean


def derive(table: str) -> tuple[dict, dict]:
    qualified = f"public.{table}"
    raw_value = json.loads((RAW / f"public.{table}-value.json").read_text())
    raw_key = json.loads((RAW / f"public.{table}-key.json").read_text())

    clean_value = copy.deepcopy(raw_value)
    record = value_record(clean_value)
    fields = []
    for field in record["fields"]:
        clean = derive_field(qualified, field["name"], field["type"])
        if clean is None:
            continue  # dropped by policy: no field, therefore no sink column
        fields.append({"name": field["name"], "type": clean})
    record["fields"] = fields

    # The key carries the primary key, and it is de-identified with the same ops
    # as the value -- if the two disagreed, upserts would write duplicate rows.
    clean_key = copy.deepcopy(raw_key)
    clean_key["fields"] = [
        {"name": f["name"], "type": derive_field(qualified, f["name"], f["type"])}
        for f in clean_key["fields"]
    ]

    return rename_namespaces(clean_key), rename_namespaces(clean_value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh-raw",
        metavar="REGISTRY",
        nargs="?",
        const="http://localhost:8081",
        help="re-fetch the raw fixtures from a live registry first (needs `make forward`)",
    )
    args = parser.parse_args()

    if args.refresh_raw:
        print(f"==> refreshing raw fixtures from {args.refresh_raw}")
        fetch_raw(args.refresh_raw)

    print("==> deriving clean schemas from (raw schema, policy) via deid.ops")
    for table in TABLES:
        clean_key, clean_value = derive(table)
        write(CLEAN / f"public.{table}-key.json", clean_key)
        write(CLEAN / f"public.{table}-value.json", clean_value)
        columns = len(value_record(clean_value)["fields"])
        print(f"  clean clean.public.{table}  ({columns} columns)")
    print(f"==> wrote {2 * len(TABLES)} files to {CLEAN.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
