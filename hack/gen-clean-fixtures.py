#!/usr/bin/env python3
"""Derive the clean Avro schema fixtures that M5's tests are written against.

M5 reads `clean.public.*` schemas from the registry. Nothing registers them yet
-- DATA-712's runner is what will -- so the fixtures stand in for that, and they
are *derived* rather than hand-authored so they stand in faithfully.

The value schema comes from ``deid.schema.derive_clean_schema``: the same
function M4 will call, over the same policy, against the raw schemas Debezium
actually registered (in pit/tests/fixtures/raw/, fetched from the live registry).
So these are not a transcription of what we think M4 will emit -- they are what it
emits.

The **key** schema is derived here rather than by ``deid``, because nothing in M4
derives one yet. A clean key is the raw key with each field run through the same
op as its counterpart in the value, which is what makes the key and the value
agree -- if they disagreed, the same logical row would upsert under two different
surrogates. When DATA-712 lands a key derivation, this should defer to it the way
the value already does.

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

from deid import ops, policy, schema  # noqa: E402  (after sys.path)

TABLES = ("patients", "providers", "appointments", "claims", "notes")
RAW = REPO / "pit" / "tests" / "fixtures" / "raw"
CLEAN = REPO / "pit" / "tests" / "fixtures" / "clean"
POLICY = REPO / "deid" / "policy" / "clinic.yml"

# The prefix M4's cleaned topics carry.
CLEAN_PREFIX = "clean."

# Fixed inputs. The salt and reference date only affect *values*, never the
# derived types, so any constant does -- and a constant keeps the output
# byte-identical between runs, which is what makes `make fixtures` a diffable
# no-op when nothing has changed.
KEYS = ops.Keys(salt=b"fixture-generation-not-a-real-salt", reference_date=date(2026, 8, 19))


def fetch_raw(registry: str) -> None:
    """Re-fetch the raw fixtures from a live registry."""
    RAW.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        for kind in ("key", "value"):
            subject = f"raw.public.{table}-{kind}"
            url = f"{registry}/subjects/{subject}/versions/latest"
            with urllib.request.urlopen(url) as response:
                fetched = json.loads(json.load(response)["schema"])
            write(RAW / f"public.{table}-{kind}.json", fetched)
            print(f"  raw   {subject}")


def write(path: Path, document: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def read(directory: Path, table: str, kind: str) -> dict:
    return json.loads((directory / f"public.{table}-{kind}.json").read_text())


def derive_key(
    raw_key: dict, table_policy: policy.TablePolicy, table: str, topic: str
) -> dict:
    """The clean key schema: every field through the same op as in the value.

    The message key carries the primary key, and it is what an upsert conflicts on
    and a delete matches. It has to be de-identified with the same op as its
    counterpart in the value or the two disagree and the sink grows duplicates
    that no join can reconcile.
    """
    clean = copy.deepcopy(raw_key)
    fields = []
    for field in clean["fields"]:
        rule = table_policy.rule_for(field["name"])
        if rule is None:
            raise SystemExit(
                f"public.{table}: the key names {field['name']}, which the policy has no "
                f"rule for. A primary key column with no rule cannot be de-identified "
                f"consistently with the value, so this has to be fixed in the policy."
            )
        clean_type = ops.build(rule, field["type"], keys=KEYS).derive_type(field["type"])
        if clean_type is ops.DROPPED:
            raise SystemExit(
                f"public.{table}: the policy drops {field['name']}, which is part of the "
                f"primary key. There would be nothing left to identify the row by."
            )
        fields.append({"name": field["name"], "type": clean_type})
    clean["fields"] = fields
    clean["namespace"] = topic
    if isinstance(clean.get("connect.name"), str):
        clean["connect.name"] = f"{topic}.{clean['connect.name'].rsplit('.', 1)[-1]}"
    return clean


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

    parsed = policy.load_policy(POLICY)
    print("==> deriving clean schemas with deid.schema.derive_clean_schema")

    for table in TABLES:
        qualified = f"public.{table}"
        topic = f"{CLEAN_PREFIX}{qualified}"
        table_policy = parsed.table(qualified)
        if table_policy is None:
            raise SystemExit(f"{qualified} has no policy: M4 would halt this topic.")

        # No try/except: a derivation that fails is a topic M4 would halt, and
        # there would be no clean schema for the sink to be built from. Guessing
        # one here would make every M5 test that depends on it meaningless.
        clean_value = schema.derive_clean_schema(
            read(RAW, table, "value"),
            table_policy,
            keys=KEYS,
            on_uncovered=parsed.on_uncovered_column,
            namespace=topic,
            source=parsed.source,
        )
        clean_key = derive_key(read(RAW, table, "key"), table_policy, table, topic)

        write(CLEAN / f"public.{table}-value.json", clean_value)
        write(CLEAN / f"public.{table}-key.json", clean_key)

        columns = len(schema.definitions(clean_value)) and _row_image(clean_value)
        print(f"  clean {topic}  ({len(columns['fields'])} columns)")

    print(f"==> wrote {2 * len(TABLES)} files to {CLEAN.relative_to(REPO)}")
    return 0


def _row_image(clean_value: dict) -> dict:
    """The row-image record, wherever the derived schema defined it."""
    for field in clean_value["fields"]:
        if field["name"] not in schema.ROW_IMAGE_FIELDS:
            continue
        branches = field["type"] if isinstance(field["type"], list) else [field["type"]]
        for branch in branches:
            if isinstance(branch, dict) and branch.get("type") == "record":
                return branch
    raise SystemExit("the derived schema defines no row image")


if __name__ == "__main__":
    raise SystemExit(main())
