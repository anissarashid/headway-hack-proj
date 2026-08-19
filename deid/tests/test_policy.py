"""What the policy parser promises, asserted.

Two kinds of test here. The first kind checks that a bad policy fails at parse
time with a message naming the table, the column and the problem -- the whole
value of parsing at the edge is that the failure is legible to whoever wrote the
YAML, so the messages are part of the contract and are asserted as such.

The second kind checks the shipped ``policy/clinic.yml`` against the source DDL,
because a policy is only auditable if it is about the columns that exist. A rule
for a column the source does not have protects nothing while looking like it
does, and a column the policy has missed halts its topic on the day someone
deploys it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from deid.policy import (
    DateShift,
    Drop,
    DuplicateKeyError,
    Fake,
    Generalize,
    Hmac,
    InvalidArgumentError,
    MalformedPolicyError,
    MissingArgumentError,
    Null,
    NumericJitter,
    Passthrough,
    Policy,
    Redact,
    PolicyError,
    ReservedFieldError,
    UncoveredColumn,
    UnknownArgumentError,
    UnknownOpError,
    load_policy,
    parse_policy,
)

DEID_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = DEID_ROOT.parent
CLINIC_POLICY = DEID_ROOT / "policy" / "clinic.yml"
CLINIC_DDL = (
    REPO_ROOT
    / "charts/pit/charts/source-pg/files/initdb/20-clinic-schema.sql"
)


def policy_doc(**tables) -> dict:
    """A minimal valid policy around the tables under test."""
    return {"on_uncovered_column": "halt_topic", "tables": tables}


# ---------------------------------------------------------------------------
# a valid policy
# ---------------------------------------------------------------------------


def test_valid_policy_parses_to_typed_rules():
    policy = parse_policy(
        policy_doc(
            **{
                "public.patients": {
                    "patient_id": {"op": "hmac", "domain": "patient"},
                    "first_name": {"op": "fake", "kind": "first_name"},
                    "date_of_birth": {"op": "generalize", "to": "birth_year", "cap_age": 89},
                    "ssn": {"op": "drop"},
                    "postal_code": {"op": "generalize", "to": "zip3"},
                    "created_at": {"op": "passthrough"},
                },
                "public.appointments": {
                    "patient_id": {"op": "hmac", "domain": "patient"},
                    "scheduled_at": {"op": "date_shift", "anchor": "patient_id"},
                },
            }
        ),
        source="test.yml",
    )

    assert policy.on_uncovered_column is UncoveredColumn.HALT_TOPIC
    assert set(policy.tables) == {"public.patients", "public.appointments"}

    assert policy.rule_for("public.patients", "patient_id").op == Hmac(domain="patient")
    assert policy.rule_for("public.patients", "first_name").op == Fake(kind="first_name")
    assert policy.rule_for("public.patients", "date_of_birth").op == Generalize(
        to="birth_year", cap_age=89
    )
    assert policy.rule_for("public.patients", "ssn").op == Drop()
    assert policy.rule_for("public.patients", "postal_code").op == Generalize(to="zip3")
    assert policy.rule_for("public.patients", "created_at").op == Passthrough()
    assert policy.rule_for("public.appointments", "scheduled_at").op == DateShift(
        anchor="patient_id"
    )


def test_the_ops_that_replace_a_value_rather_than_remove_it():
    policy = parse_policy(
        policy_doc(
            **{
                "public.appointments": {
                    "appointment_id": {"op": "hmac", "domain": "appointment"},
                    "location": {"op": "null"},
                    "intake_answers": {"op": "redact"},
                    "duration_minutes": {
                        "op": "numeric_jitter",
                        "pct": 5,
                        "anchor": "appointment_id",
                    },
                }
            }
        )
    )
    assert policy.rule_for("public.appointments", "location").op == Null()
    assert policy.rule_for("public.appointments", "intake_answers").op == Redact()
    assert policy.rule_for("public.appointments", "duration_minutes").op == NumericJitter(
        anchor="appointment_id", pct=5
    )


def test_an_unquoted_null_op_says_what_yaml_did_to_it(tmp_path):
    """`op: null` is YAML's null, and reads as a rule with no op at all."""
    path = tmp_path / "unquoted.yml"
    path.write_text(
        "tables:\n  public.appointments:\n    location: { op: null }\n", encoding="utf-8"
    )
    with pytest.raises(MissingArgumentError) as exc:
        load_policy(path)
    assert 'quote it: op: "null"' in str(exc.value)


def test_an_absurd_jitter_is_not_a_jitter():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.claims": {
                        "claim_id": {"op": "hmac", "domain": "claim"},
                        "billed_amount": {
                            "op": "numeric_jitter",
                            "pct": 90,
                            "anchor": "claim_id",
                        },
                    }
                }
            )
        )
    assert "between 1 and 25" in str(exc.value)


def test_a_jitter_cannot_be_anchored_on_a_moving_value():
    """Same rule as date_shift, and the same reason: an anchor is an identity."""
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.claims": {
                        "claim_id": {"op": "hmac", "domain": "claim"},
                        "billed_amount": {
                            "op": "numeric_jitter",
                            "pct": 5,
                            "anchor": "claim_id",
                        },
                        "paid_amount": {
                            "op": "numeric_jitter",
                            "pct": 5,
                            "anchor": "billed_amount",
                        },
                    }
                }
            )
        )
    assert "is itself numeric_jitter'd" in str(exc.value)


def test_a_date_shift_cannot_be_anchored_on_a_jittered_column():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.claims": {
                        "claim_id": {"op": "hmac", "domain": "claim"},
                        "billed_amount": {
                            "op": "numeric_jitter",
                            "pct": 5,
                            "anchor": "claim_id",
                        },
                        "submitted_at": {"op": "date_shift", "anchor": "billed_amount"},
                    }
                }
            )
        )
    assert "is itself numeric_jitter'd" in str(exc.value)


def test_uncovered_column_defaults_to_halting_the_topic():
    """Omitting the key must not be a quieter policy than setting it."""
    policy = parse_policy({"tables": {"public.patients": {"ssn": {"op": "drop"}}}})
    assert policy.on_uncovered_column is UncoveredColumn.HALT_TOPIC


def test_lookups_of_uncovered_things_are_none_not_errors():
    policy = parse_policy(policy_doc(**{"public.patients": {"ssn": {"op": "drop"}}}))
    assert policy.covers("public.patients", "ssn")
    assert not policy.covers("public.patients", "email")
    assert policy.rule_for("public.patients", "email") is None
    assert policy.rule_for("public.widgets", "ssn") is None
    assert policy.table("public.widgets") is None


def test_nothing_downstream_can_mutate_a_parsed_policy():
    """No raw dicts past this module -- and no writable ones either."""
    policy = parse_policy(policy_doc(**{"public.patients": {"ssn": {"op": "drop"}}}))
    with pytest.raises(TypeError):
        policy.tables["public.patients"] = None
    with pytest.raises(TypeError):
        policy.tables["public.patients"].rules["ssn"] = None
    with pytest.raises(Exception):
        policy.tables["public.patients"].rules["ssn"].op.name = "passthrough"


def test_hmac_domains_are_reported_for_key_setup():
    policy = parse_policy(
        policy_doc(
            **{
                "public.patients": {
                    "patient_id": {"op": "hmac", "domain": "patient"},
                    "mrn": {"op": "hmac", "domain": "mrn"},
                },
                "public.notes": {"patient_id": {"op": "hmac", "domain": "patient"}},
            }
        )
    )
    assert policy.hmac_domains == frozenset({"patient", "mrn"})


# ---------------------------------------------------------------------------
# unknown ops
# ---------------------------------------------------------------------------


def test_unknown_op_is_rejected_by_name():
    with pytest.raises(UnknownOpError) as exc:
        parse_policy(
            policy_doc(**{"public.patients": {"ssn": {"op": "scrub"}}}), source="test.yml"
        )
    assert exc.value.table == "public.patients"
    assert exc.value.column == "ssn"
    assert "unknown op 'scrub'" in str(exc.value)
    # The message has to say what the alternatives were.
    assert "drop" in str(exc.value)
    assert str(exc.value).startswith("test.yml: public.patients.ssn: ")


def test_op_that_only_differs_by_case_is_still_unknown():
    with pytest.raises(UnknownOpError):
        parse_policy(policy_doc(**{"public.patients": {"ssn": {"op": "DROP"}}}))


def test_rule_without_an_op_names_the_column():
    with pytest.raises(MissingArgumentError) as exc:
        parse_policy(policy_doc(**{"public.patients": {"ssn": {"domain": "patient"}}}))
    assert exc.value.column == "ssn"
    assert "no 'op'" in str(exc.value)


def test_rule_that_is_not_a_mapping_is_rejected():
    with pytest.raises(MalformedPolicyError) as exc:
        parse_policy(policy_doc(**{"public.patients": {"ssn": "drop"}}))
    assert exc.value.column == "ssn"
    assert "must be a mapping" in str(exc.value)


# ---------------------------------------------------------------------------
# arguments
# ---------------------------------------------------------------------------


def test_hmac_without_a_domain_is_rejected():
    """There is no default domain: guessing one is a broken join or a leak."""
    with pytest.raises(MissingArgumentError) as exc:
        parse_policy(
            policy_doc(**{"public.patients": {"patient_id": {"op": "hmac"}}}),
            source="clinic.yml",
        )
    assert exc.value.table == "public.patients"
    assert exc.value.column == "patient_id"
    assert "requires argument 'domain'" in str(exc.value)
    assert str(exc.value) == (
        "clinic.yml: public.patients.patient_id: op 'hmac' requires argument 'domain'"
    )


def test_hmac_with_an_empty_domain_is_rejected():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(**{"public.patients": {"patient_id": {"op": "hmac", "domain": "  "}}})
        )
    assert "'domain' must not be empty" in str(exc.value)


def test_date_shift_without_an_anchor_is_rejected():
    with pytest.raises(MissingArgumentError) as exc:
        parse_policy(
            policy_doc(**{"public.appointments": {"scheduled_at": {"op": "date_shift"}}})
        )
    assert exc.value.column == "scheduled_at"
    assert "requires argument 'anchor'" in str(exc.value)


def test_arguments_are_type_checked():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(**{"public.patients": {"patient_id": {"op": "hmac", "domain": 7}}})
        )
    assert "'domain' must be a string, got int" in str(exc.value)


def test_a_boolean_is_not_an_integer():
    """`cap_age: true` is a YAML footgun, not an age."""
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.patients": {
                        "date_of_birth": {"op": "generalize", "to": "birth_year", "cap_age": True}
                    }
                }
            )
        )
    assert "'cap_age' must be an integer or null" in str(exc.value)


def test_unknown_argument_is_rejected():
    with pytest.raises(UnknownArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{"public.patients": {"patient_id": {"op": "hmac", "domian": "patient"}}}
            )
        )
    assert "does not take argument 'domian'" in str(exc.value)


def test_argument_on_an_op_that_takes_none():
    with pytest.raises(UnknownArgumentError) as exc:
        parse_policy(policy_doc(**{"public.patients": {"ssn": {"op": "drop", "kind": "x"}}}))
    assert "no arguments" in str(exc.value)


def test_unknown_fake_kind_is_rejected():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(**{"public.patients": {"first_name": {"op": "fake", "kind": "ssn"}}})
        )
    assert "unknown fake kind 'ssn'" in str(exc.value)


def test_unknown_generalize_target_is_rejected():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{"public.patients": {"postal_code": {"op": "generalize", "to": "zip5"}}}
            )
        )
    assert "unknown generalize target 'zip5'" in str(exc.value)


def test_cap_age_on_a_target_that_cannot_apply_it():
    """A cap the op would ignore is a claim the policy does not honour."""
    with pytest.raises(UnknownArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.patients": {
                        "postal_code": {"op": "generalize", "to": "zip3", "cap_age": 89}
                    }
                }
            )
        )
    assert "does not take 'cap_age'" in str(exc.value)


def test_impossible_cap_age_is_rejected():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.patients": {
                        "date_of_birth": {"op": "generalize", "to": "birth_year", "cap_age": 0}
                    }
                }
            )
        )
    assert "between 1 and 120" in str(exc.value)


# ---------------------------------------------------------------------------
# the source block
# ---------------------------------------------------------------------------


def test_rule_against_the_source_block_is_rejected():
    """Point-in-time replay resolves T against source.ts_ms; nothing may touch it."""
    with pytest.raises(ReservedFieldError) as exc:
        parse_policy(
            policy_doc(**{"public.patients": {"source": {"op": "drop"}}}), source="clinic.yml"
        )
    assert exc.value.table == "public.patients"
    assert exc.value.column == "source"
    assert "source.ts_ms" in str(exc.value)


def test_rule_against_a_field_inside_source_is_rejected():
    with pytest.raises(ReservedFieldError) as exc:
        parse_policy(
            policy_doc(
                **{"public.patients": {"source.ts_ms": {"op": "date_shift", "anchor": "patient_id"}}}
            )
        )
    assert exc.value.column == "source.ts_ms"


@pytest.mark.parametrize("field", ["op", "ts_ms", "transaction"])
def test_the_rest_of_the_envelope_is_reserved_too(field):
    with pytest.raises(ReservedFieldError):
        parse_policy(policy_doc(**{"public.patients": {field: {"op": "passthrough"}}}))


def test_top_level_rule_against_source_is_rejected():
    with pytest.raises(ReservedFieldError) as exc:
        parse_policy(
            {
                "on_uncovered_column": "halt_topic",
                "source": {"ts_ms": {"op": "drop"}},
                "tables": {"public.patients": {"ssn": {"op": "drop"}}},
            }
        )
    assert "envelope" in str(exc.value)


# ---------------------------------------------------------------------------
# cross-column checks
# ---------------------------------------------------------------------------


def test_date_shift_anchor_must_be_a_covered_column():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.appointments": {
                        "scheduled_at": {"op": "date_shift", "anchor": "patient_id"}
                    }
                }
            )
        )
    assert exc.value.column == "scheduled_at"
    assert "anchor 'patient_id' has no rule" in str(exc.value)


def test_date_shift_cannot_anchor_on_itself():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.appointments": {
                        "scheduled_at": {"op": "date_shift", "anchor": "scheduled_at"}
                    }
                }
            )
        )
    assert "cannot be anchored on itself" in str(exc.value)


def test_date_shift_cannot_anchor_on_another_shifted_column():
    """Anchoring on a moving value gives a per-record offset, killing intervals."""
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            policy_doc(
                **{
                    "public.appointments": {
                        "patient_id": {"op": "hmac", "domain": "patient"},
                        "scheduled_at": {"op": "date_shift", "anchor": "patient_id"},
                        "completed_at": {"op": "date_shift", "anchor": "scheduled_at"},
                    }
                }
            )
        )
    assert exc.value.column == "completed_at"
    assert "is itself date_shift'd" in str(exc.value)


# ---------------------------------------------------------------------------
# file shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "table",
    ["patients", "public.patients.extra", "public patients", ""],
)
def test_table_names_must_be_schema_qualified(table):
    with pytest.raises(MalformedPolicyError) as exc:
        parse_policy(policy_doc(**{table: {"ssn": {"op": "drop"}}}))
    assert "schema-qualified" in str(exc.value)


def test_unknown_top_level_key_is_rejected():
    with pytest.raises(MalformedPolicyError) as exc:
        parse_policy(
            {"tables": {"public.patients": {"ssn": {"op": "drop"}}}, "on_uncoverd_column": "x"}
        )
    assert "unknown top-level key" in str(exc.value)


def test_uncovered_column_has_no_passthrough_setting():
    with pytest.raises(InvalidArgumentError) as exc:
        parse_policy(
            {
                "on_uncovered_column": "passthrough",
                "tables": {"public.patients": {"ssn": {"op": "drop"}}},
            }
        )
    assert "halt_topic" in str(exc.value)


def test_policy_with_no_tables_is_rejected():
    with pytest.raises(MalformedPolicyError):
        parse_policy({"on_uncovered_column": "halt_topic", "tables": {}})
    with pytest.raises(MalformedPolicyError):
        parse_policy({"on_uncovered_column": "halt_topic"})


def test_table_with_no_rules_is_rejected():
    with pytest.raises(MalformedPolicyError) as exc:
        parse_policy(policy_doc(**{"public.patients": {}}))
    assert "no rules" in str(exc.value)


def test_every_error_is_a_policy_error():
    """One except clause at the startup boundary has to be enough."""
    for doc in (
        policy_doc(**{"public.patients": {"ssn": {"op": "nope"}}}),
        policy_doc(**{"public.patients": {"ssn": {"op": "hmac"}}}),
        policy_doc(**{"public.patients": {"source": {"op": "drop"}}}),
        ["not", "a", "mapping"],
    ):
        with pytest.raises(PolicyError):
            parse_policy(doc)


# ---------------------------------------------------------------------------
# loading from disk
# ---------------------------------------------------------------------------


def test_duplicate_column_key_is_rejected(tmp_path):
    """YAML would let the second rule win silently. That is the audit failure."""
    path = tmp_path / "dupe.yml"
    path.write_text(
        "on_uncovered_column: halt_topic\n"
        "tables:\n"
        "  public.patients:\n"
        "    ssn: { op: drop }\n"
        "    ssn: { op: passthrough }\n",
        encoding="utf-8",
    )
    with pytest.raises(DuplicateKeyError) as exc:
        load_policy(path)
    assert "duplicate key 'ssn'" in str(exc.value)
    assert "line 5" in str(exc.value)


def test_invalid_yaml_is_a_policy_error(tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text("tables: [unclosed\n", encoding="utf-8")
    with pytest.raises(MalformedPolicyError) as exc:
        load_policy(path)
    assert "invalid YAML" in str(exc.value)


def test_empty_file_is_a_policy_error(tmp_path):
    path = tmp_path / "empty.yml"
    path.write_text("", encoding="utf-8")
    with pytest.raises(MalformedPolicyError):
        load_policy(path)


def test_missing_file_is_a_policy_error(tmp_path):
    with pytest.raises(MalformedPolicyError) as exc:
        load_policy(tmp_path / "nope.yml")
    assert "cannot read policy file" in str(exc.value)


# ---------------------------------------------------------------------------
# the shipped policy, against the schema it claims to cover
# ---------------------------------------------------------------------------


def _ddl_columns(sql: str) -> dict[str, tuple[str, ...]]:
    """Columns per table, straight out of the CREATE TABLE statements.

    Deliberately parsed from the DDL rather than listed here: a list would have
    to be kept in step by hand, which is the failure this test exists to catch.
    """
    sql = re.sub(r"--[^\n]*", "", sql)
    not_a_column = {"constraint", "primary", "foreign", "unique", "check", "exclude", "like"}
    tables: dict[str, tuple[str, ...]] = {}

    for match in re.finditer(r"\bCREATE\s+TABLE\s+(\w+)\s*\(", sql, re.IGNORECASE):
        depth, index = 1, match.end()
        while depth:
            char = sql[index]
            depth += (char == "(") - (char == ")")
            index += 1
        body = sql[match.end() : index - 1]

        items, depth, start = [], 0, 0
        for position, char in enumerate(body):
            depth += (char == "(") - (char == ")")
            if char == "," and depth == 0:
                items.append(body[start:position])
                start = position + 1
        items.append(body[start:])

        columns = []
        for item in items:
            words = item.split()
            if words and words[0].lower() not in not_a_column:
                columns.append(words[0])
        tables[match.group(1)] = tuple(columns)

    return tables


@pytest.fixture(scope="module")
def clinic() -> Policy:
    return load_policy(CLINIC_POLICY)


def test_the_shipped_policy_is_valid(clinic):
    assert clinic.on_uncovered_column is UncoveredColumn.HALT_TOPIC
    assert set(clinic.tables) == {
        "public.patients",
        "public.providers",
        "public.appointments",
        "public.claims",
        "public.notes",
    }


def test_the_shipped_policy_covers_exactly_the_source_columns(clinic):
    """Every column, no invented ones. Both directions are failures.

    A missing rule halts that topic on the day it deploys. A rule for a column
    that does not exist is worse: it reads like protection in review and does
    nothing at all.
    """
    ddl = _ddl_columns(CLINIC_DDL.read_text(encoding="utf-8"))
    assert ddl, "could not parse the clinic DDL"

    for table_name, columns in ddl.items():
        qualified = f"public.{table_name}"
        table_policy = clinic.table(qualified)
        assert table_policy is not None, f"{qualified} has no rules"
        assert set(table_policy.rules) == set(columns), (
            f"{qualified}: uncovered={sorted(set(columns) - set(table_policy.rules))} "
            f"unknown={sorted(set(table_policy.rules) - set(columns))}"
        )


def test_the_shipped_policy_drops_ssn_and_free_text(clinic):
    """The three columns whose handling the whole design is judged on."""
    assert clinic.rule_for("public.patients", "ssn").op == Drop()
    assert clinic.rule_for("public.notes", "body").op == Drop()
    assert clinic.rule_for("public.appointments", "intake_answers").op == Drop()


def test_the_shipped_policy_keeps_joins_and_age_cohorts(clinic):
    """De-identification that destroys the join graph is not a replica."""
    for table in ("public.patients", "public.appointments", "public.claims", "public.notes"):
        assert clinic.rule_for(table, "patient_id").op == Hmac(domain="patient")

    assert clinic.rule_for("public.patients", "date_of_birth").op == Generalize(
        to="birth_year", cap_age=89
    )


def test_the_shipped_policy_anchors_every_shift_on_one_entity_per_table(clinic):
    """Two anchors in one table means two offsets, and intervals across them lie."""
    for table_policy in clinic.tables.values():
        anchors = {
            rule.op.anchor
            for rule in table_policy.rules.values()
            if isinstance(rule.op, DateShift)
        }
        assert len(anchors) <= 1, f"{table_policy.name} shifts against {sorted(anchors)}"
