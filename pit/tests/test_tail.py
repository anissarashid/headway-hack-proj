"""The tail loop: reconcile, apply, wait, stop.

The registry is stubbed at the HTTP boundary rather than mocked out, so
:meth:`pit.registry.Registry.subjects`, ``latest``, ``clean_topics`` and
``schemas_for`` all run their real parsing over the real fixture schemas. What is
faked is one method that would otherwise open a socket.

That also keeps these tests off the shared dev registry. Registering real
``clean.*`` subjects to prove the loop reads them would leave version history in a
registry other people are using, and DATA-712 is what owns those subjects.
"""

from __future__ import annotations

import json

import pytest

from conftest import reset_offsets
from pit import applier, ddl, registry, tail

psycopg = pytest.importorskip("psycopg")

TABLES = ("patients", "providers", "appointments", "claims", "notes")


class StubRegistry(registry.Registry):
    """A registry serving the clean fixtures, without a socket.

    Only ``_get`` is replaced. Everything above it -- subject filtering, the
    double JSON unwrap, the key/value pairing -- is the code that runs in
    production.
    """

    def __init__(self, clean_dir, tables=TABLES):
        super().__init__(base_url="http://stub")
        object.__setattr__(self, "_dir", clean_dir)
        object.__setattr__(self, "_tables", tuple(tables))

    def _get(self, path: str) -> object:
        if path == "/subjects":
            return [
                f"clean.public.{table}-{kind}"
                for table in self._tables
                for kind in ("key", "value")
            ]
        # /subjects/clean.public.patients-value/versions/latest
        subject = path.split("/")[2]
        name = subject[len("clean.") :]
        # The registry returns the schema as a JSON string inside a JSON object.
        return {"schema": (self._dir / f"{name}.json").read_text()}


class EmptyRegistry(registry.Registry):
    def __init__(self):
        super().__init__(base_url="http://stub")

    def _get(self, path: str) -> object:
        if path == "/subjects":
            return []
        raise registry.RegistryError("nothing registered")


# ---------------------------------------------------------------------------
# the registry client, over the fixtures
# ---------------------------------------------------------------------------


def test_clean_topics_lists_only_complete_pairs(clean_dir):
    """A topic with only one half registered is skipped.

    That is M4 mid-startup, or M4 having halted that topic. Either way there is
    not enough to build a table from, and inventing the missing key schema is how
    a sink ends up with the wrong primary key.
    """
    client = StubRegistry(clean_dir)
    assert client.clean_topics() == [f"clean.public.{t}" for t in sorted(TABLES)]

    class HalfRegistered(StubRegistry):
        def _get(self, path: str) -> object:
            if path == "/subjects":
                return ["clean.public.patients-value"]  # no -key
            return super()._get(path)

    assert HalfRegistered(clean_dir).clean_topics() == []


def test_latest_unwraps_the_schema_twice(clean_dir):
    schema = StubRegistry(clean_dir).latest("clean.public.patients-value")
    assert schema["namespace"] == "clean.public.patients"


def test_latest_rejects_an_unparseable_schema(clean_dir, tmp_path):
    class Broken(StubRegistry):
        def _get(self, path: str) -> object:
            if path == "/subjects":
                return super()._get(path)
            return {"schema": "{not json"}

    with pytest.raises(registry.RegistryError, match="unparseable"):
        Broken(clean_dir).latest("clean.public.patients-value")


def test_registry_error_names_the_forward(clean_dir):
    """The message has to say what to do, because this is the common local failure."""
    client = registry.Registry(base_url="http://127.0.0.1:1")
    with pytest.raises(registry.RegistryError, match="make forward"):
        client.subjects()


# ---------------------------------------------------------------------------
# one pass, against a live sink
# ---------------------------------------------------------------------------


@pytest.fixture
def scratch(sink_dsn):
    """A connection whose `public` schema is empty, so a pass has work to do."""
    with psycopg.connect(sink_dsn) as conn:
        yield conn


@pytest.fixture
def rebased(clean_dir, tmp_path):
    """The clean fixtures, moved into a `pit_tail_test` schema.

    So a pass builds tables in a schema of its own and a live `pit_base` is
    untouched.
    """
    for table in TABLES:
        for kind in ("key", "value"):
            document = json.loads((clean_dir / f"public.{table}-{kind}.json").read_text())
            moved = json.loads(
                json.dumps(document).replace("clean.public.", "clean.pit_tail_test.")
            )
            (tmp_path / f"pit_tail_test.{table}-{kind}.json").write_text(json.dumps(moved))
    return tmp_path


class RebasedRegistry(StubRegistry):
    def _get(self, path: str) -> object:
        if path == "/subjects":
            return [
                f"clean.pit_tail_test.{table}-{kind}"
                for table in TABLES
                for kind in ("key", "value")
            ]
        subject = path.split("/")[2]
        name = subject[len("clean.") :]
        return {"schema": (self._dir / f"{name}.json").read_text()}


@pytest.fixture
def clean_registry(rebased, scratch):
    with scratch.cursor() as cursor:
        cursor.execute("drop schema if exists pit_tail_test cascade")
    scratch.commit()
    reset_offsets(scratch)
    try:
        yield RebasedRegistry(rebased)
    finally:
        reset_offsets(scratch)
        with scratch.cursor() as cursor:
            cursor.execute("drop schema if exists pit_tail_test cascade")
        scratch.commit()


def test_a_pass_builds_the_tables_the_registry_describes(scratch, clean_registry):
    """The registry path, end to end: subjects in, tables out.

    This is the path the Deployment uses. `initdb --schema-dir` covers the same
    ground from disk, but only this proves the HTTP-shaped client feeds the DDL.
    """
    result = tail.one_pass(scratch, clean_registry)

    assert len(result.topics) == len(TABLES)
    assert any(s.startswith("create table") for s in result.statements)
    live = ddl.live_columns(
        scratch,
        ddl.Table(schema="pit_tail_test", name="claims", columns=(), primary_key=()),
    )
    assert live["billed_amount"] == "numeric"
    assert live["diagnosis_codes"] == "text[]"


def test_a_second_pass_makes_no_changes(scratch, clean_registry):
    """The loop runs every interval, so a steady state has to be quiet."""
    tail.one_pass(scratch, clean_registry)
    result = tail.one_pass(scratch, clean_registry)

    assert ddl.changes(result.statements) == []
    assert "schema changes" not in tail.describe(result)


def test_a_pass_reports_offsets_and_row_counts(scratch, clean_registry):
    tail.one_pass(scratch, clean_registry)
    applier.apply(scratch, [], offsets=[applier.Offset("clean.pit_tail_test.claims", 0, 7)])

    result = tail.one_pass(scratch, clean_registry)
    assert result.offsets[("clean.pit_tail_test.claims", 0)] == 7
    assert result.rows["pit_tail_test.claims"] == 0


def test_a_new_column_in_the_registry_reaches_the_sink(scratch, clean_registry, rebased):
    """The reconcile step earning its place while the consumer is still absent.

    M4 registers a new schema version when someone covers a new source column, and
    the sink has to grow the column before a record carrying it can be applied.
    """
    tail.one_pass(scratch, clean_registry)

    path = rebased / "pit_tail_test.providers-value.json"
    document = json.loads(path.read_text())
    row_image = next(
        branch
        for field in document["fields"]
        if field["name"] == "before"
        for branch in field["type"]
        if isinstance(branch, dict)
    )
    row_image["fields"].append({"name": "newly_covered", "type": ["null", "string"]})
    path.write_text(json.dumps(document))

    result = tail.one_pass(scratch, clean_registry)
    assert any("newly_covered" in s for s in result.statements)

    live = ddl.live_columns(
        scratch,
        ddl.Table(schema="pit_tail_test", name="providers", columns=(), primary_key=()),
    )
    assert "newly_covered" in live


# ---------------------------------------------------------------------------
# the loop
# ---------------------------------------------------------------------------


def test_no_clean_subjects_is_a_wait_not_a_failure():
    """A Deployment that exits here would CrashLoopBackOff before M4 ever runs."""
    result = tail.Pass()
    assert result.waiting
    assert "waiting" in tail.describe(result)


def test_run_exits_zero_on_a_stop_request(monkeypatch):
    """Scaling to zero is a clean shutdown, not a failed pod.

    M7's snapshot CronJob scales this Deployment down to take its clone, so this
    happens on every snapshot and must not look like an error.
    """
    stopping = tail.Stopping()
    stopping.requested = True
    monkeypatch.setattr(tail.psycopg, "connect", _unreachable)
    assert tail.run("pit_base", EmptyRegistry(), once=False, stopping=stopping) == 0


def test_run_survives_an_unreachable_sink(monkeypatch):
    """A rolling restart of the sink is a blip to retry, not a reason to exit."""
    monkeypatch.setattr(tail.psycopg, "connect", _unreachable)
    assert tail.run("pit_base", EmptyRegistry(), once=True) == 0


def _unreachable(*_args, **_kwargs):
    raise psycopg.OperationalError("connection refused (test)")
