# loadgen

Synthetic load for the clinic schema. Everything it produces is fake: the
generator takes an integer and returns rows, with no network, no filesystem and
no clock involved, so there is no path by which real patient data could reach
it.

```
src/loadgen/config.py       the seed constant, row counts, distributions, edge cases
src/loadgen/vocab.py        clinical vocabulary and note templates
src/loadgen/seed.py         the generator, the loader, and the CLI
src/loadgen/__main__.py     the churn loop: ongoing inserts, updates and deletes
src/loadgen/fingerprint.py  reduce a population to a comparable digest
tests/test_seed.py          determinism, counts, shape, and the awkward cases
tests/test_churn.py         bounds, the population band, and the ledger as an oracle
```

There are two generators, and the split is deliberate. **The seed** builds one
state, in one transaction, deterministically. **The churn loop** turns that state
into a timeline. Point-in-time replay needs history to replay, and a static seed
has exactly one point in it.

## Running it

```
make loadgen-deps    # uv sync
make forward         # backgrounded; make forward-stop tears it down
make seed            # wipe and repopulate
make seed-verify     # the seed acceptance check
make churn           # five minutes of inserts, updates and deletes
make churn-verify    # check the ledger without changing anything
make churn-check     # the churn acceptance check
```

Or directly, from `loadgen/`:

```
uv run python -m loadgen.seed --reset        # wipe and repopulate
uv run python -m loadgen.seed --dry-run      # generate and digest, no database
uv run python -m loadgen.seed --fingerprint  # digest what is in the database now
uv run python -m loadgen.seed --seed 12345   # a different population

uv run python -m loadgen                     # 5 minutes at 2 transactions/second
uv run python -m loadgen --duration 30s      # short, and still hits every shape
uv run python -m loadgen --rate 8 --max-txns 500
uv run python -m loadgen --verify            # check the ledger, change nothing
```

Connection comes from `PIT_DSN`, or from the usual `PG*` variables, defaulting
to the dev release on `localhost:5432`. Seeding a database that already has rows
is refused; pass `--reset`. Churning one that is empty is refused too — there is
nothing to mutate before the seed has run.

## Reproducibility

Two runs with the same seed produce identical row contents. That is the whole
point: two people debugging the same failure should be looking at the same
rows, and M8's determinism test needs a baseline that does not move.

Three things make it hold.

**Nothing reads the clock or the environment.** Every value descends from
`config.SEED`. "Now" is `config.as_of`, a fixed instant, so the dataset does not
drift with the calendar — bump it deliberately when the data starts to feel
stale, and expect the fingerprint to change when you do.

**Each table draws from its own stream**, seeded from `(seed, table name)`. With
one shared generator, adding a column to `patients` would consume a different
number of draws and silently change every provider, appointment and note after
it. Seeding from a string is stable across processes: `random.Random` hashes it
with SHA-512, so unlike `hash()` it is unaffected by `PYTHONHASHSEED`.

**Faker is pinned exactly.** The seed guarantees the same draws from a given
generator; it says nothing about a new Faker release changing what `first_name()`
returns for a given draw. `Faker==37.4.0` in `pyproject.toml` is load-bearing,
and `uv.lock` pins the rest.

### The fingerprint

`fingerprint.py` reduces a population to one sha256, plus a short one per table
so a mismatch says which table moved. Comparing digests is a check people
actually run; comparing two databases row by row is not.

Two things are neutralised first. **Surrogate keys**, because identity sequences
do not rewind on their own and a reseeded database has the same rows under
different ids — so the canonical form drops them and expresses each foreign key
as the parent's natural key (`mrn`, `npi`, or patient-plus-timestamp for the
tables that have no natural key of their own). And **type round-tripping**: both
the in-memory dataset and the stored rows go through the same normalisation, so
the two digests are comparable, which is what proves the load did not mangle a
`numeric` or lose a timezone on the way in.

`seed --reset` does rewind the identity sequences, because identical ids make
two runs easier to diff by eye. The fingerprint deliberately does not depend on
that having happened.

## Shape

Volume is not what makes the dataset useful. A uniform generator hides every bug
that only shows up in the tail: the patient with sixty appointments is the one
whose replay is slow and who is re-identifiable from quasi-identifiers alone,
and the patient with none is the one a join-based policy silently drops.

Appointments are apportioned across patients by largest remainder over lognormal
weights, with a few designated heavy utilizers well out in the tail. The total
is exactly what `config.Counts` asks for. At the defaults:

```
visits per patient:  0 → 30 patients   1 → 28   2 → 85   3-9 → 92   10+ → 15
heaviest:            64, 54, 45, 17, 14 ...
```

## The awkward cases

These are planted, not sampled, and `_assert_edges` refuses to return a dataset
that is missing any of them. A case that shows up "usually" is a case that is
absent from the one run where it mattered.

| case | why it is here |
| --- | --- |
| null `email`, null `ssn`, null `phone` | a policy that assumes every column is populated |
| a patient over 89 | HIPAA Safe Harbor caps age; an outlier that survives generalization re-identifies itself |
| a zip starting with zero | `02134` parsed as an integer truncates to a zip3 of `213` |
| `0.00`, `0.07`, `1234.56`, `99999999.99` | `numeric(12,2)`, which Debezium encodes as base64 `VariableScaleDecimal` by default |
| unicode names across six scripts | Cyrillic, Greek, Han, Hangul, Arabic and diacritic Latin, so a policy cannot pass by handling only accented Latin |
| SSNs in five formats, two with stray whitespace | the schema comment warns against assuming one format; a tokenizer keyed on the exact string issues two tokens for one person |
| `diagnosis_codes` empty and populated | an empty array is not a null array |
| `intake_answers` null, `{}`, and nested | free text inside jsonb, with keys that vary per row because intake forms change |
| claims with a null `appointment_id` | labs and supplies bill against the patient, not the visit |
| notes amending earlier notes | a self-referencing FK, resolved during the insert |
| PHI in `notes.body` | names, DOBs, phone numbers, employers, relatives, addresses and the MRN itself, in prose |

Zips are drawn with `postcode_in_state`, so they belong to their state: a zip
that does not makes a zip3 generalization meaningless, and zip3 is the only
geography Safe Harbor leaves behind. Phone numbers are all in the 555-01xx range
reserved for fiction, and NPIs carry a real Luhn check digit over the 80840
prefix so they pass validation wherever an NPI is validated.

## One transaction

The whole seed lands in a single transaction, so the mutation ledger records it
at one `tx_at`. A point-in-time query before that instant sees an empty
database; one after it sees the whole population. There is no instant at which
the database is half-populated and a replay could legitimately land on it.

That also means the seed is insert-only — notes that are amended are inserted
with `is_amended` already true rather than updated afterwards. Ongoing churn is
a separate concern and belongs in its own generator.

`--reset` deletes rather than truncates. `TRUNCATE` does not fire row-level
triggers, so it would empty a captured table without the ledger noticing. It
also clears the history tables, which are not themselves captured: leaving them
would mix the previous population's ledger into this one's, and the
point-in-time answer at any `T` would depend on how many times the seed had been
run.

## The timeline

`python -m loadgen` keeps mutating the seeded database: inserts, updates and
deletes, each in its own explicit transaction, paced at a configurable rate.
Where the seed gives one state, this gives a stretch of wall clock with
distinguishable instants in it — which is the only thing a point-in-time query
can actually resolve. It is also how M6 picks a meaningful `T`: a `T` with
nothing on either side of it proves nothing.

### The ledger is not written here

The `<table>_history` tables are written by the `pit_audit` trigger, inside the
same transaction as the change. That is the whole reason they can be M8's oracle
— a ledger reconstructed afterwards from CDC events would be derived from the
thing under test.

What churn does is read the ledger back, per transaction, and compare it against
what it meant to do:

```
  #12    txid 887  17:48:46.141309  detach_appointment    3 rows  appointments·D claims·D notes·U  +2 cascaded
```

One `DELETE` statement, three tables of history. `claims.appointment_id` is
`ON DELETE CASCADE` so the claims disappear; `notes.appointment_id` is
`ON DELETE SET NULL` so the notes arrive as *updates*. A pipeline that only
watches for deletes gets the notes wrong, and `+2 cascaded` is where that is
visible. A transaction that mutated rows but produced no ledger entries is rolled
back and reported as an error rather than committed, because a mutation the
oracle cannot see is worse than a failed run.

### Transaction shapes

Thirteen of them, weighted. Each declares which tables it touches, and the two
things worth calling out are why any of them touch more than one:

| shape | why it is here |
| --- | --- |
| `register_patient` | patient + appointment + intake note in **one** transaction. A replay that lands inside it shows an appointment for a patient who does not exist — the case `--snap-to-txn` exists for |
| `complete_visit` | appointment closes, note written, claim submitted, one `tx_at`. Also the row that ends up with the most versions: scheduled → checked in → completed |
| `adjudicate_claim` | a claim submitted now and paid four transactions later has two versions, and the answer at `T` depends on which side of the adjudication `T` falls |
| `batch_adjudication` | one `UPDATE`, five to twenty rows, all at the same `tx_at`. A per-row applier that is not transaction-aware stops in the middle of this and is wrong about a dozen claims at once |
| `amend_note` | an addendum plus the note it supersedes: a self-referencing FK resolved inside the transaction |
| `correct_demographics` | the load-bearing one for de-identification. A phone number changes, so the same patient now has two tokens and the pipeline has to be right about which one a query at `T` should see |
| `detach_appointment` | the cascade above, with a candidate chosen so that both the cascade and the SET NULL actually happen |
| `purge_patient` | one `DELETE`, four tables of history. Bounded to a patient with a small footprint so it does not take a visible slice of the population with it |

Every shape runs once, in order, before the loop goes weighted-random. Without
that, `purge_patient` at weight 1.5 out of ~85 shows up in maybe one short run in
three, and a run that never produced a cascade delete is a run that proved less
than it appears to. `--no-warmup` turns it off.

New patients are drawn from a pre-generated population rather than built inline,
so the awkward cases in the table above — null email, over 89, leading-zero zip,
unicode name — turn up in churn-inserted rows too. Note bodies are rendered from
the patient's real demographics, read back from the row, because a note whose
names and dates do not match the columns they came from lets a de-identification
policy pass by coincidence.

### Bounded three ways

Cleaned topics run at infinite retention on a laptop PVC, so an unbounded
generator fills the disk and takes the cluster with it. There is no unbounded
mode. Whichever ceiling is reached first ends the run:

| ceiling | default | why |
| --- | --- | --- |
| `--duration` | `5m` | wall clock |
| `--max-txns` | `2000` | transactions |
| `--max-ledger-rows` | `50000` | the one that actually matters — every ledger row becomes a CDC event, and nothing downstream drops them |

The *population* is bounded separately. Insert and delete weights are biased by
how full each table is relative to a band around its configured size (0.7× to
1.4×), clamped hard at both edges: above the band inserts nearly stop, below it
deletes stop entirely. The fullest table governs, not the average — one table
running away is the failure worth stopping, and an average lets it hide behind
four that are fine. The band is anchored to `config.Counts` rather than to the
row counts observed at startup, so running churn ten times does not ratchet the
database up to 1.4¹⁰ times its size.

### One writer, so `tx_at` strictly increases

Each transaction starts only after the previous one committed, so
`transaction_timestamp()` — the ledger's point-in-time key — is strictly
increasing across the run, and every transaction is a distinct point a `T` can
land between. That is a property of there being *one* writer, not a property of
the ledger, so it is checked rather than assumed: the summary reports how many
transactions arrived out of order, and `--verify` fails if two transactions ever
share a `tx_at`. A second churn process against the same database breaks it.

Timing is the one thing here that is not deterministic. *Which* mutation happens
next, and every value inside it, descends from `config.SEED` exactly as the
seed's rows do; *when* it happens does not, because `offsets_for_times` cannot
resolve a fake timeline. Two runs against the same seeded database apply the same
plan at different instants.

### What `--verify` checks

Run automatically at the end of a churn run, and available on its own. Per
captured table:

- the ledger is **append-only**: `history_id` order and `tx_at` order agree
- a transaction is **one instant**: no `txid` appears at two different `tx_at`
- two transactions are **two instants**: distinct `txid` count equals distinct
  `tx_at` count, or there are points on the timeline nothing can tell apart
- some transactions touched **more than one table**, or `--snap-to-txn` has
  nothing to be right or wrong about
- **the ledger replays to the live tables.** For each key, take the `after_row`
  of the newest entry at or before `T` — absent if that entry was a delete — and
  the result has to be the live table, row for row. That is exactly the query M8
  will run at an arbitrary `T`; running it at `T = now` is the one case where the
  answer can be checked against something independent.

## Tests

`make seed-test` runs everything that does not need a database, which is most of
it, including a cross-process determinism check that runs the generator twice
under different `PYTHONHASHSEED` values — in-process comparison cannot catch
generation order leaking out of a set, because a set iterates consistently
within one process.

Set `PIT_TEST_DSN` to also run the tests that need a live source-pg: the seed's
round trip through Postgres, the assertion that the ledger recorded the whole
seed as one transaction of nothing but inserts, and the churn tests — that a
short run moves the row counts and reaches every shape, that commit timestamps
strictly increase, that cascades reach the ledger, and that replaying the ledger
reproduces the live tables.

One of those deserves singling out. `test_each_transaction_is_its_own_transaction`
exists because psycopg only turns a `conn.transaction()` block into a real
`BEGIN`/`COMMIT` when the connection is in autocommit mode. Without it, the first
read opens an implicit transaction, every transaction block after it degrades to a
savepoint inside that one, and a whole run of hundreds of mutations commits as a
single transaction at a single `tx_at` — a timeline with exactly one point in it.
The failure is completely silent and it invalidates everything downstream, so
`run()` refuses a connection that is not in autocommit mode.
