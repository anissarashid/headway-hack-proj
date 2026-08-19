# loadgen

Synthetic load for the clinic schema. Everything it produces is fake: the
generator takes an integer and returns rows, with no network, no filesystem and
no clock involved, so there is no path by which real patient data could reach
it.

```
src/loadgen/config.py       the seed constant, row counts, distributions, edge cases
src/loadgen/vocab.py        clinical vocabulary and note templates
src/loadgen/seed.py         the generator, the loader, and the CLI
src/loadgen/fingerprint.py  reduce a population to a comparable digest
tests/test_seed.py          determinism, counts, shape, and the awkward cases
```

## Running it

```
make loadgen-deps    # uv sync
make forward         # in another shell
make seed            # wipe and repopulate
make seed-verify     # the acceptance check
```

Or directly, from `loadgen/`:

```
uv run python -m loadgen.seed --reset        # wipe and repopulate
uv run python -m loadgen.seed --dry-run      # generate and digest, no database
uv run python -m loadgen.seed --fingerprint  # digest what is in the database now
uv run python -m loadgen.seed --seed 12345   # a different population
```

Connection comes from `PIT_DSN`, or from the usual `PG*` variables, defaulting
to the dev release on `localhost:5432`. Seeding a database that already has rows
is refused; pass `--reset`.

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

## Tests

`make seed-test` runs everything that does not need a database, which is most of
it, including a cross-process determinism check that runs the generator twice
under different `PYTHONHASHSEED` values — in-process comparison cannot catch
generation order leaking out of a set, because a set iterates consistently
within one process.

Set `PIT_TEST_DSN` to also run the two tests that need a live source-pg: the
round trip through Postgres, and the assertion that the ledger recorded the
whole seed as one transaction of nothing but inserts.
