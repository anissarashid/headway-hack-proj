# headway-hack-proj

A CDC pipeline for exercising point-in-time correctness: a Postgres source with
logical decoding enabled, a clinic schema carrying deliberately varied PHI/PII
shapes, a load generator that produces real history to replay, and a mutation
ledger that acts as the correctness oracle downstream.

## Layout

```
charts/pit/                       umbrella chart
charts/pit/charts/source-pg/      Postgres 16 source, wal_level=logical
  files/initdb/20-clinic-schema.sql     clinic tables, synthetic PHI/PII
  files/initdb/21-history-triggers.sql  the mutation ledger
scripts/verify-conf-docker.sh     cluster-free check of the rendered chart
scripts/verify-schema.sql         schema + ledger assertions, run by both paths
```

## Requirements

`helm`, `kubectl`, `docker`, and a local cluster (`minikube` by default).

```
brew install helm kubectl minikube
```

## Quickstart

```
make cluster     # start minikube
make install     # helm upgrade --install, waits for readiness
make verify-all  # both acceptance checks
```

`make help` lists everything.

## source-pg

Postgres 16 as a StatefulSet, with a ConfigMap-supplied `postgresql.conf` passed
via `-c config_file=`, a PVC from `volumeClaimTemplates`, a headless Service for
stable DNS plus a ClusterIP Service for clients, and init SQL mounted at
`/docker-entrypoint-initdb.d`.

The point of the chart is these three settings:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Debezium decodes changes through a logical replication slot. Without
`wal_level=logical` the WAL carries no row images, slot creation fails, and there
is no CDC — so nothing downstream exists. `wal_level` cannot be changed at
runtime, which is why it lives in `postgresql.conf` rather than `ALTER SYSTEM`.

We author this chart instead of depending on Bitnami's: Bitnami moved its free
images to `bitnamilegacy` in 2025 and the catalog now points at a paid registry.
A StatefulSet plus a ConfigMap is a short file and gives direct control over the
one setting that matters.

Because `config_file` replaces the `postgresql.conf` that `initdb` writes into
`PGDATA`, the rendered file must declare `data_directory`, `hba_file`, and
`ident_file`. They are set from `.Values.pgData` and must stay in step with the
`PGDATA` env var on the container.

### Verifying

`make verify` waits for rollout, prints the relevant `pg_settings`, asserts
`SHOW wal_level` is `logical`, and then creates and drops a real `pgoutput` slot
— the setting reading correctly is not the same as decoding working.

`make verify-docker` runs the same assertions against the rendered ConfigMaps
under plain docker, no cluster needed.

### Credentials

Dev defaults live in `charts/pit/charts/source-pg/values.yaml`. Debezium connects
as `debezium` (`LOGIN REPLICATION`), created by the init script. Point
`auth.existingSecret` at a Secret with keys `postgres-password` and
`replication-password` to supply real ones.

### Extending

`extraInitScripts` is a filename-to-content map merged into the initdb ConfigMap.
Use a `30-` or higher prefix; the chart owns `10-` (roles) and `20-` (schema).
Note that initdb scripts run only on first boot of an empty volume — `make clean`
drops the PVC when you need a rebuild.

## Clinic schema

Five captured tables, all synthetic, in `public`:

| table | shape that makes it interesting |
| --- | --- |
| `patients` | name split across columns, `date_of_birth date`, `ssn`, email, phone, address split with `postal_code` separate |
| `providers` | `npi`, a public identifier that is still a strong re-identification handle |
| `appointments` | FKs to patient and provider, four timestamps, an enum status, plus free text nested in `intake_answers jsonb` |
| `claims` | `numeric(12,2)` money and `diagnosis_codes text[]` |
| `notes` | `body text` — unstructured PHI mixed into clinical prose, the hardest case |

The shapes are deliberately awkward. A de-identification policy that only knows
how to hash scalar text columns will pass `intake_answers` and `diagnosis_codes`
straight through, and masking `date_of_birth` to NULL destroys every age cohort
that made the data worth keeping. Set against that, `notes.body` is the case that
decides whether the policy works at all.

Foreign keys are kept here and are not needed on the sink: this stands in for a
realistic operational primary, and the `ON DELETE` actions are part of what makes
replay hard. They mix on purpose — `CASCADE` from `patients` (one statement fans
out into deletes across three tables), `RESTRICT` on `providers`, and `SET NULL`
on `notes.appointment_id`, so deleting an appointment shows up on `notes` as an
*update*. A pipeline that only watches the deleted table gets that one wrong.

`decimal.handling.mode` matters for `claims`: Debezium's default encodes
`numeric` as base64 `VariableScaleDecimal`, which compares equal to nothing.

### The mutation ledger

Every captured table has a `<table>_history` twin, written inside the same
transaction as the change by an `AFTER INSERT OR UPDATE OR DELETE` trigger:

```
op          I, U or D
txid        the transaction id
tx_at       transaction_timestamp() -- the point-in-time key
stmt_at     statement_timestamp()   -- which statement inside the transaction
recorded_at clock_timestamp()       -- real elapsed time, for debugging
pk          the primary key, as jsonb, so composite keys need no special case
before_row  the whole row before, for U and D
after_row   the whole row after, for I and U
```

This is the correctness oracle M8 compares against, which is why it is recorded
here rather than reconstructed from CDC events later — reconstructing it from the
thing under test would not be an oracle. The expected state of a table at time
`T` is one query:

```sql
SELECT after_row
FROM (
  SELECT DISTINCT ON (pk) pk, op, after_row
  FROM patients_history
  WHERE tx_at <= :t
  ORDER BY pk, tx_at DESC, history_id DESC
) s
WHERE op <> 'D';
```

`tx_at` rather than a clock reading because it is identical for every row in a
transaction, so a multi-statement transaction lands at a single point on the
timeline and can never be half-visible to a query at `T`. `stmt_at` is how you
order two changes to the same row inside one transaction.

`REPLICA IDENTITY FULL` is set on all five: without it an update or delete
reaches the WAL carrying only the key columns, and there is no before image for
the oracle to compare against.

One known gap: `TRUNCATE` does not fire row-level triggers, so it is invisible to
the ledger. Don't truncate a captured table — delete instead.

### Adding a captured table

Create the table with a primary key, then one call:

```sql
SELECT pit_install_capture('public.referrals');
```

That sets `REPLICA IDENTITY FULL`, builds `referrals_history`, installs the audit
trigger with the primary-key columns as arguments, and adds `updated_at`
maintenance if the column exists. It is idempotent, so it can be re-run against a
live database. The history table holds two jsonb documents rather than a mirror of
the source columns, so a later `ALTER TABLE` cannot silently stop being recorded.

`pit_captured_tables` reports what is actually captured, derived from the
installed triggers rather than from a hardcoded list:

```
make psql -- then: table pit_captured_tables;
```

### Verifying the schema

```
make verify-schema   # in-cluster
make verify-docker   # no cluster; runs the same SQL
```

Both run `scripts/verify-schema.sql`, which asserts the tables exist, that
`pg_class.relreplident` is `f` for each captured table, that the source keeps its
foreign keys, and that inserts, updates, cascade deletes and `SET NULL` all land
in the ledger with the right before and after images. It works on a loaded
database as well as an empty one: the fixtures are created inside a transaction
that is rolled back at the end.
