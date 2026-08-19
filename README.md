# Point-in-Time De-Identified Database Replica (PoC)

A local, reproducible way to stand up a Postgres database that mirrors a
primary **as of an arbitrary point in time**, with all PHI/PII removed. The
source database is synthetic — **no real PHI enters this project at any point.**

## Pipeline

```
source Postgres ──(Debezium CDC)──▶ raw.*  topics ──(de-id transformer)──▶ clean.* topics ──(applier)──▶ sink Postgres
   (M2)                (M3)          (Redpanda)          (M4)               (Redpanda)        (M5/M6/M7)      (pit_base + PIT dbs)
```

Two load-bearing ideas:

1. **A point in time is an offset manifest, not a timestamp.** Each cleaned
   record's Kafka timestamp is set to `source.ts_ms` (the DB commit time), so
   `offsets_for_times(T)` resolves "the database as of T" to an exact offset per
   partition. That offset set is the manifest.
2. **The schema registry enforces the de-id policy.** The clean Avro schema is
   derived from `(raw schema, policy)`. A source column nobody wrote a policy
   rule for halts that one topic at startup instead of leaking downstream.

Everything runs on Kubernetes under an umbrella Helm chart, with the official
Redpanda chart as a dependency, so the same charts move to a shared cluster
later.

## Repo layout

```
Makefile                       inner-loop targets (make help)
charts/pit/                    umbrella Helm chart
  Chart.yaml                   depends on redpanda (charts.redpanda.com) + source-pg
  values.yaml                  cross-environment invariants
  values-local.yaml            laptop sizing (1 broker, TLS off, small)
  charts/
    source-pg/                 Postgres 16 source, wal_level=logical    (M2)
      files/initdb/20-clinic-schema.sql     clinic tables, synthetic PHI/PII
      files/initdb/21-history-triggers.sql  the mutation ledger
    sink-pg/                   PIT sink Postgres               (M5)
    connect/                   Debezium + Avro Connect         (M3)
      connectors/source-pg.json  the Debezium connector config (registered by a hook Job)
    deid/                      de-identification transformer   (M4)
    pitctl/                    pit-tail / snapshot / restore   (M5/M7)
images/
  connect/                     Connect image: Debezium + Confluent Avro converter (M3)
loadgen/                       deterministic synthetic load generator   (M2)
  src/loadgen/config.py        seed constant, counts, distributions
  src/loadgen/seed.py          the generator, loader and CLI
scripts/
  verify-conf-docker.sh        cluster-free check of the rendered chart
  verify-schema.sql            schema + ledger assertions, run by both paths
spikes/
  data-703-debezium-avro-registry/   registry-accepts-Debezium-Avro spike + findings (M3)
```

## Prerequisites

- Docker Desktop with **≥ 8.5 GB** allocated to its VM (Settings → Resources).
  The `pit` node asks Docker for 8 GB (`--memory=8192`); Docker Desktop needs a
  little more than that for its own overhead. If Docker gives less, `make up`
  exits with `Docker Desktop has only NNNN MB memory but you specified 8192MB`.
  Either raise the Docker Desktop memory or run `make up MEMORY=<fits>`.
- `minikube`, `kubectl`, `helm` (v3.10+), and `uv` on PATH.

## Quickstart (runbook)

```bash
make up          # start the pit minikube profile + namespace, deploy the stack
make verify-all  # both acceptance checks against the cluster
make forward     # port-forward console/registry (+ connect/postgres once they exist)

# verify the broker and registry answer:
curl -s localhost:8081/subjects      # -> []   (no schemas registered yet)
open  http://localhost:8080          # Redpanda Console

# populate the source database (needs `make forward` running):
make seed        # wipe and repopulate the clinic schema
make seed-verify # load generator acceptance check

make nuke        # tear everything down, including PVCs
```

`make up` is idempotent — re-run it after editing values. `make nuke` deletes
the whole minikube profile, so PVCs go too and Postgres init SQL re-runs on the
next `make up`. `make clean` is the narrower version: it drops the release and
its PVCs but leaves the cluster standing.

`make verify-docker` runs the same schema assertions with no cluster at all.

Run `make help` for every target.

## Access table

After `make forward` (leave it running in its own terminal):

| Component        | In-cluster service      | Local port | Reach it                                        | Status        |
| ---------------- | ----------------------- | ---------- | ----------------------------------------------- | ------------- |
| Redpanda Console | `pit-console`           | 8080       | `open http://localhost:8080`                    | ✅ M1          |
| Schema Registry  | `pit-redpanda` (broker) | 8081       | `curl localhost:8081/subjects` → `raw.public.*` | ✅ M1          |
| Kafka Connect    | `pit-connect`           | 8083       | `curl localhost:8083/connectors` → `["source-pg"]` | ✅ M3       |
| Source Postgres  | `pit-source-pg`         | 5432       | `psql -h localhost -p 5432 -U pit -d pit`       | ✅ M2          |
| Sink Postgres    | `sink-pg`               | 5433       | `psql -h localhost -p 5433 -U postgres`         | ⏳ pending M5  |
| Kafka broker     | `pit-redpanda`          | 9093       | in-cluster only (`pit-redpanda:9093`)           | ✅ M1          |

The Kafka broker's Kafka API is not port-forwarded — clients run inside the
cluster and address `pit-redpanda:9093` (and the registry at
`pit-redpanda:8081`) directly.

## Milestone status

- **M1 — Cluster and chart skeleton:** ✅
- **M2 — Source Postgres, clinic schema, mutation ledger:** ✅
- **M3 — Connect image and Debezium Avro source:** ✅
- M4–M8: see the [Linear project](https://linear.app/headway/project/point-in-time-de-identified-database-replica-poc-a605b4c0031e/overview).

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

## Load generator

`loadgen/` fills the schema with synthetic patients, providers, appointments,
claims and notes. Everything it produces is fake — it takes an integer and
returns rows, with no network, no filesystem and no clock involved.

```
make loadgen-deps   # uv sync
make forward        # in another shell
make seed           # wipe and repopulate
make seed-verify    # acceptance check
```

Two runs with the same seed produce identical row contents, so two people
debugging the same failure are looking at the same rows and M8's determinism
test has a stable baseline. `seed-verify` proves it by seeding twice and
comparing a digest of what landed, which also catches anything the load itself
mangles on the way into Postgres — a `numeric` rounded, a timezone lost.

Shape matters more than volume: appointments are apportioned across patients by
largest remainder over lognormal weights, so a few patients carry dozens of
visits, most have one or two, and some have none. The awkward cases the
de-identification policy has to survive are planted rather than sampled — null
emails, a patient past the Safe Harbor age cap, a zip starting with a zero,
decimal money at both ends of `numeric(12,2)`, unicode names across six scripts,
SSNs in five formats, and PHI woven into `notes.body` — and the generator
refuses to return a dataset that is missing any of them.

The whole seed lands in one transaction, so the ledger records it at a single
`tx_at`: a point-in-time query before that instant sees an empty database and
one after it sees the whole population, with no half-loaded state in between for
a replay to land on. `loadgen/README.md` has the details.

## connect — Debezium Avro CDC

Kafka Connect running the Debezium Postgres source, writing Avro to `raw.*`
topics with schemas registered in Redpanda. The Connect worker, its Service on
`:8083`, and an idempotent registration Job are the `connect` subchart; the image
is `images/connect/`.

**The image.** Debezium 2.0+ dropped the bundled Confluent Avro converter, and
Redpanda's registry speaks the Confluent API — so the image is
`quay.io/debezium/connect:3.0` plus `io.confluent:kafka-connect-avro-converter`
resolved (with its full transitive set) by a Maven build stage into a plugin
directory. Resolving with Maven rather than a hand-picked jar list is deliberate:
a missing transitive shows up as a `ClassNotFoundException` at connector start,
not at build. The converter jar is Confluent Community License — fine local,
worth a conversation before anywhere shared.

**The connector config** (`charts/pit/charts/connect/connectors/source-pg.json`).
Four settings are not defaults and each prevents a specific downstream failure:

- `enhanced.avro.schema.support=true` — Debezium's nested records and unions need it.
- `time.precision.mode=connect` — emit real Avro logical types (`date`,
  `timestamp-millis`) instead of Debezium semantic types that serialize as bare
  longs `fastavro` won't decode as datetimes.
- `decimal.handling.mode=precise` — `bytes` + `logicalType: decimal`, which decodes
  to `Decimal`; the default `VariableScaleDecimal` compares equal to nothing.
- `field.name.adjustment.mode=avro` — Avro names must match `[A-Za-z_][A-Za-z0-9_]*`.

**Registration.** A `post-install`/`post-upgrade` hook Job waits for the Connect
REST API, then `PUT /connectors/{name}/config` for each file in the connectors
ConfigMap — `PUT` is idempotent, so `helm upgrade` re-applies config instead of
failing on a name conflict. The connector name is the JSON filename. Connect's
internal config/offset/status topics are RF 1 here (the defaults of 3 never form
on a single broker).

**Registry compatibility** was the gating risk. The DATA-703 spike
(`spikes/data-703-debezium-avro-registry/`) confirmed Redpanda's registry accepts
Debezium's namespaced envelope schema and that `fastavro` round-trips the logical
types — so no alternative serialization path is needed for M4–M8. One detail for
M4: the registry canonicalizes named-type references to their relative form, so
compare derived schemas by parsed form, not exact JSON string.

### Verifying

```
make forward                              # in another shell
curl -s localhost:8083/connector-plugins  # lists io.debezium...PostgresConnector
curl -s localhost:8083/connectors         # -> ["source-pg"]
curl -s localhost:8081/subjects           # lists raw.public.<table>-key/-value
```

## PIT window lower bound

**The PIT window starts at CDC-enable time, not at the beginning of the source
database's history.** Debezium's initial snapshot stamps every pre-existing row
with the snapshot's wall clock, not the row's original commit time — so every row
that existed before CDC was enabled lands at the same instant, and any point in
time earlier than that is undefined.

Left unguarded this is the worst failure mode available: a query that looks like
it works and quietly returns wrong answers. So the enable time is recorded where
the tooling can read it, in a ConfigMap written at first install and preserved
across upgrades:

```
kubectl -n pit-poc get cm pit-connect-cdc-window -o jsonpath='{.data.cdcEnabledAt}'
```

**Requirement for M6:** `pit restore --at T` (and the underlying
timestamp→offset resolution) must **reject** a `T` earlier than `cdcEnabledAt`
with a clear error, rather than return a plausible-looking database. This is a
hard constraint on the replay work, not a nice-to-have.
